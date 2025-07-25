// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2025 Rust Nostr Developers
// Distributed under the MIT software license

//! Event ingester with automatic batching.
//!
//! Provides async event processing with automatic batching for optimal LMDB write performance.
//! Events are collected from a channel and committed in batches using a single transaction.

use std::iter;

use flume::{Receiver, Sender};
use nostr::Event;
use nostr_database::{FlatBufferBuilder, SaveEventStatus};
use tokio::sync::oneshot;

use super::error::Error;
use super::lmdb::Lmdb;
use super::Filter;

/// Pre-allocated buffer size for FlatBufferBuilder
///
/// This size is chosen to handle most Nostr events without reallocation.
/// Large events (with many tags or large content) may still trigger reallocation.
pub(crate) const FLATBUFFER_CAPACITY: usize = 70_000;

enum OperationResult {
    Save {
        result: Result<SaveEventStatus, Error>,
        tx: Option<oneshot::Sender<Result<SaveEventStatus, Error>>>,
    },
    Delete {
        result: Result<(), Error>,
        tx: Option<oneshot::Sender<Result<(), Error>>>,
    },
}

impl OperationResult {
    /// Send the result through the channel if present, or log errors
    fn send(self) {
        match self {
            Self::Save { result, tx } => {
                if let Some(tx) = tx {
                    if tx.send(result).is_err() {
                        tracing::debug!("Failed to send save result: receiver dropped");
                    }
                } else if let Err(e) = result {
                    tracing::error!(error = %e, "Event save failed in batch");
                }
            }
            Self::Delete { result, tx } => {
                if let Some(tx) = tx {
                    if tx.send(result).is_err() {
                        tracing::debug!("Failed to send delete result: receiver dropped");
                    }
                } else if let Err(e) = result {
                    tracing::error!(error = %e, "Delete operation failed in batch");
                }
            }
        }
    }
}

pub(super) enum IngesterOperation {
    SaveEvent {
        event: Event,
        tx: Option<oneshot::Sender<Result<SaveEventStatus, Error>>>,
        scope: scoped_heed::Scope,
    },
    Delete {
        filter: Filter,
        tx: Option<oneshot::Sender<Result<(), Error>>>,
        scope: scoped_heed::Scope,
    },
}

impl IngesterOperation {
    /// Create an error result for this operation type, moving the channel
    fn into_error_result(self, error: Error) -> OperationResult {
        match self {
            Self::SaveEvent { tx, .. } => OperationResult::Save {
                result: Err(error),
                tx,
            },
            Self::Delete { tx, .. } => OperationResult::Delete {
                result: Err(error),
                tx,
            },
        }
    }
}

pub(super) struct IngesterItem {
    pub(super) operation: IngesterOperation,
}

impl IngesterItem {
    #[must_use]
    pub(super) fn save_event_with_feedback_scoped(
        event: &Event,
        scope: scoped_heed::Scope,
    ) -> (Self, oneshot::Receiver<Result<SaveEventStatus, Error>>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                operation: IngesterOperation::SaveEvent {
                    event: event.clone(),
                    tx: Some(tx),
                    scope,
                },
            },
            rx,
        )
    }

    #[must_use]
    pub(super) fn delete_with_feedback_scoped(
        filter: Filter,
        scope: scoped_heed::Scope,
    ) -> (Self, oneshot::Receiver<Result<(), Error>>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                operation: IngesterOperation::Delete {
                    filter,
                    tx: Some(tx),
                    scope,
                },
            },
            rx,
        )
    }
}

#[derive(Debug)]
pub(super) struct Ingester {
    db: Lmdb,
    rx: Receiver<IngesterItem>,
}

impl Ingester {
    fn process_operation(
        &self,
        read_txn: &heed::RoTxn,
        txn: &mut heed::RwTxn,
        item: IngesterItem,
        fbb: &mut FlatBufferBuilder,
    ) -> OperationResult {
        match item.operation {
            IngesterOperation::SaveEvent { event, tx, scope } => {
                let result = self
                    .db
                    .save_event_with_txn_scoped(read_txn, txn, fbb, &scope, &event);
                OperationResult::Save { result, tx }
            }
            IngesterOperation::Delete { filter, tx, scope } => {
                let result = self
                    .db
                    .delete_with_txn_scoped(read_txn, txn, &scope, filter)
                    .map(|_| ()); // Convert Result<usize, Error> to Result<(), Error>
                OperationResult::Delete { result, tx }
            }
        }
    }

    /// Build and spawn a new ingester
    pub(super) fn run(db: Lmdb) -> Sender<IngesterItem> {
        // Create new flume channel (unbounded for maximum performance)
        let (tx, rx) = flume::unbounded();

        // Construct and spawn ingester
        let ingester = Self { db, rx };
        ingester.spawn_ingester();

        tx
    }

    fn spawn_ingester(self) {
        tokio::task::spawn_blocking(move || {
            self.run_ingester_loop();
        });
    }

    fn run_ingester_loop(self) {
        #[cfg(debug_assertions)]
        tracing::debug!("Ingester thread started with commit batching");

        let mut fbb = FlatBufferBuilder::with_capacity(FLATBUFFER_CAPACITY);
        let mut results = Vec::new();

        loop {
            let first_item = match self.rx.recv() {
                Ok(item) => item,
                Err(flume::RecvError::Disconnected) => {
                    #[cfg(debug_assertions)]
                    tracing::debug!("Ingester channel disconnected, exiting");
                    break;
                }
            };
            let batch = iter::once(first_item).chain(self.rx.drain());

            // Process batch, reusing the results vector
            self.process_batch_in_transaction(batch, &mut fbb, &mut results);

            #[cfg(debug_assertions)]
            tracing::debug!("Processed batch of {} operations", results.len());

            // Send results back through channels
            for result in results.drain(..) {
                result.send();
            }
        }

        #[cfg(debug_assertions)]
        tracing::debug!("Ingester thread exited");
    }

    fn process_batch_in_transaction(
        &self,
        batch: impl Iterator<Item = IngesterItem>,
        fbb: &mut FlatBufferBuilder,
        results: &mut Vec<OperationResult>,
    ) {
        // Open read transaction first - it's cheaper and doesn't block other readers
        let read_txn = match self.db.read_txn() {
            Ok(txn) => txn,
            Err(e) => {
                tracing::error!(error = %e, "Failed to create read transaction");
                // Send BatchTransactionFailed for all items
                for item in batch {
                    let error_result = item
                        .operation
                        .into_error_result(Error::BatchTransactionFailed);
                    results.push(error_result);
                }
                return;
            }
        };

        // Open write transaction only after read transaction is ready
        let mut write_txn = match self.db.write_txn() {
            Ok(txn) => txn,
            Err(e) => {
                tracing::error!(error = %e, "Failed to create write transaction");
                // Send BatchTransactionFailed for all items
                for item in batch {
                    let error_result = item
                        .operation
                        .into_error_result(Error::BatchTransactionFailed);
                    results.push(error_result);
                }
                return;
            }
        };

        let mut batch_iter = batch;

        for (index, item) in (&mut batch_iter).enumerate() {
            let result = self.process_operation(&read_txn, &mut write_txn, item, fbb);

            // Check if we need to abort on actual database errors (not on rejections)
            let abort_on_error = match &result {
                OperationResult::Save { result: Err(e), .. } => {
                    tracing::error!(error = %e, "Failed to save event, aborting batch");
                    true
                }
                OperationResult::Delete { result: Err(e), .. } => {
                    tracing::error!(error = %e, "Failed to delete event, aborting batch");
                    true
                }
                OperationResult::Save {
                    result: Ok(SaveEventStatus::Rejected(_)),
                    ..
                } => false, // Rejections are expected, don't abort
                _ => false,
            };

            if abort_on_error {
                // The operation that caused the error keeps its original error (already in result)
                results.push(result);

                // All previously processed operations get BatchFailed error
                for prev_result in results.iter_mut().take(index) {
                    match prev_result {
                        OperationResult::Save { result: res, .. } => {
                            *res = Err(Error::BatchTransactionFailed)
                        }
                        OperationResult::Delete { result: res, .. } => {
                            *res = Err(Error::BatchTransactionFailed)
                        }
                    }
                }

                // All remaining operations get BatchFailed error
                for item in batch_iter {
                    let error_result = item
                        .operation
                        .into_error_result(Error::BatchTransactionFailed);
                    results.push(error_result);
                }

                // Abort the write transaction first, then drop read transaction
                write_txn.abort();
                drop(read_txn);
                tracing::warn!("Transaction aborted due to errors");

                return;
            }

            results.push(result);
        }

        // If we get here, no errors occurred during processing
        // Commit write transaction first, then drop read transaction
        match write_txn.commit() {
            Ok(()) => {
                drop(read_txn);
                #[cfg(debug_assertions)]
                tracing::debug!("Transaction committed successfully");
            }
            Err(e) => {
                drop(read_txn);
                tracing::error!(error = %e, "Failed to commit transaction");
                // Convert all results to BatchTransactionFailed
                for result in results.iter_mut() {
                    match result {
                        OperationResult::Save { result: res, .. } => {
                            *res = Err(Error::BatchTransactionFailed)
                        }
                        OperationResult::Delete { result: res, .. } => {
                            *res = Err(Error::BatchTransactionFailed)
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use futures::future::join_all;
    use nostr::{EventBuilder, Keys, Kind};
    use scoped_heed::Scope;
    use tempfile::TempDir;

    use super::*;
    use crate::store::Store;

    async fn setup_test_store() -> (Arc<Store>, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let store = Store::open(temp_dir.path(), 1024 * 1024 * 10, 10, 50)
            .expect("Failed to open test store");
        (Arc::new(store), temp_dir)
    }

    /// Helper to execute futures concurrently and measure duration
    async fn execute_concurrent_saves<F, Fut, T>(
        events: &[Event],
        store: Arc<Store>,
        save_fn: F,
    ) -> Duration
    where
        F: Fn(Arc<Store>, Event) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let start = Instant::now();

        let futures: Vec<_> = events
            .iter()
            .map(|event| {
                let store = Arc::clone(&store);
                let event = event.clone();
                save_fn(store, event)
            })
            .collect();

        join_all(futures).await;
        start.elapsed()
    }

    #[tokio::test]
    async fn test_batching_vs_sequential() {
        let (store, _temp_dir) = setup_test_store().await;
        let keys = Keys::generate();

        const NUM_EVENTS: usize = 100;

        // Create events
        let mut events = Vec::new();
        for i in 0..NUM_EVENTS {
            let event = EventBuilder::text_note(format!("Test event {}", i))
                .sign_with_keys(&keys)
                .expect("Failed to sign event");
            events.push(event);
        }

        // Test 1: Saves using individual transactions (no batching)
        let transaction_duration =
            execute_concurrent_saves(&events, Arc::clone(&store), |store, event| async move {
                // Create a new transaction for each event
                let mut txn = store
                    .db
                    .write_txn()
                    .expect("Failed to create write transaction");
                let read_txn = store
                    .db
                    .read_txn()
                    .expect("Failed to create read transaction");
                let mut fbb = FlatBufferBuilder::with_capacity(FLATBUFFER_CAPACITY);

                store
                    .db
                    .save_event_with_txn_scoped(
                        &read_txn,
                        &mut txn,
                        &mut fbb,
                        &Scope::Default,
                        &event,
                    )
                    .expect("Failed to save event");

                txn.commit().expect("Failed to commit transaction");
            })
            .await;

        // Clear database
        store.wipe().expect("Failed to wipe");

        // Test 2: Saves using ingester (automatic batching)
        let batched_duration =
            execute_concurrent_saves(&events, Arc::clone(&store), |store, event| async move {
                store
                    .save_event(&event)
                    .await
                    .expect("Failed to save event");
            })
            .await;

        println!("Transaction-based saves: {:?}", transaction_duration);
        println!("Batched saves (ingester): {:?}", batched_duration);
        println!(
            "Speedup: {:.1}x",
            transaction_duration.as_secs_f64() / batched_duration.as_secs_f64()
        );

        // Batched saves should be significantly faster
        assert!(
            batched_duration < transaction_duration / 2,
            "Batched saves should be at least 2x faster than transaction-based saves"
        );
    }

    #[tokio::test]
    async fn test_batch_error_handling() {
        let (store, _temp_dir) = setup_test_store().await;
        let keys = Keys::generate();

        // Create a mix of valid and duplicate events
        let event1 = EventBuilder::text_note("Event 1")
            .sign_with_keys(&keys)
            .expect("Failed to sign event");

        // Save event1 first
        store
            .save_event(&event1)
            .await
            .expect("Failed to save event");

        // Now try to save a batch with duplicate and new events
        let event2 = EventBuilder::text_note("Event 2")
            .sign_with_keys(&keys)
            .expect("Failed to sign event");

        let event3 = EventBuilder::text_note("Event 3")
            .sign_with_keys(&keys)
            .expect("Failed to sign event");

        let futures = vec![
            store.save_event(&event1), // Duplicate
            store.save_event(&event2), // New
            store.save_event(&event3), // New
        ];

        let results = join_all(futures).await;

        // First should be rejected as duplicate
        assert!(matches!(
            results[0],
            Ok(SaveEventStatus::Rejected(
                nostr_database::RejectedReason::Duplicate
            ))
        ));

        // Others should succeed
        assert!(matches!(results[1], Ok(SaveEventStatus::Success)));
        assert!(matches!(results[2], Ok(SaveEventStatus::Success)));

        // Verify the new events were saved
        let saved_count = store.query(Filter::new()).expect("Failed to query").len();
        assert_eq!(saved_count, 3); // event1, event2, event3
    }

    #[tokio::test]
    async fn test_ingester_channel_disconnect() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = Lmdb::new(temp_dir.path(), 1024 * 1024, 10, 50).expect("Failed to create Lmdb");

        // Create ingester
        let sender = Ingester::run(db);

        // Drop the sender to disconnect the channel
        drop(sender);

        // Give ingester thread time to exit
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The ingester should have exited gracefully
        // (We can't directly test this, but the test shouldn't hang)
    }

    #[tokio::test]
    async fn test_flatbuffer_reuse() {
        let (store, _temp_dir) = setup_test_store().await;
        let keys = Keys::generate();

        // The ingester reuses a FlatBufferBuilder with FLATBUFFER_CAPACITY
        // Let's create events that would benefit from this reuse

        const NUM_BATCHES: usize = 5;
        const EVENTS_PER_BATCH: usize = 100;

        for batch in 0..NUM_BATCHES {
            let mut events = Vec::new();
            for i in 0..EVENTS_PER_BATCH {
                let event = EventBuilder::text_note(format!("Batch {} Event {}", batch, i))
                    .sign_with_keys(&keys)
                    .expect("Failed to sign event");
                events.push(event);
            }

            // Send all events in the batch concurrently
            let futures: Vec<_> = events
                .iter()
                .map(|event| {
                    let store = Arc::clone(&store);
                    let event = event.clone();
                    async move { store.save_event(&event).await }
                })
                .collect();

            let results = join_all(futures).await;

            // Check all succeeded
            for result in results {
                result.expect("Failed to save event");
            }

            // Small delay between batches
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Verify all events were saved
        let saved_count = store.query(Filter::new()).expect("Failed to query").len();
        assert_eq!(saved_count, NUM_BATCHES * EVENTS_PER_BATCH);
    }

    #[tokio::test]
    async fn test_mixed_operations_batch() {
        let (store, _temp_dir) = setup_test_store().await;
        let keys = Keys::generate();

        // Create some events
        let mut events = Vec::new();
        for i in 0..10 {
            let event = EventBuilder::text_note(format!("Event to delete {}", i))
                .sign_with_keys(&keys)
                .expect("Failed to sign event");
            store
                .save_event(&event)
                .await
                .expect("Failed to save event");
            events.push(event);
        }

        // Now create a mixed batch of saves and deletes
        let new_event1 = EventBuilder::text_note("New event 1")
            .sign_with_keys(&keys)
            .expect("Failed to sign event");

        let new_event2 = EventBuilder::text_note("New event 2")
            .sign_with_keys(&keys)
            .expect("Failed to sign event");

        // Execute mixed operations concurrently
        let save_fut1 = store.save_event(&new_event1);
        let save_fut2 = store.save_event(&new_event2);
        let delete_fut = store.delete(Filter::new().kind(Kind::TextNote).limit(5));

        let (save1_result, save2_result, delete_result) =
            tokio::join!(save_fut1, save_fut2, delete_fut);

        save1_result.expect("Failed to save new event 1");
        save2_result.expect("Failed to save new event 2");
        delete_result.expect("Failed to delete events");

        // Verify results
        let remaining = store.query(Filter::new()).expect("Failed to query").len();

        // We had 10 events, deleted 5, added 2
        assert_eq!(remaining, 7);
    }
}
