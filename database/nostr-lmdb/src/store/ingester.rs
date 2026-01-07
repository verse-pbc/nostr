// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2025 Rust Nostr Developers
// Distributed under the MIT software license

//! Event ingester for LMDB storage backend
//!
//! This module implements an asynchronous event ingester that processes database operations
//! in the background using tokio's spawn_blocking for LMDB operations.
//!
//! The ingester provides automatic batching of operations for optimal LMDB write performance.
//! Events are collected from a channel and committed in batches using a single transaction.
//!
//! ## Architecture
//!
//! The ingester runs as a tokio task (not a std::thread) and uses spawn_blocking for the
//! actual LMDB writes. This ensures that oneshot response channels are sent from within
//! the tokio runtime context, avoiding cross-thread wake race conditions that can cause
//! runtime deadlocks.

use heed::RwTxn;
use nostr::{Event, Filter};
use nostr_database::{FlatBufferBuilder, SaveEventStatus};
use scoped_heed::Scope;
use tokio::sync::{mpsc, oneshot};

use super::error::Error;
use super::lmdb::Lmdb;

/// Pre-allocated buffer size for FlatBufferBuilder
///
/// This size is chosen to handle most Nostr events without reallocation.
/// Large events (with many tags or large content) may still trigger reallocation.
const FLATBUFFER_CAPACITY: usize = 70_000;

/// Maximum batch size to process in a single transaction
const MAX_BATCH_SIZE: usize = 1000;

enum OperationResult {
    Reindex {
        result: Result<(), Error>,
        tx: Option<oneshot::Sender<Result<(), Error>>>,
    },
    Save {
        result: Result<SaveEventStatus, Error>,
        tx: Option<oneshot::Sender<Result<SaveEventStatus, Error>>>,
    },
    Delete {
        result: Result<(), Error>,
        tx: Option<oneshot::Sender<Result<(), Error>>>,
    },
    Wipe {
        result: Result<(), Error>,
        tx: Option<oneshot::Sender<Result<(), Error>>>,
    },
}

impl OperationResult {
    /// Send the result through the channel if present, or log errors
    fn send(self) {
        match self {
            Self::Reindex { result, tx } => {
                if let Some(tx) = tx {
                    if tx.send(result).is_err() {
                        tracing::debug!("Failed to send rebuild index result: receiver dropped");
                    }
                } else if let Err(e) = result {
                    tracing::error!(error = %e, "Failed to rebuild index");
                }
            }
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
            Self::Wipe { result, tx } => {
                if let Some(tx) = tx {
                    if tx.send(result).is_err() {
                        tracing::debug!("Failed to send wipe result: receiver dropped");
                    }
                } else if let Err(e) = result {
                    tracing::error!(error = %e, "Wipe operation failed in batch");
                }
            }
        }
    }
}

enum IngesterOperation {
    Reindex {
        tx: Option<oneshot::Sender<Result<(), Error>>>,
    },
    SaveEvent {
        event: Event,
        tx: Option<oneshot::Sender<Result<SaveEventStatus, Error>>>,
    },
    Delete {
        filter: Filter,
        tx: Option<oneshot::Sender<Result<(), Error>>>,
    },
    Wipe {
        tx: Option<oneshot::Sender<Result<(), Error>>>,
    },
    // Scoped operations for multi-tenant support
    RegisterScope {
        scope: Scope,
        tx: Option<oneshot::Sender<Result<(), Error>>>,
    },
    SaveEventScoped {
        event: Event,
        scope: Scope,
        tx: Option<oneshot::Sender<Result<SaveEventStatus, Error>>>,
    },
    DeleteScoped {
        filter: Filter,
        scope: Scope,
        tx: Option<oneshot::Sender<Result<(), Error>>>,
    },
    WipeScoped {
        scope: Scope,
        tx: Option<oneshot::Sender<Result<(), Error>>>,
    },
}

impl IngesterOperation {
    /// Create an error result for this operation type, moving the channel
    fn into_error_result(self, error: Error) -> OperationResult {
        match self {
            Self::Reindex { tx } => OperationResult::Reindex {
                result: Err(error),
                tx,
            },
            Self::SaveEvent { tx, .. } => OperationResult::Save {
                result: Err(error),
                tx,
            },
            Self::Delete { tx, .. } => OperationResult::Delete {
                result: Err(error),
                tx,
            },
            Self::Wipe { tx } => OperationResult::Wipe {
                result: Err(error),
                tx,
            },
            // Scoped operations use the same result types as their unscoped counterparts
            Self::RegisterScope { tx, .. } => OperationResult::Wipe {
                result: Err(error),
                tx,
            },
            Self::SaveEventScoped { tx, .. } => OperationResult::Save {
                result: Err(error),
                tx,
            },
            Self::DeleteScoped { tx, .. } => OperationResult::Delete {
                result: Err(error),
                tx,
            },
            Self::WipeScoped { tx, .. } => OperationResult::Wipe {
                result: Err(error),
                tx,
            },
        }
    }
}

pub(super) struct IngesterItem {
    operation: IngesterOperation,
}

impl IngesterItem {
    #[must_use]
    pub(super) fn reindex() -> (Self, oneshot::Receiver<Result<(), Error>>) {
        let (tx, rx) = oneshot::channel();
        let item: Self = Self {
            operation: IngesterOperation::Reindex { tx: Some(tx) },
        };
        (item, rx)
    }

    #[must_use]
    pub(super) fn save_event_with_feedback(
        event: Event,
    ) -> (Self, oneshot::Receiver<Result<SaveEventStatus, Error>>) {
        let (tx, rx) = oneshot::channel();
        let item: Self = Self {
            operation: IngesterOperation::SaveEvent {
                event,
                tx: Some(tx),
            },
        };
        (item, rx)
    }

    #[must_use]
    pub(super) fn delete_with_feedback(
        filter: Filter,
    ) -> (Self, oneshot::Receiver<Result<(), Error>>) {
        let (tx, rx) = oneshot::channel();
        let item: Self = Self {
            operation: IngesterOperation::Delete {
                filter,
                tx: Some(tx),
            },
        };
        (item, rx)
    }

    #[must_use]
    pub(super) fn wipe_with_feedback() -> (Self, oneshot::Receiver<Result<(), Error>>) {
        let (tx, rx) = oneshot::channel();
        let item: Self = Self {
            operation: IngesterOperation::Wipe { tx: Some(tx) },
        };
        (item, rx)
    }

    // Scoped operations for multi-tenant support

    #[must_use]
    pub(super) fn register_scope_with_feedback(
        scope: Scope,
    ) -> (Self, oneshot::Receiver<Result<(), Error>>) {
        let (tx, rx) = oneshot::channel();
        let item: Self = Self {
            operation: IngesterOperation::RegisterScope {
                scope,
                tx: Some(tx),
            },
        };
        (item, rx)
    }

    #[must_use]
    pub(super) fn save_event_scoped_with_feedback(
        event: Event,
        scope: Scope,
    ) -> (Self, oneshot::Receiver<Result<SaveEventStatus, Error>>) {
        let (tx, rx) = oneshot::channel();
        let item: Self = Self {
            operation: IngesterOperation::SaveEventScoped {
                event,
                scope,
                tx: Some(tx),
            },
        };
        (item, rx)
    }

    #[must_use]
    pub(super) fn delete_scoped_with_feedback(
        filter: Filter,
        scope: Scope,
    ) -> (Self, oneshot::Receiver<Result<(), Error>>) {
        let (tx, rx) = oneshot::channel();
        let item: Self = Self {
            operation: IngesterOperation::DeleteScoped {
                filter,
                scope,
                tx: Some(tx),
            },
        };
        (item, rx)
    }

    #[must_use]
    pub(super) fn wipe_scoped_with_feedback(
        scope: Scope,
    ) -> (Self, oneshot::Receiver<Result<(), Error>>) {
        let (tx, rx) = oneshot::channel();
        let item: Self = Self {
            operation: IngesterOperation::WipeScoped {
                scope,
                tx: Some(tx),
            },
        };
        (item, rx)
    }
}

/// Sender handle for the ingester
///
/// This is a wrapper around mpsc::Sender that provides a synchronous send method
/// for compatibility with the existing API.
#[derive(Debug, Clone)]
pub(super) struct IngesterSender {
    tx: mpsc::UnboundedSender<IngesterItem>,
}

impl IngesterSender {
    /// Send an item to the ingester
    ///
    /// This is non-blocking and will return an error if the ingester has shut down.
    pub(super) fn send(&self, item: IngesterItem) -> Result<(), IngesterSendError> {
        self.tx.send(item).map_err(|_| IngesterSendError)
    }
}

/// Error returned when sending to the ingester fails
#[derive(Debug)]
pub(super) struct IngesterSendError;

impl std::fmt::Display for IngesterSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ingester channel closed")
    }
}

impl std::error::Error for IngesterSendError {}

#[derive(Debug)]
pub(super) struct Ingester {
    db: Lmdb,
    rx: mpsc::UnboundedReceiver<IngesterItem>,
}

impl Ingester {
    /// Build and spawn a new ingester as a tokio task
    ///
    /// The `thread_config` parameter is ignored in this implementation since we use
    /// tokio's spawn_blocking pool instead of a dedicated thread.
    pub(super) fn run(
        db: Lmdb,
        _thread_config: Option<Box<dyn FnOnce() + Send>>,
    ) -> IngesterSender {
        // Create a new unbounded mpsc channel
        let (tx, rx) = mpsc::unbounded_channel();

        // Construct and spawn ingester as a tokio task
        let ingester = Self { db, rx };
        tokio::spawn(ingester.run_ingester_loop());

        IngesterSender { tx }
    }

    async fn run_ingester_loop(mut self) {
        tracing::debug!("Ingester task started");

        loop {
            // Wait for the first item
            let first_item = match self.rx.recv().await {
                Some(item) => item,
                // Channel closed, exit the loop
                None => {
                    tracing::debug!("Ingester channel closed, exiting.");
                    break;
                }
            };

            // Collect more items without blocking (drain pattern)
            let mut batch: Vec<IngesterItem> = Vec::with_capacity(64);
            batch.push(first_item);

            // Try to collect more items that are already queued
            while batch.len() < MAX_BATCH_SIZE {
                match self.rx.try_recv() {
                    Ok(item) => batch.push(item),
                    Err(_) => break,
                }
            }

            let batch_size = batch.len();

            // Process batch in spawn_blocking to avoid blocking the async runtime
            let db = self.db.clone();
            let results = tokio::task::spawn_blocking(move || {
                process_batch_blocking(db, batch)
            })
            .await;

            // Handle spawn_blocking result
            let results = match results {
                Ok(results) => results,
                Err(e) => {
                    tracing::error!(error = %e, "spawn_blocking panicked in ingester");
                    continue;
                }
            };

            tracing::debug!("Processed batch of {} operations", batch_size);

            // Send results back through oneshot channels
            // IMPORTANT: This happens from within the tokio task, not from a std::thread,
            // which avoids the cross-thread wake race condition that causes deadlocks.
            for result in results {
                result.send();
            }
        }

        tracing::debug!("Ingester task exited");
    }
}

/// Process a batch of operations in a blocking context
///
/// This function runs in spawn_blocking and performs the actual LMDB writes.
fn process_batch_blocking(db: Lmdb, batch: Vec<IngesterItem>) -> Vec<OperationResult> {
    let mut fbb = FlatBufferBuilder::with_capacity(FLATBUFFER_CAPACITY);
    let mut results = Vec::with_capacity(batch.len());

    process_batch_in_transaction(&db, batch.into_iter(), &mut fbb, &mut results);

    results
}

fn process_batch_in_transaction<I>(
    db: &Lmdb,
    batch: I,
    fbb: &mut FlatBufferBuilder,
    results: &mut Vec<OperationResult>,
) where
    I: Iterator<Item = IngesterItem>,
{
    // Note: We're only using a write transaction here since LMDB doesn't require
    // a separate read transaction for queries within a write transaction
    let mut write_txn = match db.write_txn() {
        Ok(txn) => txn,
        Err(e) => {
            tracing::error!(error = %e, "Failed to create write transaction");

            // Send error for all items
            for item in batch {
                results.push(
                    item.operation
                        .into_error_result(Error::BatchTransactionFailed),
                );
            }

            return;
        }
    };

    let mut batch_iter = batch.peekable();

    // Process all operations in the batch
    while let Some(item) = batch_iter.next() {
        let result = process_operation(db, &mut write_txn, item, fbb);

        // Check if we need to abort on actual database errors (not on rejections)
        let abort_on_error: bool = match &result {
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

        // Add operation to results
        results.push(result);

        if abort_on_error {
            // Mark all previous operations as failed
            mark_all_as_failed(results);

            // All remaining operations get BatchTransactionFailed error
            for item in batch_iter {
                results.push(
                    item.operation
                        .into_error_result(Error::BatchTransactionFailed),
                );
            }

            // Abort the write transaction
            write_txn.abort();
            return;
        }
    }

    // All operations succeeded, commit the transaction
    if let Err(e) = write_txn.commit() {
        tracing::error!(error = %e, "Failed to commit batch transaction");

        // Mark all operations as failed
        mark_all_as_failed(results);
    }
}

fn process_operation(
    db: &Lmdb,
    txn: &mut RwTxn,
    item: IngesterItem,
    fbb: &mut FlatBufferBuilder,
) -> OperationResult {
    match item.operation {
        IngesterOperation::Reindex { tx } => OperationResult::Reindex {
            result: db.reindex(txn),
            tx,
        },
        IngesterOperation::SaveEvent { event, tx } => {
            let result = db.save_event_with_txn(txn, fbb, &event);
            OperationResult::Save { result, tx }
        }
        IngesterOperation::Delete { filter, tx } => {
            let result = db.delete(txn, filter);
            OperationResult::Delete { result, tx }
        }
        IngesterOperation::Wipe { tx } => {
            let result = db.wipe(txn);
            OperationResult::Wipe { result, tx }
        }
        // Scoped operations for multi-tenant support
        IngesterOperation::RegisterScope { scope, tx } => {
            let result = db.register_scope(txn, &scope);
            OperationResult::Wipe { result, tx }
        }
        IngesterOperation::SaveEventScoped { event, scope, tx } => {
            let result = db.save_event_with_txn_scoped(txn, &scope, fbb, &event);
            OperationResult::Save { result, tx }
        }
        IngesterOperation::DeleteScoped { filter, scope, tx } => {
            let result = db.delete_scoped(txn, &scope, filter);
            OperationResult::Delete { result, tx }
        }
        IngesterOperation::WipeScoped { scope, tx } => {
            let result = db.wipe_scoped(txn, &scope);
            OperationResult::Wipe { result, tx }
        }
    }
}

/// Mark all operations as failed
fn mark_all_as_failed(results: &mut [OperationResult]) {
    // All previously processed operations get BatchTransactionFailed error
    for prev_result in results.iter_mut() {
        match prev_result {
            OperationResult::Reindex { result: res, .. } => {
                *res = Err(Error::BatchTransactionFailed)
            }
            OperationResult::Save { result: res, .. } => *res = Err(Error::BatchTransactionFailed),
            OperationResult::Delete { result: res, .. } => {
                *res = Err(Error::BatchTransactionFailed)
            }
            OperationResult::Wipe { result: res, .. } => *res = Err(Error::BatchTransactionFailed),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::future::join_all;
    use nostr::{EventBuilder, Keys, Kind};
    use tempfile::TempDir;

    use super::*;
    use crate::store::Store;

    async fn setup_test_store() -> (Arc<Store>, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let store = Store::open(temp_dir.path(), 1024 * 1024 * 10, 10, 50, None)
            .await
            .expect("Failed to open test store");
        (Arc::new(store), temp_dir)
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
        let saved_count = store
            .query(Filter::new())
            .await
            .expect("Failed to query")
            .len();
        assert_eq!(saved_count, 3); // event1, event2, event3
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
        let remaining = store
            .query(Filter::new())
            .await
            .expect("Failed to query")
            .len();

        // We had 10 events, deleted 5, added 2
        assert_eq!(remaining, 7);
    }
}
