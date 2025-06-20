// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2025 Rust Nostr Developers
// Distributed under the MIT software license

//! # Event Ingester with Automatic Batching
//!
//! The ingester provides asynchronous event processing through a dedicated background thread
//! with automatic batching for optimal performance.
//!
//! ## Automatic Batching
//!
//! The ingester automatically batches ALL available events in the channel. This provides:
//! - Optimal write performance without manual batching
//! - Reduced LMDB transaction overhead  
//! - Transparent to the caller - just use `save_event()` as normal
//! - No artificial limits - processes all available events in a single transaction
//!
//! ## Ingester-based vs Transaction-based Operations
//!
//! ### Ingester-based (Async with Auto-batching):
//! - `save_event()` - Async processing with automatic batching
//! - `save_event_in_scope()` - Scoped async processing with automatic batching
//! - Events are collected and committed in batches automatically
//! - Non-blocking for caller with optional feedback
//!
//! ### Transaction-based (Direct Control):
//! - `save_event_txn()` - Single event within your transaction
//! - Direct database access, bypasses ingester
//! - Full control over transaction boundaries
//! - Use when you need precise transaction control or atomic operations
//!
//! ## When to Use Each Approach
//!
//! - **Ingester (default)**: For most use cases, especially high-throughput scenarios
//! - **Transactions**: When you need atomic operations across multiple changes

use flume::{Receiver, Sender};

use nostr::Event;
use nostr_database::{FlatBufferBuilder, SaveEventStatus};
use scoped_heed::Scope;
use tokio::sync::oneshot;

use super::error::Error;
use super::lmdb::Lmdb;
use super::Filter;

enum OperationResult {
    Save(Result<SaveEventStatus, Error>),
    Delete(Result<(), Error>),
}


pub(super) enum IngesterOperation {
    SaveEvent {
        event: Event,
        tx: Option<oneshot::Sender<Result<SaveEventStatus, Error>>>,
    },
    Delete {
        filter: Filter,
        tx: Option<oneshot::Sender<Result<(), Error>>>,
    },
}

pub(super) struct IngesterItem {
    pub(super) scope: Scope,
    pub(super) operation: IngesterOperation,
}

impl IngesterItem {
    #[must_use]
    pub(super) fn save_event_with_feedback(
        event: Event,
        scope: Scope,
    ) -> (Self, oneshot::Receiver<Result<SaveEventStatus, Error>>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                scope,
                operation: IngesterOperation::SaveEvent {
                    event,
                    tx: Some(tx),
                },
            },
            rx,
        )
    }

    #[must_use]
    pub(super) fn delete_with_feedback(
        filter: Filter,
        scope: Scope,
    ) -> (Self, oneshot::Receiver<Result<(), Error>>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                scope,
                operation: IngesterOperation::Delete {
                    filter,
                    tx: Some(tx),
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
        txn: &mut heed::RwTxn,
        scope: &Scope,
        operation: &IngesterOperation,
        fbb: &mut FlatBufferBuilder,
    ) -> OperationResult {
        match operation {
            IngesterOperation::SaveEvent { event, .. } => {
                let result = self.db.save_event_with_txn(txn, scope, fbb, event);
                OperationResult::Save(result)
            }
            IngesterOperation::Delete { filter, .. } => {
                let result = self.db.delete_in_scope(txn, scope, filter.clone()).map(|_| ());
                OperationResult::Delete(result)
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

        // Return ingester sender
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

        let mut fbb = FlatBufferBuilder::with_capacity(70_000);
        
        // Process events as they arrive
        while let Ok(first_item) = self.rx.recv() {
            let mut batch = vec![first_item];
            // Drain all currently available items
            batch.extend(self.rx.drain());
            
            #[cfg(debug_assertions)]
            let batch_count = batch.len();
            
            // Process the batch in a transaction
            let results = self.process_batch_in_transaction(batch, &mut fbb);
            
            #[cfg(debug_assertions)]
            tracing::debug!("Processed batch of {} operations", batch_count);
            
            // Send results back to callers
            self.send_batch_results(results);
        }

        #[cfg(debug_assertions)]
        tracing::debug!("Ingester thread exited");
    }
    
    fn process_batch_in_transaction(
        &self,
        batch: Vec<IngesterItem>,
        fbb: &mut FlatBufferBuilder,
    ) -> Vec<(IngesterItem, OperationResult)> {
        // Group by scope for transaction efficiency
        let mut by_scope: std::collections::HashMap<Scope, Vec<IngesterItem>> = std::collections::HashMap::new();
        for item in batch {
            by_scope.entry(item.scope.clone()).or_default().push(item);
        }
        
        let mut all_results = Vec::new();
        
        // Process each scope in its own transaction
        for (scope, items) in by_scope {
            match self.db.write_txn() {
                Ok(mut txn) => {
                    let mut results = Vec::new();
                    let mut has_error = false;
                    
                    // Process all items for this scope
                    for item in items {
                        let result = self.process_operation(&mut txn, &scope, &item.operation, fbb);
                        if matches!(&result, OperationResult::Save(Err(_)) | OperationResult::Delete(Err(_))) {
                            has_error = true;
                        }
                        results.push((item, result));
                    }
                    
                    // Commit or abort based on errors
                    if has_error {
                        txn.abort();
                        tracing::warn!("Transaction aborted due to errors for scope: {:?}", scope);
                    } else {
                        match txn.commit() {
                            Ok(()) => {
                                #[cfg(debug_assertions)]
                                tracing::debug!("Transaction committed successfully for scope: {:?}", scope);
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "Failed to commit transaction");
                                // Convert all results to commit error
                                let error = Error::from(e);
                                for (_, result) in &mut results {
                                    match result {
                                        OperationResult::Save(res) => *res = Err(error.clone()),
                                        OperationResult::Delete(res) => *res = Err(error.clone()),
                                    }
                                }
                            }
                        }
                    }
                    
                    all_results.extend(results);
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to create write transaction");
                    // Send error for all items in this scope
                    for item in items {
                        let error_result = match &item.operation {
                            IngesterOperation::SaveEvent { .. } => OperationResult::Save(Err(e.clone())),
                            IngesterOperation::Delete { .. } => OperationResult::Delete(Err(e.clone())),
                        };
                        all_results.push((item, error_result));
                    }
                }
            }
        }
        
        all_results
    }
    
    fn send_batch_results(&self, results: Vec<(IngesterItem, OperationResult)>) {
        for (item, result) in results {
            match (item.operation, result) {
                (IngesterOperation::SaveEvent { tx, .. }, OperationResult::Save(res)) => {
                    if let Some(tx) = tx {
                        let _ = tx.send(res);
                    } else if let Err(e) = res {
                        tracing::error!(error = %e, "Event save failed in batch");
                    }
                }
                (IngesterOperation::Delete { tx, .. }, OperationResult::Delete(res)) => {
                    if let Some(tx) = tx {
                        let _ = tx.send(res);
                    } else if let Err(e) = res {
                        tracing::error!(error = %e, "Delete operation failed in batch");
                    }
                }
                _ => unreachable!("Mismatched operation and result types"),
            }
        }
    }
}
