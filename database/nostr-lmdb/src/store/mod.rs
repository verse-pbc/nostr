// Copyright (c) 2024 Michael Dilger
// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2025 Rust Nostr Developers
// Distributed under the MIT software license

use std::fs;
use std::path::Path;

use flume::Sender;
use nostr_database::prelude::*;
use nostr_database::{FlatBufferBuilder, SaveEventStatus};

mod error;
mod ingester;
mod lmdb;
mod types;

pub use self::error::Error;
use self::ingester::{Ingester, IngesterItem};
use self::lmdb::Lmdb;

#[derive(Debug)]
pub struct Store {
    db: Lmdb,
    ingester: Sender<IngesterItem>,
}

impl Clone for Store {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            ingester: self.ingester.clone(),
        }
    }
}

impl Store {
    pub(super) fn open<P>(
        path: P,
        map_size: usize,
        max_readers: u32,
        max_dbs: u32,
    ) -> Result<Store, Error>
    where
        P: AsRef<Path>,
    {
        let path: &Path = path.as_ref();

        // Create the directory if it doesn't exist
        fs::create_dir_all(path)?;

        let db: Lmdb = Lmdb::new(path, map_size, max_readers, max_dbs)?;
        let ingester = Ingester::run(db.clone());

        Ok(Self { db, ingester })
    }

    /// Store an event.
    pub async fn save_event(&self, event: &Event) -> Result<SaveEventStatus, Error> {
        let (item, rx) = IngesterItem::save_event_with_feedback(event.clone());

        // Send to the ingester
        self.ingester.send(item).map_err(|_| Error::MpscSend)?;

        // Wait for a reply
        rx.await?
    }

    /// Get an event by ID
    pub fn get_event_by_id(&self, id: &EventId) -> Result<Option<Event>, Error> {
        let txn = self.db.read_txn()?;
        let result = self
            .db
            .get_event_by_id(&txn, id.as_bytes())?
            .map(|event_borrow| event_borrow.into_owned());
        txn.commit()?;
        Ok(result)
    }

    /// Do we have an event
    pub fn has_event(&self, id: &EventId) -> Result<bool, Error> {
        let txn = self.db.read_txn()?;
        let has: bool = self.db.has_event(&txn, id.as_bytes())?;
        txn.commit()?;
        Ok(has)
    }

    /// Is the event deleted
    pub fn event_is_deleted(&self, id: &EventId) -> Result<bool, Error> {
        let txn = self.db.read_txn()?;
        let deleted: bool = self.db.is_deleted(&txn, id)?;
        txn.commit()?;
        Ok(deleted)
    }

    pub fn count(&self, filter: Filter) -> Result<usize, Error> {
        let txn = self.db.read_txn()?;
        let iter = self.db.query(&txn, filter)?;
        let count = iter.count();
        txn.commit()?;
        Ok(count)
    }

    // Lookup ID: EVENT_ORD_IMPL
    pub fn query(&self, filter: Filter) -> Result<Events, Error> {
        let txn = self.db.read_txn()?;
        let vec_events: Vec<Event> = self.db.query(&txn, filter.clone())?
            .map(|e| e.into_owned())
            .collect();
        let mut events_wrapper = Events::new(&filter);
        events_wrapper.extend(vec_events);
        txn.commit()?;
        Ok(events_wrapper)
    }

    pub fn negentropy_items(&self, filter: Filter) -> Result<Vec<(EventId, Timestamp)>, Error> {
        let txn = self.db.read_txn()?;
        let vec_events: Vec<Event> = self.db.query(&txn, filter)?
            .map(|e| e.into_owned())
            .collect();
        let items = vec_events
            .into_iter()
            .map(|e: Event| (e.id, e.created_at))
            .collect();
        txn.commit()?;
        Ok(items)
    }

    pub async fn delete(&self, filter: Filter) -> Result<(), Error> {
        let (item, rx) = IngesterItem::delete_with_feedback(filter);

        // Send to the ingester
        self.ingester.send(item).map_err(|_| Error::MpscSend)?;

        // Wait for a reply
        rx.await?
    }

    pub fn wipe(&self) -> Result<(), Error> {
        let mut txn = self.db.write_txn()?;
        self.db.wipe(&mut txn)?;
        txn.commit()?;

        Ok(())
    }

    // Transaction management methods

    /// Create a new read transaction
    pub fn read_transaction(&self) -> Result<heed::RoTxn<'_, heed::WithTls>, Error> {
        self.db.read_txn().map_err(Error::from)
    }

    /// Create a new write transaction
    pub fn write_transaction(&self) -> Result<heed::RwTxn<'_>, Error> {
        self.db.write_txn()
    }

    /// Save an event within a transaction
    pub fn save_event_with_txn(
        &self,
        txn: &mut heed::RwTxn,
        fbb: &mut FlatBufferBuilder,
        event: &Event,
    ) -> Result<SaveEventStatus, Error> {
        self.db.save_event_with_txn(txn, fbb, event)
    }

    /// Delete events matching a filter within a transaction
    pub fn delete_with_txn(&self, txn: &mut heed::RwTxn, filter: Filter) -> Result<usize, Error> {
        // Query events to delete
        let events: Vec<Event> = self.db.query(txn, filter)?
            .map(|e| e.into_owned())
            .collect();
        let count = events.len();

        // Delete each event
        for event in events {
            self.db.remove_event(txn, &event.id)?;
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nostr::{EventBuilder, Filter, Keys, Kind};
    use nostr_database::{FlatBufferBuilder, SaveEventStatus};
    use tempfile::TempDir;

    use super::*;
    use crate::store::ingester::FLATBUFFER_CAPACITY;

    fn setup_test_store() -> (Store, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let store = Store::open(temp_dir.path(), 1024 * 1024 * 10, 10, 10)
            .expect("Failed to open test store");
        (store, temp_dir)
    }

    #[tokio::test]
    async fn test_save_event_with_txn() {
        let (store, _temp_dir) = setup_test_store();
        let keys = Keys::generate();
        let mut fbb = FlatBufferBuilder::with_capacity(FLATBUFFER_CAPACITY);

        // Create a transaction
        let mut txn = store
            .write_transaction()
            .expect("Failed to create write transaction");

        // Save multiple events within the transaction
        let mut event_ids = Vec::new();
        for i in 0..5 {
            let event = EventBuilder::text_note(format!("Transaction event {}", i))
                .sign_with_keys(&keys)
                .expect("Failed to sign event");
            event_ids.push(event.id);

            let status = store
                .save_event_with_txn(&mut txn, &mut fbb, &event)
                .expect("Failed to save event in transaction");

            assert!(matches!(status, SaveEventStatus::Success));
        }

        // Events should not be visible before commit
        let events_before = store.query(Filter::new()).expect("Failed to query");
        assert_eq!(events_before.len(), 0);

        // Commit the transaction
        txn.commit().expect("Failed to commit transaction");

        // Events should now be visible
        let events_after = store.query(Filter::new()).expect("Failed to query");
        assert_eq!(events_after.len(), 5);

        // Verify event IDs match
        let saved_ids: Vec<_> = events_after.iter().map(|e| e.id).collect();
        for id in event_ids {
            assert!(saved_ids.contains(&id));
        }
    }

    #[tokio::test]
    async fn test_delete_with_txn() {
        let (store, _temp_dir) = setup_test_store();
        let keys = Keys::generate();

        // First, save some events
        for i in 0..10 {
            let event = EventBuilder::text_note(format!("Event {}", i))
                .sign_with_keys(&keys)
                .expect("Failed to sign event");
            store
                .save_event(&event)
                .await
                .expect("Failed to save event");
        }

        // Verify all events are saved
        let count_before = store.count(Filter::new()).expect("Failed to count");
        assert_eq!(count_before, 10);

        // Create a transaction to delete some events
        let mut txn = store
            .write_transaction()
            .expect("Failed to create write transaction");

        // Delete text notes with limit
        let delete_filter = Filter::new().kind(Kind::TextNote).limit(5);
        let deleted_count = store
            .delete_with_txn(&mut txn, delete_filter)
            .expect("Failed to delete in transaction");

        assert_eq!(deleted_count, 5);

        // Events should still be visible before commit
        let count_during = store.count(Filter::new()).expect("Failed to count");
        assert_eq!(count_during, 10);

        // Commit the transaction
        txn.commit().expect("Failed to commit transaction");

        // Now events should be deleted
        let count_after = store.count(Filter::new()).expect("Failed to count");
        assert_eq!(count_after, 5);
    }

    #[tokio::test]
    async fn test_transaction_with_flatbuffer_reuse() {
        let (store, _temp_dir) = setup_test_store();
        let keys = Keys::generate();

        // Create a single FlatBufferBuilder to reuse
        let mut fbb = FlatBufferBuilder::with_capacity(FLATBUFFER_CAPACITY);

        // Test multiple transactions reusing the same FlatBufferBuilder
        for batch in 0..3 {
            let mut txn = store
                .write_transaction()
                .expect("Failed to create write transaction");

            // Save multiple events in this transaction
            for i in 0..5 {
                let event = EventBuilder::text_note(format!("Batch {} Event {}", batch, i))
                    .sign_with_keys(&keys)
                    .expect("Failed to sign event");

                let status = store
                    .save_event_with_txn(&mut txn, &mut fbb, &event)
                    .expect("Failed to save event");

                assert!(matches!(status, SaveEventStatus::Success));
            }

            txn.commit().expect("Failed to commit transaction");
        }

        // Verify all events were saved
        let total_events = store.count(Filter::new()).expect("Failed to count");
        assert_eq!(total_events, 15); // 3 batches * 5 events
    }

    #[tokio::test]
    async fn test_transaction_abort_rollback() {
        let (store, _temp_dir) = setup_test_store();
        let keys = Keys::generate();
        let mut fbb = FlatBufferBuilder::with_capacity(FLATBUFFER_CAPACITY);

        // Create a transaction
        let mut txn = store
            .write_transaction()
            .expect("Failed to create write transaction");

        // Save some events
        for i in 0..5 {
            let event = EventBuilder::text_note(format!("Aborted event {}", i))
                .sign_with_keys(&keys)
                .expect("Failed to sign event");

            store
                .save_event_with_txn(&mut txn, &mut fbb, &event)
                .expect("Failed to save event");
        }

        // Abort the transaction instead of committing
        txn.abort();

        // No events should be saved
        let count = store.count(Filter::new()).expect("Failed to count");
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_mixed_transaction_operations() {
        let (store, _temp_dir) = setup_test_store();
        let keys = Keys::generate();
        let mut fbb = FlatBufferBuilder::with_capacity(FLATBUFFER_CAPACITY);

        // First, save some events to delete later
        for i in 0..10 {
            let event = EventBuilder::text_note(format!("Initial event {}", i))
                .sign_with_keys(&keys)
                .expect("Failed to sign event");
            store
                .save_event(&event)
                .await
                .expect("Failed to save event");
        }

        // Create a transaction with mixed operations
        let mut txn = store
            .write_transaction()
            .expect("Failed to create write transaction");

        // Delete some events
        let delete_filter = Filter::new().kind(Kind::TextNote).limit(5);
        let deleted_count = store
            .delete_with_txn(&mut txn, delete_filter)
            .expect("Failed to delete");
        assert_eq!(deleted_count, 5);

        // Add new events in the same transaction
        for i in 0..3 {
            let event = EventBuilder::text_note(format!("New event {}", i))
                .sign_with_keys(&keys)
                .expect("Failed to sign event");

            store
                .save_event_with_txn(&mut txn, &mut fbb, &event)
                .expect("Failed to save event");
        }

        // Commit the transaction
        txn.commit().expect("Failed to commit transaction");

        // Verify final state: 10 - 5 + 3 = 8 events
        let final_count = store.count(Filter::new()).expect("Failed to count");
        assert_eq!(final_count, 8);
    }

    #[tokio::test]
    async fn test_transaction_commit_consume() {
        let (store, _temp_dir) = setup_test_store();
        let keys = Keys::generate();
        let mut fbb = FlatBufferBuilder::with_capacity(FLATBUFFER_CAPACITY);

        let event = EventBuilder::text_note("Test event")
            .sign_with_keys(&keys)
            .expect("Failed to sign event");

        let mut txn = store
            .write_transaction()
            .expect("Failed to create write transaction");

        // Save an event
        store
            .save_event_with_txn(&mut txn, &mut fbb, &event)
            .expect("Failed to save event");

        // Commit the transaction - this consumes txn
        txn.commit().expect("Failed to commit");

        // Transaction has been consumed by commit, so we can't use it anymore
        // This is enforced at compile time by Rust's ownership system

        // Create a new transaction to verify the event was saved
        let mut txn2 = store
            .write_transaction()
            .expect("Failed to create second transaction");

        // Try to save a duplicate - should be rejected
        let result = store.save_event_with_txn(&mut txn2, &mut fbb, &event);
        assert!(matches!(
            result,
            Ok(SaveEventStatus::Rejected(
                nostr_database::RejectedReason::Duplicate
            ))
        ));

        txn2.commit().expect("Failed to commit second transaction");
    }

    #[tokio::test]
    async fn test_concurrent_transactions_and_ingester() {
        let (store, _temp_dir) = setup_test_store();
        let store = Arc::new(store);
        let keys = Keys::generate();

        // Start some async saves through the ingester
        let mut async_futures = Vec::new();
        for i in 0..5 {
            let store_clone = Arc::clone(&store);
            let event = EventBuilder::text_note(format!("Async event {}", i))
                .sign_with_keys(&keys)
                .expect("Failed to sign event");

            async_futures.push(async move { store_clone.save_event(&event).await });
        }

        // Meanwhile, use a transaction
        let store_clone = Arc::clone(&store);
        let txn_handle = tokio::task::spawn_blocking(move || {
            let mut fbb = FlatBufferBuilder::with_capacity(FLATBUFFER_CAPACITY);
            let mut txn = store_clone
                .write_transaction()
                .expect("Failed to create write transaction");

            for i in 0..5 {
                let event = EventBuilder::text_note(format!("Transaction event {}", i))
                    .sign_with_keys(&keys)
                    .expect("Failed to sign event");

                store_clone
                    .save_event_with_txn(&mut txn, &mut fbb, &event)
                    .expect("Failed to save event");
            }

            txn.commit().expect("Failed to commit");
        });

        // Wait for everything to complete
        for fut in async_futures {
            fut.await.expect("Failed to save async event");
        }
        txn_handle.await.expect("Transaction thread panicked");

        // Should have all 10 events
        let total_count = store.count(Filter::new()).expect("Failed to count");
        assert_eq!(total_count, 10);
    }
}
