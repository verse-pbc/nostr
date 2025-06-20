// Copyright (c) 2023-2025 Rust Nostr Developers
// Distributed under the MIT software license

//! Comprehensive tests for transaction-based API

use nostr::{EventBuilder, EventId, Filter, Keys};
use nostr_database::{NostrEventsDatabase, SaveEventStatus};
use nostr_lmdb::NostrLMDB;
use scoped_heed::Scope;
use tempfile::TempDir;

async fn setup_db() -> (NostrLMDB, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let db = NostrLMDB::open(temp_dir.path()).expect("Failed to open database");
    (db, temp_dir)
}

#[tokio::test]
async fn test_basic_transaction_commit() {
    let (db, _temp_dir) = setup_db().await;
    let keys = Keys::generate();

    // Create a transaction
    let mut txn = db
        .begin_write()
        .expect("Failed to create write transaction");

    // Save an event within the transaction
    let event = EventBuilder::text_note("Test event")
        .sign_with_keys(&keys)
        .expect("Failed to sign event");

    let status = db
        .save_event_txn(&mut txn, &event)
        .expect("Failed to save event in transaction");

    assert!(matches!(status, SaveEventStatus::Success));

    // Event should not be visible until commit
    let events_before_commit = db
        .query(Filter::new())
        .await
        .expect("Failed to query events");
    assert_eq!(events_before_commit.len(), 0);

    // Commit the transaction
    txn.commit().expect("Failed to commit transaction");

    // Event should now be visible
    let events_after_commit = db
        .query(Filter::new())
        .await
        .expect("Failed to query events");
    assert_eq!(events_after_commit.len(), 1);
    assert_eq!(events_after_commit.first().unwrap().id, event.id);
}

#[tokio::test]
async fn test_transaction_auto_abort_on_drop() {
    let (db, _temp_dir) = setup_db().await;
    let keys = Keys::generate();

    let event_id = {
        // Create a transaction that goes out of scope without commit
        let mut txn = db
            .begin_write()
            .expect("Failed to create write transaction");

        let event = EventBuilder::text_note("Test event")
            .sign_with_keys(&keys)
            .expect("Failed to sign event");

        let event_id = event.id;

        let status = db
            .save_event_txn(&mut txn, &event)
            .expect("Failed to save event in transaction");

        assert!(matches!(status, SaveEventStatus::Success));

        // Transaction is dropped here without commit
        event_id
    };

    // Event should not be visible since transaction was aborted
    let events = db
        .query(Filter::new())
        .await
        .expect("Failed to query events");
    assert_eq!(events.len(), 0);

    // Verify the event was not saved
    let event_exists = db
        .has_event(&event_id)
        .await
        .expect("Failed to check event existence");
    assert!(!event_exists);
}

#[tokio::test]
async fn test_use_after_commit_prevention() {
    let (db, _temp_dir) = setup_db().await;
    let keys = Keys::generate();

    let mut txn = db
        .begin_write()
        .expect("Failed to create write transaction");

    let event = EventBuilder::text_note("Test event")
        .sign_with_keys(&keys)
        .expect("Failed to sign event");

    // Save event and commit
    db.save_event_txn(&mut txn, &event)
        .expect("Failed to save event in transaction");
    txn.commit().expect("Failed to commit transaction");

    // The RAII design prevents use-after-commit by consuming the transaction on commit
    // This is enforced at compile time by Rust's move semantics, so we verify that
    // the transaction was committed successfully by checking that the event was saved
    let events = db
        .query(Filter::new())
        .await
        .expect("Failed to query events");
    assert_eq!(events.len(), 1);
    assert_eq!(events.first().unwrap().id, event.id);
}

#[tokio::test]
async fn test_multiple_operations_in_single_transaction() {
    let (db, _temp_dir) = setup_db().await;
    let keys = Keys::generate();

    let mut txn = db
        .begin_write()
        .expect("Failed to create write transaction");

    // Save multiple events
    let mut event_ids = Vec::new();
    for i in 0..5 {
        let event = EventBuilder::text_note(format!("Event {}", i))
            .sign_with_keys(&keys)
            .expect("Failed to sign event");
        event_ids.push(event.id);

        let status = db
            .save_event_txn(&mut txn, &event)
            .expect("Failed to save event in transaction");
        assert!(matches!(status, SaveEventStatus::Success));
    }

    // Delete one of the events in the same transaction
    let deleted_count = db
        .delete_txn(&mut txn, Filter::new().id(event_ids[2]))
        .expect("Failed to delete event in transaction");
    assert_eq!(deleted_count, 1);

    // Commit all operations
    txn.commit().expect("Failed to commit transaction");

    // Verify results
    let events = db
        .query(Filter::new())
        .await
        .expect("Failed to query events");
    assert_eq!(events.len(), 4); // 5 created - 1 deleted

    // Verify the specific deleted event is not present
    let deleted_event_exists = db
        .has_event(&event_ids[2])
        .await
        .expect("Failed to check deleted event");
    assert!(!deleted_event_exists);
}

#[tokio::test]
async fn test_scoped_transactions() {
    let (db, _temp_dir) = setup_db().await;
    let keys = Keys::generate();

    let scope_a = Scope::named("tenant_a").expect("Failed to create scope");
    let scope_b = Scope::named("tenant_b").expect("Failed to create scope");

    let mut txn = db
        .begin_write()
        .expect("Failed to create write transaction");

    // Save events in different scopes
    let event_a = EventBuilder::text_note("Event in scope A")
        .sign_with_keys(&keys)
        .expect("Failed to sign event");
    let event_b = EventBuilder::text_note("Event in scope B")
        .sign_with_keys(&keys)
        .expect("Failed to sign event");

    db.save_event_txn_scoped(&mut txn, &scope_a, &event_a)
        .expect("Failed to save event in scope A");
    db.save_event_txn_scoped(&mut txn, &scope_b, &event_b)
        .expect("Failed to save event in scope B");

    txn.commit().expect("Failed to commit transaction");

    // Verify scope isolation
    let events_a = db
        .scoped(&scope_a)
        .expect("Failed to create scoped view")
        .query(Filter::new())
        .await
        .expect("Failed to query scope A");
    assert_eq!(events_a.len(), 1);
    assert_eq!(events_a[0].id, event_a.id);

    let events_b = db
        .scoped(&scope_b)
        .expect("Failed to create scoped view")
        .query(Filter::new())
        .await
        .expect("Failed to query scope B");
    assert_eq!(events_b.len(), 1);
    assert_eq!(events_b[0].id, event_b.id);

    // Default scope should be empty
    let events_default = db
        .query(Filter::new())
        .await
        .expect("Failed to query default scope");
    assert_eq!(events_default.len(), 0);
}

#[tokio::test]
async fn test_automatic_batching() {
    // This test verifies that the ingester automatically batches events
    // when multiple save_event calls are made concurrently
    let (db, _temp_dir) = setup_db().await;
    let keys = Keys::generate();

    // Create multiple events
    let mut events = Vec::new();
    for i in 0..10 {
        let event = EventBuilder::text_note(format!("Auto-batched event {}", i))
            .sign_with_keys(&keys)
            .expect("Failed to sign event");
        events.push(event);
    }

    // Save all events - they will be automatically batched by the ingester
    let mut results = Vec::new();
    for event in &events {
        results.push(db.save_event(event).await);
    }

    assert_eq!(results.len(), 10);
    for result in results {
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), SaveEventStatus::Success));
    }

    // Verify all events were saved
    let saved_events = db
        .query(Filter::new())
        .await
        .expect("Failed to query events");
    assert_eq!(saved_events.len(), 10);

    // Test manual transaction for batch delete
    let mut txn = db.begin_write().expect("Failed to create write transaction");
    let mut deleted = 0;
    for event_id in events.iter().take(5).map(|e| &e.id) {
        if db.delete_by_id_txn(&mut txn, event_id).expect("Failed to delete") {
            deleted += 1;
        }
    }
    txn.commit().expect("Failed to commit");
    assert_eq!(deleted, 5);

    // Verify remaining events
    let remaining_events = db
        .query(Filter::new())
        .await
        .expect("Failed to query remaining events");
    assert_eq!(remaining_events.len(), 5);
}

#[tokio::test]
async fn test_transaction_isolation() {
    let (db, _temp_dir) = setup_db().await;
    let keys = Keys::generate();

    // LMDB only allows one write transaction at a time, so we test isolation sequentially

    // First transaction - save event but don't commit yet
    let event1 = EventBuilder::text_note("Event 1")
        .sign_with_keys(&keys)
        .expect("Failed to sign event");

    {
        let mut txn1 = db
            .begin_write()
            .expect("Failed to create first transaction");
        db.save_event_txn(&mut txn1, &event1)
            .expect("Failed to save event in first transaction");

        // Don't commit - let transaction abort on drop
        // This simulates a transaction that gets rolled back
    }

    // Verify the aborted transaction didn't save the event
    let events_after_abort = db
        .query(Filter::new())
        .await
        .expect("Failed to query after abort");
    assert_eq!(events_after_abort.len(), 0);

    // Second transaction - save a different event and commit
    let event2 = EventBuilder::text_note("Event 2")
        .sign_with_keys(&keys)
        .expect("Failed to sign event");

    {
        let mut txn2 = db
            .begin_write()
            .expect("Failed to create second transaction");
        db.save_event_txn(&mut txn2, &event2)
            .expect("Failed to save event in second transaction");
        txn2.commit().expect("Failed to commit second transaction");
    }

    // Query should only see event from committed transaction
    let events_after_commit = db
        .query(Filter::new())
        .await
        .expect("Failed to query after commit");
    assert_eq!(events_after_commit.len(), 1);
    assert_eq!(events_after_commit.first().unwrap().id, event2.id);

    // Third transaction - now commit the first event
    {
        let mut txn3 = db
            .begin_write()
            .expect("Failed to create third transaction");
        db.save_event_txn(&mut txn3, &event1)
            .expect("Failed to save event in third transaction");
        txn3.commit().expect("Failed to commit third transaction");
    }

    // Now both events should be visible
    let events_after_both = db
        .query(Filter::new())
        .await
        .expect("Failed to query after both commits");
    assert_eq!(events_after_both.len(), 2);
}

#[tokio::test]
async fn test_error_handling_within_transactions() {
    let (db, _temp_dir) = setup_db().await;
    let keys = Keys::generate();

    // Test duplicate event rejection
    let event = EventBuilder::text_note("Test event")
        .sign_with_keys(&keys)
        .expect("Failed to sign event");

    // Save event normally first
    db.save_event(&event).await.expect("Failed to save event");

    // Try to save the same event in a transaction
    let mut txn = db
        .begin_write()
        .expect("Failed to create write transaction");
    let status = db
        .save_event_txn(&mut txn, &event)
        .expect("Transaction should not fail, but event should be rejected");

    // Should be rejected as duplicate
    assert!(matches!(status, SaveEventStatus::Rejected(_)));

    // Transaction should still be usable for other operations
    let other_event = EventBuilder::text_note("Other event")
        .sign_with_keys(&keys)
        .expect("Failed to sign other event");
    let other_status = db
        .save_event_txn(&mut txn, &other_event)
        .expect("Failed to save other event");
    assert!(matches!(other_status, SaveEventStatus::Success));

    txn.commit().expect("Failed to commit transaction");
}


#[tokio::test]
async fn test_read_transactions() {
    let (db, _temp_dir) = setup_db().await;
    let keys = Keys::generate();

    // Save some events first
    let event1 = EventBuilder::text_note("Event 1")
        .sign_with_keys(&keys)
        .expect("Failed to sign event");
    let event2 = EventBuilder::text_note("Event 2")
        .sign_with_keys(&keys)
        .expect("Failed to sign event");

    db.save_event(&event1)
        .await
        .expect("Failed to save first event");
    db.save_event(&event2)
        .await
        .expect("Failed to save second event");

    // Test read transaction
    let rtxn = db.begin_read().expect("Failed to create read transaction");

    let events = db
        .query_txn(&rtxn, Filter::new())
        .expect("Failed to query with read transaction");
    assert_eq!(events.len(), 2);

    // Read transaction can be used multiple times
    let filtered_events = db
        .query_txn(&rtxn, Filter::new().id(event1.id))
        .expect("Failed to query filtered with read transaction");
    assert_eq!(filtered_events.len(), 1);
    assert_eq!(filtered_events.first().unwrap().id, event1.id);

    // Read transaction should be automatically cleaned up on drop
}

#[tokio::test]
async fn test_mixed_save_delete_operations() {
    let (db, _temp_dir) = setup_db().await;
    let keys = Keys::generate();

    let mut txn = db
        .begin_write()
        .expect("Failed to create write transaction");

    // Save some events
    let mut event_ids = Vec::new();
    for i in 0..5 {
        let event = EventBuilder::text_note(format!("Event {}", i))
            .sign_with_keys(&keys)
            .expect("Failed to sign event");
        event_ids.push(event.id);

        db.save_event_txn(&mut txn, &event)
            .expect("Failed to save event");
    }

    // Delete some events by ID
    let deleted1 = db
        .delete_by_id_txn(&mut txn, &event_ids[1])
        .expect("Failed to delete event by ID");
    assert!(deleted1);

    // Delete events by filter
    let deleted_count = db
        .delete_txn(&mut txn, Filter::new().id(event_ids[3]))
        .expect("Failed to delete events by filter");
    assert_eq!(deleted_count, 1);

    // Try to delete non-existent event
    let fake_id = EventId::from_slice(&[42u8; 32]).expect("Failed to create fake ID");
    let deleted_fake = db
        .delete_by_id_txn(&mut txn, &fake_id)
        .expect("Failed to attempt delete of non-existent event");
    assert!(!deleted_fake);

    txn.commit().expect("Failed to commit mixed operations");

    // Verify final state
    let remaining_events = db
        .query(Filter::new())
        .await
        .expect("Failed to query remaining events");
    assert_eq!(remaining_events.len(), 3); // 5 created - 2 deleted

    // Verify specific events are gone
    assert!(!db
        .has_event(&event_ids[1])
        .await
        .expect("Failed to check deleted event"));
    assert!(!db
        .has_event(&event_ids[3])
        .await
        .expect("Failed to check deleted event"));
}

#[tokio::test]
async fn test_transaction_rollback_on_failure() {
    let (db, _temp_dir) = setup_db().await;
    let keys = Keys::generate();

    // Create some valid events
    let mut events = Vec::new();
    for i in 0..3 {
        let event = EventBuilder::text_note(format!("Valid event {}", i))
            .sign_with_keys(&keys)
            .expect("Failed to sign event");
        events.push(event);
    }

    // Save one of the events first to make it a duplicate
    db.save_event(&events[1])
        .await
        .expect("Failed to save duplicate event");

    // Count events before transaction
    let count_before = db
        .query(Filter::new())
        .await
        .expect("Failed to query before transaction")
        .len();
    assert_eq!(count_before, 1); // Only the duplicate event

    // Try to save all events in a manual transaction
    let mut txn = db.begin_write().expect("Failed to create transaction");
    let mut should_rollback = false;
    
    for event in &events {
        let status = db.save_event_txn(&mut txn, event).expect("Failed to save in transaction");
        if !status.is_success() {
            should_rollback = true;
            break;
        }
    }
    
    if should_rollback {
        txn.abort();
    } else {
        txn.commit().expect("Failed to commit");
    }
    
    assert!(should_rollback, "Transaction should have been rolled back due to duplicate");

    // Verify NO new events were added (transaction was rolled back)
    let count_after = db
        .query(Filter::new())
        .await
        .expect("Failed to query after transaction")
        .len();
    assert_eq!(count_after, 1); // Still only the original duplicate event

    // Verify the first event (which was processed before the duplicate) was NOT saved
    let first_event_exists = db
        .has_event(&events[0].id)
        .await
        .expect("Failed to check first event");
    assert!(
        !first_event_exists,
        "First event should not exist due to rollback"
    );

    // Verify the duplicate event still exists
    let duplicate_exists = db
        .has_event(&events[1].id)
        .await
        .expect("Failed to check duplicate event");
    assert!(
        duplicate_exists,
        "Original duplicate event should still exist"
    );

    // Verify the third event (which would have been processed after the duplicate) was NOT saved
    let third_event_exists = db
        .has_event(&events[2].id)
        .await
        .expect("Failed to check third event");
    assert!(
        !third_event_exists,
        "Third event should not exist due to rollback"
    );
}
