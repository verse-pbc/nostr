// Copyright (c) 2025 Rust Nostr Developers
// Distributed under the MIT software license

//! # Transaction API Example
//!
//! This example demonstrates how to use the transaction-based API in nostr-lmdb
//! for advanced use cases where you need precise control over database operations.
//!
//! For most use cases, the automatic batching in the async save_event API is sufficient.
//! Multiple concurrent save_event calls will be automatically batched by the ingester.

use std::time::Instant;

use nostr::{EventBuilder, Filter, Keys};
use nostr_database::{NostrDatabaseWipe, NostrEventsDatabase};
use nostr_lmdb::NostrLMDB;
use scoped_heed::Scope;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize database
    let db = NostrLMDB::open("./example_db")?;

    // Clean up any existing data
    db.wipe().await?;

    // Generate keys for testing
    let keys = Keys::generate();

    println!("Transaction API Example for nostr-lmdb\n");
    println!("Note: The async save_event API automatically batches operations internally.\n");

    // Example 1: Manual Transaction Management
    println!("Example 1: Manual Transaction Management");

    let mut txn = db.begin_write()?;

    // Save multiple events in a single transaction
    let mut event_ids = Vec::new();
    for i in 0..10 {
        let event = EventBuilder::text_note(format!("Manual transaction note {}", i))
            .sign_with_keys(&keys)?;
        event_ids.push(event.id);

        db.save_event_txn(&mut txn, &event)?;
    }

    // Delete some events in the same transaction
    let deleted_count = db.delete_txn(&mut txn, Filter::new().id(event_ids[0]))?;
    println!("Deleted {} events in transaction", deleted_count);

    // Commit all changes atomically
    txn.commit()?;

    // Verify results
    let events = db.query(Filter::new()).await?;
    println!("Total events after manual transaction: {}\n", events.len());

    // Example 2: Automatic Batching Demonstration
    println!("Example 2: Automatic Batching Demonstration");
    println!("Multiple async save_event calls are automatically batched by the ingester.");

    // Create and save multiple events
    let start = Instant::now();
    let mut successful = 0;
    for i in 0..20 {
        let event = EventBuilder::text_note(format!("Auto-batched event {}", i)).sign_with_keys(&keys)?;
        // These async calls will be automatically batched by the ingester
        if db.save_event(&event).await.is_ok() {
            successful += 1;
        }
    }
    let duration = start.elapsed();

    println!("Saved {} events in {:?} (automatically batched)\n", successful, duration);

    // Example 3: Transaction API for Complex Operations
    println!("Example 3: Transaction API for Complex Operations");
    println!("Use transactions when you need atomic operations across multiple changes.");

    let mut txn = db.begin_write()?;

    // Save and delete in the same transaction
    let event_to_save = EventBuilder::text_note("Event to save in transaction").sign_with_keys(&keys)?;
    let event_to_delete = EventBuilder::text_note("Event to delete").sign_with_keys(&keys)?;
    
    // First save the event we'll delete
    db.save_event(&event_to_delete).await?;
    
    // Now in a single transaction: save one and delete another
    db.save_event_txn(&mut txn, &event_to_save)?;
    db.delete_txn(&mut txn, Filter::new().id(event_to_delete.id))?;
    
    txn.commit()?;
    println!("Completed atomic save and delete operation\n");

    // Example 4: Scoped Transactions
    println!("Example 3: Scoped Transactions");

    let scope_a = Scope::named("tenant_a")?;
    let scope_b = Scope::named("tenant_b")?;

    // Clear any existing scoped data
    db.scoped(&scope_a)?.delete(Filter::new()).await?;
    db.scoped(&scope_b)?.delete(Filter::new()).await?;

    let mut txn = db.begin_write()?;

    // Save events in different scopes within the same transaction
    let event_a = EventBuilder::text_note("Event in tenant A").sign_with_keys(&keys)?;
    let event_b = EventBuilder::text_note("Event in tenant B").sign_with_keys(&keys)?;

    db.save_event_txn_scoped(&mut txn, &scope_a, &event_a)?;
    db.save_event_txn_scoped(&mut txn, &scope_b, &event_b)?;

    txn.commit()?;

    // Verify scope isolation
    let scoped_view_a = db.scoped(&scope_a)?;
    let scoped_view_b = db.scoped(&scope_b)?;

    let events_a = scoped_view_a.query(Filter::new()).await?;
    let events_b = scoped_view_b.query(Filter::new()).await?;

    println!("Events in scope A: {}", events_a.len());
    println!("Events in scope B: {}", events_b.len());

    // Global scope should have events from examples 1 and 2
    let global_events = db.query(Filter::new()).await?;
    println!("Events in global scope: {}\n", global_events.len());

    // Example 5: Performance of Automatic Batching
    println!("Example 5: Performance of Automatic Batching");
    println!("The ingester automatically batches events received within a short time window.\n");

    // Clear for clean comparison
    db.wipe().await?;

    const NUM_EVENTS: usize = 100;
    
    // Create events
    let mut test_events = Vec::new();
    for i in 0..NUM_EVENTS {
        let event = EventBuilder::text_note(format!("Batching test event {}", i))
            .sign_with_keys(&keys)?;
        test_events.push(event);
    }

    // Save events - they will be automatically batched
    let start = Instant::now();
    let mut successful = 0;
    for event in &test_events {
        if db.save_event(event).await.is_ok() {
            successful += 1;
        }
    }
    let auto_batch_duration = start.elapsed();
    
    println!("Automatic batching results:");
    println!("  Saved {} events in {:?}", successful, auto_batch_duration);
    println!("  Events are automatically grouped into batches by the ingester");
    println!("  This provides optimal performance without manual batching\n");

    // Example 6: Error Handling in Transactions
    println!("Example 6: Error Handling in Transactions");

    let mut txn = db.begin_write()?;

    let test_event = EventBuilder::text_note("Test event for duplicate").sign_with_keys(&keys)?;

    // Save event first time
    let status1 = db.save_event_txn(&mut txn, &test_event)?;
    println!("First save: {:?}", status1);

    // Try to save the same event again (should be rejected as duplicate)
    let status2 = db.save_event_txn(&mut txn, &test_event)?;
    println!("Second save: {:?}", status2);

    // Transaction is still valid for other operations
    let other_event = EventBuilder::text_note("Different event").sign_with_keys(&keys)?;
    let status3 = db.save_event_txn(&mut txn, &other_event)?;
    println!("Other event save: {:?}", status3);

    txn.commit()?;

    // Example 7: Read Transactions
    println!("\nExample 7: Read Transactions");

    let rtxn = db.begin_read()?;

    // Multiple read operations in the same transaction
    let all_events = db.query_txn(&rtxn, Filter::new())?;
    println!("Total events: {}", all_events.len());

    if let Some(first_event) = all_events.first() {
        let filtered_events = db.query_txn(&rtxn, Filter::new().id(first_event.id))?;
        println!("Filtered events: {}", filtered_events.len());
    }

    // Read transaction is automatically cleaned up on drop
    drop(rtxn);

    println!("\nAll examples completed successfully!");

    Ok(())
}
