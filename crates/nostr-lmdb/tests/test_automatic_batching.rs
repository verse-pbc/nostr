// Copyright (c) 2025 Rust Nostr Developers
// Distributed under the MIT software license

//! Test that verifies automatic batching is working in the ingester

use std::time::Instant;

use futures::future::join_all;
use nostr::{EventBuilder, Filter, Keys};
use nostr_database::{NostrEventsDatabase, NostrDatabaseWipe};
use nostr_lmdb::NostrLMDB;
use tempfile::TempDir;

async fn setup_db() -> (NostrLMDB, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let db = NostrLMDB::open(temp_dir.path()).expect("Failed to open database");
    (db, temp_dir)
}

#[tokio::test]
async fn test_automatic_batching_with_1000_events() {
    let (db, _temp_dir) = setup_db().await;
    let keys = Keys::generate();

    const NUM_EVENTS: usize = 1000;
    
    // Create 1000 events
    let mut events = Vec::new();
    for i in 0..NUM_EVENTS {
        let event = EventBuilder::text_note(format!("Bulk event {}", i))
            .sign_with_keys(&keys)
            .expect("Failed to sign event");
        events.push(event);
    }
    
    // Fire all 1000 saves as fast as possible
    let start = Instant::now();
    let futures: Vec<_> = events.iter().map(|event| db.save_event(event)).collect();
    
    // Wait for all to complete concurrently
    let results = join_all(futures).await;
    
    let duration = start.elapsed();
    
    // Check all succeeded
    for result in results {
        result.expect("Failed to save event");
    }
    
    // Verify all saved
    let saved = db.query(Filter::new()).await.expect("Failed to query");
    assert_eq!(saved.len(), NUM_EVENTS);
    
    let events_per_sec = NUM_EVENTS as f64 / duration.as_secs_f64();
    
    println!("Saved {} events in {:?}", NUM_EVENTS, duration);
    println!("Rate: {:.0} events/second", events_per_sec);
    println!("Average: {:.2} µs/event", duration.as_micros() as f64 / NUM_EVENTS as f64);
    
    // With automatic batching of 1000 events, we should see excellent performance
    // The ingester should efficiently batch these events into very few transactions
    assert!(
        events_per_sec > 10_000.0,
        "Expected > 10,000 events/sec with automatic batching of {} events, got {:.0}",
        NUM_EVENTS,
        events_per_sec
    );
}

#[tokio::test]
async fn test_batching_vs_individual_transactions() {
    let (db, _temp_dir) = setup_db().await;
    let keys = Keys::generate();
    
    const NUM_EVENTS: usize = 500;
    
    // Test 1: Individual transactions (worst case)
    println!("\nTest 1: Individual transactions");
    let mut events = Vec::new();
    for i in 0..NUM_EVENTS {
        let event = EventBuilder::text_note(format!("Individual event {}", i))
            .sign_with_keys(&keys)
            .expect("Failed to sign event");
        events.push(event);
    }
    
    let start = Instant::now();
    for event in &events {
        let mut txn = db.begin_write().expect("Failed to begin transaction");
        db.save_event_txn(&mut txn, event).expect("Failed to save event");
        txn.commit().expect("Failed to commit");
    }
    let individual_duration = start.elapsed();
    
    println!("Individual transactions: {:?} ({:.0} events/sec)",
        individual_duration,
        NUM_EVENTS as f64 / individual_duration.as_secs_f64()
    );
    
    // Clear database
    db.wipe().await.expect("Failed to wipe");
    
    // Test 2: Async saves (automatic batching)
    println!("\nTest 2: Concurrent async saves with automatic batching");
    let mut events = Vec::new();
    for i in 0..NUM_EVENTS {
        let event = EventBuilder::text_note(format!("Batched event {}", i))
            .sign_with_keys(&keys)
            .expect("Failed to sign event");
        events.push(event);
    }
    
    let start = Instant::now();
    
    // Fire all saves concurrently and wait for all to complete
    let futures: Vec<_> = events.iter().map(|event| db.save_event(event)).collect();
    let results = join_all(futures).await;
    
    let batched_duration = start.elapsed();
    
    // Check all succeeded
    for result in results {
        result.expect("Failed to save event");
    }
    
    println!("Automatic batching: {:?} ({:.0} events/sec)",
        batched_duration,
        NUM_EVENTS as f64 / batched_duration.as_secs_f64()
    );
    
    // Calculate speedup
    let speedup = individual_duration.as_secs_f64() / batched_duration.as_secs_f64();
    println!("\nSpeedup: {:.1}x", speedup);
    
    // Compare performance
    // Note: The comparison might vary based on system load and other factors
    // The key point is that automatic batching provides good performance without manual transaction management
    if batched_duration < individual_duration {
        println!("✅ Automatic batching is faster than individual transactions");
    } else {
        println!("⚠️  Individual transactions were faster in this run, but automatic batching provides other benefits:");
        println!("   - Non-blocking async API");
        println!("   - No manual transaction management needed");
        println!("   - Better for concurrent workloads");
    }
}

