// Copyright (c) 2025 Rust Nostr Developers  
// Distributed under the MIT software license

//! # Batched Operations Demonstration
//!
//! This example demonstrates the performance benefits of batching database operations
//! using the transaction-based API, including mixed save and delete operations.

use nostr::{EventBuilder, Filter, Keys, Kind};
use nostr_database::NostrEventsDatabase;
use nostr_lmdb::{NostrLMDB, Scope};
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Batched Operations Performance Demonstration");
    println!("==============================================\n");
    
    // First demo: basic batching performance
    demo_basic_batching().await?;
    
    // Second demo: mixed operations with scopes
    demo_mixed_operations().await?;
    
    Ok(())
}

async fn demo_basic_batching() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize database
    let db = Arc::new(NostrLMDB::open("./demo_db_basic")?);
    
    // Generate keys for testing
    let keys = Keys::generate();
    let other_keys = Keys::generate();
    
    // First, populate the database with some events
    println!("📝 Populating database with test events...");
    for i in 0..1000 {
        let event = EventBuilder::text_note(format!("Test event {}", i))
            .sign_with_keys(&keys)?;
        db.save_event(&event).await?;
        
        // Also add some events from other user
        if i % 3 == 0 {
            let other_event = EventBuilder::text_note(format!("Other user event {}", i))
                .sign_with_keys(&other_keys)?;
            db.save_event(&other_event).await?;
        }
    }
    println!("✅ Created 1333 test events\n");
    
    // Demonstrate non-batched operations
    println!("❌ Non-batched Operations (individual async calls):");
    let start = Instant::now();
    
    // Save 100 events individually
    for i in 0..100 {
        let event = EventBuilder::text_note(format!("Non-batched event {}", i))
            .sign_with_keys(&keys)?;
        db.save_event(&event).await?;
    }
    
    // Delete 50 events individually
    for _i in 0..50 {
        let filter = Filter::new()
            .author(keys.public_key())
            .kinds([Kind::TextNote])
            .limit(1);
        db.delete(filter).await?;
    }
    
    let non_batched_duration = start.elapsed();
    println!("  Time: {:?}", non_batched_duration);
    println!("  Operations: 150 (100 saves + 50 deletes)");
    println!("  Rate: {:.0} ops/sec\n", 150.0 / non_batched_duration.as_secs_f64());
    
    // Demonstrate batched operations using transaction API
    println!("✅ Batched Operations (single transaction):");
    let start = Instant::now();
    
    // Clone what we need for the closure
    let db_clone = Arc::clone(&db);
    let keys_clone = keys.clone();
    
    // Use spawn_blocking since LMDB operations are sync
    let batched_result = tokio::task::spawn_blocking(move || -> Result<(usize, usize), Box<dyn std::error::Error + Send + Sync>> {
        // Create a single write transaction
        let mut txn = db_clone.write_transaction()?;
        let scope = Scope::Default;
        
        let mut saved = 0;
        let mut deleted = 0;
        
        // Save 100 events in the same transaction
        for i in 0..100 {
            let event = EventBuilder::text_note(format!("Batched event {}", i))
                .sign_with_keys(&keys_clone)?;
            db_clone.save_event_with_txn(&mut txn, &scope, &event)?;
            saved += 1;
        }
        
        // Delete 50 events in the same transaction
        for _i in 50..100 {
            let filter = Filter::new()
                .author(keys_clone.public_key())
                .kinds([Kind::TextNote])
                .limit(1);
            let count = db_clone.delete_with_txn(&mut txn, &scope, filter)?;
            deleted += count;
        }
        
        // Commit all changes at once
        txn.commit()?;
        
        Ok((saved, deleted))
    }).await?.map_err(|e| format!("Batched operation error: {}", e))?;
    
    let batched_duration = start.elapsed();
    println!("  Time: {:?}", batched_duration);
    println!("  Operations: 150 (100 saves + {} deletes)", batched_result.1);
    println!("  Rate: {:.0} ops/sec", 150.0 / batched_duration.as_secs_f64());
    println!("  Speedup: {:.1}x faster\n", non_batched_duration.as_secs_f64() / batched_duration.as_secs_f64());
    
    // Show final statistics
    println!("📊 Statistics:");
    let total_events = db.count(Filter::new()).await?;
    println!("  Total events in database: {}\n", total_events);
    
    Ok(())
}

async fn demo_mixed_operations() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize database
    let db = Arc::new(NostrLMDB::open("./demo_db_mixed")?);
    
    // Generate keys for testing
    let keys = Keys::generate();
    let other_keys = Keys::generate();
    
    // Demonstrate mixed operations in scopes
    println!("🔀 Mixed Operations with Scopes:");
    let scope_a = Scope::named("tenant_a").map_err(|e| format!("Scope error: {}", e))?;
    let scope_b = Scope::named("tenant_b").map_err(|e| format!("Scope error: {}", e))?;
    
    let mixed_start = Instant::now();
    
    // Clone what we need
    let db_clone = Arc::clone(&db);
    let keys_clone = keys.clone();
    let other_keys_clone = other_keys.clone();
    let scope_a_clone = scope_a.clone();
    let scope_b_clone = scope_b.clone();
    
    tokio::task::spawn_blocking(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut txn = db_clone.write_transaction()?;
        
        // Operations in scope A
        for i in 0..50 {
            let event = EventBuilder::text_note(format!("Scope A event {}", i))
                .sign_with_keys(&keys_clone)?;
            db_clone.save_event_with_txn(&mut txn, &scope_a_clone, &event)?;
        }
        
        // Operations in scope B
        for i in 0..50 {
            let event = EventBuilder::text_note(format!("Scope B event {}", i))
                .sign_with_keys(&other_keys_clone)?;
            db_clone.save_event_with_txn(&mut txn, &scope_b_clone, &event)?;
        }
        
        // Delete some events from scope A
        let delete_filter = Filter::new()
            .author(keys_clone.public_key())
            .kind(Kind::TextNote)
            .limit(10);
        db_clone.delete_with_txn(&mut txn, &scope_a_clone, delete_filter)?;
        
        // Commit all changes across both scopes
        txn.commit()?;
        
        Ok(())
    }).await?.map_err(|e| format!("Batched operation error: {}", e))?;
    
    println!("  Time: {:?}", mixed_start.elapsed());
    println!("  Operations: 110 (100 saves + 10 deletes across 2 scopes)");
    println!("  All committed atomically!\n");
    
    // Show final statistics
    println!("📊 Final Statistics:");
    let total_events = db.count(Filter::new()).await?;
    println!("  Total events in database: {}", total_events);
    
    // Count events in each scope
    let scope_a_view = db.scoped(&scope_a).map_err(|e| format!("Scoped view error: {}", e))?;
    let scope_b_view = db.scoped(&scope_b).map_err(|e| format!("Scoped view error: {}", e))?;
    let scope_a_count = scope_a_view.count(Filter::new()).await?;
    let scope_b_count = scope_b_view.count(Filter::new()).await?;
    
    println!("  Events in scope A: {}", scope_a_count);
    println!("  Events in scope B: {}", scope_b_count);
    
    println!("\n✨ Key Takeaways:");
    println!("  - Batching operations in a single transaction provides massive speedup");
    println!("  - Mixed save/delete operations can be atomically committed together");
    println!("  - Scoped operations maintain isolation while benefiting from batching");
    println!("  - The ingester automatically batches operations behind the scenes");
    
    Ok(())
}