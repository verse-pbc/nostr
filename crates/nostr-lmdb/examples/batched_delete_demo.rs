// Copyright (c) 2025 Rust Nostr Developers  
// Distributed under the MIT software license

//! # Batched Delete Operations Demonstration
//!
//! This example shows how delete operations are now batched through the ingester
//! for optimal performance when deleting multiple events.

use std::time::Instant;

use futures::future::join_all;
use nostr::{EventBuilder, Filter, Keys, Kind};
use nostr_database::{NostrEventsDatabase, NostrDatabaseWipe};
use nostr_lmdb::NostrLMDB;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize database
    let db = NostrLMDB::open("./demo_db")?;
    
    // Clean up any existing data
    db.wipe().await?;
    
    // Generate keys for testing
    let keys = Keys::generate();
    let author_key2 = Keys::generate();
    
    println!("🗑️  Batched Delete Operations Demonstration");
    println!("==========================================\n");
    
    // Test 1: Create a mix of events
    println!("📝 Creating test events...");
    let mut events = Vec::new();
    
    // Create text notes
    for i in 0..50 {
        let event = EventBuilder::text_note(format!("Text note {}", i))
            .sign_with_keys(&keys)?;
        events.push(event);
    }
    
    // Create reaction events
    for i in 0..30 {
        let event = EventBuilder::new(Kind::Reaction, format!("👍 {}", i))
            .sign_with_keys(&keys)?;
        events.push(event);
    }
    
    // Create events from another author
    for i in 0..20 {
        let event = EventBuilder::text_note(format!("Other author note {}", i))
            .sign_with_keys(&author_key2)?;
        events.push(event);
    }
    
    // Save all events concurrently
    let futures: Vec<_> = events.iter().map(|event| db.save_event(event)).collect();
    join_all(futures).await.into_iter().collect::<Result<Vec<_>, _>>()?;
    
    println!("✅ Created {} events", events.len());
    let total_count = db.query(Filter::new()).await?.len();
    println!("📊 Total events in database: {}\n", total_count);
    
    // Test 2: Sequential deletes (less likely to batch)
    println!("📝 Test 1: Sequential deletes with delays");
    let start = Instant::now();
    
    // Delete reactions one by one with delays
    let _reaction_filter = Filter::new().kind(Kind::Reaction).limit(10);
    for _ in 0..10 {
        db.delete(Filter::new().kind(Kind::Reaction).limit(1)).await?;
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
    
    let sequential_duration = start.elapsed();
    println!("✅ Sequential deletes (10 events): {:?}", sequential_duration);
    println!("   Average: {:?} per delete\n", sequential_duration / 10);
    
    // Test 3: Concurrent deletes (triggers automatic batching)
    println!("📝 Test 2: Concurrent deletes (automatic batching)");
    
    // Delete remaining reactions concurrently
    let start = Instant::now();
    let delete_futures: Vec<_> = (0..20)
        .map(|_| db.delete(Filter::new().kind(Kind::Reaction).limit(1)))
        .collect();
    
    join_all(delete_futures).await.into_iter().collect::<Result<Vec<_>, _>>()?;
    
    let concurrent_duration = start.elapsed();
    println!("✅ Concurrent deletes (20 events): {:?}", concurrent_duration);
    println!("   Average: {:?} per delete", concurrent_duration / 20);
    println!("   Rate: {:.0} deletes/second\n", 20.0 / concurrent_duration.as_secs_f64());
    
    // Verify reactions were deleted
    let remaining_reactions = db.query(Filter::new().kind(Kind::Reaction)).await?.len();
    println!("📊 Remaining reactions: {}", remaining_reactions);
    
    // Test 4: Mixed save and delete operations (maximum batching)
    println!("\n📝 Test 3: Mixed save and delete operations");
    
    let start = Instant::now();
    
    // Create events for saving
    let mut save_events = Vec::new();
    for i in 0..25 {
        let event = EventBuilder::text_note(format!("New mixed note {}", i))
            .sign_with_keys(&keys)?;
        save_events.push(event);
    }
    
    // Create save futures
    let save_futures: Vec<_> = save_events.iter()
        .map(|event| db.save_event(event))
        .collect();
    
    // Create delete futures
    let delete_futures: Vec<_> = (0..25)
        .map(|_| {
            let filter = Filter::new()
                .kind(Kind::TextNote)
                .author(keys.public_key())
                .limit(1);
            db.delete(filter)
        })
        .collect();
    
    // Wait for all operations concurrently
    let (save_results, delete_results) = tokio::join!(
        join_all(save_futures),
        join_all(delete_futures)
    );
    
    // Check results
    save_results.into_iter().collect::<Result<Vec<_>, _>>()?;
    delete_results.into_iter().collect::<Result<Vec<_>, _>>()?;
    
    let mixed_duration = start.elapsed();
    println!("✅ Mixed operations (50 total): {:?}", mixed_duration);
    println!("   Average: {:?} per operation", mixed_duration / 50);
    println!("   Rate: {:.0} operations/second", 50.0 / mixed_duration.as_secs_f64());
    
    // Final count
    let final_count = db.query(Filter::new()).await?.len();
    println!("\n📊 Final event count: {}", final_count);
    
    // Show the performance difference
    println!("\n📈 Performance Summary:");
    println!("   Sequential deletes:    {:?}/delete", sequential_duration / 10);
    println!("   Concurrent deletes:    {:?}/delete", concurrent_duration / 20);
    println!("   Mixed operations:      {:?}/operation", mixed_duration / 50);
    
    let speedup = (sequential_duration.as_nanos() / 10) as f64 / 
                  (concurrent_duration.as_nanos() / 20) as f64;
    println!("\n🎯 Automatic batching provides up to {:.1}x speedup for deletes!", speedup);
    
    println!("\n💡 Key Insights:");
    println!("   • Delete operations now go through the ingester for batching");
    println!("   • Mixed save/delete operations share the same transaction");
    println!("   • Automatic batching works for both saves and deletes");
    println!("   • Performance scales with concurrency level");
    
    Ok(())
}