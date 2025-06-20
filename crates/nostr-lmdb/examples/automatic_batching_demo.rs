// Copyright (c) 2025 Rust Nostr Developers  
// Distributed under the MIT software license

//! # Automatic Batching Demonstration
//!
//! This example shows how the nostr-lmdb ingester automatically batches
//! events for optimal performance without explicit batch methods.

use std::time::Instant;

use futures::future::join_all;
use nostr::{EventBuilder, Filter, Keys};
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
    
    println!("🚀 Automatic Batching Demonstration");
    println!("=====================================\n");
    
    // Test 1: Sequential saves (less likely to batch)
    println!("📝 Test 1: Sequential saves with delays");
    let mut events = Vec::new();
    for i in 0..10 {
        let event = EventBuilder::text_note(format!("Sequential event {}", i))
            .sign_with_keys(&keys)?;
        events.push(event);
    }
    
    let start = Instant::now();
    for event in &events {
        db.save_event(event).await?;
        // Small delay prevents batching
        tokio::time::sleep(tokio::time::Duration::from_millis(15)).await;
    }
    let sequential_duration = start.elapsed();
    
    println!("✅ Sequential saves (10 events): {:?}", sequential_duration);
    println!("   Average: {:?} per event\n", sequential_duration / 10);
    
    // Clear for next test
    db.wipe().await?;
    
    // Test 2: Concurrent saves (triggers automatic batching)
    println!("📝 Test 2: Concurrent saves (automatic batching)");
    const BATCH_SIZE: usize = 100;
    
    let mut events = Vec::new();
    for i in 0..BATCH_SIZE {
        let event = EventBuilder::text_note(format!("Concurrent event {}", i))
            .sign_with_keys(&keys)?;
        events.push(event);
    }
    
    let start = Instant::now();
    
    // Launch all saves concurrently and wait properly
    let futures: Vec<_> = events.iter().map(|event| db.save_event(event)).collect();
    join_all(futures).await.into_iter().collect::<Result<Vec<_>, _>>()?;
    
    let concurrent_duration = start.elapsed();
    
    println!("✅ Concurrent saves (100 events): {:?}", concurrent_duration);
    println!("   Average: {:?} per event", concurrent_duration / 100);
    println!("   Rate: {:.0} events/second\n", 100.0 / concurrent_duration.as_secs_f64());
    
    // Verify all events were saved
    let saved_count = db.query(Filter::new()).await?.len();
    println!("📊 Verified: {} events saved", saved_count);
    
    // Test 3: Rapid fire saves (maximum batching)
    println!("\n📝 Test 3: Rapid fire saves (maximum batching)");
    db.wipe().await?;
    
    const RAPID_COUNT: usize = 1000;
    let mut events = Vec::new();
    for i in 0..RAPID_COUNT {
        let event = EventBuilder::text_note(format!("Rapid event {}", i))
            .sign_with_keys(&keys)?;
        events.push(event);
    }
    
    let start = Instant::now();
    
    // Fire off all saves as fast as possible and wait properly
    let futures: Vec<_> = events.iter().map(|event| db.save_event(event)).collect();
    join_all(futures).await.into_iter().collect::<Result<Vec<_>, _>>()?;
    
    let rapid_duration = start.elapsed();
    
    println!("✅ Rapid fire saves ({} events): {:?}", RAPID_COUNT, rapid_duration);
    println!("   Average: {:.2?} per event", rapid_duration / RAPID_COUNT as u32);
    println!("   Rate: {:.0} events/second", RAPID_COUNT as f64 / rapid_duration.as_secs_f64());
    
    // Show the performance difference
    println!("\n📈 Performance Summary:");
    println!("   Sequential (with delays): {:?}/event", sequential_duration / 10);
    println!("   Concurrent (auto-batch):  {:?}/event", concurrent_duration / 100);
    println!("   Rapid fire (max batch):   {:.2?}/event", rapid_duration / RAPID_COUNT as u32);
    
    let speedup = (sequential_duration.as_nanos() / 10) as f64 / 
                  (rapid_duration.as_nanos() / RAPID_COUNT as u128) as f64;
    println!("\n🎯 Automatic batching provides up to {:.1}x speedup!", speedup);
    
    println!("\n💡 Key Insights:");
    println!("   • The ingester automatically batches events within ~10ms windows");
    println!("   • No explicit batch methods needed - just use save_event()");
    println!("   • Concurrent/rapid saves trigger automatic batching");
    println!("   • Performance scales with concurrency level");
    
    Ok(())
}