// Copyright (c) 2025 Rust Nostr Developers  
// Distributed under the MIT software license

//! # Transaction API Demonstration
//!
//! This example shows how to use the transaction-based API from NostrLMDB directly

use nostr::{EventBuilder, Filter, Keys};
use nostr_lmdb::{NostrLMDB, Scope};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize database
    let db = Arc::new(NostrLMDB::open("./demo_db")?);
    
    // Generate keys for testing
    let keys = Keys::generate();
    
    println!("🔧 Transaction API Demonstration");
    println!("==================================\n");
    
    // Use spawn_blocking since LMDB operations are sync
    let db_clone = Arc::clone(&db);
    tokio::task::spawn_blocking(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Create a write transaction
        let mut txn = db_clone.write_transaction()?;
        
        // Save multiple events in the same transaction
        let scope = Scope::named("tenant_a")?;
        
        println!("📝 Saving events in transaction...");
        for i in 0..5 {
            let event = EventBuilder::text_note(format!("Event {} in transaction", i))
                .sign_with_keys(&keys)?;
            
            let status = db_clone.save_event_with_txn(&mut txn, &scope, &event)?;
            println!("  Event {}: {:?}", i, status);
        }
        
        // Also delete some events in the same transaction
        println!("\n🗑️  Deleting events with kind=1 in same transaction...");
        let delete_filter = Filter::new().kind(nostr::Kind::TextNote).limit(2);
        let deleted_count = db_clone.delete_with_txn(&mut txn, &scope, delete_filter)?;
        println!("  Deleted {} events", deleted_count);
        
        // Commit the transaction
        println!("\n✅ Committing transaction...");
        txn.commit()?;
        
        println!("Transaction completed successfully!");
        
        Ok(())
    }).await?.map_err(|e| format!("Transaction error: {}", e))?;
    
    // Verify the results
    println!("\n📊 Verifying results...");
    let scope = Scope::named("tenant_a")?;
    let scoped_view = db.scoped(&scope).map_err(|e| format!("Scoped view error: {}", e))?;
    let remaining_events = scoped_view.query(Filter::new()).await.map_err(|e| format!("Query error: {}", e))?;
    println!("Events remaining in scope: {}", remaining_events.len());
    
    Ok(())
}