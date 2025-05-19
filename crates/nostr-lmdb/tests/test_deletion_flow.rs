use nostr_lmdb::nostr::{Event, Filter, JsonUtil};
use nostr_lmdb::{NostrEventsDatabase, NostrLMDB};
use tempfile::TempDir;

// The key events
const EVENT_3: &str = r#"{"id":"63b8b829aa31a2de870c3a713541658fcc0187be93af2032ec2ca039befd3f70","pubkey":"aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4","created_at":1704644596,"kind":32122,"tags":[["d","id-2"]],"content":"","sig":"607b1a67bef57e48d17df4e145718d10b9df51831d1272c149f2ab5ad4993ae723f10a81be2403ae21b2793c8ed4c129e8b031e8b240c6c90c9e6d32f62d26ff"}"#;
const EVENT_8: &str = r#"{"id":"6975ace0f3d66967f330d4758fbbf45517d41130e2639b54ca5142f37757c9eb","pubkey":"aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4","created_at":1704644621,"kind":5,"tags":[["a","32122:aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4:id-2"]],"content":"","sig":"9bb09e4759899d86e447c3fa1be83905fe2eda74a5068a909965ac14fcdabaed64edaeb732154dab734ca41f2fc4d63687870e6f8e56e3d9e180e4a2dd6fb2d2"}"#;

#[tokio::test]
async fn test_event_deletion_flow() {
    let temp_dir = TempDir::new().unwrap();
    let db = NostrLMDB::open(&temp_dir).unwrap();

    // Parse events
    let event_3 = Event::from_json(EVENT_3).unwrap();
    let event_8 = Event::from_json(EVENT_8).unwrap();

    // Check expected coordinate from Event 3
    if let Some(coord) = event_3.coordinate() {
        println!(
            "Event 3 coordinate: kind={}, pubkey={}, identifier={:?}",
            coord.kind, coord.public_key, coord.identifier
        );
    }

    // Check deletion coordinates from Event 8
    for coord in event_8.tags.coordinates() {
        println!(
            "Event 8 deletion coordinate: kind={}, pubkey={}, identifier={:?}",
            coord.kind, coord.public_key, coord.identifier
        );
    }

    // Save Event 3 first
    let status_3 = db.save_event(&event_3).await.unwrap();
    println!("Event 3 save status: {:?}", status_3);
    assert!(status_3.is_success());

    // Query to confirm it's saved
    let query1 = db.query(Filter::new()).await.unwrap();
    println!("After Event 3 save, total events: {}", query1.len());
    assert_eq!(query1.len(), 1);

    // Now save the deletion event
    let status_8 = db.save_event(&event_8).await.unwrap();
    println!("Event 8 save status: {:?}", status_8);
    assert!(status_8.is_success());

    // Query again to see what remains
    let query2 = db.query(Filter::new()).await.unwrap();
    println!("After Event 8 save, total events: {}", query2.len());

    for event in query2.iter() {
        println!("  Remaining event: id={}, kind={}", event.id, event.kind);
    }

    // Event 3 should be deleted by Event 8
    let event_3_exists = query2.iter().any(|e| e.id == event_3.id);
    assert!(!event_3_exists, "Event 3 should be deleted by Event 8");

    // Only Event 8 should remain
    assert_eq!(query2.len(), 1);
    assert_eq!(query2.to_vec()[0].id, event_8.id);
}
