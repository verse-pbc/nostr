use nostr_lmdb::nostr::{Event, Filter, JsonUtil};
use nostr_lmdb::{NostrEventsDatabase, NostrLMDB};
use tempfile::TempDir;

// The key events with problematic coordinate deletion
const EVENT_3: &str = r#"{"id":"63b8b829aa31a2de870c3a713541658fcc0187be93af2032ec2ca039befd3f70","pubkey":"aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4","created_at":1704644596,"kind":32122,"tags":[["d","id-2"]],"content":"","sig":"607b1a67bef57e48d17df4e145718d10b9df51831d1272c149f2ab5ad4993ae723f10a81be2403ae21b2793c8ed4c129e8b031e8b240c6c90c9e6d32f62d26ff"}"#;
const EVENT_7: &str = r#"{"id":"63dc49a8f3278a2de8dc0138939de56d392b8eb7a18c627e4d78789e2b0b09f2","pubkey":"79dff8f82963424e0bb02708a22e44b4980893e3a4be0fa3cb60a43b946764e3","created_at":1704644616,"kind":5,"tags":[["a","32122:aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4:"]],"content":"","sig":"977e54e5d57d1fbb83615d3a870037d9eb5182a679ca8357523bbf032580689cf481f76c88c7027034cfaf567ba9d9fe25fc8cd334139a0117ad5cf9fe325eef"}"#;
const EVENT_8: &str = r#"{"id":"6975ace0f3d66967f330d4758fbbf45517d41130e2639b54ca5142f37757c9eb","pubkey":"aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4","created_at":1704644621,"kind":5,"tags":[["a","32122:aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4:id-2"]],"content":"","sig":"9bb09e4759899d86e447c3fa1be83905fe2eda74a5068a909965ac14fcdabaed64edaeb732154dab734ca41f2fc4d63687870e6f8e56e3d9e180e4a2dd6fb2d2"}"#;

#[tokio::test]
async fn test_coordinate_deletion_issue() {
    let temp_dir = TempDir::new().unwrap();
    let db = NostrLMDB::open(&temp_dir).unwrap();

    // Parse events
    let event_3 = Event::from_json(EVENT_3).unwrap();
    let event_7 = Event::from_json(EVENT_7).unwrap();
    let event_8 = Event::from_json(EVENT_8).unwrap();

    // Event 3: kind=32122, pubkey=aa4..., tag=["d","id-2"]
    println!(
        "Event 3: kind={}, pubkey={}, tags={:?}, created_at={}",
        event_3.kind, event_3.pubkey, event_3.tags, event_3.created_at
    );

    // Event 7: Deletion by wrong user 79d...
    println!(
        "Event 7: kind={}, pubkey={}, tags={:?}, created_at={}",
        event_7.kind, event_7.pubkey, event_7.tags, event_7.created_at
    );

    // Event 8: Deletion by correct user aa4...
    println!(
        "Event 8: kind={}, pubkey={}, tags={:?}, created_at={}",
        event_8.kind, event_8.pubkey, event_8.tags, event_8.created_at
    );

    // Save Event 3
    let status_3 = db.save_event(&event_3).await.unwrap();
    println!("Event 3 save status: {:?}", status_3);

    // Save Event 7 (invalid deletion)
    let status_7 = db.save_event(&event_7).await.unwrap();
    println!("Event 7 save status: {:?}", status_7);

    // Save Event 8 (valid deletion)
    let status_8 = db.save_event(&event_8).await.unwrap();
    println!("Event 8 save status: {:?}", status_8);

    // Query all events
    let all_events = db.query(Filter::new()).await.unwrap();
    println!("Total events after all saves: {}", all_events.len());

    for event in all_events.iter() {
        println!(
            "Event ID: {}, Kind: {}, Content: {}",
            event.id, event.kind, event.content
        );
    }

    // Event 3 should NOT exist (deleted by Event 8)
    let event_3_exists = all_events.iter().any(|e| e.id == event_3.id);
    println!("Event 3 exists: {}", event_3_exists);
    assert!(!event_3_exists, "Event 3 should be deleted by Event 8");
}
