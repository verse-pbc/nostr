use nostr_lmdb::nostr::{Event, Filter, JsonUtil};
use nostr_lmdb::{NostrEventsDatabase, NostrLMDB};
use tempfile::TempDir;

// These are the key events from the test
const EVENT_10: &str = r#"{"id":"90a761aec9b5b60b399a76826141f529db17466deac85696a17e4a243aa271f9","pubkey":"aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4","created_at":1704645606,"kind":0,"tags":[],"content":"{\"name\":\"key-a\",\"display_name\":\"Key A\",\"lud16\":\"keya@ln.address\"}","sig":"ec8f49d4c722b7ccae102d49befff08e62db775e5da43ef51b25c47dfdd6a09dc7519310a3a63cbdb6ec6b3250e6f19518eb47be604edeb598d16cdc071d3dbc"}"#;
const EVENT_11: &str = r#"{"id":"a295422c636d3532875b75739e8dae3cdb4dd2679c6e4994c9a39c7ebf8bc620","pubkey":"79dff8f82963424e0bb02708a22e44b4980893e3a4be0fa3cb60a43b946764e3","created_at":1704646569,"kind":5,"tags":[["e","90a761aec9b5b60b399a76826141f529db17466deac85696a17e4a243aa271f9"]],"content":"","sig":"d4dc8368a4ad27eef63cacf667345aadd9617001537497108234fc1686d546c949cbb58e007a4d4b632c65ea135af4fbd7a089cc60ab89b6901f5c3fc6a47b29"}"#; // Invalid event deletion
const EVENT_12: &str = r#"{"id":"999e3e270100d7e1eaa98fcfab4a98274872c1f2dfdab024f32e42a5a12d5b5e","pubkey":"aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4","created_at":1704646606,"kind":5,"tags":[["e","90a761aec9b5b60b399a76826141f529db17466deac85696a17e4a243aa271f9"]],"content":"","sig":"4f3a33fd52784cea7ca8428fd35d94d65049712e9aa11a70b1a16a1fcd761c7b7e27afac325728b1c00dfa11e33e78b2efd0430a7e4b28f4ede5b579b3f32614"}"#;

#[tokio::test]
async fn test_invalid_deletion_event_not_stored() {
    let temp_dir = TempDir::new().unwrap();
    let db = NostrLMDB::open(&temp_dir).unwrap();

    // Parse the events
    let event_10 = Event::from_json(EVENT_10).unwrap();
    let event_11 = Event::from_json(EVENT_11).unwrap();
    let event_12 = Event::from_json(EVENT_12).unwrap();

    // Event 10: By user AA4...
    let status_10 = db.save_event(&event_10).await.unwrap();
    assert!(status_10.is_success());

    // Event 11: Invalid deletion by user 79D... (different author)
    let status_11 = db.save_event(&event_11).await.unwrap();
    println!("Event 11 status: {:?}", status_11);

    // Event 12: Valid deletion by user AA4... (same author)
    let status_12 = db.save_event(&event_12).await.unwrap();
    println!("Event 12 status: {:?}", status_12);

    // Check what events are stored
    let all_events = db.query(Filter::new()).await.unwrap();
    println!("Total events in DB: {}", all_events.len());

    for event in all_events.iter() {
        println!("Event ID: {:?}, Kind: {:?}", event.id, event.kind);
    }

    // Event 11 should NOT be stored (it's an invalid deletion)
    let event_11_exists = all_events.iter().any(|e| e.id == event_11.id);
    println!("Event 11 exists: {}", event_11_exists);

    // Event 12 should be stored (it's a valid deletion)
    let event_12_exists = all_events.iter().any(|e| e.id == event_12.id);
    println!("Event 12 exists: {}", event_12_exists);

    // Event 10 should NOT exist (deleted by event 12)
    let event_10_exists = all_events.iter().any(|e| e.id == event_10.id);
    println!("Event 10 exists: {}", event_10_exists);

    // Final assertion: Only event 12 should be in the DB
    assert_eq!(all_events.len(), 1);
    assert_eq!(all_events.to_vec()[0].id, event_12.id);
}
