use nostr_lmdb::nostr::nips::nip09::EventDeletionRequest;
use nostr_lmdb::nostr::{EventBuilder, Filter, Keys};
use nostr_lmdb::{NostrEventsDatabase, NostrLMDB};
use tempfile::TempDir;

#[tokio::test]
async fn test_invalid_deletion_rejected() {
    let temp_dir = TempDir::new().unwrap();
    let db = NostrLMDB::open(&temp_dir).unwrap();

    // Create two users
    let user_a = Keys::generate();
    let user_b = Keys::generate();

    // User A creates an event
    let event_a = EventBuilder::text_note("Event by user A")
        .sign_with_keys(&user_a)
        .unwrap();

    let status_a = db.save_event(&event_a).await.unwrap();
    assert!(status_a.is_success());

    // User B tries to delete User A's event (should be rejected)
    let deletion_request = EventDeletionRequest::new().id(event_a.id);
    let deletion_event = EventBuilder::delete(deletion_request)
        .sign_with_keys(&user_b)
        .unwrap();

    let deletion_status = db.save_event(&deletion_event).await.unwrap();
    println!("Deletion status: {:?}", deletion_status);

    // Check if deletion event is either rejected or not stored at all
    let events = db.query(Filter::new()).await.unwrap();
    println!("Events found after deletion attempt: {}", events.len());

    // Either:
    // - The deletion was rejected (only 1 event in DB)
    // - The deletion was stored but the original wasn't deleted (2 events)
    // For this test, we expect the deletion to be rejected
    if events.len() == 2 {
        // The deletion event was stored - check that it had no effect on the original
        assert!(events.iter().any(|e| e.id == event_a.id));
        assert!(events.iter().any(|e| e.id == deletion_event.id));
    } else {
        // Deletion was rejected - only the original event exists
        assert_eq!(events.len(), 1);
        assert_eq!(events.to_vec()[0].id, event_a.id);
    }
}
