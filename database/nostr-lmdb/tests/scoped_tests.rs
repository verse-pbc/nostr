// Copyright (c) 2024 Michael Dilger
// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2025 Rust Nostr Developers
// Distributed under the MIT software license

// Integration tests for scoped database access
// This file focuses on testing the ScopedView functionality.

use std::ops::Deref;
use std::sync::Arc;

use nostr::nips::nip09::EventDeletionRequest;
use nostr::{Event, EventBuilder, EventId, Filter, JsonUtil, Keys, Kind, Metadata, Tag, Timestamp};
use nostr_database::{NostrDatabase, SaveEventStatus};
use nostr_lmdb::NostrLMDB;
use scoped_heed::Scope;
use tempfile::TempDir;

// Helper struct for managing a temporary database instance for tests
struct TempDatabase {
    db: Arc<NostrLMDB>,
    _temp_dir: TempDir, // Ensures the temp directory is cleaned up when TempDatabase is dropped
}

impl Deref for TempDatabase {
    type Target = Arc<NostrLMDB>;
    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl TempDatabase {
    fn new() -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir for test database");
        // Use default database count which is sufficient for scoped tests
        let db = Arc::new(
            NostrLMDB::builder(temp_dir.path())
                .build()
                .expect("Failed to open test database"),
        );
        Self {
            db,
            _temp_dir: temp_dir,
        }
    }

    // Helper to save an event, abstracting away direct ScopedView usage for cleaner tests
    async fn save_event_in_view(&self, scope_name: Option<&str>, event: &Event) -> SaveEventStatus {
        if let Some(name) = scope_name {
            let scope = Scope::named(name).expect("Failed to create named scope");
            self.db
                .scoped(&scope)
                .expect("Failed to create scoped view")
                .save_event(event)
                .await
                .expect("Failed to save event in scoped view")
        } else {
            // Call directly on self.db for unscoped
            self.db
                .save_event(event)
                .await
                .expect("Failed to save event in unscoped view")
        }
    }

    async fn query_from_view(&self, scope_name: Option<&str>, filter: Filter) -> Vec<Event> {
        if let Some(name) = scope_name {
            let scope = Scope::named(name).expect("Failed to create named scope");
            self.db
                .scoped(&scope)
                .expect("Failed to create scoped view")
                .query(filter)
                .await
                .expect("Failed to query scoped view")
                .to_vec()
        } else {
            // Call directly on self.db for unscoped
            self.db
                .query(filter)
                .await
                .expect("Failed to query unscoped view")
                .to_vec()
        }
    }

    async fn event_by_id_from_view(
        &self,
        scope_name: Option<&str>,
        event_id: &EventId,
    ) -> Option<Event> {
        if let Some(name) = scope_name {
            let scope = Scope::named(name).expect("Failed to create named scope");
            self.db
                .scoped(&scope)
                .expect("Failed to create scoped view")
                .event_by_id(event_id)
                .await
                .expect("Failed to get event by id from scoped view")
        } else {
            // Call directly on self.db for unscoped
            self.db
                .event_by_id(event_id)
                .await
                .expect("Failed to get event by id from unscoped view")
        }
    }

    async fn delete_from_view(&self, scope_name: Option<&str>, filter: Filter) {
        if let Some(name) = scope_name {
            let scope = Scope::named(name).expect("Failed to create named scope");
            self.db
                .scoped(&scope)
                .expect("Failed to create scoped view")
                .delete(filter)
                .await
                .expect("Failed to delete from scoped view");
        } else {
            // Call directly on self.db for unscoped
            self.db
                .delete(filter)
                .await
                .expect("Failed to delete from unscoped view");
        }
    }

    async fn count_from_view(&self, scope_name: Option<&str>, filter: Filter) -> usize {
        if let Some(name) = scope_name {
            let scope = Scope::named(name).expect("Failed to create named scope");
            self.db
                .scoped(&scope)
                .expect("Failed to create scoped view")
                .count(filter)
                .await
                .expect("Failed to count in scoped view")
        } else {
            // Call directly on self.db for unscoped
            self.db
                .count(filter)
                .await
                .expect("Failed to count in unscoped view")
        }
    }
}

// Helper to create a simple text note event for testing
fn create_test_text_note(content: &str, keys: &Keys) -> Event {
    EventBuilder::text_note(content)
        .sign_with_keys(keys)
        .unwrap()
}

#[tokio::test]
async fn test_scoped_isolation_save_query() {
    let db = TempDatabase::new();
    let keys1 = Keys::generate();
    let keys2 = Keys::generate();

    let event1_s1 = create_test_text_note("event1 in scope1", &keys1);
    let event2_s2 = create_test_text_note("event2 in scope2", &keys2);

    // Save events into their respective scopes
    db.save_event_in_view(Some("scope1"), &event1_s1).await;
    db.save_event_in_view(Some("scope2"), &event2_s2).await;

    // Query scope1
    let s1_events = db.query_from_view(Some("scope1"), Filter::new()).await;
    assert_eq!(s1_events.len(), 1);
    assert_eq!(s1_events[0].id, event1_s1.id);

    // Query scope2
    let s2_events = db.query_from_view(Some("scope2"), Filter::new()).await;
    assert_eq!(s2_events.len(), 1);
    assert_eq!(s2_events[0].id, event2_s2.id);

    // Ensure scope1 doesn't see scope2's event and vice-versa
    let s1_query_for_s2_event = db
        .query_from_view(Some("scope1"), Filter::new().id(event2_s2.id))
        .await;
    assert_eq!(s1_query_for_s2_event.len(), 0);
    let s2_query_for_s1_event = db
        .query_from_view(Some("scope2"), Filter::new().id(event1_s1.id))
        .await;
    assert_eq!(s2_query_for_s1_event.len(), 0);

    // Unscoped view should see neither
    let unscoped_events = db.query_from_view(None, Filter::new()).await;
    assert_eq!(unscoped_events.len(), 0);
}

#[tokio::test]
async fn test_unscoped_view_for_legacy_data() {
    let db = TempDatabase::new();
    let keys = Keys::generate();
    let legacy_event = create_test_text_note("legacy data", &keys);

    // Save directly to DB (simulating legacy/unscoped save)
    db.save_event(&legacy_event).await.unwrap();

    // Unscoped view should see it
    let unscoped_events = db.query_from_view(None, Filter::new()).await;
    assert_eq!(unscoped_events.len(), 1);
    assert_eq!(unscoped_events[0].id, legacy_event.id);

    // Scoped view should not see it
    let s1_events = db.query_from_view(Some("scope1"), Filter::new()).await;
    assert_eq!(s1_events.len(), 0);
}

#[tokio::test]
async fn test_scoped_event_by_id() {
    let db = TempDatabase::new();
    let keys = Keys::generate();
    let event_s1 = create_test_text_note("event_s1 for event_by_id", &keys);
    db.save_event_in_view(Some("scope_test_by_id"), &event_s1)
        .await;

    // Found in correct scope
    let found_event = db
        .event_by_id_from_view(Some("scope_test_by_id"), &event_s1.id)
        .await;
    assert!(found_event.is_some());
    assert_eq!(found_event.unwrap().id, event_s1.id);

    // Not found in different scope
    let not_found_event_other_scope = db
        .event_by_id_from_view(Some("other_scope"), &event_s1.id)
        .await;
    assert!(not_found_event_other_scope.is_none());

    // Not found in unscoped view
    let not_found_event_unscoped = db.event_by_id_from_view(None, &event_s1.id).await;
    assert!(not_found_event_unscoped.is_none());
}

#[tokio::test]
async fn test_scoped_delete() {
    let db = TempDatabase::new();
    let keys_a = Keys::generate();
    let keys_b = Keys::generate();

    let event1_s1_by_a = create_test_text_note("s1a_del", &keys_a);
    let event2_s1_by_b = create_test_text_note("s1b_del", &keys_b);
    let event3_s2_by_a = create_test_text_note("s2a_del", &keys_a); // Same author, different scope

    db.save_event_in_view(Some("scope_del_1"), &event1_s1_by_a)
        .await;
    db.save_event_in_view(Some("scope_del_1"), &event2_s1_by_b)
        .await;
    db.save_event_in_view(Some("scope_del_2"), &event3_s2_by_a)
        .await;

    // Delete event1_s1_by_a from scope_del_1 by its ID
    db.delete_from_view(Some("scope_del_1"), Filter::new().id(event1_s1_by_a.id))
        .await;

    let s1_after_del_id = db.query_from_view(Some("scope_del_1"), Filter::new()).await;
    assert_eq!(s1_after_del_id.len(), 1);
    assert_eq!(s1_after_del_id[0].id, event2_s1_by_b.id);

    // Ensure event3_s2_by_a in scope_del_2 is unaffected
    let s2_events = db.query_from_view(Some("scope_del_2"), Filter::new()).await;
    assert_eq!(s2_events.len(), 1);
    assert_eq!(s2_events[0].id, event3_s2_by_a.id);

    // Delete all events by author B from scope_del_1
    db.delete_from_view(
        Some("scope_del_1"),
        Filter::new().author(keys_b.public_key()),
    )
    .await;
    let s1_after_del_author_b = db.query_from_view(Some("scope_del_1"), Filter::new()).await;
    assert_eq!(s1_after_del_author_b.len(), 0);
}

#[tokio::test]
async fn test_scoped_count() {
    let db = TempDatabase::new();
    let keys = Keys::generate();

    db.save_event_in_view(Some("scope_count"), &create_test_text_note("c1", &keys))
        .await;
    db.save_event_in_view(Some("scope_count"), &create_test_text_note("c2", &keys))
        .await;
    db.save_event_in_view(
        Some("other_scope_count"),
        &create_test_text_note("c3", &keys),
    )
    .await;
    db.save_event(&create_test_text_note("c4_unscoped", &keys))
        .await
        .unwrap(); // Legacy save

    let count_scope = db.count_from_view(Some("scope_count"), Filter::new()).await;
    assert_eq!(count_scope, 2);

    let count_other_scope = db
        .count_from_view(Some("other_scope_count"), Filter::new())
        .await;
    assert_eq!(count_other_scope, 1);

    let count_unscoped = db.count_from_view(None, Filter::new()).await;
    assert_eq!(count_unscoped, 1); // Should only count legacy/unscoped data

    // Count with filter
    let event_to_filter = create_test_text_note("filter_me", &keys);
    db.save_event_in_view(Some("scope_count"), &event_to_filter)
        .await;
    let count_scope_filtered = db
        .count_from_view(Some("scope_count"), Filter::new().id(event_to_filter.id))
        .await;
    assert_eq!(count_scope_filtered, 1);
}

// TODO: Add tests for error conditions (e.g. empty scope name - though this is tested in lib.rs, good to have integration test too)
// TODO: Add tests for replaceable events (Kind 0, 10000-19999, 30000-39999) within scopes
// TODO: Add tests for parameterized replaceable events (Kind 30000-39999 with 'd' tag) within scopes
// TODO: Add tests for event deletion (Kind 5) affecting events within the correct scope only
// TODO: Test interactions between scoped and unscoped deletions if applicable.

#[tokio::test]
async fn test_scoped_error_empty_scope_name() {
    let db = TempDatabase::new();

    // Test with empty string in Scope::named
    let empty_scope_result = Scope::named("");
    assert!(
        empty_scope_result.is_err(),
        "Expected error for empty scope name"
    );

    // Test with Scope::Default (should be Ok for an unscoped view)
    let result_unscoped = db.scoped(&Scope::Default);
    assert!(
        result_unscoped.is_ok(),
        "Expected Ok for Scope::Default, got {:?}",
        result_unscoped
    );
    let unscoped_view = result_unscoped.unwrap();
    assert!(
        unscoped_view.scope_name().is_none(),
        "Expected unscoped view to have default (None) scope name"
    );
}

#[tokio::test]
async fn test_scoped_replaceable_events() {
    let db = TempDatabase::new();
    let keys = Keys::generate();
    let scope_name = "replaceable_scope";

    // Kind 0: Metadata
    let initial_metadata_struct = Metadata::new().name("initial_name");
    let initial_metadata_event = EventBuilder::metadata(&initial_metadata_struct)
        .custom_created_at(Timestamp::from(1000)) // Ensure distinct timestamps
        .sign_with_keys(&keys)
        .unwrap();
    db.save_event_in_view(Some(scope_name), &initial_metadata_event)
        .await;

    let newer_metadata_struct = Metadata::new().name("updated_name");
    let newer_metadata_event = EventBuilder::metadata(&newer_metadata_struct)
        .custom_created_at(Timestamp::from(2000)) // Newer timestamp
        .sign_with_keys(&keys)
        .unwrap();
    db.save_event_in_view(Some(scope_name), &newer_metadata_event)
        .await;

    let queried_metadata = db
        .query_from_view(
            Some(scope_name),
            Filter::new().kind(Kind::Metadata).author(keys.public_key()),
        )
        .await;
    assert_eq!(queried_metadata.len(), 1);
    assert_eq!(queried_metadata[0].id, newer_metadata_event.id);
    assert_eq!(queried_metadata[0].content, newer_metadata_event.content);

    // Kind 10002: Generic Replaceable Event (Example)
    let initial_replaceable = EventBuilder::new(Kind::Custom(10002), "initial_content")
        .custom_created_at(Timestamp::from(3000))
        .sign_with_keys(&keys)
        .unwrap();
    db.save_event_in_view(Some(scope_name), &initial_replaceable)
        .await;

    let newer_replaceable = EventBuilder::new(Kind::Custom(10002), "updated_content")
        .custom_created_at(Timestamp::from(4000))
        .sign_with_keys(&keys)
        .unwrap();
    db.save_event_in_view(Some(scope_name), &newer_replaceable)
        .await;

    let queried_replaceable = db
        .query_from_view(
            Some(scope_name),
            Filter::new()
                .kind(Kind::Custom(10002))
                .author(keys.public_key()),
        )
        .await;
    assert_eq!(queried_replaceable.len(), 1);
    assert_eq!(queried_replaceable[0].id, newer_replaceable.id);
    assert_eq!(queried_replaceable[0].content, newer_replaceable.content);

    // Ensure other scopes are not affected
    let other_scope_events = db
        .query_from_view(
            Some("other_replaceable_scope"),
            Filter::new().kind(Kind::Metadata),
        )
        .await;
    assert_eq!(other_scope_events.len(), 0);
}

#[tokio::test]
async fn test_scoped_parameterized_replaceable_events() {
    let db = TempDatabase::new();
    let keys = Keys::generate();
    let scope_name = "param_replaceable_scope";
    let d_tag_value1 = "param1";
    let d_tag_value2 = "param2";

    // Kind 30002: Parameterized Replaceable Event
    let event1_param1 = EventBuilder::new(
        Kind::Custom(30002), // Corrected: Use Kind::Custom for non-predefined addressable kinds
        "content_param1_v1",
    )
    .custom_created_at(Timestamp::from(5000))
    .tag(Tag::identifier(d_tag_value1.to_string())) // Corrected: Use .tag()
    .sign_with_keys(&keys)
    .unwrap();
    db.save_event_in_view(Some(scope_name), &event1_param1)
        .await;

    let event2_param1_newer = EventBuilder::new(
        Kind::Custom(30002), // Corrected: Use Kind::Custom
        "content_param1_v2",
    )
    .custom_created_at(Timestamp::from(6000))
    .tag(Tag::identifier(d_tag_value1.to_string())) // Corrected: Use .tag()
    .sign_with_keys(&keys)
    .unwrap();
    db.save_event_in_view(Some(scope_name), &event2_param1_newer)
        .await;

    let event3_param2 = EventBuilder::new(
        Kind::Custom(30002), // Corrected: Use Kind::Custom
        "content_param2_v1",
    )
    .custom_created_at(Timestamp::from(7000))
    .tag(Tag::identifier(d_tag_value2.to_string())) // Corrected: Use .tag()
    .sign_with_keys(&keys)
    .unwrap();
    db.save_event_in_view(Some(scope_name), &event3_param2)
        .await;

    // Query for param1
    let queried_param1 = db
        .query_from_view(
            Some(scope_name),
            Filter::new()
                .kind(Kind::Custom(30002))
                .author(keys.public_key())
                .identifier(d_tag_value1),
        )
        .await;
    assert_eq!(queried_param1.len(), 1);
    assert_eq!(queried_param1[0].id, event2_param1_newer.id);
    assert_eq!(queried_param1[0].content, event2_param1_newer.content);

    // Query for param2
    let queried_param2 = db
        .query_from_view(
            Some(scope_name),
            Filter::new()
                .kind(Kind::Custom(30002))
                .author(keys.public_key())
                .identifier(d_tag_value2),
        )
        .await;
    assert_eq!(queried_param2.len(), 1);
    assert_eq!(queried_param2[0].id, event3_param2.id);

    // Query all for kind 30002 in scope
    let all_param_events = db
        .query_from_view(
            Some(scope_name),
            Filter::new()
                .kind(Kind::Custom(30002))
                .author(keys.public_key()),
        )
        .await;
    assert_eq!(all_param_events.len(), 2); // Should have both newest for param1 and the one for param2

    // Ensure other scopes are not affected
    let other_scope_events = db
        .query_from_view(
            Some("other_param_scope"),
            Filter::new().kind(Kind::Custom(30002)),
        )
        .await;
    assert_eq!(other_scope_events.len(), 0);
}

#[tokio::test]
async fn test_scoped_event_deletion_kind_5() {
    let db = TempDatabase::new();
    let keys = Keys::generate();
    let scope1_name = "deletion_scope1";
    let scope2_name = "deletion_scope2";

    let event_to_delete_s1 = create_test_text_note("delete_me_s1", &keys);
    let other_event_s1 = create_test_text_note("dont_delete_me_s1", &keys);
    let event_to_delete_s2 = create_test_text_note("delete_me_s2", &keys); // Same content, different scope

    db.save_event_in_view(Some(scope1_name), &event_to_delete_s1)
        .await;
    db.save_event_in_view(Some(scope1_name), &other_event_s1)
        .await;
    db.save_event_in_view(Some(scope2_name), &event_to_delete_s2)
        .await;

    // Create and save deletion event for event_to_delete_s1 in scope1
    let deletion_request_s1 = EventDeletionRequest::new().id(event_to_delete_s1.id);
    let deletion_event_s1 = EventBuilder::delete(deletion_request_s1)
        .sign_with_keys(&keys)
        .unwrap();
    db.save_event_in_view(Some(scope1_name), &deletion_event_s1)
        .await;

    // Check scope1
    let s1_events_after_delete = db.query_from_view(Some(scope1_name), Filter::new()).await;
    println!(
        "Events after deletion: {:#?}",
        s1_events_after_delete
            .iter()
            .map(|e| (e.id, e.kind))
            .collect::<Vec<_>>()
    );
    assert_eq!(s1_events_after_delete.len(), 2); // Deletion event + other_event_s1
    let found_other_event = s1_events_after_delete
        .iter()
        .find(|e| e.id == other_event_s1.id);
    assert!(found_other_event.is_some());
    let found_deleted_event = s1_events_after_delete
        .iter()
        .find(|e| e.id == event_to_delete_s1.id);
    assert!(found_deleted_event.is_none()); // The event itself should be gone
    let found_deletion_event = s1_events_after_delete
        .iter()
        .find(|e| e.id == deletion_event_s1.id);
    assert!(found_deletion_event.is_some()); // The deletion event should be present

    // Check scope2 (event_to_delete_s2 should still be there)
    let s2_events = db.query_from_view(Some(scope2_name), Filter::new()).await;
    assert_eq!(s2_events.len(), 1);
    assert_eq!(s2_events[0].id, event_to_delete_s2.id);

    // Check unscoped (should be empty)
    let unscoped_events = db.query_from_view(None, Filter::new()).await;
    assert_eq!(unscoped_events.len(), 0);
}

#[tokio::test]
async fn test_scoped_unscoped_deletion_interactions() {
    let db = TempDatabase::new();
    let keys = Keys::generate();
    let scope_name = "interaction_scope";

    let event_s1 = create_test_text_note("event_s1_interaction", &keys);
    let event_unscoped = create_test_text_note("event_unscoped_interaction", &keys);

    db.save_event_in_view(Some(scope_name), &event_s1).await;
    db.save_event(&event_unscoped).await.unwrap(); // Save unscoped

    // 1. Scoped deletion should not affect unscoped data
    db.delete_from_view(Some(scope_name), Filter::new().id(event_s1.id))
        .await;
    let unscoped_after_scoped_del = db.query_from_view(None, Filter::new()).await;
    assert_eq!(unscoped_after_scoped_del.len(), 1);
    assert_eq!(unscoped_after_scoped_del[0].id, event_unscoped.id);
    let scoped_after_scoped_del = db.query_from_view(Some(scope_name), Filter::new()).await;
    assert_eq!(scoped_after_scoped_del.len(), 0);

    // Re-add event_s1 for next test
    db.save_event_in_view(Some(scope_name), &event_s1).await;

    // Sleep to ensure event_s1 is committed before the next operations
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // 2. Unscoped deletion should not affect scoped data
    db.delete_from_view(None, Filter::new().id(event_unscoped.id))
        .await;
    let scoped_after_unscoped_del = db.query_from_view(Some(scope_name), Filter::new()).await;
    assert_eq!(scoped_after_unscoped_del.len(), 1);
    assert_eq!(scoped_after_unscoped_del[0].id, event_s1.id);
    let unscoped_after_unscoped_del = db.query_from_view(None, Filter::new()).await;
    assert_eq!(unscoped_after_unscoped_del.len(), 0);

    // 3. Kind 5 deletion in a scope should not affect unscoped data
    db.save_event(&event_unscoped).await.unwrap(); // Re-add unscoped

    // Sleep to ensure event_unscoped is committed before the deletion event
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let deletion_request_for_s1 = EventDeletionRequest::new().id(event_s1.id);
    let deletion_event_for_s1 = EventBuilder::delete(deletion_request_for_s1)
        .sign_with_keys(&keys)
        .unwrap();
    db.save_event_in_view(Some(scope_name), &deletion_event_for_s1)
        .await;

    // Sleep to ensure the deletion is processed before querying
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let unscoped_after_scoped_kind5_del = db.query_from_view(None, Filter::new()).await;
    assert_eq!(unscoped_after_scoped_kind5_del.len(), 1); // event_unscoped should remain
    assert_eq!(unscoped_after_scoped_kind5_del[0].id, event_unscoped.id);
    let scoped_after_scoped_kind5_del = db
        .query_from_view(Some(scope_name), Filter::new().id(event_s1.id))
        .await;
    assert_eq!(scoped_after_scoped_kind5_del.len(), 0); // event_s1 should be deleted

    // 4. Kind 5 deletion in unscoped should not affect scoped data
    // Create a new event_s1 since the original was deleted
    let event_s1_new = create_test_text_note("event_s1_new_after_delete", &keys);
    db.save_event_in_view(Some(scope_name), &event_s1_new).await;

    // Sleep to ensure event_s1_new is committed before the deletion event
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let deletion_request_for_unscoped = EventDeletionRequest::new().id(event_unscoped.id);
    let deletion_event_for_unscoped = EventBuilder::delete(deletion_request_for_unscoped)
        .sign_with_keys(&keys)
        .unwrap();
    db.save_event(&deletion_event_for_unscoped).await.unwrap(); // Save Kind 5 unscoped

    // Sleep to ensure the deletion is processed before querying
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let scoped_after_unscoped_kind5_del = db.query_from_view(Some(scope_name), Filter::new()).await;
    // event_s1_new + deletion event for the original event_s1
    let mut found_event_s1_new = false;
    for ev in scoped_after_unscoped_kind5_del.iter() {
        if ev.id == event_s1_new.id {
            found_event_s1_new = true;
            break;
        }
    }
    assert!(
        found_event_s1_new,
        "event_s1_new should still exist in scope"
    );

    let unscoped_after_unscoped_kind5_del = db
        .query_from_view(None, Filter::new().id(event_unscoped.id))
        .await;
    assert_eq!(
        unscoped_after_unscoped_kind5_del.len(),
        0,
        "event_unscoped should be deleted from unscoped view"
    );
}

#[tokio::test]
async fn test_simple_deletion() {
    let db = TempDatabase::new();

    let keys = Keys::generate();

    // Create a simple event
    let event1 = EventBuilder::text_note("Hello world")
        .sign_with_keys(&keys)
        .unwrap();

    // Save the event
    let status = db.save_event(&event1).await.unwrap();
    println!("Save status: {:?}", status);

    // Query to confirm it's there
    let events = db.query(Filter::new()).await.unwrap();
    println!("Events after save: {}", events.len());
    assert_eq!(events.len(), 1);

    // Create a deletion event
    let deletion = EventBuilder::new(Kind::EventDeletion, "")
        .tag(Tag::event(event1.id))
        .sign_with_keys(&keys)
        .unwrap();

    // Save the deletion
    let del_status = db.save_event(&deletion).await.unwrap();
    println!("Deletion save status: {:?}", del_status);

    // Query again - the original event should be gone
    let events_after = db.query(Filter::new()).await.unwrap();
    println!("Events after deletion: {}", events_after.len());
    let events_vec: Vec<Event> = events_after.into_iter().collect();
    for event in &events_vec {
        println!("Event: {:?} Kind: {:?}", event.id, event.kind);
    }

    // We should only have the deletion event, not the original
    assert_eq!(events_vec.len(), 1);
    assert_eq!(events_vec[0].kind, Kind::EventDeletion);
}

#[tokio::test]
async fn test_complex_deletion_debug() {
    let db = TempDatabase::new();

    // Test data from the failing test
    let event_json = r#"{"id":"90a761aec9b5b60b399a76826141f529db17466deac85696a17e4a243aa271f9","pubkey":"aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4","created_at":1704645606,"kind":0,"tags":[],"content":"{\"name\":\"key-a\",\"display_name\":\"Key A\",\"lud16\":\"keya@ln.address\"}","sig":"ec8f49d4c722b7ccae102d49befff08e62db775e5da43ef51b25c47dfdd6a09dc7519310a3a63cbdb6ec6b3250e6f19518eb47be604edeb598d16cdc071d3dbc"}"#;
    let deletion_json = r#"{"id":"999e3e270100d7e1eaa98fcfab4a98274872c1f2dfdab024f32e42a5a12d5b5e","pubkey":"aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4","created_at":1704646606,"kind":5,"tags":[["e","90a761aec9b5b60b399a76826141f529db17466deac85696a17e4a243aa271f9"]],"content":"","sig":"4f3a33fd52784cea7ca8428fd35d94d65049712e9aa11a70b1a16a1fcd761c7b7e27afac325728b1c00dfa11e33e78b2efd0430a7e4b28f4ede5b579b3f32614"}"#;

    let event = Event::from_json(event_json).unwrap();
    let deletion = Event::from_json(deletion_json).unwrap();

    println!("Event to be deleted: {:?}", event.id);
    println!("Deletion event: {:?}", deletion.id);
    println!(
        "Deletion targets: {:?}",
        deletion.tags.event_ids().collect::<Vec<_>>()
    );

    // Save the original event
    let save_status = db.save_event(&event).await.unwrap();
    println!("Save status: {:?}", save_status);

    // Query to confirm it's there
    let events_before = db.query(Filter::new()).await.unwrap();
    println!("Events before deletion: {}", events_before.len());

    // Save the deletion event
    let del_status = db.save_event(&deletion).await.unwrap();
    println!("Deletion save status: {:?}", del_status);

    // Query again
    let events_after = db.query(Filter::new()).await.unwrap();
    println!("Events after deletion: {}", events_after.len());
    let events_vec: Vec<Event> = events_after.into_iter().collect();
    for event in &events_vec {
        println!("Event: {:?} Kind: {:?}", event.id, event.kind);
    }

    // The original event should be gone
    assert!(
        !events_vec.iter().any(|e| e.id == event.id),
        "Original event should be deleted"
    );
}

#[tokio::test]
async fn test_list_scopes() {
    let db = TempDatabase::new();
    let keys = Keys::generate();

    // Create test events
    let event_scope1 = create_test_text_note("event in scope1", &keys);
    let event_scope2 = create_test_text_note("event in scope2", &keys);
    let event_scope3 = create_test_text_note("event in scope3", &keys);
    let event_unscoped = create_test_text_note("event in default scope", &keys);

    // Save events in different scopes
    db.save_event_in_view(Some("scope1"), &event_scope1).await;
    db.save_event_in_view(Some("scope2"), &event_scope2).await;
    db.save_event_in_view(Some("scope3"), &event_scope3).await;
    db.save_event_in_view(None, &event_unscoped).await;

    // Verify each scope has the correct events
    assert_eq!(
        db.query_from_view(Some("scope1"), Filter::new())
            .await
            .len(),
        1
    );
    assert_eq!(
        db.query_from_view(Some("scope2"), Filter::new())
            .await
            .len(),
        1
    );
    assert_eq!(
        db.query_from_view(Some("scope3"), Filter::new())
            .await
            .len(),
        1
    );
    assert_eq!(db.query_from_view(None, Filter::new()).await.len(), 1);

    // Get list of scopes and verify
    let scopes = db.db.list_scopes().expect("Failed to list scopes");

    // Convert scope names to a set for easier comparison
    let scope_names: Vec<Option<&str>> = scopes.iter().map(|s| s.name()).collect();

    // Verify all expected scopes are present
    assert!(
        scope_names.contains(&Some("scope1")),
        "scope1 should be in the list"
    );
    assert!(
        scope_names.contains(&Some("scope2")),
        "scope2 should be in the list"
    );
    assert!(
        scope_names.contains(&Some("scope3")),
        "scope3 should be in the list"
    );
    assert!(
        scope_names.contains(&None),
        "Default scope should be in the list"
    );

    // Verify the total count is correct (should be 4: 3 named scopes + default scope)
    assert_eq!(scopes.len(), 4, "Should have exactly 4 scopes");

    // Verify there's exactly one Default scope
    let default_scope_count = scopes.iter().filter(|s| s.is_default()).count();
    assert_eq!(
        default_scope_count, 1,
        "Should have exactly one Default scope"
    );
}
