use nostr_lmdb::nostr::nips::nip01::Coordinate;
use nostr_lmdb::nostr::{Event, Filter, JsonUtil};
use nostr_lmdb::{NostrEventsDatabase, NostrLMDB};
use tempfile::TempDir;

// Event 3: has d tag ["d", "id-2"]
const EVENT_3: &str = r#"{"id":"63b8b829aa31a2de870c3a713541658fcc0187be93af2032ec2ca039befd3f70","pubkey":"aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4","created_at":1704644596,"kind":32122,"tags":[["d","id-2"]],"content":"","sig":"607b1a67bef57e48d17df4e145718d10b9df51831d1272c149f2ab5ad4993ae723f10a81be2403ae21b2793c8ed4c129e8b031e8b240c6c90c9e6d32f62d26ff"}"#;

#[tokio::test]
async fn test_coordinate_indexing() {
    let temp_dir = TempDir::new().unwrap();
    let db = NostrLMDB::open(&temp_dir).unwrap();

    let event_3 = Event::from_json(EVENT_3).unwrap();

    // Print the actual d tag value
    for tag in event_3.tags.iter() {
        let values = tag.as_slice();
        if values.len() >= 2 && values[0] == "d" {
            println!("Event 3 d tag value: '{}'", values[1]);
        }
    }

    // Check the coordinate
    if let Some(coord) = event_3.coordinate() {
        println!(
            "Event 3 coordinate: kind={}, pubkey={}, identifier={:?}",
            coord.kind, coord.public_key, coord.identifier
        );
    }

    // Save Event 3
    let status_3 = db.save_event(&event_3).await.unwrap();
    println!("Save status: {:?}", status_3);

    // Try to query by coordinate
    use nostr_lmdb::nostr::Kind;
    let query_coord = Coordinate::new(Kind::Custom(32122), event_3.pubkey).identifier("id-2");
    let coord_filter = Filter::from(query_coord.clone());
    let coord_query = db.query(coord_filter).await.unwrap();
    println!("Query by coordinate found {} events", coord_query.len());

    for event in coord_query.iter() {
        println!(
            "  Found: id={}, d-tag: {:?}",
            event.id,
            event
                .tags
                .iter()
                .find(|tag| tag.as_slice().len() >= 2 && tag.as_slice()[0] == "d")
                .map(|tag| &tag.as_slice()[1])
        );
    }

    // Also test the direct filter creation as the deletion code does
    let tag_value_string = format!(
        "{}:{}:{}",
        Into::<u16>::into(query_coord.kind),
        query_coord.public_key,
        &query_coord.identifier
    );
    println!("Coordinate string used in deletion: '{}'", tag_value_string);
}
