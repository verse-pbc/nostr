use nostr_lmdb::nostr::{Event, JsonUtil};

// Check what the coordinate value looks like
const EVENT_7: &str = r#"{"id":"63dc49a8f3278a2de8dc0138939de56d392b8eb7a18c627e4d78789e2b0b09f2","pubkey":"79dff8f82963424e0bb02708a22e44b4980893e3a4be0fa3cb60a43b946764e3","created_at":1704644616,"kind":5,"tags":[["a","32122:aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4:"]],"content":"","sig":"977e54e5d57d1fbb83615d3a870037d9eb5182a679ca8357523bbf032580689cf481f76c88c7027034cfaf567ba9d9fe25fc8cd334139a0117ad5cf9fe325eef"}"#;
const EVENT_8: &str = r#"{"id":"6975ace0f3d66967f330d4758fbbf45517d41130e2639b54ca5142f37757c9eb","pubkey":"aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4","created_at":1704644621,"kind":5,"tags":[["a","32122:aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4:id-2"]],"content":"","sig":"9bb09e4759899d86e447c3fa1be83905fe2eda74a5068a909965ac14fcdabaed64edaeb732154dab734ca41f2fc4d63687870e6f8e56e3d9e180e4a2dd6fb2d2"}"#;

#[test]
fn debug_coordinate_values() {
    let event_7 = Event::from_json(EVENT_7).unwrap();
    let event_8 = Event::from_json(EVENT_8).unwrap();

    println!("Event 7 tags:");
    for tag in event_7.tags.iter() {
        let values = tag.as_slice();
        println!("  Tag: {:?}", values);
        if values.len() >= 2 && values[0] == "a" {
            println!("  Coordinate value: '{}'", values[1]);
        }
    }

    println!("\nEvent 8 tags:");
    for tag in event_8.tags.iter() {
        let values = tag.as_slice();
        println!("  Tag: {:?}", values);
        if values.len() >= 2 && values[0] == "a" {
            println!("  Coordinate value: '{}'", values[1]);
        }
    }

    println!("\nEvent 8 coordinates:");
    for coord in event_8.tags.coordinates() {
        println!(
            "  Coordinate: kind={}, pubkey={}, identifier={:?}",
            coord.kind, coord.public_key, coord.identifier
        );
    }
}
