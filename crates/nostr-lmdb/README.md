# Nostr LMDB

LMDB storage backend for nostr apps with multi-tenant scoped database access.

## Features

- **Scoped Database Access**: Isolate data for different tenants/applications within a single LMDB database
- **Backward Compatible**: Existing code continues to work with the unscoped (global) database
- **Efficient Storage**: Uses LMDB's memory-mapped database for high performance
- **Full Nostr Support**: Implements all NostrDatabase traits for complete Nostr protocol support

## Scoped Database Access

NostrLMDB now supports scoped database access, allowing multiple tenants to store and retrieve data in isolation:

```rust
use nostr_lmdb::{NostrLMDB, NostrEventsDatabaseScoped};

// Create scoped views for different tenants
let tenant_a = db.scoped("tenant_a")?;
let tenant_b = db.scoped("tenant_b")?;

// Data saved in one scope is isolated from others
tenant_a.save_event(&event).await?;
let events = tenant_a.query(vec![filter]).await?; // Only sees tenant_a events

// Access unscoped (legacy) data
let global = db.unscoped()?;
let events = global.query(vec![filter]).await?; // Only sees unscoped events
```

### The ScopedView Pattern

The scoped functionality is implemented using:
- `scoped_heed::ScopedBytesDatabase` for each logical table
- Scopes implemented as key prefixes for complete isolation
- Custom Nostr codecs for efficient serialization within scoped contexts
- Zero-copy access to scoped data

## Examples

See the `examples/` directory for complete working examples:
- `scoped_access.rs` - Basic usage of scoped database access
- `scoped_implementation.rs` - Implementation details and pattern explanation
- `backwards_compatibility.rs` - Demonstrates backwards compatibility with legacy API

Run examples with:
```bash
cargo run --example scoped_access
cargo run --example scoped_implementation
cargo run --example backwards_compatibility
```

## State

**This library is in an ALPHA state**, things that are implemented generally work but the API will change in breaking ways.

## Donations

`rust-nostr` is free and open-source. This means we do not earn any revenue by selling it. Instead, we rely on your financial support. If you actively use any of the `rust-nostr` libs/software/services, then please [donate](https://rust-nostr.org/donate).

## License

This project is distributed under the MIT software license - see the [LICENSE](../../LICENSE) file for details
