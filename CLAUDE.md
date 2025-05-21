# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

### Build and Run
- `cargo build` - Build the workspace in debug mode
- `cargo build --release` - Build in release mode
- `cargo run -p nostr-cli` - Run the CLI app
- `just cli` - Build the CLI in release mode
- `cargo run --example <example_name> --features <features>` - Run a specific example with required features

### Testing
- `cargo test` - Run all tests
- `cargo test -p <crate_name>` - Run tests for a specific crate
- `cargo test <test_name>` - Run a specific test
- `cargo test -- --nocapture` - Run tests with println output visible
- `cargo bench` - Run benchmarks (requires nightly)

### Linting and Formatting
- `just fmt` - Format the entire Rust code
- `just check-fmt` - Check if the Rust code is formatted
- `just precommit` - Execute a partial check before committing
- `just check` - Execute a full check including MSRV
- `just check-docs` - Check Rust docs
- `just check-deny` - Check dependency issues
- `just check-crates` - Check all crates
- `just check-crates-msrv` - Check MSRV of all crates

### Dependency Management
- `just dup` - Check duplicate dependencies
- `cargo tree -d` - Check for duplicate dependencies

### Cleaning
- `just clean` - Remove artifacts that cargo has generated

## Project Architecture

The project is organized as a workspace with multiple crates in the `crates/` directory:

### Core Components
1. **nostr** - Rust implementation of Nostr protocol
   - No_std compatible
   - Feature-flagged NIP implementations
   - Core types: Event, Keys, Filter, etc.

2. **nostr-sdk** - High-level client library
   - Builds on top of other crates
   - Provides simplified API for application development

3. **nostr-relay-pool** - Manages connections to multiple relays
   - Connection handling and state management
   - Message routing and distribution
   - Rate limiting and reconnection logic

4. **nostr-database** - Storage backends for Nostr events
   - LMDB: `nostr-lmdb`
   - NostrDB: `nostr-ndb`
   - IndexedDB: `nostr-indexeddb` (for web/WASM)

5. **nostr-connect** - Implements NIP-46 (Nostr Connect)

6. **nostr-keyring** - Key management utilities

7. **nwc** - Nostr Wallet Connect client

8. **nostr-cli** - Command-line interface

### Feature Flag System
The project makes extensive use of feature flags, especially in the core `nostr` crate:
- `std`/`alloc` - Controls standard library vs no_std usage
- Per-NIP features (e.g., `nip04`, `nip05`, etc.) - Enable specific protocol extensions
- `all-nips` - Convenience feature to enable all NIPs

## Code Organization Patterns

1. **Crate Structure**
   - `lib.rs` - Main entry point with feature gate documentation
   - Focused modules with clear responsibilities
   - `prelude.rs` - Common imports for users of the crate

2. **Error Handling**
   - Dedicated error types per component
   - Error enums with variants for different failure modes

3. **Builder Pattern**
   - Used for complex object construction (e.g., `EventBuilder`)

4. **Testing**
   - Unit tests alongside implementation code
   - Examples serve as integration tests
   - Examples are gated by required features

## Commit Guidelines

When working with this codebase, follow these commit message conventions:

```
<context>: <short description>

<description explaining reasons for the changes>
```

Where `context` is:
- `nostr` for the core protocol crate
- `sdk`, `cli`, `pool`, `connect`, `nwc`, etc. for other crates (without the `nostr-` prefix)
- `ffi`, `js` for language bindings
- `test`, `doc`, `contrib`, `ci`, `refactor`, or `book` for other changes

Issue references should use:
- `Closes <url>` or `Fixes <url>`

## Code Style Guidelines

- Use `where` clauses for trait bounds
- Use `Self` type name when possible
- Derive standard traits (`Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`) when applicable
- Use full paths for logging (`tracing::info!` not `info!`)
- Prefer `to_string()` or `to_owned()` for string conversions
- Use `match` instead of `if let ... else`
- Place module code in separate files, not inline
- Follow the project's import ordering convention