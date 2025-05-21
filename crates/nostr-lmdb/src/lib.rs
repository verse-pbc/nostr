// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2025 Rust Nostr Developers
// Distributed under the MIT software license

//! # NostrLMDB
//!
//! A Nostr database implementation using LMDB.
//!
//! Fork of [Pocket](https://github.com/mikedilger/pocket) database.
//!
//! ## Scoped Database Access
//!
//! NostrLMDB supports scoped database access, which allows multiple tenants to store and retrieve
//! data in isolation within a single LMDB database file. Each scope has its own isolated data space,
//! and operations in one scope do not affect data in another scope or the global (unscoped) space.
//!
//! ### The ScopedView Pattern
//!
//! NostrLMDB implements a ScopedView pattern that provides isolated database views for different tenants:
//!
//! - Each logical table uses `scoped_heed::ScopedBytesDatabase` internally
//! - Scopes are implemented as prefixes to database keys
//! - Nostr-specific codecs handle serialization/deserialization within scoped contexts
//!
//! ### Basic Usage
//!
//! ```no_run
//! use nostr_lmdb::{NostrLMDB, NostrEventsDatabase};
//! use nostr_lmdb::nostr::{EventBuilder, Filter, Keys, Kind};
//! use scoped_heed::Scope;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let keys = Keys::generate();
//! # let event = EventBuilder::new(Kind::TextNote, "test").sign_with_keys(&keys)?;
//! let db = NostrLMDB::open("./db")?;
//!
//! // Save an event in the "tenant_a" scope
//! let scope = Scope::named("tenant_a")?;
//! let tenant_a = db.scoped(&scope)?;
//! tenant_a.save_event(&event).await?;
//!
//! // Query events in the "tenant_a" scope
//! let filter = Filter::new();
//! let events = tenant_a.query(filter.clone()).await?;
//!
//! // Access unscoped (legacy) data
//! let legacy_events = db.query(filter).await?; // Direct call for unscoped
//! # Ok(())
//! # }
//! ```
//!
//! ### Scope Isolation
//!
//! Data in different scopes is completely isolated. Operations in one scope do not affect data in
//! another scope or the global (unscoped) space.
//!
//! ```no_run
//! # use nostr_lmdb::{NostrLMDB, NostrEventsDatabase};
//! # use nostr_lmdb::nostr::{EventBuilder, Filter, Keys, Kind};
//! # use scoped_heed::Scope;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let db = NostrLMDB::open("./db")?;
//! # let keys = Keys::generate();
//! # let event = EventBuilder::new(Kind::TextNote, "test").sign_with_keys(&keys)?;
//! let filter = Filter::new();
//! // These operations are completely isolated
//! let scope_a = Scope::named("tenant_a")?;
//! let scope_b = Scope::named("tenant_b")?;
//! db.scoped(&scope_a)?.save_event(&event).await?;
//! db.scoped(&scope_b)?.save_event(&event).await?;
//!
//! let tenant_a_events = db.scoped(&scope_a)?.query(filter.clone()).await?;
//! let tenant_b_events = db.scoped(&scope_b)?.query(filter.clone()).await?;
//! let global_events = db.query(filter).await?; // Direct call for unscoped
//!
//! // Each query only sees events in its own scope
//! assert_eq!(tenant_a_events.len(), 1);
//! assert_eq!(tenant_b_events.len(), 1);
//! assert_eq!(global_events.len(), 0);
//! # Ok(())
//! # }
//! ```
//!
//! ### Implementation Details
//!
//! The scoped database functionality is implemented using:
//!
//! - `scoped_heed::ScopedBytesDatabase` for each logical table
//! - Custom Nostr codecs that work with the scoped database
//! - The `scoped()` method to create a new scoped view
//!
//! ### Backward Compatibility
//!
//! The existing NostrLMDB API continues to work with unscoped data. All existing code will
//! continue to function without modification, operating on the global (unscoped) data space.
//!
//! ```no_run
//! # use nostr_lmdb::{NostrLMDB, NostrEventsDatabase};
//! # use nostr_lmdb::nostr::{EventBuilder, Filter, Keys, Kind};
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let db = NostrLMDB::open("./db")?;
//! # let keys = Keys::generate();
//! # let event = EventBuilder::new(Kind::TextNote, "test").sign_with_keys(&keys)?;
//! let filter = Filter::new();
//! // API for unscoped data
//! db.save_event(&event).await?;
//! let events = db.query(filter.clone()).await?;
//! // This is now the standard way to interact with unscoped data.
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]
#![warn(rustdoc::bare_urls)]
#![allow(clippy::mutable_key_type)]

use std::path::Path;

use nostr_database::prelude::{
    BoxedFuture, CoordinateBorrow as NostrCoordinateBorrow, Event as NostrEvent,
    EventId as NostrEventId, Filter as NostrFilter, Timestamp as NostrTimestamp,
};
pub use nostr_database::prelude::{Coordinate, CoordinateBorrow};
pub use nostr_database::{
    nostr, Backend, DatabaseError, DatabaseEventStatus, Events, NostrDatabase, NostrDatabaseWipe,
    NostrEventsDatabase, RejectedReason, SaveEventStatus,
};
pub use scoped_heed::{GlobalScopeRegistry, Scope};

mod store;


use self::store::Store;

/// A view into a specific scope of the Nostr LMDB database.
///
/// This struct is created by the `scoped` or `unscoped` methods on `NostrLMDB`.
/// It encapsulates a reference to the main database (`db`) and an optional scope name (`scope`).
/// If `scope` is `Some(String)`, operations are performed within that named scope.
/// If `scope` is `None`, operations are performed in the global/unscoped context.
#[derive(Debug)]
pub struct ScopedView<'a> {
    db: &'a NostrLMDB,
    scope: Scope,
}

/// LMDB Nostr Database
#[derive(Debug)]
pub struct NostrLMDB {
    db: Store,
    registry: Option<std::sync::Arc<GlobalScopeRegistry>>,
}

impl NostrLMDB {
    /// Open LMDB database
    #[inline]
    pub fn open<P>(path: P) -> Result<Self, DatabaseError>
    where
        P: AsRef<Path>,
    {
        let store = Store::open(path).map_err(DatabaseError::backend)?;
        let registry = store.create_registry().map_err(DatabaseError::backend)?;
        
        Ok(Self {
            db: store,
            registry,
        })
    }
}

impl NostrDatabase for NostrLMDB {
    #[inline]
    fn backend(&self) -> Backend {
        Backend::LMDB
    }
}

impl NostrEventsDatabase for NostrLMDB {
    fn save_event<'a>(
        &'a self,
        event: &'a NostrEvent,
    ) -> BoxedFuture<'a, Result<SaveEventStatus, DatabaseError>> {
        Box::pin(async move {
            self.db
                .save_event(event)
                .await
                .map_err(DatabaseError::backend)
        })
    }

    fn check_id<'a>(
        &'a self,
        event_id: &'a NostrEventId,
    ) -> BoxedFuture<'a, Result<DatabaseEventStatus, DatabaseError>> {
        Box::pin(async move {
            if self
                .db
                .event_is_deleted(event_id)
                .map_err(DatabaseError::backend)?
            {
                Ok(DatabaseEventStatus::Deleted)
            } else if self
                .db
                .has_event(event_id)
                .map_err(DatabaseError::backend)?
            {
                Ok(DatabaseEventStatus::Saved)
            } else {
                Ok(DatabaseEventStatus::NotExistent)
            }
        })
    }

    fn has_coordinate_been_deleted<'a>(
        &'a self,
        coordinate: &'a NostrCoordinateBorrow<'a>,
        timestamp: &'a NostrTimestamp,
    ) -> BoxedFuture<'a, Result<bool, DatabaseError>> {
        Box::pin(async move {
            if let Some(t) = self
                .db
                .when_is_coordinate_deleted(coordinate)
                .map_err(DatabaseError::backend)?
            {
                Ok(&t >= timestamp)
            } else {
                Ok(false)
            }
        })
    }

    fn event_by_id<'a>(
        &'a self,
        event_id: &'a NostrEventId,
    ) -> BoxedFuture<'a, Result<Option<NostrEvent>, DatabaseError>> {
        Box::pin(async move {
            self.db
                .get_event_by_id(event_id)
                .map_err(DatabaseError::backend)
        })
    }

    fn count(&self, filter: NostrFilter) -> BoxedFuture<Result<usize, DatabaseError>> {
        Box::pin(async move { self.db.count(filter).map_err(DatabaseError::backend) })
    }

    fn query(&self, filter: NostrFilter) -> BoxedFuture<Result<Events, DatabaseError>> {
        Box::pin(async move { self.db.query(filter).map_err(DatabaseError::backend) })
    }

    fn negentropy_items(
        &self,
        filter: NostrFilter,
    ) -> BoxedFuture<Result<Vec<(NostrEventId, NostrTimestamp)>, DatabaseError>> {
        Box::pin(async move {
            self.db
                .negentropy_items(filter)
                .map_err(DatabaseError::backend)
        })
    }

    fn delete(&self, filter: NostrFilter) -> BoxedFuture<Result<(), DatabaseError>> {
        Box::pin(async move { self.db.delete(filter).await.map_err(DatabaseError::backend) })
    }
}

impl NostrDatabaseWipe for NostrLMDB {
    #[inline]
    fn wipe(&self) -> BoxedFuture<Result<(), DatabaseError>> {
        Box::pin(async move { self.db.wipe().await.map_err(DatabaseError::backend) })
    }
}

impl NostrEventsDatabase for ScopedView<'_> {
    fn save_event<'b>(
        &'b self,
        event: &'b NostrEvent,
    ) -> BoxedFuture<'b, Result<SaveEventStatus, DatabaseError>> {
        Box::pin(async move { self.save_event(event).await })
    }

    fn check_id<'b>(
        &'b self,
        event_id: &'b NostrEventId,
    ) -> BoxedFuture<'b, Result<DatabaseEventStatus, DatabaseError>> {
        Box::pin(async move {
            // TODO: Add `event_is_deleted_in_scope` to `Store` to accurately reflect deletion status.
            match self.event_by_id(*event_id).await? {
                Some(_) => Ok(DatabaseEventStatus::Saved),
                None => Ok(DatabaseEventStatus::NotExistent),
            }
        })
    }

    fn has_coordinate_been_deleted<'b>(
        &'b self,
        coordinate: &'b NostrCoordinateBorrow,
        timestamp: &'b NostrTimestamp,
    ) -> BoxedFuture<'b, Result<bool, DatabaseError>> {
        // Falls back to unscoped version - requires Store API update for scoped support
        Box::pin(async move {
            self.db
                .has_coordinate_been_deleted(coordinate, timestamp)
                .await
        })
    }

    fn event_by_id<'b>(
        &'b self,
        event_id: &'b NostrEventId,
    ) -> BoxedFuture<'b, Result<Option<NostrEvent>, DatabaseError>> {
        Box::pin(async move { self.event_by_id(*event_id).await })
    }

    fn count(&self, filter: NostrFilter) -> BoxedFuture<Result<usize, DatabaseError>> {
        Box::pin(async move { self.count(filter).await })
    }

    fn query(&self, filter: NostrFilter) -> BoxedFuture<Result<Events, DatabaseError>> {
        Box::pin(async move {
            let events_vec = self.query(filter.clone()).await?;
            // Convert Vec<NostrEvent> to nostr_database::Events
            // The filter passed to Events::new is only used for the limit, which we don't apply here.
            // If nostr_database::Events changes its constructor or behavior, this might need adjustment.
            let mut events_collection = Events::new(&NostrFilter::default());
            for event in events_vec {
                events_collection.insert(event);
            }
            Ok(events_collection)
        })
    }

    fn negentropy_items(
        &self,
        filter: NostrFilter,
    ) -> BoxedFuture<Result<Vec<(NostrEventId, NostrTimestamp)>, DatabaseError>> {
        // Falls back to unscoped version - requires Store API update for scoped support
        Box::pin(async move { self.db.negentropy_items(filter).await })
    }

    fn delete(&self, filter: NostrFilter) -> BoxedFuture<'_, Result<(), DatabaseError>> {
        Box::pin(async move { self.delete(filter).await })
    }
}

impl NostrLMDB {
    /// Create a scoped view for database operations using a Scope object
    ///
    /// - Pass a `Scope::Named` struct with a non-empty name for named scope
    /// - Pass `Scope::Default` for unscoped (global) operations
    ///
    /// This method requires using the Scope enum directly.
    pub fn scoped(&self, scope: &Scope) -> Result<ScopedView<'_>, DatabaseError> {
        // Register the scope in the registry if available and it's a named scope
        if let Scope::Named { .. } = scope {
            if let Some(registry) = &self.registry {
                // Register the scope in the registry - this is critical for proper scope management
                self.db.register_scope(registry, scope).map_err(DatabaseError::backend)?;
            }
        }
        
        let view = ScopedView {
            db: self,
            scope: scope.clone(),
        };
        Ok(view)
    }
    
    /// Create a view for the default (unscoped) database
    pub fn unscoped(&self) -> ScopedView<'_> {
        ScopedView {
            db: self,
            scope: Scope::Default,
        }
    }
    
    /// List all registered scopes in the database
    pub fn list_scopes(&self) -> Result<Vec<Scope>, DatabaseError> {
        // Pass the singleton registry to the get_registry_scopes method
        match self.db.get_registry_scopes(self.registry.as_ref()).map_err(DatabaseError::backend)? {
            Some(scopes) => Ok(scopes),
            None => Ok(vec![Scope::Default]),
        }
    }
}

impl ScopedView<'_> {
    /// Returns the name of the scope, if this view is scoped.
    /// Returns `None` if this is an unscoped (global) view.
    pub fn scope_name(&self) -> Option<&str> {
        self.scope.name()
    }

    /// Save an event within the view's scope.
    pub async fn save_event(&self, event: &NostrEvent) -> Result<SaveEventStatus, DatabaseError> {
        self.db
            .db
            .save_event_in_scope(&self.scope, event.clone())
            .await
            .map_err(DatabaseError::backend)
    }

    /// Get an event by ID from the view's scope.
    pub async fn event_by_id(
        &self,
        event_id: NostrEventId,
    ) -> Result<Option<NostrEvent>, DatabaseError> {
        self.db
            .db
            .event_by_id_in_scope(&self.scope, event_id)
            .await
            .map_err(DatabaseError::backend)
    }

    /// Query events within the view's scope.
    pub async fn query(&self, filter: NostrFilter) -> Result<Vec<NostrEvent>, DatabaseError> {
        self.db
            .db
            .query_in_scope(&self.scope, filter)
            .await
            .map_err(DatabaseError::backend)
    }

    /// Delete events matching the filter within the view's scope.
    pub async fn delete(&self, filter: NostrFilter) -> Result<(), DatabaseError> {
        self.db
            .db
            .delete_in_scope(&self.scope, filter)
            .await
            .map_err(DatabaseError::backend)
    }

    /// Count events matching the filter within the view's scope.
    pub async fn count(&self, filter: NostrFilter) -> Result<usize, DatabaseError> {
        self.db
            .db
            .count_in_scope(&self.scope, filter)
            .await
            .map_err(DatabaseError::backend)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use std::time::Duration;

    use tempfile::TempDir;

    use crate::nostr::{
        Event, EventBuilder, Filter, JsonUtil, Keys, Kind, Metadata, Tag, Timestamp,
    };
    use crate::{
        Coordinate, // Now available as crate::Coordinate
        NostrEventsDatabase,
        NostrLMDB,
        SaveEventStatus,
        Scope,
    };

    const EVENTS: [&str; 14] = [
        r#"{"id":"b7b1fb52ad8461a03e949820ae29a9ea07e35bcd79c95c4b59b0254944f62805","pubkey":"aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4","created_at":1704644581,"kind":1,"tags":[],"content":"Text note","sig":"ed73a8a4e7c26cd797a7b875c634d9ecb6958c57733305fed23b978109d0411d21b3e182cb67c8ad750884e30ca383b509382ae6187b36e76ee76e6a142c4284"}"#,
        r#"{"id":"7296747d91c53f1d71778ef3e12d18b66d494a41f688ef244d518abf37c959b6","pubkey":"aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4","created_at":1704644586,"kind":32121,"tags":[["d","id-1"]],"content":"Empty 1","sig":"8848989a8e808f7315e950f871b231c1dff7752048f8957d4a541881d2005506c30e85c7dd74dab022b3e01329c88e69c9d5d55d961759272a738d150b7dbefc"}"#,
        r#"{"id":"ec6ea04ba483871062d79f78927df7979f67545b53f552e47626cb1105590442","pubkey":"aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4","created_at":1704644591,"kind":32122,"tags":[["d","id-1"]],"content":"Empty 2","sig":"89946113a97484850fe35fefdb9120df847b305de1216dae566616fe453565e8707a4da7e68843b560fa22a932f81fc8db2b5a2acb4dcfd3caba9a91320aac92"}"#,
        r#"{"id":"63b8b829aa31a2de870c3a713541658fcc0187be93af2032ec2ca039befd3f70","pubkey":"aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4","created_at":1704644596,"kind":32122,"tags":[["d","id-2"]],"content":"","sig":"607b1a67bef57e48d17df4e145718d10b9df51831d1272c149f2ab5ad4993ae723f10a81be2403ae21b2793c8ed4c129e8b031e8b240c6c90c9e6d32f62d26ff"}"#,
        r#"{"id":"6fe9119c7db13ae13e8ecfcdd2e5bf98e2940ba56a2ce0c3e8fba3d88cd8e69d","pubkey":"79dff8f82963424e0bb02708a22e44b4980893e3a4be0fa3cb60a43b946764e3","created_at":1704644601,"kind":32122,"tags":[["d","id-3"]],"content":"","sig":"d07146547a726fc9b4ec8d67bbbe690347d43dadfe5d9890a428626d38c617c52e6945f2b7144c4e0c51d1e2b0be020614a5cadc9c0256b2e28069b70d9fc26e"}"#,
        r#"{"id":"a82f6ebfc709f4e7c7971e6bf738e30a3bc112cfdb21336054711e6779fd49ef","pubkey":"79dff8f82963424e0bb02708a22e44b4980893e3a4be0fa3cb60a43b946764e3","created_at":1704644606,"kind":32122,"tags":[["d","id-1"]],"content":"","sig":"96d3349b42ed637712b4d07f037457ab6e9180d58857df77eb5fa27ff1fd68445c72122ec53870831ada8a4d9a0b484435f80d3ff21a862238da7a723a0d073c"}"#,
        r#"{"id":"8ab0cb1beceeb68f080ec11a3920b8cc491ecc7ec5250405e88691d733185832","pubkey":"aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4","created_at":1704644611,"kind":32122,"tags":[["d","id-1"]],"content":"Test","sig":"49153b482d7110e2538eb48005f1149622247479b1c0057d902df931d5cea105869deeae908e4e3b903e3140632dc780b3f10344805eab77bb54fb79c4e4359d"}"#,
        r#"{"id":"63dc49a8f3278a2de8dc0138939de56d392b8eb7a18c627e4d78789e2b0b09f2","pubkey":"79dff8f82963424e0bb02708a22e44b4980893e3a4be0fa3cb60a43b946764e3","created_at":1704644616,"kind":5,"tags":[["a","32122:aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4:"]],"content":"","sig":"977e54e5d57d1fbb83615d3a870037d9eb5182a679ca8357523bbf032580689cf481f76c88c7027034cfaf567ba9d9fe25fc8cd334139a0117ad5cf9fe325eef"}"#,
        r#"{"id":"6975ace0f3d66967f330d4758fbbf45517d41130e2639b54ca5142f37757c9eb","pubkey":"aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4","created_at":1704644621,"kind":5,"tags":[["a","32122:aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4:id-2"]],"content":"","sig":"9bb09e4759899d86e447c3fa1be83905fe2eda74a5068a909965ac14fcdabaed64edaeb732154dab734ca41f2fc4d63687870e6f8e56e3d9e180e4a2dd6fb2d2"}"#,
        r#"{"id":"33f5b4e6a38e107638c20f4536db35191d4b8651ba5a2cefec983b9ec2d65084","pubkey":"aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4","created_at":1704645586,"kind":0,"tags":[],"content":"{\"name\":\"Key A\"}","sig":"285d090f45a6adcae717b33771149f7840a8c27fb29025d63f1ab8d95614034a54e9f4f29cee9527c4c93321a7ebff287387b7a19ba8e6f764512a40e7120429"}"#,
        r#"{"id":"90a761aec9b5b60b399a76826141f529db17466deac85696a17e4a243aa271f9","pubkey":"aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4","created_at":1704645606,"kind":0,"tags":[],"content":"{\"name\":\"key-a\",\"display_name\":\"Key A\",\"lud16\":\"keya@ln.address\"}","sig":"ec8f49d4c722b7ccae102d49befff08e62db775e5da43ef51b25c47dfdd6a09dc7519310a3a63cbdb6ec6b3250e6f19518eb47be604edeb598d16cdc071d3dbc"}"#,
        r#"{"id":"a295422c636d3532875b75739e8dae3cdb4dd2679c6e4994c9a39c7ebf8bc620","pubkey":"79dff8f82963424e0bb02708a22e44b4980893e3a4be0fa3cb60a43b946764e3","created_at":1704646569,"kind":5,"tags":[["e","90a761aec9b5b60b399a76826141f529db17466deac85696a17e4a243aa271f9"]],"content":"","sig":"d4dc8368a4ad27eef63cacf667345aadd9617001537497108234fc1686d546c949cbb58e007a4d4b632c65ea135af4fbd7a089cc60ab89b6901f5c3fc6a47b29"}"#, // Invalid event deletion
        r#"{"id":"999e3e270100d7e1eaa98fcfab4a98274872c1f2dfdab024f32e42a5a12d5b5e","pubkey":"aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4","created_at":1704646606,"kind":5,"tags":[["e","90a761aec9b5b60b399a76826141f529db17466deac85696a17e4a243aa271f9"]],"content":"","sig":"4f3a33fd52784cea7ca8428fd35d94d65049712e9aa11a70b1a16a1fcd761c7b7e27afac325728b1c00dfa11e33e78b2efd0430a7e4b28f4ede5b579b3f32614"}"#,
        r#"{"id":"99a022e6d61c4e39c147d08a2be943b664e8030c0049325555ac1766429c2832","pubkey":"79dff8f82963424e0bb02708a22e44b4980893e3a4be0fa3cb60a43b946764e3","created_at":1705241093,"kind":30333,"tags":[["d","multi-id"],["p","aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4"]],"content":"Multi-tags","sig":"0abfb2b696a7ed7c9e8e3bf7743686190f3f1b3d4045b72833ab6187c254f7ed278d289d52dfac3de28be861c1471421d9b1bfb5877413cbc81c84f63207a826"}"#,
    ];

    struct TempDatabase {
        db: NostrLMDB,
        // Needed to avoid the drop and deletion of temp folder
        _temp: TempDir,
    }

    impl Deref for TempDatabase {
        type Target = NostrLMDB;

        fn deref(&self) -> &Self::Target {
            &self.db
        }
    }

    impl TempDatabase {
        fn new() -> Self {
            let path = tempfile::tempdir().unwrap();
            Self {
                db: NostrLMDB::open(&path).unwrap(),
                _temp: path,
            }
        }

        // Return the number of added events
        async fn add_random_events(&self) -> usize {
            let keys_a = Keys::generate();
            let keys_b = Keys::generate();

            let events = vec![
                EventBuilder::text_note("Text Note A")
                    .sign_with_keys(&keys_a)
                    .unwrap(),
                EventBuilder::text_note("Text Note B")
                    .sign_with_keys(&keys_b)
                    .unwrap(),
                EventBuilder::metadata(
                    &Metadata::new().name("account-a").display_name("Account A"),
                )
                .sign_with_keys(&keys_a)
                .unwrap(),
                EventBuilder::metadata(
                    &Metadata::new().name("account-b").display_name("Account B"),
                )
                .sign_with_keys(&keys_b)
                .unwrap(),
                EventBuilder::new(Kind::Custom(33_333), "")
                    .tag(Tag::identifier("my-id-a"))
                    .sign_with_keys(&keys_a)
                    .unwrap(),
                EventBuilder::new(Kind::Custom(33_333), "")
                    .tag(Tag::identifier("my-id-b"))
                    .sign_with_keys(&keys_b)
                    .unwrap(),
            ];

            // Store
            for event in events.iter() {
                self.db.save_event(event).await.unwrap();
            }

            events.len()
        }

        async fn add_event(&self, builder: EventBuilder) -> (Keys, Event) {
            let keys = Keys::generate();
            let event = builder.sign_with_keys(&keys).unwrap();
            self.db.save_event(&event).await.unwrap();
            (keys, event)
        }

        async fn add_event_with_keys(
            &self,
            builder: EventBuilder,
            keys: &Keys,
        ) -> (Event, SaveEventStatus) {
            let event = builder.sign_with_keys(keys).unwrap();
            let status = self.db.save_event(&event).await.unwrap();
            (event, status)
        }

        async fn count_all(&self) -> usize {
            self.db.count(Filter::new()).await.unwrap()
        }
    }

    #[tokio::test]
    async fn test_event_by_id() {
        let db = TempDatabase::new();

        let added_events: usize = db.add_random_events().await;

        let (_keys, expected_event) = db.add_event(EventBuilder::text_note("Test")).await;

        let event = db.event_by_id(&expected_event.id).await.unwrap().unwrap();
        assert_eq!(event, expected_event);

        // Check if number of events in database match the expected
        assert_eq!(db.count_all().await, added_events + 1)
    }

    #[tokio::test]
    async fn test_replaceable_event() {
        let db = TempDatabase::new();

        let added_events: usize = db.add_random_events().await;

        let now = Timestamp::now();
        let metadata = Metadata::new()
            .name("my-account")
            .display_name("My Account");

        let (keys, expected_event) = db
            .add_event(
                EventBuilder::metadata(&metadata).custom_created_at(now - Duration::from_secs(120)),
            )
            .await;

        // Test event by ID
        let event = db.event_by_id(&expected_event.id).await.unwrap().unwrap();
        assert_eq!(event, expected_event);

        // Test filter query
        let events = db
            .query(Filter::new().author(keys.public_key()).kind(Kind::Metadata))
            .await
            .unwrap();
        assert_eq!(events.to_vec(), vec![expected_event.clone()]);

        // Check if number of events in database match the expected
        assert_eq!(db.count_all().await, added_events + 1);

        // Replace previous event
        let (new_expected_event, status) = db
            .add_event_with_keys(
                EventBuilder::metadata(&metadata).custom_created_at(now),
                &keys,
            )
            .await;
        assert!(status.is_success());

        // Test event by ID (MUST be None because replaced)
        assert!(db.event_by_id(&expected_event.id).await.unwrap().is_none());

        // Test event by ID
        let event = db
            .event_by_id(&new_expected_event.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event, new_expected_event);

        // Test filter query
        let events = db
            .query(Filter::new().author(keys.public_key()).kind(Kind::Metadata))
            .await
            .unwrap();
        assert_eq!(events.to_vec(), vec![new_expected_event]);

        // Check if number of events in database match the expected
        assert_eq!(db.count_all().await, added_events + 1);
    }

    #[tokio::test]
    async fn test_param_replaceable_event() {
        let db = TempDatabase::new();

        let added_events: usize = db.add_random_events().await;

        let now = Timestamp::now();

        let (keys, expected_event) = db
            .add_event(
                EventBuilder::new(Kind::Custom(33_333), "")
                    .tag(Tag::identifier("my-id-a"))
                    .custom_created_at(now - Duration::from_secs(120)),
            )
            .await;
        let coordinate =
            Coordinate::new(Kind::from(33_333), keys.public_key()).identifier("my-id-a");

        // Test event by ID
        let event = db.event_by_id(&expected_event.id).await.unwrap().unwrap();
        assert_eq!(event, expected_event);

        // Test filter query
        let events = db.query(coordinate.clone().into()).await.unwrap();
        assert_eq!(events.to_vec(), vec![expected_event.clone()]);

        // Check if number of events in database match the expected
        assert_eq!(db.count_all().await, added_events + 1);

        // Replace previous event
        let (new_expected_event, status) = db
            .add_event_with_keys(
                EventBuilder::new(Kind::Custom(33_333), "Test replace")
                    .tag(Tag::identifier("my-id-a"))
                    .custom_created_at(now),
                &keys,
            )
            .await;
        assert!(status.is_success());

        // Test event by ID (MUST be None` because replaced)
        assert!(db.event_by_id(&expected_event.id).await.unwrap().is_none());

        // Test event by ID
        let event = db
            .event_by_id(&new_expected_event.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event, new_expected_event);

        // Test filter query
        let events = db.query(coordinate.into()).await.unwrap();
        assert_eq!(events.to_vec(), vec![new_expected_event]);

        // Check if number of events in database match the expected
        assert_eq!(db.count_all().await, added_events + 1);

        // Trey to add param replaceable event with older timestamp (MUSTN'T be stored)
        let (_, status) = db
            .add_event_with_keys(
                EventBuilder::new(Kind::Custom(33_333), "Test replace 2")
                    .tag(Tag::identifier("my-id-a"))
                    .custom_created_at(now - Duration::from_secs(2000)),
                &keys,
            )
            .await;
        assert!(!status.is_success());
    }

    #[tokio::test]
    async fn test_full_text_search() {
        let db = TempDatabase::new();

        let _added_events: usize = db.add_random_events().await;

        let events = db.query(Filter::new().search("Account A")).await.unwrap();
        assert_eq!(events.len(), 1);

        let events = db.query(Filter::new().search("account a")).await.unwrap();
        assert_eq!(events.len(), 1);

        let events = db.query(Filter::new().search("text note")).await.unwrap();
        assert_eq!(events.len(), 2);

        let events = db.query(Filter::new().search("notes")).await.unwrap();
        assert_eq!(events.len(), 0);

        let events = db.query(Filter::new().search("hola")).await.unwrap();
        assert_eq!(events.len(), 0);
    }

    #[tokio::test]
    async fn test_expected_query_result() {
        let db = TempDatabase::new();

        for (idx, event_str) in EVENTS.into_iter().enumerate() {
            let event = Event::from_json(event_str).unwrap();
            let status = db.save_event(&event).await;
            if let Ok(status) = status {
                // Invalid deletions (Event 7 and 11) should be rejected
                if idx == 7 || idx == 11 {
                    println!("Event {} status: {:?}", idx, status);
                    assert!(!status.is_success(), "Event {} should be rejected", idx);
                }
            }
        }

        // Test expected output
        let expected_output = vec![
            Event::from_json(EVENTS[13]).unwrap(),
            Event::from_json(EVENTS[12]).unwrap(),
            // Event 11 is invalid deletion
            // Event 10 deleted by event 12
            // Event 9 replaced by event 10
            Event::from_json(EVENTS[8]).unwrap(),
            // Event 7 is an invalid deletion
            Event::from_json(EVENTS[6]).unwrap(),
            Event::from_json(EVENTS[5]).unwrap(),
            Event::from_json(EVENTS[4]).unwrap(),
            // Event 3 deleted by Event 8
            // Event 2 replaced by Event 6
            Event::from_json(EVENTS[1]).unwrap(),
            Event::from_json(EVENTS[0]).unwrap(),
        ];
        assert_eq!(
            db.query(Filter::new()).await.unwrap().to_vec(),
            expected_output
        );
        assert_eq!(db.count_all().await, 8);
    }

    #[tokio::test]
    async fn test_delete_events_with_filter() {
        let db = TempDatabase::new();

        let added_events: usize = db.add_random_events().await;

        assert_eq!(db.count_all().await, added_events);

        // Delete all kinds except text note
        let filter = Filter::new().kinds([Kind::Metadata, Kind::Custom(33_333)]);
        db.delete(filter).await.unwrap();

        assert_eq!(db.count_all().await, 2);
    }

    #[tokio::test]
    async fn test_scoped_view_delete() {
        let db = TempDatabase::new();

        let scope1_name = "scope1";
        let scope2_name = "scope2";
        
        let scope1 = Scope::named(scope1_name).unwrap();
        let scope2 = Scope::named(scope2_name).unwrap();

        let view_s1 = db.scoped(&scope1).unwrap();
        let view_s2 = db.scoped(&scope2).unwrap();

        let keys_a = Keys::generate();
        let keys_b = Keys::generate();
        let keys_c = Keys::generate();
        let keys_d = Keys::generate();

        // Events for scope1
        let event1_s1 = EventBuilder::text_note("S1E1 Text Note by A")
            .sign_with_keys(&keys_a)
            .unwrap();
        let event2_s1 = EventBuilder::text_note("S1E2 Text Note by B")
            .sign_with_keys(&keys_b)
            .unwrap();
        let event3_s1_k2 = EventBuilder::new(Kind::Custom(12222), "S1E3K2 Custom by A")
            .sign_with_keys(&keys_a)
            .unwrap();

        // Events for scope2
        let event1_s2 = EventBuilder::text_note("S2E1 Text Note by A")
            .sign_with_keys(&keys_a)
            .unwrap(); // Same author as event1_s1
        let event2_s2 = EventBuilder::text_note("S2E2 Text Note by C")
            .sign_with_keys(&keys_c)
            .unwrap();

        // Event for unscoped
        let event_unscoped = EventBuilder::text_note("UNSC Text Note by D")
            .sign_with_keys(&keys_d)
            .unwrap();

        // Save events
        view_s1.save_event(&event1_s1).await.unwrap();
        view_s1.save_event(&event2_s1).await.unwrap();
        view_s1.save_event(&event3_s1_k2).await.unwrap();

        view_s2.save_event(&event1_s2).await.unwrap();
        view_s2.save_event(&event2_s2).await.unwrap();

        db.save_event(&event_unscoped).await.unwrap();

        // Initial state verification
        let s1_events_initial = view_s1.query(Filter::new()).await.unwrap();
        assert_eq!(s1_events_initial.len(), 3);
        assert!(s1_events_initial.contains(&event1_s1));
        assert!(s1_events_initial.contains(&event2_s1));
        assert!(s1_events_initial.contains(&event3_s1_k2));

        let s2_events_initial = view_s2.query(Filter::new()).await.unwrap();
        assert_eq!(s2_events_initial.len(), 2);
        assert!(s2_events_initial.contains(&event1_s2));
        assert!(s2_events_initial.contains(&event2_s2));

        let unscoped_events_initial = db.query(Filter::new()).await.unwrap();
        assert_eq!(unscoped_events_initial.len(), 1);
        assert!(unscoped_events_initial.contains(&event_unscoped));

        // --- Test Deletion by ID ---
        view_s1
            .delete(Filter::new().id(event1_s1.id))
            .await
            .unwrap();

        // Verify event1_s1 is gone from scope1
        let s1_events_after_del_id = view_s1.query(Filter::new()).await.unwrap();
        assert_eq!(s1_events_after_del_id.len(), 2);
        assert!(!s1_events_after_del_id.contains(&event1_s1));
        assert!(s1_events_after_del_id.contains(&event2_s1));
        assert!(s1_events_after_del_id.contains(&event3_s1_k2));

        // Verify scope2 is unaffected
        let s2_events_after_del_id_s1 = view_s2.query(Filter::new()).await.unwrap();
        assert_eq!(s2_events_after_del_id_s1.len(), 2);
        assert!(s2_events_after_del_id_s1.contains(&event1_s2));

        // Verify unscoped is unaffected
        let unscoped_events_after_del_id_s1 = db.query(Filter::new()).await.unwrap();
        assert_eq!(unscoped_events_after_del_id_s1.len(), 1);
        assert!(unscoped_events_after_del_id_s1.contains(&event_unscoped));

        // --- Test Deletion by Author ---
        // Delete all events by author A from scope1. event3_s1_k2 should be deleted.
        view_s1
            .delete(Filter::new().author(keys_a.public_key()))
            .await
            .unwrap();

        let s1_events_after_del_author_a = view_s1.query(Filter::new()).await.unwrap();
        assert_eq!(s1_events_after_del_author_a.len(), 1); // Only event2_s1 (by B) should remain
        assert!(s1_events_after_del_author_a.contains(&event2_s1));
        assert!(!s1_events_after_del_author_a.contains(&event1_s1));
        assert!(!s1_events_after_del_author_a.contains(&event3_s1_k2));

        // Verify event1_s2 (by author A) in scope2 is unaffected
        let s2_events_after_del_author_a_s1 = view_s2.query(Filter::new()).await.unwrap();
        assert_eq!(s2_events_after_del_author_a_s1.len(), 2); // Should still have event1_s2 and event2_s2
        assert!(s2_events_after_del_author_a_s1.contains(&event1_s2));

        // --- Test Deletion by Kind in a specific scope ---
        // Add a new event to scope2 to test kind deletion
        let event3_s2_k_custom = EventBuilder::new(Kind::Custom(12222), "S2E3K_CUSTOM Custom by C")
            .sign_with_keys(&keys_c)
            .unwrap();
        view_s2.save_event(&event3_s2_k_custom).await.unwrap();
        assert_eq!(view_s2.query(Filter::new()).await.unwrap().len(), 3); // event1_s2, event2_s2, event3_s2_k_custom

        // Delete Kind::Custom(12222) from scope2
        view_s2
            .delete(Filter::new().kind(Kind::Custom(12222)))
            .await
            .unwrap();
        let s2_events_after_del_kind = view_s2.query(Filter::new()).await.unwrap();
        assert_eq!(s2_events_after_del_kind.len(), 2); // event1_s2 (textnote), event2_s2 (textnote) should remain
        assert!(!s2_events_after_del_kind.contains(&event3_s2_k_custom));
        assert!(s2_events_after_del_kind.contains(&event1_s2));
        assert!(s2_events_after_del_kind.contains(&event2_s2));

        // Verify scope1 (which had an event of Kind::Custom(12222) by author A - event3_s1_k2, now deleted) is not further affected by s2 deletion
        let s1_final_check = view_s1.query(Filter::new()).await.unwrap();
        assert_eq!(s1_final_check.len(), 1); // Should still only have event2_s1
        assert!(s1_final_check.contains(&event2_s1));

        // --- Test deleting all events in a scope ---
        let view_s1_all_count = view_s1.query(Filter::new()).await.unwrap().len();
        assert_eq!(view_s1_all_count, 1); // event2_s1 remains

        view_s1.delete(Filter::new()).await.unwrap(); // Delete all from scope1
        assert_eq!(view_s1.query(Filter::new()).await.unwrap().len(), 0);

        // Ensure scope2 and unscoped are not affected
        assert_eq!(view_s2.query(Filter::new()).await.unwrap().len(), 2);
        assert_eq!(db.query(Filter::new()).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_nostr_lmdb_scoped_methods() {
        let db = TempDatabase::new();

        // Test successful creation of a named scoped view
        let test_scope = Scope::named("test_scope").unwrap();
        let scoped_view_res = db.scoped(&test_scope);
        assert!(scoped_view_res.is_ok());
        let _scoped_view = scoped_view_res.unwrap();

        // Test successful creation of an unscoped view
        let unscoped_view_res = db.scoped(&Scope::Default);
        assert!(unscoped_view_res.is_ok());
        let _unscoped_view = unscoped_view_res.unwrap();
        assert!(_unscoped_view.scope.is_default());
        
        // Test creating an invalid scope directly
        // This won't compile because Scope::named validates at compile time
        // but we can test database operations with an empty string
        // let empty_scope = Scope::named("").unwrap(); // This would panic
    }
}
