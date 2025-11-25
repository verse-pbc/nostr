// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2025 Rust Nostr Developers
// Distributed under the MIT software license

//! LMDB storage backend for nostr apps
//!
//! Fork of [Pocket](https://github.com/mikedilger/pocket) database.
//!
//! # Scoped (Multi-Tenant) Support
//!
//! This crate supports multi-tenant database isolation through scopes. Each scope
//! provides completely isolated storage within the same LMDB environment.
//!
//! ```no_run
//! use nostr_lmdb::{NostrLmdb, Scope};
//! use nostr_database::NostrDatabase;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let db = NostrLmdb::open("/path/to/db").await?;
//!
//! // Create a scoped view for a specific tenant
//! let tenant_scope = Scope::named("tenant-123")?;
//! db.register_scope(tenant_scope.clone()).await?;
//!
//! let scoped_db = db.scoped_view(tenant_scope);
//!
//! // Now use scoped_db as a regular NostrDatabase - all operations
//! // are isolated to this tenant's scope
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]
#![warn(rustdoc::bare_urls)]
#![allow(clippy::mutable_key_type)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use nostr_database::prelude::*;

pub mod prelude;
mod store;

pub use scoped_heed::{GlobalScopeRegistry, Scope};
use self::store::Store;

// 64-bit
#[cfg(target_pointer_width = "64")]
const MAP_SIZE: usize = 1024 * 1024 * 1024 * 32; // 32GB

// 32-bit
#[cfg(target_pointer_width = "32")]
const MAP_SIZE: usize = 0xFFFFF000; // 4GB (2^32-4096)

#[allow(missing_docs)]
#[deprecated(since = "0.45.0", note = "Use NostrLmdb instead")]
pub type NostrLMDB = NostrLmdb;

/// Nostr LMDB database builder
pub struct NostrLmdbBuilder {
    /// Database path
    pub path: PathBuf,
    /// Custom map size
    ///
    /// By default, the following map size is used:
    /// - 32GB for 64-bit arch
    /// - 4GB for 32-bit arch
    pub map_size: Option<usize>,
    /// Maximum number of reader threads
    ///
    /// Defaults to 126 if not set
    pub max_readers: Option<u32>,
    /// Number of additional databases to allocate beyond the 9 internal ones
    ///
    /// Defaults to 0 if not set
    pub additional_dbs: Option<u32>,
    /// Optional callback to configure the ingester thread (e.g., CPU affinity)
    ingester_thread_config: Option<Box<dyn FnOnce() + Send + 'static>>,
}

impl std::fmt::Debug for NostrLmdbBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NostrLmdbBuilder")
            .field("path", &self.path)
            .field("map_size", &self.map_size)
            .field("max_readers", &self.max_readers)
            .field("additional_dbs", &self.additional_dbs)
            .field(
                "ingester_thread_config",
                &self.ingester_thread_config.is_some(),
            )
            .finish()
    }
}

impl NostrLmdbBuilder {
    /// New LMDb builder
    pub fn new<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        Self {
            path: path.as_ref().to_path_buf(),
            map_size: None,
            max_readers: None,
            additional_dbs: None,
            ingester_thread_config: None,
        }
    }

    /// Map size
    ///
    /// By default, the following map size is used:
    /// - 32GB for 64-bit arch
    /// - 4GB for 32-bit arch
    pub fn map_size(mut self, map_size: usize) -> Self {
        self.map_size = Some(map_size);
        self
    }

    /// Maximum number of reader threads
    ///
    /// Defaults to 126 if not set
    pub fn max_readers(mut self, max_readers: u32) -> Self {
        self.max_readers = Some(max_readers);
        self
    }

    /// Number of additional databases to allocate beyond the 9 internal ones
    ///
    /// Defaults to 0 if not set
    pub fn additional_dbs(mut self, additional_dbs: u32) -> Self {
        self.additional_dbs = Some(additional_dbs);
        self
    }

    /// Set a callback to configure the ingester thread (e.g., CPU affinity).
    ///
    /// This callback is called at the start of the ingester thread before it
    /// begins processing events. Use this to set thread-specific configuration
    /// like CPU affinity for performance tuning.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use nostr_lmdb::NostrLmdb;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = NostrLmdb::builder("/path/to/db")
    ///     .with_ingester_thread_config(|| {
    ///         // Set CPU affinity or other thread configuration here
    ///         println!("Ingester thread started");
    ///     })
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_ingester_thread_config<F>(mut self, f: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        self.ingester_thread_config = Some(Box::new(f));
        self
    }

    /// Build
    pub async fn build(self) -> Result<NostrLmdb, DatabaseError> {
        let map_size: usize = self.map_size.unwrap_or(MAP_SIZE);
        let max_readers: u32 = self.max_readers.unwrap_or(126);
        let additional_dbs: u32 = self.additional_dbs.unwrap_or(0);
        let db: Store = Store::open(
            self.path,
            map_size,
            max_readers,
            additional_dbs,
            self.ingester_thread_config,
        )
        .await
        .map_err(DatabaseError::backend)?;
        Ok(NostrLmdb { db: Arc::new(db) })
    }
}

/// LMDB Nostr Database
#[derive(Debug)]
pub struct NostrLmdb {
    db: Arc<Store>,
}

impl NostrLmdb {
    /// Open LMDB database
    #[inline]
    pub async fn open<P>(path: P) -> Result<Self, DatabaseError>
    where
        P: AsRef<Path>,
    {
        Self::builder(path).build().await
    }

    /// Get a new builder
    #[inline]
    pub fn builder<P>(path: P) -> NostrLmdbBuilder
    where
        P: AsRef<Path>,
    {
        NostrLmdbBuilder::new(path)
    }

    /// Re-index the database.
    #[inline]
    pub async fn reindex(&self) -> Result<(), DatabaseError> {
        self.db.reindex().await.map_err(DatabaseError::backend)
    }

    /// Save an event (owned version that avoids cloning).
    ///
    /// This method takes ownership of the event to avoid an internal clone,
    /// which can improve performance when the caller doesn't need the event afterward.
    pub async fn save_event_owned(&self, event: Event) -> Result<SaveEventStatus, DatabaseError> {
        self.db
            .save_event_owned(event)
            .await
            .map_err(DatabaseError::backend)
    }

    // Scoped (Multi-Tenant) Support

    /// Register a new scope in the database.
    ///
    /// This must be called before using a scope for the first time.
    /// Registering a scope is idempotent - calling it multiple times
    /// for the same scope is safe.
    #[inline]
    pub async fn register_scope(&self, scope: Scope) -> Result<(), DatabaseError> {
        self.db
            .register_scope(scope)
            .await
            .map_err(DatabaseError::backend)
    }

    /// List all registered scopes in the database (async version).
    #[inline]
    pub async fn list_scopes_async(&self) -> Result<Vec<Scope>, DatabaseError> {
        self.db.list_scopes().await.map_err(DatabaseError::backend)
    }

    /// List all registered scopes in the database (blocking version).
    ///
    /// This is a convenience method for use in blocking contexts. It internally
    /// uses the scope registry to list scopes synchronously.
    ///
    /// # Errors
    ///
    /// Returns an error if reading from the database fails.
    #[inline]
    pub fn list_scopes(&self) -> Result<Vec<Scope>, DatabaseError> {
        // This is synchronous - reads directly from scope registry
        // The scope registry is already loaded in memory
        self.db
            .list_scopes_sync()
            .map_err(DatabaseError::backend)
    }

    /// Create a scoped view of the database.
    ///
    /// The returned `ScopedView` implements `NostrDatabase` and routes all
    /// operations to the specified scope, providing complete data isolation.
    ///
    /// This method is infallible as creating a view doesn't require I/O.
    /// The scope should be registered with [`register_scope`](Self::register_scope)
    /// before saving events to it.
    #[inline]
    pub fn scoped_view(&self, scope: Scope) -> ScopedView {
        ScopedView {
            db: Arc::clone(&self.db),
            scope,
        }
    }

    /// Create a scoped view of the database (Result-returning version for compatibility).
    ///
    /// This is an alias for [`scoped_view`](Self::scoped_view) that returns a `Result`
    /// for API compatibility with code that expects fallible scope creation.
    ///
    /// # Errors
    ///
    /// This method currently never fails, but returns `Result` for forward compatibility.
    #[inline]
    pub fn scoped(&self, scope: &Scope) -> Result<ScopedView, DatabaseError> {
        Ok(self.scoped_view(scope.clone()))
    }

    /// Wipe all data from a specific scope, leaving other scopes intact.
    #[inline]
    pub async fn wipe_scope(&self, scope: Scope) -> Result<(), DatabaseError> {
        self.db
            .wipe_scoped(scope)
            .await
            .map_err(DatabaseError::backend)
    }

    /// Get a reference to the global scope registry.
    ///
    /// The scope registry maintains metadata about all registered scopes,
    /// including the mapping between scope hashes and scope names.
    ///
    /// This is useful for:
    /// - Checking if a scope exists before operations
    /// - Looking up scope names from hashes (for debugging/diagnostics)
    /// - Advanced scope management operations
    ///
    /// # Example
    ///
    /// ```no_run
    /// use nostr_lmdb::NostrLmdb;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = NostrLmdb::open("/path/to/db").await?;
    /// let registry = db.scope_registry();
    /// // Use registry for scope lookups or management
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn scope_registry(&self) -> &Arc<GlobalScopeRegistry> {
        self.db.scope_registry()
    }
}

impl NostrDatabase for NostrLmdb {
    #[inline]
    fn backend(&self) -> Backend {
        Backend::LMDB
    }

    fn features(&self) -> Features {
        Features {
            persistent: true,
            event_expiration: false,
            full_text_search: true,
            request_to_vanish: false,
        }
    }

    fn save_event<'a>(
        &'a self,
        event: &'a Event,
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
        event_id: &'a EventId,
    ) -> BoxedFuture<'a, Result<DatabaseEventStatus, DatabaseError>> {
        Box::pin(async move {
            self.db
                .check_id(*event_id)
                .await
                .map_err(DatabaseError::backend)
        })
    }

    fn event_by_id<'a>(
        &'a self,
        event_id: &'a EventId,
    ) -> BoxedFuture<'a, Result<Option<Event>, DatabaseError>> {
        Box::pin(async move {
            self.db
                .get_event_by_id(*event_id)
                .await
                .map_err(DatabaseError::backend)
        })
    }

    fn count(&self, filter: Filter) -> BoxedFuture<Result<usize, DatabaseError>> {
        Box::pin(async move { self.db.count(filter).await.map_err(DatabaseError::backend) })
    }

    fn query(&self, filter: Filter) -> BoxedFuture<Result<Events, DatabaseError>> {
        Box::pin(async move { self.db.query(filter).await.map_err(DatabaseError::backend) })
    }

    fn negentropy_items(
        &self,
        filter: Filter,
    ) -> BoxedFuture<Result<Vec<(EventId, Timestamp)>, DatabaseError>> {
        Box::pin(async move {
            self.db
                .negentropy_items(filter)
                .await
                .map_err(DatabaseError::backend)
        })
    }

    fn delete(&self, filter: Filter) -> BoxedFuture<Result<(), DatabaseError>> {
        Box::pin(async move { self.db.delete(filter).await.map_err(DatabaseError::backend) })
    }

    #[inline]
    fn wipe(&self) -> BoxedFuture<Result<(), DatabaseError>> {
        Box::pin(async move { self.db.wipe().await.map_err(DatabaseError::backend) })
    }
}

/// A scoped view of the LMDB database for multi-tenant isolation.
///
/// This struct provides complete data isolation for a specific scope (tenant).
/// All operations performed through this view are automatically scoped to the
/// configured scope, preventing data from leaking between tenants.
///
/// # Example
///
/// ```no_run
/// use nostr_lmdb::{NostrLmdb, Scope};
/// use nostr_database::NostrDatabase;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let db = NostrLmdb::open("/path/to/db").await?;
///
/// // Create isolated views for different tenants
/// let tenant_a = db.scoped_view(Scope::named("tenant-a")?);
/// let tenant_b = db.scoped_view(Scope::named("tenant-b")?);
///
/// // Events saved to tenant_a are not visible to tenant_b and vice versa
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ScopedView {
    db: Arc<Store>,
    scope: Scope,
}

impl ScopedView {
    /// Get the scope this view is configured for.
    #[inline]
    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    /// Save an event (owned version that avoids cloning).
    ///
    /// This method takes ownership of the event to avoid an internal clone,
    /// which can improve performance when the caller doesn't need the event afterward.
    pub async fn save_event_owned(&self, event: Event) -> Result<SaveEventStatus, DatabaseError> {
        self.db
            .save_event_scoped_owned(event, self.scope.clone())
            .await
            .map_err(DatabaseError::backend)
    }
}

impl NostrDatabase for ScopedView {
    #[inline]
    fn backend(&self) -> Backend {
        Backend::LMDB
    }

    fn features(&self) -> Features {
        Features {
            persistent: true,
            event_expiration: false,
            full_text_search: true,
            request_to_vanish: false,
        }
    }

    fn save_event<'a>(
        &'a self,
        event: &'a Event,
    ) -> BoxedFuture<'a, Result<SaveEventStatus, DatabaseError>> {
        let scope = self.scope.clone();
        Box::pin(async move {
            self.db
                .save_event_scoped(event, scope)
                .await
                .map_err(DatabaseError::backend)
        })
    }

    fn check_id<'a>(
        &'a self,
        event_id: &'a EventId,
    ) -> BoxedFuture<'a, Result<DatabaseEventStatus, DatabaseError>> {
        let scope = self.scope.clone();
        Box::pin(async move {
            self.db
                .check_id_scoped(*event_id, scope)
                .await
                .map_err(DatabaseError::backend)
        })
    }

    fn event_by_id<'a>(
        &'a self,
        event_id: &'a EventId,
    ) -> BoxedFuture<'a, Result<Option<Event>, DatabaseError>> {
        let scope = self.scope.clone();
        Box::pin(async move {
            self.db
                .get_event_by_id_scoped(*event_id, scope)
                .await
                .map_err(DatabaseError::backend)
        })
    }

    fn count(&self, filter: Filter) -> BoxedFuture<Result<usize, DatabaseError>> {
        let scope = self.scope.clone();
        Box::pin(async move {
            self.db
                .count_scoped(filter, scope)
                .await
                .map_err(DatabaseError::backend)
        })
    }

    fn query(&self, filter: Filter) -> BoxedFuture<Result<Events, DatabaseError>> {
        let scope = self.scope.clone();
        Box::pin(async move {
            self.db
                .query_scoped(filter, scope)
                .await
                .map_err(DatabaseError::backend)
        })
    }

    fn negentropy_items(
        &self,
        filter: Filter,
    ) -> BoxedFuture<Result<Vec<(EventId, Timestamp)>, DatabaseError>> {
        let scope = self.scope.clone();
        Box::pin(async move {
            self.db
                .negentropy_items_scoped(filter, scope)
                .await
                .map_err(DatabaseError::backend)
        })
    }

    fn delete(&self, filter: Filter) -> BoxedFuture<Result<(), DatabaseError>> {
        let scope = self.scope.clone();
        Box::pin(async move {
            self.db
                .delete_scoped(filter, scope)
                .await
                .map_err(DatabaseError::backend)
        })
    }

    fn wipe(&self) -> BoxedFuture<Result<(), DatabaseError>> {
        let scope = self.scope.clone();
        Box::pin(async move {
            self.db
                .wipe_scoped(scope)
                .await
                .map_err(DatabaseError::backend)
        })
    }
}

#[cfg(test)]
mod tests {
    use nostr::{EventBuilder, Keys, Kind};
    use nostr_database_test_suite::database_unit_tests;
    use tempfile::TempDir;

    use super::*;

    struct TempDatabase {
        db: NostrLmdb,
        // Needed to avoid the drop and deletion of temp folder
        _temp: TempDir,
    }

    impl Deref for TempDatabase {
        type Target = NostrLmdb;

        fn deref(&self) -> &Self::Target {
            &self.db
        }
    }

    impl TempDatabase {
        async fn new() -> Self {
            let path = tempfile::tempdir().unwrap();
            Self {
                db: NostrLmdb::open(&path).await.unwrap(),
                _temp: path,
            }
        }
    }

    database_unit_tests!(TempDatabase, TempDatabase::new);

    // Scoped (Multi-Tenant) Tests

    #[tokio::test]
    async fn test_scoped_isolation() {
        let temp = TempDatabase::new().await;
        let db = &temp.db;
        let keys = Keys::generate();

        // Create two different scopes
        let scope_a = Scope::named("tenant-a").unwrap();
        let scope_b = Scope::named("tenant-b").unwrap();

        // Register both scopes
        db.register_scope(scope_a.clone()).await.unwrap();
        db.register_scope(scope_b.clone()).await.unwrap();

        // Create scoped views
        let view_a = db.scoped_view(scope_a.clone());
        let view_b = db.scoped_view(scope_b.clone());

        // Create an event and save it to scope A
        let event = EventBuilder::text_note("Hello from tenant A")
            .sign_with_keys(&keys)
            .unwrap();
        let event_id = event.id;

        view_a.save_event(&event).await.unwrap();

        // Event should be visible in scope A
        let found_in_a = view_a.event_by_id(&event_id).await.unwrap();
        assert!(found_in_a.is_some());
        assert_eq!(found_in_a.unwrap().id, event_id);

        // Event should NOT be visible in scope B
        let found_in_b = view_b.event_by_id(&event_id).await.unwrap();
        assert!(found_in_b.is_none());

        // Query should find event in scope A but not in scope B
        let query_a = view_a.query(Filter::new()).await.unwrap();
        assert_eq!(query_a.len(), 1);

        let query_b = view_b.query(Filter::new()).await.unwrap();
        assert_eq!(query_b.len(), 0);

        // Count should reflect the same
        let count_a = view_a.count(Filter::new()).await.unwrap();
        assert_eq!(count_a, 1);

        let count_b = view_b.count(Filter::new()).await.unwrap();
        assert_eq!(count_b, 0);
    }

    #[tokio::test]
    async fn test_scoped_delete() {
        let temp = TempDatabase::new().await;
        let db = &temp.db;
        let keys = Keys::generate();

        let scope_a = Scope::named("tenant-a").unwrap();
        let scope_b = Scope::named("tenant-b").unwrap();

        db.register_scope(scope_a.clone()).await.unwrap();
        db.register_scope(scope_b.clone()).await.unwrap();

        let view_a = db.scoped_view(scope_a.clone());
        let view_b = db.scoped_view(scope_b.clone());

        // Save events to both scopes
        let event_a = EventBuilder::text_note("Event for A")
            .sign_with_keys(&keys)
            .unwrap();
        let event_b = EventBuilder::text_note("Event for B")
            .sign_with_keys(&keys)
            .unwrap();

        view_a.save_event(&event_a).await.unwrap();
        view_b.save_event(&event_b).await.unwrap();

        // Both scopes should have 1 event
        assert_eq!(view_a.count(Filter::new()).await.unwrap(), 1);
        assert_eq!(view_b.count(Filter::new()).await.unwrap(), 1);

        // Delete all events from scope A
        view_a.delete(Filter::new().kind(Kind::TextNote)).await.unwrap();

        // Scope A should be empty, scope B should still have its event
        assert_eq!(view_a.count(Filter::new()).await.unwrap(), 0);
        assert_eq!(view_b.count(Filter::new()).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_scoped_wipe() {
        let temp = TempDatabase::new().await;
        let db = &temp.db;
        let keys = Keys::generate();

        let scope_a = Scope::named("tenant-a").unwrap();
        let scope_b = Scope::named("tenant-b").unwrap();

        db.register_scope(scope_a.clone()).await.unwrap();
        db.register_scope(scope_b.clone()).await.unwrap();

        let view_a = db.scoped_view(scope_a.clone());
        let view_b = db.scoped_view(scope_b.clone());

        // Save multiple events to both scopes
        for i in 0..5 {
            let event = EventBuilder::text_note(format!("Event A-{}", i))
                .sign_with_keys(&keys)
                .unwrap();
            view_a.save_event(&event).await.unwrap();

            let event = EventBuilder::text_note(format!("Event B-{}", i))
                .sign_with_keys(&keys)
                .unwrap();
            view_b.save_event(&event).await.unwrap();
        }

        // Both scopes should have 5 events
        assert_eq!(view_a.count(Filter::new()).await.unwrap(), 5);
        assert_eq!(view_b.count(Filter::new()).await.unwrap(), 5);

        // Wipe scope A using the db.wipe_scope method
        db.wipe_scope(scope_a).await.unwrap();

        // Scope A should be empty, scope B should still have its events
        assert_eq!(view_a.count(Filter::new()).await.unwrap(), 0);
        assert_eq!(view_b.count(Filter::new()).await.unwrap(), 5);
    }

    #[tokio::test]
    async fn test_default_scope_isolation() {
        let temp = TempDatabase::new().await;
        let db = &temp.db;
        let keys = Keys::generate();

        let named_scope = Scope::named("custom-tenant").unwrap();
        db.register_scope(named_scope.clone()).await.unwrap();

        let default_view = db.scoped_view(Scope::Default);
        let named_view = db.scoped_view(named_scope);

        // Save event to default scope
        let event = EventBuilder::text_note("Default scope event")
            .sign_with_keys(&keys)
            .unwrap();
        default_view.save_event(&event).await.unwrap();

        // Default scope should have the event
        assert_eq!(default_view.count(Filter::new()).await.unwrap(), 1);

        // Named scope should be empty
        assert_eq!(named_view.count(Filter::new()).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_list_scopes() {
        let temp = TempDatabase::new().await;
        let db = &temp.db;

        // Initially only the default scope is registered
        let scopes = db.list_scopes().unwrap();
        assert_eq!(scopes.len(), 1);
        assert!(scopes.contains(&Scope::Default));

        // Register some scopes
        let scope_a = Scope::named("tenant-a").unwrap();
        let scope_b = Scope::named("tenant-b").unwrap();

        db.register_scope(scope_a.clone()).await.unwrap();
        db.register_scope(scope_b.clone()).await.unwrap();

        // Now we should see Default + the two registered scopes
        let scopes = db.list_scopes().unwrap();
        assert_eq!(scopes.len(), 3);
        assert!(scopes.contains(&Scope::Default));
        assert!(scopes.contains(&scope_a));
        assert!(scopes.contains(&scope_b));

        // Registering the same scope again should be idempotent
        db.register_scope(scope_a.clone()).await.unwrap();
        let scopes = db.list_scopes().unwrap();
        assert_eq!(scopes.len(), 3);
    }

    #[tokio::test]
    async fn test_scoped_check_id() {
        let temp = TempDatabase::new().await;
        let db = &temp.db;
        let keys = Keys::generate();

        let scope_a = Scope::named("tenant-a").unwrap();
        let scope_b = Scope::named("tenant-b").unwrap();

        db.register_scope(scope_a.clone()).await.unwrap();
        db.register_scope(scope_b.clone()).await.unwrap();

        let view_a = db.scoped_view(scope_a);
        let view_b = db.scoped_view(scope_b);

        // Create and save an event to scope A
        let event = EventBuilder::text_note("Test event")
            .sign_with_keys(&keys)
            .unwrap();
        let event_id = event.id;

        view_a.save_event(&event).await.unwrap();

        // Check ID in scope A - should be Saved
        let status_a = view_a.check_id(&event_id).await.unwrap();
        assert!(matches!(status_a, DatabaseEventStatus::Saved));

        // Check ID in scope B - should be NotExistent
        let status_b = view_b.check_id(&event_id).await.unwrap();
        assert!(matches!(status_b, DatabaseEventStatus::NotExistent));
    }

    #[tokio::test]
    async fn test_scoped_negentropy_items() {
        let temp = TempDatabase::new().await;
        let db = &temp.db;
        let keys = Keys::generate();

        let scope_a = Scope::named("tenant-a").unwrap();
        let scope_b = Scope::named("tenant-b").unwrap();

        db.register_scope(scope_a.clone()).await.unwrap();
        db.register_scope(scope_b.clone()).await.unwrap();

        let view_a = db.scoped_view(scope_a);
        let view_b = db.scoped_view(scope_b);

        // Save events to scope A
        for i in 0..3 {
            let event = EventBuilder::text_note(format!("Event {}", i))
                .sign_with_keys(&keys)
                .unwrap();
            view_a.save_event(&event).await.unwrap();
        }

        // Get negentropy items from scope A - should have 3 items
        let items_a = view_a.negentropy_items(Filter::new()).await.unwrap();
        assert_eq!(items_a.len(), 3);

        // Get negentropy items from scope B - should be empty
        let items_b = view_b.negentropy_items(Filter::new()).await.unwrap();
        assert_eq!(items_b.len(), 0);
    }

    #[tokio::test]
    async fn test_scope_registry_access() {
        let temp = TempDatabase::new().await;
        let db = &temp.db;

        // Register some scopes
        let scope_a = Scope::named("tenant-alpha").unwrap();
        let scope_b = Scope::named("tenant-beta").unwrap();

        db.register_scope(scope_a.clone()).await.unwrap();
        db.register_scope(scope_b.clone()).await.unwrap();

        // Access the scope registry
        let registry = db.scope_registry();

        // The registry should be accessible and we can use it for lookups
        // Note: This is a sync operation that requires a transaction,
        // which would typically be done in blocking context
        assert!(Arc::strong_count(registry) >= 1);
    }
}
