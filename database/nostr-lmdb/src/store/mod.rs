// Copyright (c) 2024 Michael Dilger
// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2025 Rust Nostr Developers
// Distributed under the MIT software license

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_utility::task;
use flume::Sender;
use nostr_database::prelude::*;
use scoped_heed::{GlobalScopeRegistry, Scope};
use tracing::warn;

mod error;
mod filter;
mod ingester;
mod lmdb;

use self::error::Error;
use self::ingester::{Ingester, IngesterItem};
use self::lmdb::Lmdb;

/// Threshold for logging slow database operations
const SLOW_OP_THRESHOLD: Duration = Duration::from_millis(100);

/// Summarize a filter for logging purposes
fn summarize_filter(filter: &Filter) -> String {
    let mut parts = Vec::new();

    // IDs
    let ids = filter.ids.as_ref().map(|s| s.len()).unwrap_or(0);
    if ids > 0 {
        parts.push(format!("ids:{}", ids));
    }

    // Authors
    let authors = filter.authors.as_ref().map(|s| s.len()).unwrap_or(0);
    if authors > 0 {
        parts.push(format!("authors:{}", authors));
    }

    // Kinds
    if let Some(kinds) = filter.kinds.as_ref() {
        let kind_nums: Vec<u16> = kinds.iter().map(|k| k.as_u16()).collect();
        if kind_nums.len() <= 5 {
            parts.push(format!("kinds:{:?}", kind_nums));
        } else {
            parts.push(format!("kinds:[{}...]", kind_nums.len()));
        }
    }

    // Tags (just count them)
    let tag_count: usize = filter
        .generic_tags
        .iter()
        .map(|(_, v)| v.len())
        .sum();
    if tag_count > 0 {
        let tag_keys: Vec<_> = filter.generic_tags.keys().collect();
        parts.push(format!("tags:{}({:?})", tag_count, tag_keys));
    }

    // Time range
    if filter.since.is_some() || filter.until.is_some() {
        parts.push(format!(
            "time:{:?}-{:?}",
            filter.since.map(|t| t.as_secs()),
            filter.until.map(|t| t.as_secs())
        ));
    }

    // Limit
    if let Some(limit) = filter.limit {
        parts.push(format!("limit:{}", limit));
    }

    if parts.is_empty() {
        "empty".to_string()
    } else {
        parts.join(",")
    }
}

#[derive(Debug)]
pub(super) struct Store {
    db: Lmdb,
    ingester: Sender<IngesterItem>,
}

impl Store {
    pub(super) async fn open<P>(
        path: P,
        map_size: usize,
        max_readers: u32,
        additional_dbs: u32,
        ingester_thread_config: Option<Box<dyn FnOnce() + Send>>,
    ) -> Result<Store, Error>
    where
        P: AsRef<Path>,
    {
        let path: PathBuf = path.as_ref().to_path_buf();

        // Open the database in a blocking task
        let db: Lmdb = task::spawn_blocking(move || {
            // Create the directory if it doesn't exist
            fs::create_dir_all(&path)?;

            let db: Lmdb = Lmdb::new(path, map_size, max_readers, additional_dbs)?;

            Ok::<Lmdb, Error>(db)
        })
        .await??;

        // Run the ingester with optional thread configuration
        let ingester: Sender<IngesterItem> =
            Ingester::run(db.clone(), ingester_thread_config);

        Ok(Self { db, ingester })
    }

    #[inline]
    async fn interact<F, R>(&self, op_name: &'static str, f: F) -> Result<R, Error>
    where
        F: FnOnce(Lmdb) -> R + Send + 'static,
        R: Send + 'static,
    {
        let start = Instant::now();
        let db = self.db.clone();
        let result = task::spawn_blocking(move || f(db)).await?;

        let elapsed = start.elapsed();
        if elapsed > SLOW_OP_THRESHOLD {
            warn!(
                target: "nostr_lmdb::slow_op",
                operation = op_name,
                duration_ms = elapsed.as_millis() as u64,
                "Slow database operation detected"
            );
        }

        Ok(result)
    }

    /// Variant of interact that logs filter details for slow query operations
    #[inline]
    async fn interact_query<F, R>(
        &self,
        op_name: &'static str,
        filter_summary: String,
        f: F,
    ) -> Result<R, Error>
    where
        F: FnOnce(Lmdb) -> R + Send + 'static,
        R: Send + 'static,
    {
        let start = Instant::now();
        let db = self.db.clone();
        let result = task::spawn_blocking(move || f(db)).await?;

        let elapsed = start.elapsed();
        if elapsed > SLOW_OP_THRESHOLD {
            warn!(
                target: "nostr_lmdb::slow_op",
                operation = op_name,
                filter = %filter_summary,
                duration_ms = elapsed.as_millis() as u64,
                "Slow database operation detected"
            );
        }

        Ok(result)
    }

    pub(crate) async fn reindex(&self) -> Result<(), Error> {
        let (item, rx) = IngesterItem::reindex();
        self.ingester.send(item).map_err(|_| Error::FlumeSend)?;
        rx.await?
    }

    pub(super) async fn save_event(&self, event: &Event) -> Result<SaveEventStatus, Error> {
        let (item, rx) = IngesterItem::save_event_with_feedback(event.clone());
        self.ingester.send(item).map_err(|_| Error::FlumeSend)?;
        rx.await?
    }

    /// Save an event (owned version that avoids cloning)
    pub(super) async fn save_event_owned(&self, event: Event) -> Result<SaveEventStatus, Error> {
        let (item, rx) = IngesterItem::save_event_with_feedback(event);
        self.ingester.send(item).map_err(|_| Error::FlumeSend)?;
        rx.await?
    }

    pub(super) async fn get_event_by_id(&self, id: EventId) -> Result<Option<Event>, Error> {
        self.interact("get_event_by_id", move |db| {
            let txn = db.read_txn()?;
            let event: Option<Event> = db
                .get_event_by_id(&txn, id.as_bytes())?
                .map(|e| e.into_owned());
            txn.commit()?;
            Ok(event)
        })
        .await?
    }

    pub(super) async fn check_id(&self, id: EventId) -> Result<DatabaseEventStatus, Error> {
        self.interact("check_id", move |db| {
            let txn = db.read_txn()?;

            let status: DatabaseEventStatus = if db.is_deleted(&txn, &id)? {
                DatabaseEventStatus::Deleted
            } else if db.has_event(&txn, &id)? {
                DatabaseEventStatus::Saved
            } else {
                DatabaseEventStatus::NotExistent
            };

            txn.commit()?;

            Ok(status)
        })
        .await?
    }

    pub(super) async fn count(&self, filter: Filter) -> Result<usize, Error> {
        let filter_summary = summarize_filter(&filter);
        self.interact_query("count", filter_summary, move |db| {
            let txn = db.read_txn()?;
            let output = db.query(&txn, filter)?;
            let len: usize = output.count();
            txn.commit()?;
            Ok(len)
        })
        .await?
    }

    // Lookup ID: EVENT_ORD_IMPL
    pub(super) async fn query(&self, filter: Filter) -> Result<Events, Error> {
        let filter_summary = summarize_filter(&filter);
        self.interact_query("query", filter_summary, move |db| {
            let mut events: Events = Events::new(&filter);

            let txn = db.read_txn()?;
            let output = db.query(&txn, filter)?;
            events.extend(output.into_iter().map(|e| e.into_owned()));
            txn.commit()?;

            Ok(events)
        })
        .await?
    }

    pub(super) async fn negentropy_items(
        &self,
        filter: Filter,
    ) -> Result<Vec<(EventId, Timestamp)>, Error> {
        let filter_summary = summarize_filter(&filter);
        self.interact_query("negentropy_items", filter_summary, move |db| {
            let txn = db.read_txn()?;
            let events = db.query(&txn, filter)?;
            let items = events
                .into_iter()
                .map(|e| (EventId::from_byte_array(*e.id), e.created_at))
                .collect();
            txn.commit()?;
            Ok(items)
        })
        .await?
    }

    pub(super) async fn delete(&self, filter: Filter) -> Result<(), Error> {
        let (item, rx) = IngesterItem::delete_with_feedback(filter);
        self.ingester.send(item).map_err(|_| Error::FlumeSend)?;
        rx.await?
    }

    pub(super) async fn wipe(&self) -> Result<(), Error> {
        let (item, rx) = IngesterItem::wipe_with_feedback();
        self.ingester.send(item).map_err(|_| Error::FlumeSend)?;
        rx.await?
    }

    // Scoped operations for multi-tenant support

    /// Get a reference to the scope registry for managing scopes
    pub(super) fn scope_registry(&self) -> &Arc<GlobalScopeRegistry> {
        self.db.scope_registry()
    }

    /// Register a new scope in the database
    pub(super) async fn register_scope(&self, scope: Scope) -> Result<(), Error> {
        let (item, rx) = IngesterItem::register_scope_with_feedback(scope);
        self.ingester.send(item).map_err(|_| Error::FlumeSend)?;
        rx.await?
    }

    /// List all registered scopes (async version)
    pub(super) async fn list_scopes(&self) -> Result<Vec<Scope>, Error> {
        self.interact("list_scopes", move |db| {
            let txn = db.read_txn()?;
            let scopes = db.list_scopes(&txn)?;
            txn.commit()?;
            Ok(scopes)
        })
        .await?
    }

    /// List all registered scopes (sync version for use in blocking contexts)
    ///
    /// Note: This blocks the current thread. Use with caution in async contexts.
    pub(super) fn list_scopes_sync(&self) -> Result<Vec<Scope>, Error> {
        let txn = self.db.read_txn()?;
        let scopes = self.db.list_scopes(&txn)?;
        txn.commit()?;
        Ok(scopes)
    }

    /// Save an event to a specific scope
    pub(super) async fn save_event_scoped(
        &self,
        event: &Event,
        scope: Scope,
    ) -> Result<SaveEventStatus, Error> {
        let (item, rx) = IngesterItem::save_event_scoped_with_feedback(event.clone(), scope);
        self.ingester.send(item).map_err(|_| Error::FlumeSend)?;
        rx.await?
    }

    /// Save an event to a specific scope (owned version that avoids cloning)
    pub(super) async fn save_event_scoped_owned(
        &self,
        event: Event,
        scope: Scope,
    ) -> Result<SaveEventStatus, Error> {
        let (item, rx) = IngesterItem::save_event_scoped_with_feedback(event, scope);
        self.ingester.send(item).map_err(|_| Error::FlumeSend)?;
        rx.await?
    }

    /// Get an event by ID from a specific scope
    pub(super) async fn get_event_by_id_scoped(
        &self,
        id: EventId,
        scope: Scope,
    ) -> Result<Option<Event>, Error> {
        self.interact("get_event_by_id_scoped", move |db| {
            let txn = db.read_txn()?;
            let event: Option<Event> = db
                .get_event_by_id_scoped(&txn, &scope, id.as_bytes())?
                .map(|e| e.into_owned());
            txn.commit()?;
            Ok(event)
        })
        .await?
    }

    /// Check the status of an event ID in a specific scope
    pub(super) async fn check_id_scoped(
        &self,
        id: EventId,
        scope: Scope,
    ) -> Result<DatabaseEventStatus, Error> {
        self.interact("check_id_scoped", move |db| {
            let txn = db.read_txn()?;

            let status: DatabaseEventStatus = if db.is_deleted_scoped(&txn, &scope, &id)? {
                DatabaseEventStatus::Deleted
            } else if db.has_event_scoped(&txn, &scope, &id)? {
                DatabaseEventStatus::Saved
            } else {
                DatabaseEventStatus::NotExistent
            };

            txn.commit()?;

            Ok(status)
        })
        .await?
    }

    /// Count events matching a filter in a specific scope
    pub(super) async fn count_scoped(&self, filter: Filter, scope: Scope) -> Result<usize, Error> {
        let filter_summary = summarize_filter(&filter);
        self.interact_query("count_scoped", filter_summary, move |db| {
            let txn = db.read_txn()?;
            let output = db.query_scoped(&txn, &scope, filter)?;
            let len: usize = output.count();
            txn.commit()?;
            Ok(len)
        })
        .await?
    }

    /// Query events matching a filter from a specific scope
    pub(super) async fn query_scoped(&self, filter: Filter, scope: Scope) -> Result<Events, Error> {
        let filter_summary = summarize_filter(&filter);
        self.interact_query("query_scoped", filter_summary, move |db| {
            let mut events: Events = Events::new(&filter);

            let txn = db.read_txn()?;
            let output = db.query_scoped(&txn, &scope, filter)?;
            events.extend(output.into_iter().map(|e| e.into_owned()));
            txn.commit()?;

            Ok(events)
        })
        .await?
    }

    /// Get negentropy items from a specific scope
    pub(super) async fn negentropy_items_scoped(
        &self,
        filter: Filter,
        scope: Scope,
    ) -> Result<Vec<(EventId, Timestamp)>, Error> {
        let filter_summary = summarize_filter(&filter);
        self.interact_query("negentropy_items_scoped", filter_summary, move |db| {
            let txn = db.read_txn()?;
            let events = db.query_scoped(&txn, &scope, filter)?;
            let items = events
                .into_iter()
                .map(|e| (EventId::from_byte_array(*e.id), e.created_at))
                .collect();
            txn.commit()?;
            Ok(items)
        })
        .await?
    }

    /// Delete events matching a filter from a specific scope
    pub(super) async fn delete_scoped(&self, filter: Filter, scope: Scope) -> Result<(), Error> {
        let (item, rx) = IngesterItem::delete_scoped_with_feedback(filter, scope);
        self.ingester.send(item).map_err(|_| Error::FlumeSend)?;
        rx.await?
    }

    /// Wipe all data from a specific scope
    pub(super) async fn wipe_scoped(&self, scope: Scope) -> Result<(), Error> {
        let (item, rx) = IngesterItem::wipe_scoped_with_feedback(scope);
        self.ingester.send(item).map_err(|_| Error::FlumeSend)?;
        rx.await?
    }
}
