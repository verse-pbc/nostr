// Copyright (c) 2024 Michael Dilger
// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2025 Rust Nostr Developers
// Distributed under the MIT software license

use std::fs;
use std::path::Path;
use std::sync::Arc;

use async_utility::task;
use flume::Sender;
use nostr_database::prelude::*;
use nostr_database::SaveEventStatus;
use scoped_heed::{GlobalScopeRegistry, Scope};

mod error;
mod ingester;
mod lmdb;
mod types;

use self::error::Error;
use self::ingester::{Ingester, IngesterItem};
use self::lmdb::Lmdb;

#[derive(Debug)]
pub struct Store {
    db: Lmdb,
    ingester: Sender<IngesterItem>,
}

impl Store {
    pub(super) fn open<P>(
        path: P,
        map_size: usize,
        max_readers: u32,
        additional_dbs: u32,
    ) -> Result<Store, Error>
    where
        P: AsRef<Path>,
    {
        let path: &Path = path.as_ref();

        // Create the directory if it doesn't exist
        fs::create_dir_all(path)?;

        let db: Lmdb = Lmdb::new(path, map_size, max_readers, additional_dbs)?;
        let ingester: Sender<IngesterItem> = Ingester::run(db.clone());

        Ok(Self { db, ingester })
    }

    #[inline]
    async fn interact<F, R>(&self, f: F) -> Result<R, Error>
    where
        F: FnOnce(Lmdb) -> R + Send + 'static,
        R: Send + 'static,
    {
        let db = self.db.clone();
        Ok(task::spawn_blocking(move || f(db)).await?)
    }

    /// Store an event.
    pub async fn save_event(&self, event: &Event) -> Result<SaveEventStatus, Error> {
        self.save_event_in_scope(&Scope::Default, event).await
    }

    pub async fn save_event_in_scope(
        &self,
        scope: &Scope,
        event: &Event,
    ) -> Result<SaveEventStatus, Error> {
        let (item, rx) = IngesterItem::save_event_with_feedback_scoped(event, scope.clone());
        self.ingester.send(item).map_err(|_| Error::FlumeSend)?;
        rx.await?
    }

    /// Get an event by ID
    pub fn get_event_by_id(&self, id: &EventId) -> Result<Option<Event>, Error> {
        self.get_event_by_id_in_scope(&Scope::Default, id)
    }

    /// Get an event by ID in a specific scope
    pub fn get_event_by_id_in_scope(
        &self,
        scope: &Scope,
        id: &EventId,
    ) -> Result<Option<Event>, Error> {
        let txn = self.db.read_txn()?;
        let event: Option<Event> = self
            .db
            .get_event_by_id_scoped(&txn, scope, id.as_bytes())?
            .map(|e| e.into_owned());
        txn.commit()?;
        Ok(event)
    }

    /// Do we have an event
    pub fn has_event(&self, id: &EventId) -> Result<bool, Error> {
        self.has_event_in_scope(&Scope::Default, id)
    }

    /// Do we have an event in a specific scope
    pub fn has_event_in_scope(&self, scope: &Scope, id: &EventId) -> Result<bool, Error> {
        let txn = self.db.read_txn()?;
        let has: bool = self.db.has_event_scoped(&txn, scope, id.as_bytes())?;
        txn.commit()?;
        Ok(has)
    }

    /// Is the event deleted
    pub fn event_is_deleted(&self, id: &EventId) -> Result<bool, Error> {
        self.event_is_deleted_in_scope(&Scope::Default, id)
    }

    /// Is the event deleted in a specific scope
    pub fn event_is_deleted_in_scope(&self, scope: &Scope, id: &EventId) -> Result<bool, Error> {
        let txn = self.db.read_txn()?;
        let deleted: bool = self.db.is_deleted_scoped(&txn, scope, id.as_bytes())?;
        txn.commit()?;
        Ok(deleted)
    }

    pub fn count(&self, filter: Filter) -> Result<usize, Error> {
        self.count_in_scope(&Scope::Default, filter)
    }

    pub fn count_in_scope(&self, scope: &Scope, filter: Filter) -> Result<usize, Error> {
        let txn = self.db.read_txn()?;
        let count = self.db.query_with_txn_scoped(&txn, scope, filter)?.count();
        txn.commit()?;
        Ok(count)
    }

    // Lookup ID: EVENT_ORD_IMPL
    pub fn query(&self, filter: Filter) -> Result<Events, Error> {
        self.query_in_scope(&Scope::Default, filter)
    }

    pub fn query_in_scope(&self, scope: &Scope, filter: Filter) -> Result<Events, Error> {
        let mut events = Events::new(&filter);
        let txn = self.db.read_txn()?;
        let vec_events: Vec<Event> = self
            .db
            .query_with_txn_scoped(&txn, scope, filter)?
            .map(|e| e.into_owned())
            .collect();
        txn.commit()?;
        events.extend(vec_events);

        Ok(events)
    }

    pub fn negentropy_items(&self, filter: Filter) -> Result<Vec<(EventId, Timestamp)>, Error> {
        self.negentropy_items_in_scope(&Scope::Default, filter)
    }

    /// Get negentropy items in a specific scope
    pub fn negentropy_items_in_scope(
        &self,
        scope: &Scope,
        filter: Filter,
    ) -> Result<Vec<(EventId, Timestamp)>, Error> {
        let events = self.query_in_scope(scope, filter)?;
        Ok(events
            .into_iter()
            .map(|e: Event| (e.id, e.created_at))
            .collect())
    }

    pub async fn delete(&self, filter: Filter) -> Result<(), Error> {
        self.delete_in_scope(&Scope::Default, filter).await
    }

    pub async fn delete_in_scope(&self, scope: &Scope, filter: Filter) -> Result<(), Error> {
        let (item, rx) = IngesterItem::delete_with_feedback_scoped(filter, scope.clone());
        self.ingester.send(item).map_err(|_| Error::FlumeSend)?;
        rx.await?
    }

    pub async fn wipe(&self) -> Result<(), Error> {
        self.wipe_scope(&Scope::Default).await
    }

    /// Wipe a specific scope
    pub async fn wipe_scope(&self, scope: &Scope) -> Result<(), Error> {
        let scope = scope.clone();
        self.interact(move |db| {
            let mut txn = db.write_txn()?;
            db.wipe_scope(&mut txn, &scope)?;
            txn.commit()?;
            Ok(())
        })
        .await?
    }

    /// Create a GlobalScopeRegistry for this store
    pub fn create_registry(&self) -> Result<Option<Arc<GlobalScopeRegistry>>, Error> {
        self.db.create_registry()
    }

    /// Register a scope in the registry
    pub fn register_scope(
        &self,
        registry: &Arc<GlobalScopeRegistry>,
        scope: &Scope,
    ) -> Result<(), Error> {
        self.db.register_scope(registry, scope)
    }

    /// Get all scopes from the registry
    pub fn get_registry_scopes(
        &self,
        registry: Option<&Arc<GlobalScopeRegistry>>,
    ) -> Result<Option<Vec<Scope>>, Error> {
        if let Some(reg) = registry {
            // Use the provided registry
            self.db.get_registry_scopes(Some(reg))
        } else {
            // Create a new registry if none provided (backward compatibility)
            let new_registry = self.db.create_registry()?;
            self.db.get_registry_scopes(new_registry.as_ref())
        }
    }

    pub async fn event_by_id_in_scope(
        &self,
        scope: &Scope,
        event_id: EventId,
    ) -> Result<Option<Event>, Error> {
        // Use the synchronous method for direct LMDB access
        self.get_event_by_id_in_scope(scope, &event_id)
    }
}
