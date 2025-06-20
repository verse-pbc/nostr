// Copyright (c) 2024 Michael Dilger
// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2025 Rust Nostr Developers
// Distributed under the MIT software license

use std::fs;
use std::path::Path;
use std::sync::Arc;

use flume::Sender;

use nostr_database::prelude::*;
use nostr_database::{FlatBufferBuilder, SaveEventStatus};
use scoped_heed::{GlobalScopeRegistry, Scope};

mod error;
mod ingester;
mod lmdb;
mod transaction;
mod types;

pub use self::error::Error;
use self::ingester::{Ingester, IngesterItem};
use self::lmdb::Lmdb;
pub use self::lmdb::LmdbConfig;
pub use self::transaction::{ReadTransaction, WriteTransaction};

#[derive(Debug, Clone)]
pub struct Store {
    db: Lmdb,
    ingester: Sender<IngesterItem>,
}

impl Store {
    
    pub fn open_with_config<P>(path: P, config: LmdbConfig) -> Result<Store, Error>
    where
        P: AsRef<Path>,
    {
        let path: &Path = path.as_ref();

        // Create the directory if it doesn't exist
        fs::create_dir_all(path)?;

        let db: Lmdb = Lmdb::with_config(path, config)?;
        let ingester: Sender<IngesterItem> = Ingester::run(db.clone());

        Ok(Self { db, ingester })
    }

    /// Create a GlobalScopeRegistry for this store
    pub fn create_registry(&self) -> Result<Option<std::sync::Arc<GlobalScopeRegistry>>, Error> {
        self.db.create_registry()
    }

    /// Register a scope in the registry
    pub fn register_scope(
        &self,
        registry: &std::sync::Arc<GlobalScopeRegistry>,
        scope: &scoped_heed::Scope,
    ) -> Result<(), Error> {
        self.db.register_scope(registry, scope)
    }

    /// Get all scopes from the registry
    pub fn get_registry_scopes(
        &self,
        registry: Option<&Arc<GlobalScopeRegistry>>,
    ) -> Result<Option<Vec<scoped_heed::Scope>>, Error> {
        if let Some(reg) = registry {
            // Use the provided registry
            self.db.get_registry_scopes(Some(reg))
        } else {
            // Create a new registry if none provided (backward compatibility)
            let new_registry = self.db.create_registry()?;
            self.db.get_registry_scopes(new_registry.as_ref())
        }
    }


    /// Store an event.
    pub async fn save_event(&self, event: &Event) -> Result<SaveEventStatus, Error> {
        let (item, rx) = IngesterItem::save_event_with_feedback(event.clone(), Scope::Default);

        // Send to the ingester
        self.ingester.send(item).map_err(|_| Error::MpscSend)?;

        // Wait for a reply
        rx.await?
    }

    /// Get an event by ID
    pub fn get_event_by_id(&self, id: &EventId) -> Result<Option<Event>, Error> {
        self.db.unscoped().event_by_id(id)
    }

    /// Do we have an event
    pub fn has_event(&self, id: &EventId) -> Result<bool, Error> {
        let txn = self.db.read_txn()?;
        let has: bool = self.db.has_event(&txn, id.as_bytes())?;
        txn.commit()?;
        Ok(has)
    }

    /// Is the event deleted
    pub fn event_is_deleted(&self, id: &EventId) -> Result<bool, Error> {
        let txn = self.db.read_txn()?;
        let deleted: bool = self.db.is_deleted(&txn, &Scope::Default, id)?;
        txn.commit()?;
        Ok(deleted)
    }

    #[inline]
    pub fn when_is_coordinate_deleted<'a>(
        &self,
        coordinate: &'a CoordinateBorrow<'a>,
    ) -> Result<Option<Timestamp>, Error> {
        let txn = self.db.read_txn()?;
        let when = self
            .db
            .when_is_coordinate_deleted(&txn, &Scope::Default, coordinate)?;
        txn.commit()?;
        Ok(when)
    }

    pub fn count(&self, filter: Filter) -> Result<usize, Error> {
        let vec_events = self.db.unscoped().query(filter)?;
        Ok(vec_events.len())
    }

    // Lookup ID: EVENT_ORD_IMPL
    pub fn query(&self, filter: Filter) -> Result<Events, Error> {
        let vec_events = self.db.unscoped().query(filter.clone())?;
        let mut events_wrapper = Events::new(&filter);
        events_wrapper.extend(vec_events);
        Ok(events_wrapper)
    }

    pub fn negentropy_items(&self, filter: Filter) -> Result<Vec<(EventId, Timestamp)>, Error> {
        let vec_events: Vec<Event> = self.db.unscoped().query(filter)?;
        let items = vec_events
            .into_iter()
            .map(|e: Event| (e.id, e.created_at))
            .collect();
        Ok(items)
    }

    pub async fn delete(&self, filter: Filter) -> Result<(), Error> {
        let (item, rx) = IngesterItem::delete_with_feedback(filter, Scope::Default);

        // Send to the ingester
        self.ingester.send(item).map_err(|_| Error::MpscSend)?;

        // Wait for a reply
        rx.await?
    }

    pub fn wipe(&self) -> Result<(), Error> {
        let mut txn = self.db.write_txn()?;
        self.db.wipe(&mut txn)?;
        txn.commit()?;
        
        Ok(())
    }

    // New scope-aware methods
    pub async fn delete_in_scope(&self, scope: &Scope, filter: Filter) -> Result<(), Error> {
        // Validate scope if it's named
        if let Scope::Named { name, .. } = scope {
            if name.is_empty() {
                return Err(Error::EmptyScope);
            }
        }

        let (item, rx) = IngesterItem::delete_with_feedback(filter, scope.clone());

        // Send to the ingester
        self.ingester.send(item).map_err(|_| Error::MpscSend)?;

        // Wait for a reply
        rx.await?
    }

    pub async fn save_event_in_scope(
        &self,
        scope: &Scope,
        event: Event,
    ) -> Result<SaveEventStatus, Error> {
        // Validate scope if it's named
        if let Scope::Named { name, .. } = scope {
            if name.is_empty() {
                return Err(Error::EmptyScope);
            }
        }

        let (item, rx) = IngesterItem::save_event_with_feedback(event, scope.clone());

        // Send to the ingester
        self.ingester.send(item).map_err(|_| Error::MpscSend)?;

        // Wait for a reply
        rx.await?
    }

    pub async fn query_in_scope(&self, scope: &Scope, filter: Filter) -> Result<Vec<Event>, Error> {
        // Direct LMDB access - no spawn_blocking needed for reads
        let internal_sv = match scope {
            Scope::Named { name, .. } => {
                if name.is_empty() {
                    return Err(Error::EmptyScope);
                }
                self.db.scoped(name)
            }
            Scope::Default => self.db.unscoped(),
        };
        
        // Perform the query directly
        let result = internal_sv.query(filter)?;
        
        // Yield to runtime if we processed many items
        if result.len() > 100 {
            tokio::task::yield_now().await;
        }
        
        Ok(result)
    }

    pub async fn event_by_id_in_scope(
        &self,
        scope: &Scope,
        event_id: EventId,
    ) -> Result<Option<Event>, Error> {
        // Direct LMDB access - no spawn_blocking needed for reads
        let internal_sv = match scope {
            Scope::Named { name, .. } => {
                if name.is_empty() {
                    return Err(Error::EmptyScope);
                }
                self.db.scoped(name)
            }
            Scope::Default => self.db.unscoped(),
        };
        
        // Perform the lookup directly
        internal_sv.event_by_id(&event_id)
    }

    pub async fn count_in_scope(&self, scope: &Scope, filter: Filter) -> Result<usize, Error> {
        // Direct LMDB access - no spawn_blocking needed for reads
        let internal_sv = match scope {
            Scope::Named { name, .. } => {
                if name.is_empty() {
                    return Err(Error::EmptyScope);
                }
                self.db.scoped(name)
            }
            Scope::Default => self.db.unscoped(),
        };
        
        // Perform the count directly
        let count = internal_sv.count(filter)?;
        
        // Yield to runtime if the count operation was potentially heavy
        if count > 1000 {
            tokio::task::yield_now().await;
        }
        
        Ok(count)
    }

    // Transaction management methods

    /// Create a new read transaction
    pub fn read_transaction(&self) -> Result<ReadTransaction<'_>, Error> {
        ReadTransaction::new(&self.db)
    }

    /// Create a new write transaction
    pub fn write_transaction(&self) -> Result<WriteTransaction<'_>, Error> {
        WriteTransaction::new(&self.db)
    }

    /// Save an event within a transaction
    pub fn save_event_with_txn(
        &self,
        txn: &mut WriteTransaction,
        scope: &Scope,
        event: &Event,
    ) -> Result<SaveEventStatus, Error> {
        // Create a FlatBufferBuilder
        let mut fbb = FlatBufferBuilder::new();

        // Get the database reference first
        let db = txn.db();

        // Get mutable reference to the underlying RwTxn
        let rwtxn = txn.as_mut().ok_or(Error::TransactionAlreadyCommitted)?;

        // Use the transaction-aware method
        db.save_event_with_txn(rwtxn, scope, &mut fbb, event)
    }

    /// Delete events matching a filter within a transaction
    pub fn delete_with_txn(
        &self,
        txn: &mut WriteTransaction,
        scope: &Scope,
        filter: Filter,
    ) -> Result<usize, Error> {
        // Get the database reference first
        let db = txn.db();

        // Get mutable reference to the underlying RwTxn
        let rwtxn = txn.as_mut().ok_or(Error::TransactionAlreadyCommitted)?;

        // Query events to delete
        let events = db.query_with_txn(rwtxn, scope, filter)?;
        let count = events.len();

        // Delete each event
        for event in events {
            db.delete_by_id_with_txn(rwtxn, scope, &event.id)?;
        }

        Ok(count)
    }

    /// Query events within a read transaction
    pub fn query_with_txn(
        &self,
        txn: &ReadTransaction,
        scope: &Scope,
        filter: Filter,
    ) -> Result<Vec<Event>, Error> {
        txn.db().query_with_txn(txn, scope, filter)
    }

    /// Delete a specific event by ID within a transaction
    pub fn delete_by_id_with_txn(
        &self,
        txn: &mut WriteTransaction,
        scope: &Scope,
        event_id: &EventId,
    ) -> Result<bool, Error> {
        // Get the database reference first
        let db = txn.db();

        let rwtxn = txn.as_mut().ok_or(Error::TransactionAlreadyCommitted)?;

        db.delete_by_id_with_txn(rwtxn, scope, event_id)
    }
}
