// Copyright (c) 2024 Michael Dilger
// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2025 Rust Nostr Developers
// Distributed under the MIT software license

use std::fs;
use std::path::Path;
use std::sync::mpsc::Sender;

use async_utility::task;
use nostr_database::prelude::*;

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
    pub fn open<P>(path: P) -> Result<Store, Error>
    where
        P: AsRef<Path>,
    {
        let path: &Path = path.as_ref();

        // Create the directory if it doesn't exist
        fs::create_dir_all(path)?;

        let db: Lmdb = Lmdb::new(path)?;
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
        let (item, rx) = IngesterItem::with_feedback(event.clone(), None);

        // Send to the ingester
        // This will never block the current thread according to `std::sync::mpsc::Sender` docs
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
        let deleted: bool = self.db.is_deleted(&txn, None, id)?;
        txn.commit()?;
        Ok(deleted)
    }

    #[inline]
    pub fn when_is_coordinate_deleted<'a>(
        &self,
        coordinate: &'a CoordinateBorrow<'a>,
    ) -> Result<Option<Timestamp>, Error> {
        let txn = self.db.read_txn()?;
        let when = self.db.when_is_coordinate_deleted(&txn, None, coordinate)?;
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
        self.interact(move |db| {
            let read_txn = db.read_txn()?;
            let mut txn = db.write_txn()?;

            db.delete(&read_txn, &mut txn, filter)?;

            read_txn.commit()?;
            txn.commit()?;

            Ok(())
        })
        .await?
    }

    pub async fn wipe(&self) -> Result<(), Error> {
        self.interact(move |db| {
            let mut txn = db.write_txn()?;
            db.wipe(&mut txn)?;
            txn.commit()?;
            Ok(())
        })
        .await?
    }

    // New scope-aware methods
    pub async fn delete_in_scope(&self, scope: Option<&str>, filter: Filter) -> Result<(), Error> {
        let internal_sv = match scope {
            Some(s_name) => {
                if s_name.is_empty() {
                    return Err(Error::EmptyScope);
                }
                self.db.scoped(s_name) // self.db is Lmdb
            }
            None => self.db.unscoped(),
        };
        internal_sv.delete(filter).await // Calls the new async delete on internal ScopedView
    }

    pub async fn save_event_in_scope(
        &self,
        scope: Option<&str>,
        event: Event,
    ) -> Result<SaveEventStatus, Error> {
        if let Some(s_name) = scope {
            if s_name.is_empty() {
                return Err(Error::EmptyScope);
            }
        }

        let scope_string = scope.map(|s| s.to_string());
        let (item, rx) = IngesterItem::with_feedback(event, scope_string);

        // Send to the ingester
        self.ingester.send(item).map_err(|_| Error::MpscSend)?;

        // Wait for a reply
        rx.await?
    }

    pub async fn query_in_scope(
        &self,
        scope: Option<&str>,
        filter: Filter,
    ) -> Result<Vec<Event>, Error> {
        if let Some(s_name) = scope {
            if s_name.is_empty() {
                return Err(Error::EmptyScope);
            }
        }
        let db_clone = self.db.clone();
        let scope_string = scope.map(|s| s.to_string());
        task::spawn_blocking(move || {
            let internal_sv = match scope_string.as_deref() {
                Some(s_name) => {
                    // Empty check moved outside spawn_blocking
                    db_clone.scoped(s_name)
                }
                None => db_clone.unscoped(),
            };
            internal_sv.query(filter)
        })
        .await?
    }

    pub async fn event_by_id_in_scope(
        &self,
        scope: Option<&str>,
        event_id: EventId,
    ) -> Result<Option<Event>, Error> {
        if let Some(s_name) = scope {
            if s_name.is_empty() {
                return Err(Error::EmptyScope);
            }
        }
        let db_clone = self.db.clone();
        let scope_string = scope.map(|s| s.to_string());
        task::spawn_blocking(move || {
            let internal_sv = match scope_string.as_deref() {
                Some(s_name) => {
                    // Empty check moved outside spawn_blocking
                    db_clone.scoped(s_name)
                }
                None => db_clone.unscoped(),
            };
            internal_sv.event_by_id(&event_id)
        })
        .await?
    }

    pub async fn count_in_scope(
        &self,
        scope: Option<&str>,
        filter: Filter,
    ) -> Result<usize, Error> {
        if let Some(s_name) = scope {
            if s_name.is_empty() {
                return Err(Error::EmptyScope);
            }
        }
        let db_clone = self.db.clone();
        let scope_string = scope.map(|s| s.to_string());
        task::spawn_blocking(move || {
            let internal_sv = match scope_string.as_deref() {
                Some(s_name) => db_clone.scoped(s_name),
                None => db_clone.unscoped(),
            };
            match internal_sv.count(filter) {
                Ok(count_val) => Ok(count_val),
                Err(e) => Err(e),
            }
        })
        .await?
    }
}
