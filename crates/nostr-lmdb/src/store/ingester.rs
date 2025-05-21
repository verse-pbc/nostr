// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2025 Rust Nostr Developers
// Distributed under the MIT software license

use std::sync::mpsc::{Receiver, Sender};
use std::thread;

use heed::RwTxn;
use nostr::nips::nip01::Coordinate;
use nostr::{Event, EventId, Kind};
use nostr_database::{FlatBufferBuilder, RejectedReason, SaveEventStatus};
// We're using Scope from super::lmdb
use tokio::sync::oneshot;

use super::error::Error;
use super::lmdb::Lmdb;

pub(super) struct IngesterItem {
    event: Event,
    scope: Option<String>,
    tx: Option<oneshot::Sender<Result<SaveEventStatus, Error>>>,
}

impl IngesterItem {
    // #[inline]
    // pub(super) fn without_feedback(event: Event) -> Self {
    //     Self { event, tx: None }
    // }

    #[must_use]
    pub(super) fn with_feedback(
        event: Event,
        scope: Option<String>,
    ) -> (Self, oneshot::Receiver<Result<SaveEventStatus, Error>>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                event,
                scope,
                tx: Some(tx),
            },
            rx,
        )
    }
}

#[derive(Debug)]
pub(super) struct Ingester {
    db: Lmdb,
    rx: Receiver<IngesterItem>,
}

impl Ingester {
    /// Build and spawn a new ingester
    pub(super) fn run(db: Lmdb) -> Sender<IngesterItem> {
        // Create new asynchronous channel
        let (tx, rx) = std::sync::mpsc::channel();

        // Construct and spawn ingester
        let ingester = Self { db, rx };
        ingester.spawn_ingester();

        // Return ingester sender
        tx
    }

    fn spawn_ingester(self) {
        thread::spawn(move || {
            #[cfg(debug_assertions)]
            tracing::debug!("Ingester thread started");

            let mut fbb = FlatBufferBuilder::with_capacity(70_000);

            // Listen for items
            while let Ok(IngesterItem { event, scope, tx }) = self.rx.recv() {
                // Ingest
                let res = self.ingest_event(event, scope, &mut fbb);

                // If sender is available send the `Result` otherwise log as error
                match tx {
                    // Send to receiver
                    Some(tx) => {
                        let _ = tx.send(res);
                    }
                    // Log error if `Result::Err`
                    None => {
                        if let Err(e) = res {
                            tracing::error!(error = %e, "Event ingestion failed.");
                        }
                    }
                }
            }

            #[cfg(debug_assertions)]
            tracing::debug!("Ingester thread exited");
        });
    }

    fn ingest_event(
        &self,
        event: Event,
        scope_string: Option<String>,
        fbb: &mut FlatBufferBuilder,
    ) -> nostr::Result<SaveEventStatus, Error> {
        // Convert to Scope for internal use
        let scope_str = scope_string.as_deref();
        let scope = Lmdb::to_scope(scope_str)?;
        if event.kind.is_ephemeral() {
            return Ok(SaveEventStatus::Rejected(RejectedReason::Ephemeral));
        }

        // Initial read txn checks
        {
            // Acquire read txn
            let read_txn = self.db.read_txn()?;

            // Already exists
            if self
                .db
                .has_event_in_scope(&read_txn, &scope, event.id.as_bytes())?
            {
                return Ok(SaveEventStatus::Rejected(RejectedReason::Duplicate));
            }

            // Reject event if ID was deleted
            if self.db.is_deleted(&read_txn, &scope, &event.id)? {
                return Ok(SaveEventStatus::Rejected(RejectedReason::Deleted));
            }

            // Reject event if ADDR was deleted after it's created_at date
            // (non-parameterized or parameterized)
            if let Some(coordinate) = event.coordinate() {
                if let Some(time) =
                    self.db
                        .when_is_coordinate_deleted(&read_txn, &scope, &coordinate)?
                {
                    if event.created_at <= time {
                        return Ok(SaveEventStatus::Rejected(RejectedReason::Deleted));
                    }
                }
            }

            read_txn.commit()?;
        }

        // Acquire write transaction
        let mut txn = self.db.write_txn()?;

        // Remove replaceable events being replaced
        if event.kind.is_replaceable() {
            if let Some(stored_event_borrow) = self.db.find_replaceable_event_in_txn(
                &txn,
                &scope,
                &event.pubkey,
                event.kind,
            )? {
                if stored_event_borrow.created_at > event.created_at
                    || (stored_event_borrow.created_at == event.created_at
                        && stored_event_borrow.id > event.id.as_bytes())
                {
                    txn.abort();
                    return Ok(SaveEventStatus::Rejected(RejectedReason::Replaced));
                }
                // New event replaces stored_event_borrow, remove the old one
                let id_to_remove = EventId::from_slice(stored_event_borrow.id).map_err(|_| {
                    Error::Other(
                        "Failed to parse EventId for replaceable event removal".to_string(),
                    )
                })?;
                self.db
                    .remove_event_in_scope(&mut txn, &scope, &id_to_remove)?;
            }
        }

        // Remove parameterized replaceable events being replaced
        if event.kind.is_addressable() {
            if let Some(identifier) = event.tags.identifier() {
                let coordinate: Coordinate =
                    Coordinate::new(event.kind, event.pubkey).identifier(identifier);

                if let Some(stored_event_borrow) =
                    self.db
                        .find_addressable_event_in_txn(&txn, &scope, &coordinate)?
                {
                    if stored_event_borrow.created_at > event.created_at
                        || (stored_event_borrow.created_at == event.created_at
                            && stored_event_borrow.id > event.id.as_bytes())
                    {
                        txn.abort();
                        return Ok(SaveEventStatus::Rejected(RejectedReason::Replaced));
                    }
                    // New event replaces stored_event_borrow, remove the old one
                    let id_to_remove =
                        EventId::from_slice(stored_event_borrow.id).map_err(|_| {
                            Error::Other(
                                "Failed to parse EventId for addressable event removal".to_string(),
                            )
                        })?;
                    self.db
                        .remove_event_in_scope(&mut txn, &scope, &id_to_remove)?;
                }
            }
        }

        // Handle deletion events
        if let Kind::EventDeletion = event.kind {
            let invalid: bool = self.handle_deletion_event(&mut txn, &event, scope_str)?;

            if invalid {
                txn.abort();
                return Ok(SaveEventStatus::Rejected(RejectedReason::InvalidDelete));
            }
        }

        // Store and index the event
        self.db
            .store_in_scope(&mut txn, &scope, fbb, &event)?;

        // Commit
        txn.commit()?;

        Ok(SaveEventStatus::Success)
    }

    fn handle_deletion_event(
        &self,
        txn: &mut RwTxn,
        event: &Event,
        scope_str: Option<&str>,
    ) -> nostr::Result<bool, Error> {
        // Convert to Scope
        let scope = Lmdb::to_scope(scope_str)?;
        
        // Acquire read txn
        let read_txn = self.db.read_txn()?;

        for id in event.tags.event_ids() {
            if let Some(target) = self.db.get_event_by_id_in_txn(&read_txn, &scope, id)? {
                // Author must match
                if target.pubkey != event.pubkey.as_bytes() {
                    return Ok(true);
                }

                // Mark as deleted and remove event
                self.db.mark_deleted(txn, &scope, id)?;
                let target_id = EventId::from_slice(target.id).map_err(|_| {
                    Error::Other("Invalid EventId slice from target event for deletion".to_string())
                })?;
                self.db.remove_event_in_scope(txn, &scope, &target_id)?;
            }
        }

        for coordinate in event.tags.coordinates() {
            // Author must match
            if coordinate.public_key != event.pubkey {
                return Ok(true);
            }

            // Mark deleted
            self.db
                .mark_coordinate_deleted(txn, &scope, &coordinate.borrow(), event.created_at)?;

            // Remove events specified by the coordinate tag, at or before the deletion event's timestamp.
            let found_events_for_coord = self.db.find_events_by_coordinate_scoped(
                &read_txn, // Use read_txn for finding
                &scope,    // scope
                coordinate,
                event.created_at, // Delete events at or before the deletion event's timestamp
            )?;

            // Debug print
            #[cfg(debug_assertions)]
            tracing::debug!(
                "Found {} events for coordinate {:?}:{}:{:?} (created_at <= {})",
                found_events_for_coord.len(),
                coordinate.kind,
                coordinate.public_key,
                coordinate.identifier,
                event.created_at
            );

            for event_to_del_borrow in found_events_for_coord {
                // Ensure we are deleting an event of the kind specified in the coordinate tag
                if event_to_del_borrow.kind == coordinate.kind.as_u16() {
                    let id_to_del = EventId::from_slice(event_to_del_borrow.id)
                        .map_err(|_| Error::Other("Failed to parse EventId in handle_deletion_event for coordinate target".to_string()))?;
                    self.db.remove_event_in_scope(txn, &scope, &id_to_del)?;
                    // Optional: self.db.mark_deleted(txn, scope, &id_to_del)?; if not already covered
                }
            }
            // The old remove_replaceable / remove_addressable calls are replaced by the loop above
        }

        read_txn.commit()?;

        Ok(false)
    }
}
