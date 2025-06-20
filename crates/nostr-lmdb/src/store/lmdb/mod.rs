// Copyright (c) 2024 Michael Dilger
// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2025 Rust Nostr Developers
// Distributed under the MIT software license

use std::collections::BTreeSet;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use heed::{Env, EnvOpenOptions, RoTxn, RwTxn, WithTls};
use nostr::event::borrow::EventBorrow;
use nostr::event::tag::TagStandard;
use nostr::nips::nip01::Coordinate;
use nostr::{Event, EventId, Kind, Timestamp};
use nostr_database::flatbuffers::FlatBufferDecodeBorrowed;
use nostr_database::prelude::*;
use nostr_database::{nostr, FlatBufferBuilder, FlatBufferEncode, RejectedReason, SaveEventStatus};
use scoped_heed::{
    scoped_database_options, GlobalScopeRegistry, Scope, ScopedBytesDatabase, ScopedDbError,
};

mod config;
mod index;

pub use self::config::LmdbConfig;
use super::error::Error;
use super::types::DatabaseFilter;

const EVENT_ID_ALL_ZEROS: [u8; 32] = [0; 32];
const EVENT_ID_ALL_255: [u8; 32] = [255; 32];


// Type alias for the complex iterator type to reduce clippy warnings
type ScopedIterator<'txn> =
    Box<dyn Iterator<Item = Result<(&'txn [u8], &'txn [u8]), ScopedDbError>> + 'txn>;

pub struct ScopedView<'a> {
    db: &'a Lmdb,
    scope: Scope,
}

#[allow(dead_code)]
impl ScopedView<'_> {
    pub fn save_event(&self, event: &Event) -> Result<SaveEventStatus, Error> {
        let mut wtxn = self.db.env.write_txn()?;
        let scope = &self.scope;

        if event.kind == Kind::EventDeletion {
            for tag in event.tags.iter() {
                if let Some(tag_standard) = tag.as_standardized() {
                    match tag_standard {
                        TagStandard::Event {
                            event_id: event_id_to_delete,
                            ..
                        } => {
                            let can_delete = self
                                .db
                                .get_event_by_id_in_txn(&wtxn, scope, event_id_to_delete)?
                                .is_some_and(|borrow| borrow.pubkey == event.pubkey.as_bytes());

                            if can_delete {
                                self.db.remove_event_in_scope(
                                    &mut wtxn,
                                    scope,
                                    event_id_to_delete,
                                )?;
                                self.db.mark_deleted(&mut wtxn, scope, event_id_to_delete)?;
                            }
                        }
                        TagStandard::Coordinate {
                            coordinate: coord_to_delete,
                            ..
                        } => {
                            if coord_to_delete.public_key == event.pubkey {
                                // Find all events matching the coordinate to delete them
                                let events_to_remove_borrows =
                                    self.db.find_events_by_coordinate_scoped(
                                        &wtxn, // RwTxn can be used as RoTxn here for finding
                                        scope,
                                        coord_to_delete,
                                        Timestamp::max(), // Find all versions up to now
                                    )?;

                                // Collect EventIds to release borrows from wtxn before mutable operations
                                let event_ids_to_delete: Vec<EventId> = events_to_remove_borrows
                                    .iter()
                                    .map(|eb| EventId::from_slice(eb.id))
                                    .collect::<Result<Vec<_>, _>>() // Handle potential error from from_slice
                                    .map_err(|_| Error::Other("Invalid event ID slice during coordinate deletion prep".to_string()))?;

                                // Drop events_to_remove_borrows to release immutable borrow on wtxn
                                drop(events_to_remove_borrows);

                                for id_val in event_ids_to_delete {
                                    // Remove the event and its indexes
                                    self.db.remove_event_in_scope(&mut wtxn, scope, &id_val)?;
                                    // Mark it as deleted (for NIP-09 tracking, if still desired)
                                    self.db.mark_deleted(&mut wtxn, scope, &id_val)?;
                                }

                                // The original mark_coordinate_deleted might be for a different purpose (e.g., NIP-09 receipt).
                                // For now, we focus on deleting the target events.
                                // If this is still needed for NIP-09 deletion event tracking, it can be kept:
                                /* self.db.mark_coordinate_deleted(
                                    &mut wtxn,
                                    scope,
                                    &coord_to_delete.borrow(),
                                    event.created_at,
                                )?; */
                            }
                        }
                        _ => {}
                    }
                }
            }
        } else if event.kind.is_replaceable() {
            let event_id_owned = event.id;
            let existing_event_id_to_remove: Option<EventId> = if let Some(existing_event_borrow) =
                self.db
                    .find_replaceable_event_in_txn(&wtxn, scope, &event.pubkey, event.kind)?
            {
                if event.created_at < existing_event_borrow.created_at
                    || (event.created_at == existing_event_borrow.created_at
                        && event_id_owned.as_bytes() < existing_event_borrow.id)
                {
                    return Ok(SaveEventStatus::Rejected(RejectedReason::Replaced));
                }
                Some(EventId::from_slice(existing_event_borrow.id).unwrap())
            } else {
                None
            };

            if let Some(id_to_remove) = existing_event_id_to_remove {
                self.db
                    .remove_event_in_scope(&mut wtxn, scope, &id_to_remove)?;
            }
        } else if event.kind.is_addressable() {
            if let Some(identifier) = event.tags.identifier() {
                let coordinate =
                    Coordinate::new(event.kind, event.pubkey).identifier(identifier.to_string());
                let event_id_owned = event.id;

                let existing_event_id_to_remove: Option<EventId> =
                    if let Some(existing_event_borrow) =
                        self.db
                            .find_addressable_event_in_txn(&wtxn, scope, &coordinate)?
                    {
                        if event.created_at < existing_event_borrow.created_at
                            || (event.created_at == existing_event_borrow.created_at
                                && event_id_owned.as_bytes() < existing_event_borrow.id)
                        {
                            return Ok(SaveEventStatus::Rejected(RejectedReason::Replaced));
                        }
                        Some(EventId::from_slice(existing_event_borrow.id).unwrap())
                    } else {
                        None
                    };

                if let Some(id_to_remove) = existing_event_id_to_remove {
                    self.db
                        .remove_event_in_scope(&mut wtxn, scope, &id_to_remove)?;
                }
            }
        }

        let mut fbb = FlatBufferBuilder::new();
        let event_id_bytes = event.id.as_bytes();
        let event_bytes = event.encode(&mut fbb);

        self.db
            .events_sdb
            .put(&mut wtxn, scope, event_id_bytes, event_bytes)?;

        let ci_key_bytes: Vec<u8> =
            index::make_ci_index_key(&event.created_at, event.id.as_bytes());
        self.db
            .ci_index_sdb
            .put(&mut wtxn, scope, &ci_key_bytes, event_id_bytes)?;

        let akc_key_bytes: Vec<u8> = index::make_akc_index_key(
            event.pubkey.as_bytes(),
            event.kind.as_u16(),
            &event.created_at,
            event.id.as_bytes(),
        );
        self.db
            .akc_index_sdb
            .put(&mut wtxn, scope, &akc_key_bytes, event_id_bytes)?;

        let ac_key_bytes: Vec<u8> = index::make_ac_index_key(
            event.pubkey.as_bytes(),
            &event.created_at,
            event.id.as_bytes(),
        );
        self.db
            .ac_index_sdb
            .put(&mut wtxn, scope, &ac_key_bytes, event_id_bytes)?;

        for tag in event.tags.iter() {
            if let Some(single_letter_tag) = tag.single_letter_tag() {
                if let Some(tag_value) = tag.content() {
                    let atc_key_bytes: Vec<u8> = index::make_atc_index_key(
                        event.pubkey.as_bytes(),
                        &single_letter_tag,
                        tag_value,
                        &event.created_at,
                        event.id.as_bytes(),
                    );
                    self.db
                        .atc_index_sdb
                        .put(&mut wtxn, scope, &atc_key_bytes, event_id_bytes)?;

                    let ktc_key_bytes: Vec<u8> = index::make_ktc_index_key(
                        event.kind.as_u16(),
                        &single_letter_tag,
                        tag_value,
                        &event.created_at,
                        event.id.as_bytes(),
                    );
                    self.db
                        .ktc_index_sdb
                        .put(&mut wtxn, scope, &ktc_key_bytes, event_id_bytes)?;

                    let tc_key_bytes: Vec<u8> = index::make_tc_index_key(
                        &single_letter_tag,
                        tag_value,
                        &event.created_at,
                        event.id.as_bytes(),
                    );
                    self.db
                        .tc_index_sdb
                        .put(&mut wtxn, scope, &tc_key_bytes, event_id_bytes)?;
                }
            }
        }

        wtxn.commit()?;
        Ok(SaveEventStatus::Success)
    }

    pub fn event_by_id(&self, event_id: &EventId) -> Result<Option<Event>, Error> {
        let rtxn = self.db.read_txn()?;
        let scope = &self.scope;
        let event_id_bytes = event_id.as_bytes();

        match self.db.events_sdb.get(&rtxn, scope, event_id_bytes)? {
            Some(event_bytes) => {
                let event = EventBorrow::decode(event_bytes)?.into_owned();
                Ok(Some(event))
            }
            None => Ok(None),
        }
    }

    fn ci_iter<'txn>(
        &'txn self,
        txn: &'txn RoTxn,
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<ScopedIterator<'txn>, Error> {
        let start_prefix_data: Vec<u8> = index::make_ci_index_key(until, &EVENT_ID_ALL_ZEROS);
        let end_prefix_data: Vec<u8> = index::make_ci_index_key(since, &EVENT_ID_ALL_255);

        let range_bounds = (
            Bound::Included(start_prefix_data.as_slice()),
            Bound::Excluded(end_prefix_data.as_slice()),
        );

        let underlying_iterator = self
            .db
            .ci_index_sdb
            .range(txn, &self.scope, &range_bounds)
            .map_err(Error::from)?;

        let mapped_iterator = underlying_iterator;

        Ok(Box::new(mapped_iterator))
    }

    fn tc_iter<'txn>(
        &'txn self,
        txn: &'txn RoTxn,
        tag_name: &SingleLetterTag,
        tag_value: &str,
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<ScopedIterator<'txn>, Error> {
        let start_prefix_vec =
            index::make_tc_index_key(tag_name, tag_value, until, &EVENT_ID_ALL_ZEROS);
        let end_prefix_vec =
            index::make_tc_index_key(tag_name, tag_value, since, &EVENT_ID_ALL_255);

        let range_bounds = (
            Bound::Included(start_prefix_vec.as_slice()),
            Bound::Excluded(end_prefix_vec.as_slice()),
        );

        let underlying_iterator = self
            .db
            .tc_index_sdb
            .range(txn, &self.scope, &range_bounds)
            .map_err(Error::from)?;

        let mapped_iterator = underlying_iterator;

        Ok(Box::new(mapped_iterator))
    }

    fn ac_iter<'txn>(
        &'txn self,
        txn: &'txn RoTxn,
        author: &[u8; 32],
        since: Timestamp,
        until: Timestamp,
    ) -> Result<ScopedIterator<'txn>, Error> {
        let start_prefix_vec = index::make_ac_index_key(author, &until, &EVENT_ID_ALL_ZEROS);
        let end_prefix_vec = index::make_ac_index_key(author, &since, &EVENT_ID_ALL_255);

        let range_bounds = (
            Bound::Included(start_prefix_vec.as_slice()),
            Bound::Excluded(end_prefix_vec.as_slice()),
        );

        let underlying_iterator = self
            .db
            .ac_index_sdb
            .range(txn, &self.scope, &range_bounds)
            .map_err(Error::from)?;

        let mapped_iterator = underlying_iterator;

        Ok(Box::new(mapped_iterator))
    }

    fn akc_iter<'txn>(
        &'txn self,
        txn: &'txn RoTxn,
        author: &[u8; 32],
        kind: u16,
        since: Timestamp,
        until: Timestamp,
    ) -> Result<ScopedIterator<'txn>, Error> {
        let start_prefix_vec = index::make_akc_index_key(author, kind, &until, &EVENT_ID_ALL_ZEROS);
        let end_prefix_vec = index::make_akc_index_key(author, kind, &since, &EVENT_ID_ALL_255);

        let range_bounds = (
            Bound::Included(start_prefix_vec.as_slice()),
            Bound::Excluded(end_prefix_vec.as_slice()),
        );

        let underlying_iterator = self
            .db
            .akc_index_sdb
            .range(txn, &self.scope, &range_bounds)
            .map_err(Error::from)?;

        let mapped_iterator = underlying_iterator;

        Ok(Box::new(mapped_iterator))
    }

    fn atc_iter<'txn>(
        &'txn self,
        txn: &'txn RoTxn,
        author: &[u8; 32],
        tag_name: &SingleLetterTag,
        tag_value: &str,
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<ScopedIterator<'txn>, Error> {
        let start_prefix_vec: Vec<u8> =
            index::make_atc_index_key(author, tag_name, tag_value, until, &EVENT_ID_ALL_ZEROS);
        let end_prefix_vec: Vec<u8> =
            index::make_atc_index_key(author, tag_name, tag_value, since, &EVENT_ID_ALL_255);

        let range_bounds = (
            Bound::Included(start_prefix_vec.as_slice()),
            Bound::Excluded(end_prefix_vec.as_slice()),
        );

        let underlying_iterator = self
            .db
            .atc_index_sdb
            .range(txn, &self.scope, &range_bounds)
            .map_err(Error::from)?;

        let mapped_iterator = underlying_iterator;

        Ok(Box::new(mapped_iterator))
    }

    fn ktc_iter<'txn>(
        &'txn self,
        txn: &'txn RoTxn,
        kind: u16,
        tag_name: &SingleLetterTag,
        tag_value: &str,
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<ScopedIterator<'txn>, Error> {
        let start_prefix_vec =
            index::make_ktc_index_key(kind, tag_name, tag_value, until, &EVENT_ID_ALL_ZEROS);
        let end_prefix_vec =
            index::make_ktc_index_key(kind, tag_name, tag_value, since, &EVENT_ID_ALL_255);

        let range_bounds = (
            Bound::Included(start_prefix_vec.as_slice()),
            Bound::Excluded(end_prefix_vec.as_slice()),
        );

        let underlying_iterator = self
            .db
            .ktc_index_sdb
            .range(txn, &self.scope, &range_bounds)
            .map_err(Error::from)?;

        let mapped_iterator = underlying_iterator;

        Ok(Box::new(mapped_iterator))
    }

    fn iterate_filter_until_limit<'txn>(
        &'txn self,
        txn: &'txn RoTxn,
        _scope: &Scope,
        filter: &DatabaseFilter,
        iter: ScopedIterator<'txn>,
        _since: &mut Timestamp,
        limit_opt: Option<usize>,
        output: &mut Vec<Event>,
        seen_event_ids: &mut BTreeSet<[u8; 32]>,
    ) -> Result<(), Error> {
        for result in iter {
            if let Some(limit) = limit_opt {
                if output.len() >= limit {
                    return Ok(());
                }
            }

            let (_key, event_id_bytes_slice) = result.map_err(Error::from)?;

            let event_id_bytes: [u8; 32] = event_id_bytes_slice.try_into().map_err(|_| {
                Error::FlatBuffers(nostr_database::flatbuffers::Error::from(
                    ::flatbuffers::InvalidFlatbuffer::RangeOutOfBounds {
                        range: 0..0,
                        error_trace: Default::default(),
                    },
                ))
            })?;

            if seen_event_ids.contains(&event_id_bytes) {
                continue;
            }

            let event_data_bytes = self.db.events_sdb.get(txn, &self.scope, &event_id_bytes)?;

            if let Some(raw_event_bytes) = event_data_bytes {
                // Check if the event has been deleted
                let event_id = EventId::from_slice(&event_id_bytes)
                    .map_err(|_| Error::Other("Invalid event ID slice in iterate".to_string()))?;
                if self.db.is_deleted(txn, &self.scope, &event_id)? {
                    continue; // Skip deleted events
                }

                let event_borrow = EventBorrow::decode(raw_event_bytes)?;

                if filter.match_event(&event_borrow) {
                    let event = event_borrow.into_owned();
                    output.push(event);
                    seen_event_ids.insert(event_id_bytes);
                }
            } else {
                // Event ID from index not found in main events table - data inconsistency
            }
        }
        Ok(())
    }

    fn iterate_filter_and_count<'txn>(
        &'txn self,
        txn: &'txn RoTxn,
        filter: &DatabaseFilter,
        iter: ScopedIterator<'txn>,
        _since: &mut Timestamp,
        limit_opt: Option<usize>,
        current_count: &mut usize,
        seen_event_ids: &mut BTreeSet<[u8; 32]>,
    ) -> Result<(), Error> {
        for result in iter {
            if limit_opt.is_some() && *current_count >= limit_opt.unwrap() {
                return Ok(());
            }

            let (_key, event_id_bytes_slice) = result.map_err(Error::from)?;

            let event_id_bytes: [u8; 32] = event_id_bytes_slice.try_into().map_err(|_| {
                Error::FlatBuffers(nostr_database::flatbuffers::Error::from(
                    ::flatbuffers::InvalidFlatbuffer::RangeOutOfBounds {
                        range: 0..0,
                        error_trace: Default::default(),
                    },
                ))
            })?;

            if seen_event_ids.contains(&event_id_bytes) {
                continue;
            }

            let event_data_bytes = self.db.events_sdb.get(txn, &self.scope, &event_id_bytes)?;

            if let Some(raw_event_bytes) = event_data_bytes {
                // Check if the event has been deleted
                let event_id = EventId::from_slice(&event_id_bytes)
                    .map_err(|_| Error::Other("Invalid event ID slice in count".to_string()))?;
                if self.db.is_deleted(txn, &self.scope, &event_id)? {
                    continue; // Skip deleted events
                }

                let event_borrow = EventBorrow::decode(raw_event_bytes)?;

                if filter.match_event(&event_borrow) {
                    *current_count += 1;
                    seen_event_ids.insert(event_id_bytes);
                }
            } else {
                // Event ID from index not found in main events table - data inconsistency
            }
        }
        Ok(())
    }

    pub fn query(&self, filter: Filter) -> Result<Vec<Event>, Error> {
        let txn = self.db.read_txn()?;

        if let (Some(since_filter), Some(until_filter)) = (filter.since, filter.until) {
            if since_filter > until_filter {
                return Ok(Vec::new());
            }
        }

        let mut collected_events_vec: Vec<Event> = Vec::new();
        let mut seen_event_ids: BTreeSet<[u8; 32]> = BTreeSet::new();

        let limit_opt: Option<usize> = filter.limit;
        let mut query_since = filter.since.unwrap_or_else(Timestamp::min);
        let query_until = filter.until.unwrap_or_else(Timestamp::max);

        let db_filter: DatabaseFilter = filter.into();

        if !db_filter.ids.is_empty() {
            for id_bytes in db_filter.ids.iter() {
                if limit_opt.is_some() && collected_events_vec.len() >= limit_opt.unwrap() {
                    break;
                }
                if seen_event_ids.contains(id_bytes) {
                    continue;
                }
                if let Some(raw_event_bytes) =
                    self.db.events_sdb.get(&txn, &self.scope, id_bytes)?
                {
                    // Check if the event has been deleted
                    let event_id = EventId::from_slice(id_bytes)
                        .map_err(|_| Error::Other("Invalid event ID slice".to_string()))?;
                    if self.db.is_deleted(&txn, &self.scope, &event_id)? {
                        continue; // Skip deleted events
                    }

                    let event_borrow = EventBorrow::decode(raw_event_bytes)?;
                    if event_borrow.created_at >= query_since
                        && event_borrow.created_at <= query_until
                        && db_filter.match_event(&event_borrow)
                    {
                        let event = event_borrow.into_owned();
                        collected_events_vec.push(event);
                        seen_event_ids.insert(*id_bytes);
                    }
                } else {
                    // Event in filter.ids not found
                }
            }
        }

        if collected_events_vec.len() < limit_opt.unwrap_or(usize::MAX) {
            if !db_filter.authors.is_empty() && !db_filter.kinds.is_empty() {
                let mut since_val = query_since;

                for author in db_filter.authors.iter() {
                    for kind_val in db_filter.kinds.iter() {
                        let iter = self.db.akc_iter_scoped(
                            &txn,
                            &self.scope,
                            author,
                            *kind_val,
                            since_val,
                            query_until,
                        )?;
                        self.iterate_filter_until_limit(
                            &txn,
                            &self.scope,
                            &db_filter,
                            iter,
                            &mut since_val,
                            limit_opt,
                            &mut collected_events_vec,
                            &mut seen_event_ids,
                        )?;
                        if limit_opt.is_some() && collected_events_vec.len() >= limit_opt.unwrap() {
                            break;
                        }
                    }
                    if limit_opt.is_some() && collected_events_vec.len() >= limit_opt.unwrap() {
                        break;
                    }
                }
            } else if !db_filter.authors.is_empty() && !db_filter.generic_tags.is_empty() {
                let mut since_val = query_since;

                for author in db_filter.authors.iter() {
                    for (tagname, set) in db_filter.generic_tags.iter() {
                        for tag_value in set.iter() {
                            let iter = self.db.atc_iter_scoped(
                                &txn,
                                &self.scope,
                                author,
                                tagname,
                                tag_value,
                                &since_val,
                                &query_until,
                            )?;
                            self.iterate_filter_until_limit(
                                &txn,
                                &self.scope,
                                &db_filter,
                                iter,
                                &mut since_val,
                                limit_opt,
                                &mut collected_events_vec,
                                &mut seen_event_ids,
                            )?;
                            if limit_opt.is_some()
                                && collected_events_vec.len() >= limit_opt.unwrap()
                            {
                                break;
                            }
                        }
                        if limit_opt.is_some() && collected_events_vec.len() >= limit_opt.unwrap() {
                            break;
                        }
                    }
                    if limit_opt.is_some() && collected_events_vec.len() >= limit_opt.unwrap() {
                        break;
                    }
                }
            } else if !db_filter.kinds.is_empty() && !db_filter.generic_tags.is_empty() {
                let mut since_val = query_since;

                for kind_val in db_filter.kinds.iter() {
                    for (tag_name, set) in db_filter.generic_tags.iter() {
                        for tag_value in set.iter() {
                            let iter = self.db.ktc_iter_scoped(
                                &txn,
                                &self.scope,
                                *kind_val,
                                tag_name,
                                tag_value,
                                &since_val,
                                &query_until,
                            )?;
                            self.iterate_filter_until_limit(
                                &txn,
                                &self.scope,
                                &db_filter,
                                iter,
                                &mut since_val,
                                limit_opt,
                                &mut collected_events_vec,
                                &mut seen_event_ids,
                            )?;
                            if limit_opt.is_some()
                                && collected_events_vec.len() >= limit_opt.unwrap()
                            {
                                break;
                            }
                        }
                        if limit_opt.is_some() && collected_events_vec.len() >= limit_opt.unwrap() {
                            break;
                        }
                    }
                    if limit_opt.is_some() && collected_events_vec.len() >= limit_opt.unwrap() {
                        break;
                    }
                }
            } else if !db_filter.generic_tags.is_empty() {
                let mut since_val = query_since;

                for (tag_name, set) in db_filter.generic_tags.iter() {
                    for tag_value in set.iter() {
                        let iter = self.db.tc_iter_scoped(
                            &txn,
                            &self.scope,
                            tag_name,
                            tag_value,
                            &since_val,
                            &query_until,
                        )?;
                        self.iterate_filter_until_limit(
                            &txn,
                            &self.scope,
                            &db_filter,
                            iter,
                            &mut since_val,
                            limit_opt,
                            &mut collected_events_vec,
                            &mut seen_event_ids,
                        )?;
                        if limit_opt.is_some() && collected_events_vec.len() >= limit_opt.unwrap() {
                            break;
                        }
                    }
                    if limit_opt.is_some() && collected_events_vec.len() >= limit_opt.unwrap() {
                        break;
                    }
                }
            } else if !db_filter.authors.is_empty() {
                let mut since_val = query_since;

                for author in db_filter.authors.iter() {
                    let iter = self.db.ac_iter_scoped(
                        &txn,
                        &self.scope,
                        author,
                        since_val,
                        query_until,
                    )?;
                    self.iterate_filter_until_limit(
                        &txn,
                        &self.scope,
                        &db_filter,
                        iter,
                        &mut since_val,
                        limit_opt,
                        &mut collected_events_vec,
                        &mut seen_event_ids,
                    )?;
                    if limit_opt.is_some() && collected_events_vec.len() >= limit_opt.unwrap() {
                        break;
                    }
                }
            } else {
                let iter = self
                    .db
                    .ci_iter_scoped(&txn, &self.scope, &query_since, &query_until)?;
                self.iterate_filter_until_limit(
                    &txn,
                    &self.scope,
                    &db_filter,
                    iter,
                    &mut query_since,
                    limit_opt,
                    &mut collected_events_vec,
                    &mut seen_event_ids,
                )?;
            }
        }

        let ordered_unique_events: BTreeSet<Event> = collected_events_vec.into_iter().collect();
        let mut final_events: Vec<Event> = ordered_unique_events.into_iter().collect();

        if let Some(limit) = limit_opt {
            final_events.truncate(limit);
        }

        Ok(final_events)
    }

    pub fn count(&self, filter: Filter) -> Result<usize, Error> {
        let txn = self.db.read_txn()?;

        if let (Some(since_filter), Some(until_filter)) = (filter.since, filter.until) {
            if since_filter > until_filter {
                return Ok(0);
            }
        }

        let mut current_event_count: usize = 0;
        let mut seen_event_ids: BTreeSet<[u8; 32]> = BTreeSet::new();

        let limit_opt: Option<usize> = filter.limit;
        let mut query_since = filter.since.unwrap_or_else(Timestamp::min);
        let query_until = filter.until.unwrap_or_else(Timestamp::max);

        let db_filter: DatabaseFilter = filter.into();

        if !db_filter.ids.is_empty() {
            for id_bytes in db_filter.ids.iter() {
                if limit_opt.is_some() && current_event_count >= limit_opt.unwrap() {
                    break;
                }
                if seen_event_ids.contains(id_bytes) {
                    continue;
                }
                if let Some(raw_event_bytes) =
                    self.db.events_sdb.get(&txn, &self.scope, id_bytes)?
                {
                    let event_borrow = EventBorrow::decode(raw_event_bytes)?;
                    if event_borrow.created_at >= query_since
                        && event_borrow.created_at <= query_until
                        && db_filter.match_event(&event_borrow)
                    {
                        current_event_count += 1;
                        seen_event_ids.insert(*id_bytes);
                    }
                } else {
                    // Event in filter.ids not found
                }
            }
        }

        if current_event_count < limit_opt.unwrap_or(usize::MAX) {
            if !db_filter.authors.is_empty() && !db_filter.kinds.is_empty() {
                let mut since_val = query_since;
                for author in db_filter.authors.iter() {
                    for kind_val in db_filter.kinds.iter() {
                        let iter = self.db.akc_iter_scoped(
                            &txn,
                            &self.scope,
                            author,
                            *kind_val,
                            since_val,
                            query_until,
                        )?;
                        self.iterate_filter_and_count(
                            &txn,
                            &db_filter,
                            iter,
                            &mut since_val,
                            limit_opt,
                            &mut current_event_count,
                            &mut seen_event_ids,
                        )?;
                        if limit_opt.is_some() && current_event_count >= limit_opt.unwrap() {
                            break;
                        }
                    }
                    if limit_opt.is_some() && current_event_count >= limit_opt.unwrap() {
                        break;
                    }
                }
            } else if !db_filter.authors.is_empty() && !db_filter.generic_tags.is_empty() {
                let mut since_val = query_since;
                for author in db_filter.authors.iter() {
                    for (tagname, set) in db_filter.generic_tags.iter() {
                        for tag_value in set.iter() {
                            let iter = self.db.atc_iter_scoped(
                                &txn,
                                &self.scope,
                                author,
                                tagname,
                                tag_value,
                                &since_val,
                                &query_until,
                            )?;
                            self.iterate_filter_and_count(
                                &txn,
                                &db_filter,
                                iter,
                                &mut since_val,
                                limit_opt,
                                &mut current_event_count,
                                &mut seen_event_ids,
                            )?;
                            if limit_opt.is_some() && current_event_count >= limit_opt.unwrap() {
                                break;
                            }
                        }
                        if limit_opt.is_some() && current_event_count >= limit_opt.unwrap() {
                            break;
                        }
                    }
                    if limit_opt.is_some() && current_event_count >= limit_opt.unwrap() {
                        break;
                    }
                }
            } else if !db_filter.kinds.is_empty() && !db_filter.generic_tags.is_empty() {
                let mut since_val = query_since;

                for kind_val in db_filter.kinds.iter() {
                    for (tag_name, set) in db_filter.generic_tags.iter() {
                        for tag_value in set.iter() {
                            let iter = self.db.ktc_iter_scoped(
                                &txn,
                                &self.scope,
                                *kind_val,
                                tag_name,
                                tag_value,
                                &since_val,
                                &query_until,
                            )?;
                            self.iterate_filter_and_count(
                                &txn,
                                &db_filter,
                                iter,
                                &mut since_val,
                                limit_opt,
                                &mut current_event_count,
                                &mut seen_event_ids,
                            )?;
                            if limit_opt.is_some() && current_event_count >= limit_opt.unwrap() {
                                break;
                            }
                        }
                        if limit_opt.is_some() && current_event_count >= limit_opt.unwrap() {
                            break;
                        }
                    }
                    if limit_opt.is_some() && current_event_count >= limit_opt.unwrap() {
                        break;
                    }
                }
            } else if !db_filter.generic_tags.is_empty() {
                let mut since_val = query_since;

                for (tag_name, set) in db_filter.generic_tags.iter() {
                    for tag_value in set.iter() {
                        let iter = self.db.tc_iter_scoped(
                            &txn,
                            &self.scope,
                            tag_name,
                            tag_value,
                            &since_val,
                            &query_until,
                        )?;
                        self.iterate_filter_and_count(
                            &txn,
                            &db_filter,
                            iter,
                            &mut since_val,
                            limit_opt,
                            &mut current_event_count,
                            &mut seen_event_ids,
                        )?;
                        if limit_opt.is_some() && current_event_count >= limit_opt.unwrap() {
                            break;
                        }
                    }
                    if limit_opt.is_some() && current_event_count >= limit_opt.unwrap() {
                        break;
                    }
                }
            } else if !db_filter.authors.is_empty() {
                let mut since_val = query_since;

                for author in db_filter.authors.iter() {
                    let iter = self.db.ac_iter_scoped(
                        &txn,
                        &self.scope,
                        author,
                        since_val,
                        query_until,
                    )?;
                    self.iterate_filter_and_count(
                        &txn,
                        &db_filter,
                        iter,
                        &mut since_val,
                        limit_opt,
                        &mut current_event_count,
                        &mut seen_event_ids,
                    )?;
                    if limit_opt.is_some() && current_event_count >= limit_opt.unwrap() {
                        break;
                    }
                }
            } else {
                let iter = self
                    .db
                    .ci_iter_scoped(&txn, &self.scope, &query_since, &query_until)?;
                self.iterate_filter_and_count(
                    &txn,
                    &db_filter,
                    iter,
                    &mut query_since,
                    limit_opt,
                    &mut current_event_count,
                    &mut seen_event_ids,
                )?;
            }
        }
        Ok(current_event_count)
    }

    pub fn delete(&self, filter: Filter) -> Result<(), Error> {
        let events_to_delete: Vec<Event> = self.query(filter)?;
        
        let mut wtxn = self.db.env.write_txn()?;
        let scope_ref = &self.scope;

        for event_val in events_to_delete {
            let event_id_bytes = event_val.id.as_bytes();

            self.db
                .events_sdb
                .delete(&mut wtxn, scope_ref, event_id_bytes)?;

            let ci_key_bytes =
                index::make_ci_index_key(&event_val.created_at, event_val.id.as_bytes());
            self.db
                .ci_index_sdb
                .delete(&mut wtxn, scope_ref, &ci_key_bytes)?;

            let akc_key_bytes = index::make_akc_index_key(
                event_val.pubkey.as_bytes(),
                event_val.kind.as_u16(),
                &event_val.created_at,
                event_val.id.as_bytes(),
            );
            self.db
                .akc_index_sdb
                .delete(&mut wtxn, scope_ref, &akc_key_bytes)?;

            let ac_key_bytes = index::make_ac_index_key(
                event_val.pubkey.as_bytes(),
                &event_val.created_at,
                event_val.id.as_bytes(),
            );
            self.db
                .ac_index_sdb
                .delete(&mut wtxn, scope_ref, &ac_key_bytes)?;

            for tag in event_val.tags.iter() {
                if let (Some(tag_name), Some(tag_value)) =
                    (tag.single_letter_tag(), tag.content())
                {
                    let atc_index_key: Vec<u8> = index::make_atc_index_key(
                        event_val.pubkey.as_bytes(),
                        &tag_name,
                        tag_value,
                        &event_val.created_at,
                        event_val.id.as_bytes(),
                    );
                    self.db
                        .atc_index_sdb
                        .delete(&mut wtxn, scope_ref, &atc_index_key)?;

                    let ktc_index_key: Vec<u8> = index::make_ktc_index_key(
                        event_val.kind.as_u16(),
                        &tag_name,
                        tag_value,
                        &event_val.created_at,
                        event_val.id.as_bytes(),
                    );
                    self.db
                        .ktc_index_sdb
                        .delete(&mut wtxn, scope_ref, &ktc_index_key)?;

                    let tc_index_key: Vec<u8> = index::make_tc_index_key(
                        &tag_name,
                        tag_value,
                        &event_val.created_at,
                        event_val.id.as_bytes(),
                    );
                    self.db
                        .tc_index_sdb
                        .delete(&mut wtxn, scope_ref, &tc_index_key)?;
                }
            }
        }
        
        wtxn.commit()?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Lmdb {
    env: Env,
    events_sdb: ScopedBytesDatabase,
    ci_index_sdb: ScopedBytesDatabase,
    tc_index_sdb: ScopedBytesDatabase,
    ac_index_sdb: ScopedBytesDatabase,
    akc_index_sdb: ScopedBytesDatabase,
    atc_index_sdb: ScopedBytesDatabase,
    ktc_index_sdb: ScopedBytesDatabase,
    deleted_ids_sdb: ScopedBytesDatabase,
    deleted_coordinates_sdb: ScopedBytesDatabase,
}

impl Lmdb {
    pub(crate) fn create_registry(&self) -> Result<Option<Arc<GlobalScopeRegistry>>, Error> {
        let mut wtxn = self.env.write_txn()?;
        let registry = Arc::new(GlobalScopeRegistry::new(&self.env, &mut wtxn)?);
        wtxn.commit()?;
        Ok(Some(registry))
    }

    pub(crate) fn register_scope(
        &self,
        registry: &Arc<GlobalScopeRegistry>,
        scope: &Scope,
    ) -> Result<(), Error> {
        // Skip registration for default scope
        if scope.is_default() {
            return Ok(());
        }

        let mut wtxn = self.env.write_txn()?;
        registry.register_scope(&mut wtxn, scope)?;
        wtxn.commit()?;
        Ok(())
    }

    pub(crate) fn get_registry_scopes(
        &self,
        registry: Option<&Arc<GlobalScopeRegistry>>,
    ) -> Result<Option<Vec<Scope>>, Error> {
        if let Some(reg) = registry {
            let txn = self.env.read_txn()?;
            let scopes = reg.list_all_scopes(&txn)?;
            txn.commit()?;
            Ok(Some(scopes))
        } else {
            Ok(None)
        }
    }
    pub(crate) fn with_config<P>(path: P, config: LmdbConfig) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let mut env_builder = EnvOpenOptions::new();
        env_builder
            .max_dbs(config.max_dbs)
            .map_size(config.map_size)
            .max_readers(config.max_readers);
        
        // Apply flags if any
        if !config.flags.is_empty() {
            unsafe {
                env_builder.flags(config.flags);
            }
        }
        
        let env = unsafe { env_builder.open(path)? };

        let mut txn = env.write_txn()?;

        // Create a global registry for all databases
        let registry = Arc::new(GlobalScopeRegistry::new(&env, &mut txn)?);

        let events_sdb = scoped_database_options(&env, registry.clone())
            .raw_bytes()
            .name("events")
            .unnamed_for_default()
            .create(&mut txn)?;

        let ci_index_sdb = scoped_database_options(&env, registry.clone())
            .raw_bytes()
            .name("ci")
            .create(&mut txn)?;

        let tc_index_sdb = scoped_database_options(&env, registry.clone())
            .raw_bytes()
            .name("tci")
            .create(&mut txn)?;

        let ac_index_sdb = scoped_database_options(&env, registry.clone())
            .raw_bytes()
            .name("aci")
            .create(&mut txn)?;

        let akc_index_sdb = scoped_database_options(&env, registry.clone())
            .raw_bytes()
            .name("akci")
            .create(&mut txn)?;

        let atc_index_sdb = scoped_database_options(&env, registry.clone())
            .raw_bytes()
            .name("atci")
            .create(&mut txn)?;

        let ktc_index_sdb = scoped_database_options(&env, registry.clone())
            .raw_bytes()
            .name("ktci")
            .create(&mut txn)?;

        let deleted_ids_sdb = scoped_database_options(&env, registry.clone())
            .raw_bytes()
            .name("deleted-ids")
            .create(&mut txn)?;

        let deleted_coordinates_sdb = scoped_database_options(&env, registry.clone())
            .raw_bytes()
            .name("deleted-coordinates")
            .create(&mut txn)?;

        txn.commit()?;

        Ok(Self {
            env,
            events_sdb,
            ci_index_sdb,
            tc_index_sdb,
            ac_index_sdb,
            akc_index_sdb,
            atc_index_sdb,
            ktc_index_sdb,
            deleted_ids_sdb,
            deleted_coordinates_sdb,
        })
    }

    #[inline]
    pub(crate) fn read_txn(&self) -> Result<RoTxn<'_, WithTls>, Error> {
        self.env.read_txn().map_err(Error::from)
    }

    #[inline]
    pub(crate) fn write_txn(&self) -> Result<heed::RwTxn<'_>, Error> {
        self.env.write_txn().map_err(Error::from)
    }

    pub(crate) fn store_in_scope(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        fbb: &mut FlatBufferBuilder,
        event: &Event,
    ) -> Result<(), Error> {
        let id: &[u8] = event.id.as_bytes();

        self.events_sdb.put(txn, scope, id, event.encode(fbb))?;
        let ci_index_key: Vec<u8> =
            index::make_ci_index_key(&event.created_at, event.id.as_bytes());
        self.ci_index_sdb.put(txn, scope, &ci_index_key, id)?;

        let akc_index_key: Vec<u8> = index::make_akc_index_key(
            event.pubkey.as_bytes(),
            event.kind.as_u16(),
            &event.created_at,
            event.id.as_bytes(),
        );
        self.akc_index_sdb.put(txn, scope, &akc_index_key, id)?;

        let ac_index_key: Vec<u8> = index::make_ac_index_key(
            event.pubkey.as_bytes(),
            &event.created_at,
            event.id.as_bytes(),
        );
        self.ac_index_sdb.put(txn, scope, &ac_index_key, id)?;

        for tag in event.tags.iter() {
            if let (Some(tag_name), Some(tag_value)) = (tag.single_letter_tag(), tag.content()) {
                let atc_index_key: Vec<u8> = index::make_atc_index_key(
                    event.pubkey.as_bytes(),
                    &tag_name,
                    tag_value,
                    &event.created_at,
                    event.id.as_bytes(),
                );
                self.atc_index_sdb.put(txn, scope, &atc_index_key, id)?;

                let ktc_index_key: Vec<u8> = index::make_ktc_index_key(
                    event.kind.as_u16(),
                    &tag_name,
                    tag_value,
                    &event.created_at,
                    event.id.as_bytes(),
                );
                self.ktc_index_sdb.put(txn, scope, &ktc_index_key, id)?;

                let tc_index_key: Vec<u8> = index::make_tc_index_key(
                    &tag_name,
                    tag_value,
                    &event.created_at,
                    event.id.as_bytes(),
                );
                self.tc_index_sdb.put(txn, scope, &tc_index_key, id)?;
            }
        }

        Ok(())
    }

    pub(crate) fn wipe(&self, txn: &mut RwTxn) -> Result<(), Error> {
        let default_scope = &Scope::Default;
        self.events_sdb.clear(txn, default_scope)?;
        self.ci_index_sdb.clear(txn, default_scope)?;
        self.tc_index_sdb.clear(txn, default_scope)?;
        self.ac_index_sdb.clear(txn, default_scope)?;
        self.akc_index_sdb.clear(txn, default_scope)?;
        self.atc_index_sdb.clear(txn, default_scope)?;
        self.ktc_index_sdb.clear(txn, default_scope)?;
        self.deleted_ids_sdb.clear(txn, default_scope)?;
        self.deleted_coordinates_sdb.clear(txn, default_scope)?;
        Ok(())
    }

    #[inline]
    pub(crate) fn has_event(&self, txn: &RoTxn, event_id: &[u8; 32]) -> Result<bool, Error> {
        self.has_event_in_scope(txn, &Scope::Default, event_id)
    }

    pub(crate) fn has_event_in_scope(
        &self,
        txn: &RoTxn,
        scope: &Scope,
        event_id: &[u8; 32],
    ) -> Result<bool, Error> {
        Ok(self
            .get_event_by_id_in_txn(txn, scope, &EventId::from_slice(event_id).unwrap())?
            .is_some())
    }

    #[inline]
    pub(crate) fn get_event_by_id_in_txn<'txn>(
        &self,
        txn: &'txn RoTxn,
        scope: &Scope,
        event_id: &EventId,
    ) -> Result<Option<EventBorrow<'txn>>, Error> {
        match self.events_sdb.get(txn, scope, event_id.as_bytes())? {
            Some(bytes) => Ok(Some(EventBorrow::decode(bytes)?)),
            None => Ok(None),
        }
    }

    pub fn delete_in_scope(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        filter: Filter,
    ) -> Result<(), Error> {
        // Collect event IDs first to avoid borrowing issues
        let events_to_delete: Vec<EventId> = {
            let events_iter = self.query(txn, scope, filter)?;
            let mut ids = Vec::new();
            for event_result in events_iter {
                let event_borrow = event_result?;
                ids.push(EventId::from_slice(event_borrow.id)?);
            }
            ids
        };
        
        // Now delete the collected events
        for event_id in events_to_delete {
            self.remove_event_in_scope(txn, scope, &event_id)?;
        }
        Ok(())
    }

    pub fn query<'txn>(
        &'txn self,
        txn: &'txn RoTxn,
        scope: &Scope,
        filter: Filter,
    ) -> Result<Box<dyn Iterator<Item = Result<EventBorrow<'txn>, Error>> + 'txn>, Error> {
        if let (Some(since), Some(until)) = (filter.since, filter.until) {
            if since > until {
                return Ok(Box::new(std::iter::empty()));
            }
        }

        let mut output: BTreeSet<EventBorrow<'txn>> = BTreeSet::new();

        let limit: Option<usize> = filter.limit;
        let since_ts = filter.since.unwrap_or_else(Timestamp::min);
        let query_until = filter.until.unwrap_or_else(Timestamp::max);

        let db_filter: DatabaseFilter = filter.into();

        if !db_filter.ids.is_empty() {
            for id in db_filter.ids.iter() {
                if let Some(limit_val) = limit {
                    if output.len() >= limit_val {
                        break;
                    }
                }

                if let Some(event_borrow) =
                    self.get_event_by_id_in_txn(txn, scope, &EventId::from_slice(id).unwrap())?
                {
                    if event_borrow.created_at >= since_ts
                        && event_borrow.created_at <= query_until
                        && db_filter.match_event(&event_borrow)
                    {
                        output.insert(event_borrow);
                    }
                }
            }
        } else if !db_filter.authors.is_empty() && !db_filter.kinds.is_empty() {
            let current_since_for_akc_optim = since_ts;
            for author in db_filter.authors.iter() {
                for kind_val in db_filter.kinds.iter() {
                    if limit.is_some_and(|l| output.len() >= l) {
                        break;
                    }
                    let iter = self.akc_iter_scoped(
                        txn,
                        scope,
                        author,
                        *kind_val,
                        current_since_for_akc_optim,
                        query_until,
                    )?;

                    'per_event: for result in iter {
                        if limit.is_some_and(|l| output.len() >= l) {
                            break 'per_event;
                        }
                        let (_key, value) = result?;
                        let event_borrow = self
                            .get_event_by_id_in_txn(
                                txn,
                                scope,
                                &EventId::from_slice(value).unwrap(),
                            )?
                            .ok_or(Error::NotFound)?;

                        if event_borrow.created_at < since_ts
                            || event_borrow.created_at > query_until
                        {
                            continue 'per_event;
                        }

                        if db_filter.match_event(&event_borrow) {
                            output.insert(event_borrow);

                            if Kind::from(*kind_val).is_replaceable() {
                                break 'per_event;
                            }
                        }
                    }
                }
                if limit.is_some_and(|l| output.len() >= l) {
                    break;
                }
            }
        } else if !db_filter.authors.is_empty() && !db_filter.generic_tags.is_empty() {
            for author in db_filter.authors.iter() {
                for (tagname, set) in db_filter.generic_tags.iter() {
                    for tag_value in set.iter() {
                        if limit.is_some_and(|l| output.len() >= l) {
                            break;
                        }
                        let iter = self.atc_iter_scoped(
                            txn,
                            scope,
                            author,
                            tagname,
                            tag_value,
                            &since_ts,
                            &query_until,
                        )?;
                        for result in iter {
                            if limit.is_some_and(|l| output.len() >= l) {
                                break;
                            }
                            let (_key, value) = result?;
                            let event_borrow = self
                                .get_event_by_id_in_txn(
                                    txn,
                                    scope,
                                    &EventId::from_slice(value).unwrap(),
                                )?
                                .ok_or(Error::NotFound)?;
                            if event_borrow.created_at >= since_ts
                                && event_borrow.created_at <= query_until
                                && db_filter.match_event(&event_borrow)
                            {
                                output.insert(event_borrow);
                            }
                        }
                    }
                    if limit.is_some_and(|l| output.len() >= l) {
                        break;
                    }
                }
                if limit.is_some_and(|l| output.len() >= l) {
                    break;
                }
            }
        } else if !db_filter.kinds.is_empty() && !db_filter.generic_tags.is_empty() {
            for kind_val in db_filter.kinds.iter() {
                for (tag_name, set) in db_filter.generic_tags.iter() {
                    for tag_value in set.iter() {
                        if limit.is_some_and(|l| output.len() >= l) {
                            break;
                        }
                        let iter = self.ktc_iter_scoped(
                            txn,
                            scope,
                            *kind_val,
                            tag_name,
                            tag_value,
                            &since_ts,
                            &query_until,
                        )?;
                        for result in iter {
                            if limit.is_some_and(|l| output.len() >= l) {
                                break;
                            }
                            let (_key, value) = result?;
                            let event_borrow = self
                                .get_event_by_id_in_txn(
                                    txn,
                                    scope,
                                    &EventId::from_slice(value).unwrap(),
                                )?
                                .ok_or(Error::NotFound)?;
                            if event_borrow.created_at >= since_ts
                                && event_borrow.created_at <= query_until
                                && db_filter.match_event(&event_borrow)
                            {
                                output.insert(event_borrow);
                            }
                        }
                    }
                    if limit.is_some_and(|l| output.len() >= l) {
                        break;
                    }
                }
                if limit.is_some_and(|l| output.len() >= l) {
                    break;
                }
            }
        } else if !db_filter.generic_tags.is_empty() {
            for (tag_name, set) in db_filter.generic_tags.iter() {
                for tag_value in set.iter() {
                    if limit.is_some_and(|l| output.len() >= l) {
                        break;
                    }
                    let iter = self.tc_iter_scoped(
                        txn,
                        scope,
                        tag_name,
                        tag_value,
                        &since_ts,
                        &query_until,
                    )?;
                    for result in iter {
                        if limit.is_some_and(|l| output.len() >= l) {
                            break;
                        }
                        let (_key, value) = result?;
                        let event_borrow = self
                            .get_event_by_id_in_txn(
                                txn,
                                scope,
                                &EventId::from_slice(value).unwrap(),
                            )?
                            .ok_or(Error::NotFound)?;
                        if event_borrow.created_at >= since_ts
                            && event_borrow.created_at <= query_until
                            && db_filter.match_event(&event_borrow)
                        {
                            output.insert(event_borrow);
                        }
                    }
                }
                if limit.is_some_and(|l| output.len() >= l) {
                    break;
                }
            }
        } else if !db_filter.authors.is_empty() {
            for author in db_filter.authors.iter() {
                if limit.is_some_and(|l| output.len() >= l) {
                    break;
                }
                let iter = self.ac_iter_scoped(txn, scope, author, since_ts, query_until)?;
                for result in iter {
                    if limit.is_some_and(|l| output.len() >= l) {
                        break;
                    }
                    let (_key, value) = result?;
                    let event_borrow = self
                        .get_event_by_id_in_txn(txn, scope, &EventId::from_slice(value).unwrap())?
                        .ok_or(Error::NotFound)?;
                    if db_filter.match_event(&event_borrow) {
                        output.insert(event_borrow);
                    }
                }
            }
        } else {
            let iter = self.ci_iter_scoped(txn, scope, &since_ts, &query_until)?;
            for result in iter {
                if limit.is_some_and(|l| output.len() >= l) {
                    break;
                }
                let (_key, value) = result?;
                let event_borrow = self
                    .get_event_by_id_in_txn(txn, scope, &EventId::from_slice(value).unwrap())?
                    .ok_or(Error::NotFound)?;
                if db_filter.match_event(&event_borrow) {
                    output.insert(event_borrow);
                }
            }
        }

        let result_iter = output.into_iter().map(Ok);
        Ok(match limit {
            Some(l) => Box::new(result_iter.take(l)),
            None => Box::new(result_iter),
        })
    }

    pub fn find_replaceable_event_in_txn<'txn>(
        &self,
        txn: &'txn RoTxn,
        scope: &Scope,
        author: &PublicKey,
        kind: Kind,
    ) -> Result<Option<EventBorrow<'txn>>, Error> {
        if !kind.is_replaceable() {
            return Err(Error::WrongEventKind);
        }
        let iter = self.akc_iter_scoped(
            txn,
            scope,
            author.as_bytes(),
            kind.as_u16(),
            Timestamp::min(),
            Timestamp::max(),
        )?;

        for result in iter {
            let (_key, id_bytes) = result?;
            if let Some(event_borrow) =
                self.get_event_by_id_in_txn(txn, scope, &EventId::from_slice(id_bytes).unwrap())?
            {
                return Ok(Some(event_borrow));
            }
        }

        Ok(None)
    }

    pub fn find_addressable_event_in_txn<'txn>(
        &self,
        txn: &'txn RoTxn,
        scope: &Scope,
        coordinate: &Coordinate,
    ) -> Result<Option<EventBorrow<'txn>>, Error> {
        if !coordinate.kind.is_addressable() {
            return Err(Error::WrongEventKind);
        }
        let iter = self.atc_iter_scoped(
            txn,
            scope,
            coordinate.public_key.as_bytes(),
            &SingleLetterTag::lowercase(Alphabet::D),
            &coordinate.identifier,
            &Timestamp::min(),
            &Timestamp::max(),
        )?;

        for result in iter {
            let (_key, id_bytes) = result?;
            if let Some(event_borrow) =
                self.get_event_by_id_in_txn(txn, scope, &EventId::from_slice(id_bytes).unwrap())?
            {
                if event_borrow.kind == coordinate.kind.as_u16() {
                    return Ok(Some(event_borrow));
                }
            }
        }

        Ok(None)
    }

    pub(crate) fn is_deleted(
        &self,
        txn: &RoTxn,
        scope: &Scope,
        event_id: &EventId,
    ) -> Result<bool, Error> {
        Ok(self
            .deleted_ids_sdb
            .get(txn, scope, event_id.as_bytes())?
            .is_some())
    }

    pub(crate) fn mark_deleted(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        event_id: &EventId,
    ) -> Result<(), Error> {
        self.deleted_ids_sdb
            .put(txn, scope, event_id.as_bytes(), &[])?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn mark_coordinate_deleted(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        coordinate: &CoordinateBorrow,
        when: Timestamp,
    ) -> Result<(), Error> {
        let key: Vec<u8> = index::make_coordinate_index_key(coordinate);
        self.deleted_coordinates_sdb
            .put(txn, scope, &key, &when.as_u64().to_ne_bytes())?;
        Ok(())
    }

    pub(crate) fn when_is_coordinate_deleted<'txn>(
        &self,
        txn: &'txn RoTxn,
        scope: &Scope,
        coordinate: &CoordinateBorrow<'txn>,
    ) -> Result<Option<Timestamp>, Error> {
        let key: Vec<u8> = index::make_coordinate_index_key(coordinate);
        Ok(self
            .deleted_coordinates_sdb
            .get(txn, scope, &key)?
            .map(|bytes| Timestamp::from_secs(u64::from_ne_bytes(bytes.try_into().unwrap()))))
    }

    pub(crate) fn ci_iter_scoped<'txn>(
        &'txn self,
        txn: &'txn RoTxn,
        scope: &Scope,
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<ScopedIterator<'txn>, Error> {
        let start_prefix_data: Vec<u8> = index::make_ci_index_key(until, &EVENT_ID_ALL_ZEROS);
        let end_prefix_data: Vec<u8> = index::make_ci_index_key(since, &EVENT_ID_ALL_255);

        let range_bounds = (
            Bound::Included(start_prefix_data.as_slice()),
            Bound::Excluded(end_prefix_data.as_slice()),
        );

        self.ci_index_sdb
            .range(txn, scope, &range_bounds)
            .map_err(Error::from)
    }

    pub(crate) fn tc_iter_scoped<'txn>(
        &'txn self,
        txn: &'txn RoTxn,
        scope: &Scope,
        tag_name: &SingleLetterTag,
        tag_value: &str,
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<ScopedIterator<'txn>, Error> {
        let start_prefix_vec =
            index::make_tc_index_key(tag_name, tag_value, until, &EVENT_ID_ALL_ZEROS);
        let end_prefix_vec =
            index::make_tc_index_key(tag_name, tag_value, since, &EVENT_ID_ALL_255);

        let range_bounds = (
            Bound::Included(start_prefix_vec.as_slice()),
            Bound::Excluded(end_prefix_vec.as_slice()),
        );
        self.tc_index_sdb
            .range(txn, scope, &range_bounds)
            .map_err(Error::from)
    }

    pub(crate) fn ac_iter_scoped<'txn>(
        &'txn self,
        txn: &'txn RoTxn,
        scope: &Scope,
        author: &[u8; 32],
        since: Timestamp,
        until: Timestamp,
    ) -> Result<ScopedIterator<'txn>, Error> {
        let start_prefix_vec = index::make_ac_index_key(author, &until, &EVENT_ID_ALL_ZEROS);
        let end_prefix_vec = index::make_ac_index_key(author, &since, &EVENT_ID_ALL_255);

        let range_bounds = (
            Bound::Included(start_prefix_vec.as_slice()),
            Bound::Excluded(end_prefix_vec.as_slice()),
        );
        self.ac_index_sdb
            .range(txn, scope, &range_bounds)
            .map_err(Error::from)
    }

    pub(crate) fn akc_iter_scoped<'txn>(
        &'txn self,
        txn: &'txn RoTxn,
        scope: &Scope,
        author: &[u8; 32],
        kind: u16,
        since: Timestamp,
        until: Timestamp,
    ) -> Result<ScopedIterator<'txn>, Error> {
        let start_prefix_vec = index::make_akc_index_key(author, kind, &until, &EVENT_ID_ALL_ZEROS);
        let end_prefix_vec = index::make_akc_index_key(author, kind, &since, &EVENT_ID_ALL_255);

        let range_bounds = (
            Bound::Included(start_prefix_vec.as_slice()),
            Bound::Excluded(end_prefix_vec.as_slice()),
        );

        let underlying_iterator = self
            .akc_index_sdb
            .range(txn, scope, &range_bounds)
            .map_err(Error::from)?;

        let mapped_iterator = underlying_iterator;

        Ok(Box::new(mapped_iterator))
    }

    pub(crate) fn atc_iter_scoped<'txn>(
        &'txn self,
        txn: &'txn RoTxn,
        scope: &Scope,
        author: &[u8; 32],
        tag_name: &SingleLetterTag,
        tag_value: &str,
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<ScopedIterator<'txn>, Error> {
        let start_prefix_vec: Vec<u8> =
            index::make_atc_index_key(author, tag_name, tag_value, until, &EVENT_ID_ALL_ZEROS);
        let end_prefix_vec: Vec<u8> =
            index::make_atc_index_key(author, tag_name, tag_value, since, &EVENT_ID_ALL_255);

        let range_bounds = (
            Bound::Included(start_prefix_vec.as_slice()),
            Bound::Excluded(end_prefix_vec.as_slice()),
        );

        let underlying_iterator = self
            .atc_index_sdb
            .range(txn, scope, &range_bounds)
            .map_err(Error::from)?;

        let mapped_iterator = underlying_iterator;

        Ok(Box::new(mapped_iterator))
    }

    pub(crate) fn ktc_iter_scoped<'txn>(
        &'txn self,
        txn: &'txn RoTxn,
        scope: &Scope,
        kind: u16,
        tag_name: &SingleLetterTag,
        tag_value: &str,
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<ScopedIterator<'txn>, Error> {
        let start_prefix_vec =
            index::make_ktc_index_key(kind, tag_name, tag_value, until, &EVENT_ID_ALL_ZEROS);
        let end_prefix_vec =
            index::make_ktc_index_key(kind, tag_name, tag_value, since, &EVENT_ID_ALL_255);

        let range_bounds = (
            Bound::Included(start_prefix_vec.as_slice()),
            Bound::Excluded(end_prefix_vec.as_slice()),
        );

        let underlying_iterator = self
            .ktc_index_sdb
            .range(txn, scope, &range_bounds)
            .map_err(Error::from)?;

        let mapped_iterator = underlying_iterator;

        Ok(Box::new(mapped_iterator))
    }

    pub(crate) fn find_events_by_coordinate_scoped<'txn>(
        &'txn self,
        txn: &'txn RoTxn,
        scope: &Scope,
        coordinate: &Coordinate,
        before_or_at: Timestamp,
    ) -> Result<Vec<EventBorrow<'txn>>, Error> {
        let mut events = Vec::new();

        // For addressable events, we need to search by d tag
        let tag_name = SingleLetterTag::from_char('d')
            .map_err(|e| Error::Other(format!("Invalid single letter tag char 'd': {}", e)))?;

        // The tag value is just the identifier
        let tag_value = coordinate.identifier.as_str();

        let iter = self.ktc_iter_scoped(
            txn,
            scope,
            coordinate.kind.into(),
            &tag_name,
            tag_value,
            &Timestamp::min(),
            &before_or_at,
        )?;

        let mut seen_ids = BTreeSet::new();

        for result in iter {
            let (_key, event_id_bytes) = result?;
            let event_id = EventId::from_slice(event_id_bytes)
                .map_err(|_| Error::Other("Invalid event ID slice from index".to_string()))?;

            if seen_ids.contains(&event_id) {
                continue;
            }

            if let Some(event_borrow) = self.get_event_by_id_in_txn(txn, scope, &event_id)? {
                // Check that the author matches
                if event_borrow.pubkey == coordinate.public_key.as_bytes()
                    && event_borrow.kind == coordinate.kind.as_u16()
                    && event_borrow.created_at <= before_or_at
                {
                    events.push(event_borrow);
                    seen_ids.insert(event_id);
                }
            } else {
                // Log inconsistency or handle error: event ID from index not found in main DB
                // For now, just skip
            }
        }
        Ok(events)
    }

    pub(crate) fn scoped(&self, scope_name: &str) -> ScopedView<'_> {
        let scope = Scope::named(scope_name).unwrap_or_else(|_| {
            // This should never happen since we validate scope_name earlier
            tracing::warn!(
                "Invalid scope name '{}', falling back to default scope",
                scope_name
            );
            Scope::Default
        });

        ScopedView { db: self, scope }
    }

    pub(crate) fn unscoped(&self) -> ScopedView<'_> {
        ScopedView {
            db: self,
            scope: Scope::Default,
        }
    }

    pub(crate) fn remove_event_in_scope(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        event_id: &EventId,
    ) -> Result<bool, Error> {
        let (id_copy, created_at_copy, pubkey_copy, kind_copy, sl_tags_copy) = {
            if let Some(event_borrow) = self.get_event_by_id_in_txn(txn, scope, event_id)? {
                let id_val: [u8; 32] = *event_borrow.id;
                let created_at_val: Timestamp = event_borrow.created_at;
                let pubkey_val: [u8; 32] = *event_borrow.pubkey;
                let kind_val: u16 = event_borrow.kind;

                let mut sl_tags: Vec<(SingleLetterTag, String)> = Vec::new();
                for a_cow_tag in event_borrow.tags.iter() {
                    if let Some((slt, tag_value_str)) = a_cow_tag.extract() {
                        sl_tags.push((slt, tag_value_str.to_string()));
                    }
                }
                (id_val, created_at_val, pubkey_val, kind_val, sl_tags)
            } else {
                return Ok(false); // Event not found, nothing to remove
            }
        };

        // Now, perform deletions using `txn` mutably and the copied data
        self.events_sdb.delete(txn, scope, &id_copy)?;

        let ci_key: Vec<u8> = index::make_ci_index_key(&created_at_copy, &id_copy);
        self.ci_index_sdb.delete(txn, scope, &ci_key)?;

        let akc_key: Vec<u8> =
            index::make_akc_index_key(&pubkey_copy, kind_copy, &created_at_copy, &id_copy);
        self.akc_index_sdb.delete(txn, scope, &akc_key)?;

        let ac_key: Vec<u8> = index::make_ac_index_key(&pubkey_copy, &created_at_copy, &id_copy);
        self.ac_index_sdb.delete(txn, scope, &ac_key)?;

        for (slt, value_str) in sl_tags_copy.iter() {
            let atc_index_key: Vec<u8> = index::make_atc_index_key(
                &pubkey_copy,
                slt,       // Pass the SingleLetterTag directly
                value_str, // Pass the owned String as &str
                &created_at_copy,
                &id_copy,
            );
            self.atc_index_sdb.delete(txn, scope, &atc_index_key)?;

            let ktc_index_key: Vec<u8> =
                index::make_ktc_index_key(kind_copy, slt, value_str, &created_at_copy, &id_copy);
            self.ktc_index_sdb.delete(txn, scope, &ktc_index_key)?;

            let tc_index_key: Vec<u8> =
                index::make_tc_index_key(slt, value_str, &created_at_copy, &id_copy);
            self.tc_index_sdb.delete(txn, scope, &tc_index_key)?;
        }
        Ok(true)
    }

    // Transaction-aware methods for batched operations

    pub(crate) fn save_event_with_txn(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        fbb: &mut FlatBufferBuilder,
        event: &Event,
    ) -> Result<SaveEventStatus, Error> {
        // Check if event is ephemeral
        if event.kind.is_ephemeral() {
            return Ok(SaveEventStatus::Rejected(RejectedReason::Ephemeral));
        }

        // Check if event already exists
        if self.has_event_in_scope(txn, scope, event.id.as_bytes())? {
            return Ok(SaveEventStatus::Rejected(RejectedReason::Duplicate));
        }

        // Check if event ID was deleted
        if self.is_deleted(txn, scope, &event.id)? {
            return Ok(SaveEventStatus::Rejected(RejectedReason::Deleted));
        }

        // Check if coordinate was deleted after event's created_at date
        if let Some(coordinate) = event.coordinate() {
            if let Some(time) = self.when_is_coordinate_deleted(txn, scope, &coordinate)? {
                if event.created_at <= time {
                    return Ok(SaveEventStatus::Rejected(RejectedReason::Deleted));
                }
            }
        }

        // Handle deletion events
        if event.kind == Kind::EventDeletion {
            let mut _has_valid_deletion = false;
            let mut has_invalid_deletion = false;

            for tag in event.tags.iter() {
                if let Some(tag_standard) = tag.as_standardized() {
                    match tag_standard {
                        TagStandard::Event {
                            event_id: event_id_to_delete,
                            ..
                        } => {
                            if let Some(target_event) =
                                self.get_event_by_id_in_txn(txn, scope, event_id_to_delete)?
                            {
                                if target_event.pubkey == event.pubkey.as_bytes() {
                                    self.remove_event_in_scope(txn, scope, event_id_to_delete)?;
                                    self.mark_deleted(txn, scope, event_id_to_delete)?;
                                    _has_valid_deletion = true;
                                } else {
                                    // Attempting to delete someone else's event - invalid
                                    has_invalid_deletion = true;
                                }
                            }
                        }
                        TagStandard::Coordinate {
                            coordinate: coord_to_delete,
                            ..
                        } => {
                            if coord_to_delete.public_key == event.pubkey {
                                // Find all events matching the coordinate to delete them
                                let events_to_remove_borrows = self
                                    .find_events_by_coordinate_scoped(
                                        txn,
                                        scope,
                                        coord_to_delete,
                                        Timestamp::max(),
                                    )?;

                                // Collect EventIds to release borrows before mutable operations
                                let event_ids_to_delete: Vec<EventId> = events_to_remove_borrows
                                    .iter()
                                    .map(|eb| EventId::from_slice(eb.id))
                                    .collect::<Result<Vec<_>, _>>()
                                    .map_err(|_| {
                                        Error::Other(
                                            "Invalid event ID slice during coordinate deletion"
                                                .to_string(),
                                        )
                                    })?;

                                drop(events_to_remove_borrows);

                                for id_val in event_ids_to_delete {
                                    self.remove_event_in_scope(txn, scope, &id_val)?;
                                    self.mark_deleted(txn, scope, &id_val)?;
                                }
                                _has_valid_deletion = true;
                            } else {
                                // Attempting to delete someone else's coordinate - invalid
                                has_invalid_deletion = true;
                            }
                        }
                        _ => {}
                    }
                }
            }

            // If there were any invalid deletion attempts, reject the entire event
            if has_invalid_deletion {
                return Ok(SaveEventStatus::Rejected(RejectedReason::InvalidDelete));
            }
        }

        // Handle replaceable events
        if event.kind.is_replaceable() {
            if let Some(existing_event_borrow) =
                self.find_replaceable_event_in_txn(txn, scope, &event.pubkey, event.kind)?
            {
                if event.created_at < existing_event_borrow.created_at
                    || (event.created_at == existing_event_borrow.created_at
                        && event.id.as_bytes() < existing_event_borrow.id)
                {
                    return Ok(SaveEventStatus::Rejected(RejectedReason::Replaced));
                }
                // Remove the existing event
                let id_to_remove = EventId::from_slice(existing_event_borrow.id).map_err(|_| {
                    Error::Other("Invalid event ID for replaceable event".to_string())
                })?;
                self.remove_event_in_scope(txn, scope, &id_to_remove)?;
            }
        }

        // Handle addressable events
        if event.kind.is_addressable() {
            if let Some(identifier) = event.tags.identifier() {
                let coordinate =
                    Coordinate::new(event.kind, event.pubkey).identifier(identifier.to_string());

                if let Some(existing_event_borrow) =
                    self.find_addressable_event_in_txn(txn, scope, &coordinate)?
                {
                    if event.created_at < existing_event_borrow.created_at
                        || (event.created_at == existing_event_borrow.created_at
                            && event.id.as_bytes() < existing_event_borrow.id)
                    {
                        return Ok(SaveEventStatus::Rejected(RejectedReason::Replaced));
                    }
                    // Remove the existing event
                    let id_to_remove =
                        EventId::from_slice(existing_event_borrow.id).map_err(|_| {
                            Error::Other("Invalid event ID for addressable event".to_string())
                        })?;
                    self.remove_event_in_scope(txn, scope, &id_to_remove)?;
                }
            }
        }

        // Store the event and its indexes
        self.store_in_scope(txn, scope, fbb, event)?;

        Ok(SaveEventStatus::Success)
    }

    pub(crate) fn delete_by_id_with_txn(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        event_id: &EventId,
    ) -> Result<bool, Error> {
        self.remove_event_in_scope(txn, scope, event_id)
    }

    pub(crate) fn query_with_txn<'txn>(
        &'txn self,
        txn: &'txn RoTxn,
        scope: &Scope,
        filter: Filter,
    ) -> Result<Vec<Event>, Error> {
        let iter = self.query(txn, scope, filter)?;
        let mut events = Vec::new();
        for event_result in iter {
            let event_borrow = event_result?;
            events.push(event_borrow.into_owned());
        }
        Ok(events)
    }
}
