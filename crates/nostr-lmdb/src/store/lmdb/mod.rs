// Copyright (c) 2024 Michael Dilger
// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2025 Rust Nostr Developers
// Distributed under the MIT software license

use std::collections::BTreeSet;
use std::ops::Bound;
use std::path::Path;

use async_utility::task;
use heed::{Env, EnvFlags, EnvOpenOptions, RoTxn, RwTxn};
use nostr::event::borrow::EventBorrow;
use nostr_database::flatbuffers::FlatBufferDecodeBorrowed;
use nostr_database::prelude::*;
use nostr_database::{nostr, FlatBufferBuilder, FlatBufferEncode, RejectedReason, SaveEventStatus};
use scoped_heed::{scoped_database_options, ScopedBytesDatabase, ScopedDbError};

mod index;

use super::error::Error;
use super::types::DatabaseFilter;

const EVENT_ID_ALL_ZEROS: [u8; 32] = [0; 32];
const EVENT_ID_ALL_255: [u8; 32] = [255; 32];

#[cfg(target_pointer_width = "64")]
const MAP_SIZE: usize = 1024 * 1024 * 1024 * 32;

#[cfg(target_pointer_width = "32")]
const MAP_SIZE: usize = 0xFFFFF000;

// Type alias for the complex iterator type to reduce clippy warnings
type ScopedIterator<'txn> =
    Box<dyn Iterator<Item = Result<(&'txn [u8], &'txn [u8]), ScopedDbError>> + 'txn>;

pub struct ScopedView<'a> {
    db: &'a Lmdb,
    scope: Option<String>,
}

#[allow(dead_code)]
impl ScopedView<'_> {
    pub fn save_event(&self, event: &Event) -> Result<SaveEventStatus, Error> {
        let mut wtxn = self.db.env.write_txn()?;
        let scope_opt = self.scope.as_deref();

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
                                .get_event_by_id_in_txn(&wtxn, scope_opt, event_id_to_delete)?
                                .is_some_and(|borrow| borrow.pubkey == event.pubkey.as_bytes());

                            if can_delete {
                                self.db.remove_event_in_scope(
                                    &mut wtxn,
                                    scope_opt,
                                    event_id_to_delete,
                                )?;
                                self.db
                                    .mark_deleted(&mut wtxn, scope_opt, event_id_to_delete)?;
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
                                        scope_opt,
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
                                    self.db
                                        .remove_event_in_scope(&mut wtxn, scope_opt, &id_val)?;
                                    // Mark it as deleted (for NIP-09 tracking, if still desired)
                                    self.db.mark_deleted(&mut wtxn, scope_opt, &id_val)?;
                                }

                                // The original mark_coordinate_deleted might be for a different purpose (e.g., NIP-09 receipt).
                                // For now, we focus on deleting the target events.
                                // If this is still needed for NIP-09 deletion event tracking, it can be kept:
                                /* self.db.mark_coordinate_deleted(
                                    &mut wtxn,
                                    scope_opt,
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
                self.db.find_replaceable_event_in_txn(
                    &wtxn,
                    scope_opt,
                    &event.pubkey,
                    event.kind,
                )? {
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
                    .remove_event_in_scope(&mut wtxn, scope_opt, &id_to_remove)?;
            }
        } else if event.kind.is_addressable() {
            if let Some(identifier) = event.tags.identifier() {
                let coordinate =
                    Coordinate::new(event.kind, event.pubkey).identifier(identifier.to_string());
                let event_id_owned = event.id;

                let existing_event_id_to_remove: Option<EventId> =
                    if let Some(existing_event_borrow) =
                        self.db
                            .find_addressable_event_in_txn(&wtxn, scope_opt, &coordinate)?
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
                        .remove_event_in_scope(&mut wtxn, scope_opt, &id_to_remove)?;
                }
            }
        }

        let mut fbb = FlatBufferBuilder::new();
        let event_id_bytes = event.id.as_bytes();
        let event_bytes = event.encode(&mut fbb);

        self.db
            .events_sdb
            .put(&mut wtxn, scope_opt, event_id_bytes, event_bytes)?;

        let ci_key_bytes: Vec<u8> =
            index::make_ci_index_key(&event.created_at, event.id.as_bytes());
        self.db
            .ci_index_sdb
            .put(&mut wtxn, scope_opt, &ci_key_bytes, event_id_bytes)?;

        let akc_key_bytes: Vec<u8> = index::make_akc_index_key(
            event.pubkey.as_bytes(),
            event.kind.as_u16(),
            &event.created_at,
            event.id.as_bytes(),
        );
        self.db
            .akc_index_sdb
            .put(&mut wtxn, scope_opt, &akc_key_bytes, event_id_bytes)?;

        let ac_key_bytes: Vec<u8> = index::make_ac_index_key(
            event.pubkey.as_bytes(),
            &event.created_at,
            event.id.as_bytes(),
        );
        self.db
            .ac_index_sdb
            .put(&mut wtxn, scope_opt, &ac_key_bytes, event_id_bytes)?;

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
                    self.db.atc_index_sdb.put(
                        &mut wtxn,
                        scope_opt,
                        &atc_key_bytes,
                        event_id_bytes,
                    )?;

                    let ktc_key_bytes: Vec<u8> = index::make_ktc_index_key(
                        event.kind.as_u16(),
                        &single_letter_tag,
                        tag_value,
                        &event.created_at,
                        event.id.as_bytes(),
                    );
                    self.db.ktc_index_sdb.put(
                        &mut wtxn,
                        scope_opt,
                        &ktc_key_bytes,
                        event_id_bytes,
                    )?;

                    let tc_key_bytes: Vec<u8> = index::make_tc_index_key(
                        &single_letter_tag,
                        tag_value,
                        &event.created_at,
                        event.id.as_bytes(),
                    );
                    self.db.tc_index_sdb.put(
                        &mut wtxn,
                        scope_opt,
                        &tc_key_bytes,
                        event_id_bytes,
                    )?;
                }
            }
        }

        wtxn.commit()?;
        Ok(SaveEventStatus::Success)
    }

    pub fn event_by_id(&self, event_id: &EventId) -> Result<Option<Event>, Error> {
        let rtxn = self.db.read_txn()?;
        let scope_opt = self.scope.as_deref();
        let event_id_bytes = event_id.as_bytes();

        match self.db.events_sdb.get(&rtxn, scope_opt, event_id_bytes)? {
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
            .range(txn, self.scope.as_deref(), &range_bounds)
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
            .range(txn, self.scope.as_deref(), &range_bounds)
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
            .range(txn, self.scope.as_deref(), &range_bounds)
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
            .range(txn, self.scope.as_deref(), &range_bounds)
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
            .range(txn, self.scope.as_deref(), &range_bounds)
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
            .range(txn, self.scope.as_deref(), &range_bounds)
            .map_err(Error::from)?;

        let mapped_iterator = underlying_iterator;

        Ok(Box::new(mapped_iterator))
    }

    fn iterate_filter_until_limit<'txn>(
        &'txn self,
        txn: &'txn RoTxn<'txn>,
        _scope: Option<&str>,
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

            let event_data_bytes =
                self.db
                    .events_sdb
                    .get(txn, self.scope.as_deref(), &event_id_bytes)?;

            if let Some(raw_event_bytes) = event_data_bytes {
                // Check if the event has been deleted
                let event_id = EventId::from_slice(&event_id_bytes)
                    .map_err(|_| Error::Other("Invalid event ID slice in iterate".to_string()))?;
                if self.db.is_deleted(txn, self.scope.as_deref(), &event_id)? {
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

            let event_data_bytes =
                self.db
                    .events_sdb
                    .get(txn, self.scope.as_deref(), &event_id_bytes)?;

            if let Some(raw_event_bytes) = event_data_bytes {
                // Check if the event has been deleted
                let event_id = EventId::from_slice(&event_id_bytes)
                    .map_err(|_| Error::Other("Invalid event ID slice in count".to_string()))?;
                if self.db.is_deleted(txn, self.scope.as_deref(), &event_id)? {
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
                    self.db
                        .events_sdb
                        .get(&txn, self.scope.as_deref(), id_bytes)?
                {
                    // Check if the event has been deleted
                    let event_id = EventId::from_slice(id_bytes)
                        .map_err(|_| Error::Other("Invalid event ID slice".to_string()))?;
                    if self.db.is_deleted(&txn, self.scope.as_deref(), &event_id)? {
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
                            self.scope.as_deref(),
                            author,
                            *kind_val,
                            since_val,
                            query_until,
                        )?;
                        self.iterate_filter_until_limit(
                            &txn,
                            self.scope.as_deref(),
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
                                self.scope.as_deref(),
                                author,
                                tagname,
                                tag_value,
                                &since_val,
                                &query_until,
                            )?;
                            self.iterate_filter_until_limit(
                                &txn,
                                self.scope.as_deref(),
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
                                self.scope.as_deref(),
                                *kind_val,
                                tag_name,
                                tag_value,
                                &since_val,
                                &query_until,
                            )?;
                            self.iterate_filter_until_limit(
                                &txn,
                                self.scope.as_deref(),
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
                            self.scope.as_deref(),
                            tag_name,
                            tag_value,
                            &since_val,
                            &query_until,
                        )?;
                        self.iterate_filter_until_limit(
                            &txn,
                            self.scope.as_deref(),
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
                        self.scope.as_deref(),
                        author,
                        since_val,
                        query_until,
                    )?;
                    self.iterate_filter_until_limit(
                        &txn,
                        self.scope.as_deref(),
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
                let iter = self.db.ci_iter_scoped(
                    &txn,
                    self.scope.as_deref(),
                    &query_since,
                    &query_until,
                )?;
                self.iterate_filter_until_limit(
                    &txn,
                    self.scope.as_deref(),
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
                    self.db
                        .events_sdb
                        .get(&txn, self.scope.as_deref(), id_bytes)?
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
                            self.scope.as_deref(),
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
                                self.scope.as_deref(),
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
                                self.scope.as_deref(),
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
                            self.scope.as_deref(),
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
                        self.scope.as_deref(),
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
                let iter = self.db.ci_iter_scoped(
                    &txn,
                    self.scope.as_deref(),
                    &query_since,
                    &query_until,
                )?;
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

    pub async fn delete(&self, filter: Filter) -> Result<(), Error> {
        let events_to_delete: Vec<Event> = self.query(filter)?;

        let db_clone = self.db.clone();
        let scope_clone = self.scope.clone();

        task::spawn_blocking(move || {
            let mut wtxn = db_clone.env.write_txn()?;
            let scope_opt_str = scope_clone.as_deref();

            for event_val in events_to_delete {
                let event_id_bytes = event_val.id.as_bytes();

                db_clone
                    .events_sdb
                    .delete(&mut wtxn, scope_opt_str, event_id_bytes)?;

                let ci_key_bytes =
                    index::make_ci_index_key(&event_val.created_at, event_val.id.as_bytes());
                db_clone
                    .ci_index_sdb
                    .delete(&mut wtxn, scope_opt_str, &ci_key_bytes)?;

                let akc_key_bytes = index::make_akc_index_key(
                    event_val.pubkey.as_bytes(),
                    event_val.kind.as_u16(),
                    &event_val.created_at,
                    event_val.id.as_bytes(),
                );
                db_clone
                    .akc_index_sdb
                    .delete(&mut wtxn, scope_opt_str, &akc_key_bytes)?;

                let ac_key_bytes = index::make_ac_index_key(
                    event_val.pubkey.as_bytes(),
                    &event_val.created_at,
                    event_val.id.as_bytes(),
                );
                db_clone
                    .ac_index_sdb
                    .delete(&mut wtxn, scope_opt_str, &ac_key_bytes)?;

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
                        db_clone
                            .atc_index_sdb
                            .delete(&mut wtxn, scope_opt_str, &atc_index_key)?;

                        let ktc_index_key: Vec<u8> = index::make_ktc_index_key(
                            event_val.kind.as_u16(),
                            &tag_name,
                            tag_value,
                            &event_val.created_at,
                            event_val.id.as_bytes(),
                        );
                        db_clone
                            .ktc_index_sdb
                            .delete(&mut wtxn, scope_opt_str, &ktc_index_key)?;

                        let tc_index_key: Vec<u8> = index::make_tc_index_key(
                            &tag_name,
                            tag_value,
                            &event_val.created_at,
                            event_val.id.as_bytes(),
                        );
                        db_clone
                            .tc_index_sdb
                            .delete(&mut wtxn, scope_opt_str, &tc_index_key)?;
                    }
                }
            }
            wtxn.commit()?;
            Result::<(), Error>::Ok(())
        })
        .await?
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
    pub(crate) fn new<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let env: Env = unsafe {
            EnvOpenOptions::new()
                .flags(EnvFlags::NO_TLS)
                .max_dbs(18 + 1)
                .map_size(MAP_SIZE)
                .open(path)?
        };

        let mut txn = env.write_txn()?;

        let events_sdb = scoped_database_options(&env)
            .raw_bytes()
            .name("events")
            .create(&mut txn)?;

        let ci_index_sdb = scoped_database_options(&env)
            .raw_bytes()
            .name("ci_index")
            .create(&mut txn)?;

        let tc_index_sdb = scoped_database_options(&env)
            .raw_bytes()
            .name("tc_index")
            .create(&mut txn)?;

        let ac_index_sdb = scoped_database_options(&env)
            .raw_bytes()
            .name("ac_index")
            .create(&mut txn)?;

        let akc_index_sdb = scoped_database_options(&env)
            .raw_bytes()
            .name("akc_index")
            .create(&mut txn)?;

        let atc_index_sdb = scoped_database_options(&env)
            .raw_bytes()
            .name("atc_index")
            .create(&mut txn)?;

        let ktc_index_sdb = scoped_database_options(&env)
            .raw_bytes()
            .name("ktc_index")
            .create(&mut txn)?;

        let deleted_ids_sdb = scoped_database_options(&env)
            .raw_bytes()
            .name("deleted_ids")
            .create(&mut txn)?;

        let deleted_coordinates_sdb = scoped_database_options(&env)
            .raw_bytes()
            .name("deleted_coordinates")
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
    pub(crate) fn read_txn(&self) -> Result<RoTxn, Error> {
        Ok(self.env.read_txn()?)
    }

    #[inline]
    pub(crate) fn write_txn(&self) -> Result<RwTxn, Error> {
        Ok(self.env.write_txn()?)
    }

    pub(crate) fn store_in_scope(
        &self,
        txn: &mut RwTxn,
        scope: Option<&str>,
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

    pub(crate) fn remove(&self, txn: &mut RwTxn, event_borrow: &EventBorrow) -> Result<(), Error> {
        self.events_sdb.delete(txn, None, event_borrow.id)?;

        let ci_index_key: Vec<u8> =
            index::make_ci_index_key(&event_borrow.created_at, event_borrow.id);
        self.ci_index_sdb.delete(txn, None, &ci_index_key)?;

        let akc_index_key: Vec<u8> = index::make_akc_index_key(
            event_borrow.pubkey,
            event_borrow.kind,
            &event_borrow.created_at,
            event_borrow.id,
        );
        self.akc_index_sdb.delete(txn, None, &akc_index_key)?;

        let ac_index_key: Vec<u8> = index::make_ac_index_key(
            event_borrow.pubkey,
            &event_borrow.created_at,
            event_borrow.id,
        );
        self.ac_index_sdb.delete(txn, None, &ac_index_key)?;

        for cow_tag in event_borrow.tags.iter() {
            if let Some((tag_name, tag_value)) = cow_tag.extract() {
                let atc_index_key: Vec<u8> = index::make_atc_index_key(
                    event_borrow.pubkey,
                    &tag_name,
                    tag_value,
                    &event_borrow.created_at,
                    event_borrow.id,
                );
                self.atc_index_sdb.delete(txn, None, &atc_index_key)?;

                let ktc_index_key: Vec<u8> = index::make_ktc_index_key(
                    event_borrow.kind,
                    &tag_name,
                    tag_value,
                    &event_borrow.created_at,
                    event_borrow.id,
                );
                self.ktc_index_sdb.delete(txn, None, &ktc_index_key)?;

                let tc_index_key: Vec<u8> = index::make_tc_index_key(
                    &tag_name,
                    tag_value,
                    &event_borrow.created_at,
                    event_borrow.id,
                );
                self.tc_index_sdb.delete(txn, None, &tc_index_key)?;
            }
        }

        Ok(())
    }

    pub(crate) fn wipe(&self, txn: &mut RwTxn) -> Result<(), Error> {
        self.events_sdb.clear(txn, None)?;
        self.ci_index_sdb.clear(txn, None)?;
        self.tc_index_sdb.clear(txn, None)?;
        self.ac_index_sdb.clear(txn, None)?;
        self.akc_index_sdb.clear(txn, None)?;
        self.atc_index_sdb.clear(txn, None)?;
        self.ktc_index_sdb.clear(txn, None)?;
        self.deleted_ids_sdb.clear(txn, None)?;
        self.deleted_coordinates_sdb.clear(txn, None)?;
        Ok(())
    }

    #[inline]
    pub(crate) fn has_event(&self, txn: &RoTxn, event_id: &[u8; 32]) -> Result<bool, Error> {
        self.has_event_in_scope(txn, None, event_id)
    }

    pub(crate) fn has_event_in_scope(
        &self,
        txn: &RoTxn,
        scope: Option<&str>,
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
        scope: Option<&str>,
        event_id: &EventId,
    ) -> Result<Option<EventBorrow<'txn>>, Error> {
        match self.events_sdb.get(txn, scope, event_id.as_bytes())? {
            Some(bytes) => Ok(Some(EventBorrow::decode(bytes)?)),
            None => Ok(None),
        }
    }

    pub fn delete(&self, read_txn: &RoTxn, txn: &mut RwTxn, filter: Filter) -> Result<(), Error> {
        let events_iter = self.query(read_txn, None, filter)?;
        for event_result in events_iter {
            let event_borrow = event_result?;
            self.remove(txn, &event_borrow)?;
        }
        Ok(())
    }

    pub fn query<'txn>(
        &'txn self,
        txn: &'txn RoTxn<'txn>,
        scope: Option<&str>,
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
        txn: &'txn RoTxn<'txn>,
        scope: Option<&str>,
        author: &PublicKey,
        kind: Kind,
    ) -> Result<Option<EventBorrow<'txn>>, Error> {
        if !kind.is_replaceable() {
            return Err(Error::WrongEventKind);
        }
        let mut iter = self.akc_iter_scoped(
            txn,
            scope,
            author.as_bytes(),
            kind.as_u16(),
            Timestamp::min(),
            Timestamp::max(),
        )?;
        if let Some(result) = iter.next() {
            let (_key, id_bytes) = result?;
            return self.get_event_by_id_in_txn(
                txn,
                scope,
                &EventId::from_slice(id_bytes).unwrap(),
            );
        }
        Ok(None)
    }

    pub fn find_addressable_event_in_txn<'txn>(
        &self,
        txn: &'txn RoTxn<'txn>,
        scope: Option<&str>,
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
        scope: Option<&str>,
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
        scope: Option<&str>,
        event_id: &EventId,
    ) -> Result<(), Error> {
        self.deleted_ids_sdb
            .put(txn, scope, event_id.as_bytes(), &[])?;
        Ok(())
    }

    pub(crate) fn mark_coordinate_deleted(
        &self,
        txn: &mut RwTxn,
        scope: Option<&str>,
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
        txn: &'txn RoTxn<'txn>,
        scope: Option<&str>,
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
        txn: &'txn RoTxn<'txn>,
        scope: Option<&str>,
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
        txn: &'txn RoTxn<'txn>,
        scope: Option<&str>,
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
        txn: &'txn RoTxn<'txn>,
        scope: Option<&str>,
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
        txn: &'txn RoTxn<'txn>,
        scope: Option<&str>,
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
        txn: &'txn RoTxn<'txn>,
        scope: Option<&str>,
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
        txn: &'txn RoTxn<'txn>,
        scope: Option<&str>,
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
        txn: &'txn RoTxn<'txn>,
        scope: Option<&str>,
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
        ScopedView {
            db: self,
            scope: Some(scope_name.to_string()),
        }
    }

    pub(crate) fn unscoped(&self) -> ScopedView<'_> {
        ScopedView {
            db: self,
            scope: None,
        }
    }

    pub(crate) fn remove_event_in_scope(
        &self,
        txn: &mut RwTxn,
        scope: Option<&str>,
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
}
