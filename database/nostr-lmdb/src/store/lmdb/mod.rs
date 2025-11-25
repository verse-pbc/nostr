// Copyright (c) 2024 Michael Dilger
// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2025 Rust Nostr Developers
// Distributed under the MIT software license

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::iter;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use heed::byteorder::NativeEndian;
use heed::types::{Bytes, Unit, U64};
use heed::{Database, Env, EnvFlags, EnvOpenOptions, RoRange, RoTxn, RwTxn};
use nostr::prelude::*;
use nostr_database::flatbuffers::FlatBufferDecodeBorrowed;
use nostr_database::{FlatBufferBuilder, FlatBufferEncode, RejectedReason, SaveEventStatus};
use scoped_heed::{scoped_database_options, GlobalScopeRegistry, Scope, ScopedBytesDatabase};

mod index;

use self::index::EventIndexKeys;
use super::error::Error;
use super::filter::DatabaseFilter;

/// Type alias for the complex iterator type returned by scoped operations
type ScopedIterator<'txn> =
    Box<dyn Iterator<Item = Result<(&'txn [u8], &'txn [u8]), scoped_heed::ScopedDbError>> + 'txn>;

const EVENT_ID_ALL_ZEROS: [u8; 32] = [0; 32];
const EVENT_ID_ALL_255: [u8; 32] = [255; 32];

#[derive(Debug)]
enum QueryFilterPattern {
    Ids,
    AuthorsAndKinds,
    AuthorsAndTags,
    AuthorKindsAndTags,
    KindsAndTags,
    Tags,
    Authors,
    Scraping,
}

impl QueryFilterPattern {
    fn from_filter(filter: &DatabaseFilter) -> Self {
        if !filter.ids.is_empty() {
            Self::Ids
        } else if !filter.authors.is_empty()
            && !filter.kinds.is_empty()
            && !filter.generic_tags.is_empty()
        {
            Self::AuthorKindsAndTags
        } else if !filter.authors.is_empty() && !filter.kinds.is_empty() {
            Self::AuthorsAndKinds
        } else if !filter.authors.is_empty() && !filter.generic_tags.is_empty() {
            Self::AuthorsAndTags
        } else if !filter.kinds.is_empty() && !filter.generic_tags.is_empty() {
            Self::KindsAndTags
        } else if !filter.generic_tags.is_empty() {
            Self::Tags
        } else if !filter.authors.is_empty() {
            Self::Authors
        } else {
            Self::Scraping
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Lmdb {
    /// LMDB env
    env: Env,
    /// Events
    events: Database<Bytes, Bytes>, // Event ID, Event
    /// CreatedAt + ID index
    ci_index: Database<Bytes, Bytes>, // <Index>, Event ID
    /// Tag + CreatedAt + ID index
    tc_index: Database<Bytes, Bytes>, // <Index>, Event ID
    /// Author + CreatedAt + ID index
    ac_index: Database<Bytes, Bytes>, // <Index>, Event ID
    /// Author + Kind + CreatedAt + ID index
    akc_index: Database<Bytes, Bytes>, // <Index>, Event ID
    /// Author + Tag + CreatedAt + ID index
    atc_index: Database<Bytes, Bytes>, // <Index>, Event ID
    /// Kind + Tag + CreatedAt + ID index
    ktc_index: Database<Bytes, Bytes>, // <Index>, Event ID
    /// Deleted IDs
    deleted_ids: Database<Bytes, Unit>, // Event ID
    /// Deleted coordinates
    deleted_coordinates: Database<Bytes, U64<NativeEndian>>, // Coordinate, UNIX timestamp
    // Scoped versions of databases for multi-tenant support
    /// Scoped Events
    events_scoped: ScopedBytesDatabase,
    /// Scoped CreatedAt + ID index
    ci_index_scoped: ScopedBytesDatabase,
    /// Scoped Tag + CreatedAt + ID index
    tc_index_scoped: ScopedBytesDatabase,
    /// Scoped Author + CreatedAt + ID index
    ac_index_scoped: ScopedBytesDatabase,
    /// Scoped Author + Kind + CreatedAt + ID index
    akc_index_scoped: ScopedBytesDatabase,
    /// Scoped Author + Tag + CreatedAt + ID index
    atc_index_scoped: ScopedBytesDatabase,
    /// Scoped Kind + Tag + CreatedAt + ID index
    ktc_index_scoped: ScopedBytesDatabase,
    /// Scoped Deleted IDs
    deleted_ids_scoped: ScopedBytesDatabase,
    /// Scoped Deleted coordinates
    deleted_coordinates_scoped: ScopedBytesDatabase,
    /// Global scope registry for multi-tenant support
    scope_registry: Arc<GlobalScopeRegistry>,
}

impl Lmdb {
    pub(super) fn new<P>(
        path: P,
        map_size: usize,
        max_readers: u32,
        additional_dbs: u32,
    ) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        // Construct LMDB env
        // Note: We need additional DBs for scoped versions
        // Original: 9 DBs (1 unnamed for events, 8 named)
        // Scoped: Each ScopedBytesDatabase creates 2 internal DBs (default + scoped)
        // So we need: 9 original + (9 scoped * 2) + 1 registry = 28 DBs
        let env: Env = unsafe {
            EnvOpenOptions::new()
                .flags(EnvFlags::NO_TLS)
                .max_dbs(30 + additional_dbs)
                .max_readers(max_readers)
                .map_size(map_size)
                .open(path)?
        };

        // Acquire write transaction
        let mut txn = env.write_txn()?;

        // Open/Create maps (original non-scoped for backward compatibility)
        let events = env
            .database_options()
            .types::<Bytes, Bytes>()
            .create(&mut txn)?;
        let ci_index = env
            .database_options()
            .types::<Bytes, Bytes>()
            .name("ci")
            .create(&mut txn)?;
        let tc_index = env
            .database_options()
            .types::<Bytes, Bytes>()
            .name("tci")
            .create(&mut txn)?;
        let ac_index = env
            .database_options()
            .types::<Bytes, Bytes>()
            .name("aci")
            .create(&mut txn)?;
        let akc_index = env
            .database_options()
            .types::<Bytes, Bytes>()
            .name("akci")
            .create(&mut txn)?;
        let atc_index = env
            .database_options()
            .types::<Bytes, Bytes>()
            .name("atci")
            .create(&mut txn)?;
        let ktc_index = env
            .database_options()
            .types::<Bytes, Bytes>()
            .name("ktci")
            .create(&mut txn)?;
        let deleted_ids = env
            .database_options()
            .types::<Bytes, Unit>()
            .name("deleted-ids")
            .create(&mut txn)?;
        let deleted_coordinates = env
            .database_options()
            .types::<Bytes, U64<NativeEndian>>()
            .name("deleted-coordinates")
            .create(&mut txn)?;

        // Create global scope registry for multi-tenant support
        let scope_registry = Arc::new(GlobalScopeRegistry::new(&env, &mut txn)?);

        // Create scoped database wrappers using the builder pattern
        // Note: Using unnamed_for_default() for events to maintain backward compatibility
        let events_scoped = scoped_database_options(&env, scope_registry.clone())
            .raw_bytes()
            .name("events")
            .unnamed_for_default()
            .create(&mut txn)?;
        let ci_index_scoped = scoped_database_options(&env, scope_registry.clone())
            .raw_bytes()
            .name("ci_scoped")
            .create(&mut txn)?;
        let tc_index_scoped = scoped_database_options(&env, scope_registry.clone())
            .raw_bytes()
            .name("tci_scoped")
            .create(&mut txn)?;
        let ac_index_scoped = scoped_database_options(&env, scope_registry.clone())
            .raw_bytes()
            .name("aci_scoped")
            .create(&mut txn)?;
        let akc_index_scoped = scoped_database_options(&env, scope_registry.clone())
            .raw_bytes()
            .name("akci_scoped")
            .create(&mut txn)?;
        let atc_index_scoped = scoped_database_options(&env, scope_registry.clone())
            .raw_bytes()
            .name("atci_scoped")
            .create(&mut txn)?;
        let ktc_index_scoped = scoped_database_options(&env, scope_registry.clone())
            .raw_bytes()
            .name("ktci_scoped")
            .create(&mut txn)?;
        let deleted_ids_scoped = scoped_database_options(&env, scope_registry.clone())
            .raw_bytes()
            .name("deleted-ids_scoped")
            .create(&mut txn)?;
        let deleted_coordinates_scoped = scoped_database_options(&env, scope_registry.clone())
            .raw_bytes()
            .name("deleted-coordinates_scoped")
            .create(&mut txn)?;

        // Commit changes
        txn.commit()?;

        Ok(Self {
            env,
            events,
            ci_index,
            tc_index,
            ac_index,
            akc_index,
            atc_index,
            ktc_index,
            deleted_ids,
            deleted_coordinates,
            events_scoped,
            ci_index_scoped,
            tc_index_scoped,
            ac_index_scoped,
            akc_index_scoped,
            atc_index_scoped,
            ktc_index_scoped,
            deleted_ids_scoped,
            deleted_coordinates_scoped,
            scope_registry,
        })
    }

    /// Get a read transaction
    ///
    /// This should never block the current thread
    #[inline]
    pub(crate) fn read_txn(&self) -> Result<RoTxn, Error> {
        Ok(self.env.read_txn()?)
    }

    /// Get a write transaction
    ///
    /// This blocks the current thread if there is another write txn
    #[inline]
    pub(crate) fn write_txn(&self) -> Result<RwTxn, Error> {
        Ok(self.env.write_txn()?)
    }

    /// Store and index the event
    pub(crate) fn store(
        &self,
        txn: &mut RwTxn,
        fbb: &mut FlatBufferBuilder,
        event: &Event,
    ) -> Result<(), Error> {
        // Store event
        self.events
            .put(txn, event.id.as_bytes(), event.encode(fbb))?;

        // Index event
        let event: EventBorrow = EventBorrow::from(event);
        let index: EventIndexKeys = EventIndexKeys::new(event);
        self.index_event(txn, index)
    }

    fn index_event(&self, txn: &mut RwTxn, index: EventIndexKeys) -> Result<(), Error> {
        self.ci_index.put(txn, &index.ci_index, &index.id)?;
        self.akc_index.put(txn, &index.akc_index, &index.id)?;
        self.ac_index.put(txn, &index.ac_index, &index.id)?;

        for tag in index.tags.into_iter() {
            self.atc_index.put(txn, &tag.atc_index, &index.id)?;
            self.ktc_index.put(txn, &tag.ktc_index, &index.id)?;
            self.tc_index.put(txn, &tag.tc_index, &index.id)?;
        }

        Ok(())
    }

    /// Deletes an event and all its index entries using pre-collected DeletionInfo.
    ///
    /// This is a helper function that centralizes the deletion logic used by multiple
    /// methods (`remove_replaceable`, `remove_addressable`, `handle_deletion_event`).
    /// It eliminates code duplication and ensures all indexes are properly cleaned up.
    ///
    /// # Arguments
    /// * `txn` - The write transaction to use for deletions
    /// * `info` - Pre-collected information about the event to delete
    ///
    /// # Note
    /// This method does NOT:
    /// - Mark events as deleted (that's a semantic operation)
    /// - Verify permissions or validate the deletion
    /// - Check if the event exists
    ///
    /// It only performs the mechanical deletion from all indexes.
    fn remove(&self, txn: &mut RwTxn, index: &EventIndexKeys) -> Result<(), Error> {
        self.events.delete(txn, &index.id)?;
        self.ci_index.delete(txn, &index.ci_index)?;
        self.akc_index.delete(txn, &index.akc_index)?;
        self.ac_index.delete(txn, &index.ac_index)?;

        // Delete tag indexes
        for tag in &index.tags {
            self.atc_index.delete(txn, &tag.atc_index)?;
            self.ktc_index.delete(txn, &tag.ktc_index)?;
            self.tc_index.delete(txn, &tag.tc_index)?;
        }

        Ok(())
    }

    pub(crate) fn wipe(&self, txn: &mut RwTxn) -> Result<(), Error> {
        // Wipe events
        self.events.clear(txn)?;

        // Wipe indexes
        self.wipe_indexes(txn)?;

        Ok(())
    }

    fn wipe_indexes(&self, txn: &mut RwTxn) -> Result<(), Error> {
        self.ci_index.clear(txn)?;
        self.tc_index.clear(txn)?;
        self.ac_index.clear(txn)?;
        self.akc_index.clear(txn)?;
        self.atc_index.clear(txn)?;
        self.ktc_index.clear(txn)?;
        self.deleted_ids.clear(txn)?;
        self.deleted_coordinates.clear(txn)?;
        Ok(())
    }

    pub(super) fn reindex(&self, txn: &mut RwTxn) -> Result<(), Error> {
        // First, wipe all indexes
        self.wipe_indexes(txn)?;

        // Collect indexes
        // TODO: avoid this allocation
        let size: u64 = self.events.len(txn)?;
        let mut indexes: Vec<EventIndexKeys> = Vec::with_capacity(size as usize);

        for result in self.events.iter(txn)? {
            let (_id, event) = result?;

            // Decode event
            if let Ok(event) = EventBorrow::decode(event) {
                // Build indexes
                let index: EventIndexKeys = EventIndexKeys::new(event);
                indexes.push(index);
            }
        }

        for index in indexes.into_iter() {
            self.index_event(txn, index)?;
        }

        Ok(())
    }

    #[inline]
    pub(crate) fn has_event(&self, txn: &RoTxn, event_id: &EventId) -> Result<bool, Error> {
        Ok(self.get_event_by_id(txn, event_id.as_bytes())?.is_some())
    }

    /// Save event with transaction support - uses single transaction for batch consistency
    pub(crate) fn save_event_with_txn(
        &self,
        txn: &mut RwTxn,
        fbb: &mut FlatBufferBuilder,
        event: &Event,
    ) -> Result<SaveEventStatus, Error> {
        if event.kind.is_ephemeral() {
            return Ok(SaveEventStatus::Rejected(RejectedReason::Ephemeral));
        }

        // Already exists
        if self.has_event(txn, &event.id)? {
            return Ok(SaveEventStatus::Rejected(RejectedReason::Duplicate));
        }

        // Reject event if ID was deleted
        if self.is_deleted(txn, &event.id)? {
            return Ok(SaveEventStatus::Rejected(RejectedReason::Deleted));
        }

        // Reject event if ADDR was deleted after it's created_at date
        // (non-parameterized or parameterized)
        if let Some(coordinate) = event.coordinate() {
            if let Some(time) = self.when_is_coordinate_deleted(txn, &coordinate)? {
                if event.created_at <= time {
                    return Ok(SaveEventStatus::Rejected(RejectedReason::Deleted));
                }
            }
        }

        // Remove replaceable events being replaced
        if event.kind.is_replaceable() {
            if let Some(stored) = self.find_replaceable_event(txn, &event.pubkey, event.kind)? {
                if has_event_been_replaced(&stored, event) {
                    return Ok(SaveEventStatus::Rejected(RejectedReason::Replaced));
                }

                let coordinate = Coordinate::new(event.kind, event.pubkey);
                self.remove_replaceable(txn, &coordinate, &event.created_at)?;
            }
        }

        // Remove addressable events being replaced
        if event.kind.is_addressable() {
            if let Some(identifier) = event.tags.identifier() {
                let coordinate = Coordinate::new(event.kind, event.pubkey).identifier(identifier);

                if let Some(stored) = self.find_addressable_event(txn, &coordinate)? {
                    if has_event_been_replaced(&stored, event) {
                        return Ok(SaveEventStatus::Rejected(RejectedReason::Replaced));
                    }

                    self.remove_addressable(txn, &coordinate, Timestamp::max())?;
                }
            }
        }

        // Handle deletion events
        if event.kind == Kind::EventDeletion {
            let invalid: bool = self.handle_deletion_event(txn, event)?;
            if invalid {
                return Ok(SaveEventStatus::Rejected(RejectedReason::InvalidDelete));
            }
        }

        self.store(txn, fbb, event)?;

        Ok(SaveEventStatus::Success)
    }

    #[inline]
    pub(crate) fn get_event_by_id<'a>(
        &self,
        txn: &'a RoTxn,
        event_id: &[u8],
    ) -> Result<Option<EventBorrow<'a>>, Error> {
        match self.events.get(txn, event_id)? {
            Some(bytes) => Ok(Some(EventBorrow::decode(bytes)?)),
            None => Ok(None),
        }
    }

    /// Delete events
    pub fn delete(&self, txn: &mut RwTxn, filter: Filter) -> Result<(), Error> {
        // First, collect all deletion info while we have immutable borrows
        let indexes: Vec<EventIndexKeys> = {
            let events = self.query(txn, filter)?;
            events
                .into_iter()
                .map(|event| EventIndexKeys::new(event))
                .collect()
        }; // All EventBorrow instances dropped here

        // Now we can safely mutate the transaction
        for index in indexes {
            self.remove(txn, &index)?;
        }

        Ok(())
    }

    /// Find all events that match the filter
    pub fn query<'a>(
        &self,
        txn: &'a RoTxn,
        filter: Filter,
    ) -> Result<Box<dyn Iterator<Item = EventBorrow<'a>> + 'a>, Error> {
        if let (Some(since), Some(until)) = (filter.since, filter.until) {
            if since > until {
                return Ok(Box::new(iter::empty()));
            }
        }

        // We insert into a BTreeSet to keep them time-ordered
        let mut output: BTreeSet<EventBorrow<'a>> = BTreeSet::new();

        let limit: Option<usize> = filter.limit;
        let since = filter.since.unwrap_or_else(Timestamp::min);
        let until = filter.until.unwrap_or_else(Timestamp::max);

        let filter: DatabaseFilter = filter.into();

        // Identify pattern
        let pattern: QueryFilterPattern = QueryFilterPattern::from_filter(&filter);

        tracing::debug!("Querying by pattern: {pattern:?}");

        // Query by pattern
        match pattern {
            QueryFilterPattern::Ids => self.query_by_ids(txn, filter, limit, &mut output)?,
            QueryFilterPattern::AuthorsAndKinds => {
                self.query_by_authors_and_kinds(txn, filter, since, &until, limit, &mut output)?
            }
            QueryFilterPattern::AuthorsAndTags => {
                self.query_by_authors_and_tags(txn, filter, since, &until, limit, &mut output)?
            }
            QueryFilterPattern::AuthorKindsAndTags => self.query_by_authors_kinds_and_tags(
                txn,
                filter,
                since,
                &until,
                limit,
                &mut output,
            )?,
            QueryFilterPattern::KindsAndTags => {
                self.query_by_kinds_and_tags(txn, filter, since, &until, limit, &mut output)?
            }
            QueryFilterPattern::Tags => {
                self.query_by_tags(txn, filter, since, &until, limit, &mut output)?
            }
            QueryFilterPattern::Authors => {
                self.query_by_authors(txn, filter, since, &until, limit, &mut output)?
            }
            QueryFilterPattern::Scraping => {
                self.query_by_scraping(txn, filter, &since, &until, limit, &mut output)?
            }
        }

        // Optionally apply limit
        Ok(match limit {
            Some(limit) => Box::new(output.into_iter().take(limit)),
            None => Box::new(output.into_iter()),
        })
    }

    fn query_by_ids<'a>(
        &self,
        txn: &'a RoTxn,
        filter: DatabaseFilter,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error> {
        // Fetch by id
        for id in filter.ids.iter() {
            // Check if limit is set
            if let Some(limit) = limit {
                // Stop if limited
                if output.len() >= limit {
                    break;
                }
            }

            if let Some(event) = self.get_event_by_id(txn, id)? {
                if filter.match_event(&event) {
                    output.insert(event);
                }
            }
        }

        Ok(())
    }

    fn query_by_authors_and_kinds<'a>(
        &self,
        txn: &'a RoTxn,
        filter: DatabaseFilter,
        since: Timestamp,
        until: &Timestamp,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error> {
        // We may bring since forward if we hit the limit without going back that
        // far, so we use a mutable since:
        let mut since: Timestamp = since;

        for author in filter.authors.iter() {
            for kind in filter.kinds.iter() {
                let iter = self.akc_iter(txn, author, *kind, &since, until)?;

                // Count how many we have found of this author-kind pair, so we
                // can possibly update `since`
                let mut paircount = 0;

                'per_event: for result in iter {
                    let (_key, value) = result?;
                    let event = self.get_event_by_id(txn, value)?.ok_or(Error::NotFound)?;

                    // If we have gone beyond since, we can stop early
                    // (We have to check because `since` might change in this loop)
                    if event.created_at < since {
                        break 'per_event;
                    }

                    // check against the rest of the filter
                    if filter.match_event(&event) {
                        let created_at = event.created_at;

                        // Accept the event
                        output.insert(event);
                        paircount += 1;

                        // Stop this pair if limited
                        if let Some(limit) = limit {
                            if paircount >= limit {
                                // Since we found the limit just among this pair,
                                // potentially move since forward
                                if created_at > since {
                                    since = created_at;
                                }
                                break 'per_event;
                            }
                        }

                        // If kind is replaceable (and not parameterized)
                        // then don't take any more events for this author-kind
                        // pair.
                        // NOTE that this optimization is difficult to implement
                        // for other replaceable event situations
                        if Kind::from(*kind).is_replaceable() {
                            break 'per_event;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn query_by_authors_and_tags<'a>(
        &self,
        txn: &'a RoTxn,
        filter: DatabaseFilter,
        since: Timestamp,
        until: &Timestamp,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error> {
        // We may bring since forward if we hit the limit without going back that
        // far, so we use a mutable since:
        let mut since: Timestamp = since;

        for author in filter.authors.iter() {
            for (tagname, set) in filter.generic_tags.iter() {
                for tag_value in set.iter() {
                    let iter = self
                        .atc_iter(txn, author, tagname, tag_value, &since, until)?
                        .filter_map(|res| {
                            let (_k, v) = res.ok()?;
                            Some(v)
                        });
                    self.iterate_filter_until_limit(txn, &filter, iter, &mut since, limit, output)?;
                }
            }
        }

        Ok(())
    }

    fn query_by_authors_kinds_and_tags<'a>(
        &self,
        txn: &'a RoTxn,
        filter: DatabaseFilter,
        since: Timestamp,
        until: &Timestamp,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error> {
        // We may bring since forward if we hit the limit without going back that
        // far, so we use a mutable since:
        let mut since: Timestamp = since;

        for author in filter.authors.iter() {
            for kind in filter.kinds.iter() {
                // Author + Kind index
                let akc_iter = self.akc_iter(txn, author, *kind, &since, until)?;

                // Collect Author + Kind BTree set
                let akc_set: BTreeSet<&[u8]> = akc_iter
                    .filter_map(|res| {
                        let (_k, v) = res.ok()?;
                        Some(v)
                    })
                    .collect();

                for (tagname, set) in filter.generic_tags.iter() {
                    for tag_value in set.iter() {
                        // Author + Tag index
                        let atc_iter =
                            self.atc_iter(txn, author, tagname, tag_value, &since, until)?;

                        // Collect Author + Tag BTree set
                        let atc_set: BTreeSet<&[u8]> = atc_iter
                            .filter_map(|res| {
                                let (_k, v) = res.ok()?;
                                Some(v)
                            })
                            .collect();

                        // Intersection
                        let iter = atc_set.intersection(&akc_set).copied();

                        self.iterate_filter_until_limit(
                            txn, &filter, iter, &mut since, limit, output,
                        )?;
                    }
                }
            }
        }

        Ok(())
    }

    fn query_by_kinds_and_tags<'a>(
        &self,
        txn: &'a RoTxn,
        filter: DatabaseFilter,
        since: Timestamp,
        until: &Timestamp,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error> {
        // We may bring since forward if we hit the limit without going back that
        // far, so we use a mutable since:
        let mut since: Timestamp = since;

        for kind in filter.kinds.iter() {
            for (tag_name, set) in filter.generic_tags.iter() {
                for tag_value in set.iter() {
                    let iter = self
                        .ktc_iter(txn, *kind, tag_name, tag_value, &since, until)?
                        .filter_map(|res| {
                            let (_k, v) = res.ok()?;
                            Some(v)
                        });
                    self.iterate_filter_until_limit(txn, &filter, iter, &mut since, limit, output)?;
                }
            }
        }

        Ok(())
    }

    fn query_by_tags<'a>(
        &self,
        txn: &'a RoTxn,
        filter: DatabaseFilter,
        since: Timestamp,
        until: &Timestamp,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error> {
        // We may bring since forward if we hit the limit without going back that
        // far, so we use a mutable since:
        let mut since: Timestamp = since;

        for (tag_name, set) in filter.generic_tags.iter() {
            for tag_value in set.iter() {
                let iter = self
                    .tc_iter(txn, tag_name, tag_value, &since, until)?
                    .filter_map(|res| {
                        let (_k, v) = res.ok()?;
                        Some(v)
                    });
                self.iterate_filter_until_limit(txn, &filter, iter, &mut since, limit, output)?;
            }
        }

        Ok(())
    }

    fn query_by_authors<'a>(
        &self,
        txn: &'a RoTxn,
        filter: DatabaseFilter,
        since: Timestamp,
        until: &Timestamp,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error> {
        // We may bring since forward if we hit the limit without going back that
        // far, so we use a mutable since:
        let mut since: Timestamp = since;

        for author in filter.authors.iter() {
            let iter = self.ac_iter(txn, author, &since, until)?.filter_map(|res| {
                let (_k, v) = res.ok()?;
                Some(v)
            });
            self.iterate_filter_until_limit(txn, &filter, iter, &mut since, limit, output)?;
        }

        Ok(())
    }

    /// SCRAPE
    ///
    /// This is INEFFICIENT as it scans through many events
    fn query_by_scraping<'a>(
        &self,
        txn: &'a RoTxn,
        filter: DatabaseFilter,
        since: &Timestamp,
        until: &Timestamp,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error> {
        let iter = self.ci_iter(txn, since, until)?;

        for result in iter {
            // Check if limit is set
            if let Some(limit) = limit {
                // Stop if limited
                if output.len() >= limit {
                    break;
                }
            }

            let (_key, value) = result?;
            let event = self.get_event_by_id(txn, value)?.ok_or(Error::NotFound)?;

            if filter.match_event(&event) {
                output.insert(event);
            }
        }

        Ok(())
    }

    fn iterate_filter_until_limit<'a, 'i, I>(
        &self,
        txn: &'a RoTxn,
        filter: &DatabaseFilter,
        iter: I,
        since: &mut Timestamp,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error>
    where
        I: IntoIterator<Item = &'i [u8]>,
    {
        let mut count: usize = 0;

        for id in iter {
            // Get event by ID
            let event = self.get_event_by_id(txn, id)?.ok_or(Error::NotFound)?;

            if event.created_at < *since {
                break;
            }

            // check against the rest of the filter
            if filter.match_event(&event) {
                let created_at = event.created_at;

                // Accept the event
                output.insert(event);
                count += 1;

                // Check if limit is set
                if let Some(limit) = limit {
                    // Stop if limited
                    if count >= limit {
                        if created_at > *since {
                            *since = created_at;
                        }
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn find_replaceable_event<'a>(
        &self,
        txn: &'a RoTxn,
        author: &PublicKey,
        kind: Kind,
    ) -> Result<Option<EventBorrow<'a>>, Error> {
        if !kind.is_replaceable() {
            return Err(Error::WrongEventKind);
        }

        let mut iter = self.akc_iter(
            txn,
            author.as_bytes(),
            kind.as_u16(),
            &Timestamp::min(),
            &Timestamp::max(),
        )?;

        if let Some(result) = iter.next() {
            let (_key, id) = result?;
            return self.get_event_by_id(txn, id);
        }

        Ok(None)
    }

    pub fn find_addressable_event<'a>(
        &'a self,
        txn: &'a RoTxn,
        addr: &Coordinate,
    ) -> Result<Option<EventBorrow<'a>>, Error> {
        if !addr.kind.is_addressable() {
            return Err(Error::WrongEventKind);
        }

        let iter = self.atc_iter(
            txn,
            addr.public_key.as_bytes(),
            &SingleLetterTag::lowercase(Alphabet::D),
            &addr.identifier,
            &Timestamp::min(),
            &Timestamp::max(),
        )?;

        for result in iter {
            let (_key, id) = result?;
            let event = self.get_event_by_id(txn, id)?.ok_or(Error::NotFound)?;

            // the atc index doesn't have kind, so we have to compare the kinds
            if event.kind != addr.kind.as_u16() {
                continue;
            }

            return Ok(Some(event));
        }

        Ok(None)
    }

    /// Remove all replaceable events with the matching author-kind
    /// Kind must be a replaceable (not parameterized replaceable) event kind
    pub fn remove_replaceable(
        &self,
        txn: &mut RwTxn,
        coordinate: &Coordinate,
        until: &Timestamp,
    ) -> Result<(), Error> {
        if !coordinate.kind.is_replaceable() {
            return Err(Error::WrongEventKind);
        }

        let iter = self.akc_iter(
            txn,
            coordinate.public_key.as_bytes(),
            coordinate.kind.as_u16(),
            &Timestamp::zero(),
            until,
        )?;

        // Collect indexes for all events first to avoid iterator lifetime issues
        let mut indexes: Vec<EventIndexKeys> = Vec::new();

        for result in iter {
            let (_key, id) = result?;
            if let Some(event) = self.get_event_by_id(txn, id)? {
                indexes.push(EventIndexKeys::new(event));
            }
        }

        // Now perform deletions
        for index in indexes {
            self.remove(txn, &index)?;
        }

        Ok(())
    }

    /// Remove all parameterized-replaceable events with the matching author-kind-d
    /// Kind must be a parameterized-replaceable event kind
    pub fn remove_addressable(
        &self,
        txn: &mut RwTxn,
        coordinate: &Coordinate,
        until: Timestamp,
    ) -> Result<(), Error> {
        if !coordinate.kind.is_addressable() {
            return Err(Error::WrongEventKind);
        }

        let iter = self.atc_iter(
            txn,
            coordinate.public_key.as_bytes(),
            &SingleLetterTag::lowercase(Alphabet::D),
            &coordinate.identifier,
            &Timestamp::min(),
            &until,
        )?;

        // Collect DeletionInfo for all events first to avoid iterator lifetime issues
        let mut indexes = Vec::new();

        for result in iter {
            let (_key, id) = result?;
            if let Some(event) = self.get_event_by_id(txn, id)? {
                // Our index doesn't have Kind embedded, so we have to check it
                if event.kind == coordinate.kind.as_u16() {
                    indexes.push(EventIndexKeys::new(event));
                }
            }
        }

        // Now perform deletions
        for index in indexes {
            self.remove(txn, &index)?;
        }

        Ok(())
    }

    #[inline]
    pub(crate) fn is_deleted(&self, txn: &RoTxn, event_id: &EventId) -> Result<bool, Error> {
        Ok(self.deleted_ids.get(txn, event_id.as_bytes())?.is_some())
    }

    pub(crate) fn mark_deleted(&self, txn: &mut RwTxn, event_id: &EventId) -> Result<(), Error> {
        self.deleted_ids.put(txn, event_id.as_bytes(), &())?;
        Ok(())
    }

    pub(crate) fn mark_coordinate_deleted(
        &self,
        txn: &mut RwTxn,
        coordinate: &CoordinateBorrow,
        when: Timestamp,
    ) -> Result<(), Error> {
        let key: Vec<u8> = index::make_coordinate_index_key(coordinate);
        self.deleted_coordinates.put(txn, &key, &when.as_secs())?;
        Ok(())
    }

    pub(crate) fn when_is_coordinate_deleted<'a>(
        &self,
        txn: &RoTxn,
        coordinate: &'a CoordinateBorrow<'a>,
    ) -> Result<Option<Timestamp>, Error> {
        let key: Vec<u8> = index::make_coordinate_index_key(coordinate);
        Ok(self
            .deleted_coordinates
            .get(txn, &key)?
            .map(Timestamp::from_secs))
    }

    pub(crate) fn ci_iter<'a>(
        &'a self,
        txn: &'a RoTxn,
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<RoRange<'a, Bytes, Bytes>, Error> {
        let start_prefix = index::make_ci_index_key(until, &EVENT_ID_ALL_ZEROS);
        let end_prefix = index::make_ci_index_key(since, &EVENT_ID_ALL_255);
        let range = (
            Bound::Included(start_prefix.as_slice()),
            Bound::Excluded(end_prefix.as_slice()),
        );
        Ok(self.ci_index.range(txn, &range)?)
    }

    pub(crate) fn tc_iter<'a>(
        &'a self,
        txn: &'a RoTxn,
        tag_name: &SingleLetterTag,
        tag_value: &str,
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<RoRange<'a, Bytes, Bytes>, Error> {
        let start_prefix = index::make_tc_index_key(
            tag_name,
            tag_value,
            until, // scan goes backwards in time
            &EVENT_ID_ALL_ZEROS,
        );
        let end_prefix = index::make_tc_index_key(tag_name, tag_value, since, &EVENT_ID_ALL_255);
        let range = (
            Bound::Included(start_prefix.as_slice()),
            Bound::Excluded(end_prefix.as_slice()),
        );
        Ok(self.tc_index.range(txn, &range)?)
    }

    pub(crate) fn ac_iter<'a>(
        &'a self,
        txn: &'a RoTxn,
        author: &[u8; 32],
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<RoRange<'a, Bytes, Bytes>, Error> {
        let start_prefix = index::make_ac_index_key(author, until, &EVENT_ID_ALL_ZEROS);
        let end_prefix = index::make_ac_index_key(author, since, &EVENT_ID_ALL_255);
        let range = (
            Bound::Included(start_prefix.as_slice()),
            Bound::Excluded(end_prefix.as_slice()),
        );
        Ok(self.ac_index.range(txn, &range)?)
    }

    pub(crate) fn akc_iter<'a>(
        &'a self,
        txn: &'a RoTxn,
        author: &[u8; 32],
        kind: u16,
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<RoRange<'a, Bytes, Bytes>, Error> {
        let start_prefix = index::make_akc_index_key(author, kind, until, &EVENT_ID_ALL_ZEROS);
        let end_prefix = index::make_akc_index_key(author, kind, since, &EVENT_ID_ALL_255);
        let range = (
            Bound::Included(start_prefix.as_slice()),
            Bound::Excluded(end_prefix.as_slice()),
        );
        Ok(self.akc_index.range(txn, &range)?)
    }

    pub(crate) fn atc_iter<'a>(
        &'a self,
        txn: &'a RoTxn,
        author: &[u8; 32],
        tag_name: &SingleLetterTag,
        tag_value: &str,
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<RoRange<'a, Bytes, Bytes>, Error> {
        let start_prefix: Vec<u8> = index::make_atc_index_key(
            author,
            tag_name,
            tag_value,
            until, // scan goes backwards in time
            &EVENT_ID_ALL_ZEROS,
        );
        let end_prefix: Vec<u8> =
            index::make_atc_index_key(author, tag_name, tag_value, since, &EVENT_ID_ALL_255);
        let range = (
            Bound::Included(start_prefix.as_slice()),
            Bound::Excluded(end_prefix.as_slice()),
        );
        Ok(self.atc_index.range(txn, &range)?)
    }

    fn handle_deletion_event(&self, txn: &mut RwTxn, event: &Event) -> Result<bool, Error> {
        // Collect DeletionInfo and EventIds for all valid targets first
        let mut deletions_to_process = Vec::new();

        for id in event.tags.event_ids() {
            if let Some(target) = self.get_event_by_id(txn, id.as_bytes())? {
                // Author must match
                if target.pubkey != event.pubkey.as_bytes() {
                    return Ok(true);
                }

                deletions_to_process.push((*id, EventIndexKeys::new(target)));
            }
        }

        // Now process all deletions
        for (id, info) in deletions_to_process {
            // Mark the event ID as deleted (for NIP-09 deletion events)
            self.mark_deleted(txn, &id)?;

            // Remove from all indexes
            self.remove(txn, &info)?;
        }

        for coordinate in event.tags.coordinates() {
            // Author must match
            if coordinate.public_key != event.pubkey {
                return Ok(true);
            }

            // Mark deleted
            self.mark_coordinate_deleted(txn, &coordinate.borrow(), event.created_at)?;

            // Remove events (up to the created_at of the deletion event)
            if coordinate.kind.is_replaceable() {
                self.remove_replaceable(txn, coordinate, &event.created_at)?;
            } else if coordinate.kind.is_addressable() {
                self.remove_addressable(txn, coordinate, event.created_at)?;
            }
        }

        Ok(false)
    }

    pub(crate) fn ktc_iter<'a>(
        &'a self,
        txn: &'a RoTxn,
        kind: u16,
        tag_name: &SingleLetterTag,
        tag_value: &str,
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<RoRange<'a, Bytes, Bytes>, Error> {
        let start_prefix = index::make_ktc_index_key(
            kind,
            tag_name,
            tag_value,
            until, // scan goes backwards in time
            &EVENT_ID_ALL_ZEROS,
        );
        let end_prefix =
            index::make_ktc_index_key(kind, tag_name, tag_value, since, &EVENT_ID_ALL_255);
        let range = (
            Bound::Included(start_prefix.as_slice()),
            Bound::Excluded(end_prefix.as_slice()),
        );
        Ok(self.ktc_index.range(txn, &range)?)
    }

    // ============================================================================
    // Scoped methods for multi-tenant support
    // ============================================================================

    /// Get the scope registry for registering new scopes
    #[inline]
    pub(crate) fn scope_registry(&self) -> &Arc<GlobalScopeRegistry> {
        &self.scope_registry
    }

    /// Register a new scope in the global registry
    pub(crate) fn register_scope(&self, txn: &mut RwTxn, scope: &Scope) -> Result<(), Error> {
        if !scope.is_default() {
            self.scope_registry.register_scope(txn, scope)?;
        }
        Ok(())
    }

    /// List all registered scopes
    pub(crate) fn list_scopes(&self, txn: &RoTxn) -> Result<Vec<Scope>, Error> {
        Ok(self.scope_registry.list_all_scopes(txn)?)
    }

    /// Get event by ID within a scope
    #[inline]
    pub(crate) fn get_event_by_id_scoped<'a>(
        &self,
        txn: &'a RoTxn,
        scope: &Scope,
        event_id: &[u8],
    ) -> Result<Option<EventBorrow<'a>>, Error> {
        match self.events_scoped.get(txn, scope, event_id)? {
            Some(bytes) => Ok(Some(EventBorrow::decode(bytes)?)),
            None => Ok(None),
        }
    }

    /// Check if event exists within a scope
    #[inline]
    pub(crate) fn has_event_scoped(
        &self,
        txn: &RoTxn,
        scope: &Scope,
        event_id: &EventId,
    ) -> Result<bool, Error> {
        Ok(self
            .get_event_by_id_scoped(txn, scope, event_id.as_bytes())?
            .is_some())
    }

    /// Store event within a scope
    pub(crate) fn store_scoped(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        fbb: &mut FlatBufferBuilder,
        event: &Event,
    ) -> Result<(), Error> {
        // Store event
        self.events_scoped
            .put(txn, scope, event.id.as_bytes(), event.encode(fbb))?;

        // Index event
        let event: EventBorrow = EventBorrow::from(event);
        let index_keys: EventIndexKeys = EventIndexKeys::new(event);
        self.index_event_scoped(txn, scope, index_keys)
    }

    fn index_event_scoped(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        index: EventIndexKeys,
    ) -> Result<(), Error> {
        self.ci_index_scoped
            .put(txn, scope, &index.ci_index, &index.id)?;
        self.akc_index_scoped
            .put(txn, scope, &index.akc_index, &index.id)?;
        self.ac_index_scoped
            .put(txn, scope, &index.ac_index, &index.id)?;

        for tag in index.tags.into_iter() {
            self.atc_index_scoped
                .put(txn, scope, &tag.atc_index, &index.id)?;
            self.ktc_index_scoped
                .put(txn, scope, &tag.ktc_index, &index.id)?;
            self.tc_index_scoped
                .put(txn, scope, &tag.tc_index, &index.id)?;
        }

        Ok(())
    }

    fn remove_scoped(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        index: &EventIndexKeys,
    ) -> Result<(), Error> {
        self.events_scoped.delete(txn, scope, &index.id)?;
        self.ci_index_scoped.delete(txn, scope, &index.ci_index)?;
        self.akc_index_scoped.delete(txn, scope, &index.akc_index)?;
        self.ac_index_scoped.delete(txn, scope, &index.ac_index)?;

        // Delete tag indexes
        for tag in &index.tags {
            self.atc_index_scoped.delete(txn, scope, &tag.atc_index)?;
            self.ktc_index_scoped.delete(txn, scope, &tag.ktc_index)?;
            self.tc_index_scoped.delete(txn, scope, &tag.tc_index)?;
        }

        Ok(())
    }

    /// Wipe all data in a scope
    pub(crate) fn wipe_scoped(&self, txn: &mut RwTxn, scope: &Scope) -> Result<(), Error> {
        self.events_scoped.clear(txn, scope)?;
        self.ci_index_scoped.clear(txn, scope)?;
        self.tc_index_scoped.clear(txn, scope)?;
        self.ac_index_scoped.clear(txn, scope)?;
        self.akc_index_scoped.clear(txn, scope)?;
        self.atc_index_scoped.clear(txn, scope)?;
        self.ktc_index_scoped.clear(txn, scope)?;
        self.deleted_ids_scoped.clear(txn, scope)?;
        self.deleted_coordinates_scoped.clear(txn, scope)?;
        Ok(())
    }

    /// Check if event ID is deleted within a scope
    #[inline]
    pub(crate) fn is_deleted_scoped(
        &self,
        txn: &RoTxn,
        scope: &Scope,
        event_id: &EventId,
    ) -> Result<bool, Error> {
        Ok(self
            .deleted_ids_scoped
            .get(txn, scope, event_id.as_bytes())?
            .is_some())
    }

    /// Mark event ID as deleted within a scope
    pub(crate) fn mark_deleted_scoped(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        event_id: &EventId,
    ) -> Result<(), Error> {
        self.deleted_ids_scoped
            .put(txn, scope, event_id.as_bytes(), &[])?;
        Ok(())
    }

    /// Mark coordinate as deleted within a scope
    pub(crate) fn mark_coordinate_deleted_scoped(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        coordinate: &CoordinateBorrow,
        when: Timestamp,
    ) -> Result<(), Error> {
        let key: Vec<u8> = index::make_coordinate_index_key(coordinate);
        let when_bytes = when.as_secs().to_le_bytes();
        self.deleted_coordinates_scoped
            .put(txn, scope, &key, &when_bytes)?;
        Ok(())
    }

    /// Check when a coordinate was deleted within a scope
    pub(crate) fn when_is_coordinate_deleted_scoped(
        &self,
        txn: &RoTxn,
        scope: &Scope,
        coordinate: &CoordinateBorrow,
    ) -> Result<Option<Timestamp>, Error> {
        let key: Vec<u8> = index::make_coordinate_index_key(coordinate);
        Ok(self
            .deleted_coordinates_scoped
            .get(txn, scope, &key)?
            .map(|bytes| {
                let timestamp_u64 = u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]));
                Timestamp::from_secs(timestamp_u64)
            }))
    }

    /// Save event with transaction support within a scope
    pub(crate) fn save_event_with_txn_scoped(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        fbb: &mut FlatBufferBuilder,
        event: &Event,
    ) -> Result<SaveEventStatus, Error> {
        if event.kind.is_ephemeral() {
            return Ok(SaveEventStatus::Rejected(RejectedReason::Ephemeral));
        }

        // Already exists
        if self.has_event_scoped(txn, scope, &event.id)? {
            return Ok(SaveEventStatus::Rejected(RejectedReason::Duplicate));
        }

        // Reject event if ID was deleted
        if self.is_deleted_scoped(txn, scope, &event.id)? {
            return Ok(SaveEventStatus::Rejected(RejectedReason::Deleted));
        }

        // Reject event if ADDR was deleted after it's created_at date
        if let Some(coordinate) = event.coordinate() {
            if let Some(time) = self.when_is_coordinate_deleted_scoped(txn, scope, &coordinate)? {
                if event.created_at <= time {
                    return Ok(SaveEventStatus::Rejected(RejectedReason::Deleted));
                }
            }
        }

        // Remove replaceable events being replaced
        if event.kind.is_replaceable() {
            if let Some(stored) =
                self.find_replaceable_event_scoped(txn, scope, &event.pubkey, event.kind)?
            {
                if has_event_been_replaced(&stored, event) {
                    return Ok(SaveEventStatus::Rejected(RejectedReason::Replaced));
                }

                let coordinate = Coordinate::new(event.kind, event.pubkey);
                self.remove_replaceable_scoped(txn, scope, &coordinate, &event.created_at)?;
            }
        }

        // Remove addressable events being replaced
        if event.kind.is_addressable() {
            if let Some(identifier) = event.tags.identifier() {
                let coordinate = Coordinate::new(event.kind, event.pubkey).identifier(identifier);

                if let Some(stored) = self.find_addressable_event_scoped(txn, scope, &coordinate)? {
                    if has_event_been_replaced(&stored, event) {
                        return Ok(SaveEventStatus::Rejected(RejectedReason::Replaced));
                    }

                    self.remove_addressable_scoped(txn, scope, &coordinate, Timestamp::max())?;
                }
            }
        }

        // Handle deletion events
        if event.kind == Kind::EventDeletion {
            let invalid: bool = self.handle_deletion_event_scoped(txn, scope, event)?;
            if invalid {
                return Ok(SaveEventStatus::Rejected(RejectedReason::InvalidDelete));
            }
        }

        self.store_scoped(txn, scope, fbb, event)?;

        Ok(SaveEventStatus::Success)
    }

    fn handle_deletion_event_scoped(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        event: &Event,
    ) -> Result<bool, Error> {
        let mut deletions_to_process = Vec::new();

        for id in event.tags.event_ids() {
            if let Some(target) = self.get_event_by_id_scoped(txn, scope, id.as_bytes())? {
                if target.pubkey != event.pubkey.as_bytes() {
                    return Ok(true);
                }
                deletions_to_process.push((*id, EventIndexKeys::new(target)));
            }
        }

        for (id, info) in deletions_to_process {
            self.mark_deleted_scoped(txn, scope, &id)?;
            self.remove_scoped(txn, scope, &info)?;
        }

        for coordinate in event.tags.coordinates() {
            if coordinate.public_key != event.pubkey {
                return Ok(true);
            }

            self.mark_coordinate_deleted_scoped(txn, scope, &coordinate.borrow(), event.created_at)?;

            if coordinate.kind.is_replaceable() {
                self.remove_replaceable_scoped(txn, scope, coordinate, &event.created_at)?;
            } else if coordinate.kind.is_addressable() {
                self.remove_addressable_scoped(txn, scope, coordinate, event.created_at)?;
            }
        }

        Ok(false)
    }

    /// Find replaceable event within a scope
    pub fn find_replaceable_event_scoped<'a>(
        &self,
        txn: &'a RoTxn,
        scope: &Scope,
        author: &PublicKey,
        kind: Kind,
    ) -> Result<Option<EventBorrow<'a>>, Error> {
        if !kind.is_replaceable() {
            return Err(Error::WrongEventKind);
        }

        let mut iter = self.akc_iter_scoped(
            txn,
            scope,
            author.as_bytes(),
            kind.as_u16(),
            &Timestamp::min(),
            &Timestamp::max(),
        )?;

        if let Some(result) = iter.next() {
            let (_key, id) = result?;
            return self.get_event_by_id_scoped(txn, scope, id);
        }

        Ok(None)
    }

    /// Find addressable event within a scope
    pub fn find_addressable_event_scoped<'a>(
        &'a self,
        txn: &'a RoTxn,
        scope: &Scope,
        addr: &Coordinate,
    ) -> Result<Option<EventBorrow<'a>>, Error> {
        if !addr.kind.is_addressable() {
            return Err(Error::WrongEventKind);
        }

        let iter = self.atc_iter_scoped(
            txn,
            scope,
            addr.public_key.as_bytes(),
            &SingleLetterTag::lowercase(Alphabet::D),
            &addr.identifier,
            &Timestamp::min(),
            &Timestamp::max(),
        )?;

        for result in iter {
            let (_key, id) = result?;
            let event = self
                .get_event_by_id_scoped(txn, scope, id)?
                .ok_or(Error::NotFound)?;

            if event.kind == addr.kind.as_u16() {
                return Ok(Some(event));
            }
        }

        Ok(None)
    }

    /// Remove replaceable events within a scope
    pub fn remove_replaceable_scoped(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        coordinate: &Coordinate,
        until: &Timestamp,
    ) -> Result<(), Error> {
        if !coordinate.kind.is_replaceable() {
            return Err(Error::WrongEventKind);
        }

        let iter = self.akc_iter_scoped(
            txn,
            scope,
            coordinate.public_key.as_bytes(),
            coordinate.kind.as_u16(),
            &Timestamp::zero(),
            until,
        )?;

        let mut indexes: Vec<EventIndexKeys> = Vec::new();

        for result in iter {
            let (_key, id) = result?;
            if let Some(event) = self.get_event_by_id_scoped(txn, scope, id)? {
                indexes.push(EventIndexKeys::new(event));
            }
        }

        for index in indexes {
            self.remove_scoped(txn, scope, &index)?;
        }

        Ok(())
    }

    /// Remove addressable events within a scope
    pub fn remove_addressable_scoped(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        coordinate: &Coordinate,
        until: Timestamp,
    ) -> Result<(), Error> {
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
            &until,
        )?;

        let mut indexes = Vec::new();

        for result in iter {
            let (_key, id) = result?;
            if let Some(event) = self.get_event_by_id_scoped(txn, scope, id)? {
                if event.kind == coordinate.kind.as_u16() {
                    indexes.push(EventIndexKeys::new(event));
                }
            }
        }

        for index in indexes {
            self.remove_scoped(txn, scope, &index)?;
        }

        Ok(())
    }

    /// Query events within a scope
    pub fn query_scoped<'a>(
        &self,
        txn: &'a RoTxn,
        scope: &Scope,
        filter: Filter,
    ) -> Result<Box<dyn Iterator<Item = EventBorrow<'a>> + 'a>, Error> {
        if let (Some(since), Some(until)) = (filter.since, filter.until) {
            if since > until {
                return Ok(Box::new(iter::empty()));
            }
        }

        let mut output: BTreeSet<EventBorrow<'a>> = BTreeSet::new();

        let limit: Option<usize> = filter.limit;
        let since = filter.since.unwrap_or_else(Timestamp::min);
        let until = filter.until.unwrap_or_else(Timestamp::max);

        let filter: DatabaseFilter = filter.into();
        let pattern: QueryFilterPattern = QueryFilterPattern::from_filter(&filter);

        tracing::debug!("Querying scoped by pattern: {pattern:?}");

        match pattern {
            QueryFilterPattern::Ids => {
                self.query_by_ids_scoped(txn, scope, filter, limit, &mut output)?
            }
            QueryFilterPattern::AuthorsAndKinds => {
                self.query_by_authors_and_kinds_scoped(
                    txn, scope, filter, since, &until, limit, &mut output,
                )?;
            }
            QueryFilterPattern::AuthorsAndTags => {
                self.query_by_authors_and_tags_scoped(
                    txn, scope, filter, since, &until, limit, &mut output,
                )?;
            }
            QueryFilterPattern::AuthorKindsAndTags => {
                self.query_by_authors_kinds_and_tags_scoped(
                    txn, scope, filter, since, &until, limit, &mut output,
                )?;
            }
            QueryFilterPattern::KindsAndTags => {
                self.query_by_kinds_and_tags_scoped(
                    txn, scope, filter, since, &until, limit, &mut output,
                )?;
            }
            QueryFilterPattern::Tags => {
                self.query_by_tags_scoped(txn, scope, filter, since, &until, limit, &mut output)?;
            }
            QueryFilterPattern::Authors => {
                self.query_by_authors_scoped(
                    txn, scope, filter, since, &until, limit, &mut output,
                )?;
            }
            QueryFilterPattern::Scraping => {
                self.query_by_scraping_scoped(
                    txn, scope, filter, &since, &until, limit, &mut output,
                )?;
            }
        }

        Ok(match limit {
            Some(limit) => Box::new(output.into_iter().take(limit)),
            None => Box::new(output.into_iter()),
        })
    }

    fn query_by_ids_scoped<'a>(
        &self,
        txn: &'a RoTxn,
        scope: &Scope,
        filter: DatabaseFilter,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error> {
        for id in filter.ids.iter() {
            if let Some(limit) = limit {
                if output.len() >= limit {
                    break;
                }
            }

            if let Some(event) = self.get_event_by_id_scoped(txn, scope, id)? {
                if filter.match_event(&event) {
                    output.insert(event);
                }
            }
        }
        Ok(())
    }

    fn query_by_authors_and_kinds_scoped<'a>(
        &self,
        txn: &'a RoTxn,
        scope: &Scope,
        filter: DatabaseFilter,
        since: Timestamp,
        until: &Timestamp,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error> {
        let mut since = since;

        for author in filter.authors.iter() {
            for kind in filter.kinds.iter() {
                let iter = self.akc_iter_scoped(txn, scope, author, *kind, &since, until)?;

                let mut paircount = 0;

                'per_event: for result in iter {
                    let (_key, value) = result?;
                    let event = self
                        .get_event_by_id_scoped(txn, scope, value)?
                        .ok_or(Error::NotFound)?;

                    if event.created_at < since {
                        break 'per_event;
                    }

                    if filter.match_event(&event) {
                        let created_at = event.created_at;
                        output.insert(event);
                        paircount += 1;

                        if let Some(limit) = limit {
                            if paircount >= limit {
                                if created_at > since {
                                    since = created_at;
                                }
                                break 'per_event;
                            }
                        }

                        if Kind::from(*kind).is_replaceable() {
                            break 'per_event;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn query_by_authors_and_tags_scoped<'a>(
        &self,
        txn: &'a RoTxn,
        scope: &Scope,
        filter: DatabaseFilter,
        since: Timestamp,
        until: &Timestamp,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error> {
        let mut since = since;

        for author in filter.authors.iter() {
            for (tagname, set) in filter.generic_tags.iter() {
                for tag_value in set.iter() {
                    let iter =
                        self.atc_iter_scoped(txn, scope, author, tagname, tag_value, &since, until)?;
                    self.iterate_filter_until_limit_scoped(
                        txn, scope, &filter, iter, &mut since, limit, output,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn query_by_authors_kinds_and_tags_scoped<'a>(
        &self,
        txn: &'a RoTxn,
        scope: &Scope,
        filter: DatabaseFilter,
        since: Timestamp,
        until: &Timestamp,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error> {
        let mut since = since;

        for author in filter.authors.iter() {
            for kind in filter.kinds.iter() {
                let akc_iter = self.akc_iter_scoped(txn, scope, author, *kind, &since, until)?;

                let akc_set: BTreeSet<&[u8]> = akc_iter
                    .filter_map(|res| {
                        let (_k, v) = res.ok()?;
                        Some(v)
                    })
                    .collect();

                for (tagname, set) in filter.generic_tags.iter() {
                    for tag_value in set.iter() {
                        let atc_iter = self.atc_iter_scoped(
                            txn, scope, author, tagname, tag_value, &since, until,
                        )?;

                        let atc_set: BTreeSet<&[u8]> = atc_iter
                            .filter_map(|res| {
                                let (_k, v) = res.ok()?;
                                Some(v)
                            })
                            .collect();

                        let iter = atc_set.intersection(&akc_set).copied();

                        self.iterate_filter_until_limit_scoped_ids(
                            txn, scope, &filter, iter, &mut since, limit, output,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn query_by_kinds_and_tags_scoped<'a>(
        &self,
        txn: &'a RoTxn,
        scope: &Scope,
        filter: DatabaseFilter,
        since: Timestamp,
        until: &Timestamp,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error> {
        let mut since = since;

        for kind in filter.kinds.iter() {
            for (tag_name, set) in filter.generic_tags.iter() {
                for tag_value in set.iter() {
                    let iter =
                        self.ktc_iter_scoped(txn, scope, *kind, tag_name, tag_value, &since, until)?;
                    self.iterate_filter_until_limit_scoped(
                        txn, scope, &filter, iter, &mut since, limit, output,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn query_by_tags_scoped<'a>(
        &self,
        txn: &'a RoTxn,
        scope: &Scope,
        filter: DatabaseFilter,
        since: Timestamp,
        until: &Timestamp,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error> {
        let mut since = since;

        for (tag_name, set) in filter.generic_tags.iter() {
            for tag_value in set.iter() {
                let iter = self.tc_iter_scoped(txn, scope, tag_name, tag_value, &since, until)?;
                self.iterate_filter_until_limit_scoped(
                    txn, scope, &filter, iter, &mut since, limit, output,
                )?;
            }
        }
        Ok(())
    }

    fn query_by_authors_scoped<'a>(
        &self,
        txn: &'a RoTxn,
        scope: &Scope,
        filter: DatabaseFilter,
        since: Timestamp,
        until: &Timestamp,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error> {
        let mut since = since;

        for author in filter.authors.iter() {
            let iter = self.ac_iter_scoped(txn, scope, author, &since, until)?;
            self.iterate_filter_until_limit_scoped(
                txn, scope, &filter, iter, &mut since, limit, output,
            )?;
        }
        Ok(())
    }

    fn query_by_scraping_scoped<'a>(
        &self,
        txn: &'a RoTxn,
        scope: &Scope,
        filter: DatabaseFilter,
        since: &Timestamp,
        until: &Timestamp,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error> {
        let iter = self.ci_iter_scoped(txn, scope, since, until)?;

        for result in iter {
            if let Some(limit) = limit {
                if output.len() >= limit {
                    break;
                }
            }

            let (_key, value) = result?;
            let event = self
                .get_event_by_id_scoped(txn, scope, value)?
                .ok_or(Error::NotFound)?;

            if filter.match_event(&event) {
                output.insert(event);
            }
        }

        Ok(())
    }

    fn iterate_filter_until_limit_scoped<'a>(
        &self,
        txn: &'a RoTxn,
        scope: &Scope,
        filter: &DatabaseFilter,
        iter: ScopedIterator<'a>,
        since: &mut Timestamp,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error> {
        let mut count: usize = 0;

        for result in iter {
            let (_key, id) = result?;
            let event = self
                .get_event_by_id_scoped(txn, scope, id)?
                .ok_or(Error::NotFound)?;

            if event.created_at < *since {
                break;
            }

            if filter.match_event(&event) {
                let created_at = event.created_at;
                output.insert(event);
                count += 1;

                if let Some(limit) = limit {
                    if count >= limit {
                        if created_at > *since {
                            *since = created_at;
                        }
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    fn iterate_filter_until_limit_scoped_ids<'a, 'i, I>(
        &self,
        txn: &'a RoTxn,
        scope: &Scope,
        filter: &DatabaseFilter,
        iter: I,
        since: &mut Timestamp,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error>
    where
        I: IntoIterator<Item = &'i [u8]>,
    {
        let mut count: usize = 0;

        for id in iter {
            let event = self
                .get_event_by_id_scoped(txn, scope, id)?
                .ok_or(Error::NotFound)?;

            if event.created_at < *since {
                break;
            }

            if filter.match_event(&event) {
                let created_at = event.created_at;
                output.insert(event);
                count += 1;

                if let Some(limit) = limit {
                    if count >= limit {
                        if created_at > *since {
                            *since = created_at;
                        }
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Delete events within a scope
    pub fn delete_scoped(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        filter: Filter,
    ) -> Result<(), Error> {
        let indexes: Vec<EventIndexKeys> = {
            let events = self.query_scoped(txn, scope, filter)?;
            events
                .into_iter()
                .map(|event| EventIndexKeys::new(event))
                .collect()
        };

        for index in indexes {
            self.remove_scoped(txn, scope, &index)?;
        }

        Ok(())
    }

    // Scoped iterator methods
    pub(crate) fn ci_iter_scoped<'txn>(
        &self,
        txn: &'txn RoTxn,
        scope: &Scope,
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<ScopedIterator<'txn>, Error> {
        let start_prefix = index::make_ci_index_key(until, &EVENT_ID_ALL_ZEROS);
        let end_prefix = index::make_ci_index_key(since, &EVENT_ID_ALL_255);
        let range = (
            Bound::Included(start_prefix.as_slice()),
            Bound::Excluded(end_prefix.as_slice()),
        );
        self.ci_index_scoped
            .range(txn, scope, &range)
            .map_err(Error::from)
    }

    pub(crate) fn tc_iter_scoped<'txn>(
        &self,
        txn: &'txn RoTxn,
        scope: &Scope,
        tag_name: &SingleLetterTag,
        tag_value: &str,
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<ScopedIterator<'txn>, Error> {
        let start_prefix = index::make_tc_index_key(tag_name, tag_value, until, &EVENT_ID_ALL_ZEROS);
        let end_prefix = index::make_tc_index_key(tag_name, tag_value, since, &EVENT_ID_ALL_255);
        let range = (
            Bound::Included(start_prefix.as_slice()),
            Bound::Excluded(end_prefix.as_slice()),
        );
        self.tc_index_scoped
            .range(txn, scope, &range)
            .map_err(Error::from)
    }

    pub(crate) fn ac_iter_scoped<'txn>(
        &self,
        txn: &'txn RoTxn,
        scope: &Scope,
        author: &[u8; 32],
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<ScopedIterator<'txn>, Error> {
        let start_prefix = index::make_ac_index_key(author, until, &EVENT_ID_ALL_ZEROS);
        let end_prefix = index::make_ac_index_key(author, since, &EVENT_ID_ALL_255);
        let range = (
            Bound::Included(start_prefix.as_slice()),
            Bound::Excluded(end_prefix.as_slice()),
        );
        self.ac_index_scoped
            .range(txn, scope, &range)
            .map_err(Error::from)
    }

    pub(crate) fn akc_iter_scoped<'txn>(
        &self,
        txn: &'txn RoTxn,
        scope: &Scope,
        author: &[u8; 32],
        kind: u16,
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<ScopedIterator<'txn>, Error> {
        let start_prefix = index::make_akc_index_key(author, kind, until, &EVENT_ID_ALL_ZEROS);
        let end_prefix = index::make_akc_index_key(author, kind, since, &EVENT_ID_ALL_255);
        let range = (
            Bound::Included(start_prefix.as_slice()),
            Bound::Excluded(end_prefix.as_slice()),
        );
        self.akc_index_scoped
            .range(txn, scope, &range)
            .map_err(Error::from)
    }

    pub(crate) fn atc_iter_scoped<'txn>(
        &self,
        txn: &'txn RoTxn,
        scope: &Scope,
        author: &[u8; 32],
        tag_name: &SingleLetterTag,
        tag_value: &str,
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<ScopedIterator<'txn>, Error> {
        let start_prefix: Vec<u8> =
            index::make_atc_index_key(author, tag_name, tag_value, until, &EVENT_ID_ALL_ZEROS);
        let end_prefix: Vec<u8> =
            index::make_atc_index_key(author, tag_name, tag_value, since, &EVENT_ID_ALL_255);
        let range = (
            Bound::Included(start_prefix.as_slice()),
            Bound::Excluded(end_prefix.as_slice()),
        );
        self.atc_index_scoped
            .range(txn, scope, &range)
            .map_err(Error::from)
    }

    pub(crate) fn ktc_iter_scoped<'txn>(
        &self,
        txn: &'txn RoTxn,
        scope: &Scope,
        kind: u16,
        tag_name: &SingleLetterTag,
        tag_value: &str,
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<ScopedIterator<'txn>, Error> {
        let start_prefix =
            index::make_ktc_index_key(kind, tag_name, tag_value, until, &EVENT_ID_ALL_ZEROS);
        let end_prefix =
            index::make_ktc_index_key(kind, tag_name, tag_value, since, &EVENT_ID_ALL_255);
        let range = (
            Bound::Included(start_prefix.as_slice()),
            Bound::Excluded(end_prefix.as_slice()),
        );
        self.ktc_index_scoped
            .range(txn, scope, &range)
            .map_err(Error::from)
    }
}

/// Check if the new event should replace the stored one.
fn has_event_been_replaced(stored: &EventBorrow, event: &Event) -> bool {
    match stored.created_at.cmp(&event.created_at) {
        Ordering::Greater => true,
        Ordering::Equal => {
            // NIP-01: When timestamps are identical, keep the event with the lowest ID
            stored.id < event.id.as_bytes()
        }
        // Stored event is older than the new event, so it is not replaced yet.
        Ordering::Less => false,
    }
}
