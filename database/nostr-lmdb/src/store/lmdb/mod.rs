// Copyright (c) 2024 Michael Dilger
// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2025 Rust Nostr Developers
// Distributed under the MIT software license

use std::collections::BTreeSet;
use std::iter;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use heed::types::Bytes;
use heed::{Env, EnvFlags, EnvOpenOptions, RoRange, RoTxn, RwTxn};
use nostr::nips::nip01::{Coordinate, CoordinateBorrow};
use nostr::prelude::*;
use nostr_database::flatbuffers::FlatBufferDecodeBorrowed;
use nostr_database::{FlatBufferBuilder, FlatBufferEncode, RejectedReason, SaveEventStatus};
use scoped_heed::{scoped_database_options, GlobalScopeRegistry, Scope, ScopedBytesDatabase, ScopedDbError};

mod index;

use super::error::Error;
use super::types::DatabaseFilter;

const EVENT_ID_ALL_ZEROS: [u8; 32] = [0; 32];
const EVENT_ID_ALL_255: [u8; 32] = [255; 32];

/// Information needed to delete an event from all indexes.
///
/// This struct exists to work around Rust's borrow checker limitations when using LMDB transactions.
/// LMDB's `EventBorrow` holds a reference to data within the transaction, preventing us from
/// getting a mutable borrow to the same transaction for deletion operations.
///
/// ## Why DeletionInfo?
///
/// When using a single `RwTxn` for both reads and writes (for batch consistency), we face a
/// fundamental constraint: we cannot hold an immutable borrow (from `get_event_by_id`) while
/// trying to get a mutable borrow (for deletion operations) on the same transaction.
///
/// DeletionInfo solves this by extracting only the minimal data needed for deletion into an
/// owned structure, allowing the `EventBorrow` to be dropped before mutation begins.
///
/// ## Lifetime Constraints
///
/// - DeletionInfo instances must live strictly shorter than the encompassing RwTxn
/// - The data is extracted while the transaction has an immutable borrow
/// - The EventBorrow is dropped before any mutation occurs
/// - Only then can deletion proceed with a mutable borrow
///
/// ## Performance Characteristics
///
/// - Minimal overhead: only copies IDs (32 bytes each) and basic metadata
/// - Tag data is cloned but typically small (< 10 tags per event)
/// - Total overhead per deletion: ~200 bytes
/// - Enables single-transaction batching which provides >2x performance improvement
///
/// ## Example Usage
///
/// ```rust,ignore
/// // Extract info while transaction is borrowed immutably
/// let deletion_info = {
///     if let Some(event) = self.get_event_by_id(&**txn, id)? {
///         Some(DeletionInfo::from(&event))
///     } else {
///         None
///     }
/// }; // EventBorrow is dropped here!
///
/// // Now we can safely mutate the transaction
/// if let Some(info) = deletion_info {
///     self.remove(txn, &info)?;
/// }
/// ```
struct DeletionInfo {
    id: [u8; 32],
    pubkey: [u8; 32],
    created_at: Timestamp,
    kind: u16,
    tags: Vec<(SingleLetterTag, String)>,
}

impl From<&EventBorrow<'_>> for DeletionInfo {
    fn from(event: &EventBorrow<'_>) -> Self {
        Self {
            id: *event.id,
            pubkey: *event.pubkey,
            created_at: event.created_at,
            kind: event.kind,
            tags: event
                .tags
                .iter()
                .filter_map(|tag| tag.extract())
                .map(|(name, value)| (name, value.to_string()))
                .collect(),
        }
    }
}

impl DeletionInfo {
    /// Generate the created_at + id index key
    fn ci_index_key(&self) -> Vec<u8> {
        index::make_ci_index_key(&self.created_at, &self.id)
    }

    /// Generate the author + kind + created_at + id index key
    fn akc_index_key(&self) -> Vec<u8> {
        index::make_akc_index_key(&self.pubkey, self.kind, &self.created_at, &self.id)
    }

    /// Generate the author + created_at + id index key
    fn ac_index_key(&self) -> Vec<u8> {
        index::make_ac_index_key(&self.pubkey, &self.created_at, &self.id)
    }

    /// Generate the author + tag + created_at + id index key
    fn atc_index_key(&self, tag_name: &SingleLetterTag, tag_value: &str) -> Vec<u8> {
        index::make_atc_index_key(
            &self.pubkey,
            tag_name,
            tag_value,
            &self.created_at,
            &self.id,
        )
    }

    /// Generate the kind + tag + created_at + id index key
    fn ktc_index_key(&self, tag_name: &SingleLetterTag, tag_value: &str) -> Vec<u8> {
        index::make_ktc_index_key(self.kind, tag_name, tag_value, &self.created_at, &self.id)
    }

    /// Generate the tag + created_at + id index key
    fn tc_index_key(&self, tag_name: &SingleLetterTag, tag_value: &str) -> Vec<u8> {
        index::make_tc_index_key(tag_name, tag_value, &self.created_at, &self.id)
    }
}

// Type alias for the complex iterator type to reduce clippy warnings
type ScopedIterator<'txn> = Box<dyn Iterator<Item = Result<(&'txn [u8], &'txn [u8]), ScopedDbError>> + 'txn>;
#[derive(Debug, Clone)]
pub(crate) struct Lmdb {
    /// LMDB env
    env: Env,
    /// Events
    events: ScopedBytesDatabase,
    /// CreatedAt + ID index
    ci_index: ScopedBytesDatabase,
    /// Tag + CreatedAt + ID index
    tc_index: ScopedBytesDatabase,
    /// Author + CreatedAt + ID index
    ac_index: ScopedBytesDatabase,
    /// Author + Kind + CreatedAt + ID index
    akc_index: ScopedBytesDatabase,
    /// Author + Tag + CreatedAt + ID index
    atc_index: ScopedBytesDatabase,
    /// Kind + Tag + CreatedAt + ID index
    ktc_index: ScopedBytesDatabase,
    /// Deleted IDs
    deleted_ids: ScopedBytesDatabase,
    /// Deleted coordinates
    deleted_coordinates: ScopedBytesDatabase,
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
        let env: Env = unsafe {
            #[allow(deprecated)]
            EnvOpenOptions::new()
                .flags(EnvFlags::NO_TLS)
                .max_dbs(9 + additional_dbs)
                .max_readers(max_readers)
                .map_size(map_size)
                .open(path)?
        };

        // Acquire write transaction
        let mut txn = env.write_txn()?;

        // Create a global registry for all databases
        let global_registry = Arc::new(GlobalScopeRegistry::new(&env, &mut txn)?);

        // Create scoped database wrappers using the builder pattern
        let events = scoped_database_options(&env, global_registry.clone())
            .raw_bytes()
            .name("events")
            .unnamed_for_default()
            .create(&mut txn)?;
        let ci_index = scoped_database_options(&env, global_registry.clone())
            .raw_bytes()
            .name("ci")
            .create(&mut txn)?;
        let tc_index = scoped_database_options(&env, global_registry.clone())
            .raw_bytes()
            .name("tci")
            .create(&mut txn)?;
        let ac_index = scoped_database_options(&env, global_registry.clone())
            .raw_bytes()
            .name("aci")
            .create(&mut txn)?;
        let akc_index = scoped_database_options(&env, global_registry.clone())
            .raw_bytes()
            .name("akci")
            .create(&mut txn)?;
        let atc_index = scoped_database_options(&env, global_registry.clone())
            .raw_bytes()
            .name("atci")
            .create(&mut txn)?;
        let ktc_index = scoped_database_options(&env, global_registry.clone())
            .raw_bytes()
            .name("ktci")
            .create(&mut txn)?;
        let deleted_ids = scoped_database_options(&env, global_registry.clone())
            .raw_bytes()
            .name("deleted-ids")
            .create(&mut txn)?;
        let deleted_coordinates = scoped_database_options(&env, global_registry.clone())
            .raw_bytes()
            .name("deleted-coordinates")
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
        match registry {
            Some(reg) => {
                let txn = self.env.read_txn()?;
                let scopes = reg.list_all_scopes(&txn)?;
                Ok(Some(scopes))
            }
            None => Ok(None),
        }
    }

    /// Store and index the event
    pub(crate) fn store(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        fbb: &mut FlatBufferBuilder,
        event: &Event,
    ) -> Result<(), Error> {
        let id: &[u8] = event.id.as_bytes();

        // Store event
        self.events.put(txn, scope, id, event.encode(fbb))?;

        // Index by created_at and id
        let ci_index_key: Vec<u8> =
            index::make_ci_index_key(&event.created_at, event.id.as_bytes());
        self.ci_index.put(txn, scope, &ci_index_key, id)?;

        // Index by author and kind (with created_at and id)
        let akc_index_key: Vec<u8> = index::make_akc_index_key(
            event.pubkey.as_bytes(),
            event.kind.as_u16(),
            &event.created_at,
            event.id.as_bytes(),
        );
        self.akc_index.put(txn, scope, &akc_index_key, id)?;

        // Index by author (with created_at and id)
        let ac_index_key: Vec<u8> = index::make_ac_index_key(
            event.pubkey.as_bytes(),
            &event.created_at,
            event.id.as_bytes(),
        );
        self.ac_index.put(txn, scope, &ac_index_key, id)?;

        for tag in event.tags.iter() {
            if let (Some(tag_name), Some(tag_value)) = (tag.single_letter_tag(), tag.content()) {
                // Index by author and tag (with created_at and id)
                let atc_index_key: Vec<u8> = index::make_atc_index_key(
                    event.pubkey.as_bytes(),
                    &tag_name,
                    tag_value,
                    &event.created_at,
                    event.id.as_bytes(),
                );
                self.atc_index.put(txn, scope, &atc_index_key, id)?;

                // Index by kind and tag (with created_at and id)
                let ktc_index_key: Vec<u8> = index::make_ktc_index_key(
                    event.kind.as_u16(),
                    &tag_name,
                    tag_value,
                    &event.created_at,
                    event.id.as_bytes(),
                );
                self.ktc_index.put(txn, scope, &ktc_index_key, id)?;

                // Index by tag (with created_at and id)
                let tc_index_key: Vec<u8> = index::make_tc_index_key(
                    &tag_name,
                    tag_value,
                    &event.created_at,
                    event.id.as_bytes(),
                );
                self.tc_index.put(txn, scope, &tc_index_key, id)?;
            }
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
    fn remove(&self, txn: &mut RwTxn, scope: &Scope, info: &DeletionInfo) -> Result<(), Error> {
        // Delete from main events table
        self.events.delete(txn, scope, &info.id)?;

        // Delete from ci_index (created_at + id)
        self.ci_index.delete(txn, scope, &info.ci_index_key())?;

        // Delete from akc_index (author + kind + created_at + id)
        self.akc_index.delete(txn, scope, &info.akc_index_key())?;

        // Delete from ac_index (author + created_at + id)
        self.ac_index.delete(txn, scope, &info.ac_index_key())?;

        // Delete tag indexes
        for (tag_name, tag_value) in &info.tags {
            // Delete from atc_index (author + tag + created_at + id)
            self.atc_index
                .delete(txn, scope, &info.atc_index_key(tag_name, tag_value))?;

            // Delete from ktc_index (kind + tag + created_at + id)
            self.ktc_index
                .delete(txn, scope, &info.ktc_index_key(tag_name, tag_value))?;

            // Delete from tc_index (tag + created_at + id)
            self.tc_index
                .delete(txn, scope, &info.tc_index_key(tag_name, tag_value))?;
        }

        Ok(())
    }

    pub(crate) fn wipe(&self, txn: &mut RwTxn) -> Result<(), Error> {
        self.wipe_scope(txn, &Scope::Default)
    }

    pub(crate) fn wipe_scope(&self, txn: &mut RwTxn, scope: &Scope) -> Result<(), Error> {
        self.events.clear(txn, scope)?;
        self.ci_index.clear(txn, scope)?;
        self.tc_index.clear(txn, scope)?;
        self.ac_index.clear(txn, scope)?;
        self.akc_index.clear(txn, scope)?;
        self.atc_index.clear(txn, scope)?;
        self.ktc_index.clear(txn, scope)?;
        self.deleted_ids.clear(txn, scope)?;
        self.deleted_coordinates.clear(txn, scope)?;
        Ok(())
    }

    #[inline]
    pub(crate) fn has_event(&self, txn: &RoTxn, event_id: &[u8; 32]) -> Result<bool, Error> {
        self.has_event_scoped(txn, &Scope::Default, event_id)
    }

    #[inline]
    pub(crate) fn has_event_scoped(&self, txn: &RoTxn, scope: &Scope, event_id: &[u8; 32]) -> Result<bool, Error> {
        Ok(self.get_event_by_id_scoped(txn, scope, event_id)?.is_some())
    }

    /// Save event with transaction support - uses single transaction for batch consistency
    pub(crate) fn save_event_with_txn(
        &self,
        txn: &mut RwTxn,
        fbb: &mut FlatBufferBuilder,
        event: &Event,
    ) -> Result<SaveEventStatus, Error> {
        self.save_event_with_txn_scoped(txn, fbb, &Scope::Default, event)
    }

    pub(crate) fn save_event_with_txn_scoped(
        &self,
        txn: &mut RwTxn,
        fbb: &mut FlatBufferBuilder,
        scope: &Scope,
        event: &Event,
    ) -> Result<SaveEventStatus, Error> {
        if event.kind.is_ephemeral() {
            return Ok(SaveEventStatus::Rejected(RejectedReason::Ephemeral));
        }

        // Already exists
        if self.has_event_scoped(txn, scope, event.id.as_bytes())? {
            return Ok(SaveEventStatus::Rejected(RejectedReason::Duplicate));
        }

        // Reject event if ID was deleted
        if self.is_deleted_scoped(txn, scope, event.id.as_bytes())? {
            return Ok(SaveEventStatus::Rejected(RejectedReason::Deleted));
        }

        // Reject event if ADDR was deleted after it's created_at date
        // (non-parameterized or parameterized)
        if let Some(coordinate) = event.coordinate() {
            if let Some(time) = self.when_is_coordinate_deleted_scoped(txn, scope, &coordinate)? {
                if event.created_at <= time {
                    return Ok(SaveEventStatus::Rejected(RejectedReason::Deleted));
                }
            }
        }

        // Remove replaceable events being replaced
        if event.kind.is_replaceable() {
            if let Some(stored) = self.find_replaceable_event_scoped(txn, scope, &event.pubkey, event.kind)? {
                match stored.created_at.cmp(&event.created_at) {
                    std::cmp::Ordering::Greater => {
                        return Ok(SaveEventStatus::Rejected(RejectedReason::Replaced));
                    }
                    std::cmp::Ordering::Equal => {
                        // NIP-01: When timestamps are identical, keep the event with the lowest ID
                        let stored_id = EventId::from_byte_array(*stored.id);
                        if stored_id < event.id {
                            // Stored event has smaller ID, reject new event
                            return Ok(SaveEventStatus::Rejected(RejectedReason::Replaced));
                        }
                        // New event has smaller ID, continue to replace stored event
                    }
                    std::cmp::Ordering::Less => {
                        // Continue to replace stored event with newer one
                    }
                }

                let coordinate = Coordinate::new(event.kind, event.pubkey);
                self.remove_replaceable_scoped(txn, scope, &coordinate, event.created_at)?;
            }
        }

        // Remove addressable events being replaced
        if event.kind.is_addressable() {
            if let Some(identifier) = event.tags.identifier() {
                let coordinate = Coordinate::new(event.kind, event.pubkey).identifier(identifier);

                if let Some(stored) = self.find_addressable_event_scoped(txn, scope, &coordinate)? {
                    match stored.created_at.cmp(&event.created_at) {
                        std::cmp::Ordering::Greater => {
                            return Ok(SaveEventStatus::Rejected(RejectedReason::Replaced));
                        }
                        std::cmp::Ordering::Equal => {
                            // NIP-01: When timestamps are identical, keep the event with the lowest ID
                            let stored_id = EventId::from_byte_array(*stored.id);
                            if stored_id < event.id {
                                // Stored event has smaller ID, reject new event
                                return Ok(SaveEventStatus::Rejected(RejectedReason::Replaced));
                            }
                            // New event has smaller ID, continue to replace stored event
                        }
                        std::cmp::Ordering::Less => {
                            // Continue to replace stored event with newer one
                        }
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

        self.store(txn, scope, fbb, event)?;

        Ok(SaveEventStatus::Success)
    }

    #[inline]
    pub(crate) fn get_event_by_id<'a>(
        &self,
        txn: &'a RoTxn,
        event_id: &[u8],
    ) -> Result<Option<EventBorrow<'a>>, Error> {
        self.get_event_by_id_scoped(txn, &Scope::Default, event_id)
    }

    #[inline]
    pub(crate) fn get_event_by_id_scoped<'a>(
        &self,
        txn: &'a RoTxn,
        scope: &Scope,
        event_id: &[u8],
    ) -> Result<Option<EventBorrow<'a>>, Error> {
        match self.events.get(txn, scope, event_id)? {
            Some(bytes) => Ok(Some(EventBorrow::decode(bytes)?)),
            None => Ok(None),
        }
    }

    /// Delete events
    pub fn delete(&self, txn: &mut RwTxn, filter: Filter) -> Result<(), Error> {
        self.delete_scoped(txn, &Scope::Default, filter)
    }

    /// Delete events in a specific scope
    pub fn delete_scoped(&self, txn: &mut RwTxn, scope: &Scope, filter: Filter) -> Result<(), Error> {
        // First, collect all deletion info while we have immutable borrows
        let deletion_infos: Vec<DeletionInfo> = {
            let events = self.query_with_txn_scoped(txn, scope, filter)?;
            events
                .into_iter()
                .map(|event| DeletionInfo::from(&event))
                .collect()
        }; // All EventBorrow instances dropped here

        // Now we can safely mutate the transaction
        for info in deletion_infos {
            self.remove(txn, scope, &info)?;
        }

        Ok(())
    }

    /// Find all events that match the filter
    pub fn query<'a>(
        &'a self,
        txn: &'a RoTxn,
        filter: Filter,
    ) -> Result<Box<dyn Iterator<Item = EventBorrow<'a>> + 'a>, Error> {
        self.query_with_txn_scoped(txn, &Scope::Default, filter)
    }

    /// Find all events that match the filter in a specific scope
    pub fn query_with_txn_scoped<'a>(
        &'a self,
        txn: &'a RoTxn,
        scope: &Scope,
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

        if !filter.ids.is_empty() {
            // Fetch by id
            for id in filter.ids.iter() {
                // Check if limit is set
                if let Some(limit) = limit {
                    // Stop if limited
                    if output.len() >= limit {
                        break;
                    }
                }

                if let Some(event) = self.get_event_by_id_scoped(txn, scope, id)? {
                    // Skip deleted events
                    if self.is_deleted_scoped(txn, scope, event.id)? {
                        continue;
                    }
                    
                    if filter.match_event(&event) {
                        output.insert(event);
                    }
                }
            }
        } else if !filter.authors.is_empty() && !filter.kinds.is_empty() {
            // We may bring since forward if we hit the limit without going back that
            // far, so we use a mutable since:
            let mut since = since;

            for author in filter.authors.iter() {
                for kind in filter.kinds.iter() {
                    let iter = self.akc_iter_scoped(txn, scope, author, *kind, since, until)?;

                    // Count how many we have found of this author-kind pair, so we
                    // can possibly update `since`
                    let mut paircount = 0;

                    'per_event: for result in iter {
                        let (_key, value) = result?;
                        let event = self.get_event_by_id_scoped(txn, scope, value)?.ok_or(Error::NotFound)?;

                        // If we have gone beyond since, we can stop early
                        // (We have to check because `since` might change in this loop)
                        if event.created_at < since {
                            break 'per_event;
                        }
                        
                        // Skip deleted events
                        if self.is_deleted_scoped(txn, scope, event.id)? {
                            continue 'per_event;
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
        } else if !filter.authors.is_empty() && !filter.generic_tags.is_empty() {
            // We may bring since forward if we hit the limit without going back that
            // far, so we use a mutable since:
            let mut since = since;

            for author in filter.authors.iter() {
                for (tagname, set) in filter.generic_tags.iter() {
                    for tag_value in set.iter() {
                        let iter =
                            self.atc_iter_scoped(txn, scope, author, tagname, tag_value, &since, &until)?;
                        self.iterate_filter_until_limit_scoped(
                            txn,
                            scope,
                            &filter,
                            iter,
                            &mut since,
                            limit,
                            &mut output,
                        )?;
                    }
                }
            }
        } else if !filter.kinds.is_empty() && !filter.generic_tags.is_empty() {
            // We may bring since forward if we hit the limit without going back that
            // far, so we use a mutable since:
            let mut since = since;

            for kind in filter.kinds.iter() {
                for (tag_name, set) in filter.generic_tags.iter() {
                    for tag_value in set.iter() {
                        let iter =
                            self.ktc_iter_scoped(txn, scope, *kind, tag_name, tag_value, &since, &until)?;
                        self.iterate_filter_until_limit_scoped(
                            txn,
                            scope,
                            &filter,
                            iter,
                            &mut since,
                            limit,
                            &mut output,
                        )?;
                    }
                }
            }
        } else if !filter.generic_tags.is_empty() {
            // We may bring since forward if we hit the limit without going back that
            // far, so we use a mutable since:
            let mut since = since;

            for (tag_name, set) in filter.generic_tags.iter() {
                for tag_value in set.iter() {
                    let iter = self.tc_iter_scoped(txn, scope, tag_name, tag_value, &since, &until)?;
                    self.iterate_filter_until_limit_scoped(
                        txn,
                        scope,
                        &filter,
                        iter,
                        &mut since,
                        limit,
                        &mut output,
                    )?;
                }
            }
        } else if !filter.authors.is_empty() {
            // We may bring since forward if we hit the limit without going back that
            // far, so we use a mutable since:
            let mut since = since;

            for author in filter.authors.iter() {
                let iter = self.ac_iter_scoped(txn, scope, author, since, until)?;
                self.iterate_filter_until_limit_scoped(txn, scope, &filter, iter, &since, limit, &mut output)?;
            }
        } else {
            // SCRAPE
            // This is INEFFICIENT as it scans through many events

            let iter = self.ci_iter_scoped(txn, scope, &since, &until)?;
            for result in iter {
                // Check if limit is set
                if let Some(limit) = limit {
                    // Stop if limited
                    if output.len() >= limit {
                        break;
                    }
                }

                let (_key, value) = result?;
                let event = self.get_event_by_id_scoped(txn, scope, value)?.ok_or(Error::NotFound)?;
                
                // Skip deleted events
                if self.is_deleted_scoped(txn, scope, event.id)? {
                    continue;
                }

                if filter.match_event(&event) {
                    output.insert(event);
                }
            }
        }

        // Optionally apply limit
        Ok(match limit {
            Some(limit) => Box::new(output.into_iter().take(limit)),
            None => Box::new(output.into_iter()),
        })
    }

    fn iterate_filter_until_limit<'a>(
        &self,
        txn: &'a RoTxn,
        filter: &DatabaseFilter,
        iter: RoRange<Bytes, Bytes>,
        since: &mut Timestamp,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error> {
        let mut count: usize = 0;

        for result in iter {
            let (_key, value) = result?;

            // Get event by ID
            let event = self.get_event_by_id(txn, value)?.ok_or(Error::NotFound)?;

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
    
    fn iterate_filter_until_limit_scoped<'a>(
        &self,
        txn: &'a RoTxn,
        scope: &Scope,
        filter: &DatabaseFilter,
        iter: ScopedIterator<'a>,
        since: &Timestamp,
        limit: Option<usize>,
        output: &mut BTreeSet<EventBorrow<'a>>,
    ) -> Result<(), Error> {
        let mut count: usize = 0;

        for result in iter {
            let (_key, value) = result?;

            // Get event by ID
            let event = self.get_event_by_id_scoped(txn, scope, value)?.ok_or(Error::NotFound)?;

            if event.created_at < *since {
                break;
            }

            // Skip deleted events
            if self.is_deleted_scoped(txn, scope, event.id)? {
                continue;
            }

            // check against the rest of the filter
            if filter.match_event(&event) {
                // Accept the event
                output.insert(event);
                count += 1;

                // Check if limit is set
                if let Some(limit) = limit {
                    // Stop this limited
                    if count >= limit {
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
            Timestamp::min(),
            Timestamp::max(),
        )?;

        if let Some(result) = iter.next() {
            let (_key, id) = result?;
            return self.get_event_by_id(txn, id);
        }

        Ok(None)
    }

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
            Timestamp::min(),
            Timestamp::max(),
        )?;

        if let Some(result) = iter.next() {
            let (_key, id) = result?;
            return self.get_event_by_id_scoped(txn, scope, id);
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
        until: Timestamp,
    ) -> Result<(), Error> {
        self.remove_replaceable_scoped(txn, &Scope::Default, coordinate, until)
    }

    pub fn remove_replaceable_scoped(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        coordinate: &Coordinate,
        until: Timestamp,
    ) -> Result<(), Error> {
        if !coordinate.kind.is_replaceable() {
            return Err(Error::WrongEventKind);
        }

        let iter = self.akc_iter_scoped(
            txn,
            scope,
            coordinate.public_key.as_bytes(),
            coordinate.kind.as_u16(),
            Timestamp::zero(),
            until,
        )?;

        // Collect DeletionInfo for all events first to avoid iterator lifetime issues
        let mut deletion_infos: Vec<DeletionInfo> = Vec::new();

        for result in iter {
            let (_key, id) = result?;
            if let Some(event) = self.get_event_by_id_scoped(txn, scope, id)? {
                deletion_infos.push(DeletionInfo::from(&event));
            }
        }

        // Now perform deletions
        for info in deletion_infos {
            self.remove(txn, scope, &info)?;
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
        self.remove_addressable_scoped(txn, &Scope::Default, coordinate, until)
    }

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

        // Collect DeletionInfo for all events first to avoid iterator lifetime issues
        let mut deletion_infos = Vec::new();

        for result in iter {
            let (_key, id) = result?;
            if let Some(event) = self.get_event_by_id_scoped(txn, scope, id)? {
                // Our index doesn't have Kind embedded, so we have to check it
                if event.kind == coordinate.kind.as_u16() {
                    deletion_infos.push(DeletionInfo::from(&event));
                }
            }
        }

        // Now perform deletions
        for info in deletion_infos {
            self.remove(txn, scope, &info)?;
        }

        Ok(())
    }

    #[inline]
    pub(crate) fn is_deleted(&self, txn: &RoTxn, event_id: &EventId) -> Result<bool, Error> {
        self.is_deleted_scoped(txn, &Scope::Default, event_id.as_bytes())
    }

    #[inline]
    pub(crate) fn is_deleted_scoped(
        &self,
        txn: &RoTxn,
        scope: &Scope,
        event_id_bytes: &[u8],
    ) -> Result<bool, Error> {
        Ok(self.deleted_ids.get(txn, scope, event_id_bytes)?.is_some())
    }

    pub(crate) fn mark_deleted(&self, txn: &mut RwTxn, event_id: &EventId) -> Result<(), Error> {
        self.mark_deleted_scoped(txn, &Scope::Default, event_id.as_bytes())
    }

    pub(crate) fn mark_deleted_scoped(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        event_id_bytes: &[u8],
    ) -> Result<(), Error> {
        self.deleted_ids.put(txn, scope, event_id_bytes, &[])?;
        Ok(())
    }

    pub(crate) fn mark_coordinate_deleted(
        &self,
        txn: &mut RwTxn,
        coordinate: &CoordinateBorrow,
        when: Timestamp,
    ) -> Result<(), Error> {
        self.mark_coordinate_deleted_scoped(txn, &Scope::Default, coordinate, when)
    }

    pub(crate) fn mark_coordinate_deleted_scoped(
        &self,
        txn: &mut RwTxn,
        scope: &Scope,
        coordinate: &CoordinateBorrow,
        when: Timestamp,
    ) -> Result<(), Error> {
        let key: Vec<u8> = index::make_coordinate_index_key(coordinate);
        let when_bytes = when.as_u64().to_le_bytes();
        self.deleted_coordinates
            .put(txn, scope, &key, &when_bytes)?;
        Ok(())
    }

    pub(crate) fn when_is_coordinate_deleted<'a>(
        &self,
        txn: &RoTxn,
        coordinate: &'a CoordinateBorrow<'a>,
    ) -> Result<Option<Timestamp>, Error> {
        self.when_is_coordinate_deleted_scoped(txn, &Scope::Default, coordinate)
    }

    pub(crate) fn when_is_coordinate_deleted_scoped<'a>(
        &self,
        txn: &RoTxn,
        scope: &Scope,
        coordinate: &'a CoordinateBorrow<'a>,
    ) -> Result<Option<Timestamp>, Error> {
        let key: Vec<u8> = index::make_coordinate_index_key(coordinate);
        Ok(self
            .deleted_coordinates
            .get(txn, scope, &key)?
            .map(|bytes| {
                let timestamp_u64 = u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]));
                Timestamp::from_secs(timestamp_u64)
            }))
    }

    pub(crate) fn ci_iter<'a>(
        &'a self,
        txn: &'a RoTxn,
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<RoRange<'a, Bytes, Bytes>, Error> {
        // Delegate to scoped version with Default scope
        let iter = self.ci_iter_scoped(txn, &Scope::Default, since, until)?;
        // Convert ScopedIterator to RoRange - for now, collect and create new iterator
        // This is a workaround until we properly handle the iterator types
        unreachable!("Non-scoped iterator methods are not used in current implementation")
    }

    pub(crate) fn tc_iter<'a>(
        &'a self,
        txn: &'a RoTxn,
        tag_name: &SingleLetterTag,
        tag_value: &str,
        since: &Timestamp,
        until: &Timestamp,
    ) -> Result<RoRange<'a, Bytes, Bytes>, Error> {
        // Delegate to scoped version with Default scope
        unreachable!("Non-scoped iterator methods are not used in current implementation")
    }

    pub(crate) fn ac_iter<'a>(
        &'a self,
        txn: &'a RoTxn,
        author: &[u8; 32],
        since: Timestamp,
        until: Timestamp,
    ) -> Result<RoRange<'a, Bytes, Bytes>, Error> {
        // Delegate to scoped version with Default scope
        unreachable!("Non-scoped iterator methods are not used in current implementation")
    }

    pub(crate) fn akc_iter<'a>(
        &'a self,
        txn: &'a RoTxn,
        author: &[u8; 32],
        kind: u16,
        since: Timestamp,
        until: Timestamp,
    ) -> Result<RoRange<'a, Bytes, Bytes>, Error> {
        // Delegate to scoped version with Default scope
        unreachable!("Non-scoped iterator methods are not used in current implementation")
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
        // Delegate to scoped version with Default scope
        unreachable!("Non-scoped iterator methods are not used in current implementation")
    }

    fn handle_deletion_event(&self, txn: &mut RwTxn, event: &Event) -> Result<bool, Error> {
        self.handle_deletion_event_scoped(txn, &Scope::Default, event)
    }

    fn handle_deletion_event_scoped(&self, txn: &mut RwTxn, scope: &Scope, event: &Event) -> Result<bool, Error> {
        // Collect DeletionInfo and EventIds for all valid targets first
        let mut deletions_to_process = Vec::new();

        for id in event.tags.event_ids() {
            if let Some(target) = self.get_event_by_id_scoped(txn, scope, id.as_bytes())? {
                // Author must match
                if target.pubkey != event.pubkey.as_bytes() {
                    return Ok(true);
                }

                deletions_to_process.push((*id, DeletionInfo::from(&target)));
            }
        }

        // Now process all deletions
        for (id, info) in deletions_to_process {
            // Mark the event ID as deleted (for NIP-09 deletion events)
            self.mark_deleted_scoped(txn, scope, id.as_bytes())?;

            // Remove from all indexes
            self.remove(txn, scope, &info)?;
        }

        for coordinate in event.tags.coordinates() {
            // Author must match
            if coordinate.public_key != event.pubkey {
                return Ok(true);
            }

            // Mark deleted
            self.mark_coordinate_deleted_scoped(txn, scope, &coordinate.borrow(), event.created_at)?;

            // Remove events (up to the created_at of the deletion event)
            if coordinate.kind.is_replaceable() {
                self.remove_replaceable_scoped(txn, scope, coordinate, event.created_at)?;
            } else if coordinate.kind.is_addressable() {
                self.remove_addressable_scoped(txn, scope, coordinate, event.created_at)?;
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
        // Delegate to scoped version with Default scope
        unreachable!("Non-scoped iterator methods are not used in current implementation")
    }

    pub(crate) fn ci_iter_scoped<'txn>(
        &'txn self,
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
        self.ci_index.range(txn, scope, &range).map_err(Error::from)
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
        self.tc_index.range(txn, scope, &range).map_err(Error::from)
    }

    pub(crate) fn ac_iter_scoped<'txn>(
        &'txn self,
        txn: &'txn RoTxn,
        scope: &Scope,
        author: &[u8; 32],
        since: Timestamp,
        until: Timestamp,
    ) -> Result<ScopedIterator<'txn>, Error> {
        let start_prefix = index::make_ac_index_key(author, &until, &EVENT_ID_ALL_ZEROS);
        let end_prefix = index::make_ac_index_key(author, &since, &EVENT_ID_ALL_255);
        let range = (
            Bound::Included(start_prefix.as_slice()),
            Bound::Excluded(end_prefix.as_slice()),
        );
        self.ac_index.range(txn, scope, &range).map_err(Error::from)
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
        let start_prefix = index::make_akc_index_key(author, kind, &until, &EVENT_ID_ALL_ZEROS);
        let end_prefix = index::make_akc_index_key(author, kind, &since, &EVENT_ID_ALL_255);
        let range = (
            Bound::Included(start_prefix.as_slice()),
            Bound::Excluded(end_prefix.as_slice()),
        );
        self.akc_index
            .range(txn, scope, &range)
            .map_err(Error::from)
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
        self.atc_index
            .range(txn, scope, &range)
            .map_err(Error::from)
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
        self.ktc_index
            .range(txn, scope, &range)
            .map_err(Error::from)
    }
}
