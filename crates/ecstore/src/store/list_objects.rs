// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// Narrow a store error into the filemeta vocabulary used inside the per-set
/// walk closures. Faithful port of the retired blanket
/// `From<StorageError> for rustfs_filemeta::Error`: unmatched variants fold
/// into the io-backed `other()`, whose boxed identity the io bridge recovers
/// on the way back (see conversion_roundtrip_tests).
fn to_filemeta_err(err: Error) -> rustfs_filemeta::Error {
    err.narrow_to_filemeta().unwrap_or_else(rustfs_filemeta::Error::other)
}

use crate::bucket::lifecycle::bucket_lifecycle_ops::free_version_remote_tuple_matches;
use crate::bucket::metadata_sys::{get_versioning_config, has_authoritative_never_versioned_state};
use crate::bucket::utils::{check_list_objs_args, has_bad_path_component};
use crate::bucket::versioning::VersioningApi;
use crate::cache_value::metacache_set::{
    FallbackClaimTracker, ListPathRawOptions, ListPathRawOutcome, list_path_raw, list_path_raw_with_claim_tracker,
    list_path_raw_with_outcome,
};
use crate::disk::error::DiskError;
use crate::disk::{DiskInfo, DiskStore, RUSTFS_META_BUCKET};
use crate::error::{
    Error, Result, StorageError, is_all_disk_not_found, is_all_not_found, is_all_volume_not_found, to_object_err,
};
use crate::object_api::{ObjectInfo, ObjectOptions};
use crate::set_disk::SetDisks;
use crate::storage_api_contracts::list::{
    ListObjectsInfo as StorageListObjectsInfo, StorageListObjectVersionsInfo, StorageListObjectsV2Info, StorageObjectInfoOrErr,
    StorageWalkOptions, VersionMarker, WalkVersionsSortOrder,
};
use crate::store::ECStore;
use crate::store::utils::is_reserved_or_invalid_bucket;
use futures::future::join_all;
use rand::seq::SliceRandom;
use rustfs_filemeta::{
    FileMeta, FileMetaShallowVersion, MetaCacheEntries, MetaCacheEntriesSorted, MetaCacheEntry, MetadataResolutionParams,
};
use rustfs_io_metrics::{
    LIST_OBJECTS_GATHER_OUTCOME_INPUT_CLOSED, LIST_OBJECTS_GATHER_OUTCOME_LIMIT_REACHED, LIST_OBJECTS_SOURCE_WALKER,
    ListObjectsGatherCaller, ListObjectsGatherObservation,
};
use rustfs_utils::path::{self, SLASH_SEPARATOR, base_dir_from_prefix};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::future::Future;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::{Instrument, debug, error, info, warn};
use uuid::Uuid;

const MAX_OBJECT_LIST: i32 = 1000;
// const MAX_DELETE_LIST: i32 = 1000;
// const MAX_UPLOADS_LIST: i32 = 10000;
// const MAX_PARTS_LIST: i32 = 10000;

const METACACHE_SHARE_PREFIX: bool = false;

type ListObjectsInfo = StorageListObjectsInfo<ObjectInfo>;
type ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;
type ListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>;
type ObjectInfoOrErr = StorageObjectInfoOrErr<ObjectInfo, Error>;
type WalkOptions = StorageWalkOptions<fn(&rustfs_filemeta::FileInfo) -> bool>;

/// Build the versioned listing options used by recursive object walks.
///
/// `list_path_result` normally derives these scan bounds before dispatching to
/// a disk. Set walks call the version scan directly, so they must establish the
/// same base directory and child-prefix filter at this boundary.
fn recursive_walk_list_path_options(bucket: String, prefix: &str, opts: &WalkOptions) -> ListPathOptions {
    let mut list_options = ListPathOptions {
        bucket,
        prefix: prefix.to_owned(),
        marker: opts.marker.clone(),
        limit: i32::try_from(opts.limit).unwrap_or(i32::MAX),
        ask_disks: opts.ask_disks.clone(),
        incl_deleted: true,
        recursive: true,
        versioned: true,
        caller: ListObjectsGatherCaller::S3ListVersions,
        walkdir_timeout: opts.walkdir_timeout,
        walkdir_stall_timeout: opts.walkdir_stall_timeout,
        ..Default::default()
    };
    list_options.base_dir = base_dir_from_prefix(prefix);
    list_options.set_filter();
    list_options
}

struct ListObjectVersionsInput<'a> {
    bucket: &'a str,
    prefix: &'a str,
    marker: Option<String>,
    version_marker: Option<String>,
    delimiter: Option<String>,
    max_keys: i32,
    include_version_purge: bool,
}

const EVENT_LISTING_MERGE_REJECTED: &str = "listing_merge_rejected";
const EVENT_SET_LIST_PATH: &str = "set_list_path";
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_LISTING: &str = "listing";

const LIST_MERGED_INPUT_BUFFER: usize = 1;

fn list_merged_entry_channel() -> (Sender<MetaCacheEntry>, Receiver<MetaCacheEntry>) {
    mpsc::channel(LIST_MERGED_INPUT_BUFFER)
}

fn normalize_max_keys(max_keys: i32) -> i32 {
    max_keys.min(MAX_OBJECT_LIST)
}

fn ensure_non_empty_listing_disks(bucket: &str, path: &str, disks: &[DiskStore]) -> Result<()> {
    if disks.is_empty() {
        warn!(
            bucket = %bucket,
            path = %path,
            "listing candidate disks collapsed to empty set"
        );
        return Err(StorageError::ErasureReadQuorum);
    }

    Ok(())
}

fn walk_result_from_set_errors(errs: &[Option<Error>]) -> Result<()> {
    if is_all_not_found(errs) {
        if is_all_volume_not_found(errs) {
            return Err(StorageError::VolumeNotFound);
        }

        return Ok(());
    }

    // Every set is unreachable (all drives offline / drive-id mismatch, with no
    // healthy set contributing entries): this is an availability failure, not an
    // empty listing. Surface it instead of silently swallowing DiskNotFound in
    // the not-found loop below.
    if is_all_disk_not_found(errs) {
        return Err(StorageError::DiskNotFound);
    }

    for err in errs.iter().flatten() {
        if err.is_not_found() {
            continue;
        }

        return Err(err.clone());
    }

    Ok(())
}

pub fn max_keys_plus_one(max_keys: i32, add_one: bool) -> i32 {
    let mut max_keys = max_keys;
    if !(0..=MAX_OBJECT_LIST).contains(&max_keys) {
        max_keys = MAX_OBJECT_LIST;
    }
    if add_one {
        max_keys += 1;
    }
    max_keys
}

fn list_versions_scan_limit(max_keys: i32, has_version_marker: bool) -> i32 {
    if max_keys <= 0 {
        return 0;
    }

    // The marker object's versions may all be filtered out after gathering.
    // Reserve its raw entry in addition to the next-page lookahead entry.
    max_keys_plus_one(max_keys, true) + i32::from(has_version_marker)
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum GatherResultsState {
    LimitReached,
    InputClosed,
    /// The results receiver was dropped before the single page was delivered.
    ///
    /// With a capacity-1 results channel and at most one send per
    /// `gather_results` call, a send failure is equivalent to the consumer
    /// (the `list_path` wrapper future) having been dropped — e.g. the client
    /// disconnected mid-listing. This is a benign completion, not an error.
    ConsumerGone,
}

/// One page of merged listing entries.
#[derive(Debug, Default)]
pub struct ListPage {
    pub entries: MetaCacheEntriesSorted,
    /// Every producer reached the end of the requested namespace while this
    /// page was collected, so no continuation page can exist. A page cut at
    /// the collector's limit, or fed by a producer that stopped at its own
    /// per-disk limit, is never exhausted even when it is empty.
    pub exhausted: bool,
}

/// Which page worker finished, with what it knows about the stream end.
enum ListWorkerExit {
    Producer(ListPathRawOutcome),
    Gather(GatherResultsState),
}

#[derive(Clone)]
struct ListPathLogContext {
    bucket: String,
    base_dir: String,
    prefix: String,
    marker: String,
    limit: i32,
}

impl ListPathLogContext {
    fn from_options(opts: &ListPathOptions) -> Self {
        Self {
            bucket: opts.bucket.clone(),
            base_dir: opts.base_dir.clone(),
            prefix: opts.prefix.clone(),
            marker: opts.marker.clone().unwrap_or_default(),
            limit: opts.limit,
        }
    }
}

fn log_list_path_worker_error<E>(component: &'static str, stage: &'static str, context: &ListPathLogContext, err: &E)
where
    E: std::fmt::Display + ?Sized,
{
    error!(
        component,
        stage,
        bucket = %context.bucket,
        prefix = %context.prefix,
        base_dir = %context.base_dir,
        limit = context.limit,
        marker = %context.marker,
        error = %err,
        "list_path worker failed"
    );
}

fn log_list_path_finished(
    component: &'static str,
    context: &ListPathLogContext,
    elapsed_ms: f64,
    candidate_count: usize,
    has_error: bool,
) {
    debug!(
        component,
        bucket = %context.bucket,
        prefix = %context.prefix,
        base_dir = %context.base_dir,
        limit = context.limit,
        marker = %context.marker,
        candidate_count,
        has_error,
        elapsed_ms,
        "list_path finished"
    );
}

#[derive(Debug, Default, Clone)]
pub struct ListPathOptions {
    pub caller: ListObjectsGatherCaller,
    pub id: Option<String>,

    // Bucket of the listing.
    pub bucket: String,

    // Directory inside the bucket.
    // When unset listPath will set this based on Prefix
    pub base_dir: String,

    // Scan/return only content with prefix.
    pub prefix: String,

    // FilterPrefix will return only results with this prefix when scanning.
    // Should never contain a slash.
    // Prefix should still be set.
    pub filter_prefix: Option<String>,

    // Marker to resume listing.
    pub marker: Option<String>,

    // Include marker itself in the returned entries. Version listings need this
    // when a version marker selects a later version from the marker object.
    pub include_marker: bool,

    // Limit the number of results.
    pub limit: i32,

    // The number of disks to ask.
    pub ask_disks: String,

    // InclDeleted will keep all entries where latest version is a delete marker.
    pub incl_deleted: bool,

    // Authoritative bucket metadata proves that delete markers cannot exist.
    pub skip_hidden_prefix_check: bool,

    // Scan recursively.
    // If false only main directory will be scanned.
    // Should always be true if Separator is n SlashSeparator.
    pub recursive: bool,

    // Separator to use.
    pub separator: Option<String>,

    // Create indicates that the lister should not attempt to load an existing cache.
    pub create: bool,

    // Include pure directories.
    pub include_directories: bool,

    // Transient is set if the cache is transient due to an error or being a reserved bucket.
    // This means the cache metadata will not be persisted on disk.
    // A transient result will never be returned from the cache so knowing the list id is required.
    pub transient: bool,

    // Versioned is this a ListObjectVersions call.
    pub versioned: bool,

    pub stop_disk_at_limit: bool,

    pub pool_idx: Option<usize>,
    pub set_idx: Option<usize>,
    pub cursor_source: Option<ListSourceMode>,
    pub cursor_generation: Option<String>,
    pub walkdir_timeout: Option<Duration>,
    pub walkdir_stall_timeout: Option<Duration>,
}

fn list_path_folds_common_prefixes(options: &ListPathOptions) -> bool {
    !options.versioned
        && (options.include_directories
            || options
                .separator
                .as_deref()
                .is_some_and(|separator| !separator.is_empty() && separator != SLASH_SEPARATOR))
}

async fn can_skip_hidden_prefix_check(options: &ListPathOptions) -> bool {
    if options.recursive || options.incl_deleted || options.versioned {
        return false;
    }

    has_authoritative_never_versioned_state(&options.bucket)
        .await
        .unwrap_or(false)
}

const MARKER_TAG_VERSION: &str = "v2";
const LEGACY_MARKER_TAG_VERSIONS: &[&str] = &["v1", MARKER_TAG_VERSION];
const LIST_CACHE_MARKER_PREFIX: &str = "[rustfs_cache:";
const LIST_CURSOR_SOURCE_WALKER: &str = "walker";
const LIST_CURSOR_SOURCE_INDEX_KEY_ONLY: &str = "index_key_only";
const LIST_CURSOR_SOURCE_INDEX_VERIFIED_PAGE: &str = "index_verified_page";
const LIST_CURSOR_SOURCE_INDEX_METADATA_FAST: &str = "index_metadata_fast";
const DEFAULT_API_LIST_QUORUM: &str = "strict";
const ENV_API_LIST_OBJECTS_QUORUM: &str = "RUSTFS_LIST_OBJECTS_QUORUM";
const DEFAULT_API_LIST_OBJECTS_QUORUM: &str = "strict";

/// Identifies the source that produced a ListObjects page.
///
/// `Walker` is the current live xl.meta-backed path. Index modes are future
/// integration points only: key-only and verified-page modes still require live
/// metadata validation before they can satisfy a strong listing, while
/// metadata-fast is explicitly eventually consistent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListSourceMode {
    Walker,
    IndexKeyOnly,
    IndexVerifiedPage,
    IndexMetadataFast,
}

impl ListSourceMode {
    fn from_cursor_value(source: &str) -> Option<Self> {
        match source {
            LIST_CURSOR_SOURCE_WALKER => Some(Self::Walker),
            LIST_CURSOR_SOURCE_INDEX_KEY_ONLY => Some(Self::IndexKeyOnly),
            LIST_CURSOR_SOURCE_INDEX_VERIFIED_PAGE => Some(Self::IndexVerifiedPage),
            LIST_CURSOR_SOURCE_INDEX_METADATA_FAST => Some(Self::IndexMetadataFast),
            _ => None,
        }
    }
}

static SCANNER_NAMESPACE_MUTATION_GENERATION: AtomicU64 = AtomicU64::new(0);

pub(super) fn scanner_namespace_mutation_generation() -> u64 {
    SCANNER_NAMESPACE_MUTATION_GENERATION.load(Ordering::Acquire)
}

pub(crate) fn observe_scanner_namespace_mutations(bucket: &str, delta: u64) {
    if bucket == RUSTFS_META_BUCKET {
        return;
    }

    let _ = SCANNER_NAMESPACE_MUTATION_GENERATION
        .try_update(Ordering::AcqRel, Ordering::Acquire, |current| Some(current.saturating_add(delta)));
}

#[cfg(test)]
fn reset_scanner_namespace_mutation_generation_for_test() {
    SCANNER_NAMESPACE_MUTATION_GENERATION.store(0, Ordering::Release);
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ListContinuationV2 {
    version: &'static str,
    id: Option<String>,
    pool_idx: Option<usize>,
    set_idx: Option<usize>,
    source: Option<ListSourceMode>,
    generation: Option<String>,
}

impl ListContinuationV2 {
    fn parse(marker_tag: &str) -> Option<(Self, bool)> {
        let mut parsed = Self {
            version: MARKER_TAG_VERSION,
            id: None,
            pool_idx: None,
            set_idx: None,
            source: None,
            generation: None,
        };
        let mut has_list_cache = false;
        let mut should_create = false;

        for tag in marker_tag.split(',') {
            let Some((key, value)) = tag.split_once(':') else {
                continue;
            };

            match key {
                "rustfs_cache" => {
                    match value {
                        "v1" => parsed.version = "v1",
                        MARKER_TAG_VERSION => parsed.version = MARKER_TAG_VERSION,
                        _ => return None,
                    }
                    has_list_cache = true;
                }
                "id" => {
                    parsed.id = Some(value.to_owned());
                }
                "return" => {
                    parsed.id = Some(Uuid::new_v4().to_string());
                    should_create = true;
                }
                "p" => match value.parse::<usize>() {
                    Ok(res) => parsed.pool_idx = Some(res),
                    Err(_) => {
                        parsed.id = Some(Uuid::new_v4().to_string());
                        should_create = true;
                    }
                },
                "s" => match value.parse::<usize>() {
                    Ok(res) => parsed.set_idx = Some(res),
                    Err(_) => {
                        parsed.id = Some(Uuid::new_v4().to_string());
                        should_create = true;
                    }
                },
                "src" => {
                    parsed.source = Some(ListSourceMode::from_cursor_value(value)?);
                }
                "gen" => {
                    if value.is_empty() {
                        return None;
                    }
                    parsed.generation = Some(value.to_owned());
                }
                _ => (),
            }
        }

        if has_list_cache && LEGACY_MARKER_TAG_VERSIONS.contains(&parsed.version) {
            Some((parsed, should_create))
        } else {
            None
        }
    }
}

fn normalize_list_quorum(value: &str) -> &'static str {
    let value = value.trim();
    if value.eq_ignore_ascii_case("disk") {
        "disk"
    } else if value.eq_ignore_ascii_case("reduced") {
        "reduced"
    } else if value.eq_ignore_ascii_case("optimal") {
        "optimal"
    } else if value.eq_ignore_ascii_case("auto") {
        "auto"
    } else {
        DEFAULT_API_LIST_QUORUM
    }
}

fn list_objects_quorum_from_env() -> String {
    let value = rustfs_utils::get_env_str(ENV_API_LIST_OBJECTS_QUORUM, DEFAULT_API_LIST_OBJECTS_QUORUM);
    normalize_list_quorum(&value).to_owned()
}

/// Name of the last RAW entry the walker returned for this page (after
/// `forward_past`), captured before delimiter folding collapses many keys into a
/// few common prefixes. Used as a continuation marker so a fully re-folded page
/// can still advance past its scan window (ECA-03 / #944).
fn last_scanned_entry_name(entries: &MetaCacheEntriesSorted) -> Option<String> {
    entries.entries().last().map(|entry| entry.name.clone())
}

fn filter_versions_common_prefixes_after_marker(objects: &mut Vec<ObjectInfo>, delimiter: Option<&str>, marker: Option<&str>) {
    if delimiter.is_some()
        && let Some(marker) = marker
    {
        // S3 excludes CommonPrefixes that are not lexicographically greater than KeyMarker.
        // Filter before applying max-keys so a resumed prefix cannot consume the page budget.
        objects.retain(|object| !(object.is_dir && object.mod_time.is_none()) || object.name.as_str() > marker);
    }
}

fn last_raw_key_for_prefix<'a>(raw_keys: impl Iterator<Item = &'a str>, prefix: &str) -> Option<&'a str> {
    raw_keys.filter(|key| key.starts_with(prefix)).last()
}

fn versioned_prefix_page_boundary_key<'a>(
    objects: &[ObjectInfo],
    raw_keys: impl Iterator<Item = &'a str>,
    delimiter: Option<&str>,
    max_keys: i32,
    disk_has_more: bool,
) -> Option<String> {
    if delimiter.is_none() || !disk_has_more || max_keys <= 0 || objects.len() < max_keys as usize {
        return None;
    }

    let last_visible = objects.get(max_keys as usize - 1)?;
    if !last_visible.is_dir || last_visible.mod_time.is_some() {
        return None;
    }

    // A folded prefix cannot advance the raw versioned walker. Continue at the last
    // raw key represented by the final returned prefix, not at the next prefix's key.
    last_raw_key_for_prefix(raw_keys, &last_visible.name).map(str::to_owned)
}

/// Ordinary cursors are plain keys. Cache-shaped literal keys retain an
/// old-readable escape until legacy tag parsing can be removed.
// RUSTFS_COMPAT_TODO(listing-cursor-legacy-tag): Remove after the minimum upgrade source stops issuing tagged cursors.
fn escape_list_marker(marker: String) -> String {
    // One old-readable outer tag protects a literal cache-shaped key.
    if marker
        .rfind(LIST_CACHE_MARKER_PREFIX)
        .is_some_and(|start| marker[start..].contains(']'))
    {
        format!("{marker}[rustfs_cache:v2,return:]")
    } else {
        marker
    }
}

fn build_list_next_marker(objects: &[ObjectInfo], prefixes: &[String]) -> Option<String> {
    objects
        .last()
        .map(|last| last.name.clone())
        .or_else(|| prefixes.last().cloned())
        .map(escape_list_marker)
}

fn build_list_versions_next_marker(objects: &[ObjectInfo], prefixes: &[String]) -> (Option<String>, Option<String>) {
    if let Some(last) = objects.last() {
        // A null version carries the synthesized `Some(Uuid::nil())` identity
        // here; advertise it as the literal `null` marker so a resumed listing
        // parses it back to `VersionMarker::Null` instead of a nil UUID that
        // `find_version_index` can never match (issue #6745).
        let version_marker = if last.is_dir && last.mod_time.is_none() {
            // A CommonPrefix has no version to resume; a version marker would
            // make the next page include this same prefix again.
            None
        } else {
            Some(
                last.version_id
                    .filter(|v| !v.is_nil())
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "null".to_string()),
            )
        };
        (Some(last.name.clone()), version_marker)
    } else if let Some(last_prefix) = prefixes.last() {
        (Some(last_prefix.clone()), None)
    } else {
        (None, None)
    }
}

fn list_objects_paginate(
    get_objects: Vec<ObjectInfo>,
    delimiter: &Option<String>,
    max_keys: i32,
    disk_has_more: bool,
    include_version_id: bool,
    last_scanned_key: Option<&str>,
) -> (Vec<ObjectInfo>, Vec<String>, bool, Option<String>, Option<String>) {
    let mut get_objects = get_objects;
    let mut is_truncated = false;
    let mut next_marker = None;
    let mut next_version_idmarker = None;

    if max_keys <= 0 {
        get_objects.clear();
    } else if get_objects.len() > max_keys as usize {
        is_truncated = true;
        get_objects.truncate(max_keys as usize);
    }

    if is_truncated {
        if include_version_id {
            (next_marker, next_version_idmarker) = build_list_versions_next_marker(&get_objects, &[]);
        } else {
            next_marker = build_list_next_marker(&get_objects, &[]);
        }
    }

    let mut prefixes: Vec<String> = Vec::new();
    let mut prefix_set: HashSet<String> = HashSet::new();
    let mut objects = Vec::with_capacity(get_objects.len());
    for obj in get_objects {
        if delimiter.is_some() {
            if obj.is_dir && obj.mod_time.is_none() {
                if prefix_set.insert(obj.name.clone()) {
                    prefixes.push(obj.name);
                }
            } else {
                objects.push(obj);
            }
        } else {
            objects.push(obj);
        }
    }

    if !is_truncated && disk_has_more {
        let visible_count = objects.len() + prefixes.len();
        let should_truncate = if delimiter.is_none() {
            visible_count > 0
        } else {
            visible_count >= max_keys as usize
        };
        if delimiter.is_some()
            && include_version_id
            && let Some(last_scanned) = last_scanned_key
        {
            is_truncated = true;
            next_marker = Some(last_scanned.to_owned());
        } else if should_truncate {
            is_truncated = true;
            if include_version_id {
                (next_marker, next_version_idmarker) = build_list_versions_next_marker(&objects, &prefixes);
            } else {
                next_marker = build_list_next_marker(&objects, &prefixes);
            }
        } else if delimiter.is_some()
            && let Some(last_scanned) = last_scanned_key
        {
            // Delimiter re-fold guard (backlog ECA-03 / #944): the walker hit its
            // raw candidate limit (`disk_has_more`), but the scanned keys collapsed
            // into fewer than `max_keys` common prefixes, so `should_truncate` above
            // stays false. Without this branch we would return `is_truncated=false`
            // with no marker and silently drop every key past the scan window, a
            // data-loss bug for backup/sync/mirror/reconcile consumers.
            //
            // Continue from the last RAW scanned key, not from a folded prefix.
            // `forward_past` advances on `name > marker`, and a folded prefix such as
            // "data-" is lexicographically smaller than its own keys ("data-0001"),
            // so using it as the marker would re-list and re-fold the same keys
            // forever. A real scanned key strictly advances past the already-scanned
            // window and guarantees finite pagination.
            is_truncated = true;
            next_marker = Some(escape_list_marker(last_scanned.to_owned()));
            // A bare key marker with no version id makes `forward_past` skip the whole
            // scanned window on the next page; its folded prefixes are already emitted.
        }
    }

    (objects, prefixes, is_truncated, next_marker, next_version_idmarker)
}

struct VersionedListPagination<'a, I> {
    delimiter: &'a Option<String>,
    max_keys: i32,
    disk_has_more: bool,
    marker: Option<&'a str>,
    last_scanned_key: Option<&'a str>,
    raw_keys: I,
}

fn list_objects_paginate_versions<'a, I>(
    mut get_objects: Vec<ObjectInfo>,
    pagination: VersionedListPagination<'a, I>,
) -> (Vec<ObjectInfo>, Vec<String>, bool, Option<String>, Option<String>)
where
    I: Iterator<Item = &'a str>,
{
    filter_versions_common_prefixes_after_marker(&mut get_objects, pagination.delimiter.as_deref(), pagination.marker);
    let prefix_at_page_boundary = versioned_prefix_page_boundary_key(
        &get_objects,
        pagination.raw_keys,
        pagination.delimiter.as_deref(),
        pagination.max_keys,
        pagination.disk_has_more,
    );

    let (objects, prefixes, is_truncated, mut next_marker, mut next_version_idmarker) = list_objects_paginate(
        get_objects,
        pagination.delimiter,
        pagination.max_keys,
        pagination.disk_has_more,
        true,
        pagination.last_scanned_key,
    );
    if is_truncated && let Some(raw_key) = prefix_at_page_boundary {
        next_marker = Some(raw_key);
        next_version_idmarker = None;
    }

    (objects, prefixes, is_truncated, next_marker, next_version_idmarker)
}

fn list_metadata_resolution_params(bucket: String, listing_quorum: usize, versioned: bool) -> MetadataResolutionParams {
    MetadataResolutionParams {
        dir_quorum: listing_quorum,
        obj_quorum: listing_quorum,
        requested_versions: if versioned { 0 } else { 1 },
        bucket,
        ..Default::default()
    }
}

fn positive_ask_disks(ask_disks: i32) -> Option<usize> {
    usize::try_from(ask_disks).ok().filter(|asked| *asked > 0)
}

fn listing_quorum_from_ask_disks(ask_disks: i32) -> usize {
    positive_ask_disks(ask_disks)
        .map(|asked| asked.div_ceil(2))
        .unwrap_or_default()
}

/// Fewer online drives than a read quorum cannot prove a page complete.
/// MinIO lists from whatever drives answer; RustFS fails the page instead.
fn set_listing_read_floor(drive_count: usize, parity_count: usize) -> usize {
    if parity_count == 0 {
        drive_count
    } else {
        drive_count.div_ceil(2)
    }
}

fn bounded_usize_to_i32(value: usize) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

fn clamp_ask_disks_to_available(ask_disks: i32, available_disks: usize) -> i32 {
    ask_disks.min(bounded_usize_to_i32(available_disks))
}

fn parse_version_marker(marker: String) -> Result<VersionMarker> {
    Ok(VersionMarker::parse(marker)?)
}

fn version_marker_for_entries(
    entries: &MetaCacheEntriesSorted,
    key_marker: Option<&str>,
    version_marker: Option<VersionMarker>,
) -> Option<VersionMarker> {
    let marker = version_marker?;
    let Some(key_marker) = key_marker else {
        return Some(marker);
    };

    entries
        .entries()
        .first()
        .is_some_and(|entry| entry.name.as_str() == key_marker)
        .then_some(marker)
}

impl ListPathOptions {
    pub fn set_filter(&mut self) {
        if METACACHE_SHARE_PREFIX {
            return;
        }
        if self.prefix == self.base_dir {
            return;
        }

        let s = SLASH_SEPARATOR.chars().next().unwrap_or_default();
        self.filter_prefix = {
            let fp = self.prefix.trim_start_matches(&self.base_dir).trim_matches(s);

            if fp.contains(s) || fp.is_empty() {
                None
            } else {
                Some(fp.to_owned())
            }
        }
    }

    /// Strips a trailing legacy cursor tag from the request marker.
    ///
    /// Releases up to 1.0.x appended `[rustfs_cache:v2,id:...,src:walker,gen:live]`
    /// to every internal continuation marker and exposed it through the V2
    /// continuation token and the ListObjectVersions `NextKeyMarker`. Current
    /// servers still accept the tagged form in V2 continuation tokens and
    /// legacy object-list markers. Version-listing key markers are literal keys.
    ///
    /// Newly issued cursors protect cache-shaped literal keys with one legacy
    /// outer tag. Parse only at the listing entry point; parsing the resulting
    /// literal key again would move the walker back to its stem.
    // RUSTFS_COMPAT_TODO(listing-cursor-legacy-tag): accept markers tagged by 1.0.x servers. Remove after every supported
    // direct-upgrade release issues plain key markers, so no client can still hold a tagged cursor.
    pub fn parse_marker(&mut self) {
        let Some(marker) = self.marker.clone() else {
            return;
        };
        let Some(start_idx) = marker.rfind(LIST_CACHE_MARKER_PREFIX) else {
            return;
        };
        let Some(end_offset) = marker[start_idx..].rfind(']') else {
            return;
        };

        let end_idx = start_idx + end_offset;
        let tag_body = &marker[start_idx + 1..end_idx];

        let Some((continuation, should_create)) = ListContinuationV2::parse(tag_body) else {
            return;
        };

        self.marker = Some(marker[..start_idx].to_owned());
        self.id = continuation.id;
        self.pool_idx = continuation.pool_idx;
        self.set_idx = continuation.set_idx;
        self.cursor_source = continuation.source;
        self.cursor_generation = continuation.generation;
        if should_create {
            self.create = true;
        }
    }
}

impl ECStore {
    #[allow(clippy::too_many_arguments)]
    // @continuation_token marker
    // @start_after as marker when continuation_token empty
    // @delimiter default="/", empty when recursive
    // @max_keys limit
    pub async fn inner_list_objects_v2(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
        _fetch_owner: bool,
        start_after: Option<String>,
        incl_deleted: bool,
    ) -> Result<ListObjectsV2Info> {
        let marker = {
            if continuation_token.is_none() {
                // StartAfter is a literal object key, even if it resembles a
                // legacy cursor. Protect it from the compatibility parser.
                start_after.map(escape_list_marker)
            } else {
                continuation_token.clone()
            }
        };

        let loi = self
            .list_objects_generic(bucket, prefix, marker, delimiter, max_keys, incl_deleted)
            .await?;
        Ok(ListObjectsV2Info {
            is_truncated: loi.is_truncated,
            continuation_token,
            next_continuation_token: loi.next_marker,
            objects: loi.objects,
            prefixes: loi.prefixes,
        })
    }

    pub async fn list_objects_generic(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
        incl_deleted: bool,
    ) -> Result<ListObjectsInfo> {
        // Stored keys cannot contain repeated separators. Keep the query literal
        // and avoid walking or cleaning up an aliased filesystem path.
        if prefix.contains("//") && !has_bad_path_component(prefix) && !prefix.contains('\0') {
            check_list_objs_args(bucket, "", &marker)?;
            return Ok(ListObjectsInfo::default());
        }
        let max_keys = normalize_max_keys(max_keys);
        let effective_max_keys = if max_keys <= 0 { 0 } else { max_keys_plus_one(max_keys, true) };
        let mut opts = ListPathOptions {
            bucket: bucket.to_owned(),
            caller: ListObjectsGatherCaller::S3List,
            prefix: prefix.to_owned(),
            separator: delimiter.clone(),
            // Always request max_keys + 1 to detect if there are more results
            limit: effective_max_keys,
            marker,
            incl_deleted,
            ask_disks: list_objects_quorum_from_env(),
            ..Default::default()
        };
        // A marker from a 1.0.x client may still carry the legacy cursor tag;
        // strip it before any name comparison (notably `forward_past`), or every
        // key sorting between `<marker>` and `<marker>[` is silently skipped on
        // the continuation page (backlog#1047).
        opts.parse_marker();
        let mut page = self
            .clone()
            .list_path(&opts)
            .await
            .map_err(|err| to_object_err(err, vec![bucket, prefix]))?;

        let disk_has_more = !page.exhausted;

        page.entries.forward_past(opts.marker.clone());

        // Last RAW scanned key, captured before folding, so `list_objects_paginate`
        // can advance past a fully-collapsed common-prefix page (ECA-03 / #944).
        let last_scanned_key = last_scanned_entry_name(&page.entries);

        let get_objects =
            ObjectInfo::from_meta_cache_entries_sorted_infos(&page.entries, bucket, prefix, delimiter.clone()).await;

        let (objects, prefixes, is_truncated, next_marker, _) =
            list_objects_paginate(get_objects, &delimiter, max_keys, disk_has_more, false, last_scanned_key.as_deref());

        Ok(ListObjectsInfo {
            is_truncated,
            next_marker,
            objects,
            prefixes,
        })
    }

    pub async fn inner_list_object_versions(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        version_marker: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
    ) -> Result<ListObjectVersionsInfo> {
        self.inner_list_object_versions_with_projection(ListObjectVersionsInput {
            bucket,
            prefix,
            marker,
            version_marker,
            delimiter,
            max_keys,
            include_version_purge: false,
        })
        .await
    }

    pub(crate) async fn list_object_versions_for_lifecycle(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        version_marker: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
    ) -> Result<ListObjectVersionsInfo> {
        self.inner_list_object_versions_with_projection(ListObjectVersionsInput {
            bucket,
            prefix,
            marker,
            version_marker,
            delimiter,
            max_keys,
            include_version_purge: true,
        })
        .await
    }

    /// Capture one logical key's version group for write-triggered expiry.
    pub(crate) async fn get_object_versions_for_lifecycle(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
        bucket_incarnation_id: Uuid,
    ) -> Result<Vec<ObjectInfo>> {
        crate::bucket::utils::check_object_args(bucket, object)?;
        let mut opts = opts.clone();
        // Lock order matches writes: bucket lifecycle, metadata validation,
        // then object namespace. Borrow fences supplied by an outer operation.
        let bucket_guard = if opts.bucket_lifecycle_lock_fence.is_none() {
            let guard = self
                .acquire_bucket_incarnation_fence_with_options(bucket, bucket_incarnation_id, Some(&opts))
                .await?;
            guard.attach_to_object_options(&mut opts);
            Some(guard)
        } else {
            let incarnation =
                crate::bucket::metadata_sys::get_bucket_incarnation_id_for_options_in(&self.ctx, bucket, &opts).await?;
            if incarnation != bucket_incarnation_id {
                return Err(Error::BucketNotFound(bucket.to_owned()));
            }
            None
        };
        let metadata_sys = crate::bucket::metadata_sys::bucket_metadata_sys_of(&self.ctx)?;
        let metadata_sys = metadata_sys.read().await.clone();
        let (versioning, _) = metadata_sys.get_versioning_config(bucket).await?;
        let disk_object = path::encode_dir_object(object);
        let object_guard = self
            .acquire_object_read_lock_if_needed("lifecycle_versions", bucket, &disk_object, &mut opts)
            .await?;

        // As with version listings, include every physical set: migration can
        // leave a key's history in more than its current hash destination.
        let reads = self
            .pools
            .iter()
            .flat_map(|pool| &pool.disk_set)
            .map(|set| set.read_object_versions_for_lifecycle(bucket, object));
        let mut entries = Vec::new();
        for result in join_all(reads).await {
            if let Some(entry) = result.map_err(|error| to_object_err(error.into(), vec![bucket, object]))? {
                entries.push(entry);
            }
        }
        if self.pools.is_empty()
            || bucket_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
            || opts
                .bucket_lifecycle_lock_fence
                .as_ref()
                .is_some_and(|fence| fence.is_lock_lost())
            || opts.namespace_lock_fence.as_ref().is_some_and(|fence| fence.is_lock_lost())
            || object_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
        {
            return Err(Error::PreconditionFailed);
        }
        let mut entries = entries.into_iter();
        let Some(mut entry) = entries.next() else {
            return Ok(Vec::new());
        };
        if entries.len() > 0 {
            merge_object_entry_versions(&mut entry, entries)?;
        }
        // This projection excludes tier free-version bookkeeping but retains
        // pending version purges for the evaluator's replication safeguards.
        let versions = entry.file_info_versions(bucket)?;
        let versioned = versioning.versioned(object);
        Ok(versions
            .versions
            .iter()
            .map(|version| ObjectInfo::from_file_info(version, bucket, object, versioned))
            .collect())
    }

    async fn inner_list_object_versions_with_projection(
        self: Arc<Self>,
        input: ListObjectVersionsInput<'_>,
    ) -> Result<ListObjectVersionsInfo> {
        let ListObjectVersionsInput {
            bucket,
            prefix,
            marker,
            version_marker,
            delimiter,
            max_keys,
            include_version_purge,
        } = input;
        let max_keys = normalize_max_keys(max_keys);
        if marker.is_none() && version_marker.is_some() {
            return Err(StorageError::NotImplemented);
        }

        let has_version_marker = version_marker.is_some();
        let version_marker = if let Some(marker) = version_marker {
            Some(parse_version_marker(marker)?)
        } else {
            None
        };

        let effective_max_keys = list_versions_scan_limit(max_keys, has_version_marker);
        let opts = ListPathOptions {
            bucket: bucket.to_owned(),
            caller: if include_version_purge {
                ListObjectsGatherCaller::Lifecycle
            } else {
                ListObjectsGatherCaller::S3ListVersions
            },
            prefix: prefix.to_owned(),
            separator: delimiter.clone(),
            limit: effective_max_keys,
            marker,
            incl_deleted: true,
            ask_disks: list_objects_quorum_from_env(),
            versioned: true,
            include_marker: has_version_marker,
            ..Default::default()
        };
        let mut page = self
            .list_path(&opts)
            .await
            .map_err(|err| to_object_err(err, vec![bucket, prefix]))?;

        let disk_has_more = !page.exhausted;

        if !has_version_marker {
            page.entries.forward_past(opts.marker.clone());
        }

        let version_marker = version_marker_for_entries(&page.entries, opts.marker.as_deref(), version_marker);

        // Last RAW scanned key, captured before folding (ECA-03 / #944).
        let last_scanned_key = last_scanned_entry_name(&page.entries);

        let entries = page.entries;
        let get_objects = if include_version_purge {
            ObjectInfo::from_meta_cache_entries_sorted_versions_for_lifecycle(
                &entries,
                bucket,
                prefix,
                delimiter.clone(),
                version_marker,
            )
            .await
        } else {
            ObjectInfo::from_meta_cache_entries_sorted_versions(&entries, bucket, prefix, delimiter.clone(), version_marker).await
        };

        let (objects, prefixes, is_truncated, next_marker, next_version_idmarker) = list_objects_paginate_versions(
            get_objects,
            VersionedListPagination {
                delimiter: &delimiter,
                max_keys,
                disk_has_more,
                marker: opts.marker.as_deref(),
                last_scanned_key: last_scanned_key.as_deref(),
                raw_keys: entries.entries().iter().map(|entry| entry.name.as_str()),
            },
        );

        Ok(ListObjectVersionsInfo {
            is_truncated,
            next_marker,
            next_version_idmarker,
            objects,
            prefixes,
        })
    }

    pub async fn list_path(self: Arc<Self>, o: &ListPathOptions) -> Result<ListPage> {
        // tracing::warn!("list_path opt {:?}", &o);

        let list_path_started = std::time::Instant::now();

        check_list_objs_args(&o.bucket, &o.prefix, &o.marker)?;
        // if opts.prefix.ends_with(SLASH_SEPARATOR) {
        //     return Err(Error::msg("eof"));
        // }

        let mut o = o.clone();
        o.marker = o.marker.filter(|v| v >= &o.prefix);

        // Nothing can match these requests; answer with an exhausted empty
        // page instead of walking.
        let marker_outside_prefix = o
            .marker
            .as_deref()
            .is_some_and(|marker| !o.prefix.is_empty() && !marker.starts_with(&o.prefix));
        if marker_outside_prefix || o.limit == 0 || o.prefix.starts_with(SLASH_SEPARATOR) {
            return Ok(ListPage {
                entries: MetaCacheEntriesSorted::default(),
                exhausted: true,
            });
        }

        let slash_separator = Some(SLASH_SEPARATOR.to_owned());

        o.include_directories = o.separator == slash_separator;

        if (o.separator == slash_separator || o.separator.is_none()) && !o.recursive {
            o.recursive = o.separator != slash_separator;
            o.separator = slash_separator;
        } else {
            o.recursive = true
        }

        o.skip_hidden_prefix_check = can_skip_hidden_prefix_check(&o).await;

        if o.base_dir.is_empty() {
            o.base_dir = base_dir_from_prefix(&o.prefix);
        }

        o.transient = o.transient || is_reserved_or_invalid_bucket(&o.bucket, false);
        o.set_filter();
        if o.transient {
            o.create = false;
        }
        let log_context = ListPathLogContext::from_options(&o);

        // cancel channel
        let cancel = CancellationToken::new();

        let (sender, recv) = mpsc::channel(o.limit as usize);
        let store = self.clone();
        let mut opts = o.clone();
        opts.stop_disk_at_limit = true;
        let producer_cancel = cancel.clone();
        let producer = async move { store.list_merged(producer_cancel, opts, sender).await };
        let mut page = match collect_list_path_results("store", &log_context, cancel, o.clone(), recv, producer).await {
            Ok(page) => page,
            Err(err) => {
                log_list_path_finished("store", &log_context, list_path_started.elapsed().as_secs_f64() * 1000.0, 0, true);
                return Err(err);
            }
        };

        page.entries.reuse = true;
        page.entries.o.0.truncate(o.limit as usize);

        rustfs_io_metrics::record_stage_duration(
            "store_list_objects_list_path",
            list_path_started.elapsed().as_secs_f64() * 1000.0,
        );

        log_list_path_finished(
            "store",
            &log_context,
            list_path_started.elapsed().as_secs_f64() * 1000.0,
            page.entries.entries().len(),
            false,
        );

        Ok(page)
    }

    // Read all
    async fn list_merged(
        &self,
        rx: CancellationToken,
        opts: ListPathOptions,
        sender: Sender<MetaCacheEntry>,
    ) -> Result<ListPathRawOutcome> {
        // warn!("list_merged ops {:?}", &opts);
        let merge_started = std::time::Instant::now();

        debug!(
            list_path_limit = opts.limit,
            recursive = opts.recursive,
            pool_count = self.pools.len(),
            requested_marker = %opts.marker.as_deref().unwrap_or(""),
            "store list_merged started"
        );

        let rx = rx.child_token();
        let mut futures = Vec::new();

        let mut inputs = Vec::new();

        for sets in self.pools.iter() {
            for set in sets.disk_set.iter() {
                let (send, recv) = list_merged_entry_channel();

                inputs.push(recv);
                let opts = opts.clone();
                let rx_clone = rx.clone();
                futures.push(set.list_path(rx_clone, opts, send));
            }
        }

        let merge_task = spawn_listing_merge(rx, inputs, sender);

        let results = join_all(futures).await;
        merge_task.await.map_err(Error::from)??;

        let mut outcome = ListPathRawOutcome::default();
        let mut errs = Vec::new();
        for result in results {
            match result {
                Ok(set_outcome) => {
                    outcome.limit_reached |= set_outcome.limit_reached;
                    errs.push(None);
                }
                Err(err) => errs.push(Some(err)),
            }
        }

        // All sets returned not-found — the listing is simply empty.
        // We intentionally do NOT distinguish VolumeNotFound here: during
        // listing, a missing volume on a set is equivalent to "no entries"
        // and must not surface as an error to the caller. The original
        // VolumeNotFound check caused spurious errors when a pool had not
        // yet created the bucket prefix on every set.
        if is_all_not_found(&errs) {
            return Ok(outcome);
        }

        if let Some(err) = errs.iter().flatten().next() {
            return Err(err.clone());
        }

        rustfs_io_metrics::record_stage_duration(
            "store_list_objects_list_merged",
            merge_started.elapsed().as_secs_f64() * 1000.0,
        );

        debug!(
            set_pair_count = self.pools.len(),
            limit_reached = outcome.limit_reached,
            error_count = errs.iter().filter(|err| err.is_some()).count(),
            "store list_merged finished"
        );

        Ok(outcome)
    }

    #[allow(unused_assignments)]
    pub async fn walk_internal(
        self: Arc<Self>,
        rx: CancellationToken,
        bucket: &str,
        prefix: &str,
        result: Sender<ObjectInfoOrErr>,
        opts: WalkOptions,
    ) -> Result<()> {
        check_list_objs_args(bucket, prefix, &None)?;

        let rx = rx.child_token();
        let mut futures = Vec::new();
        let mut inputs = Vec::new();

        for eset in self.pools.iter() {
            for set in eset.disk_set.iter() {
                let (mut disks, infos, _) = set.get_online_disks_with_healing_and_info(true).await;
                let opts = opts.clone();

                let (sender, list_out_rx) = mpsc::channel::<MetaCacheEntry>(1);
                inputs.push(list_out_rx);
                let rx_clone = rx.clone();
                futures.push(async move {
                    let mut ask_disks = get_list_quorum(&opts.ask_disks, set.set_drive_count as i32);
                    if ask_disks == -1 {
                        let new_disks = get_quorum_disks(&disks, &infos, disks.len().div_ceil(2));
                        if !new_disks.is_empty() {
                            disks = new_disks;
                        } else {
                            ask_disks = get_list_quorum("strict", set.set_drive_count as i32);
                        }
                    }

                    if set.set_drive_count == 4 {
                        ask_disks = bounded_usize_to_i32(disks.len());
                    } else if ask_disks > bounded_usize_to_i32(disks.len()) {
                        ask_disks = clamp_ask_disks_to_available(ask_disks, disks.len());
                    }

                    let listing_quorum = listing_quorum_from_ask_disks(ask_disks);
                    let fallback_disks = {
                        if let Some(asked_disks) = positive_ask_disks(ask_disks)
                            && disks.len() > asked_disks
                        {
                            let mut rand = rand::rng();
                            disks.shuffle(&mut rand);
                            disks.split_off(asked_disks)
                        } else {
                            Vec::new()
                        }
                    };

                    let resolver = list_metadata_resolution_params(bucket.to_owned(), listing_quorum, !opts.latest_only);

                    let path = base_dir_from_prefix(prefix);
                    ensure_non_empty_listing_disks(bucket, &path, &disks).map_err(to_filemeta_err)?;

                    let mut filter_prefix = {
                        prefix
                            .trim_start_matches(&path)
                            .trim_start_matches(SLASH_SEPARATOR)
                            .trim_end_matches(SLASH_SEPARATOR)
                            .to_owned()
                    };

                    if filter_prefix == path {
                        filter_prefix = "".to_owned();
                    }

                    let tx1 = sender.clone();
                    let tx2 = sender.clone();

                    list_path_raw(
                        rx_clone,
                        ListPathRawOptions {
                            disks: disks.into_iter().map(Some).collect(),
                            fallback_disks: fallback_disks.into_iter().map(Some).collect(),
                            bucket: bucket.to_owned(),
                            path,
                            recursive: true,
                            incl_deleted: !opts.latest_only,
                            filter_prefix: Some(filter_prefix),
                            forward_to: opts.marker.clone(),
                            min_disks: listing_quorum,
                            per_disk_limit: bounded_usize_to_i32(opts.limit),
                            // Skip the total walkdir timeout for listing operations.
                            // Large buckets (millions of objects) can take longer than
                            // the default 5s walkdir timeout to produce the first page
                            // of results. The stall timeout still protects against
                            // drives that stop making forward progress.
                            skip_walkdir_total_timeout: opts.walkdir_timeout.is_none(),
                            walkdir_timeout: opts.walkdir_timeout,
                            walkdir_stall_timeout: opts.walkdir_stall_timeout,
                            agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                                Box::pin({
                                    let value = tx1.clone();
                                    async move {
                                        if entry.is_dir() {
                                            return Ok(());
                                        }
                                        if let Err(err) = value.send(entry).await {
                                            error!("list_path send fail {:?}", err);
                                        }
                                        Ok(())
                                    }
                                })
                            })),
                            partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<DiskError>]| {
                                Box::pin({
                                    let value = tx2.clone();
                                    let resolver = resolver.clone();
                                    async move {
                                        if let Some(entry) = entries.resolve(resolver)
                                            && let Err(err) = value.send(entry).await
                                        {
                                            error!("list_path send fail {:?}", err);
                                        }
                                    }
                                })
                            })),
                            finished: None,
                            ..Default::default()
                        },
                    )
                    .await
                });
            }
        }

        let (merge_tx, merge_rx) = mpsc::channel::<MetaCacheEntry>(100);
        let bucket = bucket.to_owned();
        // Not awaited: callers may drain `result` only after this returns, so
        // waiting on the forwarder here could stall them on the channel bound.
        let _forwarder = spawn_walk_entry_forwarder(rx.clone(), merge_rx, result, opts, bucket.clone());

        let merge_task = spawn_listing_merge(rx, inputs, merge_tx);

        let walk_started = std::time::Instant::now();
        let walk_results = join_all(futures).await;
        merge_task.await.map_err(Error::from)??;
        let mut errs = Vec::new();
        for walk_result in walk_results {
            match walk_result {
                Ok(_) => errs.push(None),
                Err(err) => errs.push(Some(err.into())),
            }
        }
        rustfs_io_metrics::record_stage_duration(
            "store_list_objects_walk_internal",
            walk_started.elapsed().as_secs_f64() * 1000.0,
        );

        let result = walk_result_from_set_errors(&errs);
        if let Err(err) = &result {
            error!(
                bucket = %bucket,
                prefix = %prefix,
                error = ?err,
                set_errors = ?errs,
                "walk_internal list_path_raw tasks failed"
            );
        }

        result
    }
}

/// Forwards merged raw entries to a walk consumer as `ObjectInfoOrErr`,
/// applying the walk's version projection, sort order and filter. The first
/// entry that fails to decode ends the stream with that error; a consumer that
/// stopped reading ends it silently. Either exit cancels the walk's producers.
fn spawn_walk_entry_forwarder(
    rx: CancellationToken,
    mut entries: Receiver<MetaCacheEntry>,
    result: Sender<ObjectInfoOrErr>,
    opts: WalkOptions,
    bucket: String,
) -> tokio::task::JoinHandle<()> {
    // Capture before spawning so even an abort before the first poll cancels
    // the producers. Callers pass a child token to isolate request cancellation.
    let cancel_on_drop = rx.clone().drop_guard();
    tokio::spawn(
        async move {
            let _cancel_on_drop = cancel_on_drop;
            let versioning = tokio::select! {
                _ = rx.cancelled() => return,
                _ = result.closed() => return,
                versioning = get_versioning_config(&bucket) => versioning.ok().map(|(config, _)| config),
            };
            let to_object_info = |fi: &rustfs_filemeta::FileInfo| {
                let versioned = versioning.as_ref().is_some_and(|config| config.versioned(&fi.name));
                ObjectInfo::from_file_info(fi, &bucket, &fi.name, versioned)
            };
            let passes_filter = |fi: &rustfs_filemeta::FileInfo| opts.filter.is_none_or(|filter| filter(fi));
            let decode_failed = |err: rustfs_filemeta::Error| ObjectInfoOrErr {
                item: None,
                err: Some(err.into()),
            };

            loop {
                let entry = tokio::select! {
                    _ = rx.cancelled() => return,
                    _ = result.closed() => return,
                    entry = entries.recv() => match entry {
                        Some(entry) => entry,
                        None => return,
                    },
                };
                if entry.is_dir() {
                    continue;
                }
                if opts.latest_only {
                    let fi = match entry.to_fileinfo(&bucket) {
                        Ok(fi) => fi,
                        Err(err) => {
                            let _ = send_or_cancel(&rx, &result, decode_failed(err)).await;
                            return;
                        }
                    };
                    if passes_filter(&fi)
                        && !matches!(
                            send_or_cancel(
                                &rx,
                                &result,
                                ObjectInfoOrErr {
                                    item: Some(to_object_info(&fi)),
                                    err: None,
                                }
                            )
                            .await,
                            Ok(true)
                        )
                    {
                        return;
                    }
                    continue;
                }

                let mut versions = match if opts.include_free_versions {
                    entry.file_info_versions_with_free_versions(&bucket)
                } else {
                    entry.file_info_versions(&bucket)
                } {
                    Ok(versions) => versions,
                    Err(err) => {
                        let _ = send_or_cancel(&rx, &result, decode_failed(err)).await;
                        return;
                    }
                };
                // `FileMeta` keeps versions newest-first, so ascending emission is
                // the exact reverse. Replication resync walks ascending so that
                // re-applied versions keep the original version-stack order.
                if opts.versions_sort == WalkVersionsSortOrder::Ascending {
                    versions.versions.reverse();
                }
                let free_versions = if opts.include_free_versions {
                    versions.free_versions.as_slice()
                } else {
                    &[]
                };
                for fi in versions.versions.iter().chain(free_versions) {
                    if passes_filter(fi)
                        && !matches!(
                            send_or_cancel(
                                &rx,
                                &result,
                                ObjectInfoOrErr {
                                    item: Some(to_object_info(fi)),
                                    err: None,
                                }
                            )
                            .await,
                            Ok(true)
                        )
                    {
                        return;
                    }
                }
            }
        }
        .instrument(tracing::Span::current()),
    )
}

/// Own every page worker until completion, including when the request is dropped.
async fn collect_list_path_results<P>(
    component: &'static str,
    context: &ListPathLogContext,
    cancel: CancellationToken,
    opts: ListPathOptions,
    recv: Receiver<MetaCacheEntry>,
    producer: P,
) -> Result<ListPage>
where
    P: Future<Output = Result<ListPathRawOutcome>> + Send + 'static,
{
    let _cancel_guard = cancel.clone().drop_guard();
    let (result_tx, mut result_rx) = mpsc::channel(1);
    let mut workers = JoinSet::new();
    #[cfg(feature = "diagnostic-read-version-caller")]
    let mut diagnostic_context = crate::cluster::rpc::runtime_sources::ReadVersionDiagnosticContext::capture_entry(
        rustfs_io_metrics::internode_metrics::ReadVersionEntrySource::ListObjects,
    );
    #[cfg(feature = "diagnostic-read-version-caller")]
    let request_caller = diagnostic_context.caller.or_else(|| {
        use rustfs_io_metrics::ListObjectsGatherCaller;
        use rustfs_io_metrics::internode_metrics::ReadVersionRequestCaller;

        match opts.caller {
            ListObjectsGatherCaller::S3List | ListObjectsGatherCaller::S3ListVersions => {
                Some(ReadVersionRequestCaller::ListObjects)
            }
            ListObjectsGatherCaller::Lifecycle => Some(ReadVersionRequestCaller::Lifecycle),
            ListObjectsGatherCaller::Other => Some(ReadVersionRequestCaller::Other),
            ListObjectsGatherCaller::Unattributed => None,
        }
    });
    #[cfg(feature = "diagnostic-read-version-caller")]
    {
        diagnostic_context.caller = request_caller;
    }
    let producer = async move { producer.await.map(ListWorkerExit::Producer) };
    #[cfg(feature = "diagnostic-read-version-caller")]
    let producer = crate::cluster::rpc::runtime_sources::with_read_version_diagnostic_context(
        diagnostic_context,
        rustfs_io_metrics::internode_metrics::ReadVersionContextBoundary::ListProducer,
        producer,
    );
    workers.spawn(producer.instrument(tracing::Span::current()));
    let gather_cancel = cancel.clone();
    let gather = async move {
        gather_results(gather_cancel, opts, recv, result_tx)
            .await
            .map(ListWorkerExit::Gather)
    };
    #[cfg(feature = "diagnostic-read-version-caller")]
    let gather = crate::cluster::rpc::runtime_sources::with_read_version_diagnostic_context(
        diagnostic_context,
        rustfs_io_metrics::internode_metrics::ReadVersionContextBoundary::ListGather,
        gather,
    );
    workers.spawn(gather.instrument(tracing::Span::current()));

    let mut failure = None;
    let mut stopping = false;
    let mut gather_state = None;
    let mut producer_outcome = None;
    while let Some(completion) = workers.join_next().await {
        let error = match completion {
            Ok(Ok(ListWorkerExit::Gather(state))) => {
                if matches!(state, GatherResultsState::LimitReached | GatherResultsState::ConsumerGone) {
                    stopping = true;
                }
                gather_state = Some(state);
                None
            }
            // EOF is only authoritative after the producer also succeeds.
            Ok(Ok(ListWorkerExit::Producer(outcome))) => {
                producer_outcome = Some(outcome);
                None
            }
            Ok(Err(err)) => Some(err),
            Err(err) if stopping && err.is_cancelled() => None,
            // Do not expose a panic payload through the S3 error or logs.
            Err(err) if err.is_panic() => Some(Error::other("listing worker panicked")),
            Err(_) => Some(Error::other("listing worker unexpectedly cancelled")),
        };
        if let Some(error) = error {
            failure.get_or_insert(error);
            stopping = true;
        }
        if stopping {
            cancel.cancel();
            workers.abort_all();
        }
        // Drain even after abort: a completed error/panic must not be hidden
        // by a page limit becoming ready at the same time.
    }

    if let Some(error) = failure {
        log_list_path_worker_error(component, "worker_error", context, &error);
        return Err(error);
    }

    // gather_results sends at most one page into a capacity-1 channel, so it
    // never needs this receiver to make progress. Read only after both joins.
    let Ok(entries) = result_rx.try_recv() else {
        return Err(Error::other("listing worker exited without a page"));
    };
    // The page is complete only when the collector drained a closed input and
    // every producer stopped at the end of its namespace rather than at a
    // per-disk limit: a bounded producer can close the input after spending
    // its budget on entries this page filtered out.
    let exhausted = matches!(gather_state, Some(GatherResultsState::InputClosed))
        && producer_outcome.is_some_and(|outcome| !outcome.limit_reached);
    Ok(ListPage { entries, exhausted })
}

async fn gather_results(
    rx: CancellationToken,
    opts: ListPathOptions,
    recv: Receiver<MetaCacheEntry>,
    results_tx: Sender<MetaCacheEntriesSorted>,
) -> Result<GatherResultsState> {
    let mut recv = recv;
    let mut entries = Vec::new();
    let list_metrics_enabled = rustfs_io_metrics::get_stage_metrics_enabled();
    let gather_started = list_metrics_enabled.then(std::time::Instant::now);
    let mut scanned_entries = 0usize;
    let mut candidate_entries = 0usize;
    let mut previous_prefix = String::new();
    let folds_prefixes = list_path_folds_common_prefixes(&opts);

    while let Some(mut entry) = recv.recv().await {
        scanned_entries += 1;
        #[cfg(windows)]
        {
            // normalize windows path separator
            entry.name = entry.name.replace("\\", "/");
        }

        // TODO(backlog): integrate rx.recv() for incremental listing results

        if let Some(marker) = &opts.marker
            && ((!opts.include_marker && &entry.name <= marker) || (opts.include_marker && &entry.name < marker))
        {
            continue;
        }

        if !entry.name.starts_with(&opts.prefix) {
            continue;
        }

        if let Some(separator) = &opts.separator
            && !opts.recursive
            && !entry.is_in_dir(&opts.prefix, separator)
        {
            continue;
        }

        let is_object = entry.is_object();
        let is_latest_delete_marker = is_object && entry.is_latest_delete_marker();

        if !opts.include_directories && (entry.is_dir() || (!opts.versioned && is_latest_delete_marker)) {
            continue;
        }

        if !opts.incl_deleted && is_latest_delete_marker {
            continue;
        }

        // Version LIST projections omit cleanup-only free versions. They must
        // not consume the lookahead budget before a later visible version.
        // Cleanup walks consume the raw set stream without this collector.
        if opts.versioned
            && is_object
            && entry
                .xl_meta()
                .is_ok_and(|meta| !meta.versions.is_empty() && meta.versions.iter().all(|version| version.header.free_version()))
        {
            continue;
        }

        if folds_prefixes
            && let Some(separator) = opts.separator.as_deref().filter(|separator| !separator.is_empty())
            && let Some(offset) = entry.name[opts.prefix.len()..].find(separator)
        {
            let common_prefix = &entry.name[..opts.prefix.len() + offset + separator.len()];
            if common_prefix == previous_prefix
                || opts.marker.as_deref().is_some_and(|marker| {
                    (!opts.include_marker && common_prefix <= marker) || (opts.include_marker && common_prefix < marker)
                })
            {
                continue;
            }
            previous_prefix.clear();
            previous_prefix.push_str(common_prefix);
        }

        // TODO(backlog): integrate lifecycle evaluation during object listing

        entries.push(Some(entry));
        candidate_entries += 1;

        if opts.limit > 0 && entries.len() >= opts.limit as usize {
            rx.cancel();
            let filtered = scanned_entries.saturating_sub(candidate_entries);
            if let Some(started) = gather_started {
                let duration_ms = started.elapsed().as_secs_f64() * 1000.0;
                rustfs_io_metrics::record_list_objects_gather(ListObjectsGatherObservation {
                    source: LIST_OBJECTS_SOURCE_WALKER,
                    caller: opts.caller,
                    outcome: LIST_OBJECTS_GATHER_OUTCOME_LIMIT_REACHED,
                    limit: opts.limit,
                    scanned_entries,
                    returned_entries: candidate_entries,
                    duration_ms,
                    has_prefix: !opts.prefix.is_empty(),
                    has_delimiter: opts.separator.as_ref().is_some_and(|separator| !separator.is_empty()),
                    has_marker: opts.marker.is_some(),
                });
                rustfs_io_metrics::record_stage_duration("store_list_objects_gather", duration_ms);
            }
            debug!(
                bucket = %opts.bucket,
                prefix = %opts.prefix,
                limit = opts.limit,
                scanned_entries = scanned_entries,
                returned_entries = candidate_entries,
                filtered_entries = filtered,
                marker = %opts.marker.as_deref().unwrap_or(""),
                "list_objects gather_results reached page limit and cancelled upstream listing"
            );

            // Invariant: the results channel has capacity 1 and this function
            // performs at most one send per call, so a send failure here means
            // the receiver was dropped (consumer gone), never a full buffer.
            if results_tx
                .send(MetaCacheEntriesSorted {
                    o: MetaCacheEntries(entries),
                    ..Default::default()
                })
                .await
                .is_err()
            {
                debug!(
                    bucket = %opts.bucket,
                    prefix = %opts.prefix,
                    "list_objects gather_results consumer disconnected before limited page delivery"
                );
                return Ok(GatherResultsState::ConsumerGone);
            }
            return Ok(GatherResultsState::LimitReached);
        }
    }

    // The input closed before the limit. Whether that is the end of the
    // namespace is decided by `collect_list_path_results`, which also sees how
    // the producers stopped.
    let filtered = scanned_entries.saturating_sub(candidate_entries);
    if let Some(started) = gather_started {
        let duration_ms = started.elapsed().as_secs_f64() * 1000.0;
        rustfs_io_metrics::record_list_objects_gather(ListObjectsGatherObservation {
            source: LIST_OBJECTS_SOURCE_WALKER,
            caller: opts.caller,
            outcome: LIST_OBJECTS_GATHER_OUTCOME_INPUT_CLOSED,
            limit: opts.limit,
            scanned_entries,
            returned_entries: candidate_entries,
            duration_ms,
            has_prefix: !opts.prefix.is_empty(),
            has_delimiter: opts.separator.as_ref().is_some_and(|separator| !separator.is_empty()),
            has_marker: opts.marker.is_some(),
        });
        rustfs_io_metrics::record_stage_duration("store_list_objects_gather", duration_ms);
    }
    debug!(
        bucket = %opts.bucket,
        prefix = %opts.prefix,
        scanned_entries = scanned_entries,
        returned_entries = candidate_entries,
        filtered_entries = filtered,
        marker = %opts.marker.as_deref().unwrap_or(""),
        "list_objects gather_results drained all candidates without hitting limit"
    );

    // Invariant: the results channel has capacity 1 and this function performs
    // at most one send per call, so a send failure here means the receiver was
    // dropped (consumer gone), never a full buffer.
    if results_tx
        .send(MetaCacheEntriesSorted {
            o: MetaCacheEntries(entries),
            ..Default::default()
        })
        .await
        .is_err()
    {
        debug!(
            bucket = %opts.bucket,
            prefix = %opts.prefix,
            "list_objects gather_results consumer disconnected before eof page delivery"
        );
        return Ok(GatherResultsState::ConsumerGone);
    }

    Ok(GatherResultsState::InputClosed)
}

/// Head entry of one input channel inside the k-way merge.
///
/// The entry name is canonicalized once at construction (see `new`), so heap
/// comparisons never allocate; walker-produced names are already canonical,
/// making the rewrite a no-op in the common case.
struct MergeHead {
    entry: MetaCacheEntry,
    idx: usize,
}

impl MergeHead {
    fn new(entry: MetaCacheEntry, idx: usize) -> Self {
        let mut entry = entry;
        // Canonicalize the name at intake: collapse redundant internal
        // segments (`a//b` → `a/b`) but keep one trailing slash, since it is
        // what distinguishes a prefix dir / dir-marker from a plain object.
        // Ordering, grouping, and the emitted stream then all use the same
        // canonical byte order that downstream marker filtering and S3
        // pagination compare with — ordering by the *cleaned* key while
        // markers compare raw names let a prefix dir `a/` jump ahead of a
        // sibling like `a.txt`, and a page boundary there dropped the sibling
        // on the continuation (backlog#1046).
        let cleaned = path::clean(&entry.name);
        if !cleaned.is_empty() && cleaned != "." && cleaned != entry.name {
            let canonical = if entry.name.ends_with(SLASH_SEPARATOR) {
                format!("{cleaned}{SLASH_SEPARATOR}")
            } else {
                cleaned
            };
            if canonical != entry.name {
                entry.name = canonical;
            }
        }
        Self { entry, idx }
    }

    fn sort_key(&self) -> &str {
        &self.entry.name
    }
}

impl PartialEq for MergeHead {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for MergeHead {}

impl PartialOrd for MergeHead {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeHead {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Ascending channel index preserves the legacy scan order for equal keys.
        self.sort_key().cmp(other.sort_key()).then_with(|| self.idx.cmp(&other.idx))
    }
}

enum MergePoll {
    Entry(Box<MergeHead>),
    Closed,
    Cancelled,
}

async fn poll_merge_head(rx: &CancellationToken, in_channels: &mut [Receiver<MetaCacheEntry>], idx: usize) -> MergePoll {
    tokio::select! {
        entry = in_channels[idx].recv() => match entry {
            Some(entry) => MergePoll::Entry(Box::new(MergeHead::new(entry, idx))),
            None => MergePoll::Closed,
        },
        _ = rx.cancelled() => MergePoll::Cancelled,
    }
}

async fn send_or_cancel<T: Send>(rx: &CancellationToken, out_channel: &Sender<T>, entry: T) -> Result<bool> {
    tokio::select! {
        result = out_channel.send(entry) => Ok(result.is_ok()),
        _ = rx.cancelled() => Ok(false),
    }
}

fn listing_merge_corruption(object: &str, version_id: Option<Uuid>, validation: &'static str) -> Error {
    error!(
        event = EVENT_LISTING_MERGE_REJECTED,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_LISTING,
        result = "rejected",
        object,
        version_id = ?version_id,
        validation,
        "Listing version merge rejected metadata"
    );
    Error::FileCorrupt
}

/// Each input has already been resolved inside its own erasure set. This is a
/// union of version histories, never a quorum vote between unrelated pools.
fn merge_object_entry_versions(first: &mut MetaCacheEntry, others: impl Iterator<Item = MetaCacheEntry>) -> Result<()> {
    let name = first.name.clone();
    let mut versions: HashMap<(Option<Uuid>, bool), (FileMetaShallowVersion, ObjectInfo)> = HashMap::new();
    for mut entry in std::iter::once(std::mem::take(first)).chain(others) {
        let meta = match entry.cached.take() {
            Some(meta) => meta,
            None => FileMeta::load(&entry.metadata).map_err(|_| listing_merge_corruption(&name, None, "metadata_decode"))?,
        };
        if meta.versions.is_empty() {
            return Err(listing_merge_corruption(&name, None, "empty_version_stack"));
        }
        for version in meta.versions {
            let corrupt = |validation| listing_merge_corruption(&name, version.header.version_id, validation);
            let parsed = version.parse_version_meta().map_err(|_| corrupt("version_decode"))?;
            if !parsed.valid() || parsed.version_type != version.header.version_type {
                return Err(corrupt("version_type_or_validity"));
            }
            let fi = parsed
                .into_fileinfo("", &name, true)
                .map_err(|_| corrupt("file_info_decode"))?;
            let version_id = fi.version_id.filter(|id| !id.is_nil());
            if version_id != version.header.version_id.filter(|id| !id.is_nil())
                || fi.mod_time != version.header.mod_time
                || fi.tier_free_version() != version.header.free_version()
            {
                return Err(corrupt("version_header_mismatch"));
            }
            let info = ObjectInfo::from_file_info(&fi, "", &name, true);
            let identity = (version_id, version.header.free_version());
            match versions.entry(identity) {
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert((version, info));
                }
                std::collections::hash_map::Entry::Occupied(mut slot) => {
                    let (previous, previous_info) = slot.get();
                    // Suspended and unversioned writes replace the one null
                    // slot. Distinct UUID versions never supersede each other.
                    if version_id.is_none() && info.mod_time != previous_info.mod_time {
                        if info.mod_time > previous_info.mod_time {
                            slot.insert((version, info));
                        }
                        continue;
                    }
                    let equivalent = if info.delete_marker && previous_info.delete_marker {
                        super::object::is_equivalent_data_movement_delete_marker(&info, previous_info)
                    } else {
                        crate::data_movement::is_equivalent_data_movement_object_identity(&info, previous_info, true, true)
                    };
                    if !equivalent {
                        return Err(corrupt("version_identity_conflict"));
                    }
                    // Equivalent migrated copies can have different coding or
                    // data directories. Choose a stable representation without
                    // making input order part of the S3 version order.
                    if version.meta < previous.meta {
                        slot.insert((version, info));
                    }
                }
            }
        }
    }
    let mut live_remote_references = HashMap::<String, HashMap<String, Vec<ObjectInfo>>>::new();
    for (_, info) in versions.values() {
        if !info.transitioned_object.free_version && info.transitioned_object.status == rustfs_filemeta::TRANSITION_COMPLETE {
            live_remote_references
                .entry(info.transitioned_object.tier.clone())
                .or_default()
                .entry(info.transitioned_object.name.clone())
                .or_default()
                .push(info.clone());
        }
    }
    // Keep cleanup durable in its source xl.meta, but do not expose it to a
    // merged recovery walk while another physical pool still owns the tuple.
    versions.retain(|_, (_, info)| {
        !info.transitioned_object.free_version
            || !live_remote_references
                .get(info.transitioned_object.tier.as_str())
                .and_then(|by_name| by_name.get(info.transitioned_object.name.as_str()))
                .is_some_and(|candidates| {
                    candidates
                        .iter()
                        .any(|live| free_version_remote_tuple_matches(info, live).unwrap_or(false))
                })
    });
    let mut merged = FileMeta::new();
    merged.versions = versions.into_values().map(|(version, _)| version).collect();
    merged.versions.sort_by(|a, b| {
        if a.header.sorts_before(&b.header) {
            std::cmp::Ordering::Less
        } else if b.header.sorts_before(&a.header) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });
    let metadata = merged.marshal_msg()?;
    *first = MetaCacheEntry {
        name,
        metadata,
        cached: Some(merged),
        reusable: true,
    };
    Ok(())
}

/// `rx` is private to the producers. Cancelling it on a merge error must not
/// cancel the request token, which would suppress that error at the API edge.
fn spawn_listing_merge(
    rx: CancellationToken,
    inputs: Vec<Receiver<MetaCacheEntry>>,
    sender: Sender<MetaCacheEntry>,
) -> AbortOnDropHandle<Result<()>> {
    AbortOnDropHandle::new(tokio::spawn(
        async move {
            let result = merge_entry_channels(rx.clone(), inputs, sender, 1).await;
            if result.is_err() {
                rx.cancel();
            }
            result
        }
        .instrument(tracing::Span::current()),
    ))
}

async fn merge_entry_channels(
    rx: CancellationToken,
    in_channels: Vec<Receiver<MetaCacheEntry>>,
    out_channel: Sender<MetaCacheEntry>,
    read_quorum: usize,
) -> Result<()> {
    if in_channels.is_empty() {
        return Ok(());
    }

    rustfs_io_metrics::record_list_objects_merge(LIST_OBJECTS_SOURCE_WALKER, in_channels.len(), read_quorum);

    let mut in_channels = in_channels;
    if in_channels.len() == 1 {
        loop {
            tokio::select! {
                has_entry = in_channels[0].recv()=>{
                    if let Some(entry) = has_entry{
                        // warn!("merge_entry_channels entry {}", &entry.name);
                        if !send_or_cancel(&rx, &out_channel, entry).await? {
                            return Ok(());
                        }
                    } else {
                        return Ok(())
                    }
                },
                _ = rx.cancelled()=>{
                    info!("merge_entry_channels rx.recv() cancel");
                    return Ok(())
                },
            }
        }
    }

    // K-way merge across per-channel sorted streams via a min-heap keyed by
    // the canonical entry name (see `MergeHead::new`): advancing the merge
    // costs O(log channels) per entry and comparisons are allocation-free.
    // The output byte order must match what markers and `forward_past`
    // compare with, or pagination drops keys at page boundaries.
    //
    // Note on same-name resolution: prefix-dir groups collapse into a single
    // emission and objects shadow same-named prefix dirs. The legacy
    // `merge_file_meta_versions` block that used to live here was dead code:
    // it only ran for prefix-dir groups, whose entries have empty metadata,
    // so `xl_meta()` always failed. Cross-drive version merging happens in
    // the metadata resolve path (`MetaCacheEntries::resolve`), not here.
    let mut heap: BinaryHeap<Reverse<Box<MergeHead>>> = BinaryHeap::with_capacity(in_channels.len());
    for idx in 0..in_channels.len() {
        match poll_merge_head(&rx, &mut in_channels, idx).await {
            MergePoll::Entry(head) => heap.push(Reverse(head)),
            MergePoll::Closed => {}
            MergePoll::Cancelled => {
                info!("merge_entry_channels rx.recv() cancel");
                return Ok(());
            }
        }
    }

    // Cross-iteration dedup state. Per-channel streams are sorted by name and
    // the heap orders by the same canonical names, so equal names are always
    // contiguous in pop order: a single last-emitted slot suffices to collapse
    // a lagging channel's duplicate that missed its group (its head cannot pop
    // after anything greater has been emitted).
    let mut last_emitted = String::new();
    let mut group: Vec<Box<MergeHead>> = Vec::new();
    let mut object_entries: Vec<MetaCacheEntry> = Vec::new();
    let mut refill: Vec<usize> = Vec::with_capacity(in_channels.len());

    while let Some(Reverse(first)) = heap.pop() {
        // Collect every channel head with the same canonical name so the
        // duplicate rules below see the whole same-name group at once.
        group.clear();
        refill.clear();
        refill.push(first.idx);
        while heap.peek().is_some_and(|Reverse(next)| next.sort_key() == first.sort_key()) {
            let Some(Reverse(next)) = heap.pop() else { break };
            refill.push(next.idx);
            group.push(next);
        }

        // Resolve the same-name group to one winner (heads arrive in ascending
        // channel order):
        //  - prefix dir vs prefix dir: the first (lowest channel) wins;
        //  - object vs object: merge the independently resolved version stacks;
        //  - object vs prefix dir: same-name means both end with the separator,
        //    i.e. the object is an explicit "directory marker" for the same S3
        //    key — it shadows the prefix dir so the key does not surface as
        //    both Contents and a CommonPrefix (issue #1797). A marker arriving
        //    a round late (after its channel's prefix dir was emitted) is
        //    collapsed by `last_emitted` instead — same single S3 key, the
        //    already-sent representation wins.
        // A plain object `a` and a prefix dir `a/` have different names and
        // therefore different groups: both are emitted, each at its own sorted
        // position (backlog#880 / #4563 coexistence).
        let mut dir_winner: Option<Box<MergeHead>> = None;
        let mut object_winner: Option<Box<MergeHead>> = None;
        for head in std::iter::once(first).chain(group.drain(..)) {
            if head.entry.is_dir() {
                if dir_winner.is_none() {
                    dir_winner = Some(head);
                }
            } else if let Some(winner) = object_winner.as_ref() {
                // Key-only candidates carry no version metadata and cannot
                // replace a resolved stack or contribute a quorum vote.
                if head.entry.is_object() {
                    if winner.entry.is_object() {
                        object_entries.push(head.entry);
                    } else {
                        object_winner = Some(head);
                    }
                }
            } else {
                object_winner = Some(head);
            }
        }

        if !object_entries.is_empty()
            && let Some(winner) = object_winner.as_mut()
        {
            merge_object_entry_versions(&mut winner.entry, object_entries.drain(..))?;
        }

        if let Some(head) = object_winner.or(dir_winner)
            && head.entry.name != last_emitted
        {
            last_emitted.clear();
            last_emitted.push_str(&head.entry.name);
            let MergeHead { entry, .. } = *head;
            if !send_or_cancel(&rx, &out_channel, entry).await? {
                return Ok(());
            }
        }

        for &idx in &refill {
            match poll_merge_head(&rx, &mut in_channels, idx).await {
                MergePoll::Entry(head) => heap.push(Reverse(head)),
                MergePoll::Closed => {}
                MergePoll::Cancelled => {
                    info!("merge_entry_channels rx.recv() cancel");
                    return Ok(());
                }
            }
        }
    }

    Ok(())
}

impl SetDisks {
    pub(crate) async fn inner_list_object_versions_for_recursive_delete(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        version_marker: Option<String>,
        max_keys: i32,
    ) -> Result<ListObjectVersionsInfo> {
        let max_keys = normalize_max_keys(max_keys);
        if marker.is_none() && version_marker.is_some() {
            return Err(StorageError::NotImplemented);
        }

        let has_version_marker = version_marker.is_some();
        let version_marker = version_marker.map(parse_version_marker).transpose()?;
        let effective_max_keys = list_versions_scan_limit(max_keys, has_version_marker);
        let opts = ListPathOptions {
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
            limit: effective_max_keys,
            marker,
            incl_deleted: true,
            ask_disks: list_objects_quorum_from_env(),
            versioned: true,
            include_marker: has_version_marker,
            ..Default::default()
        };
        let mut page = self
            .list_path_result(&opts)
            .await
            .map_err(|err| to_object_err(err, vec![bucket, prefix]))?;
        let disk_has_more = !page.exhausted;
        if !has_version_marker {
            page.entries.forward_past(opts.marker.clone());
        }
        let version_marker = version_marker_for_entries(&page.entries, opts.marker.as_deref(), version_marker);
        let last_scanned_key = last_scanned_entry_name(&page.entries);
        let entries = page.entries;
        let get_objects = ObjectInfo::from_meta_cache_entries_sorted_versions_for_recursive_delete(
            &entries,
            bucket,
            prefix,
            None,
            version_marker,
        )
        .await?;
        let (objects, prefixes, is_truncated, next_marker, next_version_idmarker) =
            list_objects_paginate(get_objects, &None, max_keys, disk_has_more, true, last_scanned_key.as_deref());
        Ok(ListObjectVersionsInfo {
            is_truncated,
            next_marker,
            next_version_idmarker,
            objects,
            prefixes,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn walk_internal(
        self: Arc<Self>,
        rx: CancellationToken,
        bucket: &str,
        prefix: &str,
        result: Sender<ObjectInfoOrErr>,
        opts: WalkOptions,
    ) -> Result<()> {
        check_list_objs_args(bucket, prefix, &None)?;

        let rx = rx.child_token();
        let (entry_tx, entry_rx) = mpsc::channel::<MetaCacheEntry>(100);
        let forwarder = spawn_walk_entry_forwarder(rx.clone(), entry_rx, result, opts.clone(), bucket.to_owned());

        let list_options = recursive_walk_list_path_options(bucket.to_owned(), prefix, &opts);
        let list_result = self.list_versions_authoritatively(rx, list_options, entry_tx).await;

        let _ = forwarder.await;

        match list_result {
            Ok(_) => Ok(()),
            Err(err) => walk_result_from_set_errors(&[Some(err)]),
        }
    }

    pub async fn list_path_result(self: Arc<Self>, o: &ListPathOptions) -> Result<ListPage> {
        check_list_objs_args(&o.bucket, &o.prefix, &o.marker)?;

        let list_path_started = std::time::Instant::now();

        let mut o = o.clone();
        o.marker = o.marker.filter(|v| v >= &o.prefix);

        // Nothing can match these requests; answer with an exhausted empty
        // page instead of walking.
        let marker_outside_prefix = o
            .marker
            .as_deref()
            .is_some_and(|marker| !o.prefix.is_empty() && !marker.starts_with(&o.prefix));
        if marker_outside_prefix || o.limit == 0 || o.prefix.starts_with(SLASH_SEPARATOR) {
            return Ok(ListPage {
                entries: MetaCacheEntriesSorted::default(),
                exhausted: true,
            });
        }

        let slash_separator = Some(SLASH_SEPARATOR.to_owned());
        o.include_directories = o.separator == slash_separator;

        if (o.separator == slash_separator || o.separator.is_none()) && !o.recursive {
            o.recursive = o.separator != slash_separator;
            o.separator = slash_separator;
        } else {
            o.recursive = true;
        }

        o.skip_hidden_prefix_check = can_skip_hidden_prefix_check(&o).await;

        if o.base_dir.is_empty() {
            o.base_dir = base_dir_from_prefix(&o.prefix);
        }

        o.transient = o.transient || is_reserved_or_invalid_bucket(&o.bucket, false);
        o.set_filter();
        if o.transient {
            o.create = false;
        }
        let log_context = ListPathLogContext::from_options(&o);

        let cancel = CancellationToken::new();
        let (sender, recv) = mpsc::channel(o.limit as usize);
        let set = self.clone();
        let mut opts = o.clone();
        opts.stop_disk_at_limit = true;
        let producer_cancel = cancel.clone();
        // Set-scoped version pages back recursive prefix deletes, which must
        // not skip a version that may still exist.
        let producer = async move {
            if opts.versioned {
                set.list_versions_authoritatively(producer_cancel, opts, sender).await
            } else {
                set.list_path(producer_cancel, opts, sender).await
            }
        };
        let mut page = match collect_list_path_results("set_disks", &log_context, cancel, o.clone(), recv, producer).await {
            Ok(page) => page,
            Err(err) => {
                log_list_path_finished("set_disks", &log_context, list_path_started.elapsed().as_secs_f64() * 1000.0, 0, true);
                return Err(err);
            }
        };

        page.entries.reuse = true;
        page.entries.o.0.truncate(o.limit as usize);

        rustfs_io_metrics::record_stage_duration(
            "sets_list_objects_list_path",
            list_path_started.elapsed().as_secs_f64() * 1000.0,
        );

        log_list_path_finished(
            "set_disks",
            &log_context,
            list_path_started.elapsed().as_secs_f64() * 1000.0,
            page.entries.entries().len(),
            false,
        );

        Ok(page)
    }

    /// Version scan for set-scoped callers that act on absence: set walks
    /// (tier reference proofs, tier-delete journals, pool maintenance) and the
    /// recursive prefix-delete scan. Every online drive is read and each
    /// version is resolved at the read quorum of its own erasure layout; a
    /// version that cannot be ruled out fails the scan instead of being
    /// omitted. Merged `ListObjectVersions` pages go through `list_path`.
    async fn list_versions_authoritatively(
        &self,
        rx: CancellationToken,
        opts: ListPathOptions,
        sender: Sender<MetaCacheEntry>,
    ) -> Result<ListPathRawOutcome> {
        let (disks, _, _) = self.get_online_disks_with_healing_and_info(true).await;
        let disk_count = self.set_drive_count;
        let parity = self.default_parity_count;
        let read_quorum = set_listing_read_floor(disk_count, parity);
        if disk_count == 0 || disks.len() < read_quorum {
            return Err(DiskError::ErasureReadQuorum.into());
        }
        let first_error = Arc::new(tokio::sync::Mutex::new(None));
        let error_sink = Arc::clone(&first_error);
        let bucket = opts.bucket.clone();
        let cancel = rx.clone();
        let result = list_path_raw_with_claim_tracker(
            rx,
            ListPathRawOptions {
                disks: disks.into_iter().map(Some).collect(),
                bucket: opts.bucket,
                path: opts.base_dir,
                recursive: opts.recursive,
                incl_deleted: true,
                skip_hidden_prefix_check: opts.skip_hidden_prefix_check,
                filter_prefix: opts.filter_prefix,
                forward_to: opts.marker,
                min_disks: read_quorum,
                preserve_replica_metadata: true,
                // A reader's local page may be consumed by stale entries. Only
                // the resolved, merged page may stop the authoritative walk.
                per_disk_limit: 0,
                skip_walkdir_total_timeout: true,
                walkdir_timeout: opts.walkdir_timeout,
                walkdir_stall_timeout: opts.walkdir_stall_timeout,
                partial: Some(Box::new(move |entries, errors| {
                    let resolved = Self::resolve_listed_versions(&bucket, entries, errors, disk_count, parity);
                    let sender = sender.clone();
                    let cancel = cancel.clone();
                    let first_error = Arc::clone(&error_sink);
                    Box::pin(async move {
                        match resolved {
                            Ok(Some(entry)) => {
                                let _ = send_or_cancel(&cancel, &sender, entry).await;
                            }
                            Ok(None) => {}
                            Err(error) => {
                                first_error.lock().await.get_or_insert(error);
                            }
                        }
                    })
                })),
                ..Default::default()
            },
            FallbackClaimTracker::default(),
        )
        .await;
        if let Some(error) = first_error.lock().await.take() {
            return Err(error.into());
        }
        result.map_err(Into::into)
    }

    /// Streams the merged raw entries of this set into `sender` and reports
    /// how the producers stopped.
    ///
    /// Latest-version listing follows MinIO's `listPath`: an entry is visible
    /// when about half of the asked drives carry it, a split sample resolves
    /// to the newest version that reaches that quorum, and an entry that
    /// reaches no quorum is omitted instead of failing the page. Whether the
    /// listed version is readable remains the read path's decision.
    pub async fn list_path(
        &self,
        rx: CancellationToken,
        opts: ListPathOptions,
        sender: Sender<MetaCacheEntry>,
    ) -> Result<ListPathRawOutcome> {
        let list_path_started = std::time::Instant::now();

        let (mut disks, infos, _) = self.get_online_disks_with_healing_and_info(true).await;
        let online_disks = disks.len();
        // Unlike MinIO, a set with fewer than half of its drives online does
        // not list from that minority: the whole page is unavailable.
        let set_read_quorum = self.set_drive_count.div_ceil(2);
        if self.set_drive_count == 0 || online_disks < set_read_quorum {
            debug!(
                event = EVENT_SET_LIST_PATH,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LISTING,
                state = "below_set_read_quorum",
                bucket = %opts.bucket,
                prefix = %opts.base_dir,
                set_drive_count = self.set_drive_count,
                online_disks,
                set_read_quorum,
                "Set listing is below read quorum"
            );
            return Err(StorageError::ErasureReadQuorum);
        }

        let mut ask_disks = get_list_quorum(&opts.ask_disks, self.set_drive_count as i32);
        if ask_disks == -1 {
            let new_disks = get_quorum_disks(&disks, &infos, disks.len().div_ceil(2));
            if !new_disks.is_empty() {
                disks = new_disks;
                ask_disks = 1;
            } else {
                ask_disks = get_list_quorum("strict", self.set_drive_count as i32);
            }
        }

        if self.set_drive_count == 4 {
            ask_disks = bounded_usize_to_i32(disks.len());
        } else if ask_disks > bounded_usize_to_i32(disks.len()) {
            ask_disks = clamp_ask_disks_to_available(ask_disks, disks.len());
        }

        let listing_quorum = listing_quorum_from_ask_disks(ask_disks);
        let mut fallback_disks = Vec::new();
        if let Some(asked_disks) = positive_ask_disks(ask_disks)
            && disks.len() > asked_disks
        {
            let mut rand = rand::rng();
            disks.shuffle(&mut rand);
            // A fallback drive only replaces a primary reader that fails; it
            // never adds a vote to an entry the primary readers already saw.
            fallback_disks = disks.split_off(asked_disks);
        }
        let reader_disks = disks.len();
        let fallback_count = fallback_disks.len();

        let bucket = opts.bucket.clone();
        let base_dir = opts.base_dir.clone();
        // Version pages keep every version that reaches listing quorum
        // (requestedVersions = 0); object pages keep only the latest.
        let resolver = list_metadata_resolution_params(bucket.clone(), listing_quorum, opts.versioned);

        let limit = {
            // A raw scan budget can be consumed entirely by keys folding into
            // one common prefix. Let the logical collector stop these walks.
            if opts.limit > 0 && opts.stop_disk_at_limit && !list_path_folds_common_prefixes(&opts) {
                opts.limit + 4 + (opts.limit / 16)
            } else {
                0
            }
        };

        debug!(
            event = EVENT_SET_LIST_PATH,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_LISTING,
            state = "quorum_selected",
            bucket = %bucket,
            prefix = %base_dir,
            set_drive_count = self.set_drive_count,
            online_disks,
            asked_disks = ask_disks,
            listing_quorum,
            fallback_disks = fallback_count,
            limit = opts.limit,
            per_disk_limit = limit,
            "Set list_path selected listing quorum"
        );

        let agreed_sender = sender.clone();
        let agreed_cancel = rx.clone();
        let partial_sender = sender;
        let partial_cancel = rx.clone();

        let result = list_path_raw_with_outcome(
            rx,
            ListPathRawOptions {
                disks: disks.into_iter().map(Some).collect(),
                fallback_disks: fallback_disks.into_iter().map(Some).collect(),
                bucket: opts.bucket,
                path: opts.base_dir,
                recursive: opts.recursive,
                incl_deleted: opts.incl_deleted,
                skip_hidden_prefix_check: opts.skip_hidden_prefix_check,
                filter_prefix: opts.filter_prefix,
                forward_to: opts.marker,
                min_disks: listing_quorum,
                per_disk_limit: limit,
                // A foreground listing is bounded by lack of drive progress (the walk
                // stall timeout) and by the page limit, never by how long a healthy
                // walk takes — a large prefix on slow media is not a fault (#4644).
                skip_walkdir_total_timeout: true,
                walkdir_timeout: opts.walkdir_timeout,
                walkdir_stall_timeout: opts.walkdir_stall_timeout,
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    let sender = agreed_sender.clone();
                    let cancel = agreed_cancel.clone();
                    Box::pin(async move {
                        let _ = send_or_cancel(&cancel, &sender, entry).await;
                        Ok(())
                    })
                })),
                partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<DiskError>]| {
                    let sender = partial_sender.clone();
                    let cancel = partial_cancel.clone();
                    let resolver = resolver.clone();
                    Box::pin(async move {
                        if cancel.is_cancelled() {
                            return;
                        }
                        if let Some(entry) = entries.resolve(resolver) {
                            let _ = send_or_cancel(&cancel, &sender, entry).await;
                        }
                    })
                })),
                finished: None,
                ..Default::default()
            },
        )
        .await;

        let elapsed_ms = list_path_started.elapsed().as_secs_f64() * 1000.0;
        rustfs_io_metrics::record_stage_duration("set_disks_list_objects_list_path", elapsed_ms);

        match &result {
            Ok(outcome) => debug!(
                event = EVENT_SET_LIST_PATH,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LISTING,
                result = "ok",
                bucket = %bucket,
                prefix = %base_dir,
                reader_disks,
                fallback_disks = fallback_count,
                listing_quorum,
                limit_reached = outcome.limit_reached,
                elapsed_ms,
                "Set list_path finished"
            ),
            Err(err) => debug!(
                event = EVENT_SET_LIST_PATH,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LISTING,
                result = "error",
                bucket = %bucket,
                prefix = %base_dir,
                reader_disks,
                fallback_disks = fallback_count,
                listing_quorum,
                elapsed_ms,
                error = %err,
                "Set list_path finished"
            ),
        }

        result.map_err(Error::from)
    }
}

fn get_list_quorum(quorum: &str, drive_count: i32) -> i32 {
    match quorum {
        "disk" => 1,
        "reduced" => 2,
        "optimal" => (drive_count + 1) / 2,
        "auto" => -1,
        _ => drive_count, // defaults to 'strict'
    }
}

fn get_quorum_disk_infos(disks: &[DiskStore], infos: &[DiskInfo], read_quorum: usize) -> (Vec<DiskStore>, Vec<DiskInfo>) {
    let common_mutations = calc_common_counter(infos, read_quorum);
    let mut new_disks = Vec::new();
    let mut new_infos = Vec::new();

    for (i, info) in infos.iter().enumerate() {
        let mutations = info.metrics.total_deletes + info.metrics.total_writes;
        if mutations >= common_mutations {
            new_disks.push(disks[i].clone());
            new_infos.push(infos[i].clone());
        }
    }

    (new_disks, new_infos)
}

fn get_quorum_disks(disks: &[DiskStore], infos: &[DiskInfo], read_quorum: usize) -> Vec<DiskStore> {
    let (new_disks, _) = get_quorum_disk_infos(disks, infos, read_quorum);
    new_disks
}

fn calc_common_counter(infos: &[DiskInfo], read_quorum: usize) -> u64 {
    let mut max = 0;
    let mut common_count = 0;
    let mut signature_map: HashMap<u64, usize> = HashMap::new();

    for info in infos {
        if !info.error.is_empty() {
            continue;
        }
        let mutations = info.metrics.total_deletes + info.metrics.total_writes;
        *signature_map.entry(mutations).or_insert(0) += 1;
    }

    for (&ops, &count) in &signature_map {
        if max < count && common_count < ops {
            max = count;
            common_count = ops;
        }
    }

    if max < read_quorum {
        return 0;
    }
    common_count
}

// list_path_raw

#[cfg(test)]
mod differential_tests;

#[cfg(test)]
mod lifecycle_exact_tests;

#[cfg(test)]
mod test {
    use super::{
        ENV_API_LIST_OBJECTS_QUORUM, GatherResultsState, ListPathOptions, ListSourceMode, MAX_OBJECT_LIST, RUSTFS_META_BUCKET,
        VersionMarker, gather_results, list_merged_entry_channel, list_metadata_resolution_params, list_objects_paginate,
        list_objects_quorum_from_env, max_keys_plus_one, merge_entry_channels, normalize_list_quorum,
        observe_scanner_namespace_mutations, parse_version_marker, recursive_walk_list_path_options,
        reset_scanner_namespace_mutation_generation_for_test, scanner_namespace_mutation_generation, send_or_cancel,
        version_marker_for_entries, walk_result_from_set_errors,
    };
    use crate::bucket::replication::ReplicationState;
    use crate::cache_value::metacache_set::ListPathRawOutcome;
    use crate::disk::{DeleteOptions, DiskAPI, STORAGE_FORMAT_FILE, endpoint::Endpoint, error::DiskError};
    use crate::error::{Result, StorageError};
    use crate::object_api::{ObjectInfo, ObjectOptions, PutObjReader};
    use crate::set_disk::{SetDisks, hermetic_set_disks_isolated};
    use crate::storage_api_contracts::namespace::NamespaceLocking as _;
    use crate::storage_api_contracts::object::ObjectIO as _;
    use bytes::Bytes;
    use http::HeaderMap;
    use rustfs_filemeta::{
        FileInfo, FileMeta, FileMetaVersion, MetaCacheEntries, MetaCacheEntriesSorted, MetaCacheEntry, MetaDeleteMarker,
        ObjectPartInfo, VersionType,
    };
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::io::AsyncReadExt as _;
    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;
    use tracing_subscriber::fmt::MakeWriter;

    #[test]
    fn recursive_walk_list_options_scope_scan_to_requested_prefix() {
        let options =
            recursive_walk_list_path_options("bucket".to_owned(), "photos/2026/000001.jpg", &super::WalkOptions::default());

        assert_eq!(options.base_dir, "photos/2026/");
        assert_eq!(options.filter_prefix.as_deref(), Some("000001.jpg"));

        let directory_options =
            recursive_walk_list_path_options("bucket".to_owned(), "photos/2026/", &super::WalkOptions::default());
        assert_eq!(directory_options.base_dir, "photos/2026/");
        assert_eq!(directory_options.filter_prefix, None);

        let object_options = recursive_walk_list_path_options("bucket".to_owned(), "object.jpg", &super::WalkOptions::default());
        assert_eq!(object_options.base_dir, "");
        assert_eq!(object_options.filter_prefix.as_deref(), Some("object.jpg"));
    }

    #[derive(Clone, Default)]
    struct CapturedLogs {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    struct CapturedLogWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl CapturedLogs {
        fn contents(&self) -> String {
            let buffer = self
                .buffer
                .lock()
                .expect("captured logs mutex should not be poisoned")
                .clone();
            String::from_utf8(buffer).expect("captured logs should be valid UTF-8")
        }
    }

    impl std::io::Write for CapturedLogWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.buffer
                .lock()
                .expect("captured logs mutex should not be poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedLogWriter {
                buffer: Arc::clone(&self.buffer),
            }
        }
    }
    use uuid::Uuid;

    fn test_meta_entry(name: &str) -> MetaCacheEntry {
        MetaCacheEntry {
            name: name.to_owned(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn list_merged_entry_channel_backpressures_after_one_head() {
        let (tx, mut rx) = list_merged_entry_channel();
        tx.try_send(test_meta_entry("object-a"))
            .expect("first merge head should fit in the input buffer");

        match tx.try_send(test_meta_entry("object-b")) {
            Err(tokio::sync::mpsc::error::TrySendError::Full(entry)) => assert_eq!(entry.name, "object-b"),
            other => panic!("second merge head should wait for the k-way merge refill, got {other:?}"),
        }

        assert_eq!(rx.recv().await.expect("first merge head should remain available").name, "object-a");
        tx.try_send(test_meta_entry("object-b"))
            .expect("input buffer should accept the next head after refill");
    }

    fn test_object_meta_entry(name: &str) -> MetaCacheEntry {
        let mut meta = FileMeta::new();
        let mut metadata = HashMap::new();
        metadata.insert("etag".to_string(), "etag".to_string());
        let mut fi = FileInfo::new(name, 2, 2);
        fi.erasure.index = 1;
        fi.data_dir = Some(Uuid::from_u128(0x1234));
        fi.volume = "bucket".to_owned();
        fi.name = name.to_owned();
        fi.size = 1;
        fi.parts = vec![ObjectPartInfo {
            number: 1,
            size: 1,
            actual_size: 1,
            ..Default::default()
        }];
        fi.mod_time = Some(time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp"));
        fi.metadata = metadata;
        meta.add_version(fi).expect("test metadata should accept object version");
        let metadata = meta.marshal_msg().expect("test metadata should marshal");

        MetaCacheEntry {
            name: name.to_owned(),
            metadata,
            cached: Some(meta),
            reusable: false,
        }
    }

    #[tokio::test]
    async fn walk_forwarder_skips_synthetic_directories_and_keeps_directory_objects() {
        for latest_only in [true, false] {
            let (entry_tx, entry_rx) = mpsc::channel(4);
            entry_tx
                .send(MetaCacheEntry {
                    name: "synthetic/".to_owned(),
                    ..Default::default()
                })
                .await
                .expect("queue synthetic directory");
            for name in ["marker/", "object"] {
                entry_tx.send(test_object_meta_entry(name)).await.expect("queue real object");
            }
            drop(entry_tx);
            let (result_tx, mut result_rx) = mpsc::channel(4);
            let parent = CancellationToken::new();
            let cancel = parent.child_token();
            let forwarder = super::spawn_walk_entry_forwarder(
                cancel.clone(),
                entry_rx,
                result_tx,
                super::WalkOptions {
                    latest_only,
                    ..Default::default()
                },
                "bucket".to_owned(),
            );
            timeout(Duration::from_secs(5), forwarder)
                .await
                .expect("finite walk should finish")
                .expect("forwarder should not panic");
            let mut names = Vec::new();
            while let Some(result) = result_rx.recv().await {
                assert!(result.err.is_none(), "directory projection must not produce a decoding error");
                names.push(result.item.expect("real object should be projected").name);
            }
            assert_eq!(names, ["marker/", "object"], "latest_only={latest_only}");
            assert!(cancel.is_cancelled(), "finished forwarder must stop its producers");
            assert!(!parent.is_cancelled(), "walk completion must not cancel its caller");
        }
    }

    #[tokio::test]
    async fn walk_forwarder_cancels_idle_producers_when_consumer_closes() {
        let (entry_tx, entry_rx) = mpsc::channel(1);
        let (result_tx, result_rx) = mpsc::channel(1);
        let parent = CancellationToken::new();
        let cancel = parent.child_token();
        let forwarder = super::spawn_walk_entry_forwarder(
            cancel.clone(),
            entry_rx,
            result_tx,
            super::WalkOptions {
                filter: Some(|_| false),
                ..Default::default()
            },
            "bucket".to_owned(),
        );
        drop(result_rx);
        timeout(Duration::from_secs(5), forwarder)
            .await
            .expect("consumer closure must stop the walk even without an emitted entry")
            .expect("forwarder should not panic");
        assert!(entry_tx.is_closed(), "producer channel must close without scanning more entries");
        assert!(cancel.is_cancelled(), "consumer closure must cancel disk scans");
        assert!(!parent.is_cancelled(), "consumer closure must not cancel its caller");
    }

    #[tokio::test]
    async fn walk_forwarder_cancellation_interrupts_output_backpressure() {
        let (entry_tx, entry_rx) = mpsc::channel(2);
        entry_tx
            .send(test_object_meta_entry("first"))
            .await
            .expect("queue first object");
        entry_tx
            .send(test_object_meta_entry("second"))
            .await
            .expect("queue second object");
        let (result_tx, mut result_rx) = mpsc::channel(1);
        let parent = CancellationToken::new();
        let cancel = parent.child_token();
        let forwarder = super::spawn_walk_entry_forwarder(
            cancel.clone(),
            entry_rx,
            result_tx,
            super::WalkOptions::default(),
            "bucket".to_owned(),
        );
        timeout(Duration::from_secs(5), async {
            while entry_tx.capacity() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("forwarder should read the second entry while its output is full");
        cancel.cancel();
        timeout(Duration::from_secs(5), forwarder)
            .await
            .expect("cancellation must interrupt a blocked output send")
            .expect("forwarder should not panic");
        assert_eq!(result_rx.recv().await.expect("first output").item.expect("first object").name, "first");
        assert!(
            result_rx.recv().await.is_none(),
            "second entry must not pass the full output after cancellation"
        );
        assert!(entry_tx.is_closed());
        assert!(!parent.is_cancelled());
    }

    #[tokio::test]
    async fn walk_forwarder_abort_before_poll_cancels_producers() {
        let (entry_tx, entry_rx) = mpsc::channel(1);
        let (result_tx, _result_rx) = mpsc::channel(1);
        let parent = CancellationToken::new();
        let cancel = parent.child_token();
        let forwarder = super::spawn_walk_entry_forwarder(
            cancel.clone(),
            entry_rx,
            result_tx,
            super::WalkOptions::default(),
            "bucket".to_owned(),
        );
        forwarder.abort();
        assert!(forwarder.await.expect_err("task should be aborted").is_cancelled());
        assert!(cancel.is_cancelled(), "unpolled task must retain the producer cancellation guard");
        assert!(entry_tx.is_closed());
        assert!(!parent.is_cancelled());
    }

    #[tokio::test]
    async fn walk_forwarder_reports_corruption_before_cancelling_producers() {
        let (entry_tx, entry_rx) = mpsc::channel(1);
        entry_tx
            .send(MetaCacheEntry {
                name: "corrupt".to_owned(),
                metadata: vec![0xff],
                ..Default::default()
            })
            .await
            .expect("queue corrupt metadata");
        let (result_tx, mut result_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let forwarder = super::spawn_walk_entry_forwarder(
            cancel.clone(),
            entry_rx,
            result_tx,
            super::WalkOptions::default(),
            "bucket".to_owned(),
        );
        timeout(Duration::from_secs(5), forwarder)
            .await
            .expect("corruption must terminate the walk")
            .expect("forwarder should not panic");
        let result = result_rx.recv().await.expect("corruption must be reported");
        assert!(result.item.is_none());
        assert!(result.err.is_some(), "corruption must not become a successful end of stream");
        assert!(result_rx.recv().await.is_none());
        assert!(cancel.is_cancelled());
        assert!(entry_tx.is_closed());
    }

    fn test_unversioned_object_meta_entry(
        name: &str,
        mod_time: time::OffsetDateTime,
        etag: &str,
        data_dir: Uuid,
    ) -> MetaCacheEntry {
        test_unversioned_object_meta_entry_with_erasure(name, mod_time, etag, data_dir, 2, 2)
    }

    fn test_unversioned_object_meta_entry_with_erasure(
        name: &str,
        mod_time: time::OffsetDateTime,
        etag: &str,
        data_dir: Uuid,
        data_blocks: usize,
        parity_blocks: usize,
    ) -> MetaCacheEntry {
        let mut meta = FileMeta::new();
        let mut fi = FileInfo::new(name, data_blocks, parity_blocks);
        fi.erasure.index = 1;
        fi.data_dir = Some(data_dir);
        fi.volume = "bucket".to_owned();
        fi.name = name.to_owned();
        fi.size = 1;
        fi.parts = vec![ObjectPartInfo {
            number: 1,
            size: 1,
            actual_size: 1,
            ..Default::default()
        }];
        fi.mod_time = Some(mod_time);
        fi.metadata.insert("etag".to_string(), etag.to_string());
        meta.add_version(fi).expect("test metadata should accept unversioned object");
        let metadata = meta.marshal_msg().expect("test metadata should marshal");

        MetaCacheEntry {
            name: name.to_owned(),
            metadata,
            cached: Some(meta),
            reusable: false,
        }
    }

    pub(super) fn test_pending_version_purge_meta_entry(bucket: &str, name: &str) -> (MetaCacheEntry, Uuid) {
        let version_id = Uuid::from_u128(0x1234_5678_9abc_def0_1234_5678_9abc_def0);
        let data_dir = Uuid::from_u128(0xabcd_ef12_3456_7890_abcd_ef12_3456_7890);
        let mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let mut meta = FileMeta::new();
        let mut fi = FileInfo::new(name, 2, 2);
        fi.erasure.index = 1;
        fi.data_dir = Some(data_dir);
        fi.volume = bucket.to_owned();
        fi.name = name.to_owned();
        fi.version_id = Some(version_id);
        fi.size = 1;
        fi.parts = vec![ObjectPartInfo {
            number: 1,
            size: 1,
            actual_size: 1,
            ..Default::default()
        }];
        fi.mod_time = Some(mod_time);
        fi.metadata.insert("etag".to_string(), "pending-purge-etag".to_string());

        meta.add_version(fi)
            .expect("test metadata should accept pending-purge object version");
        meta.delete_version(&FileInfo {
            volume: bucket.to_owned(),
            name: name.to_owned(),
            version_id: Some(version_id),
            replication_state_internal: Some(crate::bucket::replication::replication_state_to_filemeta(&ReplicationState {
                version_purge_status_internal: Some("arn:target-a=PENDING;".to_string()),
                purge_targets: crate::bucket::replication::version_purge_statuses_map("arn:target-a=PENDING;"),
                ..Default::default()
            })),
            ..Default::default()
        })
        .expect("version purge status should be persisted");
        let metadata = meta.marshal_msg().expect("test metadata should marshal");

        (
            MetaCacheEntry {
                name: name.to_owned(),
                metadata,
                cached: Some(meta),
                reusable: false,
            },
            data_dir,
        )
    }

    fn test_null_version_meta_entry(name: &str, mod_time: time::OffsetDateTime) -> MetaCacheEntry {
        let mut fi = FileInfo::new(name, 2, 2);
        fi.erasure.index = 1;
        fi.data_dir = Some(Uuid::from_u128(0x5678));
        fi.volume = "bucket".to_owned();
        fi.name = name.to_owned();
        fi.version_id = None;
        fi.versioned = false;
        fi.size = 1;
        fi.parts = vec![ObjectPartInfo {
            number: 1,
            size: 1,
            actual_size: 1,
            ..Default::default()
        }];
        fi.mod_time = Some(mod_time);
        fi.metadata.insert("etag".to_string(), "null-etag".to_string());

        let mut meta = FileMeta::new();
        meta.add_version(fi).expect("test metadata should accept null object version");
        let metadata = meta.marshal_msg().expect("test metadata should marshal");

        MetaCacheEntry {
            name: name.to_owned(),
            metadata,
            cached: Some(meta),
            reusable: false,
        }
    }

    fn test_object_meta_entry_with_erasure_versions(
        name: &str,
        versions: &[(time::OffsetDateTime, &str, usize, usize)],
    ) -> MetaCacheEntry {
        let mut meta = FileMeta::new();
        for (idx, (mod_time, etag, data_blocks, parity_blocks)) in versions.iter().enumerate() {
            let mut metadata = HashMap::new();
            metadata.insert("etag".to_string(), (*etag).to_string());

            let mut fi = FileInfo::new(name, *data_blocks, *parity_blocks);
            fi.erasure.index = 1;
            fi.data_dir = Some(Uuid::from_u128(0x1234));
            fi.volume = "bucket".to_owned();
            fi.name = name.to_owned();
            let version_idx = u128::try_from(idx + 1).expect("test version index should fit u128");
            fi.version_id = Some(Uuid::from_u128(version_idx));
            fi.versioned = true;
            fi.size = 1;
            fi.parts = vec![ObjectPartInfo {
                number: 1,
                size: 1,
                actual_size: 1,
                ..Default::default()
            }];
            fi.mod_time = Some(*mod_time);
            fi.metadata = metadata;

            meta.add_version(fi).expect("test metadata should accept object version");
        }
        let metadata = meta.marshal_msg().expect("test metadata should marshal");

        MetaCacheEntry {
            name: name.to_owned(),
            metadata,
            cached: Some(meta),
            reusable: false,
        }
    }

    fn test_delete_marker_meta_entry(name: &str, mod_time: time::OffsetDateTime) -> MetaCacheEntry {
        let mut meta = FileMeta::new();
        let delete_marker = FileMetaVersion {
            version_type: VersionType::Delete,
            object: None,
            delete_marker: Some(MetaDeleteMarker {
                version_id: Some(Uuid::from_u128(0x44444444555566667777888888888888)),
                mod_time: Some(mod_time),
                meta_sys: HashMap::new(),
            }),
            legacy_object: None,
            write_version: 1,
            uses_legacy_checksum: false,
        };
        meta.versions.push(
            delete_marker
                .try_into()
                .expect("test delete marker should convert to shallow version"),
        );
        let metadata = meta.marshal_msg().expect("test delete marker metadata should marshal");

        MetaCacheEntry {
            name: name.to_owned(),
            metadata,
            cached: Some(meta),
            reusable: false,
        }
    }

    fn test_transitioned_meta_entry(name: &str, remote_object: &str, delete_source: bool) -> MetaCacheEntry {
        let mut source = FileInfo::new(name, 2, 2);
        source.volume = "bucket".to_string();
        source.name = name.to_string();
        source.version_id = Some(Uuid::from_u128(1));
        source.versioned = true;
        source.size = 1;
        source.mod_time = Some(time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp"));
        source.transition_status = rustfs_filemeta::TRANSITION_COMPLETE.to_string();
        source.transition_tier = "WARM".to_string();
        source.transitioned_objname = remote_object.to_string();
        source.transition_version = Some("remote-version".to_string());
        source.transition_version_state = rustfs_filemeta::TransitionVersionState::Exact;
        rustfs_utils::http::metadata_compat::insert_str(
            &mut source.metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            "00".repeat(32),
        );

        let mut meta = FileMeta::new();
        meta.add_version(source.clone())
            .expect("test metadata should accept transitioned source");
        if delete_source {
            let mut delete = FileInfo {
                name: name.to_string(),
                version_id: source.version_id,
                ..Default::default()
            };
            delete.set_tier_free_version_id(&Uuid::from_u128(2).to_string());
            meta.delete_version(&delete)
                .expect("transitioned delete should create a free-version owner");
        }
        let metadata = meta.marshal_msg().expect("test transitioned metadata should marshal");
        MetaCacheEntry {
            name: name.to_string(),
            metadata,
            cached: Some(meta),
            reusable: false,
        }
    }

    fn test_object_with_delete_marker_meta_entry(
        name: &str,
        object_mod_time: time::OffsetDateTime,
        delete_mod_time: time::OffsetDateTime,
    ) -> MetaCacheEntry {
        let mut object = test_object_meta_entry_with_erasure_versions(name, &[(object_mod_time, "object-etag", 4, 2)]);
        let delete = test_delete_marker_meta_entry(name, delete_mod_time);
        let mut metadata = object.cached.take().expect("test object metadata should be cached");
        let delete_version = delete
            .cached
            .expect("test delete marker metadata should be cached")
            .versions
            .into_iter()
            .next()
            .expect("test delete marker should contain one version");
        metadata.versions.insert(0, delete_version);
        let encoded = metadata.marshal_msg().expect("test metadata should marshal");

        MetaCacheEntry {
            name: name.to_owned(),
            metadata: encoded,
            cached: Some(metadata),
            reusable: false,
        }
    }

    fn test_dir_meta_entry(name: &str) -> MetaCacheEntry {
        MetaCacheEntry {
            name: name.to_owned(),
            ..Default::default()
        }
    }

    fn sorted_entries(names: &[&str]) -> MetaCacheEntriesSorted {
        MetaCacheEntriesSorted {
            o: MetaCacheEntries(names.iter().map(|name| Some(test_meta_entry(name))).collect()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn list_path_gather_results_returns_after_limit_without_waiting_for_input_close() {
        let (entry_tx, entry_rx) = mpsc::channel(4);
        let (result_tx, mut result_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();

        entry_tx
            .send(test_meta_entry("obj-a"))
            .await
            .expect("test entry should be queued");

        let handle = tokio::spawn(gather_results(
            cancel.clone(),
            ListPathOptions {
                bucket: "bucket".to_owned(),
                limit: 1,
                incl_deleted: true,
                ..Default::default()
            },
            entry_rx,
            result_tx,
        ));

        let result = timeout(Duration::from_secs(1), result_rx.recv())
            .await
            .expect("limited result should be sent promptly")
            .expect("limited result should be present");
        assert_eq!(result.entries().len(), 1);

        let state = timeout(Duration::from_secs(1), handle)
            .await
            .expect("gather_results should finish after sending a limited result")
            .expect("gather_results task should not panic")
            .expect("gather_results should succeed");
        assert_eq!(state, GatherResultsState::LimitReached);
        assert!(cancel.is_cancelled());
    }

    #[tokio::test]
    async fn list_path_gather_results_keeps_marker_entry_for_version_marker_listing() {
        let (entry_tx, entry_rx) = mpsc::channel(4);
        let (result_tx, mut result_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();

        entry_tx
            .send(test_meta_entry("obj-a"))
            .await
            .expect("first test entry should be queued");
        entry_tx
            .send(test_meta_entry("obj-b"))
            .await
            .expect("second test entry should be queued");

        let handle = tokio::spawn(gather_results(
            cancel.clone(),
            ListPathOptions {
                bucket: "bucket".to_owned(),
                marker: Some("obj-a".to_owned()),
                include_marker: true,
                limit: 2,
                incl_deleted: true,
                versioned: true,
                ..Default::default()
            },
            entry_rx,
            result_tx,
        ));

        let result = timeout(Duration::from_secs(1), result_rx.recv())
            .await
            .expect("limited result should be sent promptly")
            .expect("limited result should be present");
        let entries = result;
        let names = entries
            .entries()
            .into_iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(names, ["obj-a", "obj-b"]);

        let state = timeout(Duration::from_secs(1), handle)
            .await
            .expect("gather_results should finish after sending a limited result")
            .expect("gather_results task should not panic")
            .expect("gather_results should succeed");
        assert_eq!(state, GatherResultsState::LimitReached);
        assert!(cancel.is_cancelled());
    }

    #[tokio::test]
    async fn list_path_gather_results_counts_common_prefixes_before_page_limit() {
        for separator in ["/", "-"] {
            for extra_object in [false, true] {
                let prefix_name = format!("a{separator}");
                let first = if separator == "/" {
                    prefix_name.clone()
                } else {
                    format!("{prefix_name}first")
                };
                let mut input = vec![test_object_meta_entry(&first)];
                if separator == "/" {
                    input.push(test_dir_meta_entry(&prefix_name));
                } else {
                    input.push(test_object_meta_entry(&format!("{prefix_name}second")));
                }
                input.push(test_object_meta_entry("b"));
                if extra_object {
                    input.push(test_object_meta_entry("c"));
                }
                let (entry_tx, entry_rx) = mpsc::channel(input.len());
                let (result_tx, mut result_rx) = mpsc::channel(1);
                for entry in input {
                    entry_tx.send(entry).await.expect("delimiter candidates should queue");
                }
                drop(entry_tx);
                let state = gather_results(
                    CancellationToken::new(),
                    ListPathOptions {
                        bucket: "bucket".to_owned(),
                        separator: Some(separator.to_owned()),
                        recursive: separator != "/",
                        include_directories: separator == "/",
                        limit: 3,
                        ..Default::default()
                    },
                    entry_rx,
                    result_tx,
                )
                .await
                .expect("delimiter collection should succeed");
                let result = result_rx.recv().await.expect("delimiter page should arrive");
                let mut expected = vec![first, "b".to_owned()];
                if extra_object {
                    expected.push("c".to_owned());
                }
                assert_eq!(
                    result
                        .entries()
                        .into_iter()
                        .map(|entry| entry.name.clone())
                        .collect::<Vec<_>>(),
                    expected,
                    "delimiter={separator}, extra={extra_object}"
                );
                assert_eq!(
                    state,
                    if extra_object {
                        GatherResultsState::LimitReached
                    } else {
                        GatherResultsState::InputClosed
                    },
                    "n=max must reach EOF; max+1 must retain lookahead"
                );
            }
        }
        for include_marker in [false, true] {
            let (entry_tx, entry_rx) = mpsc::channel(2);
            let (result_tx, mut result_rx) = mpsc::channel(1);
            for name in ["a/child", "b"] {
                entry_tx
                    .send(test_object_meta_entry(name))
                    .await
                    .expect("marker candidates should queue");
            }
            drop(entry_tx);
            gather_results(
                CancellationToken::new(),
                ListPathOptions {
                    bucket: "bucket".to_owned(),
                    separator: Some("/".to_owned()),
                    recursive: true,
                    include_directories: true,
                    marker: Some("a/".to_owned()),
                    include_marker,
                    limit: 3,
                    ..Default::default()
                },
                entry_rx,
                result_tx,
            )
            .await
            .expect("logical marker collection should succeed");
            let entries = result_rx.recv().await.expect("logical marker page should arrive");
            let projected = ObjectInfo::from_meta_cache_entries_sorted_infos(&entries, "bucket", "", Some("/".to_owned())).await;
            let names: Vec<_> = projected.into_iter().map(|object| object.name).collect();
            assert_eq!(
                names,
                if include_marker { vec!["a/", "b"] } else { vec!["b"] },
                "a folded prefix equal to the marker must follow include_marker={include_marker}"
            );
        }
    }

    #[test]
    fn list_versions_pagination_scan_limit_boundaries() {
        for has_version_marker in [false, true] {
            assert_eq!(super::list_versions_scan_limit(-1, has_version_marker), 0);
            assert_eq!(super::list_versions_scan_limit(0, has_version_marker), 0);
            let marker_slot = i32::from(has_version_marker);
            assert_eq!(super::list_versions_scan_limit(1, has_version_marker), 2 + marker_slot);
            assert_eq!(super::list_versions_scan_limit(MAX_OBJECT_LIST, has_version_marker), 1001 + marker_slot);
            assert_eq!(super::list_versions_scan_limit(i32::MAX, has_version_marker), 1001 + marker_slot);
        }
    }

    #[tokio::test]
    async fn list_versions_pagination_does_not_require_an_empty_final_page() {
        use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
        use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};

        let (dirs, store) = isolated_store_over_temp_disks().await;
        let bucket = "version-pagination-bucket";
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("pagination bucket should be created");
        let mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");

        for kind in ["objects", "deletes", "null", "mixed", "delimiter"] {
            let count = if kind == "mixed" { 5 } else { 10 };
            let mut expected = Vec::new();
            for index in 0..count {
                let name = if kind == "delimiter" && index % 2 == 1 {
                    format!("{kind}/testobject-{index:02}/child")
                } else {
                    format!("{kind}/testobject-{index:02}")
                };
                let entry = match kind {
                    "deletes" => test_delete_marker_meta_entry(&name, mod_time),
                    "null" => test_null_version_meta_entry(&name, mod_time),
                    "mixed" => test_object_with_delete_marker_meta_entry(&name, mod_time, mod_time + time::Duration::SECOND),
                    _ => test_object_meta_entry_with_erasure_versions(&name, &[(mod_time, "etag", 2, 2)]),
                };
                for dir in &dirs {
                    let object_dir = dir.path().join(bucket).join(&name);
                    tokio::fs::create_dir_all(&object_dir)
                        .await
                        .expect("pagination object directory should be created");
                    tokio::fs::write(object_dir.join(STORAGE_FORMAT_FILE), &entry.metadata)
                        .await
                        .expect("pagination metadata should be written");
                }
                if kind == "delimiter" && index % 2 == 1 {
                    expected.push((name.trim_end_matches("child").to_owned(), None, false));
                } else {
                    let versions = entry.file_info_versions(bucket).expect("fixture versions should decode");
                    expected.extend(
                        versions
                            .versions
                            .iter()
                            .map(|version| (name.clone(), version.version_id, version.deleted)),
                    );
                }
            }
            let prefix = format!("{kind}/");
            let delimiter = (kind == "delimiter").then(|| "/".to_owned());
            // Exercise each public/internal entry point with the reported page size.
            // The store entry point also covers exact and one-over limit boundaries.
            for (layer, max_keys) in [(0, 0), (0, 1), (0, 5), (0, 9), (0, 10), (0, 11), (3, 5)] {
                if layer == 3 && delimiter.is_some() {
                    continue;
                }
                let mut marker = None;
                let mut version_marker = None;
                let expected_pages = if max_keys == 0 {
                    1
                } else {
                    10usize.div_ceil(usize::try_from(max_keys).expect("positive page size"))
                };
                let mut actual = Vec::new();
                for page in 0..expected_pages {
                    let result = match layer {
                        0 => {
                            store
                                .clone()
                                .inner_list_object_versions(bucket, &prefix, marker, version_marker, delimiter.clone(), max_keys)
                                .await
                        }
                        _ => {
                            store.pools[0].disk_set[0]
                                .clone()
                                .inner_list_object_versions_for_recursive_delete(
                                    bucket,
                                    &prefix,
                                    marker,
                                    version_marker,
                                    max_keys,
                                )
                                .await
                        }
                    }
                    .expect("version page should list successfully");
                    let page_size = usize::try_from(max_keys).expect("nonnegative page size");
                    assert_eq!(
                        result.objects.len() + result.prefixes.len(),
                        (10 - page * page_size).min(page_size),
                        "{kind}, layer {layer}, max_keys {max_keys}, page {page}"
                    );
                    let has_more = page + 1 < expected_pages;
                    assert_eq!(result.is_truncated, has_more, "{kind}, layer {layer}, max_keys {max_keys}, page {page}");
                    assert_eq!(
                        result.next_marker.is_some(),
                        has_more,
                        "key marker must exist only when another page exists"
                    );
                    if !has_more {
                        assert!(
                            result.next_version_idmarker.is_none(),
                            "the final page must not advertise a version marker"
                        );
                    }
                    actual.extend(
                        result
                            .objects
                            .into_iter()
                            .map(|object| (object.name, object.version_id, object.delete_marker)),
                    );
                    actual.extend(result.prefixes.into_iter().map(|prefix| (prefix, None, false)));
                    marker = result.next_marker;
                    version_marker = result.next_version_idmarker;
                }
                // Objects and CommonPrefixes are serialized separately; compare their
                // identities without relying on their relative position in the response.
                actual.sort();
                let mut expected = if max_keys == 0 { Vec::new() } else { expected.clone() };
                expected.sort();
                assert_eq!(actual, expected, "{kind}, layer {layer}, max_keys {max_keys}");
            }
        }
    }

    #[test]
    fn version_marker_is_applied_only_when_key_marker_entry_is_present() {
        let version_marker = Some(VersionMarker::Null);

        let listed_after_deleted_marker = sorted_entries(&["obj-b", "obj-c"]);
        assert_eq!(
            version_marker_for_entries(&listed_after_deleted_marker, Some("obj-a"), version_marker),
            None
        );

        let listed_with_marker = sorted_entries(&["obj-a", "obj-b"]);
        assert_eq!(
            version_marker_for_entries(&listed_with_marker, Some("obj-a"), version_marker),
            version_marker
        );
    }

    #[tokio::test]
    async fn list_path_gather_results_skips_marker_entry_by_default() {
        let (entry_tx, entry_rx) = mpsc::channel(4);
        let (result_tx, mut result_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();

        entry_tx
            .send(test_meta_entry("obj-a"))
            .await
            .expect("first test entry should be queued");
        entry_tx
            .send(test_meta_entry("obj-b"))
            .await
            .expect("second test entry should be queued");
        drop(entry_tx);

        let handle = tokio::spawn(gather_results(
            cancel.clone(),
            ListPathOptions {
                bucket: "bucket".to_owned(),
                marker: Some("obj-a".to_owned()),
                limit: 2,
                incl_deleted: true,
                ..Default::default()
            },
            entry_rx,
            result_tx,
        ));

        let result = timeout(Duration::from_secs(1), result_rx.recv())
            .await
            .expect("eof result should be sent promptly")
            .expect("eof result should be present");
        let entries = result;
        let names = entries
            .entries()
            .into_iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(names, ["obj-b"]);

        let state = timeout(Duration::from_secs(1), handle)
            .await
            .expect("gather_results should finish after input closes")
            .expect("gather_results task should not panic")
            .expect("gather_results should succeed");
        assert_eq!(state, GatherResultsState::InputClosed);
        assert!(!cancel.is_cancelled());
    }

    #[tokio::test]
    async fn list_path_gather_results_filters_delete_marker_after_name_filters() {
        let (entry_tx, entry_rx) = mpsc::channel(4);
        let (result_tx, mut result_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_500).expect("valid timestamp");

        entry_tx
            .send(test_delete_marker_meta_entry("obj-a", mod_time))
            .await
            .expect("delete marker should be queued");
        entry_tx
            .send(test_object_meta_entry("obj-b"))
            .await
            .expect("object should be queued");
        drop(entry_tx);

        let handle = tokio::spawn(gather_results(
            cancel.clone(),
            ListPathOptions {
                bucket: "bucket".to_owned(),
                marker: Some("obj-a".to_owned()),
                limit: 2,
                incl_deleted: false,
                ..Default::default()
            },
            entry_rx,
            result_tx,
        ));

        let result = timeout(Duration::from_secs(1), result_rx.recv())
            .await
            .expect("result should be sent promptly")
            .expect("result should be present");
        let entries = result;
        let names = entries
            .entries()
            .into_iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(names, ["obj-b"]);

        let state = timeout(Duration::from_secs(1), handle)
            .await
            .expect("gather_results should finish")
            .expect("gather_results task should not panic")
            .expect("gather_results should succeed");
        assert_eq!(state, GatherResultsState::InputClosed);
        assert!(!cancel.is_cancelled());
    }

    #[tokio::test]
    async fn list_path_gather_results_skips_directory_delete_marker_by_default() {
        let (entry_tx, entry_rx) = mpsc::channel(4);
        let (result_tx, mut result_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_500).expect("valid timestamp");

        entry_tx
            .send(test_delete_marker_meta_entry("folder/", mod_time))
            .await
            .expect("directory delete marker should be queued");
        entry_tx
            .send(test_object_meta_entry("visible"))
            .await
            .expect("visible object should be queued");
        drop(entry_tx);

        let handle = tokio::spawn(gather_results(
            cancel.clone(),
            ListPathOptions {
                bucket: "bucket".to_owned(),
                separator: Some("/".to_owned()),
                include_directories: true,
                limit: 2,
                ..Default::default()
            },
            entry_rx,
            result_tx,
        ));

        let result = timeout(Duration::from_secs(1), result_rx.recv())
            .await
            .expect("eof result should be sent promptly")
            .expect("eof result should be present");
        let entries = result;
        let names = entries
            .entries()
            .into_iter()
            .map(|entry| entry.name.clone())
            .collect::<Vec<_>>();

        assert_eq!(names, ["visible".to_string()]);

        let state = timeout(Duration::from_secs(1), handle)
            .await
            .expect("gather_results should finish after input closes")
            .expect("gather_results task should not panic")
            .expect("gather_results should succeed");
        assert_eq!(state, GatherResultsState::InputClosed);
        assert!(!cancel.is_cancelled());
    }

    #[tokio::test]
    async fn list_path_gather_results_reports_consumer_gone_when_receiver_dropped_at_limit() {
        let (entry_tx, entry_rx) = mpsc::channel(4);
        let (result_tx, result_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();

        entry_tx
            .send(test_meta_entry("obj-a"))
            .await
            .expect("test entry should be queued");
        drop(result_rx);

        let state = timeout(
            Duration::from_secs(1),
            gather_results(
                cancel.clone(),
                ListPathOptions {
                    bucket: "bucket".to_owned(),
                    limit: 1,
                    incl_deleted: true,
                    ..Default::default()
                },
                entry_rx,
                result_tx,
            ),
        )
        .await
        .expect("gather_results should finish promptly when the consumer is gone")
        .expect("consumer disconnect must be a benign completion, not an error");

        assert_eq!(state, GatherResultsState::ConsumerGone);
        // The limit branch cancels upstream before attempting the send.
        assert!(cancel.is_cancelled());
    }

    #[tokio::test]
    async fn list_path_gather_results_reports_consumer_gone_when_receiver_dropped_at_eof() {
        let (entry_tx, entry_rx) = mpsc::channel(4);
        let (result_tx, result_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();

        entry_tx
            .send(test_meta_entry("obj-a"))
            .await
            .expect("test entry should be queued");
        drop(entry_tx);
        drop(result_rx);

        let state = timeout(
            Duration::from_secs(1),
            gather_results(
                cancel.clone(),
                ListPathOptions {
                    bucket: "bucket".to_owned(),
                    limit: 8,
                    incl_deleted: true,
                    ..Default::default()
                },
                entry_rx,
                result_tx,
            ),
        )
        .await
        .expect("gather_results should finish promptly when the consumer is gone")
        .expect("consumer disconnect must be a benign completion, not an error");

        assert_eq!(state, GatherResultsState::ConsumerGone);
        // The eof branch never cancels; the wrapper's ConsumerGone arm does.
        assert!(!cancel.is_cancelled());
    }

    /// A bounded producer can spend its whole per-disk budget on entries this
    /// page filters out. The closed input is then not the end of the namespace:
    /// the page must stay non-exhausted so the caller advertises a continuation.
    #[tokio::test]
    async fn list_path_bounded_producer_keeps_filtered_empty_page_non_exhausted() {
        for limit_reached in [true, false] {
            let (sender, recv) = mpsc::channel(4);
            let page = collect_list_path_results(
                CancellationToken::new(),
                ListPathOptions {
                    bucket: "bucket".to_owned(),
                    prefix: "requested/".to_owned(),
                    limit: 8,
                    incl_deleted: true,
                    ..Default::default()
                },
                recv,
                async move {
                    sender
                        .send(test_meta_entry("outside-prefix"))
                        .await
                        .expect("filtered test entry should be queued");
                    drop(sender);
                    Ok(ListPathRawOutcome { limit_reached })
                },
            )
            .await
            .expect("a fully filtered page is still a successful page");

            assert!(page.entries.entries().is_empty(), "the only entry lies outside the prefix");
            assert_eq!(
                page.exhausted, !limit_reached,
                "limit_reached={limit_reached}: a producer that stopped at its limit must not yield an exhausted page"
            );
        }
    }

    /// A-1 guard (rustfs/backlog#1306): pin that a *successful* send is never
    /// misclassified as `ConsumerGone`. With the receiver alive, gather_results
    /// must deliver the page and return the real drain state that
    /// `collect_list_path_results` turns into `ListPage::exhausted` (limit
    /// reached => the disk may hold more; input closed => everything offered
    /// was drained). Together with the receiver-dropped ConsumerGone tests
    /// above, this locks in "ConsumerGone is returned only on send failure": a
    /// broken send-failure check that classified a successful send as
    /// ConsumerGone would drop the delivered page and trip this test.
    #[tokio::test]
    async fn list_path_gather_results_send_success_returns_real_drain_state() {
        // Limit reached with the receiver alive: LimitReached.
        {
            let (entry_tx, entry_rx) = mpsc::channel(4);
            let (result_tx, mut result_rx) = mpsc::channel(1);
            let cancel = CancellationToken::new();
            entry_tx
                .send(test_meta_entry("obj-a"))
                .await
                .expect("test entry should be queued");

            let handle = tokio::spawn(gather_results(
                cancel.clone(),
                ListPathOptions {
                    bucket: "bucket".to_owned(),
                    limit: 1,
                    incl_deleted: true,
                    ..Default::default()
                },
                entry_rx,
                result_tx,
            ));

            let result = timeout(Duration::from_secs(1), result_rx.recv())
                .await
                .expect("limited result should be sent promptly")
                .expect("limited result should be present");
            assert_eq!(result.entries().len(), 1, "the limit page must be delivered");

            let state = timeout(Duration::from_secs(1), handle)
                .await
                .expect("gather_results should finish")
                .expect("gather_results task should not panic")
                .expect("a successful send must not surface as an error");
            assert_eq!(state, GatherResultsState::LimitReached);
        }

        // Input closed with the receiver alive: InputClosed.
        {
            let (entry_tx, entry_rx) = mpsc::channel(4);
            let (result_tx, mut result_rx) = mpsc::channel(1);
            let cancel = CancellationToken::new();
            entry_tx
                .send(test_meta_entry("obj-a"))
                .await
                .expect("test entry should be queued");
            drop(entry_tx);

            let handle = tokio::spawn(gather_results(
                cancel.clone(),
                ListPathOptions {
                    bucket: "bucket".to_owned(),
                    limit: 8,
                    incl_deleted: true,
                    ..Default::default()
                },
                entry_rx,
                result_tx,
            ));

            let result = timeout(Duration::from_secs(1), result_rx.recv())
                .await
                .expect("eof result should be sent promptly")
                .expect("eof result should be present");
            assert_eq!(result.entries().len(), 1, "the drained page must be delivered");

            let state = timeout(Duration::from_secs(1), handle)
                .await
                .expect("gather_results should finish")
                .expect("gather_results task should not panic")
                .expect("a successful send must not surface as an error");
            assert_eq!(state, GatherResultsState::InputClosed);
        }
    }

    // The subscriber installed via `set_default` is thread-local, so this test
    // must run on the current-thread runtime; a multi-thread runtime would make
    // the log-absence assertion vacuous.
    #[tokio::test(flavor = "current_thread")]
    async fn list_path_gather_results_consumer_gone_emits_no_worker_failure_log() {
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(logs.clone())
            .with_ansi(false)
            .without_time()
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let (entry_tx, entry_rx) = mpsc::channel(4);
        let (result_tx, result_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();

        entry_tx
            .send(test_meta_entry("obj-a"))
            .await
            .expect("test entry should be queued");
        drop(entry_tx);
        drop(result_rx);

        let state = timeout(
            Duration::from_secs(1),
            gather_results(
                cancel.clone(),
                ListPathOptions {
                    bucket: "bucket".to_owned(),
                    limit: 8,
                    incl_deleted: true,
                    ..Default::default()
                },
                entry_rx,
                result_tx,
            ),
        )
        .await
        .expect("gather_results should finish promptly when the consumer is gone")
        .expect("consumer disconnect must be a benign completion, not an error");
        assert_eq!(state, GatherResultsState::ConsumerGone);

        let captured = logs.contents();
        // The consumer disconnect still surfaces as a debug-level ops signal,
        // proving the subscriber captured output (non-vacuous assertion)...
        assert!(captured.contains("consumer disconnected"));
        // ...but it must never be logged as a worker failure. The wrappers'
        // ConsumerGone arm contains no log call, so a direct gather_results
        // invocation covers the only code path that could emit this string
        // for a consumer disconnect.
        assert!(!captured.contains("list_path worker failed"));
    }

    #[test]
    fn list_path_forward_past_is_idempotent_for_same_marker() {
        let mut first_page = sorted_entries(&["obj-0001", "obj-0002", "obj-0003", "obj-0004"]);
        first_page.forward_past(Some("obj-0002".to_string()));

        let first_page_names = first_page
            .entries()
            .into_iter()
            .map(|entry| entry.name.clone())
            .collect::<Vec<_>>();

        let mut second_page = sorted_entries(&["obj-0001", "obj-0002", "obj-0003", "obj-0004"]);
        second_page.forward_past(Some("obj-0002".to_string()));

        let second_page_names = second_page
            .entries()
            .into_iter()
            .map(|entry| entry.name.clone())
            .collect::<Vec<_>>();

        assert_eq!(first_page_names, second_page_names);
        assert_eq!(first_page_names, vec!["obj-0003".to_string(), "obj-0004".to_string()]);
    }

    #[test]
    fn list_path_forward_past_keeps_source_switch_page_boundary() {
        let mut walker_page_after_index = sorted_entries(&["obj-0001", "obj-0002", "obj-0003", "obj-0004"]);
        walker_page_after_index.forward_past(Some("obj-0002".to_string()));

        let page_names = walker_page_after_index
            .entries()
            .into_iter()
            .map(|entry| entry.name.clone())
            .collect::<Vec<_>>();

        assert_eq!(page_names, vec!["obj-0003".to_string(), "obj-0004".to_string()]);
    }

    #[test]
    fn list_path_parse_marker_replay_still_stable() {
        let marker = "photos/2026/image.jpg[rustfs_cache:v1,id:list-cache-id,p:3,s:7]".to_string();

        let mut parsed = ListPathOptions {
            marker: Some(marker),
            ..Default::default()
        };
        parsed.parse_marker();
        assert_eq!(parsed.marker.as_deref(), Some("photos/2026/image.jpg"));
        assert_eq!(parsed.id.as_deref(), Some("list-cache-id"));
        assert_eq!(parsed.pool_idx, Some(3));
        assert_eq!(parsed.set_idx, Some(7));

        // A 1.0.x server re-tagged the stripped key the same way on replay.
        let base_marker = parsed.marker.clone().expect("marker should parse");
        let replay_marker = format!("{base_marker}[rustfs_cache:v2,id:list-cache-id,p:3,s:7]");
        let mut replay = ListPathOptions {
            marker: Some(replay_marker),
            ..Default::default()
        };
        replay.parse_marker();

        assert_eq!(replay.marker.as_deref(), Some("photos/2026/image.jpg"));
        assert_eq!(replay.id.as_deref(), Some("list-cache-id"));
        assert_eq!(replay.pool_idx, Some(3));
        assert_eq!(replay.set_idx, Some(7));
        assert!(!replay.create);
    }

    // Folded common-prefix entry as produced by `from_meta_cache_entries_sorted_infos`:
    // `is_dir=true` with `mod_time=None`, which `list_objects_paginate` treats as a
    // CommonPrefix rather than an object.
    fn folded_prefix_object(name: &str) -> ObjectInfo {
        ObjectInfo {
            is_dir: true,
            name: name.to_owned(),
            ..Default::default()
        }
    }

    fn plain_object(name: &str) -> ObjectInfo {
        ObjectInfo {
            name: name.to_owned(),
            mod_time: Some(time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp")),
            ..Default::default()
        }
    }

    // Mimics the delimiter folding in `from_meta_cache_entries_sorted_infos`:
    // consecutive keys sharing the prefix up to (and including) the first delimiter
    // collapse into a single CommonPrefix entry.
    fn fold_delimiter_page(raw: &[String], prefix: &str, delimiter: &str) -> Vec<ObjectInfo> {
        let mut out = Vec::new();
        let mut prev_prefix = String::new();
        for name in raw {
            let remaining = name.strip_prefix(prefix).unwrap_or(name.as_str());
            if let Some(idx) = remaining.find(delimiter) {
                let cut = prefix.len() + idx + delimiter.len();
                let curr = name[..cut].to_string();
                if curr == prev_prefix {
                    continue;
                }
                prev_prefix = curr.clone();
                out.push(folded_prefix_object(&curr));
            } else {
                out.push(plain_object(name));
            }
        }
        out
    }

    // A truncated versions listing ending on a null version must advertise the
    // literal `null` continuation marker: a nil UUID parses to
    // `VersionMarker::Version(nil)`, which no stored version matches, so the
    // resumed page would replay every version (issue #6745).
    #[test]
    fn build_list_versions_next_marker_reports_null_for_nil_version_id() {
        let null_version = ObjectInfo {
            name: "obj-a".to_owned(),
            version_id: Some(uuid::Uuid::nil()),
            ..Default::default()
        };
        let real_version = ObjectInfo {
            name: "obj-b".to_owned(),
            version_id: Some(uuid::Uuid::from_u128(7)),
            ..Default::default()
        };

        let (next_marker, next_version_idmarker) = super::build_list_versions_next_marker(&[null_version], &[]);
        assert_eq!(next_marker.as_deref(), Some("obj-a"));
        assert_eq!(next_version_idmarker.as_deref(), Some("null"));

        let (_, next_version_idmarker) = super::build_list_versions_next_marker(&[real_version], &[]);
        assert_eq!(next_version_idmarker.as_deref(), Some(uuid::Uuid::from_u128(7).to_string().as_str()));
    }

    // The internal continuation marker is the plain last key. Earlier releases
    // appended a metacache tag that no server component consumed and that leaked
    // onto the wire as the ListObjectVersions NextKeyMarker.
    #[test]
    fn list_objects_paginate_next_marker_is_the_plain_last_key() {
        let get_objects = vec![plain_object("obj-0001"), plain_object("obj-0002"), plain_object("obj-0003")];

        let (objects, prefixes, is_truncated, next_marker, next_version_idmarker) =
            list_objects_paginate(get_objects, &None, 2, false, false, None);

        assert_eq!(objects.len(), 2);
        assert!(prefixes.is_empty());
        assert!(is_truncated);
        assert_eq!(next_marker.as_deref(), Some("obj-0002"));
        assert!(next_version_idmarker.is_none());
    }

    #[test]
    fn list_objects_paginate_versions_next_key_marker_is_the_plain_last_key() {
        let version = uuid::Uuid::from_u128(7);
        let mut get_objects = vec![plain_object("obj-0001"), plain_object("obj-0002"), plain_object("obj-0003")];
        get_objects[1].version_id = Some(version);

        let (_, _, is_truncated, next_marker, next_version_idmarker) =
            list_objects_paginate(get_objects, &None, 2, false, true, None);

        assert!(is_truncated);
        assert_eq!(next_marker.as_deref(), Some("obj-0002"));
        assert_eq!(next_version_idmarker.as_deref(), Some(version.to_string().as_str()));
    }

    #[test]
    fn list_objects_paginate_preserves_literal_cache_tag_boundaries() {
        for key in [
            "b[rustfs_cache:v1,return:]",
            "b[rustfs_cache:v2,id:x]suffix",
            "b[rustfs_cache:v2,id:x][rustfs_cache:v1,return:]",
            "b[rustfs_cache:future]",
        ] {
            for include_version_id in [false, true] {
                let (_, _, truncated, marker, _) =
                    list_objects_paginate(vec![plain_object(key), plain_object("zz")], &None, 1, false, include_version_id, None);
                assert!(truncated);
                let marker = marker.expect("truncated page needs a cursor");
                if include_version_id {
                    assert_eq!(marker, key, "version markers must be literal keys");
                    continue;
                }
                let mut opts = ListPathOptions {
                    marker: Some(marker),
                    ..Default::default()
                };
                opts.parse_marker();
                assert_eq!(opts.marker.as_deref(), Some(key), "the next page must start after the literal key");
            }
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn literal_cache_tag_cursors_advance_through_real_objects_and_versions() {
        use crate::storage_api_contracts::bucket::BucketOperations as _;
        use crate::storage_api_contracts::object::ObjectIO as _;

        let bucket = "literal-cache-tag-cursors";
        let keys = [
            "b0",
            "b1",
            "b2",
            "b[rustfs_cache:v1,return:]",
            "c[rustfs_cache:v2,id:x]suffix",
            "d[rustfs_cache:v2,id:x][rustfs_cache:v1,return:]",
            "zz",
        ];
        let (_dirs, store) = crate::bucket::metadata_sys::test_support::isolated_store_over_temp_disks().await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        store
            .make_bucket(bucket, &crate::storage_api_contracts::bucket::MakeBucketOptions::default())
            .await
            .expect("create bucket");
        for key in keys {
            store
                .put_object(
                    bucket,
                    key,
                    &mut crate::object_api::PutObjReader::from_vec(vec![1]),
                    &crate::object_api::ObjectOptions::default(),
                )
                .await
                .expect("seed literal object");
        }
        let mut marker = None;
        let mut v1_marker = None;
        let mut key_marker = None;
        let mut version_marker = None;
        for (index, key) in keys.into_iter().enumerate() {
            let page = store
                .clone()
                .inner_list_objects_v2(bucket, "", marker, None, 1, false, None, false)
                .await
                .expect("list next object");
            assert_eq!(page.objects.iter().map(|object| object.name.as_str()).collect::<Vec<_>>(), [key]);
            assert_eq!(page.is_truncated, index + 1 < keys.len());
            marker = page.next_continuation_token;

            // V1 sends the literal NextMarker back as StartAfter.
            let v1_page = store
                .clone()
                .inner_list_objects_v2(bucket, "", None, None, 1, false, v1_marker, false)
                .await
                .expect("list next object through the V1 marker path");
            assert_eq!(v1_page.objects.iter().map(|object| object.name.as_str()).collect::<Vec<_>>(), [key]);
            assert_eq!(v1_page.is_truncated, index + 1 < keys.len());
            v1_marker = v1_page.is_truncated.then(|| key.to_owned());

            let versions = store
                .clone()
                .inner_list_object_versions(bucket, "", key_marker, version_marker, None, 1)
                .await
                .expect("list next version");
            assert_eq!(versions.objects.iter().map(|object| object.name.as_str()).collect::<Vec<_>>(), [key]);
            assert_eq!(versions.is_truncated, index + 1 < keys.len());
            if versions.is_truncated {
                assert_eq!(versions.next_marker.as_deref(), Some(key), "NextKeyMarker must be the literal key");
            }
            key_marker = versions.next_marker;
            version_marker = versions.next_version_idmarker;
        }
        assert!(marker.is_none());
        assert!(v1_marker.is_none());
        assert!(key_marker.is_none());

        let literal = "b[rustfs_cache:v1,return:]";
        let after_literal = store
            .clone()
            .inner_list_objects_v2(bucket, "", None, None, 1, false, Some(literal.to_owned()), false)
            .await
            .expect("start after a caller-supplied literal key");
        assert_eq!(after_literal.objects[0].name, "c[rustfs_cache:v2,id:x]suffix");

        let resumed = store
            .clone()
            .inner_list_object_versions(bucket, "", Some(literal.to_owned()), None, None, 1)
            .await
            .expect("resume from a caller-supplied literal key marker");
        assert_eq!(resumed.objects[0].name, "c[rustfs_cache:v2,id:x]suffix");

        let resumed_version = store
            .clone()
            .inner_list_object_versions(bucket, "", Some(literal.to_owned()), Some("null".to_owned()), None, 1)
            .await
            .expect("resume after the literal key's null version");
        assert_eq!(resumed_version.objects[0].name, "c[rustfs_cache:v2,id:x]suffix");

        let recursive_delete_page = store.pools[0].disk_set[0]
            .clone()
            .inner_list_object_versions_for_recursive_delete(bucket, "", Some(literal.to_owned()), None, 1)
            .await
            .expect("resume recursive-delete validation from the literal key");
        assert_eq!(recursive_delete_page.objects[0].name, "c[rustfs_cache:v2,id:x]suffix");
    }

    #[tokio::test]
    async fn list_object_versions_next_key_marker_is_the_plain_last_key() {
        use crate::storage_api_contracts::bucket::BucketOperations as _;
        use crate::storage_api_contracts::object::ObjectIO as _;

        let bucket = "plain-key-marker";
        let (_dirs, store) = crate::bucket::metadata_sys::test_support::isolated_store_over_temp_disks().await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        store
            .make_bucket(bucket, &crate::storage_api_contracts::bucket::MakeBucketOptions::default())
            .await
            .expect("create bucket");
        for key in ["obj-a", "obj-b", "obj-c"] {
            store
                .put_object(
                    bucket,
                    key,
                    &mut crate::object_api::PutObjReader::from_vec(vec![1]),
                    &crate::object_api::ObjectOptions::default(),
                )
                .await
                .expect("seed object");
        }

        let versions = store
            .clone()
            .inner_list_object_versions(bucket, "", None, None, None, 2)
            .await
            .expect("list versions");
        assert!(versions.is_truncated);
        assert_eq!(versions.next_marker.as_deref(), Some("obj-b"));
        assert_eq!(versions.next_version_idmarker.as_deref(), Some("null"));

        let page = store
            .clone()
            .inner_list_objects_v2(bucket, "", None, None, 2, false, None, false)
            .await
            .expect("list objects");
        assert!(page.is_truncated);
        assert_eq!(page.next_continuation_token.as_deref(), Some("obj-b"));

        // A client that paginated across the upgrade still hands back the
        // marker with the legacy tag appended; the walker resumes after `obj-b`.
        let resumed = store
            .clone()
            .inner_list_objects_v2(
                bucket,
                "",
                Some("obj-b[rustfs_cache:v2,id:list-cache-id,src:walker,gen:live]".to_string()),
                None,
                2,
                false,
                None,
                false,
            )
            .await
            .expect("resume from a legacy tagged marker");
        assert_eq!(resumed.objects.iter().map(|o| o.name.as_str()).collect::<Vec<_>>(), vec!["obj-c"]);
        assert!(!resumed.is_truncated);
        assert!(resumed.next_continuation_token.is_none());
    }

    // ECA-03 / #944: a page whose raw keys fully collapse into fewer than max_keys
    // common prefixes must still report truncation and carry a continuation marker,
    // otherwise every key beyond the scan window is silently dropped.
    #[test]
    fn list_objects_paginate_delimiter_refold_reports_truncation_with_raw_marker() {
        let delimiter = Some("-".to_string());
        let get_objects = vec![folded_prefix_object("data-")];

        let (objects, prefixes, is_truncated, next_marker, _v) = list_objects_paginate(
            get_objects,
            &delimiter,
            1000,
            true, // disk_has_more: walker filled its raw candidate limit
            false,
            Some("data-1001"), // last RAW scanned key
        );

        assert!(objects.is_empty());
        assert_eq!(prefixes, vec!["data-".to_string()]);
        assert!(is_truncated, "re-folded page with more on disk must be truncated");
        // Continuation must be the last RAW key, not the folded prefix "data-"
        // (which is lexicographically < its own keys and would loop forever).
        assert_eq!(next_marker.as_deref(), Some("data-1001"));
    }

    // When the disk is actually exhausted, the re-fold guard must NOT fabricate a
    // marker even though a last scanned key is available.
    #[test]
    fn list_objects_paginate_delimiter_refold_no_marker_when_disk_exhausted() {
        let delimiter = Some("-".to_string());
        let get_objects = vec![folded_prefix_object("data-")];

        let (_objects, prefixes, is_truncated, next_marker, _v) =
            list_objects_paginate(get_objects, &delimiter, 1000, false, false, Some("data-1001"));

        assert_eq!(prefixes, vec!["data-".to_string()]);
        assert!(!is_truncated);
        assert!(next_marker.is_none());
    }

    // Regression guard: the new last-scanned-key parameter must not hijack the
    // no-delimiter path, which keeps using the last visible object as its marker.
    #[test]
    fn list_objects_paginate_no_delimiter_ignores_raw_scanned_key() {
        let get_objects = vec![plain_object("obj-0001"), plain_object("obj-0002")];

        let (objects, prefixes, is_truncated, next_marker, _v) =
            list_objects_paginate(get_objects, &None, 1000, true, false, Some("zzz-unrelated"));

        assert_eq!(objects.len(), 2);
        assert!(prefixes.is_empty());
        assert!(is_truncated);
        assert_eq!(next_marker.as_deref(), Some("obj-0002"));
    }

    // ECA-03 / #944 (versioned variant): a versions listing whose raw keys fully
    // collapse into fewer than max_keys common prefixes must still report
    // truncation and carry a continuation marker, otherwise every key and every
    // version past the scan window is silently dropped.
    #[test]
    fn list_objects_paginate_versioned_delimiter_refold_reports_truncation() {
        let delimiter = Some("-".to_string());
        let get_objects = vec![folded_prefix_object("data-")];

        let (objects, prefixes, is_truncated, next_marker, next_version_idmarker) = list_objects_paginate(
            get_objects,
            &delimiter,
            1000,
            true, // disk_has_more: walker filled its raw candidate limit
            true,
            Some("data-1001"), // last RAW scanned key
        );

        assert!(objects.is_empty());
        assert_eq!(prefixes, vec!["data-".to_string()]);
        assert!(is_truncated, "versioned re-folded page with more on disk must be truncated");
        // The marker must be the last RAW key, NOT the folded prefix "data-":
        // versioned walks are excluded from the gather-level common-prefix
        // collector, so a prefix marker could never advance `forward_past`.
        assert_eq!(next_marker.as_deref(), Some("data-1001"));
        assert!(next_version_idmarker.is_none(), "a folded prefix page carries no version marker");
    }

    // Same contract when the page is FILLED by folded common prefixes
    // (`visible_count >= max_keys`): the versioned path must still prefer the
    // raw scanned key over `prefixes.last()`, which would loop forever.
    #[test]
    fn list_objects_paginate_versioned_delimiter_full_prefix_page_uses_raw_marker() {
        let delimiter = Some("-".to_string());
        let get_objects = vec![folded_prefix_object("data-")];

        let (objects, prefixes, is_truncated, next_marker, next_version_idmarker) =
            list_objects_paginate(get_objects, &delimiter, 1, true, true, Some("data-0002"));

        assert!(objects.is_empty());
        assert_eq!(prefixes, vec!["data-".to_string()]);
        assert!(is_truncated, "a prefix-filled versioned page with more on disk must be truncated");
        assert_eq!(next_marker.as_deref(), Some("data-0002"));
        assert!(next_version_idmarker.is_none());
    }

    #[test]
    fn versioned_delimiter_boundaries_preserve_cache_shaped_raw_keys() {
        let delimiter = Some("-".to_owned());
        let raw_key = "data-0002[rustfs_cache:v2,return:]";
        let (_, _, truncated, marker, version_marker) =
            list_objects_paginate(vec![folded_prefix_object("data-")], &delimiter, 2, true, true, Some(raw_key));
        assert!(truncated);
        assert_eq!(marker.as_deref(), Some(raw_key));
        assert!(version_marker.is_none());

        let (_, _, truncated, marker, version_marker) = super::list_objects_paginate_versions(
            vec![folded_prefix_object("data-")],
            super::VersionedListPagination {
                delimiter: &delimiter,
                max_keys: 1,
                disk_has_more: true,
                marker: None,
                last_scanned_key: Some(raw_key),
                raw_keys: std::iter::once(raw_key),
            },
        );
        assert!(truncated);
        assert_eq!(marker.as_deref(), Some(raw_key));
        assert!(version_marker.is_none());
    }

    // End-to-end simulation of versioned pagination with `max_keys=1`, where the
    // next page resumes via a bare key marker (`forward_past` keeps `name > marker`
    // only, matching `inner_list_object_versions_with_projection` without a version
    // marker). Before the raw-key marker the same window replayed forever, so
    // `other-`/`zzz` were never listed.
    #[test]
    fn list_objects_paginate_versioned_delimiter_prefix_filled_pages_terminate() {
        let all: Vec<String> = [
            "data-0001",
            "data-0002",
            "data-0003",
            "data-0004",
            "data-0005",
            "other-0001",
            "zzz",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        let delimiter = Some("-".to_string());
        let max_keys = 1i32;
        let limit = max_keys_plus_one(max_keys, true) as usize;

        let mut marker: Option<String> = None;
        let mut seen_prefixes = Vec::new();
        let mut seen_objects = Vec::new();
        let mut page_prefixes = Vec::new();
        let mut pages = 0;

        loop {
            pages += 1;
            assert!(pages <= 16, "versioned pagination did not terminate (possible infinite loop)");

            let start = match &marker {
                Some(m) => all.partition_point(|k| k.as_str() <= m.as_str()),
                None => 0,
            };
            let window: Vec<String> = all[start..].iter().take(limit).cloned().collect();
            let disk_has_more = window.len() == limit;
            let last_scanned = window.last().cloned();

            let get_objects = fold_delimiter_page(&window, "", "-");
            let (objects, prefixes, is_truncated, next_marker, _v) = super::list_objects_paginate_versions(
                get_objects,
                super::VersionedListPagination {
                    delimiter: &delimiter,
                    max_keys,
                    disk_has_more,
                    marker: marker.as_deref(),
                    last_scanned_key: last_scanned.as_deref(),
                    raw_keys: window.iter().map(String::as_str),
                },
            );

            assert!(
                prefixes.windows(2).all(|pair| pair[0] != pair[1]),
                "a page must not contain duplicate common prefixes"
            );
            assert!(
                prefixes
                    .iter()
                    .all(|prefix| marker.as_deref().is_none_or(|marker| prefix.as_str() > marker)),
                "a resumed page must not repeat a prefix at or before its key marker"
            );
            page_prefixes.push(prefixes.clone());
            seen_objects.extend(objects.into_iter().map(|object| object.name));
            seen_prefixes.extend(prefixes);

            if !is_truncated {
                assert!(next_marker.is_none());
                break;
            }

            let next = next_marker.expect("truncated versioned page must carry a continuation marker");
            if let Some(prev) = &marker {
                assert!(
                    next.as_str() > prev.as_str(),
                    "next_marker must strictly advance to stay finite, got {next} after {prev}"
                );
            }
            marker = Some(next);
        }

        assert_eq!(
            page_prefixes,
            vec![vec!["data-".to_string()], vec![], vec!["other-".to_string()], vec![]],
            "versioned pagination must not repeat a folded prefix while scanning later raw keys"
        );
        assert_eq!(
            seen_prefixes,
            vec!["data-".to_string(), "other-".to_string()],
            "versioned pagination must preserve the ordered visible result sequence"
        );
        assert_eq!(seen_objects, vec!["zzz".to_string()]);
    }

    #[test]
    fn list_objects_paginate_versioned_prefix_boundary_uses_last_raw_key_in_returned_prefix() {
        let all: Vec<String> = ["alpha-0001", "beta-0001", "charlie-0001"]
            .into_iter()
            .map(str::to_string)
            .collect();
        let delimiter = Some("-".to_string());
        let max_keys = 1;
        let limit = max_keys_plus_one(max_keys, true) as usize;
        let mut marker: Option<String> = None;
        let mut page_prefixes = Vec::new();
        let mut pages = 0;

        loop {
            pages += 1;
            assert!(pages <= 8, "versioned prefix pagination did not terminate");

            let start = marker
                .as_deref()
                .map(|marker| all.partition_point(|key| key.as_str() <= marker))
                .unwrap_or(0);
            let window: Vec<String> = all[start..].iter().take(limit).cloned().collect();
            let disk_has_more = window.len() == limit;
            let last_scanned = window.last().cloned();

            let get_objects = fold_delimiter_page(&window, "", "-");
            let (objects, prefixes, is_truncated, next_marker, _) = super::list_objects_paginate_versions(
                get_objects,
                super::VersionedListPagination {
                    delimiter: &delimiter,
                    max_keys,
                    disk_has_more,
                    marker: marker.as_deref(),
                    last_scanned_key: last_scanned.as_deref(),
                    raw_keys: window.iter().map(String::as_str),
                },
            );

            assert_eq!(objects.len() + prefixes.len(), 1, "a page must respect max-keys");
            page_prefixes.push(prefixes);

            if !is_truncated {
                assert!(next_marker.is_none());
                break;
            }

            let next = next_marker.expect("truncated page must carry a continuation key marker");
            if let Some(previous) = &marker {
                assert!(next > *previous, "the continuation marker must strictly advance");
            }
            marker = Some(next);
        }

        assert_eq!(
            page_prefixes,
            vec![
                vec!["alpha-".to_string()],
                vec!["beta-".to_string()],
                vec!["charlie-".to_string()]
            ]
        );
    }

    #[test]
    fn versioned_marker_filter_does_not_change_no_delimiter_directories() {
        let mut objects = vec![folded_prefix_object("data-")];
        super::filter_versions_common_prefixes_after_marker(&mut objects, None, Some("z"));

        assert_eq!(objects.len(), 1);
        assert!(super::versioned_prefix_page_boundary_key(&objects, std::iter::once("data-"), None, 1, true).is_none());
    }

    // End-to-end pagination over 5000 `data-*` + 100 `other-*` with delimiter '-'.
    // Every full page re-folds to a single common prefix (< max_keys), the exact
    // trigger for ECA-03 / #944. Asserts pagination terminates in a bounded number
    // of pages (no infinite loop) and covers BOTH common prefixes (no data loss).
    #[test]
    fn list_objects_paginate_delimiter_refold_paginates_all_prefixes_finitely() {
        use std::collections::BTreeSet;

        let mut all: Vec<String> = Vec::new();
        for i in 1..=5000 {
            all.push(format!("data-{i:04}"));
        }
        for i in 1..=100 {
            all.push(format!("other-{i:04}"));
        }
        all.sort();

        let delimiter = Some("-".to_string());
        let max_keys = 1000i32;
        // Raw candidates the walker gathers per page (max_keys + 1 lookahead).
        let limit = max_keys_plus_one(max_keys, true) as usize;

        let mut marker: Option<String> = None;
        let mut seen_prefixes: BTreeSet<String> = BTreeSet::new();
        let mut pages = 0;

        loop {
            pages += 1;
            assert!(pages <= 64, "pagination did not terminate (possible infinite loop)");

            // Emulate the walker: skip keys <= marker (forward_past semantics), then
            // gather up to `limit` raw candidates.
            let start = match &marker {
                Some(m) => all.partition_point(|k| k.as_str() <= m.as_str()),
                None => 0,
            };
            let window: Vec<String> = all[start..].iter().take(limit).cloned().collect();
            // disk_has_more mirrors gather_results filling its +1 lookahead slot.
            let disk_has_more = window.len() == limit;
            let last_scanned = window.last().cloned();

            let get_objects = fold_delimiter_page(&window, "", "-");

            let (objects, prefixes, is_truncated, next_marker, _v) =
                list_objects_paginate(get_objects, &delimiter, max_keys, disk_has_more, false, last_scanned.as_deref());

            assert!(objects.is_empty(), "every key folds into a common prefix");
            for prefix in prefixes {
                seen_prefixes.insert(prefix);
            }

            if !is_truncated {
                assert!(next_marker.is_none());
                break;
            }

            let next = next_marker.expect("truncated page must carry a continuation marker");
            if let Some(prev) = &marker {
                assert!(next.as_str() > prev.as_str(), "next_marker must strictly advance to stay finite");
            }
            marker = Some(next);
        }

        let expected: BTreeSet<String> = ["data-".to_string(), "other-".to_string()].into_iter().collect();
        assert_eq!(seen_prefixes, expected, "pagination must cover every common prefix");
    }

    #[test]
    fn test_max_keys_plus_one_caps_before_lookahead() {
        assert_eq!(max_keys_plus_one(999, true), 1000);
        assert_eq!(max_keys_plus_one(MAX_OBJECT_LIST, true), MAX_OBJECT_LIST + 1);
        assert_eq!(max_keys_plus_one(MAX_OBJECT_LIST + 1, true), MAX_OBJECT_LIST + 1);
        assert_eq!(max_keys_plus_one(i32::MAX, true), MAX_OBJECT_LIST + 1);
        assert_eq!(max_keys_plus_one(-1, true), MAX_OBJECT_LIST + 1);
    }

    #[test]
    fn normalize_list_quorum_accepts_supported_values() {
        assert_eq!(normalize_list_quorum("strict"), "strict");
        assert_eq!(normalize_list_quorum("disk"), "disk");
        assert_eq!(normalize_list_quorum("reduced"), "reduced");
        assert_eq!(normalize_list_quorum("optimal"), "optimal");
        assert_eq!(normalize_list_quorum("auto"), "auto");
        assert_eq!(normalize_list_quorum(" OPTIMAL "), "optimal");
    }

    #[test]
    fn normalize_list_quorum_falls_back_to_strict() {
        assert_eq!(normalize_list_quorum(""), "strict");
        assert_eq!(normalize_list_quorum("unknown"), "strict");
    }

    #[test]
    #[serial_test::serial]
    fn list_objects_quorum_from_env_defaults_to_strict() {
        temp_env::with_var_unset(ENV_API_LIST_OBJECTS_QUORUM, || {
            assert_eq!(list_objects_quorum_from_env(), "strict");
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_objects_quorum_from_env_honors_supported_value() {
        // Use a non-default value so the assertion proves the variable is read.
        temp_env::with_var(ENV_API_LIST_OBJECTS_QUORUM, Some("optimal"), || {
            assert_eq!(list_objects_quorum_from_env(), "optimal");
        });
    }

    #[test]
    #[serial_test::serial]
    fn scanner_namespace_generation_tracks_only_user_namespace_mutations() {
        reset_scanner_namespace_mutation_generation_for_test();

        assert_eq!(scanner_namespace_mutation_generation(), 0);
        observe_scanner_namespace_mutations(RUSTFS_META_BUCKET, 3);
        assert_eq!(scanner_namespace_mutation_generation(), 0);

        observe_scanner_namespace_mutations("photos", 2);
        assert_eq!(scanner_namespace_mutation_generation(), 2);
    }

    #[tokio::test]
    async fn listing_preserves_valid_legacy_residue_until_explicit_delete() {
        use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
        use crate::storage_api_contracts::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};

        use crate::storage_api_contracts::object::{ObjectIO as _, ObjectOperations as _};

        let (dirs, store) = isolated_store_over_temp_disks().await;
        let bucket = "stage-e-exact-delete-legacy";
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created with authoritative metadata");
        let transaction = uuid::Uuid::new_v4();
        let mut backups = Vec::new();
        for object in ["repro/obj-00001.bin", "repro/obj-00002.bin"] {
            let mut reader = crate::object_api::PutObjReader::from_vec(vec![0x62; 1024 * 1024]);
            store
                .put_object(
                    bucket,
                    object,
                    &mut reader,
                    &crate::object_api::ObjectOptions {
                        write_completion: crate::object_api::WriteCompletion::TailDrained,
                        ..Default::default()
                    },
                )
                .await
                .expect("create genuine per-drive metadata and shards");
            for dir in &dirs {
                let object_dir = dir.path().join(bucket).join(object);
                let mut entries = tokio::fs::read_dir(&object_dir).await.expect("read actual object root");
                let mut marked = 0;
                while let Some(entry) = entries.next_entry().await.expect("read object entry") {
                    if uuid::Uuid::parse_str(&entry.file_name().to_string_lossy()).is_ok() {
                        tokio::fs::write(
                            entry
                                .path()
                                .join(format!("{}{}", crate::disk::local::DELETE_DATA_DIR_MARKER_PREFIX, transaction)),
                            [],
                        )
                        .await
                        .expect("mark actual data dir as historical delete residue");
                        marked += 1;
                    }
                }
                assert_eq!(marked, 1, "the real shard must have exactly one data directory");
                let backup = object_dir.join(transaction.to_string());
                tokio::fs::create_dir_all(&backup).await.expect("create legacy rollback root");
                tokio::fs::rename(
                    object_dir.join(crate::disk::STORAGE_FORMAT_FILE),
                    backup.join(crate::disk::STORAGE_FORMAT_FILE_BACKUP),
                )
                .await
                .expect("retain the actual valid metadata as a historical backup");
                let backup = backup.join(crate::disk::STORAGE_FORMAT_FILE_BACKUP);
                let bytes = tokio::fs::read(&backup).await.expect("read the historical backup");
                backups.push((backup, bytes));
            }
        }

        let delimited = store
            .clone()
            .list_objects_generic(bucket, "", None, Some("/".to_owned()), 1000, false)
            .await
            .expect("delimiter listing should succeed");
        assert!(delimited.objects.is_empty());
        assert!(delimited.prefixes.is_empty(), "residue must not appear as a common prefix");

        let result = store
            .clone()
            .list_objects_generic(bucket, "", None, None, 1000, false)
            .await
            .expect("recursive bucket listing should succeed");
        assert!(result.objects.is_empty());
        let exact_prefix = store
            .clone()
            .list_objects_generic(bucket, "repro/", None, None, 1000, false)
            .await
            .expect("recursive prefix listing should succeed");
        assert!(exact_prefix.objects.is_empty());
        for (path, bytes) in backups {
            assert_eq!(tokio::fs::read(path).await.expect("LIST must preserve backup"), bytes);
        }
        for dir in &dirs {
            assert!(
                dir.path().join(bucket).join("repro").exists(),
                "LIST must preserve the historical residue"
            );
        }
        for object in ["repro/obj-00001.bin", "repro/obj-00002.bin"] {
            store
                .delete_object(bucket, object, crate::object_api::ObjectOptions::default())
                .await
                .expect("fresh exact-key DELETE should reclaim eligible legacy residue");
            for dir in &dirs {
                assert!(!dir.path().join(bucket).join(object).exists(), "exact DELETE must reclaim the root");
            }
        }
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("the bucket is empty once the residue is reclaimed");
    }

    #[tokio::test]
    async fn empty_recursive_listing_preserves_committed_delete_residue() {
        use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
        use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};

        let (dirs, store) = isolated_store_over_temp_disks().await;
        let bucket = "listing-purge-bucket";
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created with authoritative metadata");
        let data_dir = uuid::Uuid::new_v4();
        let transaction = uuid::Uuid::new_v4();
        for dir in &dirs {
            let residue = dir
                .path()
                .join(bucket)
                .join("ghost")
                .join("nested")
                .join("object")
                .join(data_dir.to_string());
            tokio::fs::create_dir_all(&residue)
                .await
                .expect("committed delete residue should be created");
            tokio::fs::write(residue.join("part.1"), b"stale")
                .await
                .expect("stale part should be written");
            tokio::fs::write(
                residue.join(format!("{}{}", crate::disk::local::DELETE_DATA_DIR_MARKER_PREFIX, transaction)),
                [],
            )
            .await
            .expect("committed delete marker should be written");
        }

        let result = store
            .list_objects_generic(bucket, "ghost/", None, None, 1, false)
            .await
            .expect("empty recursive listing should succeed");

        assert!(result.objects.is_empty());
        assert!(result.prefixes.is_empty());
        for dir in &dirs {
            let residue = dir.path().join(bucket).join("ghost/nested/object").join(data_dir.to_string());
            assert_eq!(tokio::fs::read(residue.join("part.1")).await.expect("LIST preserves part"), b"stale");
            assert!(
                residue
                    .join(format!("{}{}", crate::disk::local::DELETE_DATA_DIR_MARKER_PREFIX, transaction))
                    .exists()
            );
        }
    }

    #[tokio::test]
    async fn large_preserved_residue_does_not_create_a_common_prefix() {
        use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
        use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};

        let (dirs, store) = isolated_store_over_temp_disks().await;
        let bucket = "large-listing-residue";
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket");
        for object in 0..20 {
            let data_dir = uuid::Uuid::new_v4();
            let transaction = uuid::Uuid::new_v4();
            for dir in &dirs {
                let residue = dir
                    .path()
                    .join(bucket)
                    .join(format!("ghost/{object:02}"))
                    .join(data_dir.to_string());
                tokio::fs::create_dir_all(&residue).await.expect("create residue");
                tokio::fs::write(residue.join("part.1"), b"stale")
                    .await
                    .expect("write residue part");
                tokio::fs::write(
                    residue.join(format!("{}{}", crate::disk::local::DELETE_DATA_DIR_MARKER_PREFIX, transaction)),
                    [],
                )
                .await
                .expect("write residue marker");
            }
        }

        let recursive = store
            .clone()
            .list_objects_generic(bucket, "", None, None, 1000, false)
            .await
            .expect("list recursively");
        assert!(recursive.objects.is_empty());
        let delimited = store
            .list_objects_generic(bucket, "", None, Some("/".to_owned()), 1000, false)
            .await
            .expect("list with delimiter");
        assert!(delimited.objects.is_empty());
        assert!(delimited.prefixes.is_empty(), "residue must not produce a common prefix");
        for dir in &dirs {
            assert!(dir.path().join(bucket).join("ghost/00").exists(), "listing must not remove residue");
        }
    }

    // A corrupt historical backup must survive LIST and a failed exact DELETE.
    #[tokio::test]
    async fn empty_recursive_listing_preserves_corrupt_delete_rollback_backups() {
        use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
        use crate::storage_api_contracts::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};
        use crate::storage_api_contracts::object::ObjectOperations as _;

        let (dirs, store) = isolated_store_over_temp_disks().await;
        let bucket = "listing-purge-rollback-backup";
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created with authoritative metadata");
        let transaction = uuid::Uuid::new_v4();
        for object in ["repro/obj-00001.bin", "repro/obj-00002.bin"] {
            let data_dir = uuid::Uuid::new_v4();
            for dir in &dirs {
                let object_dir = dir.path().join(bucket).join(object);
                let residue = object_dir.join(data_dir.to_string());
                tokio::fs::create_dir_all(&residue)
                    .await
                    .expect("committed data dir should be created");
                tokio::fs::write(residue.join("part.1"), b"stale")
                    .await
                    .expect("stale part should be written");
                tokio::fs::write(
                    residue.join(format!("{}{}", crate::disk::local::DELETE_DATA_DIR_MARKER_PREFIX, transaction)),
                    [],
                )
                .await
                .expect("committed delete marker should be written");
                let backup = object_dir.join(transaction.to_string());
                tokio::fs::create_dir_all(&backup)
                    .await
                    .expect("rollback dir should be created");
                tokio::fs::write(backup.join(crate::disk::STORAGE_FORMAT_FILE_BACKUP), b"rollback metadata")
                    .await
                    .expect("rollback backup should be written");
            }
        }

        let result = store
            .clone()
            .list_objects_generic(bucket, "", None, None, 1000, false)
            .await
            .expect("recursive bucket listing should succeed");
        assert!(result.objects.is_empty());
        for dir in &dirs {
            let backup = dir
                .path()
                .join(bucket)
                .join("repro/obj-00001.bin")
                .join(transaction.to_string())
                .join(crate::disk::STORAGE_FORMAT_FILE_BACKUP);
            assert_eq!(tokio::fs::read(backup).await.expect("LIST preserves backup"), b"rollback metadata");
        }
        let err = store
            .delete_object(bucket, "repro/obj-00001.bin", crate::object_api::ObjectOptions::default())
            .await
            .expect_err("corrupt metadata must not be reclaimed");
        assert!(matches!(err, StorageError::FileCorrupt));
        assert!(store.delete_bucket(bucket, &DeleteBucketOptions::default()).await.is_err());
        for dir in &dirs {
            let backup = dir
                .path()
                .join(bucket)
                .join("repro/obj-00001.bin")
                .join(transaction.to_string())
                .join(crate::disk::STORAGE_FORMAT_FILE_BACKUP);
            assert_eq!(
                tokio::fs::read(backup).await.expect("failed cleanup preserves backup"),
                b"rollback metadata"
            );
        }
    }

    #[tokio::test]
    async fn empty_recursive_bucket_listing_preserves_empty_directory_prefixes() {
        use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
        use crate::storage_api_contracts::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};

        let (dirs, store) = isolated_store_over_temp_disks().await;
        let bucket = "recursive-bucket-orphan-purge";
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created with authoritative metadata");

        for dir in &dirs {
            tokio::fs::create_dir_all(dir.path().join(bucket).join("ghost").join("nested").join("leaf"))
                .await
                .expect("metadata-less orphan directory tree should be created");
        }

        let result = store
            .clone()
            .list_objects_generic(bucket, "", None, None, 1000, false)
            .await
            .expect("recursive bucket listing should succeed");

        assert!(result.objects.is_empty(), "orphan directories are not S3 objects");
        assert!(result.prefixes.is_empty(), "a delimiter-less listing has no CommonPrefixes");
        for dir in &dirs {
            assert!(
                dir.path().join(bucket).join("ghost/nested/leaf").exists(),
                "LIST must preserve empty directories"
            );
        }
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("bucket deletion removes empty directories");

        let versioned_bucket = "recursive-bucket-versioned-orphan";
        store
            .make_bucket(versioned_bucket, &MakeBucketOptions::default())
            .await
            .expect("versioned test bucket should be created");
        store
            .update_bucket_metadata_config(
                versioned_bucket,
                crate::bucket::metadata::BUCKET_VERSIONING_CONFIG,
                b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec(),
            )
            .await
            .expect("bucket versioning should be enabled");
        for dir in &dirs {
            tokio::fs::create_dir_all(dir.path().join(versioned_bucket).join("ghost").join("nested").join("leaf"))
                .await
                .expect("versioned bucket orphan tree should be created");
        }

        store
            .clone()
            .list_objects_generic(versioned_bucket, "", None, None, 1000, false)
            .await
            .expect("recursive versioned bucket listing should succeed");
        for dir in &dirs {
            assert!(
                dir.path().join(versioned_bucket).join("ghost").exists(),
                "root recursive LIST must not purge orphan residue in a versioned bucket"
            );
        }
    }

    #[tokio::test]
    async fn list_objects_exact_prefix_paginates_across_storage_layers() {
        use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
        use crate::object_api::{ObjectOptions, PutObjReader};
        use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};
        use crate::storage_api_contracts::object::{ObjectIO as _, ObjectOperations as _};

        let (_dirs, store) = isolated_store_over_temp_disks().await;
        let bucket = "exact-prefix-pagination-bucket";
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("exact-prefix pagination bucket should be created");
        for name in ["a", "ab"] {
            store.pools[0]
                .put_object(
                    bucket,
                    name,
                    &mut PutObjReader::from_vec(b"pagination fixture".to_vec()),
                    &ObjectOptions {
                        no_lock: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("pagination fixture object should be written");
            let info = store
                .get_object_info(
                    bucket,
                    name,
                    &ObjectOptions {
                        no_lock: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("exact-prefix fixture must be readable by object lookup");
            assert!(!info.delete_marker && info.version_purge_status.is_empty());
        }

        for (prefix, expected) in [("a", vec!["a", "ab"]), ("ab", vec!["ab"]), ("missing", vec![])] {
            let mut marker = None;
            for page in 0..expected.len().max(1) {
                let result = store
                    .clone()
                    .list_objects_generic(bucket, prefix, marker.clone(), None, 1, false)
                    .await
                    .expect("exact-prefix page should list successfully");
                let names: Vec<_> = result.objects.iter().map(|object| object.name.as_str()).collect();
                let expected_page: Vec<_> = expected.get(page).copied().into_iter().collect();
                assert_eq!(names, expected_page, "prefix {prefix}, page {page}");
                assert!(result.prefixes.is_empty(), "recursive listing should not return common prefixes");
                let has_more = page + 1 < expected.len();
                assert_eq!(result.is_truncated, has_more, "prefix {prefix}, page {page}");
                assert_eq!(result.next_marker.is_some(), has_more, "only non-final pages should carry a marker");
                if has_more {
                    assert_ne!(result.next_marker, marker, "pagination marker must advance");
                }
                marker = result.next_marker;
            }
        }
    }

    #[tokio::test]
    async fn list_objects_hides_pending_version_purge_across_walk_and_exact_prefix() {
        use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
        use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};

        let (dirs, store) = isolated_store_over_temp_disks().await;
        let bucket = "pending-purge-listing-bucket";
        let object = "spilo/_permtest";
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created with authoritative metadata");

        let (entry, data_dir) = test_pending_version_purge_meta_entry(bucket, object);
        for dir in &dirs {
            let object_dir = dir.path().join(bucket).join(object);
            tokio::fs::create_dir_all(object_dir.join(data_dir.to_string()))
                .await
                .expect("pending-purge object data directory should be created");
            tokio::fs::write(object_dir.join(data_dir.to_string()).join("part.1"), b"x")
                .await
                .expect("pending-purge object part should be written");
            tokio::fs::write(object_dir.join(STORAGE_FORMAT_FILE), &entry.metadata)
                .await
                .expect("pending-purge metadata should be written");
        }

        let recursive = store
            .clone()
            .list_objects_generic(bucket, "", None, None, 1000, false)
            .await
            .expect("recursive listing should succeed");
        assert!(
            recursive.objects.is_empty(),
            "recursive ListObjectsV2 should hide pending version-purge entries"
        );
        assert!(
            recursive.prefixes.is_empty(),
            "recursive ListObjectsV2 should not synthesize prefixes from hidden entries"
        );

        let exact = store
            .list_objects_generic(bucket, object, None, None, 1, false)
            .await
            .expect("exact-prefix listing should succeed");
        assert!(
            exact.objects.is_empty(),
            "exact-prefix max_keys=1 listing should hide pending version-purge entries"
        );
        assert!(exact.prefixes.is_empty());
    }

    #[tokio::test]
    async fn empty_delimiter_listing_hides_and_preserves_committed_delete_residue() {
        use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
        use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};

        let (dirs, store) = isolated_store_over_temp_disks().await;
        let bucket = "listing-purge-delimiter-bucket";
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created with authoritative metadata");
        let data_dir = uuid::Uuid::new_v4();
        let transaction = uuid::Uuid::new_v4();
        for dir in &dirs {
            let residue = dir
                .path()
                .join(bucket)
                .join("metrics")
                .join("2026")
                .join("object.parquet")
                .join(data_dir.to_string());
            tokio::fs::create_dir_all(&residue)
                .await
                .expect("committed delete residue should be created");
            tokio::fs::write(residue.join("part.1"), b"stale")
                .await
                .expect("stale part should be written");
            tokio::fs::write(
                residue.join(format!("{}{}", crate::disk::local::DELETE_DATA_DIR_MARKER_PREFIX, transaction)),
                [],
            )
            .await
            .expect("committed delete marker should be written");
        }

        // The object directory holds only a deleted version's data dir, so a
        // console-style browse of its parent must not show it as a folder.
        let result = store
            .clone()
            .list_objects_generic(bucket, "metrics/2026/", None, Some("/".to_owned()), 1000, false)
            .await
            .expect("delimiter listing should succeed");
        assert!(result.objects.is_empty());
        assert!(result.prefixes.is_empty(), "delete residue must not surface as a prefix");
        for dir in &dirs {
            let residue = dir
                .path()
                .join(bucket)
                .join("metrics/2026/object.parquet")
                .join(data_dir.to_string());
            assert_eq!(tokio::fs::read(residue.join("part.1")).await.expect("LIST preserves part"), b"stale");
            assert!(
                residue
                    .join(format!("{}{}", crate::disk::local::DELETE_DATA_DIR_MARKER_PREFIX, transaction))
                    .exists()
            );
        }
    }

    #[tokio::test]
    async fn empty_delimiter_listing_of_residue_ancestor_hides_and_preserves_whole_tree() {
        use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
        use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};

        let (dirs, store) = isolated_store_over_temp_disks().await;
        let bucket = "listing-purge-ancestor-bucket";
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created with authoritative metadata");
        let data_dir = uuid::Uuid::new_v4();
        let transaction = uuid::Uuid::new_v4();
        for dir in &dirs {
            let residue = dir
                .path()
                .join(bucket)
                .join("metrics/kubelet/2026/08/28/23/74992556388248657933757.parquet")
                .join(data_dir.to_string());
            tokio::fs::create_dir_all(&residue)
                .await
                .expect("committed delete residue should be created");
            tokio::fs::write(residue.join("part.1"), b"stale")
                .await
                .expect("stale part should be written");
            tokio::fs::write(
                residue.join(format!("{}{}", crate::disk::local::DELETE_DATA_DIR_MARKER_PREFIX, transaction)),
                [],
            )
            .await
            .expect("committed delete marker should be written");
        }

        // Browsing an ancestor must not expose the deleted key's date folders.
        let result = store
            .clone()
            .list_objects_generic(bucket, "metrics/", None, Some("/".to_owned()), 1000, false)
            .await
            .expect("delimiter listing should succeed");
        assert!(result.objects.is_empty());
        assert!(result.prefixes.is_empty(), "ancestors of delete residue must not surface as prefixes");
        for dir in &dirs {
            let residue = dir
                .path()
                .join(bucket)
                .join("metrics/kubelet/2026/08/28/23/74992556388248657933757.parquet")
                .join(data_dir.to_string());
            assert_eq!(tokio::fs::read(residue.join("part.1")).await.expect("LIST preserves part"), b"stale");
            assert!(
                residue
                    .join(format!("{}{}", crate::disk::local::DELETE_DATA_DIR_MARKER_PREFIX, transaction))
                    .exists()
            );
        }
    }

    #[tokio::test]
    async fn empty_delimiter_listing_of_mixed_residue_ancestor_preserves_every_subtree() {
        use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
        use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};

        let (dirs, store) = isolated_store_over_temp_disks().await;
        let bucket = "listing-purge-mixed-bucket";
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created with authoritative metadata");
        let committed_dir = uuid::Uuid::new_v4();
        let unmarked_dir = uuid::Uuid::new_v4();
        let backup_dir = uuid::Uuid::new_v4();
        let transaction = uuid::Uuid::new_v4();
        for dir in &dirs {
            let committed = dir
                .path()
                .join(bucket)
                .join("metrics/cpu/2026/08/28/22/a.parquet")
                .join(committed_dir.to_string());
            tokio::fs::create_dir_all(&committed)
                .await
                .expect("committed delete residue should be created");
            tokio::fs::write(committed.join(crate::disk::STORAGE_FORMAT_FILE_BACKUP), b"rollback metadata")
                .await
                .expect("committed rollback backup should be written");
            tokio::fs::write(committed.join("part.1"), b"stale")
                .await
                .expect("stale part should be written");
            tokio::fs::write(
                committed.join(format!("{}{}", crate::disk::local::DELETE_DATA_DIR_MARKER_PREFIX, transaction)),
                [],
            )
            .await
            .expect("committed delete marker should be written");

            // Residue left by a build that never wrote delete markers, or a
            // PUT still streaming its parts: indistinguishable, so never purged.
            let unmarked = dir
                .path()
                .join(bucket)
                .join("metrics/kubelet/2026/08/28/23/74992556388248657933757.parquet")
                .join(unmarked_dir.to_string());
            tokio::fs::create_dir_all(&unmarked)
                .await
                .expect("unmarked residue should be created");
            tokio::fs::write(unmarked.join("part.1"), b"stale")
                .await
                .expect("stale part should be written");

            let backup = dir
                .path()
                .join(bucket)
                .join("metrics/statefulset/2026/08/26/18/object.parquet")
                .join(backup_dir.to_string());
            tokio::fs::create_dir_all(&backup)
                .await
                .expect("rollback backup directory should be created");
            tokio::fs::write(backup.join(crate::disk::STORAGE_FORMAT_FILE_BACKUP), b"rollback metadata")
                .await
                .expect("rollback backup should be written");
        }

        let result = store
            .clone()
            .list_objects_generic(bucket, "metrics/", None, Some("/".to_owned()), 1000, false)
            .await
            .expect("delimiter listing should succeed");
        assert!(result.objects.is_empty());
        assert!(result.prefixes.is_empty(), "residue subtrees may not surface as prefixes");
        for dir in &dirs {
            let metrics = dir.path().join(bucket).join("metrics");
            let committed = metrics.join("cpu/2026/08/28/22/a.parquet").join(committed_dir.to_string());
            assert_eq!(
                tokio::fs::read(committed.join("part.1"))
                    .await
                    .expect("LIST preserves committed part"),
                b"stale"
            );
            assert_eq!(
                tokio::fs::read(committed.join(crate::disk::STORAGE_FORMAT_FILE_BACKUP))
                    .await
                    .expect("LIST preserves backup"),
                b"rollback metadata"
            );
            assert!(
                metrics
                    .join("kubelet/2026/08/28/23/74992556388248657933757.parquet")
                    .join(unmarked_dir.to_string())
                    .join("part.1")
                    .exists(),
                "residue without a committed marker must survive the listing"
            );
            assert_eq!(
                tokio::fs::read(
                    metrics
                        .join("statefulset/2026/08/26/18/object.parquet")
                        .join(backup_dir.to_string())
                        .join(crate::disk::STORAGE_FORMAT_FILE_BACKUP)
                )
                .await
                .expect("uncommitted rollback backup must survive the listing"),
                b"rollback metadata"
            );
        }
    }

    #[test]
    fn list_metadata_resolution_params_limits_plain_listing_to_latest_version() {
        let resolver = list_metadata_resolution_params("bucket".to_string(), 3, false);

        assert_eq!(resolver.dir_quorum, 3);
        assert_eq!(resolver.obj_quorum, 3);
        assert_eq!(resolver.bucket, "bucket");
        assert_eq!(resolver.requested_versions, 1);
    }

    #[test]
    fn authoritative_version_scan_rejects_stale_markers_at_full_set_read_quorum() {
        let marker = test_delete_marker_meta_entry(
            "marker.bin",
            time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid fixture time"),
        );
        for copies in [1, 4, 7, 8, 9, 16] {
            let entries = MetaCacheEntries((0..16).map(|slot| (slot < copies).then(|| marker.clone())).collect());
            let resolved = SetDisks::resolve_listed_versions("bucket", entries, &vec![None; 16], 16, 4)
                .expect("known marker replicas and exact absence must be decidable");
            assert_eq!(resolved.is_some(), copies >= 8, "marker copies: {copies}");
        }
    }

    #[test]
    fn authoritative_version_scan_checks_history_after_a_quorum_marker() {
        let time = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid fixture time");
        let mut marker = test_delete_marker_meta_entry("history.bin", time + time::Duration::seconds(1));
        let history = test_object_meta_entry_with_erasure_versions("history.bin", &[(time, "old", 12, 4)]);
        marker
            .cached
            .as_mut()
            .expect("marker fixture")
            .versions
            .extend(history.cached.expect("history fixture").versions);
        marker.metadata = marker
            .cached
            .as_ref()
            .expect("combined fixture")
            .marshal_msg()
            .expect("encode history");
        for copies in [8, 11, 12, 16] {
            let entries = MetaCacheEntries((0..16).map(|slot| (slot < copies).then(|| marker.clone())).collect());
            let resolved = SetDisks::resolve_listed_versions("bucket", entries, &vec![None; 16], 16, 4)
                .expect("known history must resolve")
                .expect("marker has read quorum");
            let versions = resolved.file_info_versions("bucket").expect("read selected history").versions;
            assert!(versions[0].deleted, "marker must remain latest");
            assert_eq!(versions.len(), if copies >= 12 { 2 } else { 1 });
        }
    }

    #[test]
    fn authoritative_version_scan_preserves_metadata_uncertainty() {
        let marker = test_delete_marker_meta_entry(
            "marker.bin",
            time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid fixture time"),
        );
        let entries = MetaCacheEntries(vec![Some(marker); 4]);
        assert!(matches!(
            SetDisks::resolve_listed_versions("bucket", entries, &vec![None; 4], 16, 4),
            Err(DiskError::ErasureReadQuorum)
        ));
    }

    #[test]
    fn authoritative_version_scan_tolerates_corrupt_replicas_only_with_version_quorum() {
        let entry = test_object_meta_entry_with_erasure_versions(
            "readable.bin",
            &[(time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(), "current", 12, 4)],
        );
        let corrupt = MetaCacheEntry {
            name: entry.name.clone(),
            metadata: vec![0xff],
            ..Default::default()
        };
        for copies in [11, 12, 15] {
            let entries = MetaCacheEntries(
                (0..16)
                    .map(|slot| Some(if slot < copies { entry.clone() } else { corrupt.clone() }))
                    .collect(),
            );
            let resolved = SetDisks::resolve_listed_versions("bucket", entries, &vec![None; 16], 16, 4);
            if copies >= 12 {
                assert!(resolved.unwrap().is_some(), "a readable version must tolerate corrupt replicas");
            } else {
                assert!(resolved.is_err(), "corruption cannot establish the missing version's absence");
            }
        }
    }

    /// Writes `metadata(slot)` as `name`'s xl.meta on every slot that has one.
    async fn write_version_replicas(
        dirs: &[tempfile::TempDir],
        bucket: &str,
        name: &str,
        metadata: impl Fn(usize) -> Option<Vec<u8>>,
    ) {
        for (slot, dir) in dirs.iter().enumerate() {
            let bucket_dir = dir.path().join(bucket);
            tokio::fs::create_dir_all(&bucket_dir).await.expect("create bucket directory");
            let Some(metadata) = metadata(slot) else {
                continue;
            };
            let object_dir = bucket_dir.join(name);
            tokio::fs::create_dir_all(&object_dir).await.expect("create object directory");
            tokio::fs::write(object_dir.join(STORAGE_FORMAT_FILE), metadata)
                .await
                .expect("persist replica metadata");
        }
    }

    /// Version ids per key from a strict-mode version walk of `set`.
    async fn strict_version_listing(set: &SetDisks, bucket: &str) -> Vec<(String, Vec<Option<Uuid>>)> {
        let (sender, mut receiver) = mpsc::channel(1);
        let walk = set.list_path(
            CancellationToken::new(),
            ListPathOptions {
                bucket: bucket.to_owned(),
                versioned: true,
                incl_deleted: true,
                recursive: true,
                ask_disks: "strict".to_owned(),
                limit: 100,
                ..Default::default()
            },
            sender,
        );
        let drain = async {
            let mut entries = Vec::new();
            while let Some(entry) = receiver.recv().await {
                if !entry.is_dir() {
                    entries.push(entry);
                }
            }
            entries
        };
        let (result, entries) = timeout(Duration::from_secs(10), async { tokio::join!(walk, drain) })
            .await
            .expect("version listing must terminate");
        result.expect("sub-quorum replicas must not fail a version listing");
        entries
            .into_iter()
            .map(|entry| {
                let versions = entry.file_info_versions(bucket).expect("listed metadata decodes").versions;
                (entry.name, versions.into_iter().map(|version| version.version_id).collect())
            })
            .collect()
    }

    #[tokio::test]
    async fn version_listing_hides_stale_markers_below_strict_listing_quorum() {
        let bucket = "version-list-strict-markers";
        let (dirs, set) = crate::ecstore_validation_blackbox::make_local_set_disks(16, 4).await;
        let marker = test_delete_marker_meta_entry(
            "marker",
            time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid fixture time"),
        );
        for copies in [1, 4, 7, 8, 9, 16] {
            write_version_replicas(&dirs, bucket, &format!("marker-{copies:02}"), |slot| {
                (slot < copies).then(|| marker.metadata.clone())
            })
            .await;
        }
        let marker_id = Some(Uuid::from_u128(0x44444444555566667777888888888888));
        // Strict mode asks all sixteen drives, so a version needs eight copies.
        assert_eq!(
            strict_version_listing(&set, bucket).await,
            ["marker-08", "marker-09", "marker-16"].map(|name| (name.to_owned(), vec![marker_id]))
        );
    }

    #[tokio::test]
    async fn version_listing_checks_history_per_version_at_listing_quorum() {
        let bucket = "version-list-history";
        let (dirs, set) = crate::ecstore_validation_blackbox::make_local_set_disks(16, 4).await;
        let time = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid fixture time");
        let marker = test_delete_marker_meta_entry("history", time + time::Duration::seconds(1));
        let mut with_history = marker.clone();
        let history = test_object_meta_entry_with_erasure_versions("history", &[(time, "old", 12, 4)]);
        with_history
            .cached
            .as_mut()
            .expect("marker fixture")
            .versions
            .extend(history.cached.expect("history fixture").versions);
        with_history.metadata = with_history
            .cached
            .as_ref()
            .expect("combined fixture")
            .marshal_msg()
            .expect("encode history");
        // (marker copies, history copies), in listing order: each version
        // needs its own quorum.
        let cases = [(7, 7), (8, 8), (12, 11), (16, 7), (16, 8), (16, 16)];
        for (markers, histories) in cases {
            write_version_replicas(&dirs, bucket, &format!("history-{markers:02}-{histories:02}"), |slot| {
                if slot < histories {
                    Some(with_history.metadata.clone())
                } else {
                    (slot < markers).then(|| marker.metadata.clone())
                }
            })
            .await;
        }
        let marker_id = Some(Uuid::from_u128(0x44444444555566667777888888888888));
        let expected = cases
            .iter()
            .filter_map(|&(markers, histories)| {
                let versions = [(markers, marker_id), (histories, Some(Uuid::from_u128(1)))]
                    .into_iter()
                    .filter_map(|(copies, version)| (copies >= 8).then_some(version))
                    .collect::<Vec<_>>();
                (!versions.is_empty()).then(|| (format!("history-{markers:02}-{histories:02}"), versions))
            })
            .collect::<Vec<_>>();
        assert_eq!(strict_version_listing(&set, bucket).await, expected);
    }

    #[test]
    fn version_listing_tolerates_corrupt_replicas_at_listing_quorum() {
        let entry = test_object_meta_entry_with_erasure_versions(
            "readable.bin",
            &[(time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(), "current", 12, 4)],
        );
        let corrupt = MetaCacheEntry {
            name: entry.name.clone(),
            metadata: vec![0xff],
            ..Default::default()
        };
        // The resolver a strict version walk over sixteen drives uses.
        let resolver = rustfs_filemeta::MetadataResolutionParams {
            dir_quorum: 8,
            obj_quorum: 8,
            bucket: "bucket".to_owned(),
            ..Default::default()
        };
        for copies in [7, 8, 11, 12, 15] {
            let entries = MetaCacheEntries(
                (0..16)
                    .map(|slot| Some(if slot < copies { entry.clone() } else { corrupt.clone() }))
                    .collect(),
            );
            assert_eq!(entries.resolve(resolver.clone()).is_some(), copies >= 8, "readable copies: {copies}");
        }
    }

    #[tokio::test]
    async fn version_listing_does_not_expose_four_stale_marker_replicas() {
        let bucket = "version-list-stale-marker";
        let (dirs, set) = crate::ecstore_validation_blackbox::make_local_set_disks(16, 4).await;
        let marker = test_delete_marker_meta_entry(
            "marker.bin",
            time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid fixture time"),
        );
        let current = test_object_meta_entry_with_erasure_versions(
            "new.bin",
            &[(
                time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid fixture time")
                    + time::Duration::seconds(1),
                "new",
                12,
                4,
            )],
        );
        for (slot, dir) in dirs.iter().enumerate() {
            let current_dir = dir.path().join(bucket).join("new.bin");
            tokio::fs::create_dir_all(&current_dir)
                .await
                .expect("create current object directory");
            tokio::fs::write(current_dir.join("xl.meta"), &current.metadata)
                .await
                .expect("persist current version");
            if slot < 4 {
                let stale_dir = dir.path().join(bucket).join("marker.bin");
                tokio::fs::create_dir_all(&stale_dir)
                    .await
                    .expect("create stale marker directory");
                tokio::fs::write(stale_dir.join("xl.meta"), &marker.metadata)
                    .await
                    .expect("persist stale marker");
            }
        }
        // Sampled modes ask fewer drives and, as in MinIO, may list stale
        // replicas that happen to fill the sample.
        let listed = strict_version_listing(&set, bucket).await;
        assert_eq!(listed, [("new.bin".to_owned(), vec![Some(Uuid::from_u128(1))])]);
        for dir in dirs.iter().take(4) {
            assert_eq!(
                tokio::fs::read(dir.path().join(bucket).join("marker.bin/xl.meta"))
                    .await
                    .expect("listing must preserve unresolved physical marker"),
                marker.metadata
            );
        }
    }

    #[test]
    fn list_metadata_resolution_params_keeps_all_versions_for_version_listing() {
        let resolver = list_metadata_resolution_params("bucket".to_string(), 3, true);

        assert_eq!(resolver.dir_quorum, 3);
        assert_eq!(resolver.obj_quorum, 3);
        assert_eq!(resolver.bucket, "bucket");
        assert_eq!(resolver.requested_versions, 0);
    }

    /// Lists `bucket` on one set and returns each emitted object with the
    /// etag of the version it selected as latest (or `delete-marker`).
    async fn collect_listing_etags(
        set: &SetDisks,
        bucket: &str,
        mode: &str,
    ) -> (Result<ListPathRawOutcome>, Vec<(String, String)>) {
        let (sender, mut receiver) = mpsc::channel(1);
        let walk = set.list_path(
            CancellationToken::new(),
            ListPathOptions {
                bucket: bucket.to_owned(),
                ask_disks: mode.to_owned(),
                recursive: true,
                limit: 100,
                ..Default::default()
            },
            sender,
        );
        let drain = async {
            let mut listed = Vec::new();
            while let Some(entry) = receiver.recv().await {
                if entry.is_dir() {
                    continue;
                }
                let latest = match entry.to_fileinfo(bucket) {
                    Ok(info) if info.deleted => "delete-marker".to_owned(),
                    Ok(info) => info.metadata.get("etag").cloned().unwrap_or_default(),
                    Err(err) => format!("undecodable: {err}"),
                };
                listed.push((entry.name, latest));
            }
            listed
        };
        timeout(Duration::from_secs(10), async { tokio::join!(walk, drain) })
            .await
            .expect("native listing must terminate")
    }

    async fn single_set_store(drive_count: usize) -> (Vec<tempfile::TempDir>, Arc<crate::store::ECStore>) {
        let mut dirs = Vec::with_capacity(drive_count);
        let mut endpoints = Vec::with_capacity(drive_count);
        for disk_idx in 0..drive_count {
            let dir = tempfile::tempdir().expect("disk directory");
            let mut endpoint = Endpoint::try_from(dir.path().to_str().expect("disk path")).expect("disk endpoint");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(disk_idx);
            dirs.push(dir);
            endpoints.push(endpoint);
        }
        let endpoints = crate::layout::endpoints::EndpointServerPools(vec![crate::layout::endpoints::PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: drive_count,
            endpoints: crate::layout::endpoints::Endpoints::from(endpoints),
            cmd_line: "version-listing-quorum".into(),
            platform: "test".into(),
        }]);
        let ctx = Arc::new(crate::runtime::instance::InstanceContext::new());
        crate::store::init_local_disks_with_instance_ctx(&ctx, endpoints.clone())
            .await
            .expect("initialize disks");
        let store = crate::store::ECStore::new_with_instance_ctx(
            "127.0.0.1:0".parse().expect("test address"),
            endpoints,
            CancellationToken::new(),
            ctx,
        )
        .await
        .expect("initialize store");
        (dirs, store)
    }

    fn versioned_object_metadata(bucket: &str, name: &str, versions: &[(u128, i64)], slot: usize) -> Bytes {
        let mut meta = FileMeta::new();
        for (id, modified) in versions {
            let mut fi = FileInfo::new(name, 12, 4);
            fi.volume = bucket.to_owned();
            fi.name = name.to_owned();
            fi.version_id = Some(Uuid::from_u128(*id));
            fi.versioned = true;
            fi.erasure.index = slot + 1;
            fi.data_dir = Some(Uuid::from_u128(0x1000 + id));
            fi.mod_time = Some(time::OffsetDateTime::from_unix_timestamp(*modified).expect("fixture time"));
            fi.size = 1;
            fi.parts = vec![ObjectPartInfo {
                number: 1,
                size: 1,
                actual_size: 1,
                ..Default::default()
            }];
            fi.metadata.insert("etag".to_owned(), format!("etag-{id}"));
            fi.validate_for_metadata_read().expect("fixture version must be readable");
            meta.add_version(fi).expect("encode fixture version");
        }
        Bytes::from(meta.marshal_msg().expect("encode fixture xl.meta"))
    }

    // A versioned DeleteObject(versionId) can reach write quorum while three
    // drives are offline; they keep the deleted version after they return. A
    // different drive going offline later must not turn that sub-quorum
    // residue into a 503 for every ListObjectVersions page covering the key.
    #[tokio::test]
    #[serial_test::serial]
    async fn version_listing_omits_sub_quorum_versions_while_a_drive_is_offline() {
        temp_env::async_with_vars([(ENV_API_LIST_OBJECTS_QUORUM, Some("strict"))], async {
            let bucket = "version-list-offline-stale";
            let (_dirs, store) = single_set_store(16).await;
            let set = store.pools[0].disk_set[0].clone();
            assert_eq!((set.set_drive_count, set.default_parity_count), (16, 4));
            let committed: &[(u128, i64)] = &[(3, 1_700_000_003), (1, 1_700_000_001)];
            let stale: &[(u128, i64)] = &[(3, 1_700_000_003), (2, 1_700_000_002), (1, 1_700_000_001)];
            for (slot, disk) in set.disks.read().await.iter().enumerate() {
                let disk = disk.as_ref().expect("fixture drive is online");
                disk.make_volume(bucket).await.expect("create bucket volume");
                for (name, versions) in [
                    ("doc", if slot < 3 { stale } else { committed }),
                    ("other", &[(4, 1_700_000_004)][..]),
                ] {
                    disk.write_all(
                        bucket,
                        &format!("{name}/{STORAGE_FORMAT_FILE}"),
                        versioned_object_metadata(bucket, name, versions, slot),
                    )
                    .await
                    .expect("persist fixture metadata");
                }
            }
            let expected = [
                ("doc".to_owned(), Some(Uuid::from_u128(3))),
                ("doc".to_owned(), Some(Uuid::from_u128(1))),
                ("other".to_owned(), Some(Uuid::from_u128(4))),
            ];
            let list = || {
                let store = store.clone();
                async move {
                    store
                        .inner_list_object_versions(bucket, "", None, None, None, 1000)
                        .await
                        .map(|page| {
                            assert!(!page.is_truncated);
                            page.objects
                                .into_iter()
                                .map(|object| (object.name, object.version_id))
                                .collect::<Vec<_>>()
                        })
                }
            };

            assert_eq!(list().await.expect("all drives online"), expected);
            set.disks.write().await[15] = None;
            assert_eq!(
                list().await.expect("one offline drive must not fail the page"),
                expected,
                "the three stale copies are below listing quorum and must be omitted"
            );

            // Set-scoped scans back destructive decisions (tier reference
            // proofs, raw prefix deletes) and keep failing closed instead.
            let (sender, mut receiver) = mpsc::channel(16);
            let walk = set
                .clone()
                .walk(CancellationToken::new(), bucket, "", sender, super::WalkOptions::default());
            let drain = async { while receiver.recv().await.is_some() {} };
            let (walk, ()) = timeout(Duration::from_secs(10), async { tokio::join!(walk, drain) })
                .await
                .expect("set walk must terminate");
            assert!(matches!(walk, Err(StorageError::ErasureReadQuorum)), "{walk:?}");
            let recursive_delete_scan = set
                .clone()
                .inner_list_object_versions_for_recursive_delete(bucket, "", None, None, 1000)
                .await;
            assert!(
                matches!(recursive_delete_scan, Err(StorageError::InsufficientReadQuorum(..))),
                "{:?}",
                recursive_delete_scan.map(|page| page.objects.len())
            );

            {
                let mut disks = set.disks.write().await;
                for disk in disks.iter_mut().skip(8) {
                    *disk = None;
                }
            }
            assert_eq!(list().await.expect("half of the set still lists"), expected);
            set.disks.write().await[7] = None;
            assert!(
                matches!(list().await, Err(StorageError::InsufficientReadQuorum(..))),
                "fewer than half of the set's drives cannot serve a version page"
            );
        })
        .await;
    }

    async fn collect_native_listing(set: &SetDisks, bucket: &str, mode: &str) -> (Result<ListPathRawOutcome>, Vec<String>) {
        let (sender, mut receiver) = mpsc::channel(1);
        let walk = set.list_path(
            CancellationToken::new(),
            ListPathOptions {
                bucket: bucket.to_owned(),
                ask_disks: mode.to_owned(),
                recursive: true,
                limit: 100,
                ..Default::default()
            },
            sender,
        );
        let drain = async {
            let mut names = Vec::new();
            while let Some(entry) = receiver.recv().await {
                if !entry.is_dir() {
                    names.push(entry.name);
                }
            }
            names
        };
        timeout(Duration::from_secs(10), async { tokio::join!(walk, drain) })
            .await
            .expect("native listing must terminate")
    }

    #[tokio::test]
    async fn issue_8828_lists_ec8_at_half_of_a_default_ec4_set() {
        let bucket = "issue-8828";
        let (dirs, set) = crate::ecstore_validation_blackbox::make_local_set_disks(16, 4).await;
        let entry = test_object_meta_entry_with_erasure_versions(
            "readable.bin",
            &[(
                time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("fixture time"),
                "etag",
                8,
                8,
            )],
        );
        for (slot, dir) in dirs.iter().enumerate() {
            let path = dir.path().join(bucket);
            tokio::fs::create_dir_all(&path).await.expect("create bucket directory");
            if slot < 9 {
                let path = path.join(&entry.name);
                tokio::fs::create_dir_all(&path).await.expect("create object directory");
                tokio::fs::write(path.join(STORAGE_FORMAT_FILE), &entry.metadata)
                    .await
                    .expect("write metadata");
            }
        }
        // A legal EC8 commit can occupy nine drives even while all sixteen are online.
        for online in [16, 8, 7] {
            {
                let mut disks = set.disks.write().await;
                for disk in disks.iter_mut().skip(online) {
                    *disk = None;
                }
            }
            for mode in ["optimal", "strict"] {
                let (result, names) = collect_native_listing(&set, bucket, mode).await;
                if online == 16 && mode == "optimal" {
                    // `optimal` asks a random half of the drives and needs four of
                    // the nine copies in that sample, as MinIO's optimal mode does;
                    // about one sample in six holds fewer, so the key may be
                    // omitted. It must still never fail the page.
                    result.expect("a sampled listing must not fail the page");
                    assert!(names.is_empty() || names == ["readable.bin"], "online {online}, mode {mode}: {names:?}");
                } else if online >= 8 {
                    result.expect("EC8 committed metadata must remain listable");
                    assert_eq!(names, ["readable.bin"], "online {online}, mode {mode}");
                } else {
                    assert!(matches!(result, Err(StorageError::ErasureReadQuorum)), "mode {mode}: {result:?}");
                    assert!(names.is_empty(), "below read quorum must not publish objects");
                }
            }
        }
    }

    #[tokio::test]
    async fn issue_8828_lists_at_listing_quorum_without_failing_the_page() {
        // Eight of sixteen drives are online, so every mode asks those eight
        // and needs four copies. Readability is the read path's decision:
        // a key below its data quorum still lists, a stale minority stays
        // hidden, and neither outcome fails the page for its sibling.
        for (data, parity, copies, leading_free, listed) in [
            (12, 4, 8, false, true),
            (8, 8, 7, false, true),
            (12, 4, 8, true, true),
            (8, 8, 4, false, true),
            (8, 8, 3, false, false),
            (12, 4, 1, false, false),
        ] {
            let bucket = "issue-8828-unreadable";
            let (dirs, set) = crate::ecstore_validation_blackbox::make_local_set_disks(16, 4).await;
            let mut entry = test_object_meta_entry_with_erasure_versions(
                "unreadable.bin",
                &[(
                    time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("fixture time"),
                    "etag",
                    data,
                    parity,
                )],
            );
            if leading_free {
                let mut transitioned = test_transitioned_meta_entry(&entry.name, "remote-key", true);
                let mut metadata = transitioned.cached.take().expect("free-version fixture");
                metadata
                    .versions
                    .extend(entry.cached.take().expect("object fixture").versions);
                assert!(metadata.versions[0].header.free_version());
                entry.metadata = metadata.marshal_msg().expect("free and object metadata");
                entry.cached = None;
            }
            let sibling = test_object_meta_entry_with_erasure_versions(
                "sibling.bin",
                &[(
                    time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("fixture time"),
                    "sibling",
                    12,
                    4,
                )],
            );
            for (slot, dir) in dirs.iter().enumerate() {
                let path = dir.path().join(bucket);
                tokio::fs::create_dir_all(&path).await.expect("create bucket directory");
                let sibling_path = path.join(&sibling.name);
                tokio::fs::create_dir_all(&sibling_path)
                    .await
                    .expect("create sibling directory");
                tokio::fs::write(sibling_path.join(STORAGE_FORMAT_FILE), &sibling.metadata)
                    .await
                    .expect("write sibling metadata");
                if slot < copies {
                    let path = path.join(&entry.name);
                    tokio::fs::create_dir_all(&path).await.expect("create object directory");
                    tokio::fs::write(path.join(STORAGE_FORMAT_FILE), &entry.metadata)
                        .await
                        .expect("write metadata");
                }
            }
            {
                let mut disks = set.disks.write().await;
                for disk in disks.iter_mut().skip(8) {
                    *disk = None;
                }
            }
            for mode in ["optimal", "strict"] {
                let (result, names) = collect_native_listing(&set, bucket, mode).await;
                let context = format!("layout {data}+{parity}, copies {copies}, free {leading_free}, mode {mode}");
                result.unwrap_or_else(|err| panic!("{context}: one object must never fail the page: {err:?}"));
                let expected: &[&str] = if listed {
                    &["sibling.bin", "unreadable.bin"]
                } else {
                    &["sibling.bin"]
                };
                assert_eq!(names, expected, "{context}");
            }
        }
    }

    #[tokio::test]
    async fn issue_8828_collector_uses_the_latest_user_version_after_cleanup_records() {
        for kind in ["object", "delete-marker", "free-only"] {
            let bucket = "issue-8828-cleanup";
            let (dirs, set) = crate::ecstore_validation_blackbox::make_local_set_disks(16, 4).await;
            let mut entry = test_transitioned_meta_entry("key", "remote-key", true);
            let metadata = entry.cached.as_mut().expect("cleanup fixture");
            assert!(metadata.versions[0].header.free_version());
            let time = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("older user version");
            match kind {
                "object" => metadata.versions.extend(
                    test_object_meta_entry_with_erasure_versions("key", &[(time, "etag", 8, 8)])
                        .cached
                        .expect("object fixture")
                        .versions,
                ),
                "delete-marker" => metadata.versions.extend(
                    test_delete_marker_meta_entry("key", time)
                        .cached
                        .expect("delete marker fixture")
                        .versions,
                ),
                _ => {}
            }
            entry.metadata = metadata.marshal_msg().expect("cleanup and user metadata");
            for cached in [true, false] {
                for legacy in [false, true] {
                    let mut candidate = entry.clone();
                    if !cached {
                        candidate.cached = None;
                    }
                    if legacy {
                        candidate.metadata[6..8].copy_from_slice(&2u16.to_le_bytes());
                    }
                    assert_eq!(
                        candidate.is_latest_delete_marker(),
                        kind != "object",
                        "{kind}, cached={cached}, legacy={legacy}"
                    );
                }
            }
            for dir in &dirs {
                let path = dir.path().join(bucket).join(&entry.name);
                tokio::fs::create_dir_all(&path).await.expect("create metadata directory");
                tokio::fs::write(path.join(STORAGE_FORMAT_FILE), &entry.metadata)
                    .await
                    .expect("write cleanup history");
            }
            {
                let mut disks = set.disks.write().await;
                for disk in disks.iter_mut().skip(8) {
                    *disk = None;
                }
            }
            let result = set
                .clone()
                .list_path_result(&ListPathOptions {
                    bucket: bucket.to_owned(),
                    recursive: true,
                    ask_disks: "strict".to_owned(),
                    limit: 100,
                    ..Default::default()
                })
                .await
                .expect("collector should finish");
            assert!(result.exhausted, "a complete metacache page is reported as exhausted");
            let names: Vec<_> = result
                .entries
                .o
                .0
                .into_iter()
                .flatten()
                .filter(|entry| !entry.is_dir())
                .map(|entry| entry.name)
                .collect();
            if kind == "object" {
                assert_eq!(names, ["key"], "cleanup records must not hide a readable user version");
            } else {
                assert!(names.is_empty(), "real deletion and cleanup-only records remain hidden");
            }
        }
    }

    #[tokio::test]
    async fn latest_listing_resolves_unversioned_two_by_two_split_to_the_newer_version() {
        let bucket = "list-two-by-two-overwrite";
        let object = "object";
        let old_mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let new_mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("valid timestamp");
        let old = test_unversioned_object_meta_entry(object, old_mod_time, "old-etag", Uuid::from_u128(1));
        let new = test_unversioned_object_meta_entry(object, new_mod_time, "new-etag", Uuid::from_u128(2));
        let (_temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        for (index, disk) in disks.iter().enumerate() {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
            let metadata = if index < 2 { &old.metadata } else { &new.metadata };
            disk.write_all(bucket, &format!("{object}/{STORAGE_FORMAT_FILE}"), Bytes::copy_from_slice(metadata))
                .await
                .expect("split metadata should be written");
        }

        for mode in ["optimal", "strict"] {
            let (split_result, split_entries) = collect_listing_etags(&set, bucket, mode).await;
            split_result.expect("a 2+2 split must not fail the listing");
            assert_eq!(
                split_entries,
                [(object.to_string(), "new-etag".to_string())],
                "{mode}: a tie lists the newer generation, as GET serves it"
            );
        }

        disks[0]
            .write_all(bucket, &format!("{object}/{STORAGE_FORMAT_FILE}"), Bytes::copy_from_slice(&new.metadata))
            .await
            .expect("third new-generation copy should be written");
        let (committed_result, committed_entries) = collect_listing_etags(&set, bucket, "optimal").await;
        committed_result.expect("a 3+1 committed generation should list successfully");
        assert_eq!(committed_entries, [(object.to_string(), "new-etag".to_string())]);

        for disk in disks.iter().skip(1) {
            disk.delete(
                bucket,
                object,
                DeleteOptions {
                    recursive: true,
                    immediate: true,
                    ..Default::default()
                },
            )
            .await
            .expect("quorum deletion should remove the object copy");
        }
        let (deleted_result, deleted_entries) = collect_listing_etags(&set, bucket, "optimal").await;
        deleted_result.expect("a quorum-deleted object should remain a successful empty listing");
        assert!(deleted_entries.is_empty(), "one stale copy must not resurrect a quorum-deleted object");
    }

    async fn write_listing_metadata(
        dirs: &[tempfile::TempDir],
        bucket: &str,
        entry: &MetaCacheEntry,
        slots: impl IntoIterator<Item = usize>,
    ) {
        for slot in slots {
            let path = dirs[slot].path().join(bucket).join(&entry.name);
            tokio::fs::create_dir_all(&path).await.expect("create object directory");
            tokio::fs::write(path.join(STORAGE_FORMAT_FILE), &entry.metadata)
                .await
                .expect("write object metadata");
        }
    }

    #[tokio::test]
    async fn latest_listing_torn_write_visibility_follows_listing_quorum() {
        // A torn write leaves the new generation on `copies` drives. The newest
        // version carried by `ceil(asked/2)` asked drives is listed; otherwise a
        // previous generation that reaches the quorum stays latest, and a key
        // that reaches no quorum is omitted. The object's own erasure geometry
        // never changes the rule, and no outcome fails the page.
        let bucket = "torn-write-visibility";
        let old_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let new_time = old_time + time::Duration::seconds(100);
        for (drives, parity) in [(4_usize, 2_usize), (16, 8), (16, 4)] {
            let data = drives - parity;
            let write_quorum = if data == parity { data + 1 } else { data };
            let half = drives.div_ceil(2);
            let mut copy_counts = vec![write_quorum - 1, write_quorum, half - 1, half];
            copy_counts.sort_unstable();
            copy_counts.dedup();

            let (dirs, set) = crate::ecstore_validation_blackbox::make_local_set_disks(drives, parity).await;
            for dir in &dirs {
                tokio::fs::create_dir_all(dir.path().join(bucket))
                    .await
                    .expect("create bucket directory");
            }
            let mut cases = Vec::new();
            for &copies in &copy_counts {
                for overwrite in [false, true] {
                    let name = format!("{}-{copies:02}", if overwrite { "overwrite" } else { "new" });
                    let new =
                        test_unversioned_object_meta_entry_with_erasure(&name, new_time, "new", Uuid::from_u128(2), data, parity);
                    write_listing_metadata(&dirs, bucket, &new, 0..copies).await;
                    if overwrite {
                        let old = test_unversioned_object_meta_entry_with_erasure(
                            &name,
                            old_time,
                            "old",
                            Uuid::from_u128(1),
                            data,
                            parity,
                        );
                        write_listing_metadata(&dirs, bucket, &old, copies..drives).await;
                    }
                    cases.push((name, copies, overwrite));
                }
            }
            cases.sort();

            for online in [drives, half] {
                let offline: Vec<_> = {
                    let mut disks = set.disks.write().await;
                    disks.iter_mut().skip(online).map(Option::take).collect()
                };
                let quorum = online.div_ceil(2);
                let expected: Vec<(String, String)> = cases
                    .iter()
                    .filter_map(|(name, copies, overwrite)| {
                        let new_copies = (*copies).min(online);
                        let old_copies = if *overwrite { online - new_copies } else { 0 };
                        if new_copies >= quorum {
                            Some((name.clone(), "new".to_owned()))
                        } else if old_copies >= quorum {
                            Some((name.clone(), "old".to_owned()))
                        } else {
                            None
                        }
                    })
                    .collect();
                // A sampling mode is deterministic only when it asks every online drive.
                let modes: &[&str] = if drives == 4 || online == half {
                    &["strict", "optimal"]
                } else {
                    &["strict"]
                };
                for mode in modes {
                    let context = format!("{drives} drives EC {data}+{parity}, {online} online, {mode}");
                    let (result, listed) = collect_listing_etags(&set, bucket, mode).await;
                    result.unwrap_or_else(|err| panic!("{context}: a torn write must not fail the page: {err:?}"));
                    assert_eq!(listed, expected, "{context}");
                }
                let mut disks = set.disks.write().await;
                for (slot, disk) in disks.iter_mut().skip(online).zip(offline) {
                    *slot = disk;
                }
            }
        }
    }

    #[tokio::test]
    async fn latest_get_follows_read_quorum_on_torn_writes() {
        use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};
        use crate::storage_api_contracts::object::ObjectOperations as _;

        // HEAD and GET serve the version with the most copies once it reaches
        // the object's data blocks, and a tie goes to the newer version, as in
        // MinIO's getObjectFileInfo and as heal converges the set. GET used to
        // demand write quorum whenever enough metadata answered, so a 2+2 or
        // 8+8 split, or a newer side that could still hold write quorum
        // through an offline drive, listed but answered 503.
        let opts = ObjectOptions {
            suppress_read_repair: true,
            ..Default::default()
        };
        for (drives, parity) in [(4_usize, 2_usize), (16, 8), (16, 4)] {
            let data = drives - parity;
            let bucket = format!("torn-write-get-{drives}-{parity}");
            let (dirs, set) = crate::ecstore_validation_blackbox::make_local_set_disks(drives, parity).await;
            set.make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("bucket should be created");
            // Two committed generations of one key give every drive its own
            // inline shard of each generation; the cases copy them per drive.
            let mut generations = Vec::new();
            for body in [b"old generation".as_slice(), b"new generation".as_slice()] {
                let info = set
                    .put_object(
                        &bucket,
                        "template",
                        &mut PutObjReader::from_vec(body.to_vec()),
                        &ObjectOptions {
                            no_lock: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("generation PUT should commit on every drive");
                let mut metadata = Vec::with_capacity(drives);
                for dir in &dirs {
                    metadata.push(
                        tokio::fs::read(dir.path().join(&bucket).join("template").join(STORAGE_FORMAT_FILE))
                            .await
                            .expect("generation metadata should carry the inline shard"),
                    );
                }
                generations.push((body, info.etag.expect("PUT should report an etag"), metadata));
            }
            let (new_body, new_etag, new) = generations.pop().expect("new generation");
            let (old_body, old_etag, old) = generations.pop().expect("old generation");

            // New copies occupy the first drives, so the offline drive always
            // removes one new copy. Each phase reads its own keys so the GET
            // metadata cache never answers for the other phase.
            let mut cases = Vec::new();
            for offline in [false, true] {
                for copies in 1..drives {
                    for overwrite in [false, true] {
                        let name = format!(
                            "{}-{}-{copies:02}",
                            if offline { "offline" } else { "online" },
                            if overwrite { "overwrite" } else { "new" }
                        );
                        for slot in 0..drives {
                            let bytes = if slot < copies {
                                &new[slot]
                            } else if overwrite {
                                &old[slot]
                            } else {
                                continue;
                            };
                            let path = dirs[slot].path().join(&bucket).join(&name);
                            tokio::fs::create_dir_all(&path).await.expect("create object directory");
                            tokio::fs::write(path.join(STORAGE_FORMAT_FILE), bytes)
                                .await
                                .expect("write generation metadata");
                        }
                        cases.push((offline, name, copies, overwrite));
                    }
                }
            }

            for offline in [false, true] {
                let parked = if offline { set.disks.write().await[0].take() } else { None };
                for (_, name, copies, overwrite) in cases.iter().filter(|case| case.0 == offline) {
                    let new_copies = copies - usize::from(offline);
                    let old_copies = if *overwrite { drives - copies } else { 0 };
                    let expected = if new_copies >= data && new_copies >= old_copies {
                        Some((&new_etag, new_body))
                    } else if old_copies >= data {
                        Some((&old_etag, old_body))
                    } else {
                        None
                    };
                    let context = format!("{drives} drives EC {data}+{parity}, {name}: {new_copies} new, {old_copies} old");
                    let head = set.get_object_info(&bucket, name, &opts).await;
                    let get = set.get_object_reader(&bucket, name, None, HeaderMap::new(), &opts).await;
                    match (expected, head, get) {
                        (Some((etag, body)), Ok(info), Ok(mut reader)) => {
                            assert_eq!(info.etag.as_ref(), Some(etag), "{context}: HEAD");
                            assert_eq!(reader.object_info.etag.as_ref(), Some(etag), "{context}: GET");
                            let mut streamed = Vec::new();
                            reader
                                .stream
                                .read_to_end(&mut streamed)
                                .await
                                .unwrap_or_else(|err| panic!("{context}: GET body: {err:?}"));
                            assert_eq!(streamed, body, "{context}: GET body");
                        }
                        (Some((etag, _)), head, get) => panic!(
                            "{context}: expected {etag}, HEAD {:?}, GET {:?}",
                            head.map(|info| info.etag),
                            get.map(|reader| reader.object_info.etag)
                        ),
                        (None, head, get) => {
                            for (method, result) in [
                                ("HEAD", head.map(|info| info.etag)),
                                ("GET", get.map(|reader| reader.object_info.etag)),
                            ] {
                                let err = result.expect_err(&format!("{context}: {method} expected 404 or unavailable"));
                                assert!(
                                    matches!(
                                        err,
                                        StorageError::ObjectNotFound(..)
                                            | StorageError::FileNotFound
                                            | StorageError::ErasureReadQuorum
                                            | StorageError::InsufficientReadQuorum(..)
                                    ),
                                    "{context}: {method} expected 404 or unavailable, got {err:?}"
                                );
                            }
                        }
                    }
                }
                if offline {
                    set.disks.write().await[0] = parked;
                }
            }
        }
    }

    #[tokio::test]
    async fn latest_listing_and_get_agree_on_hermetic_set_states() {
        #[derive(Clone, Copy)]
        enum Generation {
            Old,
            New,
            Corrupt,
        }

        async fn put_generation(
            set: &SetDisks,
            dirs: &[tempfile::TempDir],
            bucket: &str,
            object: &str,
            body: &[u8],
        ) -> (String, Vec<Vec<u8>>) {
            let mut reader = PutObjReader::from_vec(body.to_vec());
            let info = set
                .put_object(
                    bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        no_lock: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("generation PUT should commit on every drive");
            let mut metadata = Vec::with_capacity(dirs.len());
            for dir in dirs {
                metadata.push(
                    tokio::fs::read(dir.path().join(bucket).join(object).join(STORAGE_FORMAT_FILE))
                        .await
                        .expect("generation metadata should carry the inline shard"),
                );
            }
            (info.etag.expect("PUT should report an etag"), metadata)
        }

        let bucket = "list-get-consistency";
        let (dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        for disk in &disks {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let committed = "a-committed";
        let (old_etag, old) = put_generation(&set, &dirs, bucket, committed, b"old generation").await;
        let (new_etag, new) = put_generation(&set, &dirs, bucket, committed, b"new generation").await;
        assert_ne!(old_etag, new_etag);

        let states = [
            (
                "b-copied",
                [
                    Some(Generation::New),
                    Some(Generation::New),
                    Some(Generation::New),
                    Some(Generation::New),
                ],
            ),
            (
                "c-split",
                [
                    Some(Generation::Old),
                    Some(Generation::Old),
                    Some(Generation::New),
                    Some(Generation::New),
                ],
            ),
            ("d-torn-two", [Some(Generation::New), Some(Generation::New), None, None]),
            ("e-torn-one", [Some(Generation::New), None, None, None]),
            ("f-stale-one", [None, None, None, Some(Generation::Old)]),
            (
                "fa-overwrite-two-of-three",
                [Some(Generation::New), Some(Generation::New), Some(Generation::Old), None],
            ),
            (
                "g-overwrite-torn-one",
                [
                    Some(Generation::New),
                    Some(Generation::Old),
                    Some(Generation::Old),
                    Some(Generation::Old),
                ],
            ),
            (
                "h-corrupt-majority",
                [
                    Some(Generation::New),
                    Some(Generation::Corrupt),
                    Some(Generation::Corrupt),
                    Some(Generation::Corrupt),
                ],
            ),
        ];
        for (name, copies) in &states {
            for (slot, copy) in copies.iter().enumerate() {
                let Some(copy) = copy else {
                    continue;
                };
                let path = dirs[slot].path().join(bucket).join(name);
                tokio::fs::create_dir_all(&path).await.expect("create state directory");
                let bytes: &[u8] = match copy {
                    Generation::Old => &old[slot],
                    Generation::New => &new[slot],
                    Generation::Corrupt => b"corrupt xl.meta",
                };
                tokio::fs::write(path.join(STORAGE_FORMAT_FILE), bytes)
                    .await
                    .expect("write state metadata");
            }
        }

        let (result, listed) = collect_listing_etags(&set, bucket, "strict").await;
        result.expect("LIST must not fail because one object is unresolved or corrupt");
        let expected: Vec<(String, String)> = [
            ("a-committed", &new_etag),
            ("b-copied", &new_etag),
            ("c-split", &new_etag),
            ("d-torn-two", &new_etag),
            ("fa-overwrite-two-of-three", &new_etag),
            ("g-overwrite-torn-one", &old_etag),
        ]
        .into_iter()
        .map(|(name, etag)| (name.to_owned(), etag.clone()))
        .collect();
        assert_eq!(listed, expected);

        for name in std::iter::once(committed).chain(states.iter().map(|(name, _)| *name)) {
            let listed_etag = listed.iter().find(|(listed, _)| listed == name).map(|(_, etag)| etag);
            let get = set
                .get_object_reader(bucket, name, None, HeaderMap::new(), &ObjectOptions::default())
                .await;
            match (listed_etag, get) {
                (Some(listed_etag), Ok(mut reader)) => {
                    let mut body = Vec::new();
                    reader
                        .stream
                        .read_to_end(&mut body)
                        .await
                        .expect("listed object should stream");
                    assert_eq!(
                        reader.object_info.etag.as_ref(),
                        Some(listed_etag),
                        "{name}: GET must serve the version LIST selected"
                    );
                }
                (Some(_), Err(err)) => assert!(
                    matches!(
                        err,
                        StorageError::ObjectNotFound(..) | StorageError::FileNotFound | StorageError::VersionNotFound(..)
                    ),
                    "{name}: a listed key may only be 200 or 404, got {err:?}"
                ),
                (None, Ok(reader)) => panic!("{name}: GET serves {:?} that LIST omitted", reader.object_info.etag),
                // Majority-corrupt metadata surfaces as `FileCorrupt`, an
                // unavailability error like the read-quorum variants.
                (None, Err(err)) => assert!(
                    matches!(
                        err,
                        StorageError::ObjectNotFound(..)
                            | StorageError::FileNotFound
                            | StorageError::ErasureReadQuorum
                            | StorageError::InsufficientReadQuorum(..)
                            | StorageError::FileCorrupt
                    ),
                    "{name}: an omitted key must be 404 or unavailable, got {err:?}"
                ),
            }
        }
    }

    #[tokio::test]
    async fn latest_listing_resolves_concurrent_overwrite_split_without_object_lock() {
        let bucket = "concurrent-overwrite-split";
        let ctx = Arc::new(crate::runtime::instance::InstanceContext::new());
        ctx.update_erasure_type(crate::layout::endpoints::SetupType::Erasure).await;
        let (dirs, set) = crate::ecstore_validation_blackbox::make_local_set_disks_with_ctx(16, 8, ctx).await;
        for dir in &dirs {
            tokio::fs::create_dir_all(dir.path().join(bucket))
                .await
                .expect("create bucket directory");
        }
        let old_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let new_time = old_time + time::Duration::seconds(1);
        // old|new copies of an overwrite that is still publishing.
        let mut expected = Vec::new();
        for (old_copies, latest) in [(5, "new"), (8, "new"), (11, "old")] {
            let name = format!("split-{old_copies:02}-{:02}", 16 - old_copies);
            let old = test_unversioned_object_meta_entry_with_erasure(&name, old_time, "old", Uuid::from_u128(1), 8, 8);
            let new = test_unversioned_object_meta_entry_with_erasure(&name, new_time, "new", Uuid::from_u128(2), 8, 8);
            write_listing_metadata(&dirs, bucket, &old, 0..old_copies).await;
            write_listing_metadata(&dirs, bucket, &new, old_copies..16).await;
            expected.push((name, latest.to_owned()));
        }
        // A delete marker that half the set already carries hides the key.
        let tombstone = "tombstone-08-08";
        let object = test_object_meta_entry_with_erasure_versions(tombstone, &[(old_time, "object-etag", 4, 2)]);
        let marked = test_object_with_delete_marker_meta_entry(tombstone, old_time, new_time);
        write_listing_metadata(&dirs, bucket, &object, 0..8).await;
        write_listing_metadata(&dirs, bucket, &marked, 8..16).await;
        expected.push((tombstone.to_owned(), "delete-marker".to_owned()));

        // Hold every key's write lock the way an in-flight overwrite does.
        let mut guards = Vec::new();
        for (name, _) in &expected {
            guards.push(
                set.new_ns_lock(bucket, name)
                    .await
                    .expect("namespace lock should be created")
                    .get_write_lock(Duration::from_secs(5))
                    .await
                    .expect("writer lock should be acquired"),
            );
        }
        let (result, listed) = timeout(Duration::from_secs(5), collect_listing_etags(&set, bucket, "strict"))
            .await
            .expect("LIST must not wait for an object lock");
        drop(guards);
        result.expect("a split generation must not fail the listing");
        assert_eq!(listed, expected);
    }

    #[test]
    fn test_null_version_marker_handling() {
        let parsed = parse_version_marker("null".to_string()).expect("null marker should parse");
        assert_eq!(parsed, VersionMarker::Null);

        let valid_uuid = "550e8400-e29b-41d4-a716-446655440000";
        let parsed = parse_version_marker(valid_uuid.to_string()).expect("uuid marker should parse");
        assert_eq!(parsed, VersionMarker::Version(Uuid::parse_str(valid_uuid).unwrap()));
    }

    /// Test that next_version_idmarker returns "null" for non-versioned objects
    #[test]
    fn test_next_version_idmarker_null_string() {
        // When version_id is None, next_version_idmarker should be "null"
        let version_id: Option<Uuid> = None;
        let next_version_idmarker = version_id.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string());
        assert_eq!(next_version_idmarker, "null");

        // When version_id is Some, next_version_idmarker should be the UUID string
        let version_id: Option<Uuid> = Some(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap());
        let next_version_idmarker = version_id.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string());
        assert_eq!(next_version_idmarker, "550e8400-e29b-41d4-a716-446655440000");
    }

    /// Test the round-trip: next_version_idmarker -> VersionIdMarker parameter -> parsing
    #[test]
    fn test_version_marker_round_trip() {
        // Scenario 1: Non-versioned object
        // Server returns "null" as NextVersionIdMarker
        // Client sends "null" as VersionIdMarker
        // Server parses "null" as an explicit null-version marker
        let server_response = "null";
        let client_request = server_response;
        let parsed = parse_version_marker(client_request.to_string()).expect("null marker should parse");
        assert_eq!(parsed, VersionMarker::Null);

        // Scenario 2: Versioned object
        // Server returns UUID as NextVersionIdMarker
        // Client sends UUID as VersionIdMarker
        // Server parses UUID correctly
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let server_response = uuid_str;
        let client_request = server_response;
        let parsed = parse_version_marker(client_request.to_string()).expect("uuid marker should parse");
        assert_eq!(parsed, VersionMarker::Version(Uuid::parse_str(uuid_str).unwrap()));
    }

    #[test]
    fn list_path_marker_round_trip_preserves_set_index() {
        // The exact tag shape a 1.0.x walker appended to its markers.
        let marker = format!(
            "photos/2026/image.jpg[rustfs_cache:{},id:list-cache-id,p:3,s:7,src:walker,gen:live]",
            super::MARKER_TAG_VERSION
        );

        let mut parsed = ListPathOptions {
            marker: Some(marker),
            ..Default::default()
        };
        parsed.parse_marker();

        assert_eq!(parsed.marker.as_deref(), Some("photos/2026/image.jpg"));
        assert_eq!(parsed.id.as_deref(), Some("list-cache-id"));
        assert_eq!(parsed.pool_idx, Some(3));
        assert_eq!(parsed.set_idx, Some(7));
        assert_eq!(parsed.cursor_source, Some(ListSourceMode::Walker));
        assert_eq!(parsed.cursor_generation.as_deref(), Some("live"));
        assert!(!parsed.create);
    }

    #[test]
    fn list_path_marker_parser_preserves_source_aware_cursor_fields() {
        let mut parsed = ListPathOptions {
            marker: Some(
                "photos/2026/image.jpg[rustfs_cache:v2,id:list-cache-id,p:3,s:7,src:index_key_only,gen:generation-42]"
                    .to_string(),
            ),
            ..Default::default()
        };

        parsed.parse_marker();

        assert_eq!(parsed.marker.as_deref(), Some("photos/2026/image.jpg"));
        assert_eq!(parsed.id.as_deref(), Some("list-cache-id"));
        assert_eq!(parsed.pool_idx, Some(3));
        assert_eq!(parsed.set_idx, Some(7));
        assert_eq!(parsed.cursor_source, Some(ListSourceMode::IndexKeyOnly));
        assert_eq!(parsed.cursor_generation.as_deref(), Some("generation-42"));
        assert!(!parsed.create);
    }

    #[test]
    fn list_path_marker_parser_rejects_unknown_cursor_source() {
        let original = "photos/2026/image.jpg[rustfs_cache:v2,id:list-cache-id,src:unknown,gen:generation-42]";
        let mut parsed = ListPathOptions {
            marker: Some(original.to_string()),
            ..Default::default()
        };

        parsed.parse_marker();

        assert_eq!(parsed.marker.as_deref(), Some(original));
        assert!(parsed.id.is_none());
        assert!(parsed.cursor_source.is_none());
        assert!(parsed.cursor_generation.is_none());
        assert!(!parsed.create);
    }

    #[test]
    fn list_path_marker_parser_uses_trailing_cache_tag() {
        let mut parsed = ListPathOptions {
            marker: Some(format!(
                "photos/[archive]/image.jpg[rustfs_cache:{},id:list-cache-id,p:3,s:7]",
                super::MARKER_TAG_VERSION
            )),
            ..Default::default()
        };

        parsed.parse_marker();

        assert_eq!(parsed.marker.as_deref(), Some("photos/[archive]/image.jpg"));
        assert_eq!(parsed.id.as_deref(), Some("list-cache-id"));
        assert_eq!(parsed.pool_idx, Some(3));
        assert_eq!(parsed.set_idx, Some(7));
    }

    #[test]
    fn list_path_marker_parser_return_tag_forces_cache_refresh() {
        let mut parsed = ListPathOptions {
            marker: Some("photos/2026/image.jpg[rustfs_cache:v2,return:,p:3,s:7]".to_string()),
            ..Default::default()
        };

        parsed.parse_marker();

        assert_eq!(parsed.marker.as_deref(), Some("photos/2026/image.jpg"));
        assert!(parsed.id.is_some());
        assert_eq!(parsed.pool_idx, Some(3));
        assert_eq!(parsed.set_idx, Some(7));
        assert!(parsed.create);
    }

    #[test]
    fn list_path_marker_parser_recovers_from_corrupt_set_index() {
        let mut parsed = ListPathOptions {
            marker: Some("photos/2026/image.jpg[rustfs_cache:v2,id:list-cache-id,p:3,s:not-a-number]".to_string()),
            ..Default::default()
        };

        parsed.parse_marker();

        assert_eq!(parsed.marker.as_deref(), Some("photos/2026/image.jpg"));
        assert!(parsed.id.is_some());
        assert_ne!(parsed.id.as_deref(), Some("list-cache-id"));
        assert!(parsed.create);
        assert_eq!(parsed.pool_idx, Some(3));
        assert!(parsed.set_idx.is_none());
    }

    #[test]
    fn list_path_marker_parser_accepts_legacy_v1_tag() {
        let mut parsed = ListPathOptions {
            marker: Some("photos/2026/image.jpg[rustfs_cache:v1,id:legacy-cache-id,p:4,s:1]".to_string()),
            ..Default::default()
        };

        parsed.parse_marker();

        assert_eq!(parsed.marker.as_deref(), Some("photos/2026/image.jpg"));
        assert_eq!(parsed.id.as_deref(), Some("legacy-cache-id"));
        assert_eq!(parsed.pool_idx, Some(4));
        assert_eq!(parsed.set_idx, Some(1));
        assert!(parsed.cursor_source.is_none());
        assert!(parsed.cursor_generation.is_none());
        assert!(!parsed.create);
    }

    #[test]
    fn list_path_marker_parser_ignores_unsupported_cache_tag_version() {
        let marker = "photos/image.jpg[rustfs_cache:v0,id:list-cache-id,p:3,s:7]".to_string();
        let mut parsed = ListPathOptions {
            marker: Some(marker.clone()),
            ..Default::default()
        };

        parsed.parse_marker();

        assert_eq!(parsed.marker.as_deref(), Some(marker.as_str()));
        assert!(parsed.id.is_none());
        assert!(parsed.pool_idx.is_none());
        assert!(parsed.set_idx.is_none());
        assert!(!parsed.create);
    }

    #[test]
    fn walk_result_from_set_errors_returns_real_error_after_missing_entries() {
        let err = walk_result_from_set_errors(&[Some(StorageError::FileNotFound), Some(StorageError::FileAccessDenied)])
            .expect_err("walk should fail when any set reports a real listing error");

        assert_eq!(err, StorageError::FileAccessDenied);
    }

    #[test]
    fn walk_result_from_set_errors_prefers_real_error_over_not_found() {
        let err = walk_result_from_set_errors(&[
            Some(StorageError::VolumeNotFound),
            Some(StorageError::DiskNotFound),
            Some(StorageError::FileAccessDenied),
        ])
        .expect_err("walk should report the real listing error");

        assert_eq!(err, StorageError::FileAccessDenied);
    }

    #[test]
    fn walk_result_from_set_errors_preserves_volume_not_found() {
        let err = walk_result_from_set_errors(&[Some(StorageError::VolumeNotFound), Some(StorageError::VolumeNotFound)])
            .expect_err("all volume-not-found set errors should remain visible");

        assert_eq!(err, StorageError::VolumeNotFound);
    }

    #[test]
    fn walk_result_from_set_errors_allows_missing_entries() {
        walk_result_from_set_errors(&[Some(StorageError::FileNotFound), Some(StorageError::VolumeNotFound)])
            .expect("missing objects under an existing listing path should not fail the walk");
    }

    // `Unexpected` is a failure like any other; it no longer doubles as the
    // "this set reached the end of its listing" sentinel.
    #[test]
    fn walk_result_from_set_errors_surfaces_unexpected_next_to_successes() {
        let err = walk_result_from_set_errors(&[None, Some(StorageError::Unexpected)])
            .expect_err("an unexpected set failure must fail the walk instead of posing as EOF");

        assert_eq!(err, StorageError::Unexpected);
    }

    // Regression for #952 (ECA-11): when every set is unreachable the walk must
    // surface an availability error, not silently succeed with an empty listing.
    #[test]
    fn walk_result_from_set_errors_surfaces_full_offline() {
        let err = walk_result_from_set_errors(&[Some(StorageError::DiskNotFound), Some(StorageError::DiskNotFound)])
            .expect_err("all sets unreachable must surface an availability error, not an empty listing");

        assert_eq!(err, StorageError::DiskNotFound);
    }

    // A partial outage (some set healthy) must still be tolerated as before: the
    // healthy set's success short-circuits the all-offline detection.
    #[test]
    fn walk_result_from_set_errors_tolerates_partial_offline() {
        walk_result_from_set_errors(&[None, Some(StorageError::DiskNotFound)])
            .expect("a partial outage with a healthy set must not fail the walk");
    }

    #[tokio::test]
    async fn merge_entry_channels_produces_sorted_unique_output_from_two_channels() {
        let (tx_a, rx_a) = mpsc::channel(4);
        let (tx_b, rx_b) = mpsc::channel(4);
        let (out_tx, mut out_rx) = mpsc::channel(8);

        // Send sorted entries from two channels with some overlap
        tx_a.send(test_meta_entry("obj-a")).await.unwrap();
        tx_a.send(test_meta_entry("obj-c")).await.unwrap();
        tx_a.send(test_meta_entry("obj-e")).await.unwrap();
        drop(tx_a);

        tx_b.send(test_meta_entry("obj-b")).await.unwrap();
        tx_b.send(test_meta_entry("obj-d")).await.unwrap();
        drop(tx_b);

        let rx = CancellationToken::new();
        let handle = tokio::spawn(merge_entry_channels(rx, vec![rx_a, rx_b], out_tx, 1));

        let mut results = Vec::new();
        while let Some(entry) = out_rx.recv().await {
            results.push(entry.name.clone());
        }

        handle.await.unwrap().unwrap();

        // Results should be sorted and deduplicated
        assert_eq!(results, vec!["obj-a", "obj-b", "obj-c", "obj-d", "obj-e"]);
    }

    #[tokio::test]
    async fn merge_entry_channels_deduplicates_entries_across_channels() {
        let (tx_a, rx_a) = mpsc::channel(4);
        let (tx_b, rx_b) = mpsc::channel(4);
        let (out_tx, mut out_rx) = mpsc::channel(8);

        // Both channels have the same entry
        tx_a.send(test_meta_entry("obj-a")).await.unwrap();
        tx_a.send(test_meta_entry("obj-c")).await.unwrap();
        drop(tx_a);

        tx_b.send(test_meta_entry("obj-a")).await.unwrap();
        tx_b.send(test_meta_entry("obj-b")).await.unwrap();
        drop(tx_b);

        let rx = CancellationToken::new();
        let handle = tokio::spawn(merge_entry_channels(rx, vec![rx_a, rx_b], out_tx, 1));

        let mut results = Vec::new();
        while let Some(entry) = out_rx.recv().await {
            results.push(entry.name.clone());
        }

        handle.await.unwrap().unwrap();

        // "obj-a" should appear only once despite being in both channels
        assert_eq!(results, vec!["obj-a", "obj-b", "obj-c"]);
    }

    #[tokio::test]
    async fn merge_entry_channels_preserves_cross_pool_delete_marker_versions() {
        let (tx_a, rx_a) = mpsc::channel(4);
        let (tx_b, rx_b) = mpsc::channel(4);
        let (tx_c, rx_c) = mpsc::channel(4);
        let (out_tx, mut out_rx) = mpsc::channel(4);
        let mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_500).expect("valid timestamp");
        let stale_mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let stale_object = test_object_meta_entry_with_erasure_versions("obj-a", &[(stale_mod_time, "stale-etag", 4, 2)]);

        tx_a.send(test_delete_marker_meta_entry("obj-a", mod_time))
            .await
            .expect("first delete marker should queue");
        tx_b.send(test_delete_marker_meta_entry("obj-a", mod_time))
            .await
            .expect("second delete marker should queue");
        tx_c.send(stale_object).await.expect("stale object should queue");
        drop(tx_a);
        drop(tx_b);
        drop(tx_c);

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(merge_entry_channels(cancel, vec![rx_a, rx_b, rx_c], out_tx, 2));

        let mut merged = timeout(Duration::from_secs(1), out_rx.recv())
            .await
            .expect("merged entry should be emitted promptly")
            .expect("merged entry should be present");
        assert_eq!(merged.name, "obj-a");
        assert!(
            merged.is_latest_delete_marker(),
            "a newer marker must remain current across independently resolved pools"
        );
        let versions = merged.file_info_versions("bucket").expect("merged versions should decode");
        assert_eq!(versions.versions.len(), 2, "retain the historical object and deduplicate the marker");
        assert!(versions.versions[0].deleted && versions.versions[0].is_latest);
        assert!(!versions.versions[1].deleted && !versions.versions[1].is_latest);
        assert!(
            matches!(timeout(Duration::from_secs(1), out_rx.recv()).await, Ok(None)),
            "merge should not emit a duplicate entry for the same key"
        );

        handle
            .await
            .expect("merge task should not panic")
            .expect("merge task should succeed");
    }

    fn rewrite_test_version(mut entry: MetaCacheEntry, change: impl FnOnce(&mut FileMetaVersion)) -> MetaCacheEntry {
        let meta = entry.cached.as_mut().expect("test metadata should be decoded");
        assert_eq!(meta.versions.len(), 1);
        let mut version = meta.versions[0].parse_version_meta().expect("test version should decode");
        change(&mut version);
        meta.versions[0] = version.try_into().expect("test version should encode");
        entry.metadata = meta.marshal_msg().expect("test metadata should encode");
        entry
    }

    async fn merge_test_object_entries(entries: Vec<MetaCacheEntry>) -> Result<MetaCacheEntry> {
        let mut inputs = Vec::with_capacity(entries.len());
        for entry in entries {
            let (sender, receiver) = mpsc::channel(1);
            sender.send(entry).await.expect("fixture entry should queue");
            inputs.push(receiver);
        }
        let (sender, mut receiver) = mpsc::channel(1);
        let task = tokio::spawn(merge_entry_channels(CancellationToken::new(), inputs, sender, 1));
        let entry = receiver.recv().await;
        task.await.expect("merge must not panic")?;
        Ok(entry.expect("a valid same-key group must produce an entry"))
    }

    #[tokio::test]
    async fn merge_entry_channels_orders_complete_histories_independently_of_pool_order() {
        let time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let first = test_object_meta_entry_with_erasure_versions("key", &[(time, "first", 4, 2)]);
        let second = rewrite_test_version(
            test_object_meta_entry_with_erasure_versions("key", &[(time, "second", 4, 2)]),
            |version| version.object.as_mut().expect("object version").version_id = Some(Uuid::from_u128(2)),
        );
        let marker = test_delete_marker_meta_entry("key", time + time::Duration::seconds(1));
        let inputs = [first, second, marker];
        let mut expected = None;
        for order in [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]] {
            let entry = merge_test_object_entries(order.map(|index| inputs[index].clone()).to_vec())
                .await
                .expect("disjoint version chains should merge");
            let versions = entry.file_info_versions("bucket").expect("merged versions should decode");
            assert_eq!(versions.versions.len(), 3);
            assert!(versions.versions[0].deleted && versions.versions[0].is_latest);
            assert!(
                versions.versions[1..]
                    .iter()
                    .all(|version| !version.deleted && !version.is_latest)
            );
            assert!(versions.versions.iter().all(|version| version.num_versions == 3));
            let identities = versions.versions.iter().map(|version| version.version_id).collect::<Vec<_>>();
            assert!(identities.contains(&Some(Uuid::from_u128(1))));
            assert!(identities.contains(&Some(Uuid::from_u128(2))));
            if let Some(expected) = &expected {
                assert_eq!(&identities, expected, "equal-time versions must have stable pagination order");
            } else {
                expected = Some(identities);
            }
        }
    }

    #[tokio::test]
    async fn merge_entry_channels_key_only_candidates_do_not_override_version_metadata() {
        let time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let marker = test_delete_marker_meta_entry("key", time);
        for entries in [
            vec![test_meta_entry("key"), marker.clone()],
            vec![marker.clone(), test_meta_entry("key")],
        ] {
            let mut merged = merge_test_object_entries(entries)
                .await
                .expect("merge a name with resolved metadata");
            assert!(merged.is_latest_delete_marker());
            assert_eq!(merged.file_info_versions("bucket").expect("decode marker").versions.len(), 1);
        }
    }

    #[tokio::test]
    async fn merge_entry_channels_accepts_equivalent_migrated_coding_and_data_dirs() {
        let time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let first = test_object_meta_entry_with_erasure_versions("key", &[(time, "same-etag", 4, 2)]);
        let second = rewrite_test_version(
            test_object_meta_entry_with_erasure_versions("key", &[(time, "same-etag", 6, 2)]),
            |version| version.object.as_mut().expect("object version").data_dir = Some(Uuid::from_u128(42)),
        );
        let forward = merge_test_object_entries(vec![first.clone(), second.clone()])
            .await
            .expect("valid migration copies");
        let reverse = merge_test_object_entries(vec![second, first])
            .await
            .expect("reversed migration copies");
        assert_eq!(forward.metadata, reverse.metadata, "representation must not depend on channel order");
        let versions = forward.file_info_versions("bucket").expect("merged metadata should decode");
        assert_eq!(versions.versions.len(), 1);
        assert_eq!(versions.versions[0].metadata.get("etag").map(String::as_str), Some("same-etag"));
    }

    #[tokio::test]
    async fn merge_entry_channels_data_movement_accepts_inline_layout_changes() {
        let time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let external = test_object_meta_entry_with_erasure_versions("key", &[(time, "same-etag", 2, 1)]);
        let inline = rewrite_test_version(
            test_object_meta_entry_with_erasure_versions("key", &[(time, "same-etag", 5, 4)]),
            |version| {
                rustfs_utils::http::insert_bytes(
                    &mut version.object.as_mut().expect("object version").meta_sys,
                    rustfs_utils::http::SUFFIX_INLINE_DATA,
                    b"true".to_vec(),
                );
            },
        );
        let legacy_inline = rewrite_test_version(inline.clone(), |version| {
            version
                .object
                .as_mut()
                .expect("object version")
                .meta_sys
                .remove(&rustfs_utils::http::internal_key_rustfs(rustfs_utils::http::SUFFIX_INLINE_DATA));
        });
        let copies = [external, legacy_inline, inline];
        let mut expected = None;
        for order in [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]] {
            let merged = merge_test_object_entries(order.map(|index| copies[index].clone()).to_vec())
                .await
                .expect("physical inline placement and legacy aliases must not change object identity");
            assert_eq!(
                merged
                    .file_info_versions("bucket")
                    .expect("decode merged history")
                    .versions
                    .len(),
                1
            );
            if let Some(expected) = &expected {
                assert_eq!(&merged.metadata, expected, "representation must not depend on pool order");
            } else {
                expected = Some(merged.metadata);
            }
        }
    }

    #[test]
    fn data_movement_listing_conflict_logs_identity_without_metadata_values() {
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_ansi(false)
            .without_time()
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let mut original = test_object_meta_entry_with_erasure_versions("failing-key", &[(time, "same-etag", 2, 1)]);
        let conflicting = rewrite_test_version(original.clone(), |version| {
            version
                .object
                .as_mut()
                .expect("object version")
                .meta_user
                .insert("x-amz-meta-private".to_string(), "sensitive-value-must-not-be-logged".to_string());
        });
        assert_eq!(
            super::merge_object_entry_versions(&mut original, std::iter::once(conflicting))
                .expect_err("conflicting user metadata must still fail closed"),
            StorageError::FileCorrupt
        );
        let output = logs.contents();
        assert!(output.contains("listing_merge_rejected"));
        assert!(output.contains("failing-key"));
        assert!(output.contains("00000000-0000-0000-0000-000000000001"));
        assert!(output.contains("version_identity_conflict"));
        assert!(!output.contains("sensitive-value-must-not-be-logged"));
    }

    #[tokio::test]
    async fn merge_entry_channels_defers_free_version_while_same_remote_source_is_live() {
        let live = test_transitioned_meta_entry("key", "remote/shared", false);
        let free = test_transitioned_meta_entry("key", "remote/shared", true);
        for inputs in [vec![live.clone(), free.clone()], vec![free.clone(), live.clone()]] {
            let merged = merge_test_object_entries(inputs)
                .await
                .expect("same remote source and cleanup owner should merge");
            let versions = merged
                .file_info_versions_with_free_versions("bucket")
                .expect("merged transition history should decode");
            assert_eq!(versions.versions.len(), 1);
            assert!(versions.free_versions.is_empty(), "a live remote reference must defer cleanup discovery");
        }

        let unrelated = test_transitioned_meta_entry("key", "remote/other", false);
        let merged = merge_test_object_entries(vec![free, unrelated])
            .await
            .expect("unrelated remote references should merge");
        let versions = merged
            .file_info_versions_with_free_versions("bucket")
            .expect("merged transition history should decode");
        assert_eq!(versions.versions.len(), 1);
        assert_eq!(versions.free_versions.len(), 1, "an unrelated source must not suppress cleanup");
    }

    #[tokio::test]
    async fn merge_entry_channels_rejects_conflicting_version_identity_and_metadata() {
        let time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let original = test_object_meta_entry_with_erasure_versions("key", &[(time, "original", 4, 2)]);
        for key in [
            "etag",
            "x-amz-tagging",
            "x-amz-object-lock-mode",
            "x-amz-object-lock-retain-until-date",
        ] {
            let changed = rewrite_test_version(original.clone(), |version| {
                version
                    .object
                    .as_mut()
                    .expect("object version")
                    .meta_user
                    .insert(key.to_string(), "changed".to_string());
            });
            for pair in [[original.clone(), changed.clone()], [changed, original.clone()]] {
                let err = merge_test_object_entries(pair.to_vec())
                    .await
                    .expect_err("conflicting copies must fail");
                assert_eq!(err, StorageError::FileCorrupt, "conflict in {key} must not become arbitrary metadata");
            }
        }
        let marker = rewrite_test_version(test_delete_marker_meta_entry("key", time), |version| {
            version.delete_marker.as_mut().expect("delete marker").version_id = Some(Uuid::from_u128(1));
        });
        assert_eq!(
            merge_test_object_entries(vec![original, marker])
                .await
                .expect_err("UUID type conflict"),
            StorageError::FileCorrupt
        );
    }

    #[tokio::test]
    async fn merge_entry_channels_reconciles_null_overwrite_without_losing_uuid_history() {
        let time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let history = test_object_meta_entry_with_erasure_versions("key", &[(time, "history", 4, 2)]);
        let old_null = rewrite_test_version(history.clone(), |version| {
            version.object.as_mut().expect("null object").version_id = None;
        });
        let marker = rewrite_test_version(test_delete_marker_meta_entry("key", time + time::Duration::seconds(1)), |version| {
            version.delete_marker.as_mut().expect("null marker").version_id = Some(Uuid::nil());
        });
        for inputs in [
            vec![old_null.clone(), marker.clone(), history.clone()],
            vec![history, marker, old_null],
        ] {
            let entry = merge_test_object_entries(inputs)
                .await
                .expect("new null slot should replace old null slot");
            let versions = entry.file_info_versions("bucket").expect("null versions should decode");
            assert_eq!(versions.versions.len(), 2);
            assert!(versions.versions[0].deleted && versions.versions[0].is_latest);
            assert!(versions.versions[0].version_id.is_none_or(|id| id.is_nil()));
            assert_eq!(versions.versions[1].version_id, Some(Uuid::from_u128(1)));
        }
    }

    #[tokio::test]
    async fn merge_entry_channels_rejects_corrupt_version_headers_and_empty_stacks() {
        let time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let original = test_object_meta_entry_with_erasure_versions("key", &[(time, "etag", 4, 2)]);
        for empty in [false, true] {
            let mut corrupt = original.clone();
            let meta = corrupt.cached.as_mut().expect("fixture metadata");
            if empty {
                meta.versions.clear();
            } else {
                meta.versions[0].header.version_id = Some(Uuid::from_u128(99));
            }
            corrupt.metadata = meta.marshal_msg().expect("encode corrupt fixture");
            assert_eq!(
                merge_test_object_entries(vec![original.clone(), corrupt])
                    .await
                    .expect_err("corrupt candidate must fail"),
                StorageError::FileCorrupt
            );
        }
        let mut malformed = original.clone();
        malformed.cached = None;
        malformed.metadata = vec![0xff];
        assert_eq!(
            merge_test_object_entries(vec![original, malformed])
                .await
                .expect_err("malformed metadata must fail"),
            StorageError::FileCorrupt
        );
    }

    #[tokio::test]
    async fn merge_entry_channels_does_not_combine_subquorum_markers_across_erasure_sets() {
        let time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let old = test_object_meta_entry_with_erasure_versions("key", &[(time, "history", 4, 2)]);
        let marked = test_object_with_delete_marker_meta_entry("key", time, time + time::Duration::seconds(1));
        let mut inputs = Vec::new();
        for marker_copies in [1, 2] {
            let resolver = list_metadata_resolution_params("bucket".to_string(), 3, true);
            let copies = (0..3)
                .map(|index| Some(if index < marker_copies { marked.clone() } else { old.clone() }))
                .collect();
            let entry = MetaCacheEntries(copies)
                .resolve(resolver)
                .expect("each set independently retains its quorum-backed history");
            inputs.push(entry);
        }
        let merged = merge_test_object_entries(inputs).await.expect("merge resolved histories");
        let versions = merged.file_info_versions("bucket").expect("decode merged history");
        assert_eq!(
            versions.versions.len(),
            1,
            "three marker copies across two EC domains do not form a quorum"
        );
        assert!(!versions.versions[0].deleted);
    }

    async fn collect_list_path_results<P>(
        cancel: CancellationToken,
        opts: ListPathOptions,
        recv: mpsc::Receiver<MetaCacheEntry>,
        producer: P,
    ) -> Result<super::ListPage>
    where
        P: std::future::Future<Output = Result<ListPathRawOutcome>> + Send + 'static,
    {
        let context = super::ListPathLogContext::from_options(&opts);
        super::collect_list_path_results("test", &context, cancel, opts, recv, producer).await
    }

    #[tokio::test]
    async fn list_path_workers_report_producer_panic_instead_of_empty_eof() {
        let (sender, recv) = mpsc::channel(1);
        let result = timeout(
            Duration::from_secs(1),
            collect_list_path_results(CancellationToken::new(), ListPathOptions::default(), recv, async move {
                let _sender = sender;
                panic!("test listing producer panic");
            }),
        )
        .await
        .expect("producer panic must not strand the collector");
        assert!(
            result
                .expect_err("panic must be reported")
                .to_string()
                .contains("listing worker panicked")
        );
    }

    #[cfg(feature = "diagnostic-read-version-caller")]
    #[tokio::test]
    async fn list_path_producer_worker_preserves_s3_and_caller_context() {
        let mut request = rustfs_io_metrics::s3_http_metrics::S3HttpRequestGuard::new("GET");
        let api_operation = request
            .in_scope(|| {
                rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::ListObjectsV2);
                rustfs_io_metrics::s3_http_metrics::current_s3_http_operation_context()
            })
            .expect("dispatched list operation should be captured");
        request.response(200);

        let (sender, recv) = mpsc::channel(1);
        let opts = ListPathOptions {
            caller: rustfs_io_metrics::ListObjectsGatherCaller::S3List,
            ..Default::default()
        };
        let result = rustfs_io_metrics::s3_http_metrics::with_s3_http_operation_context(Some(api_operation), async move {
            collect_list_path_results(CancellationToken::new(), opts, recv, async move {
                assert_eq!(
                    rustfs_io_metrics::s3_http_metrics::current_s3_http_operation_name(),
                    Some("s3:ListObjectsV2")
                );
                assert_eq!(
                    crate::cluster::rpc::runtime_sources::current_read_version_request_caller(),
                    Some(rustfs_io_metrics::internode_metrics::ReadVersionRequestCaller::ListObjects)
                );
                drop(sender);
                Ok(ListPathRawOutcome::default())
            })
            .await
        })
        .await;

        let page = result.expect("an empty drained page is a successful page");
        assert!(page.exhausted && page.entries.entries().is_empty());
        assert_eq!(rustfs_io_metrics::s3_http_metrics::current_s3_http_operation_name(), None);
    }

    #[tokio::test]
    async fn list_path_workers_preserve_error_after_input_closes() {
        let (sender, recv) = mpsc::channel(1);
        let result = collect_list_path_results(CancellationToken::new(), ListPathOptions::default(), recv, async move {
            drop(sender);
            tokio::task::yield_now().await;
            Err(StorageError::FileCorrupt)
        })
        .await;
        assert_eq!(
            result.expect_err("producer failure must surface after the input closes"),
            StorageError::FileCorrupt
        );
    }

    /// `Unexpected` used to double as the EOF sentinel, so a producer failing
    /// with it read as "listing complete". It must surface as the error it is.
    #[tokio::test]
    async fn list_path_workers_surface_unexpected_producer_failure_instead_of_empty_page() {
        let (sender, recv) = mpsc::channel(1);
        let result = collect_list_path_results(CancellationToken::new(), ListPathOptions::default(), recv, async move {
            drop(sender);
            tokio::task::yield_now().await;
            Err(StorageError::Unexpected)
        })
        .await;
        assert_eq!(
            result.expect_err("an unexpected producer failure must not pose as a complete empty page"),
            StorageError::Unexpected
        );
    }

    #[tokio::test]
    async fn list_path_workers_drain_buffer_before_successful_eof() {
        let (sender, recv) = mpsc::channel(3);
        let result = collect_list_path_results(
            CancellationToken::new(),
            ListPathOptions {
                limit: 4,
                incl_deleted: true,
                ..Default::default()
            },
            recv,
            async move {
                for name in ["a", "b", "c"] {
                    sender.try_send(test_meta_entry(name)).expect("queue page");
                }
                Ok(ListPathRawOutcome::default())
            },
        )
        .await;
        let page = result.expect("a drained page is a successful page");
        assert!(page.exhausted, "a closed input with no bounded producer is the end of the listing");
        assert_eq!(
            page.entries
                .entries()
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            ["a", "b", "c"]
        );
    }

    #[tokio::test]
    async fn list_path_workers_abort_stalled_producer_at_page_limit() {
        let (sender, recv) = mpsc::channel(1);
        let dropped = CancellationToken::new();
        let producer_dropped = dropped.clone();
        let result = timeout(
            Duration::from_secs(1),
            collect_list_path_results(
                CancellationToken::new(),
                ListPathOptions {
                    limit: 1,
                    incl_deleted: true,
                    ..Default::default()
                },
                recv,
                async move {
                    let _drop_guard = producer_dropped.drop_guard();
                    sender.send(test_meta_entry("a")).await.expect("send page");
                    std::future::pending::<Result<ListPathRawOutcome>>().await
                },
            ),
        )
        .await
        .expect("page limit must not wait for a producer that ignores cancellation");
        let page = result.expect("page");
        assert!(!page.exhausted, "a page cut at its limit may have more");
        assert_eq!(page.entries.entries().len(), 1);
        assert!(dropped.is_cancelled(), "producer must be dropped before the page is returned");
    }

    #[tokio::test]
    async fn list_path_workers_parent_abort_drops_both_children() {
        let (sender, recv) = mpsc::channel(1);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let cancel = CancellationToken::new();
        let dropped = CancellationToken::new();
        let producer_dropped = dropped.clone();
        let parent = tokio::spawn(collect_list_path_results(cancel.clone(), ListPathOptions::default(), recv, async move {
            let _drop_guard = producer_dropped.drop_guard();
            started_tx.send(()).expect("notify parent");
            std::future::pending::<Result<ListPathRawOutcome>>().await
        }));
        started_rx.await.expect("producer started");
        parent.abort();
        assert!(parent.await.expect_err("parent aborted").is_cancelled());
        timeout(Duration::from_secs(1), dropped.cancelled())
            .await
            .expect("producer dropped");
        timeout(Duration::from_secs(1), sender.closed())
            .await
            .expect("collector dropped");
        assert!(cancel.is_cancelled());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn list_path_workers_preserve_ready_error_at_page_limit() {
        for _ in 0..128 {
            let (sender, recv) = mpsc::channel(1);
            let result = collect_list_path_results(
                CancellationToken::new(),
                ListPathOptions {
                    limit: 1,
                    incl_deleted: true,
                    ..Default::default()
                },
                recv,
                async move {
                    sender.try_send(test_meta_entry("a")).expect("queue page");
                    Err(StorageError::FileCorrupt)
                },
            )
            .await;
            assert_eq!(
                result.expect_err("a ready producer error must win over the page limit"),
                StorageError::FileCorrupt
            );
        }
    }

    #[tokio::test]
    async fn listing_merge_drop_stops_blocked_inputs() {
        let (input_tx, input_rx) = mpsc::channel(1);
        let (output_tx, _output_rx) = mpsc::channel(1);
        let task = super::spawn_listing_merge(CancellationToken::new(), vec![input_rx], output_tx);
        drop(task);

        timeout(Duration::from_secs(1), input_tx.closed())
            .await
            .expect("dropping the merge owner must release an idle producer");
    }

    #[tokio::test]
    async fn listing_merge_preserves_error_after_partial_output_without_cancelling_request() {
        let time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let (first_tx, first_rx) = mpsc::channel(2);
        let (second_tx, second_rx) = mpsc::channel(1);
        first_tx.send(test_meta_entry("a/")).await.expect("queue preceding prefix");
        first_tx
            .send(test_object_meta_entry_with_erasure_versions("b", &[(time, "one", 4, 2)]))
            .await
            .expect("queue first copy");
        second_tx
            .send(test_object_meta_entry_with_erasure_versions("b", &[(time, "two", 4, 2)]))
            .await
            .expect("queue conflicting copy");
        drop(first_tx);
        drop(second_tx);
        let request = CancellationToken::new();
        let workers = request.child_token();
        let (sender, mut receiver) = mpsc::channel(1);
        let task = super::spawn_listing_merge(workers.clone(), vec![first_rx, second_rx], sender);
        assert_eq!(receiver.recv().await.expect("preceding result should arrive").name, "a/");
        assert!(receiver.recv().await.is_none());
        assert_eq!(
            task.await
                .expect("merge task must not panic")
                .expect_err("conflict must propagate"),
            StorageError::FileCorrupt
        );
        assert!(workers.is_cancelled(), "failed merge must stop the disk producers");
        assert!(!request.is_cancelled(), "the API must still observe the merge error");
    }

    #[tokio::test]
    async fn merge_entry_channels_handles_single_channel() {
        let (tx, rx) = mpsc::channel(4);
        let (out_tx, mut out_rx) = mpsc::channel(8);

        tx.send(test_meta_entry("obj-a")).await.unwrap();
        tx.send(test_meta_entry("obj-b")).await.unwrap();
        drop(tx);

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(merge_entry_channels(cancel, vec![rx], out_tx, 1));

        let mut results = Vec::new();
        while let Some(entry) = out_rx.recv().await {
            results.push(entry.name.clone());
        }

        handle.await.unwrap().unwrap();

        assert_eq!(results, vec!["obj-a", "obj-b"]);
    }

    #[tokio::test]
    async fn merge_entry_channels_treats_dropped_output_receiver_as_completion() {
        let (tx_a, rx_a) = mpsc::channel(4);
        let (tx_b, rx_b) = mpsc::channel(4);
        let (out_tx, out_rx) = mpsc::channel(1);

        tx_a.send(test_meta_entry("obj-a")).await.unwrap();
        tx_b.send(test_meta_entry("obj-b")).await.unwrap();
        drop(tx_a);
        drop(tx_b);
        drop(out_rx);

        let result = timeout(
            Duration::from_secs(1),
            merge_entry_channels(CancellationToken::new(), vec![rx_a, rx_b], out_tx, 1),
        )
        .await
        .expect("merge should stop promptly when its consumer disconnects");

        assert!(result.is_ok(), "consumer disconnect must not surface as a merge worker error");
    }

    #[test]
    fn walk_ascending_versions_contract_reverses_newest_first_metadata() {
        // Documents the invariant the walk `versions_sort` handling relies on:
        // `FileMeta` keeps versions newest-first, so ascending emission is the
        // exact reverse of `file_info_versions` output.
        let older = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let newer = time::OffsetDateTime::from_unix_timestamp(1_705_312_500).expect("valid timestamp");
        let entry =
            test_object_meta_entry_with_erasure_versions("obj-a", &[(older, "etag-old", 4, 2), (newer, "etag-new", 4, 2)]);

        let fvs = entry.file_info_versions("bucket").expect("versions should parse");
        assert_eq!(fvs.versions.len(), 2, "both versions should be visible");
        assert!(
            fvs.versions[0].mod_time >= fvs.versions[1].mod_time,
            "file_info_versions must yield newest-first"
        );
        assert!(fvs.versions[0].is_latest, "the first (newest) version carries is_latest");

        let mut ascending = fvs.versions;
        ascending.reverse();
        assert!(
            ascending[0].mod_time <= ascending[1].mod_time,
            "reversing newest-first output must produce ascending mod_time order"
        );
        assert!(
            ascending.last().expect("versions present").is_latest,
            "ascending emission ends with the latest version"
        );
    }

    #[tokio::test]
    async fn merge_entry_channels_merges_interleaved_channels_in_global_order() {
        let mut txs = Vec::new();
        let mut rxs = Vec::new();
        for _ in 0..4 {
            let (tx, rx) = mpsc::channel(16);
            txs.push(tx);
            rxs.push(rx);
        }

        // Round-robin keys so every channel stays sorted while the global
        // stream interleaves across all four channels.
        let names: Vec<String> = (0..24).map(|i| format!("obj-{i:02}")).collect();
        for (i, name) in names.iter().enumerate() {
            txs[i % 4].send(test_meta_entry(name)).await.unwrap();
        }
        // The same trailing key on three channels must be emitted once.
        for tx in txs.iter().take(3) {
            tx.send(test_meta_entry("obj-99")).await.unwrap();
        }
        drop(txs);

        let (out_tx, mut out_rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(merge_entry_channels(cancel, rxs, out_tx, 1));

        let mut results = Vec::new();
        while let Some(entry) = out_rx.recv().await {
            results.push(entry.name.clone());
        }
        handle.await.unwrap().unwrap();

        let mut expected = names;
        expected.push("obj-99".to_owned());
        assert_eq!(results, expected);
    }

    #[tokio::test]
    async fn merge_entry_channels_emits_when_cleaned_order_differs_from_raw_order() {
        // `a//c` canonicalizes to `a/c` at merge intake and `a/b` is already
        // canonical. Both distinct keys must survive, in canonical byte order
        // (`a/b` < `a/c`) with the canonical spelling — emitting the raw
        // `a//c` spelling would break downstream marker comparisons, which use
        // the emitted names (backlog#1046).
        let (tx_a, rx_a) = mpsc::channel(4);
        let (tx_b, rx_b) = mpsc::channel(4);
        let (out_tx, mut out_rx) = mpsc::channel(8);

        tx_a.send(test_meta_entry("a/b")).await.unwrap();
        drop(tx_a);
        tx_b.send(test_meta_entry("a//c")).await.unwrap();
        drop(tx_b);

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(merge_entry_channels(cancel, vec![rx_a, rx_b], out_tx, 1));

        let mut results = Vec::new();
        while let Some(entry) = out_rx.recv().await {
            results.push(entry.name.clone());
        }
        handle.await.unwrap().unwrap();

        assert_eq!(
            results,
            vec!["a/b", "a/c"],
            "distinct keys must both be emitted, canonicalized, in canonical order"
        );
    }

    #[tokio::test]
    async fn merge_entry_channels_prefix_dir_sorts_after_sibling_keys() {
        // Regression for backlog#1046. Walker streams are sorted by raw byte
        // order, where a sibling like `a.txt` ('.' = 0x2E) sorts between the
        // object `a` and the prefix dir `a/` ('/' = 0x2F). The old cleaned-key
        // heap grouped channel B's `a/` (cleaned "a") with `a` and emitted it
        // BEFORE `a.txt`; a page boundary right after that `a/` made the next
        // page's marker filter drop `a.txt` (silent key loss), and channel A's
        // own late `a/` was then emitted a second time (duplicate prefix).
        // The merged stream must be `a`, `a.txt`, `a/` with exactly one `a/`.
        let (tx_a, rx_a) = mpsc::channel(8);
        let (tx_b, rx_b) = mpsc::channel(8);
        let (out_tx, mut out_rx) = mpsc::channel(16);

        // Set 1 holds object `a`, sibling `a.txt`, and children under `a/`.
        tx_a.send(test_object_meta_entry("a")).await.unwrap();
        tx_a.send(test_object_meta_entry("a.txt")).await.unwrap();
        tx_a.send(test_dir_meta_entry("a/")).await.unwrap();
        drop(tx_a);
        // Set 2 holds only children under `a/`, so its stream is just `a/`.
        tx_b.send(test_dir_meta_entry("a/")).await.unwrap();
        drop(tx_b);

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(merge_entry_channels(cancel, vec![rx_a, rx_b], out_tx, 1));

        let mut results = Vec::new();
        while let Some(entry) = out_rx.recv().await {
            results.push((entry.name.clone(), entry.is_dir()));
        }
        handle.await.unwrap().unwrap();

        assert_eq!(
            results,
            vec![("a".to_owned(), false), ("a.txt".to_owned(), false), ("a/".to_owned(), true),],
            "the prefix dir must sort after raw-smaller siblings and be emitted exactly once"
        );
    }

    #[tokio::test]
    async fn merge_entry_channels_collapses_canonical_duplicates_from_one_channel_across_rounds() {
        // A raw-ascending stream `a//b`, `a/b` ('/' < 'b') canonicalizes to two
        // consecutive `a/b` heads in consecutive merge rounds — they never meet
        // in one group, so only the cross-round `last_emitted` guard collapses
        // the duplicate. The second channel keeps the k-way path (not the
        // single-channel passthrough) engaged.
        let (tx_a, rx_a) = mpsc::channel(4);
        let (tx_b, rx_b) = mpsc::channel(4);
        let (out_tx, mut out_rx) = mpsc::channel(8);

        tx_a.send(test_meta_entry("a//b")).await.unwrap();
        tx_a.send(test_meta_entry("a/b")).await.unwrap();
        drop(tx_a);
        tx_b.send(test_meta_entry("z")).await.unwrap();
        drop(tx_b);

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(merge_entry_channels(cancel, vec![rx_a, rx_b], out_tx, 1));

        let mut results = Vec::new();
        while let Some(entry) = out_rx.recv().await {
            results.push(entry.name.clone());
        }
        handle.await.unwrap().unwrap();

        assert_eq!(
            results,
            vec!["a/b", "z"],
            "canonical duplicates arriving in consecutive rounds must collapse to one emission"
        );
    }

    #[tokio::test]
    async fn merge_entry_channels_lagging_dir_marker_collapses_into_emitted_prefix() {
        // A non-recursive walk emits the prefix dir `folder/` and the marker
        // object `folder/` adjacently on one channel. When another channel's
        // `folder/` groups with the dir first, the marker pops one round late
        // and must collapse into the already-emitted prefix (one S3 key, one
        // emission) rather than surfacing the same name twice — the old code
        // emitted both, and a page boundary between them dropped one.
        let (tx_a, rx_a) = mpsc::channel(4);
        let (tx_b, rx_b) = mpsc::channel(4);
        let (out_tx, mut out_rx) = mpsc::channel(8);

        tx_a.send(test_dir_meta_entry("folder/")).await.unwrap();
        tx_a.send(test_object_meta_entry("folder/")).await.unwrap();
        drop(tx_a);
        tx_b.send(test_dir_meta_entry("folder/")).await.unwrap();
        drop(tx_b);

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(merge_entry_channels(cancel, vec![rx_a, rx_b], out_tx, 1));

        let mut results = Vec::new();
        while let Some(entry) = out_rx.recv().await {
            results.push(entry.name.clone());
        }
        handle.await.unwrap().unwrap();

        assert_eq!(results, vec!["folder/"], "the same S3 key must be emitted exactly once");
    }

    #[tokio::test]
    async fn merge_entry_channels_object_and_same_named_prefix_dir_coexist() {
        // Regression for backlog #880. A plain object `a` and the prefix dir
        // `a/` are *distinct* S3 keys: `a` belongs in Contents and `a/` rolls up
        // into CommonPrefixes. The old cleaned-key rule grouped them and let the
        // object shadow the prefix dir, dropping the CommonPrefix — that was the
        // bug. Both must be emitted (object first, since raw `a` < `a/`).
        let (tx_a, rx_a) = mpsc::channel(4);
        let (tx_b, rx_b) = mpsc::channel(4);
        let (out_tx, mut out_rx) = mpsc::channel(8);

        tx_a.send(test_dir_meta_entry("a/")).await.unwrap();
        drop(tx_a);
        tx_b.send(test_object_meta_entry("a")).await.unwrap();
        drop(tx_b);

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(merge_entry_channels(cancel, vec![rx_a, rx_b], out_tx, 1));

        let mut results = Vec::new();
        while let Some(entry) = out_rx.recv().await {
            results.push((entry.name.clone(), entry.is_object(), entry.is_dir()));
        }
        handle.await.unwrap().unwrap();

        assert_eq!(
            results,
            vec![("a".to_owned(), true, false), ("a/".to_owned(), false, true)],
            "the object `a` and the prefix dir `a/` are distinct keys and must both survive"
        );
    }

    #[tokio::test]
    async fn merge_entry_channels_object_and_prefix_dir_coexist_from_one_channel() {
        // The object `a` and prefix dir `a/` can arrive sequentially on a single
        // channel (each drive is internally sorted, and raw `a` < `a/`). They are
        // then processed in separate merge iterations, so the per-key dedup must
        // still let both survive rather than collapsing the later prefix dir.
        let (tx, rx) = mpsc::channel(4);
        let (out_tx, mut out_rx) = mpsc::channel(8);

        tx.send(test_object_meta_entry("a")).await.unwrap();
        tx.send(test_dir_meta_entry("a/")).await.unwrap();
        drop(tx);

        // Two channels so the k-way merge path (not the single-channel fast
        // path) is exercised; the second channel is empty and closed up front so
        // the merge does not block waiting on it.
        let (tx_empty, rx_empty) = mpsc::channel(4);
        drop(tx_empty);
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(merge_entry_channels(cancel, vec![rx, rx_empty], out_tx, 1));

        let mut results = Vec::new();
        while let Some(entry) = out_rx.recv().await {
            results.push((entry.name.clone(), entry.is_object(), entry.is_dir()));
        }
        handle.await.unwrap().unwrap();

        assert_eq!(results, vec![("a".to_owned(), true, false), ("a/".to_owned(), false, true)],);
    }

    #[tokio::test]
    async fn merge_entry_channels_directory_marker_object_shadows_prefix_dir() {
        // An explicit "directory marker" object `folder/` (trailing slash, real
        // metadata) denotes the *same* S3 key as the prefix dir `folder/`, unlike
        // a plain object `a` vs `a/`. It must keep shadowing the prefix dir so the
        // key does not surface as both Contents and a CommonPrefix (issue #1797).
        let (tx_a, rx_a) = mpsc::channel(4);
        let (tx_b, rx_b) = mpsc::channel(4);
        let (out_tx, mut out_rx) = mpsc::channel(8);

        tx_a.send(test_dir_meta_entry("folder/")).await.unwrap();
        drop(tx_a);
        tx_b.send(test_object_meta_entry("folder/")).await.unwrap();
        drop(tx_b);

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(merge_entry_channels(cancel, vec![rx_a, rx_b], out_tx, 1));

        let mut results = Vec::new();
        while let Some(entry) = out_rx.recv().await {
            assert!(entry.is_object(), "the directory-marker object must shadow the same-named prefix dir");
            results.push(entry.name.clone());
        }
        handle.await.unwrap().unwrap();

        assert_eq!(results, vec!["folder/"]);
    }

    #[tokio::test]
    async fn merge_entry_channels_collapses_equivalent_prefix_dirs() {
        let (tx_a, rx_a) = mpsc::channel(4);
        let (tx_b, rx_b) = mpsc::channel(4);
        let (out_tx, mut out_rx) = mpsc::channel(8);

        tx_a.send(test_dir_meta_entry("x/")).await.unwrap();
        drop(tx_a);
        tx_b.send(test_dir_meta_entry("x/")).await.unwrap();
        drop(tx_b);

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(merge_entry_channels(cancel, vec![rx_a, rx_b], out_tx, 1));

        let mut results = Vec::new();
        while let Some(entry) = out_rx.recv().await {
            results.push(entry.name.clone());
        }
        handle.await.unwrap().unwrap();

        assert_eq!(results, vec!["x/"]);
    }

    #[tokio::test]
    async fn merge_entry_channels_normalizes_uncleaned_names_before_grouping() {
        let (tx_a, rx_a) = mpsc::channel(4);
        let (tx_b, rx_b) = mpsc::channel(4);
        let (out_tx, mut out_rx) = mpsc::channel(8);

        // Both names clean to "a/b"; they must merge into a single emission.
        tx_a.send(test_meta_entry("a//b")).await.unwrap();
        drop(tx_a);
        tx_b.send(test_meta_entry("a/b")).await.unwrap();
        drop(tx_b);

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(merge_entry_channels(cancel, vec![rx_a, rx_b], out_tx, 1));

        let mut results = Vec::new();
        while let Some(entry) = out_rx.recv().await {
            results.push(entry.name.clone());
        }
        handle.await.unwrap().unwrap();

        assert_eq!(results, vec!["a/b"]);
    }

    #[tokio::test]
    async fn merge_entry_channels_respects_cancellation() {
        let (tx, rx) = mpsc::channel::<MetaCacheEntry>(4);
        let (out_tx, mut out_rx) = mpsc::channel(8);

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        // Use a single channel so the cancellation check in tokio::select! is exercised.
        let handle = tokio::spawn(merge_entry_channels(cancel_clone, vec![rx], out_tx, 1));

        // Send an entry to prove the function is running and processing data.
        tx.send(test_meta_entry("a")).await.unwrap();
        let received = timeout(Duration::from_millis(500), out_rx.recv())
            .await
            .expect("should receive entry before cancellation")
            .map(|e| e.name);
        assert_eq!(received, Some("a".to_string()));

        // Cancel while the sender is still alive (channel is not closed).
        cancel.cancel();

        let result = timeout(Duration::from_secs(2), handle)
            .await
            .expect("merge should not hang after cancellation")
            .expect("task should not panic");
        assert!(result.is_ok(), "merge should return Ok on cancellation");

        // Keep tx alive until after the assertion so the channel doesn't close prematurely.
        drop(tx);
    }

    #[tokio::test]
    async fn merge_entry_channels_respects_cancellation_with_multiple_live_channels() {
        let (tx_a, rx_a) = mpsc::channel::<MetaCacheEntry>(4);
        let (tx_b, rx_b) = mpsc::channel::<MetaCacheEntry>(4);
        let (out_tx, _out_rx) = mpsc::channel(8);

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        let handle = tokio::spawn(merge_entry_channels(cancel_clone, vec![rx_a, rx_b], out_tx, 1));
        cancel.cancel();

        let result = timeout(Duration::from_secs(2), handle)
            .await
            .expect("multi-channel merge should not hang after cancellation")
            .expect("task should not panic");
        assert!(result.is_ok(), "multi-channel merge should return Ok on cancellation");

        drop(tx_a);
        drop(tx_b);
    }

    #[tokio::test]
    async fn merge_entry_channels_respects_cancellation_when_output_is_full() {
        let (tx_a, rx_a) = mpsc::channel::<MetaCacheEntry>(4);
        let (tx_b, rx_b) = mpsc::channel::<MetaCacheEntry>(4);
        let (out_tx, _out_rx) = mpsc::channel(1);

        out_tx
            .send(test_meta_entry("already-buffered"))
            .await
            .expect("output channel should accept initial entry");
        tx_a.send(test_meta_entry("a"))
            .await
            .expect("input channel a should accept entry");
        tx_b.send(test_meta_entry("b"))
            .await
            .expect("input channel b should accept entry");

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        let handle = tokio::spawn(merge_entry_channels(cancel_clone, vec![rx_a, rx_b], out_tx, 1));
        cancel.cancel();

        let result = timeout(Duration::from_secs(2), handle)
            .await
            .expect("merge should not hang when output is full and cancellation fires")
            .expect("task should not panic");
        assert!(result.is_ok(), "merge should return Ok when cancelled during output send");

        drop(tx_a);
        drop(tx_b);
    }

    #[tokio::test]
    async fn send_or_cancel_returns_false_when_cancelled_while_output_is_full() {
        let (out_tx, _out_rx) = mpsc::channel::<MetaCacheEntry>(1);
        out_tx
            .send(test_meta_entry("already-buffered"))
            .await
            .expect("output channel should accept initial entry");

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        let handle = tokio::spawn(async move { send_or_cancel(&cancel_clone, &out_tx, test_meta_entry("next")).await });
        cancel.cancel();

        let result = timeout(Duration::from_secs(2), handle)
            .await
            .expect("send_or_cancel should not hang when cancellation wins over backpressure")
            .expect("task should not panic")
            .expect("send_or_cancel should succeed");
        assert!(!result, "send_or_cancel should report cancellation instead of a successful send");
    }
}
