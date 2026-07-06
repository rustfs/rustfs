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

use crate::bucket::metadata_sys::get_versioning_config;
use crate::bucket::utils::check_list_objs_args;
use crate::bucket::versioning::VersioningApi;
use crate::cache_value::metacache_set::{FallbackClaimTracker, ListPathRawOptions, list_path_raw_with_claim_tracker};
use crate::core::sets::Sets;
use crate::disk::error::DiskError;
use crate::disk::{DiskAPI, DiskInfo, DiskStore, RUSTFS_META_BUCKET, WalkDirOptions};
use crate::error::{
    Error, Result, StorageError, is_all_not_found, is_all_volume_not_found, is_err_bucket_not_found, to_object_err,
};
use crate::object_api::{ObjectInfo, ObjectOptions};
use crate::set_disk::SetDisks;
use crate::storage_api_contracts::{
    list::{
        ListObjectsInfo as StorageListObjectsInfo, StorageListObjectVersionsInfo, StorageListObjectsV2Info,
        StorageObjectInfoOrErr, StorageWalkOptions, VersionMarker, WalkVersionsSortOrder,
    },
    object::ObjectOperations as _,
};
use crate::store::ECStore;
use crate::store::utils::is_reserved_or_invalid_bucket;
use bytes::Bytes;
use futures::future::join_all;
use rand::seq::SliceRandom;
use rustfs_filemeta::{
    FileMeta, FileMetaShallowVersion, MetaCacheEntries, MetaCacheEntriesSorted, MetaCacheEntriesSortedResult, MetaCacheEntry,
    MetacacheReader, MetadataResolutionParams, is_io_eof, merge_file_meta_versions,
};
use rustfs_io_metrics::{
    LIST_OBJECTS_GATHER_OUTCOME_INPUT_CLOSED, LIST_OBJECTS_GATHER_OUTCOME_LIMIT_REACHED, LIST_OBJECTS_SOURCE_WALKER,
    ListObjectsGatherObservation, ListObjectsIndexPageObservation,
};
use rustfs_utils::path::{self, SLASH_SEPARATOR, base_dir_from_prefix};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::duplex;
use tokio::sync::broadcast::{self};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{OnceCell, RwLock};
use tokio_util::sync::CancellationToken;
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

    for err in errs.iter().flatten() {
        if err == &Error::Unexpected || err.is_not_found() {
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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum GatherResultsState {
    LimitReached,
    InputClosed,
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
}

const MARKER_TAG_VERSION: &str = "v2";
const LEGACY_MARKER_TAG_VERSIONS: &[&str] = &["v1", MARKER_TAG_VERSION];
const LIST_CURSOR_SOURCE_WALKER: &str = "walker";
const LIST_CURSOR_SOURCE_INDEX_KEY_ONLY: &str = "index_key_only";
const LIST_CURSOR_SOURCE_INDEX_VERIFIED_PAGE: &str = "index_verified_page";
const LIST_CURSOR_SOURCE_INDEX_METADATA_FAST: &str = "index_metadata_fast";
const LIST_CURSOR_GENERATION_LIVE: &str = "live";
const ENV_API_LIST_QUORUM: &str = "RUSTFS_API_LIST_QUORUM";
const DEFAULT_API_LIST_QUORUM: &str = "strict";
const ENV_API_LIST_OBJECTS_QUORUM: &str = "RUSTFS_LIST_OBJECTS_QUORUM";
const DEFAULT_API_LIST_OBJECTS_QUORUM: &str = "optimal";
const ENV_API_LIST_OBJECTS_INDEX_MODE: &str = "RUSTFS_LIST_OBJECTS_INDEX_MODE";
const ENV_API_LIST_OBJECTS_INDEX_PROVIDER: &str = "RUSTFS_LIST_OBJECTS_INDEX_PROVIDER";
const ENV_API_LIST_OBJECTS_INDEX_PROVIDER_PATH: &str = "RUSTFS_LIST_OBJECTS_INDEX_PROVIDER_PATH";
const ENV_API_LIST_OBJECTS_INDEX_PROVIDER_GENERATION: &str = "RUSTFS_LIST_OBJECTS_INDEX_PROVIDER_GENERATION";
const ENV_API_LIST_OBJECTS_NAMESPACE_JOURNAL_PATH: &str = "RUSTFS_LIST_OBJECTS_NAMESPACE_JOURNAL_PATH";
const LIST_OBJECTS_INDEX_PROVIDER_WALKER_KEY_ONLY: &str = "walker_key_only";
const LIST_OBJECTS_INDEX_PROVIDER_PERSISTENT_KEY_ONLY: &str = "persistent_key_only";
const LIST_OBJECTS_INDEX_PROVIDER_PERSISTENT_KEY_ONLY_DEFAULT_GENERATION: &str = "persistent-key-only";
const PERSISTENT_KEY_ONLY_INDEX_HEADER: &str = "# rustfs-listobjects-key-only-v1";
const PERSISTENT_KEY_ONLY_INDEX_BUCKET_HEADER: &str = "# bucket=";
const PERSISTENT_KEY_ONLY_INDEX_GENERATION_HEADER: &str = "# generation=";
const PERSISTENT_KEY_ONLY_INDEX_CHECKPOINT_HEADER: &str = "# checkpoint_high_water_mark=";
const LIST_OBJECTS_NAMESPACE_JOURNAL_HEADER: &str = "# rustfs-listobjects-namespace-journal-v1";
const LIST_OBJECTS_NAMESPACE_JOURNAL_BUCKET_HEADER: &str = "# bucket=";
const LIST_OBJECTS_NAMESPACE_JOURNAL_HIGH_WATER_MARK_HEADER: &str = "# high_water_mark=";
const LIST_OBJECTS_NAMESPACE_JOURNAL_STATUS_HEADER: &str = "# status=";
const LIST_OBJECTS_NAMESPACE_JOURNAL_STATUS_HEALTHY: &str = "healthy";
const LIST_OBJECTS_NAMESPACE_JOURNAL_STATUS_DEGRADED: &str = "degraded";
const LIST_OBJECTS_NAMESPACE_JOURNAL_DEFAULT_DIR: &str = "namespace-mutation-journal";
const LIST_OBJECTS_NAMESPACE_JOURNAL_SYSTEM_PREFIX: &str = "listobjects/ns-journal/v1";

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
    fn cursor_value(self) -> &'static str {
        match self {
            Self::Walker => LIST_CURSOR_SOURCE_WALKER,
            Self::IndexKeyOnly => LIST_CURSOR_SOURCE_INDEX_KEY_ONLY,
            Self::IndexVerifiedPage => LIST_CURSOR_SOURCE_INDEX_VERIFIED_PAGE,
            Self::IndexMetadataFast => LIST_CURSOR_SOURCE_INDEX_METADATA_FAST,
        }
    }

    fn from_cursor_value(source: &str) -> Option<Self> {
        match source {
            LIST_CURSOR_SOURCE_WALKER => Some(Self::Walker),
            LIST_CURSOR_SOURCE_INDEX_KEY_ONLY => Some(Self::IndexKeyOnly),
            LIST_CURSOR_SOURCE_INDEX_VERIFIED_PAGE => Some(Self::IndexVerifiedPage),
            LIST_CURSOR_SOURCE_INDEX_METADATA_FAST => Some(Self::IndexMetadataFast),
            _ => None,
        }
    }

    fn metadata_authority(self) -> ListMetadataAuthority {
        match self {
            Self::Walker => ListMetadataAuthority::LiveXlMeta,
            Self::IndexKeyOnly | Self::IndexVerifiedPage => ListMetadataAuthority::LiveVerifiedIndexCandidate,
            Self::IndexMetadataFast => ListMetadataAuthority::IndexSnapshotEventuallyConsistent,
        }
    }

    fn requires_live_metadata_verification(self) -> bool {
        matches!(self, Self::IndexKeyOnly | Self::IndexVerifiedPage)
    }

    fn can_satisfy_strong_listing(self) -> bool {
        !matches!(self.metadata_authority(), ListMetadataAuthority::IndexSnapshotEventuallyConsistent)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListMetadataAuthority {
    /// Object metadata was read from live xl.meta and can be authoritative for
    /// strong ListObjects results.
    LiveXlMeta,
    /// The index provided candidate keys/pages; live xl.meta validation still
    /// owns authoritative object metadata.
    LiveVerifiedIndexCandidate,
    /// Metadata came from an index snapshot and must not be treated as strong
    /// consistency-equivalent to live xl.meta.
    IndexSnapshotEventuallyConsistent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListIndexFallbackReason {
    Disabled,
    Rebuilding,
    Unhealthy,
    Lagging,
    Degraded,
    Corrupt,
    MissingGeneration,
    GenerationMismatch,
    UnsupportedRequest,
}

impl ListIndexFallbackReason {
    fn metric_label(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Rebuilding => "rebuilding",
            Self::Unhealthy => "unhealthy",
            Self::Lagging => "lagging",
            Self::Degraded => "degraded",
            Self::Corrupt => "corrupt",
            Self::MissingGeneration => "missing_generation",
            Self::GenerationMismatch => "generation_mismatch",
            Self::UnsupportedRequest => "unsupported_request",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListObjectsIndexProviderKind {
    WalkerKeyOnly,
    PersistentKeyOnly,
}

impl ListObjectsIndexProviderKind {
    fn metric_label(self) -> &'static str {
        match self {
            Self::WalkerKeyOnly => LIST_OBJECTS_INDEX_PROVIDER_WALKER_KEY_ONLY,
            Self::PersistentKeyOnly => LIST_OBJECTS_INDEX_PROVIDER_PERSISTENT_KEY_ONLY,
        }
    }
}

fn list_objects_index_provider_metric_label(provider: Option<ListObjectsIndexProviderKind>) -> &'static str {
    provider.map(ListObjectsIndexProviderKind::metric_label).unwrap_or("none")
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ListObjectsIndexProviderState {
    kind: ListObjectsIndexProviderKind,
    lifecycle: ListIndexLifecycle,
    persistent_path: Option<PathBuf>,
    persistent_generation: Option<String>,
}

impl ListObjectsIndexProviderState {
    fn walker_key_only() -> Self {
        let mut lifecycle = ListIndexLifecycle::disabled(0);
        lifecycle.begin_rebuild(LIST_CURSOR_GENERATION_LIVE, 0);
        lifecycle.checkpoint_rebuild(0);
        let _ = lifecycle.publish_rebuild();
        Self {
            kind: ListObjectsIndexProviderKind::WalkerKeyOnly,
            lifecycle,
            persistent_path: None,
            persistent_generation: None,
        }
    }

    fn persistent_key_only(path: Option<PathBuf>, generation: Option<String>) -> Self {
        let mut lifecycle = ListIndexLifecycle::disabled(0);
        if path.as_ref().is_some_and(|path| !path.as_os_str().is_empty()) {
            lifecycle.begin_rebuild(
                generation
                    .as_deref()
                    .unwrap_or(LIST_OBJECTS_INDEX_PROVIDER_PERSISTENT_KEY_ONLY_DEFAULT_GENERATION),
                0,
            );
        } else {
            lifecycle.mark_degraded();
        }

        Self {
            kind: ListObjectsIndexProviderKind::PersistentKeyOnly,
            lifecycle,
            persistent_path: path,
            persistent_generation: generation,
        }
    }

    fn from_kind(kind: ListObjectsIndexProviderKind) -> Self {
        match kind {
            ListObjectsIndexProviderKind::WalkerKeyOnly => Self::walker_key_only(),
            ListObjectsIndexProviderKind::PersistentKeyOnly => Self::persistent_key_only(None, None),
        }
    }

    fn health_snapshot(&self) -> ListIndexHealthSnapshot {
        self.lifecycle.health_snapshot()
    }
}

#[derive(Debug, Clone)]
struct PersistentKeyOnlyIndexCache {
    path: PathBuf,
    len: u64,
    modified: Option<SystemTime>,
    index: Arc<PersistentKeyOnlyIndex>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PersistentKeyOnlyIndex {
    bucket: Option<String>,
    generation: String,
    checkpoint_high_water_mark: u64,
    keys: Arc<Vec<String>>,
}

static PERSISTENT_KEY_ONLY_INDEX_CACHE: OnceCell<RwLock<Option<PersistentKeyOnlyIndexCache>>> = OnceCell::const_new();
static LIST_OBJECTS_NAMESPACE_JOURNAL_LOCK: OnceCell<RwLock<()>> = OnceCell::const_new();
static LIST_OBJECTS_MUTATION_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static LIST_OBJECTS_BUCKET_MUTATION_SEQUENCE: OnceCell<RwLock<HashMap<String, u64>>> = OnceCell::const_new();
static LIST_OBJECTS_NAMESPACE_JOURNAL_DEGRADED_BUCKETS: OnceCell<RwLock<HashSet<String>>> = OnceCell::const_new();

async fn persistent_key_only_index_cache() -> &'static RwLock<Option<PersistentKeyOnlyIndexCache>> {
    PERSISTENT_KEY_ONLY_INDEX_CACHE
        .get_or_init(|| async { RwLock::new(None) })
        .await
}

async fn list_objects_namespace_journal_lock() -> &'static RwLock<()> {
    LIST_OBJECTS_NAMESPACE_JOURNAL_LOCK
        .get_or_init(|| async { RwLock::new(()) })
        .await
}

async fn list_objects_bucket_mutation_sequence() -> &'static RwLock<HashMap<String, u64>> {
    LIST_OBJECTS_BUCKET_MUTATION_SEQUENCE
        .get_or_init(|| async { RwLock::new(HashMap::new()) })
        .await
}

async fn list_objects_namespace_journal_degraded_buckets() -> &'static RwLock<HashSet<String>> {
    LIST_OBJECTS_NAMESPACE_JOURNAL_DEGRADED_BUCKETS
        .get_or_init(|| async { RwLock::new(HashSet::new()) })
        .await
}

fn advance_global_list_objects_mutation_sequence(sequence: u64) -> u64 {
    let mut current = LIST_OBJECTS_MUTATION_SEQUENCE.load(Ordering::Acquire);
    loop {
        if current >= sequence {
            return current;
        }
        match LIST_OBJECTS_MUTATION_SEQUENCE.compare_exchange(current, sequence, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return sequence,
            Err(observed) => current = observed,
        }
    }
}

async fn advance_list_objects_mutation_sequence(bucket: &str, sequence: u64) -> u64 {
    let sequence = advance_global_list_objects_mutation_sequence(sequence);
    let sequences = list_objects_bucket_mutation_sequence().await;
    let mut sequences = sequences.write().await;
    sequences
        .entry(bucket.to_owned())
        .and_modify(|current| *current = (*current).max(sequence))
        .or_insert(sequence);
    sequence
}

pub(super) async fn observe_list_objects_mutation(store: &ECStore, bucket: &str) -> u64 {
    match observe_list_objects_mutations(store, bucket, 1).await {
        Some(sequence) => sequence,
        None => 0,
    }
}

pub(super) async fn observe_list_objects_mutations(store: &ECStore, bucket: &str, count: usize) -> Option<u64> {
    observe_list_objects_mutations_with_store(Some(store), bucket, count).await
}

async fn observe_list_objects_mutations_with_store(store: Option<&ECStore>, bucket: &str, count: usize) -> Option<u64> {
    if count == 0 {
        return None;
    }

    let delta = match u64::try_from(count) {
        Ok(value) => value,
        Err(_) => u64::MAX,
    };
    let next = LIST_OBJECTS_MUTATION_SEQUENCE
        .fetch_add(delta, Ordering::AcqRel)
        .saturating_add(delta);
    let sequences = list_objects_bucket_mutation_sequence().await;
    let mut sequences = sequences.write().await;
    sequences
        .entry(bucket.to_owned())
        .and_modify(|current| *current = (*current).max(next))
        .or_insert(next);
    persist_observed_list_objects_mutation(store, bucket, next).await;
    Some(next)
}

async fn current_list_objects_mutation_sequence(bucket: &str) -> u64 {
    current_list_objects_mutation_snapshot(None, bucket).await.high_water_mark
}

#[cfg(test)]
async fn reset_list_objects_mutation_sequences_for_test() {
    LIST_OBJECTS_MUTATION_SEQUENCE.store(0, Ordering::Release);
    let sequences = list_objects_bucket_mutation_sequence().await;
    sequences.write().await.clear();
    let degraded = list_objects_namespace_journal_degraded_buckets().await;
    degraded.write().await.clear();
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NamespaceMutationJournalStatus {
    Healthy,
    Degraded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NamespaceMutationJournalState {
    bucket: Option<String>,
    high_water_mark: u64,
    status: NamespaceMutationJournalStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NamespaceMutationJournalSnapshot {
    high_water_mark: u64,
    degraded: bool,
}

#[derive(Clone)]
enum NamespaceMutationJournalBackend<'a> {
    System(&'a ECStore),
    LocalPath(PathBuf),
}

fn parse_namespace_mutation_journal_state(contents: &str) -> Option<NamespaceMutationJournalState> {
    let mut bucket = None;
    let mut high_water_mark = None;
    let mut status = NamespaceMutationJournalStatus::Healthy;

    for line in contents.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() || line == LIST_OBJECTS_NAMESPACE_JOURNAL_HEADER {
            continue;
        }
        if let Some(value) = line.strip_prefix(LIST_OBJECTS_NAMESPACE_JOURNAL_BUCKET_HEADER) {
            if !value.is_empty() {
                bucket = Some(value.to_owned());
            }
            continue;
        }
        if let Some(value) = line.strip_prefix(LIST_OBJECTS_NAMESPACE_JOURNAL_HIGH_WATER_MARK_HEADER) {
            high_water_mark = value.parse::<u64>().ok();
            continue;
        }
        if let Some(value) = line.strip_prefix(LIST_OBJECTS_NAMESPACE_JOURNAL_STATUS_HEADER) {
            status = match value {
                LIST_OBJECTS_NAMESPACE_JOURNAL_STATUS_HEALTHY => NamespaceMutationJournalStatus::Healthy,
                LIST_OBJECTS_NAMESPACE_JOURNAL_STATUS_DEGRADED => NamespaceMutationJournalStatus::Degraded,
                _ => return None,
            };
        }
    }

    high_water_mark.map(|high_water_mark| NamespaceMutationJournalState {
        bucket,
        high_water_mark,
        status,
    })
}

fn encode_namespace_mutation_journal_state(bucket: &str, sequence: u64, degraded: bool) -> String {
    let mut contents = String::new();
    contents.push_str(LIST_OBJECTS_NAMESPACE_JOURNAL_HEADER);
    contents.push('\n');
    contents.push_str(LIST_OBJECTS_NAMESPACE_JOURNAL_BUCKET_HEADER);
    contents.push_str(bucket);
    contents.push('\n');
    contents.push_str(LIST_OBJECTS_NAMESPACE_JOURNAL_HIGH_WATER_MARK_HEADER);
    contents.push_str(&sequence.to_string());
    contents.push('\n');
    contents.push_str(LIST_OBJECTS_NAMESPACE_JOURNAL_STATUS_HEADER);
    contents.push_str(if degraded {
        LIST_OBJECTS_NAMESPACE_JOURNAL_STATUS_DEGRADED
    } else {
        LIST_OBJECTS_NAMESPACE_JOURNAL_STATUS_HEALTHY
    });
    contents.push('\n');
    contents
}

fn namespace_mutation_journal_path(root: &Path, bucket: &str) -> PathBuf {
    root.join(format!("{bucket}.state"))
}

fn namespace_mutation_journal_system_path(bucket: &str) -> String {
    format!("{LIST_OBJECTS_NAMESPACE_JOURNAL_SYSTEM_PREFIX}/{bucket}/state")
}

fn list_objects_namespace_journal_root_from_env() -> Option<PathBuf> {
    std::env::var_os(ENV_API_LIST_OBJECTS_NAMESPACE_JOURNAL_PATH)
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
}

fn list_objects_namespace_journal_root_from_provider_path(provider_path: &Path) -> Option<PathBuf> {
    provider_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(|parent| parent.join(LIST_OBJECTS_NAMESPACE_JOURNAL_DEFAULT_DIR))
}

fn list_objects_namespace_journal_backend<'a>(
    store: Option<&'a ECStore>,
    provider_path: Option<&Path>,
) -> Option<NamespaceMutationJournalBackend<'a>> {
    if let Some(root) = list_objects_namespace_journal_root_from_env() {
        return Some(NamespaceMutationJournalBackend::LocalPath(root));
    }
    if let Some(store) = store {
        return Some(NamespaceMutationJournalBackend::System(store));
    }
    provider_path
        .and_then(list_objects_namespace_journal_root_from_provider_path)
        .map(NamespaceMutationJournalBackend::LocalPath)
}

fn list_objects_namespace_journal_write_quorum(total_disks: usize) -> usize {
    (total_disks / 2) + 1
}

fn validate_namespace_mutation_journal_state(bucket: &str, contents: &str) -> Result<NamespaceMutationJournalState> {
    let Some(state) = parse_namespace_mutation_journal_state(contents) else {
        return Err(Error::other("list objects namespace mutation journal state is corrupt"));
    };
    if state
        .bucket
        .as_deref()
        .is_some_and(|persisted_bucket| persisted_bucket != bucket)
    {
        return Err(Error::other("list objects namespace mutation journal bucket mismatch"));
    }
    Ok(state)
}

async fn system_namespace_mutation_journal_state_candidates(
    store: &ECStore,
    bucket: &str,
) -> Result<Vec<Option<NamespaceMutationJournalState>>> {
    if store.pools.is_empty() {
        return Err(Error::other("list objects namespace mutation journal has no storage pools"));
    }

    let path = namespace_mutation_journal_system_path(bucket);
    let mut states = Vec::with_capacity(store.pools.len());

    for pool in &store.pools {
        let set = pool.get_disks_by_key(bucket);
        let disks = set.disk_inventory().await;
        let quorum = list_objects_namespace_journal_write_quorum(set.set_drive_count);
        let mut successes = 0usize;
        let mut pool_state: Option<NamespaceMutationJournalState> = None;
        let mut futures = Vec::with_capacity(disks.len());

        for disk in disks.into_iter().flatten() {
            let path = path.clone();
            futures.push(async move { disk.read_all(RUSTFS_META_BUCKET, &path).await });
        }

        for result in join_all(futures).await {
            match result {
                Ok(bytes) => {
                    successes += 1;
                    let contents = String::from_utf8_lossy(&bytes);
                    let state = validate_namespace_mutation_journal_state(bucket, contents.as_ref())?;
                    match pool_state.as_ref() {
                        Some(current)
                            if current.high_water_mark > state.high_water_mark
                                || (current.high_water_mark == state.high_water_mark
                                    && current.status == NamespaceMutationJournalStatus::Degraded) => {}
                        _ => {
                            pool_state = Some(state);
                        }
                    }
                }
                Err(DiskError::FileNotFound) => {
                    successes += 1;
                }
                Err(err) => {
                    debug!(
                        bucket = %bucket,
                        path = %path,
                        error = %err,
                        "failed to read namespace mutation journal state from disk"
                    );
                }
            }
        }

        if successes < quorum {
            return Err(Error::other("list objects namespace mutation journal read quorum was not met"));
        }
        states.push(pool_state);
    }

    Ok(states)
}

async fn load_system_namespace_mutation_journal_state(
    store: &ECStore,
    bucket: &str,
) -> Result<Option<NamespaceMutationJournalState>> {
    let mut merged: Option<NamespaceMutationJournalState> = None;
    let mut saw_state = false;

    for state in system_namespace_mutation_journal_state_candidates(store, bucket).await? {
        let Some(state) = state else {
            continue;
        };
        saw_state = true;
        match merged.as_ref() {
            Some(current)
                if current.high_water_mark > state.high_water_mark
                    || (current.high_water_mark == state.high_water_mark
                        && current.status == NamespaceMutationJournalStatus::Degraded) => {}
            _ => merged = Some(state),
        }
    }

    Ok(saw_state.then_some(merged).flatten())
}

async fn write_system_namespace_mutation_journal_state(
    store: &ECStore,
    bucket: &str,
    sequence: u64,
    degraded: bool,
) -> Result<()> {
    if store.pools.is_empty() {
        return Err(Error::other("list objects namespace mutation journal has no storage pools"));
    }

    let path = namespace_mutation_journal_system_path(bucket);
    let contents = Bytes::from(encode_namespace_mutation_journal_state(bucket, sequence, degraded));

    for pool in &store.pools {
        let set = pool.get_disks_by_key(bucket);
        let disks = set.disk_inventory().await;
        let quorum = list_objects_namespace_journal_write_quorum(set.set_drive_count);
        let mut futures = Vec::with_capacity(disks.len());

        for disk in disks.into_iter().flatten() {
            let path = path.clone();
            let contents = contents.clone();
            futures.push(async move { disk.write_all(RUSTFS_META_BUCKET, &path, contents).await });
        }

        let successes = join_all(futures).await.into_iter().filter(|result| result.is_ok()).count();
        if successes < quorum {
            return Err(Error::other("list objects namespace mutation journal write quorum was not met"));
        }
    }

    Ok(())
}

async fn load_local_namespace_mutation_journal_state(root: &Path, bucket: &str) -> Result<Option<NamespaceMutationJournalState>> {
    let journal_path = namespace_mutation_journal_path(root, bucket);
    let lock = list_objects_namespace_journal_lock().await;
    // Journal IO is serialized and does not take provider/index locks, preserving high-water monotonicity.
    let _guard = lock.read().await;
    let contents = match tokio::fs::read_to_string(&journal_path).await {
        Ok(contents) => contents,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(Error::Io(err)),
    };
    validate_namespace_mutation_journal_state(bucket, &contents).map(Some)
}

async fn load_namespace_mutation_journal_state(
    backend: NamespaceMutationJournalBackend<'_>,
    bucket: &str,
) -> Result<Option<NamespaceMutationJournalState>> {
    match backend {
        NamespaceMutationJournalBackend::System(store) => load_system_namespace_mutation_journal_state(store, bucket).await,
        NamespaceMutationJournalBackend::LocalPath(root) => load_local_namespace_mutation_journal_state(&root, bucket).await,
    }
}

async fn write_local_namespace_mutation_journal_state(
    root: &Path,
    bucket: &str,
    sequence: u64,
    clear_degraded: bool,
) -> Result<NamespaceMutationJournalSnapshot> {
    let journal_path = namespace_mutation_journal_path(root, bucket);
    let lock = list_objects_namespace_journal_lock().await;
    // Journal IO is serialized and does not take provider/index locks, preserving high-water monotonicity.
    let guard = lock.write().await;

    let persisted_state = match tokio::fs::read_to_string(&journal_path).await {
        Ok(contents) => {
            let state = validate_namespace_mutation_journal_state(bucket, &contents)?;
            Some(state)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => return Err(Error::Io(err)),
    };
    let persisted_sequence = persisted_state.as_ref().map(|state| state.high_water_mark).unwrap_or(0);
    let degraded = persisted_state
        .as_ref()
        .is_some_and(|state| state.status == NamespaceMutationJournalStatus::Degraded)
        && !clear_degraded;
    let sequence = sequence.max(persisted_sequence);

    if let Some(parent) = journal_path.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await.map_err(Error::Io)?;
    }

    let contents = encode_namespace_mutation_journal_state(bucket, sequence, degraded);

    let tmp_path = journal_path.with_extension("tmp");
    tokio::fs::write(&tmp_path, contents).await.map_err(Error::Io)?;
    tokio::fs::rename(&tmp_path, &journal_path).await.map_err(Error::Io)?;
    drop(guard);
    if clear_degraded {
        let degraded = list_objects_namespace_journal_degraded_buckets().await;
        degraded.write().await.remove(bucket);
    }
    Ok(NamespaceMutationJournalSnapshot {
        high_water_mark: sequence,
        degraded,
    })
}

async fn write_namespace_mutation_journal_state(
    backend: NamespaceMutationJournalBackend<'_>,
    bucket: &str,
    sequence: u64,
    clear_degraded: bool,
) -> Result<NamespaceMutationJournalSnapshot> {
    let persisted_state = load_namespace_mutation_journal_state(backend.clone(), bucket).await?;
    let persisted_sequence = persisted_state.as_ref().map(|state| state.high_water_mark).unwrap_or(0);
    let degraded = persisted_state
        .as_ref()
        .is_some_and(|state| state.status == NamespaceMutationJournalStatus::Degraded)
        && !clear_degraded;
    let sequence = sequence.max(persisted_sequence);

    match backend {
        NamespaceMutationJournalBackend::System(store) => {
            write_system_namespace_mutation_journal_state(store, bucket, sequence, degraded).await?;
        }
        NamespaceMutationJournalBackend::LocalPath(root) => {
            return write_local_namespace_mutation_journal_state(&root, bucket, sequence, clear_degraded).await;
        }
    }

    if clear_degraded {
        let degraded = list_objects_namespace_journal_degraded_buckets().await;
        degraded.write().await.remove(bucket);
    }

    Ok(NamespaceMutationJournalSnapshot {
        high_water_mark: sequence,
        degraded,
    })
}

async fn mark_namespace_mutation_journal_degraded(store: Option<&ECStore>, bucket: &str, sequence: u64) {
    let degraded = list_objects_namespace_journal_degraded_buckets().await;
    degraded.write().await.insert(bucket.to_owned());

    if let Some(backend) = list_objects_namespace_journal_backend(store, None)
        && let Err(err) = write_namespace_mutation_journal_degraded_state(backend, bucket, sequence).await
    {
        warn!(
            bucket = %bucket,
            sequence,
            error = %err,
            "failed to persist degraded namespace mutation journal state"
        );
    }
}

async fn write_local_namespace_mutation_journal_degraded_state(root: &Path, bucket: &str, sequence: u64) -> Result<()> {
    let journal_path = namespace_mutation_journal_path(root, bucket);
    let lock = list_objects_namespace_journal_lock().await;
    // Journal IO is serialized and does not take provider/index locks, preserving high-water monotonicity.
    let _guard = lock.write().await;

    let persisted_sequence = match tokio::fs::read_to_string(&journal_path).await {
        Ok(contents) => parse_namespace_mutation_journal_state(&contents)
            .filter(|state| {
                state
                    .bucket
                    .as_deref()
                    .is_none_or(|persisted_bucket| persisted_bucket == bucket)
            })
            .map(|state| state.high_water_mark)
            .unwrap_or(0),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => 0,
        Err(err) => return Err(Error::Io(err)),
    };
    let sequence = sequence.max(persisted_sequence);

    if let Some(parent) = journal_path.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await.map_err(Error::Io)?;
    }

    let contents = encode_namespace_mutation_journal_state(bucket, sequence, true);

    let tmp_path = journal_path.with_extension("tmp");
    tokio::fs::write(&tmp_path, contents).await.map_err(Error::Io)?;
    tokio::fs::rename(&tmp_path, &journal_path).await.map_err(Error::Io)?;
    Ok(())
}

async fn write_namespace_mutation_journal_degraded_state(
    backend: NamespaceMutationJournalBackend<'_>,
    bucket: &str,
    sequence: u64,
) -> Result<()> {
    let persisted_state = load_namespace_mutation_journal_state(backend.clone(), bucket).await?;
    let sequence = sequence.max(persisted_state.map(|state| state.high_water_mark).unwrap_or(0));

    match backend {
        NamespaceMutationJournalBackend::System(store) => {
            write_system_namespace_mutation_journal_state(store, bucket, sequence, true).await
        }
        NamespaceMutationJournalBackend::LocalPath(root) => {
            write_local_namespace_mutation_journal_degraded_state(&root, bucket, sequence).await
        }
    }
}

async fn current_list_objects_mutation_snapshot(store: Option<&ECStore>, bucket: &str) -> NamespaceMutationJournalSnapshot {
    let sequences = list_objects_bucket_mutation_sequence().await;
    let in_memory_sequence = {
        let sequences = sequences.read().await;
        sequences.get(bucket).copied().unwrap_or(0)
    };

    let degraded = list_objects_namespace_journal_degraded_buckets().await;
    let in_memory_degraded = {
        let degraded = degraded.read().await;
        degraded.contains(bucket)
    };

    let Some(backend) = list_objects_namespace_journal_backend(store, None) else {
        return NamespaceMutationJournalSnapshot {
            high_water_mark: in_memory_sequence,
            degraded: in_memory_degraded,
        };
    };

    match load_namespace_mutation_journal_state(backend, bucket).await {
        Ok(Some(state)) => {
            advance_list_objects_mutation_sequence(bucket, state.high_water_mark).await;
            NamespaceMutationJournalSnapshot {
                high_water_mark: in_memory_sequence.max(state.high_water_mark),
                degraded: in_memory_degraded || state.status == NamespaceMutationJournalStatus::Degraded,
            }
        }
        Ok(None) => NamespaceMutationJournalSnapshot {
            high_water_mark: in_memory_sequence,
            degraded: in_memory_degraded,
        },
        Err(err) => {
            warn!(
                bucket = %bucket,
                error = %err,
                "list objects namespace mutation journal could not be loaded; falling back to walker"
            );
            let degraded = list_objects_namespace_journal_degraded_buckets().await;
            degraded.write().await.insert(bucket.to_owned());
            NamespaceMutationJournalSnapshot {
                high_water_mark: in_memory_sequence,
                degraded: true,
            }
        }
    }
}

async fn invalidate_persistent_key_only_index(path: &Path) {
    match tokio::fs::remove_file(path).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            warn!(
                index_path = %path.display(),
                error = %err,
                "failed to remove persistent key-only index after mutation checkpoint persist failure"
            );
        }
    }

    let cache = persistent_key_only_index_cache().await;
    let mut cached = cache.write().await;
    if cached.as_ref().is_some_and(|cached| cached.path == path) {
        *cached = None;
    }
}

async fn persist_observed_list_objects_mutation(store: Option<&ECStore>, bucket: &str, sequence: u64) {
    if is_reserved_or_invalid_bucket(bucket, false) {
        return;
    }

    let Some(path) = list_objects_index_provider_path_from_env().filter(|_| {
        matches!(
            list_objects_index_provider_from_env(),
            Some(ListObjectsIndexProviderKind::PersistentKeyOnly)
        )
    }) else {
        return;
    };

    let Some(backend) = list_objects_namespace_journal_backend(store, Some(&path)) else {
        return;
    };

    if let Err(err) = write_namespace_mutation_journal_state(backend, bucket, sequence, false).await {
        warn!(
            bucket = %bucket,
            sequence,
            error = %err,
            "namespace mutation journal commit failed; degrading persistent key-only provider"
        );
        mark_namespace_mutation_journal_degraded(store, bucket, sequence).await;
        invalidate_persistent_key_only_index(&path).await;
    }
}

fn parse_persistent_key_only_index(contents: &str) -> PersistentKeyOnlyIndex {
    let mut bucket = None;
    let mut generation = None;
    let mut checkpoint_high_water_mark = None;
    let mut keys = Vec::new();

    for line in contents.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        if let Some(value) = line.strip_prefix(PERSISTENT_KEY_ONLY_INDEX_BUCKET_HEADER) {
            if !value.is_empty() {
                bucket = Some(value.to_owned());
            }
            continue;
        }
        if let Some(value) = line.strip_prefix(PERSISTENT_KEY_ONLY_INDEX_GENERATION_HEADER) {
            if is_valid_list_objects_index_generation(value) {
                generation = Some(value.to_owned());
            }
            continue;
        }
        if let Some(value) = line.strip_prefix(PERSISTENT_KEY_ONLY_INDEX_CHECKPOINT_HEADER) {
            checkpoint_high_water_mark = value.parse::<u64>().ok();
            continue;
        }
        if line.starts_with('#') {
            continue;
        }
        keys.push(line.to_owned());
    }

    keys.sort_unstable();
    keys.dedup();
    let checkpoint_high_water_mark = checkpoint_high_water_mark.unwrap_or_else(|| match u64::try_from(keys.len()) {
        Ok(value) => value,
        Err(_) => u64::MAX,
    });

    PersistentKeyOnlyIndex {
        bucket,
        generation: generation.unwrap_or_else(|| LIST_OBJECTS_INDEX_PROVIDER_PERSISTENT_KEY_ONLY_DEFAULT_GENERATION.to_owned()),
        checkpoint_high_water_mark,
        keys: Arc::new(keys),
    }
}

fn persistent_key_only_index_health(
    index: &PersistentKeyOnlyIndex,
    mutation_snapshot: NamespaceMutationJournalSnapshot,
) -> ListIndexHealthSnapshot {
    let mut lifecycle = ListIndexLifecycle::disabled(0);
    let mutation_high_water_mark = mutation_snapshot.high_water_mark.max(index.checkpoint_high_water_mark);
    lifecycle.begin_rebuild(&index.generation, mutation_high_water_mark);
    lifecycle.checkpoint_rebuild(index.checkpoint_high_water_mark);
    let _ = lifecycle.publish_rebuild();
    if mutation_snapshot.degraded {
        lifecycle.mark_degraded();
        return lifecycle.health_snapshot();
    }
    lifecycle.observe_mutation_high_water_mark(mutation_high_water_mark);
    lifecycle.health_snapshot()
}

fn persistent_key_only_index_matches_provider(
    index: &PersistentKeyOnlyIndex,
    bucket: &str,
    provider_state: &ListObjectsIndexProviderState,
) -> bool {
    if index.bucket.as_deref().is_some_and(|index_bucket| index_bucket != bucket) {
        return false;
    }
    provider_state
        .persistent_generation
        .as_deref()
        .is_none_or(|generation| index.generation == generation)
}

fn generated_persistent_key_only_generation(configured: Option<&str>) -> String {
    if let Some(configured) = configured
        && is_valid_list_objects_index_generation(configured)
    {
        return configured.to_owned();
    }

    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    format!("{LIST_OBJECTS_INDEX_PROVIDER_PERSISTENT_KEY_ONLY_DEFAULT_GENERATION}-{millis}")
}

async fn write_persistent_key_only_index(
    store: Option<&ECStore>,
    path: &Path,
    bucket: &str,
    generation: &str,
    checkpoint_high_water_mark: u64,
    keys: &[String],
) -> Result<PersistentKeyOnlyIndex> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await.map_err(Error::Io)?;
    }

    let mut keys = keys.to_vec();
    keys.sort_unstable();
    keys.dedup();
    let mut contents = String::new();
    contents.push_str(PERSISTENT_KEY_ONLY_INDEX_HEADER);
    contents.push('\n');
    contents.push_str(PERSISTENT_KEY_ONLY_INDEX_BUCKET_HEADER);
    contents.push_str(bucket);
    contents.push('\n');
    contents.push_str(PERSISTENT_KEY_ONLY_INDEX_GENERATION_HEADER);
    contents.push_str(generation);
    contents.push('\n');
    contents.push_str(PERSISTENT_KEY_ONLY_INDEX_CHECKPOINT_HEADER);
    contents.push_str(&checkpoint_high_water_mark.to_string());
    contents.push('\n');
    for key in &keys {
        contents.push_str(key);
        contents.push('\n');
    }

    let Some(journal_backend) = list_objects_namespace_journal_backend(store, Some(path)) else {
        return Err(Error::other("list objects namespace mutation journal path is not configured"));
    };
    let tmp_path = path.with_extension("tmp");
    tokio::fs::write(&tmp_path, contents).await.map_err(Error::Io)?;
    write_namespace_mutation_journal_state(journal_backend, bucket, checkpoint_high_water_mark, true).await?;
    tokio::fs::rename(&tmp_path, path).await.map_err(Error::Io)?;

    Ok(PersistentKeyOnlyIndex {
        bucket: Some(bucket.to_owned()),
        generation: generation.to_owned(),
        checkpoint_high_water_mark,
        keys: Arc::new(keys),
    })
}

async fn load_persistent_key_only_index(store: Option<&ECStore>, path: &Path) -> Result<Arc<PersistentKeyOnlyIndex>> {
    let metadata = tokio::fs::metadata(path).await.map_err(Error::Io)?;
    let len = metadata.len();
    let modified = metadata.modified().ok();
    let cache = persistent_key_only_index_cache().await;

    {
        let cached = cache.read().await;
        if let Some(cached) = cached.as_ref()
            && cached.path == path
            && cached.len == len
            && cached.modified == modified
        {
            return Ok(cached.index.clone());
        }
    }

    let contents = tokio::fs::read_to_string(path).await.map_err(Error::Io)?;
    let index = Arc::new(parse_persistent_key_only_index(&contents));
    if let Some(bucket) = index.bucket.as_deref() {
        let restored_sequence = match list_objects_namespace_journal_backend(store, Some(path)) {
            Some(journal_backend) => load_namespace_mutation_journal_state(journal_backend, bucket)
                .await?
                .map(|state| state.high_water_mark)
                .unwrap_or(index.checkpoint_high_water_mark)
                .max(index.checkpoint_high_water_mark),
            None => index.checkpoint_high_water_mark,
        };
        advance_list_objects_mutation_sequence(bucket, restored_sequence).await;
    }
    let mut cached = cache.write().await;
    *cached = Some(PersistentKeyOnlyIndexCache {
        path: path.to_path_buf(),
        len,
        modified,
        index: index.clone(),
    });
    Ok(index)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListIndexSourceDecision {
    UseIndex(ListSourceMode),
    FallbackToWalker(ListIndexFallbackReason),
}

trait ListMetadataIndexGeneration {
    fn generation_id(&self) -> &str;
}

trait ListMetadataIndexHealth {
    fn fallback_reason(&self) -> Option<ListIndexFallbackReason>;

    fn is_healthy(&self) -> bool {
        self.fallback_reason().is_none()
    }
}

trait ListMetadataIndexPage {
    fn source_mode(&self) -> ListSourceMode;
    fn generation(&self) -> &dyn ListMetadataIndexGeneration;
    fn keys(&self) -> &[String];

    fn metadata_authority(&self) -> ListMetadataAuthority {
        self.source_mode().metadata_authority()
    }
}

trait ListMetadataIndexPageIterator {
    type Page: ListMetadataIndexPage;

    fn next_page(&mut self) -> Option<Self::Page>;
}

trait ListMetadataIndexKeyLookup {
    type Page: ListMetadataIndexPage;

    fn lookup_page(&self, opts: &ListPathOptions) -> ListIndexSourceDecision;
}

fn select_list_index_provider_source_mode(
    opts: &ListPathOptions,
    requested: ListSourceMode,
    health: &ListIndexHealthSnapshot,
) -> ListIndexSourceDecision {
    let mut parsed_opts = opts.clone();
    parsed_opts.parse_marker();

    if parsed_opts
        .cursor_source
        .is_some_and(|source| source != requested && source != ListSourceMode::Walker)
    {
        return ListIndexSourceDecision::FallbackToWalker(ListIndexFallbackReason::UnsupportedRequest);
    }

    if let Some(cursor_generation) = parsed_opts.cursor_generation.as_deref()
        && health
            .active_generation
            .as_deref()
            .is_some_and(|active_generation| cursor_generation != active_generation)
    {
        return ListIndexSourceDecision::FallbackToWalker(ListIndexFallbackReason::GenerationMismatch);
    }

    select_list_index_source_mode(Some(requested), health.active_generation.as_deref(), health)
}

fn select_list_index_source_mode(
    requested: Option<ListSourceMode>,
    generation: Option<&str>,
    health: &dyn ListMetadataIndexHealth,
) -> ListIndexSourceDecision {
    let Some(mode) = requested else {
        return ListIndexSourceDecision::FallbackToWalker(ListIndexFallbackReason::Disabled);
    };

    if mode == ListSourceMode::Walker {
        return ListIndexSourceDecision::FallbackToWalker(ListIndexFallbackReason::Disabled);
    }

    if generation.is_none_or(str::is_empty) {
        return ListIndexSourceDecision::FallbackToWalker(ListIndexFallbackReason::MissingGeneration);
    }

    if let Some(reason) = health.fallback_reason() {
        return ListIndexSourceDecision::FallbackToWalker(reason);
    }

    ListIndexSourceDecision::UseIndex(mode)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListIndexLifecycleState {
    Disabled,
    Rebuilding,
    Healthy,
    Lagging,
    Degraded,
    Corrupt,
}

impl ListIndexLifecycleState {
    fn fallback_reason(self) -> Option<ListIndexFallbackReason> {
        match self {
            Self::Disabled => Some(ListIndexFallbackReason::Disabled),
            Self::Rebuilding => Some(ListIndexFallbackReason::Rebuilding),
            Self::Healthy => None,
            Self::Lagging => Some(ListIndexFallbackReason::Lagging),
            Self::Degraded => Some(ListIndexFallbackReason::Degraded),
            Self::Corrupt => Some(ListIndexFallbackReason::Corrupt),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ListIndexHealthSnapshot {
    state: ListIndexLifecycleState,
    active_generation: Option<String>,
    staging_generation: Option<String>,
    checkpoint_high_water_mark: u64,
    mutation_high_water_mark: u64,
    mutation_lag: u64,
    fallback_reason: Option<ListIndexFallbackReason>,
}

impl ListMetadataIndexHealth for ListIndexHealthSnapshot {
    fn fallback_reason(&self) -> Option<ListIndexFallbackReason> {
        self.fallback_reason
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ListIndexLifecycle {
    state: ListIndexLifecycleState,
    active_generation: Option<String>,
    staging_generation: Option<String>,
    checkpoint_high_water_mark: u64,
    mutation_high_water_mark: u64,
    max_allowed_lag: u64,
}

impl ListIndexLifecycle {
    fn disabled(max_allowed_lag: u64) -> Self {
        Self {
            state: ListIndexLifecycleState::Disabled,
            active_generation: None,
            staging_generation: None,
            checkpoint_high_water_mark: 0,
            mutation_high_water_mark: 0,
            max_allowed_lag,
        }
    }

    fn begin_rebuild(&mut self, generation: impl Into<String>, current_high_water_mark: u64) {
        self.state = ListIndexLifecycleState::Rebuilding;
        self.staging_generation = Some(generation.into());
        self.mutation_high_water_mark = current_high_water_mark;
    }

    fn checkpoint_rebuild(&mut self, checkpoint_high_water_mark: u64) {
        if self.state == ListIndexLifecycleState::Rebuilding && self.staging_generation.is_some() {
            self.checkpoint_high_water_mark = checkpoint_high_water_mark;
        }
    }

    fn publish_rebuild(&mut self) -> bool {
        let Some(generation) = self.staging_generation.take() else {
            return false;
        };

        self.active_generation = Some(generation);
        self.state = ListIndexLifecycleState::Healthy;
        self.mutation_high_water_mark = self.checkpoint_high_water_mark;
        true
    }

    fn recover_after_restart(mut self) -> Self {
        self.staging_generation = None;
        self.state = if self.active_generation.is_some() {
            ListIndexLifecycleState::Healthy
        } else {
            ListIndexLifecycleState::Disabled
        };
        self
    }

    fn observe_mutation_high_water_mark(&mut self, mutation_high_water_mark: u64) {
        self.mutation_high_water_mark = self.mutation_high_water_mark.max(mutation_high_water_mark);
        if matches!(self.state, ListIndexLifecycleState::Healthy | ListIndexLifecycleState::Lagging)
            && self.mutation_lag() > self.max_allowed_lag
        {
            self.state = ListIndexLifecycleState::Lagging;
        }
    }

    fn mark_degraded(&mut self) {
        self.state = ListIndexLifecycleState::Degraded;
    }

    fn mark_corrupt(&mut self) {
        self.state = ListIndexLifecycleState::Corrupt;
    }

    fn mutation_lag(&self) -> u64 {
        self.mutation_high_water_mark.saturating_sub(self.checkpoint_high_water_mark)
    }

    fn health_snapshot(&self) -> ListIndexHealthSnapshot {
        ListIndexHealthSnapshot {
            state: self.state,
            active_generation: self.active_generation.clone(),
            staging_generation: self.staging_generation.clone(),
            checkpoint_high_water_mark: self.checkpoint_high_water_mark,
            mutation_high_water_mark: self.mutation_high_water_mark,
            mutation_lag: self.mutation_lag(),
            fallback_reason: self.state.fallback_reason(),
        }
    }
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
    fn encode_marker(&self, marker: &str) -> String {
        let mut marker_tag = String::with_capacity(marker.len() + 64);
        marker_tag.push_str(marker);
        marker_tag.push_str("[rustfs_cache:");
        marker_tag.push_str(self.version);
        match &self.id {
            Some(id) => {
                marker_tag.push_str(",id:");
                marker_tag.push_str(id);
            }
            None => marker_tag.push_str(",return:"),
        }
        if let Some(pool_idx) = self.pool_idx {
            marker_tag.push_str(",p:");
            marker_tag.push_str(&pool_idx.to_string());
        }
        if let Some(set_idx) = self.set_idx {
            marker_tag.push_str(",s:");
            marker_tag.push_str(&set_idx.to_string());
        }
        if let Some(source) = &self.source {
            marker_tag.push_str(",src:");
            marker_tag.push_str(source.cursor_value());
        }
        if let Some(generation) = &self.generation {
            marker_tag.push_str(",gen:");
            marker_tag.push_str(generation);
        }
        marker_tag.push(']');
        marker_tag
    }

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

fn list_quorum_from_env() -> String {
    let value = rustfs_utils::get_env_str(ENV_API_LIST_QUORUM, DEFAULT_API_LIST_QUORUM);
    normalize_list_quorum(&value).to_owned()
}

fn list_objects_quorum_from_env() -> String {
    let value = rustfs_utils::get_env_str(ENV_API_LIST_OBJECTS_QUORUM, DEFAULT_API_LIST_OBJECTS_QUORUM);
    normalize_list_quorum(&value).to_owned()
}

fn list_objects_index_mode_from_env() -> Option<ListSourceMode> {
    let value = rustfs_utils::get_env_str(ENV_API_LIST_OBJECTS_INDEX_MODE, "");
    let value = value.trim();
    if value.eq_ignore_ascii_case(LIST_CURSOR_SOURCE_INDEX_KEY_ONLY) || value.eq_ignore_ascii_case("key_only") {
        Some(ListSourceMode::IndexKeyOnly)
    } else if value.eq_ignore_ascii_case(LIST_CURSOR_SOURCE_INDEX_VERIFIED_PAGE) || value.eq_ignore_ascii_case("verified_page") {
        Some(ListSourceMode::IndexVerifiedPage)
    } else {
        None
    }
}

fn list_objects_index_provider_from_env() -> Option<ListObjectsIndexProviderKind> {
    let value = rustfs_utils::get_env_str(ENV_API_LIST_OBJECTS_INDEX_PROVIDER, "");
    let value = value.trim();
    if value.eq_ignore_ascii_case(LIST_OBJECTS_INDEX_PROVIDER_WALKER_KEY_ONLY) {
        Some(ListObjectsIndexProviderKind::WalkerKeyOnly)
    } else if value.eq_ignore_ascii_case(LIST_OBJECTS_INDEX_PROVIDER_PERSISTENT_KEY_ONLY)
        || value.eq_ignore_ascii_case("persisted_key_only")
    {
        Some(ListObjectsIndexProviderKind::PersistentKeyOnly)
    } else {
        None
    }
}

fn list_objects_index_provider_path_from_env() -> Option<PathBuf> {
    let value = rustfs_utils::get_env_str(ENV_API_LIST_OBJECTS_INDEX_PROVIDER_PATH, "");
    let value = value.trim();
    if value.is_empty() { None } else { Some(PathBuf::from(value)) }
}

fn is_valid_list_objects_index_generation(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty() && !value.contains([',', ':', '[', ']'])
}

fn list_objects_index_provider_generation_from_env() -> Option<String> {
    let value = rustfs_utils::get_env_str(ENV_API_LIST_OBJECTS_INDEX_PROVIDER_GENERATION, "");
    let value = value.trim();
    is_valid_list_objects_index_generation(value).then(|| value.to_owned())
}

fn list_objects_index_provider_state_from_env() -> Option<ListObjectsIndexProviderState> {
    list_objects_index_provider_from_env().map(|kind| match kind {
        ListObjectsIndexProviderKind::WalkerKeyOnly => ListObjectsIndexProviderState::walker_key_only(),
        ListObjectsIndexProviderKind::PersistentKeyOnly => ListObjectsIndexProviderState::persistent_key_only(
            list_objects_index_provider_path_from_env(),
            list_objects_index_provider_generation_from_env(),
        ),
    })
}

fn list_objects_key_only_provider_health(provider: Option<ListObjectsIndexProviderKind>) -> ListIndexHealthSnapshot {
    provider
        .map(ListObjectsIndexProviderState::from_kind)
        .map(|state| state.health_snapshot())
        .unwrap_or_else(|| ListIndexLifecycle::disabled(0).health_snapshot())
}

fn record_list_objects_index_fallback(mode: ListSourceMode, reason: ListIndexFallbackReason) {
    rustfs_io_metrics::record_list_objects_index_fallback(mode.cursor_value(), reason.metric_label());
}

fn record_list_objects_index_opt_in_fallback(opts: &ListPathOptions, mode: ListSourceMode) {
    if !rustfs_io_metrics::get_stage_metrics_enabled() {
        return;
    }

    let health = list_objects_key_only_provider_health(None);
    let reason = match select_list_index_provider_source_mode(opts, mode, &health) {
        ListIndexSourceDecision::FallbackToWalker(reason) => reason,
        ListIndexSourceDecision::UseIndex(_) => ListIndexFallbackReason::UnsupportedRequest,
    };

    record_list_objects_index_fallback(mode, reason);
    debug!(
        bucket = %opts.bucket,
        prefix = %opts.prefix,
        source = mode.cursor_value(),
        reason = reason.metric_label(),
        "list_objects opt-in index path fell back to walker"
    );
}

fn append_list_cache_id_to_marker(marker: String, cache_id: Option<&str>) -> String {
    let Some(id) = cache_id else {
        return marker;
    };

    ListContinuationV2 {
        version: MARKER_TAG_VERSION,
        id: Some(id.to_owned()),
        pool_idx: None,
        set_idx: None,
        source: Some(ListSourceMode::Walker),
        generation: Some(LIST_CURSOR_GENERATION_LIVE.to_owned()),
    }
    .encode_marker(&marker)
}

fn build_list_next_marker(objects: &[ObjectInfo], prefixes: &[String], cache_id: Option<&str>) -> Option<String> {
    if let Some(last) = objects.last() {
        Some(append_list_cache_id_to_marker(last.name.clone(), cache_id))
    } else {
        prefixes
            .last()
            .map(|marker| append_list_cache_id_to_marker(marker.clone(), cache_id))
    }
}

fn build_list_versions_next_marker(
    objects: &[ObjectInfo],
    prefixes: &[String],
    cache_id: Option<&str>,
) -> (Option<String>, Option<String>) {
    if let Some(last) = objects.last() {
        (
            Some(append_list_cache_id_to_marker(last.name.clone(), cache_id)),
            Some(last.version_id.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string())),
        )
    } else if let Some(last_prefix) = prefixes.last() {
        (Some(append_list_cache_id_to_marker(last_prefix.clone(), cache_id)), None)
    } else {
        (None, None)
    }
}

fn list_objects_paginate(
    get_objects: Vec<ObjectInfo>,
    delimiter: &Option<String>,
    max_keys: i32,
    disk_has_more: bool,
    cache_id: Option<&str>,
    include_version_id: bool,
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
            (next_marker, next_version_idmarker) = build_list_versions_next_marker(&get_objects, &[], cache_id);
        } else {
            next_marker = build_list_next_marker(&get_objects, &[], cache_id);
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
        if should_truncate {
            is_truncated = true;
            if include_version_id {
                (next_marker, next_version_idmarker) = build_list_versions_next_marker(&objects, &prefixes, cache_id);
            } else {
                next_marker = build_list_next_marker(&objects, &prefixes, cache_id);
            }
        }
    }

    (objects, prefixes, is_truncated, next_marker, next_version_idmarker)
}

enum VerifiedIndexVisibleEntry {
    Object(ObjectInfo),
    Prefix(String),
}

impl VerifiedIndexVisibleEntry {
    fn marker(&self) -> &str {
        match self {
            Self::Object(object) => &object.name,
            Self::Prefix(prefix) => prefix,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct VerifiedIndexCandidateStats {
    candidate_keys: usize,
    skipped_keys: usize,
    common_prefixes: usize,
    live_verify_attempts: usize,
    live_verify_hits: usize,
    live_verify_misses: usize,
}

struct VerifiedIndexCandidateResult {
    info: ListObjectsInfo,
    stats: VerifiedIndexCandidateStats,
}

async fn list_objects_from_verified_index_candidates<F, Fut>(
    prefix: &str,
    marker: Option<&str>,
    delimiter: &Option<String>,
    max_keys: i32,
    candidate_keys: &[String],
    live_verify: F,
) -> Result<ListObjectsInfo>
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Result<Option<ObjectInfo>>>,
{
    Ok(list_objects_from_verified_index_candidates_with_optional_stats(
        prefix,
        marker,
        delimiter,
        max_keys,
        candidate_keys,
        false,
        live_verify,
    )
    .await?
    .info)
}

async fn list_objects_from_verified_index_candidates_with_stats<F, Fut>(
    prefix: &str,
    marker: Option<&str>,
    delimiter: &Option<String>,
    max_keys: i32,
    candidate_keys: &[String],
    live_verify: F,
) -> Result<VerifiedIndexCandidateResult>
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Result<Option<ObjectInfo>>>,
{
    list_objects_from_verified_index_candidates_with_optional_stats(
        prefix,
        marker,
        delimiter,
        max_keys,
        candidate_keys,
        true,
        live_verify,
    )
    .await
}

async fn list_objects_from_verified_index_candidates_with_optional_stats<F, Fut>(
    prefix: &str,
    marker: Option<&str>,
    delimiter: &Option<String>,
    max_keys: i32,
    candidate_keys: &[String],
    collect_stats: bool,
    mut live_verify: F,
) -> Result<VerifiedIndexCandidateResult>
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Result<Option<ObjectInfo>>>,
{
    let max_keys = normalize_max_keys(max_keys);
    let mut stats = if collect_stats {
        VerifiedIndexCandidateStats {
            candidate_keys: candidate_keys.len(),
            ..Default::default()
        }
    } else {
        VerifiedIndexCandidateStats::default()
    };
    if max_keys <= 0 {
        return Ok(VerifiedIndexCandidateResult {
            info: ListObjectsInfo::default(),
            stats,
        });
    }

    let page_limit = usize::try_from(max_keys_plus_one(max_keys, true)).unwrap_or(usize::MAX);
    let mut visible_entries = Vec::new();
    let mut prefix_set = HashSet::new();

    for key in candidate_keys {
        if marker.is_some_and(|marker| key.as_str() <= marker) || !key.starts_with(prefix) {
            if collect_stats {
                stats.skipped_keys += 1;
            }
            continue;
        }

        if let Some(separator) = delimiter
            && !separator.is_empty()
        {
            let suffix = key.trim_start_matches(prefix);
            if let Some((common_prefix, _)) = suffix.split_once(separator) {
                let common_prefix = format!("{prefix}{common_prefix}{separator}");
                if prefix_set.insert(common_prefix.clone()) {
                    if collect_stats {
                        stats.common_prefixes += 1;
                    }
                    visible_entries.push(VerifiedIndexVisibleEntry::Prefix(common_prefix));
                }
                if visible_entries.len() >= page_limit {
                    break;
                }
                continue;
            }
        }

        if collect_stats {
            stats.live_verify_attempts += 1;
        }
        match live_verify(key.clone()).await? {
            Some(object) => {
                if collect_stats {
                    stats.live_verify_hits += 1;
                }
                visible_entries.push(VerifiedIndexVisibleEntry::Object(object));
                if visible_entries.len() >= page_limit {
                    break;
                }
            }
            None => {
                if collect_stats {
                    stats.live_verify_misses += 1;
                }
            }
        }
    }

    let is_truncated = visible_entries.len() > max_keys as usize;
    if is_truncated {
        visible_entries.truncate(max_keys as usize);
    }

    let next_marker = if is_truncated {
        visible_entries.last().map(|entry| entry.marker().to_owned())
    } else {
        None
    };

    let mut objects = Vec::new();
    let mut prefixes = Vec::new();
    for entry in visible_entries {
        match entry {
            VerifiedIndexVisibleEntry::Object(object) => objects.push(object),
            VerifiedIndexVisibleEntry::Prefix(prefix) => prefixes.push(prefix),
        }
    }

    Ok(VerifiedIndexCandidateResult {
        info: ListObjectsInfo {
            is_truncated,
            next_marker,
            objects,
            prefixes,
        },
        stats,
    })
}

fn list_metadata_resolution_params(
    bucket: String,
    listing_quorum: usize,
    latest_object_quorum: usize,
    versioned: bool,
) -> MetadataResolutionParams {
    let quorum = if versioned {
        listing_quorum
    } else {
        latest_object_quorum.max(listing_quorum)
    };
    let mut resolver = MetadataResolutionParams {
        dir_quorum: quorum,
        obj_quorum: quorum,
        bucket,
        ..Default::default()
    };

    if !versioned {
        resolver.requested_versions = 1;
    }

    resolver
}

fn resolve_listing_entries(
    entries: MetaCacheEntries,
    resolver: MetadataResolutionParams,
    enforce_write_quorum: bool,
) -> Option<MetaCacheEntry> {
    if enforce_write_quorum {
        entries.resolve_with_write_quorum(resolver)
    } else {
        entries.resolve(resolver)
    }
}

enum ListingEntryResolution {
    Resolved(MetaCacheEntry),
    NeedsSupplement(MetaCacheEntry, Option<MetaCacheEntry>),
    Rejected,
}

#[derive(Clone)]
struct FallbackListingEntry {
    endpoint: String,
    entry: MetaCacheEntry,
}

type FallbackListingEntries = HashMap<String, Vec<FallbackListingEntry>>;

#[derive(Clone)]
struct ListingSupplementOptions {
    bucket: String,
    path: String,
    recursive: bool,
    filter_prefix: Option<String>,
    forward_to: Option<String>,
    per_disk_limit: i32,
    skip_total_timeout: bool,
}

struct ListingSupplement {
    options: ListingSupplementOptions,
    fallback_disks: Arc<Vec<DiskStore>>,
    claim_tracker: FallbackClaimTracker,
    entries: Option<Arc<OnceCell<FallbackListingEntries>>>,
}

impl ListingSupplement {
    fn new(
        options: ListingSupplementOptions,
        fallback_disks: Arc<Vec<DiskStore>>,
        claim_tracker: FallbackClaimTracker,
    ) -> Arc<Self> {
        let entries = if options.per_disk_limit > 0 {
            Some(Arc::new(OnceCell::new()))
        } else {
            None
        };

        Arc::new(Self {
            options,
            fallback_disks,
            claim_tracker,
            entries,
        })
    }

    fn is_empty(&self) -> bool {
        self.fallback_disks.is_empty()
    }

    async fn entries_for(&self, object: &str) -> Vec<Option<MetaCacheEntry>> {
        if self.fallback_disks.is_empty() {
            return Vec::new();
        }

        let claimed_keys = self.claim_tracker.claimed_keys().await;
        let Some(entries) = &self.entries else {
            return self.read_object_entries(object, &claimed_keys).await;
        };

        let entries = entries.get_or_init(|| self.load_entries()).await;
        let claimed_keys = self.claim_tracker.claimed_keys().await;
        fallback_entries_for_object(entries, object, &claimed_keys)
    }

    async fn load_entries(&self) -> FallbackListingEntries {
        let claimed_keys = self.claim_tracker.claimed_keys().await;
        let futures = self.fallback_disks.iter().filter_map(|disk| {
            let endpoint = disk.endpoint().to_string();
            if claimed_keys.contains(&endpoint) {
                return None;
            }

            Some(read_fallback_listing_disk(disk.clone(), endpoint, self.options.clone()))
        });

        let per_disk_entries = join_all(futures).await;
        let entry_count = per_disk_entries.iter().map(Vec::len).sum();
        let mut entries = FallbackListingEntries::with_capacity(entry_count);
        for disk_entries in per_disk_entries {
            for entry in disk_entries {
                entries.entry(entry.entry.name.clone()).or_default().push(entry);
            }
        }

        entries
    }

    async fn read_object_entries(&self, object: &str, claimed_keys: &HashSet<String>) -> Vec<Option<MetaCacheEntry>> {
        let futures = self.fallback_disks.iter().filter_map(|disk| {
            let endpoint = disk.endpoint().to_string();
            if claimed_keys.contains(&endpoint) {
                return None;
            }

            Some(read_fallback_object_disk(
                disk.clone(),
                endpoint,
                self.options.bucket.clone(),
                object.to_owned(),
            ))
        });

        let entries = join_all(futures).await;
        let claimed_keys = self.claim_tracker.claimed_keys().await;
        entries
            .into_iter()
            .flatten()
            .filter(|entry| !claimed_keys.contains(&entry.endpoint))
            .map(|entry| Some(entry.entry))
            .collect()
    }
}

fn fallback_entries_for_object(
    entries: &FallbackListingEntries,
    object: &str,
    claimed_keys: &HashSet<String>,
) -> Vec<Option<MetaCacheEntry>> {
    entries
        .get(object)
        .into_iter()
        .flat_map(|entries| entries.iter())
        .filter(|entry| !claimed_keys.contains(&entry.endpoint))
        .map(|entry| Some(entry.entry.clone()))
        .collect()
}

async fn read_fallback_listing_disk(
    disk: DiskStore,
    endpoint: String,
    options: ListingSupplementOptions,
) -> Vec<FallbackListingEntry> {
    let (rd, mut wr) = duplex(64);
    let walk_endpoint = endpoint.clone();
    let walk_job = tokio::spawn(async move {
        disk.walk_dir(
            WalkDirOptions {
                bucket: options.bucket,
                base_dir: options.path,
                recursive: options.recursive,
                report_notfound: false,
                filter_prefix: options.filter_prefix,
                forward_to: options.forward_to,
                limit: options.per_disk_limit,
                skip_total_timeout: options.skip_total_timeout,
                ..Default::default()
            },
            &mut wr,
        )
        .await
    });

    let mut reader = MetacacheReader::new(rd);
    let mut entries = Vec::new();
    let mut stream_ok = true;
    loop {
        match reader.peek().await {
            Ok(Some(entry)) => {
                entries.push(FallbackListingEntry {
                    endpoint: endpoint.clone(),
                    entry,
                });
                if let Err(err) = reader.skip(1).await {
                    debug!(
                        endpoint = %endpoint,
                        error = ?err,
                        "fallback listing supplement stream failed while advancing reader"
                    );
                    stream_ok = false;
                    break;
                }
            }
            Ok(None) => break,
            Err(err) if err == rustfs_filemeta::Error::Unexpected || is_io_eof(&err) => break,
            Err(err) => {
                debug!(
                    endpoint = %endpoint,
                    error = ?err,
                    "fallback listing supplement stream failed while reading entry"
                );
                stream_ok = false;
                break;
            }
        }
    }
    drop(reader);

    match walk_job.await {
        Ok(Ok(())) if stream_ok => entries,
        Ok(Ok(())) => Vec::new(),
        Ok(Err(err)) => {
            debug!(
                endpoint = %walk_endpoint,
                error = ?err,
                "fallback listing supplement walk_dir failed"
            );
            Vec::new()
        }
        Err(err) => {
            debug!(
                endpoint = %walk_endpoint,
                error = ?err,
                "fallback listing supplement task failed"
            );
            Vec::new()
        }
    }
}

async fn read_fallback_object_disk(
    disk: DiskStore,
    endpoint: String,
    bucket: String,
    object: String,
) -> Option<FallbackListingEntry> {
    match disk.read_xl(&bucket, &object, false).await {
        Ok(raw) if raw.buf.is_empty() => None,
        Ok(raw) => Some(FallbackListingEntry {
            endpoint,
            entry: MetaCacheEntry {
                name: object,
                metadata: raw.buf,
                cached: None,
                reusable: false,
            },
        }),
        Err(err) if DiskError::is_err_object_not_found(&err) || DiskError::is_err_version_not_found(&err) => None,
        Err(err) => {
            debug!(
                endpoint = %endpoint,
                bucket = %bucket,
                object = %object,
                error = ?err,
                "fallback listing supplement read_xl failed"
            );
            None
        }
    }
}

fn version_requires_supplement(
    version_required_quorum: usize,
    reader_disks: usize,
    selected_object_versions: usize,
    requested_versions: usize,
) -> bool {
    reader_disks < version_required_quorum && (requested_versions == 0 || selected_object_versions < requested_versions)
}

fn cached_entry_needs_supplement(
    cached: &FileMeta,
    reader_disks: usize,
    resolver: &MetadataResolutionParams,
    enforce_write_quorum: bool,
) -> bool {
    if !enforce_write_quorum {
        return false;
    }

    let mut selected_object_versions = 0;
    for version in cached.versions.iter() {
        let required_quorum = version.write_quorum(resolver.obj_quorum).max(resolver.obj_quorum);
        if version_requires_supplement(required_quorum, reader_disks, selected_object_versions, resolver.requested_versions) {
            return true;
        }

        if !version.header.free_version() {
            selected_object_versions += 1;
        }
        if resolver.requested_versions > 0 && selected_object_versions == resolver.requested_versions {
            break;
        }
    }

    false
}

fn listing_entries_supplement_target(
    entries: &MetaCacheEntries,
    resolver: &MetadataResolutionParams,
    enforce_write_quorum: bool,
) -> Option<String> {
    if !enforce_write_quorum {
        return None;
    }

    for entry in entries.0.iter().flatten() {
        if entry.is_dir() {
            continue;
        }

        let reader_disks = entries
            .0
            .iter()
            .filter(|candidate| {
                candidate
                    .as_ref()
                    .is_some_and(|candidate| candidate.name == entry.name && candidate.is_object())
            })
            .count();
        let mut entry = entry.clone();
        if let Ok(cached) = entry.xl_meta()
            && cached_entry_needs_supplement(&cached, reader_disks, resolver, enforce_write_quorum)
        {
            return Some(entry.name);
        }
    }

    None
}

async fn resolve_listing_entries_with_supplement(
    entries: MetaCacheEntries,
    resolver: MetadataResolutionParams,
    enforce_write_quorum: bool,
    supplement: Arc<ListingSupplement>,
) -> Option<MetaCacheEntry> {
    if !supplement.is_empty()
        && let Some(object) = listing_entries_supplement_target(&entries, &resolver, enforce_write_quorum)
    {
        let mut candidates = entries.0;
        candidates.extend(supplement.entries_for(&object).await);
        return resolve_listing_entries(MetaCacheEntries(candidates), resolver, enforce_write_quorum);
    }

    resolve_listing_entries(entries, resolver, enforce_write_quorum)
}

async fn resolve_agreed_listing_entry_with_supplement(
    entry: MetaCacheEntry,
    reader_disks: usize,
    resolver: MetadataResolutionParams,
    enforce_write_quorum: bool,
    supplement: Arc<ListingSupplement>,
) -> Option<MetaCacheEntry> {
    if supplement.is_empty() {
        return None;
    }

    let mut candidates = Vec::with_capacity(reader_disks);
    candidates.resize(reader_disks, Some(entry.clone()));
    candidates.extend(supplement.entries_for(&entry.name).await);
    resolve_listing_entries(MetaCacheEntries(candidates), resolver, enforce_write_quorum)
}

fn listing_entry_with_selected_versions(
    mut entry: MetaCacheEntry,
    meta_ver: u8,
    data: rustfs_filemeta::InlineData,
    selected_versions: Vec<FileMetaShallowVersion>,
) -> Option<MetaCacheEntry> {
    let merged_cached = FileMeta {
        meta_ver,
        data,
        versions: selected_versions,
    };
    let metadata = merged_cached.marshal_msg().ok()?;

    entry.metadata = metadata;
    entry.cached = Some(merged_cached);
    Some(entry)
}

fn resolve_agreed_listing_entry(
    mut entry: MetaCacheEntry,
    reader_disks: usize,
    resolver: MetadataResolutionParams,
    enforce_write_quorum: bool,
) -> ListingEntryResolution {
    if !enforce_write_quorum {
        return ListingEntryResolution::Resolved(entry);
    }

    if entry.is_dir() {
        return if reader_disks >= resolver.dir_quorum {
            ListingEntryResolution::Resolved(entry)
        } else {
            ListingEntryResolution::Rejected
        };
    }

    if reader_disks < resolver.obj_quorum {
        return ListingEntryResolution::Rejected;
    }

    let cached = match entry.xl_meta() {
        Ok(cached) => cached,
        Err(_) if resolver.obj_quorum <= 1 => return ListingEntryResolution::Resolved(entry),
        Err(_) => return ListingEntryResolution::Rejected,
    };
    let mut selected_versions = Vec::new();
    let mut selected_object_versions = 0;
    let mut first_selected_idx = None;
    let mut needs_supplement = false;

    for (idx, version) in cached.versions.iter().enumerate() {
        let required_quorum = version.write_quorum(resolver.obj_quorum).max(resolver.obj_quorum);
        if reader_disks < required_quorum {
            needs_supplement |=
                version_requires_supplement(required_quorum, reader_disks, selected_object_versions, resolver.requested_versions);
            continue;
        }

        if first_selected_idx.is_none() {
            first_selected_idx = Some(idx);
        }
        if !version.header.free_version() {
            selected_object_versions += 1;
        }
        selected_versions.push(version.clone());

        if resolver.requested_versions > 0 && selected_object_versions == resolver.requested_versions {
            break;
        }
    }

    if needs_supplement {
        let fallback = if selected_versions.is_empty() {
            None
        } else if selected_versions.len() == cached.versions.len() {
            Some(entry.clone())
        } else {
            listing_entry_with_selected_versions(entry.clone(), cached.meta_ver, cached.data, selected_versions)
        };
        return ListingEntryResolution::NeedsSupplement(entry, fallback);
    }

    if selected_versions.is_empty() {
        return ListingEntryResolution::Rejected;
    }

    let selected_latest = first_selected_idx == Some(0)
        && resolver.requested_versions == 1
        && selected_versions
            .first()
            .is_some_and(|version| !version.header.free_version());
    if selected_latest || selected_versions.len() == cached.versions.len() {
        return ListingEntryResolution::Resolved(entry);
    }

    listing_entry_with_selected_versions(entry, cached.meta_ver, cached.data, selected_versions)
        .map(ListingEntryResolution::Resolved)
        .unwrap_or(ListingEntryResolution::Rejected)
}

fn latest_listing_object_quorum(
    listing_quorum: usize,
    drive_count: usize,
    parity_count: usize,
    enforce_write_quorum: bool,
) -> usize {
    latest_listing_required_object_quorum(listing_quorum, drive_count, parity_count, enforce_write_quorum)
}

fn latest_listing_required_object_quorum(
    listing_quorum: usize,
    drive_count: usize,
    parity_count: usize,
    enforce_write_quorum: bool,
) -> usize {
    if !enforce_write_quorum {
        return listing_quorum;
    }

    write_quorum_for_drive_count(drive_count, parity_count).max(listing_quorum)
}

fn enforce_latest_listing_write_quorum(strict_latest: bool, ask_disks: &str) -> bool {
    strict_latest && !matches!(normalize_list_quorum(ask_disks), "disk" | "reduced")
}

#[cfg(test)]
fn latest_listing_allow_agreed_objects(enforce_write_quorum: bool, reader_disks: usize, object_quorum: usize) -> bool {
    !enforce_write_quorum || reader_disks >= object_quorum
}

fn latest_listing_raw_min_disks(listing_quorum: usize, object_quorum: usize, enforce_write_quorum: bool) -> usize {
    if enforce_write_quorum { object_quorum } else { listing_quorum }
}

fn positive_ask_disks(ask_disks: i32) -> Option<usize> {
    usize::try_from(ask_disks).ok().filter(|asked| *asked > 0)
}

fn listing_quorum_from_ask_disks(ask_disks: i32) -> usize {
    positive_ask_disks(ask_disks)
        .map(|asked| asked.div_ceil(2))
        .unwrap_or_default()
}

fn bounded_usize_to_i32(value: usize) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

fn clamp_ask_disks_to_available(ask_disks: i32, available_disks: usize) -> i32 {
    ask_disks.min(bounded_usize_to_i32(available_disks))
}

fn expand_ask_disks_for_object_quorum(ask_disks: i32, available_disks: usize, object_quorum: usize) -> i32 {
    let Some(asked_disks) = positive_ask_disks(ask_disks) else {
        return ask_disks;
    };

    bounded_usize_to_i32(asked_disks.max(object_quorum).min(available_disks))
}

fn write_quorum_for_drive_count(drive_count: usize, parity_count: usize) -> usize {
    if drive_count == 0 {
        return 0;
    }

    let data_drives = drive_count.saturating_sub(parity_count);
    if data_drives == parity_count {
        data_drives.saturating_add(1)
    } else {
        data_drives
    }
}

fn parse_version_marker(marker: String) -> Result<VersionMarker> {
    Ok(VersionMarker::parse(marker)?)
}

fn version_marker_for_entries(
    entries: Option<&MetaCacheEntriesSorted>,
    key_marker: Option<&str>,
    version_marker: Option<VersionMarker>,
) -> Option<VersionMarker> {
    let marker = version_marker?;
    let Some(key_marker) = key_marker else {
        return Some(marker);
    };

    entries
        .and_then(|entries| entries.entries().first().map(|entry| entry.name.as_str() == key_marker))
        .unwrap_or_default()
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

    pub fn parse_marker(&mut self) {
        let Some(marker) = self.marker.clone() else {
            return;
        };
        let Some(start_idx) = marker.rfind("[rustfs_cache:") else {
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

    pub fn encode_marker(&mut self, marker: &str) -> String {
        ListContinuationV2 {
            version: MARKER_TAG_VERSION,
            id: self.id.clone(),
            pool_idx: self.pool_idx,
            set_idx: self.set_idx,
            source: Some(ListSourceMode::Walker),
            generation: Some(LIST_CURSOR_GENERATION_LIVE.to_owned()),
        }
        .encode_marker(marker)
    }
}

impl ECStore {
    async fn collect_persistent_key_only_index_keys(self: Arc<Self>, opts: &ListPathOptions) -> Result<Vec<String>> {
        let mut marker = None;
        let mut keys = Vec::new();

        loop {
            let previous_marker = marker.clone();
            let page_opts = ListPathOptions {
                bucket: opts.bucket.clone(),
                prefix: String::new(),
                marker: marker.clone(),
                separator: None,
                limit: max_keys_plus_one(MAX_OBJECT_LIST, true),
                ask_disks: list_objects_quorum_from_env(),
                incl_deleted: false,
                cursor_source: Some(ListSourceMode::Walker),
                cursor_generation: Some(LIST_CURSOR_GENERATION_LIVE.to_owned()),
                ..Default::default()
            };
            let mut list_result = self
                .clone()
                .list_path(&page_opts)
                .await
                .unwrap_or_else(|err| MetaCacheEntriesSortedResult {
                    err: Some(err.into()),
                    ..Default::default()
                });
            let reached_end = match list_result.err.take() {
                Some(err) if err == rustfs_filemeta::Error::Unexpected => true,
                Some(err) => return Err(Error::from(err)),
                None => false,
            };

            if let Some(result) = list_result.entries.as_mut() {
                result.forward_past(marker.clone());
            }

            let page_keys = list_result
                .entries
                .as_ref()
                .map(|entries| {
                    entries
                        .entries()
                        .into_iter()
                        .map(|entry| entry.name.clone())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            if page_keys.is_empty() {
                break;
            }

            marker = page_keys.last().cloned();
            if marker == previous_marker {
                return Err(Error::other("persistent key-only index rebuild marker did not advance"));
            }
            keys.extend(page_keys);

            if reached_end {
                break;
            }
        }

        Ok(keys)
    }

    async fn rebuild_persistent_key_only_index(
        self: Arc<Self>,
        opts: &ListPathOptions,
        path: &Path,
        configured_generation: Option<&str>,
    ) -> Result<Arc<PersistentKeyOnlyIndex>> {
        let generation = generated_persistent_key_only_generation(configured_generation);
        const MAX_REBUILD_ATTEMPTS: usize = 3;

        for attempt in 1..=MAX_REBUILD_ATTEMPTS {
            let start_sequence = current_list_objects_mutation_snapshot(Some(self.as_ref()), &opts.bucket).await;
            if start_sequence.degraded {
                return Err(Error::other("list objects namespace mutation journal is degraded"));
            }
            let keys = self.clone().collect_persistent_key_only_index_keys(opts).await?;
            let end_sequence = current_list_objects_mutation_snapshot(Some(self.as_ref()), &opts.bucket).await;
            if end_sequence.degraded {
                return Err(Error::other("list objects namespace mutation journal is degraded"));
            }
            if start_sequence.high_water_mark == end_sequence.high_water_mark {
                write_persistent_key_only_index(
                    Some(self.as_ref()),
                    path,
                    &opts.bucket,
                    &generation,
                    end_sequence.high_water_mark,
                    &keys,
                )
                .await?;
                return load_persistent_key_only_index(Some(self.as_ref()), path).await;
            }

            debug!(
                bucket = %opts.bucket,
                attempt,
                start_sequence = start_sequence.high_water_mark,
                end_sequence = end_sequence.high_water_mark,
                "persistent key-only index rebuild raced with object mutations"
            );
        }

        Err(Error::other(
            "persistent key-only index rebuild could not reach a stable mutation checkpoint",
        ))
    }

    async fn prepare_persistent_key_only_index(
        self: Arc<Self>,
        opts: &ListPathOptions,
        provider_state: &ListObjectsIndexProviderState,
    ) -> Result<Arc<PersistentKeyOnlyIndex>> {
        let Some(path) = provider_state.persistent_path.as_ref() else {
            return Err(Error::other("persistent key-only provider path is not configured"));
        };

        match load_persistent_key_only_index(Some(self.as_ref()), path).await {
            Ok(index) if persistent_key_only_index_matches_provider(&index, &opts.bucket, provider_state) => {
                let current_sequence = current_list_objects_mutation_snapshot(Some(self.as_ref()), &opts.bucket).await;
                if !current_sequence.degraded && current_sequence.high_water_mark <= index.checkpoint_high_water_mark {
                    Ok(index)
                } else {
                    self.rebuild_persistent_key_only_index(opts, path, provider_state.persistent_generation.as_deref())
                        .await
                }
            }
            Ok(_) => {
                self.rebuild_persistent_key_only_index(opts, path, provider_state.persistent_generation.as_deref())
                    .await
            }
            Err(Error::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {
                self.rebuild_persistent_key_only_index(opts, path, provider_state.persistent_generation.as_deref())
                    .await
            }
            Err(err) => Err(err),
        }
    }

    async fn list_objects_from_opt_in_key_only_provider(
        self: Arc<Self>,
        opts: &ListPathOptions,
        mode: ListSourceMode,
        max_keys: i32,
        incl_deleted: bool,
    ) -> Result<Option<ListObjectsInfo>> {
        let list_metrics_enabled = rustfs_io_metrics::get_stage_metrics_enabled();
        let provider_state = list_objects_index_provider_state_from_env();
        let provider = provider_state.as_ref().map(|state| state.kind);
        let provider_label = if list_metrics_enabled {
            list_objects_index_provider_metric_label(provider)
        } else {
            "none"
        };
        if list_metrics_enabled {
            rustfs_io_metrics::record_list_objects_index_attempt(
                mode.cursor_value(),
                provider_label,
                !opts.prefix.is_empty(),
                opts.separator.as_ref().is_some_and(|separator| !separator.is_empty()),
                opts.marker.is_some(),
            );
        }

        if incl_deleted || !mode.can_satisfy_strong_listing() || !mode.requires_live_metadata_verification() {
            if list_metrics_enabled {
                record_list_objects_index_fallback(mode, ListIndexFallbackReason::UnsupportedRequest);
            }
            return Ok(None);
        }

        if is_reserved_or_invalid_bucket(&opts.bucket, false) {
            if list_metrics_enabled {
                record_list_objects_index_fallback(mode, ListIndexFallbackReason::UnsupportedRequest);
            }
            return Ok(None);
        }

        let Some(provider_state) = provider_state else {
            if list_metrics_enabled {
                record_list_objects_index_fallback(mode, ListIndexFallbackReason::Disabled);
            }
            return Ok(None);
        };

        let mut provider_opts = opts.clone();
        provider_opts.parse_marker();

        let (candidate_keys, health) = match provider_state.kind {
            ListObjectsIndexProviderKind::WalkerKeyOnly => {
                let health = provider_state.health_snapshot();
                match select_list_index_provider_source_mode(&provider_opts, mode, &health) {
                    ListIndexSourceDecision::FallbackToWalker(reason) => {
                        if list_metrics_enabled {
                            record_list_objects_index_fallback(mode, reason);
                        }
                        debug!(
                            bucket = %opts.bucket,
                            prefix = %opts.prefix,
                            source = mode.cursor_value(),
                            reason = reason.metric_label(),
                            "list_objects opt-in index path fell back to walker"
                        );
                        return Ok(None);
                    }
                    ListIndexSourceDecision::UseIndex(_) => {}
                }
                provider_opts.cursor_source = Some(mode);
                provider_opts.cursor_generation = health.active_generation.clone();

                let mut list_result =
                    self.clone()
                        .list_path(&provider_opts)
                        .await
                        .unwrap_or_else(|err| MetaCacheEntriesSortedResult {
                            err: Some(err.into()),
                            ..Default::default()
                        });

                if let Some(err) = list_result.err.take()
                    && err != rustfs_filemeta::Error::Unexpected
                {
                    if list_metrics_enabled {
                        record_list_objects_index_fallback(mode, ListIndexFallbackReason::Unhealthy);
                    }
                    return Ok(None);
                }

                if let Some(result) = list_result.entries.as_mut() {
                    result.forward_past(provider_opts.marker.clone());
                }

                let candidate_keys = Arc::new(
                    list_result
                        .entries
                        .as_ref()
                        .map(|entries| {
                            entries
                                .entries()
                                .into_iter()
                                .map(|entry| entry.name.clone())
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default(),
                );
                (candidate_keys, health)
            }
            ListObjectsIndexProviderKind::PersistentKeyOnly => {
                let Some(path) = provider_state.persistent_path.as_ref() else {
                    if list_metrics_enabled {
                        record_list_objects_index_fallback(mode, ListIndexFallbackReason::Degraded);
                    }
                    return Ok(None);
                };
                match self
                    .clone()
                    .prepare_persistent_key_only_index(&provider_opts, &provider_state)
                    .await
                {
                    Ok(index) => {
                        let current_sequence =
                            current_list_objects_mutation_snapshot(Some(self.as_ref()), &provider_opts.bucket).await;
                        let health = persistent_key_only_index_health(&index, current_sequence);
                        match select_list_index_provider_source_mode(&provider_opts, mode, &health) {
                            ListIndexSourceDecision::FallbackToWalker(reason) => {
                                if list_metrics_enabled {
                                    record_list_objects_index_fallback(mode, reason);
                                }
                                debug!(
                                    bucket = %opts.bucket,
                                    prefix = %opts.prefix,
                                    source = mode.cursor_value(),
                                    provider = provider_state.kind.metric_label(),
                                    reason = reason.metric_label(),
                                    "list_objects persistent key-only provider fell back to walker"
                                );
                                return Ok(None);
                            }
                            ListIndexSourceDecision::UseIndex(_) => {}
                        }
                        provider_opts.cursor_source = Some(mode);
                        provider_opts.cursor_generation = health.active_generation.clone();
                        (index.keys.clone(), health)
                    }
                    Err(err) => {
                        if list_metrics_enabled {
                            record_list_objects_index_fallback(mode, ListIndexFallbackReason::Unhealthy);
                        }
                        debug!(
                            bucket = %opts.bucket,
                            prefix = %opts.prefix,
                            source = mode.cursor_value(),
                            provider = provider_state.kind.metric_label(),
                            path = %path.display(),
                            error = %err,
                            "list_objects persistent key-only provider failed to load candidates"
                        );
                        return Ok(None);
                    }
                }
            }
        };

        let bucket = provider_opts.bucket.clone();
        let verified = list_objects_from_verified_index_candidates_with_optional_stats(
            &provider_opts.prefix,
            provider_opts.marker.as_deref(),
            &provider_opts.separator,
            max_keys,
            candidate_keys.as_slice(),
            list_metrics_enabled,
            |key| {
                let store = self.clone();
                let bucket = bucket.clone();
                async move {
                    match store
                        .get_object_info(
                            &bucket,
                            &key,
                            &ObjectOptions {
                                no_lock: true,
                                ..Default::default()
                            },
                        )
                        .await
                    {
                        Ok(object) => Ok(Some(object)),
                        Err(err) if err.is_not_found() => Ok(None),
                        Err(err) => {
                            if list_metrics_enabled {
                                rustfs_io_metrics::record_list_objects_index_live_verify_failure(
                                    mode.cursor_value(),
                                    "read_error",
                                );
                            }
                            Err(err)
                        }
                    }
                }
            },
        )
        .await?;
        let stats = verified.stats;
        let mut result = verified.info;

        if list_metrics_enabled {
            rustfs_io_metrics::record_list_objects_index_served(ListObjectsIndexPageObservation {
                source: mode.cursor_value(),
                provider: provider_label,
                candidate_keys: stats.candidate_keys,
                live_verify_attempts: stats.live_verify_attempts,
                live_verify_hits: stats.live_verify_hits,
                live_verify_misses: stats.live_verify_misses,
                returned_objects: result.objects.len(),
                returned_prefixes: result.prefixes.len(),
                is_truncated: result.is_truncated,
            });
        }

        if let Some(next_marker) = result.next_marker.take() {
            result.next_marker = Some(
                ListContinuationV2 {
                    version: MARKER_TAG_VERSION,
                    id: None,
                    pool_idx: None,
                    set_idx: None,
                    source: Some(mode),
                    generation: health.active_generation,
                }
                .encode_marker(&next_marker),
            );
        }

        Ok(Some(result))
    }

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
                start_after
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
        let max_keys = normalize_max_keys(max_keys);
        let effective_max_keys = if max_keys <= 0 { 0 } else { max_keys_plus_one(max_keys, true) };
        let opts = ListPathOptions {
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
            separator: delimiter.clone(),
            // Always request max_keys + 1 to detect if there are more results
            limit: effective_max_keys,
            marker,
            incl_deleted,
            ask_disks: list_objects_quorum_from_env(),
            ..Default::default()
        };
        if let Some(mode) = list_objects_index_mode_from_env() {
            if let Some(result) = self
                .clone()
                .list_objects_from_opt_in_key_only_provider(&opts, mode, max_keys, incl_deleted)
                .await?
            {
                return Ok(result);
            }
        }

        // Optimization: use get for single object lookup with exact prefix
        if !opts.prefix.is_empty() && max_keys == 1 && opts.marker.is_none() {
            match self
                .get_object_info(
                    &opts.bucket,
                    &opts.prefix,
                    &ObjectOptions {
                        no_lock: true,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(res) => {
                    return Ok(ListObjectsInfo {
                        objects: vec![res],
                        ..Default::default()
                    });
                }
                Err(err) => {
                    if is_err_bucket_not_found(&err) {
                        return Err(err);
                    }
                }
            };
        };

        let mut list_result = self
            .list_path(&opts)
            .await
            .unwrap_or_else(|err| MetaCacheEntriesSortedResult {
                err: Some(err.into()),
                ..Default::default()
            });
        let next_cache_id = list_result.entries.as_ref().and_then(|entries| entries.list_id.clone());

        // err=None means gather_results filled its limit → disk has more data
        let disk_has_more = list_result.err.is_none();

        if let Some(err) = list_result.err.take()
            && err != rustfs_filemeta::Error::Unexpected
        {
            return Err(to_object_err(err.into(), vec![bucket, prefix]));
        }

        if let Some(result) = list_result.entries.as_mut() {
            result.forward_past(opts.marker);
        }

        // contextCanceled

        let get_objects = ObjectInfo::from_meta_cache_entries_sorted_infos(
            &list_result.entries.unwrap_or_default(),
            bucket,
            prefix,
            delimiter.clone(),
        )
        .await;

        let (objects, prefixes, is_truncated, next_marker, next_version_idmarker) =
            list_objects_paginate(get_objects, &delimiter, max_keys, disk_has_more, next_cache_id.as_deref(), false);
        let _ = next_version_idmarker;

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

        let effective_max_keys = if max_keys <= 0 { 0 } else { max_keys_plus_one(max_keys, true) };
        // Always request max_keys + 1 to detect if there are more results
        let opts = ListPathOptions {
            bucket: bucket.to_owned(),
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

        let mut list_result = self
            .list_path(&opts)
            .await
            .unwrap_or_else(|err| MetaCacheEntriesSortedResult {
                err: Some(err.into()),
                ..Default::default()
            });
        let next_cache_id = list_result.entries.as_ref().and_then(|entries| entries.list_id.clone());

        // err=None means gather_results filled its limit → disk has more data
        let disk_has_more = list_result.err.is_none();

        if let Some(err) = list_result.err.take()
            && err != rustfs_filemeta::Error::Unexpected
        {
            return Err(to_object_err(err.into(), vec![bucket, prefix]));
        }

        if let Some(result) = list_result.entries.as_mut()
            && !has_version_marker
        {
            result.forward_past(opts.marker.clone());
        }

        let version_marker = version_marker_for_entries(list_result.entries.as_ref(), opts.marker.as_deref(), version_marker);

        let get_objects = ObjectInfo::from_meta_cache_entries_sorted_versions(
            &list_result.entries.unwrap_or_default(),
            bucket,
            prefix,
            delimiter.clone(),
            version_marker,
        )
        .await;

        let (objects, prefixes, is_truncated, next_marker, next_version_idmarker) =
            list_objects_paginate(get_objects, &delimiter, max_keys, disk_has_more, next_cache_id.as_deref(), true);

        Ok(ListObjectVersionsInfo {
            is_truncated,
            next_marker,
            next_version_idmarker,
            objects,
            prefixes,
        })
    }

    pub async fn list_path(self: Arc<Self>, o: &ListPathOptions) -> Result<MetaCacheEntriesSortedResult> {
        // tracing::warn!("list_path opt {:?}", &o);

        let list_path_started = std::time::Instant::now();

        check_list_objs_args(&o.bucket, &o.prefix, &o.marker)?;
        // if opts.prefix.ends_with(SLASH_SEPARATOR) {
        //     return Err(Error::msg("eof"));
        // }

        let mut o = o.clone();
        o.marker = o.marker.filter(|v| v >= &o.prefix);

        if let Some(marker) = &o.marker
            && !o.prefix.is_empty()
            && !marker.starts_with(&o.prefix)
        {
            return Err(Error::Unexpected);
        }

        if o.limit == 0 {
            return Err(Error::Unexpected);
        }

        if o.prefix.starts_with(SLASH_SEPARATOR) {
            return Err(Error::Unexpected);
        }

        let slash_separator = Some(SLASH_SEPARATOR.to_owned());

        o.include_directories = o.separator == slash_separator;

        if (o.separator == slash_separator || o.separator.is_none()) && !o.recursive {
            o.recursive = o.separator != slash_separator;
            o.separator = slash_separator;
        } else {
            o.recursive = true
        }

        o.parse_marker();

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

        let (err_tx, mut err_rx) = broadcast::channel::<Arc<Error>>(1);

        let (sender, recv) = mpsc::channel(o.limit as usize);

        let store = self.clone();
        let opts = o.clone();
        let cancel_rx1 = cancel.clone();
        let cancel_rx1_for_err = cancel_rx1.clone();
        let err_tx1 = err_tx.clone();
        let job1_context = log_context.clone();
        let job1 = tokio::spawn(
            async move {
                let mut opts = opts;
                opts.stop_disk_at_limit = true;
                if let Err(err) = store.list_merged(cancel_rx1, opts, sender).await
                    && !cancel_rx1_for_err.is_cancelled()
                {
                    log_list_path_worker_error("store", "list_merged", &job1_context, &err);
                    let _ = err_tx1.send(Arc::new(err));
                }
            }
            .instrument(tracing::Span::current()),
        );

        let cancel_rx2 = cancel.clone();

        let (result_tx, mut result_rx) = mpsc::channel(1);
        let err_tx2 = err_tx.clone();
        let opts = o.clone();
        let job2_context = log_context.clone();
        let job2 = tokio::spawn(
            async move {
                match gather_results(cancel_rx2, opts, recv, result_tx).await {
                    Ok(GatherResultsState::LimitReached) => cancel.cancel(),
                    Ok(GatherResultsState::InputClosed) => {}
                    Err(err) => {
                        log_list_path_worker_error("store", "gather_results", &job2_context, &err);
                        let _ = err_tx2.send(Arc::new(err));
                        cancel.cancel();
                    }
                }
            }
            .instrument(tracing::Span::current()),
        );

        let mut result = {
            // receiver result
            tokio::select! {
               res = err_rx.recv() =>{

                match res{
                    Ok(err) => {
                        log_list_path_worker_error("store", "worker_error", &log_context, err.as_ref());
                        MetaCacheEntriesSortedResult{ entries: None, err: Some(err.as_ref().clone().into()) }
                    },
                    Err(err) => {
                        log_list_path_worker_error("store", "error_channel_closed", &log_context, &err);

                        MetaCacheEntriesSortedResult{ entries: None, err: Some(rustfs_filemeta::Error::other(err)) }
                    },
                }
               },
               Some(result) = result_rx.recv()=>{
                result
               }
            }
        };

        // wait spawns exit
        join_all(vec![job1, job2]).await;

        if let Ok(err) = err_rx.try_recv() {
            log_list_path_worker_error("store", "trailing_worker_error", &log_context, err.as_ref());
            result.err = Some(err.as_ref().clone().into());
        }

        if result.err.is_some() {
            log_list_path_finished("store", &log_context, list_path_started.elapsed().as_secs_f64() * 1000.0, 0, true);
            return Ok(result);
        }

        if let Some(entries) = result.entries.as_mut() {
            entries.reuse = true;
            let truncated = !entries.entries().is_empty() || result.err.is_none();
            entries.o.0.truncate(o.limit as usize);
            if !o.transient && truncated {
                entries.list_id = if let Some(id) = o.id {
                    Some(id)
                } else {
                    Some(Uuid::new_v4().to_string())
                }
            }

            if !truncated {
                result.err = Some(Error::Unexpected.into());
            }
        }

        rustfs_io_metrics::record_stage_duration(
            "store_list_objects_list_path",
            list_path_started.elapsed().as_secs_f64() * 1000.0,
        );

        log_list_path_finished(
            "store",
            &log_context,
            list_path_started.elapsed().as_secs_f64() * 1000.0,
            result
                .entries
                .as_ref()
                .map(|entries| entries.entries().len())
                .unwrap_or_default(),
            result.err.is_some(),
        );

        Ok(result)
    }

    // Read all
    async fn list_merged(
        &self,
        rx: CancellationToken,
        opts: ListPathOptions,
        sender: Sender<MetaCacheEntry>,
    ) -> Result<Vec<ObjectInfo>> {
        // warn!("list_merged ops {:?}", &opts);
        let merge_started = std::time::Instant::now();

        debug!(
            list_path_limit = opts.limit,
            recursive = opts.recursive,
            pool_count = self.pools.len(),
            requested_marker = %opts.marker.as_deref().unwrap_or(""),
            "store list_merged started"
        );

        let mut futures = Vec::new();

        let mut inputs = Vec::new();

        for sets in self.pools.iter() {
            for set in sets.disk_set.iter() {
                let (send, recv) = mpsc::channel(100);

                inputs.push(recv);
                let opts = opts.clone();
                let rx_clone = rx.clone();
                futures.push(set.list_path(rx_clone, opts, send));
            }
        }

        tokio::spawn(
            async move {
                if let Err(err) = merge_entry_channels(rx, inputs, sender.clone(), 1).await {
                    error!("merge_entry_channels err {:?}", err)
                }
            }
            .instrument(tracing::Span::current()),
        );

        // let merge_res = merge_entry_channels(rx, inputs, sender.clone(), 1).await;

        // TODO: cancelList

        // let merge_res = merge_entry_channels(rx, inputs, sender.clone(), 1).await;

        let results = join_all(futures).await;

        let mut all_at_eof = true;

        let mut errs = Vec::new();
        for result in results {
            if let Err(err) = result {
                all_at_eof = false;
                errs.push(Some(err));
            } else {
                errs.push(None);
            }
        }

        // All sets returned not-found — the listing is simply empty.
        // We intentionally do NOT distinguish VolumeNotFound here: during
        // listing, a missing volume on a set is equivalent to "no entries"
        // and must not surface as an error to the caller. The original
        // VolumeNotFound check caused spurious errors when a pool had not
        // yet created the bucket prefix on every set.
        if is_all_not_found(&errs) {
            return Ok(Vec::new());
        }

        // merge_res?;

        // TODO check cancel

        for err in errs.iter() {
            if let Some(err) = err {
                if err == &Error::Unexpected {
                    continue;
                }

                return Err(err.clone());
            } else {
                all_at_eof = false;
                continue;
            }
        }

        // check all_at_eof

        rustfs_io_metrics::record_stage_duration(
            "store_list_objects_list_merged",
            merge_started.elapsed().as_secs_f64() * 1000.0,
        );

        debug!(
            set_pair_count = self.pools.len(),
            all_at_eof = all_at_eof,
            error_count = errs.iter().filter(|err| err.is_some()).count(),
            "store list_merged finished"
        );

        _ = all_at_eof;

        Ok(Vec::new())
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
                    let enforce_write_quorum = enforce_latest_listing_write_quorum(opts.latest_only, &opts.ask_disks);
                    let write_quorum_parity = set.default_parity_count;
                    let required_obj_quorum = latest_listing_required_object_quorum(
                        listing_quorum,
                        set.set_drive_count,
                        write_quorum_parity,
                        enforce_write_quorum,
                    );
                    ask_disks = expand_ask_disks_for_object_quorum(ask_disks, disks.len(), required_obj_quorum);
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
                    let fallback_disks = Arc::new(fallback_disks);
                    let claim_tracker = FallbackClaimTracker::default();

                    let obj_quorum = latest_listing_object_quorum(
                        listing_quorum,
                        set.set_drive_count,
                        write_quorum_parity,
                        enforce_write_quorum,
                    );
                    let raw_min_disks = latest_listing_raw_min_disks(listing_quorum, obj_quorum, enforce_write_quorum);

                    let resolver =
                        list_metadata_resolution_params(bucket.to_owned(), listing_quorum, obj_quorum, !opts.latest_only);
                    let agreed_resolver = resolver.clone();
                    let partial_resolver = resolver.clone();
                    let reader_disks = disks.len();

                    let path = base_dir_from_prefix(prefix);
                    ensure_non_empty_listing_disks(bucket, &path, &disks)?;

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

                    // let (sender, rx1) = mpsc::channel(100);

                    let tx1 = sender.clone();
                    let tx2 = sender.clone();
                    let supplement = ListingSupplement::new(
                        ListingSupplementOptions {
                            bucket: bucket.to_owned(),
                            path: path.clone(),
                            recursive: true,
                            filter_prefix: Some(filter_prefix.clone()),
                            forward_to: opts.marker.clone(),
                            per_disk_limit: bounded_usize_to_i32(opts.limit),
                            skip_total_timeout: false,
                        },
                        fallback_disks.clone(),
                        claim_tracker.clone(),
                    );
                    let agreed_supplement = supplement.clone();
                    let partial_supplement = supplement;

                    list_path_raw_with_claim_tracker(
                        rx_clone,
                        ListPathRawOptions {
                            disks: disks.iter().cloned().map(Some).collect(),
                            fallback_disks: fallback_disks.iter().cloned().map(Some).collect(),
                            bucket: bucket.to_owned(),
                            path,
                            recursive: true,
                            filter_prefix: Some(filter_prefix),
                            forward_to: opts.marker.clone(),
                            min_disks: raw_min_disks,
                            per_disk_limit: bounded_usize_to_i32(opts.limit),
                            agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                                Box::pin({
                                    let value = tx1.clone();
                                    let resolver = agreed_resolver.clone();
                                    let supplement = agreed_supplement.clone();
                                    async move {
                                        let entry = match resolve_agreed_listing_entry(
                                            entry,
                                            reader_disks,
                                            resolver.clone(),
                                            enforce_write_quorum,
                                        ) {
                                            ListingEntryResolution::Resolved(entry) => entry,
                                            ListingEntryResolution::NeedsSupplement(entry, fallback) => {
                                                let Some(entry) = resolve_agreed_listing_entry_with_supplement(
                                                    entry,
                                                    reader_disks,
                                                    resolver,
                                                    enforce_write_quorum,
                                                    supplement,
                                                )
                                                .await
                                                .or(fallback) else {
                                                    return;
                                                };
                                                entry
                                            }
                                            ListingEntryResolution::Rejected => return,
                                        };
                                        if entry.is_dir() {
                                            return;
                                        }
                                        if let Err(err) = value.send(entry).await {
                                            error!("list_path send fail {:?}", err);
                                        }
                                    }
                                })
                            })),
                            partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<DiskError>]| {
                                Box::pin({
                                    let value = tx2.clone();
                                    let resolver = partial_resolver.clone();
                                    let supplement = partial_supplement.clone();
                                    async move {
                                        if let Some(entry) = resolve_listing_entries_with_supplement(
                                            entries,
                                            resolver,
                                            enforce_write_quorum,
                                            supplement,
                                        )
                                        .await
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
                        claim_tracker,
                    )
                    .await
                });
            }
        }

        let (merge_tx, mut merge_rx) = mpsc::channel::<MetaCacheEntry>(100);

        let bucket = bucket.to_owned();
        let bucket_clone = bucket.clone();

        let vcf = match get_versioning_config(&bucket).await {
            Ok((res, _)) => Some(res),
            Err(_) => None,
        };

        tokio::spawn(
            async move {
                let mut sent_err = false;
                while let Some(entry) = merge_rx.recv().await {
                    if opts.latest_only {
                        let fi = match entry.to_fileinfo(&bucket_clone) {
                            Ok(res) => res,
                            Err(err) => {
                                if !sent_err {
                                    let item = ObjectInfoOrErr {
                                        item: None,
                                        err: Some(err.into()),
                                    };

                                    if let Err(err) = result.send(item).await {
                                        error!("walk result send err {:?}", err);
                                    }

                                    sent_err = true;

                                    return;
                                }

                                continue;
                            }
                        };

                        if let Some(filter) = opts.filter {
                            if filter(&fi) {
                                let item = ObjectInfoOrErr {
                                    item: Some(ObjectInfo::from_file_info(&fi, &bucket_clone, &fi.name, {
                                        if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                    })),
                                    err: None,
                                };

                                if let Err(err) = result.send(item).await {
                                    error!("walk result send err {:?}", err);
                                }
                            }
                        } else {
                            let item = ObjectInfoOrErr {
                                item: Some(ObjectInfo::from_file_info(&fi, &bucket_clone, &fi.name, {
                                    if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                })),
                                err: None,
                            };

                            if let Err(err) = result.send(item).await {
                                error!("walk result send err {:?}", err);
                            }
                        }
                        continue;
                    }

                    let fvs = match if opts.include_free_versions {
                        entry.file_info_versions_with_free_versions(&bucket_clone)
                    } else {
                        entry.file_info_versions(&bucket_clone)
                    } {
                        Ok(res) => res,
                        Err(err) => {
                            let item = ObjectInfoOrErr {
                                item: None,
                                err: Some(err.into()),
                            };

                            if let Err(err) = result.send(item).await {
                                error!("walk result send err {:?}", err);
                            }
                            return;
                        }
                    };

                    if opts.versions_sort == WalkVersionsSortOrder::Ascending {
                        //TODO: SORT
                    }

                    for fi in fvs.versions.iter() {
                        if let Some(filter) = opts.filter {
                            if filter(fi) {
                                let item = ObjectInfoOrErr {
                                    item: Some(ObjectInfo::from_file_info(fi, &bucket_clone, &fi.name, {
                                        if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                    })),
                                    err: None,
                                };

                                if let Err(err) = result.send(item).await {
                                    error!("walk result send err {:?}", err);
                                }
                            }
                        } else {
                            let item = ObjectInfoOrErr {
                                item: Some(ObjectInfo::from_file_info(fi, &bucket_clone, &fi.name, {
                                    if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                })),
                                err: None,
                            };

                            if let Err(err) = result.send(item).await {
                                error!("walk result send err {:?}", err);
                            }
                        }
                    }

                    if opts.include_free_versions {
                        for fi in fvs.free_versions.iter() {
                            if let Some(filter) = opts.filter {
                                if filter(fi) {
                                    let item = ObjectInfoOrErr {
                                        item: Some(ObjectInfo::from_file_info(fi, &bucket_clone, &fi.name, {
                                            if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                        })),
                                        err: None,
                                    };

                                    if let Err(err) = result.send(item).await {
                                        error!("walk result send err {:?}", err);
                                    }
                                }
                            } else {
                                let item = ObjectInfoOrErr {
                                    item: Some(ObjectInfo::from_file_info(fi, &bucket_clone, &fi.name, {
                                        if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                    })),
                                    err: None,
                                };

                                if let Err(err) = result.send(item).await {
                                    error!("walk result send err {:?}", err);
                                }
                            }
                        }
                    }
                }
            }
            .instrument(tracing::Span::current()),
        );

        tokio::spawn(
            async move {
                if let Err(err) = merge_entry_channels(rx, inputs, merge_tx, 1).await {
                    error!("merge_entry_channels err {:?}", err)
                }
            }
            .instrument(tracing::Span::current()),
        );

        let walk_started = std::time::Instant::now();
        let walk_results = join_all(futures).await;
        let mut errs = Vec::new();
        for walk_result in walk_results {
            match walk_result {
                Ok(()) => errs.push(None),
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

async fn gather_results(
    rx: CancellationToken,
    opts: ListPathOptions,
    recv: Receiver<MetaCacheEntry>,
    results_tx: Sender<MetaCacheEntriesSortedResult>,
) -> Result<GatherResultsState> {
    let mut recv = recv;
    let mut entries = Vec::new();
    let list_metrics_enabled = rustfs_io_metrics::get_stage_metrics_enabled();
    let gather_started = list_metrics_enabled.then(std::time::Instant::now);
    let mut scanned_entries = 0usize;
    let mut candidate_entries = 0usize;

    while let Some(mut entry) = recv.recv().await {
        scanned_entries += 1;
        #[cfg(windows)]
        {
            // normalize windows path separator
            entry.name = entry.name.replace("\\", "/");
        }

        // TODO: rx.recv()

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

        if !opts.incl_deleted && is_latest_delete_marker && !entry.is_object_dir() {
            continue;
        }

        // TODO: Lifecycle

        entries.push(Some(entry));
        candidate_entries += 1;

        if opts.limit > 0 && entries.len() >= opts.limit as usize {
            rx.cancel();
            let filtered = scanned_entries.saturating_sub(candidate_entries);
            if let Some(started) = gather_started {
                let duration_ms = started.elapsed().as_secs_f64() * 1000.0;
                rustfs_io_metrics::record_list_objects_gather(ListObjectsGatherObservation {
                    source: LIST_OBJECTS_SOURCE_WALKER,
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

            results_tx
                .send(MetaCacheEntriesSortedResult {
                    entries: Some(MetaCacheEntriesSorted {
                        o: MetaCacheEntries(entries),
                        ..Default::default()
                    }),
                    err: None,
                })
                .await
                .map_err(Error::other)?;
            return Ok(GatherResultsState::LimitReached);
        }
    }

    // finish not full, return eof
    let filtered = scanned_entries.saturating_sub(candidate_entries);
    if let Some(started) = gather_started {
        let duration_ms = started.elapsed().as_secs_f64() * 1000.0;
        rustfs_io_metrics::record_list_objects_gather(ListObjectsGatherObservation {
            source: LIST_OBJECTS_SOURCE_WALKER,
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

    results_tx
        .send(MetaCacheEntriesSortedResult {
            entries: Some(MetaCacheEntriesSorted {
                o: MetaCacheEntries(entries),
                ..Default::default()
            }),
            err: Some(Error::Unexpected.into()),
        })
        .await
        .map_err(Error::other)?;

    Ok(GatherResultsState::InputClosed)
}

async fn select_from(
    rx: &CancellationToken,
    in_channels: &mut [Receiver<MetaCacheEntry>],
    idx: usize,
    top: &mut [Option<MetaCacheEntry>],
    n_done: &mut usize,
) -> Result<bool> {
    let entry = tokio::select! {
        entry = in_channels[idx].recv() => entry,
        _ = rx.cancelled() => return Ok(false),
    };

    match entry {
        Some(entry) => {
            top[idx] = Some(entry);
        }
        None => {
            top[idx] = None;
            *n_done += 1;
        }
    }
    Ok(true)
}

async fn send_or_cancel(rx: &CancellationToken, out_channel: &Sender<MetaCacheEntry>, entry: MetaCacheEntry) -> Result<bool> {
    tokio::select! {
        result = out_channel.send(entry) => {
            result.map_err(Error::other)?;
            Ok(true)
        }
        _ = rx.cancelled() => Ok(false),
    }
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

    let mut top: Vec<Option<MetaCacheEntry>> = vec![None; in_channels.len()];
    let mut n_done = 0;

    let in_channels_len = in_channels.len();

    for idx in 0..in_channels_len {
        if !select_from(&rx, &mut in_channels, idx, &mut top, &mut n_done).await? {
            return Ok(());
        }
    }

    let mut last = String::new();
    let mut to_merge: Vec<usize> = Vec::new();
    loop {
        if n_done == in_channels.len() {
            return Ok(());
        }

        let mut best = top[0].clone();
        let mut best_idx = 0;
        to_merge.clear();

        // Note: `select_from` mutates `top[idx]` during the inner loop, but this is safe
        // because each borrow from `top[other_idx]` is only used before any later
        // `select_from` call that can mutate that slot.

        for other_idx in 1..top.len() {
            if let Some(other_entry) = &top[other_idx] {
                if let Some(best_entry) = &best {
                    if path::clean(&best_entry.name) == path::clean(&other_entry.name) {
                        let dir_matches = best_entry.is_dir() && other_entry.is_dir();
                        let suffix_matches =
                            best_entry.name.ends_with(SLASH_SEPARATOR) == other_entry.name.ends_with(SLASH_SEPARATOR);

                        if dir_matches && suffix_matches {
                            to_merge.push(other_idx);
                            continue;
                        }

                        if !dir_matches {
                            // dir and object has the save name
                            if other_entry.is_dir() {
                                // TODO: read next entry to top
                                if !select_from(&rx, &mut in_channels, other_idx, &mut top, &mut n_done).await? {
                                    return Ok(());
                                }
                                continue;
                            }

                            to_merge.clear();

                            best = Some(other_entry.clone());
                            best_idx = other_idx;
                            continue;
                        }
                    }

                    if best_entry.name > other_entry.name {
                        to_merge.clear();
                        best = Some(other_entry.clone());
                        best_idx = other_idx;
                    }
                } else {
                    best = Some(other_entry.clone());
                    best_idx = other_idx;
                }
            }
        }

        if !to_merge.is_empty() {
            if let Some(entry) = &best {
                let mut versions = Vec::with_capacity(to_merge.len() + 1);

                let mut has_xl = { entry.clone().xl_meta().ok() };

                if let Some(x) = &has_xl {
                    versions.push(x.versions.clone());
                }

                for &idx in to_merge.iter() {
                    let has_entry = top[idx].clone();

                    if let Some(entry) = has_entry {
                        let xl2 = match entry.clone().xl_meta() {
                            Ok(res) => res,
                            Err(_) => {
                                if !select_from(&rx, &mut in_channels, idx, &mut top, &mut n_done).await? {
                                    return Ok(());
                                }

                                continue;
                            }
                        };

                        versions.push(xl2.versions.clone());

                        if has_xl.is_none() {
                            if !select_from(&rx, &mut in_channels, best_idx, &mut top, &mut n_done).await? {
                                return Ok(());
                            }

                            best_idx = idx;
                            best = Some(entry.clone());
                            has_xl = Some(xl2);
                        } else {
                            if !select_from(&rx, &mut in_channels, best_idx, &mut top, &mut n_done).await? {
                                return Ok(());
                            }
                        }
                    }
                }

                if let Some(xl) = has_xl.as_mut()
                    && !versions.is_empty()
                {
                    xl.versions = merge_file_meta_versions(read_quorum, true, 0, &versions);

                    if let Ok(meta) = xl.marshal_msg()
                        && let Some(b) = best.as_mut()
                    {
                        b.metadata = meta;
                        b.cached = Some(xl.clone());
                    }
                }
            }

            to_merge.clear();
        }

        if let Some(best_entry) = &best
            && best_entry.name > last
        {
            if !send_or_cancel(&rx, &out_channel, best_entry.clone()).await? {
                return Ok(());
            }
            last = best_entry.name.clone();
        }

        if !select_from(&rx, &mut in_channels, best_idx, &mut top, &mut n_done).await? {
            return Ok(());
        }
    }
}

impl Sets {
    #[allow(clippy::too_many_arguments)]
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
        let marker = if continuation_token.is_none() {
            start_after
        } else {
            continuation_token.clone()
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
        let max_keys = normalize_max_keys(max_keys);
        let effective_max_keys = if max_keys <= 0 { 0 } else { max_keys_plus_one(max_keys, true) };
        let opts = ListPathOptions {
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
            separator: delimiter.clone(),
            limit: effective_max_keys,
            marker,
            incl_deleted,
            ask_disks: list_objects_quorum_from_env(),
            ..Default::default()
        };

        if !opts.prefix.is_empty() && max_keys == 1 && opts.marker.is_none() {
            match self
                .get_object_info(
                    &opts.bucket,
                    &opts.prefix,
                    &ObjectOptions {
                        no_lock: true,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(res) => {
                    return Ok(ListObjectsInfo {
                        objects: vec![res],
                        ..Default::default()
                    });
                }
                Err(err) => {
                    if is_err_bucket_not_found(&err) {
                        return Err(err);
                    }
                }
            };
        }

        let mut list_result = self
            .list_path(&opts)
            .await
            .unwrap_or_else(|err| MetaCacheEntriesSortedResult {
                err: Some(err.into()),
                ..Default::default()
            });
        let next_cache_id = list_result.entries.as_ref().and_then(|entries| entries.list_id.clone());

        let disk_has_more = list_result.err.is_none();

        if let Some(err) = list_result.err.take()
            && err != rustfs_filemeta::Error::Unexpected
        {
            return Err(to_object_err(err.into(), vec![bucket, prefix]));
        }

        if let Some(result) = list_result.entries.as_mut() {
            result.forward_past(opts.marker);
        }

        let get_objects = ObjectInfo::from_meta_cache_entries_sorted_infos(
            &list_result.entries.unwrap_or_default(),
            bucket,
            prefix,
            delimiter.clone(),
        )
        .await;

        let (objects, prefixes, is_truncated, next_marker, next_version_idmarker) =
            list_objects_paginate(get_objects, &delimiter, max_keys, disk_has_more, next_cache_id.as_deref(), false);
        let _ = next_version_idmarker;

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

        let effective_max_keys = if max_keys <= 0 { 0 } else { max_keys_plus_one(max_keys, true) };
        let opts = ListPathOptions {
            bucket: bucket.to_owned(),
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

        let mut list_result = self
            .list_path(&opts)
            .await
            .unwrap_or_else(|err| MetaCacheEntriesSortedResult {
                err: Some(err.into()),
                ..Default::default()
            });
        let next_cache_id = list_result.entries.as_ref().and_then(|entries| entries.list_id.clone());

        let disk_has_more = list_result.err.is_none();

        if let Some(err) = list_result.err.take()
            && err != rustfs_filemeta::Error::Unexpected
        {
            return Err(to_object_err(err.into(), vec![bucket, prefix]));
        }

        if let Some(result) = list_result.entries.as_mut()
            && !has_version_marker
        {
            result.forward_past(opts.marker.clone());
        }

        let version_marker = version_marker_for_entries(list_result.entries.as_ref(), opts.marker.as_deref(), version_marker);

        let get_objects = ObjectInfo::from_meta_cache_entries_sorted_versions(
            &list_result.entries.unwrap_or_default(),
            bucket,
            prefix,
            delimiter.clone(),
            version_marker,
        )
        .await;

        let (objects, prefixes, is_truncated, next_marker, next_version_idmarker) =
            list_objects_paginate(get_objects, &delimiter, max_keys, disk_has_more, next_cache_id.as_deref(), true);

        Ok(ListObjectVersionsInfo {
            is_truncated,
            next_marker,
            next_version_idmarker,
            objects,
            prefixes,
        })
    }

    pub async fn list_path(self: Arc<Self>, o: &ListPathOptions) -> Result<MetaCacheEntriesSortedResult> {
        check_list_objs_args(&o.bucket, &o.prefix, &o.marker)?;
        let list_path_started = std::time::Instant::now();

        let mut o = o.clone();
        o.marker = o.marker.filter(|v| v >= &o.prefix);

        if let Some(marker) = &o.marker
            && !o.prefix.is_empty()
            && !marker.starts_with(&o.prefix)
        {
            return Err(Error::Unexpected);
        }

        if o.limit == 0 {
            return Err(Error::Unexpected);
        }

        if o.prefix.starts_with(SLASH_SEPARATOR) {
            return Err(Error::Unexpected);
        }

        let slash_separator = Some(SLASH_SEPARATOR.to_owned());
        o.include_directories = o.separator == slash_separator;

        if (o.separator == slash_separator || o.separator.is_none()) && !o.recursive {
            o.recursive = o.separator != slash_separator;
            o.separator = slash_separator;
        } else {
            o.recursive = true;
        }

        o.parse_marker();

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
        let (err_tx, mut err_rx) = broadcast::channel::<Arc<Error>>(1);
        let (sender, recv) = mpsc::channel(o.limit as usize);

        let sets = self.clone();
        let opts = o.clone();
        let cancel_rx1 = cancel.clone();
        let cancel_rx1_for_err = cancel_rx1.clone();
        let err_tx1 = err_tx.clone();
        let job1_context = log_context.clone();
        let job1 = tokio::spawn(
            async move {
                let mut opts = opts;
                opts.stop_disk_at_limit = true;
                if let Err(err) = sets.list_merged(cancel_rx1, opts, sender).await
                    && !cancel_rx1_for_err.is_cancelled()
                {
                    log_list_path_worker_error("sets", "list_merged", &job1_context, &err);
                    let _ = err_tx1.send(Arc::new(err));
                }
            }
            .instrument(tracing::Span::current()),
        );

        let cancel_rx2 = cancel.clone();
        let (result_tx, mut result_rx) = mpsc::channel(1);
        let err_tx2 = err_tx.clone();
        let opts = o.clone();
        let job2_context = log_context.clone();
        let job2 = tokio::spawn(
            async move {
                match gather_results(cancel_rx2, opts, recv, result_tx).await {
                    Ok(GatherResultsState::LimitReached) => cancel.cancel(),
                    Ok(GatherResultsState::InputClosed) => {}
                    Err(err) => {
                        log_list_path_worker_error("sets", "gather_results", &job2_context, &err);
                        let _ = err_tx2.send(Arc::new(err));
                        cancel.cancel();
                    }
                }
            }
            .instrument(tracing::Span::current()),
        );

        let mut result = tokio::select! {
            res = err_rx.recv() => {
                match res {
                    Ok(err) => {
                        log_list_path_worker_error("sets", "worker_error", &log_context, err.as_ref());
                        MetaCacheEntriesSortedResult { entries: None, err: Some(err.as_ref().clone().into()) }
                    },
                    Err(err) => {
                        log_list_path_worker_error("sets", "error_channel_closed", &log_context, &err);
                        MetaCacheEntriesSortedResult { entries: None, err: Some(rustfs_filemeta::Error::other(err)) }
                    },
                }
            }
            Some(result) = result_rx.recv() => result,
        };

        join_all(vec![job1, job2]).await;

        if let Ok(err) = err_rx.try_recv() {
            log_list_path_worker_error("sets", "trailing_worker_error", &log_context, err.as_ref());
            result.err = Some(err.as_ref().clone().into());
        }

        if result.err.is_some() {
            log_list_path_finished("sets", &log_context, list_path_started.elapsed().as_secs_f64() * 1000.0, 0, true);
            return Ok(result);
        }

        if let Some(entries) = result.entries.as_mut() {
            entries.reuse = true;
            let truncated = !entries.entries().is_empty() || result.err.is_none();
            entries.o.0.truncate(o.limit as usize);
            if !o.transient && truncated {
                entries.list_id = if let Some(id) = o.id {
                    Some(id)
                } else {
                    Some(Uuid::new_v4().to_string())
                };
            }

            if !truncated {
                result.err = Some(Error::Unexpected.into());
            }
        }

        log_list_path_finished(
            "sets",
            &log_context,
            list_path_started.elapsed().as_secs_f64() * 1000.0,
            result
                .entries
                .as_ref()
                .map(|entries| entries.entries().len())
                .unwrap_or_default(),
            result.err.is_some(),
        );

        Ok(result)
    }

    async fn list_merged(
        &self,
        rx: CancellationToken,
        opts: ListPathOptions,
        sender: Sender<MetaCacheEntry>,
    ) -> Result<Vec<ObjectInfo>> {
        let merge_started = std::time::Instant::now();

        debug!(
            list_path_limit = opts.limit,
            recursive = opts.recursive,
            set_count = self.disk_set.len(),
            requested_marker = %opts.marker.as_deref().unwrap_or(""),
            "sets list_merged started"
        );

        let mut futures = Vec::new();
        let mut inputs = Vec::new();

        for set in &self.disk_set {
            let (send, recv) = mpsc::channel(100);
            inputs.push(recv);
            let opts = opts.clone();
            let rx_clone = rx.clone();
            let set = set.clone();
            futures.push(async move { set.list_path(rx_clone, opts, send).await });
        }

        tokio::spawn(
            async move {
                if let Err(err) = merge_entry_channels(rx, inputs, sender.clone(), 1).await {
                    error!("merge_entry_channels err {:?}", err);
                }
            }
            .instrument(tracing::Span::current()),
        );

        let results = join_all(futures).await;
        let mut all_at_eof = true;
        let mut errs = Vec::new();
        for result in results {
            if let Err(err) = result {
                all_at_eof = false;
                errs.push(Some(err));
            } else {
                errs.push(None);
            }
        }

        if is_all_not_found(&errs) {
            return Ok(Vec::new());
        }

        for err in &errs {
            if let Some(err) = err {
                if err == &Error::Unexpected {
                    continue;
                }
                return Err(err.clone());
            } else {
                all_at_eof = false;
            }
        }

        rustfs_io_metrics::record_stage_duration("sets_list_objects_list_merged", merge_started.elapsed().as_secs_f64() * 1000.0);

        debug!(
            set_count = self.disk_set.len(),
            all_at_eof = all_at_eof,
            error_count = errs.iter().filter(|err| err.is_some()).count(),
            "sets list_merged finished"
        );

        _ = all_at_eof;
        Ok(Vec::new())
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

        let mut futures = Vec::new();
        let mut inputs = Vec::new();

        for set in &self.disk_set {
            let (mut disks, infos, _) = set.get_online_disks_with_healing_and_info(true).await;
            let opts = opts.clone();
            let (sender, list_out_rx) = mpsc::channel::<MetaCacheEntry>(1);
            inputs.push(list_out_rx);
            let rx_clone = rx.clone();
            let set = set.clone();
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
                let enforce_write_quorum = enforce_latest_listing_write_quorum(opts.latest_only, &opts.ask_disks);
                let write_quorum_parity = set.default_parity_count;
                let required_obj_quorum = latest_listing_required_object_quorum(
                    listing_quorum,
                    set.set_drive_count,
                    write_quorum_parity,
                    enforce_write_quorum,
                );
                ask_disks = expand_ask_disks_for_object_quorum(ask_disks, disks.len(), required_obj_quorum);
                let fallback_disks = if let Some(asked_disks) = positive_ask_disks(ask_disks)
                    && disks.len() > asked_disks
                {
                    let mut rand = rand::rng();
                    disks.shuffle(&mut rand);
                    disks.split_off(asked_disks)
                } else {
                    Vec::new()
                };
                let fallback_disks = Arc::new(fallback_disks);
                let claim_tracker = FallbackClaimTracker::default();

                let obj_quorum =
                    latest_listing_object_quorum(listing_quorum, set.set_drive_count, write_quorum_parity, enforce_write_quorum);
                let raw_min_disks = latest_listing_raw_min_disks(listing_quorum, obj_quorum, enforce_write_quorum);
                let resolver = list_metadata_resolution_params(bucket.to_owned(), listing_quorum, obj_quorum, !opts.latest_only);
                let agreed_resolver = resolver.clone();
                let partial_resolver = resolver.clone();
                let reader_disks = disks.len();

                let path = base_dir_from_prefix(prefix);
                ensure_non_empty_listing_disks(bucket, &path, &disks)?;

                let mut filter_prefix = prefix
                    .trim_start_matches(&path)
                    .trim_start_matches(SLASH_SEPARATOR)
                    .trim_end_matches(SLASH_SEPARATOR)
                    .to_owned();
                if filter_prefix == path {
                    filter_prefix = "".to_owned();
                }

                let tx1 = sender.clone();
                let tx2 = sender.clone();
                let supplement = ListingSupplement::new(
                    ListingSupplementOptions {
                        bucket: bucket.to_owned(),
                        path: path.clone(),
                        recursive: true,
                        filter_prefix: Some(filter_prefix.clone()),
                        forward_to: opts.marker.clone(),
                        per_disk_limit: bounded_usize_to_i32(opts.limit),
                        skip_total_timeout: false,
                    },
                    fallback_disks.clone(),
                    claim_tracker.clone(),
                );
                let agreed_supplement = supplement.clone();
                let partial_supplement = supplement;

                list_path_raw_with_claim_tracker(
                    rx_clone,
                    ListPathRawOptions {
                        disks: disks.iter().cloned().map(Some).collect(),
                        fallback_disks: fallback_disks.iter().cloned().map(Some).collect(),
                        bucket: bucket.to_owned(),
                        path,
                        recursive: true,
                        filter_prefix: Some(filter_prefix),
                        forward_to: opts.marker.clone(),
                        min_disks: raw_min_disks,
                        per_disk_limit: bounded_usize_to_i32(opts.limit),
                        agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                            Box::pin({
                                let value = tx1.clone();
                                let resolver = agreed_resolver.clone();
                                let supplement = agreed_supplement.clone();
                                async move {
                                    let entry = match resolve_agreed_listing_entry(
                                        entry,
                                        reader_disks,
                                        resolver.clone(),
                                        enforce_write_quorum,
                                    ) {
                                        ListingEntryResolution::Resolved(entry) => entry,
                                        ListingEntryResolution::NeedsSupplement(entry, fallback) => {
                                            let Some(entry) = resolve_agreed_listing_entry_with_supplement(
                                                entry,
                                                reader_disks,
                                                resolver,
                                                enforce_write_quorum,
                                                supplement,
                                            )
                                            .await
                                            .or(fallback) else {
                                                return;
                                            };
                                            entry
                                        }
                                        ListingEntryResolution::Rejected => return,
                                    };
                                    if entry.is_dir() {
                                        return;
                                    }
                                    if let Err(err) = value.send(entry).await {
                                        error!("list_path send fail {:?}", err);
                                    }
                                }
                            })
                        })),
                        partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<DiskError>]| {
                            Box::pin({
                                let value = tx2.clone();
                                let resolver = partial_resolver.clone();
                                let supplement = partial_supplement.clone();
                                async move {
                                    if let Some(entry) = resolve_listing_entries_with_supplement(
                                        entries,
                                        resolver,
                                        enforce_write_quorum,
                                        supplement,
                                    )
                                    .await
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
                    claim_tracker,
                )
                .await
            });
        }

        let (merge_tx, mut merge_rx) = mpsc::channel::<MetaCacheEntry>(100);
        let bucket = bucket.to_owned();
        let bucket_clone = bucket.clone();

        let vcf = match get_versioning_config(&bucket).await {
            Ok((res, _)) => Some(res),
            Err(_) => None,
        };

        tokio::spawn(
            async move {
                let mut sent_err = false;
                while let Some(entry) = merge_rx.recv().await {
                    if opts.latest_only {
                        let fi = match entry.to_fileinfo(&bucket_clone) {
                            Ok(res) => res,
                            Err(err) => {
                                if !sent_err {
                                    let item = ObjectInfoOrErr {
                                        item: None,
                                        err: Some(err.into()),
                                    };
                                    if let Err(err) = result.send(item).await {
                                        error!("walk result send err {:?}", err);
                                    }
                                    sent_err = true;
                                    return;
                                }
                                continue;
                            }
                        };

                        if let Some(filter) = opts.filter {
                            if filter(&fi) {
                                let item = ObjectInfoOrErr {
                                    item: Some(ObjectInfo::from_file_info(&fi, &bucket_clone, &fi.name, {
                                        if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                    })),
                                    err: None,
                                };
                                if let Err(err) = result.send(item).await {
                                    error!("walk result send err {:?}", err);
                                }
                            }
                        } else {
                            let item = ObjectInfoOrErr {
                                item: Some(ObjectInfo::from_file_info(&fi, &bucket_clone, &fi.name, {
                                    if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                })),
                                err: None,
                            };
                            if let Err(err) = result.send(item).await {
                                error!("walk result send err {:?}", err);
                            }
                        }
                        continue;
                    }

                    let fvs = match if opts.include_free_versions {
                        entry.file_info_versions_with_free_versions(&bucket_clone)
                    } else {
                        entry.file_info_versions(&bucket_clone)
                    } {
                        Ok(res) => res,
                        Err(err) => {
                            let item = ObjectInfoOrErr {
                                item: None,
                                err: Some(err.into()),
                            };
                            if let Err(err) = result.send(item).await {
                                error!("walk result send err {:?}", err);
                            }
                            return;
                        }
                    };

                    for fi in &fvs.versions {
                        if let Some(filter) = opts.filter {
                            if filter(fi) {
                                let item = ObjectInfoOrErr {
                                    item: Some(ObjectInfo::from_file_info(fi, &bucket_clone, &fi.name, {
                                        if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                    })),
                                    err: None,
                                };
                                if let Err(err) = result.send(item).await {
                                    error!("walk result send err {:?}", err);
                                }
                            }
                        } else {
                            let item = ObjectInfoOrErr {
                                item: Some(ObjectInfo::from_file_info(fi, &bucket_clone, &fi.name, {
                                    if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                })),
                                err: None,
                            };
                            if let Err(err) = result.send(item).await {
                                error!("walk result send err {:?}", err);
                            }
                        }
                    }

                    if opts.include_free_versions {
                        for fi in &fvs.free_versions {
                            if let Some(filter) = opts.filter {
                                if filter(fi) {
                                    let item = ObjectInfoOrErr {
                                        item: Some(ObjectInfo::from_file_info(fi, &bucket_clone, &fi.name, {
                                            if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                        })),
                                        err: None,
                                    };
                                    if let Err(err) = result.send(item).await {
                                        error!("walk result send err {:?}", err);
                                    }
                                }
                            } else {
                                let item = ObjectInfoOrErr {
                                    item: Some(ObjectInfo::from_file_info(fi, &bucket_clone, &fi.name, {
                                        if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                    })),
                                    err: None,
                                };
                                if let Err(err) = result.send(item).await {
                                    error!("walk result send err {:?}", err);
                                }
                            }
                        }
                    }
                }
            }
            .instrument(tracing::Span::current()),
        );

        tokio::spawn(
            async move {
                if let Err(err) = merge_entry_channels(rx, inputs, merge_tx, 1).await {
                    error!("merge_entry_channels err {:?}", err)
                }
            }
            .instrument(tracing::Span::current()),
        );

        let walk_started = std::time::Instant::now();
        let walk_results = join_all(futures).await;
        let mut errs = Vec::new();
        for walk_result in walk_results {
            match walk_result {
                Ok(()) => errs.push(None),
                Err(err) => errs.push(Some(err.into())),
            }
        }
        rustfs_io_metrics::record_stage_duration(
            "sets_list_objects_walk_internal",
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

impl SetDisks {
    #[allow(clippy::too_many_arguments)]
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
        let marker = if continuation_token.is_none() {
            start_after
        } else {
            continuation_token.clone()
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
        let max_keys = normalize_max_keys(max_keys);
        let effective_max_keys = if max_keys <= 0 { 0 } else { max_keys_plus_one(max_keys, true) };
        let opts = ListPathOptions {
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
            separator: delimiter.clone(),
            limit: effective_max_keys,
            marker,
            incl_deleted,
            ask_disks: list_objects_quorum_from_env(),
            ..Default::default()
        };

        if !opts.prefix.is_empty() && max_keys == 1 && opts.marker.is_none() {
            match self
                .get_object_info(
                    &opts.bucket,
                    &opts.prefix,
                    &ObjectOptions {
                        no_lock: true,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(res) => {
                    return Ok(ListObjectsInfo {
                        objects: vec![res],
                        ..Default::default()
                    });
                }
                Err(err) => {
                    if is_err_bucket_not_found(&err) {
                        return Err(err);
                    }
                }
            };
        }

        let mut list_result = self
            .list_path_result(&opts)
            .await
            .unwrap_or_else(|err| MetaCacheEntriesSortedResult {
                err: Some(err.into()),
                ..Default::default()
            });
        let next_cache_id = list_result.entries.as_ref().and_then(|entries| entries.list_id.clone());

        let disk_has_more = list_result.err.is_none();

        if let Some(err) = list_result.err.take()
            && err != rustfs_filemeta::Error::Unexpected
        {
            return Err(to_object_err(err.into(), vec![bucket, prefix]));
        }

        if let Some(result) = list_result.entries.as_mut() {
            result.forward_past(opts.marker);
        }

        let get_objects = ObjectInfo::from_meta_cache_entries_sorted_infos(
            &list_result.entries.unwrap_or_default(),
            bucket,
            prefix,
            delimiter.clone(),
        )
        .await;

        let (objects, prefixes, is_truncated, next_marker, _next_version_idmarker) =
            list_objects_paginate(get_objects, &delimiter, max_keys, disk_has_more, next_cache_id.as_deref(), false);

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

        let effective_max_keys = if max_keys <= 0 { 0 } else { max_keys_plus_one(max_keys, true) };
        let opts = ListPathOptions {
            bucket: bucket.to_owned(),
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

        let mut list_result = self
            .list_path_result(&opts)
            .await
            .unwrap_or_else(|err| MetaCacheEntriesSortedResult {
                err: Some(err.into()),
                ..Default::default()
            });
        let next_cache_id = list_result.entries.as_ref().and_then(|entries| entries.list_id.clone());

        let disk_has_more = list_result.err.is_none();

        if let Some(err) = list_result.err.take()
            && err != rustfs_filemeta::Error::Unexpected
        {
            return Err(to_object_err(err.into(), vec![bucket, prefix]));
        }

        if let Some(result) = list_result.entries.as_mut()
            && !has_version_marker
        {
            result.forward_past(opts.marker.clone());
        }

        let version_marker = version_marker_for_entries(list_result.entries.as_ref(), opts.marker.as_deref(), version_marker);

        let get_objects = ObjectInfo::from_meta_cache_entries_sorted_versions(
            &list_result.entries.unwrap_or_default(),
            bucket,
            prefix,
            delimiter.clone(),
            version_marker,
        )
        .await;

        let (objects, prefixes, is_truncated, next_marker, next_version_idmarker) =
            list_objects_paginate(get_objects, &delimiter, max_keys, disk_has_more, next_cache_id.as_deref(), true);

        Ok(ListObjectVersionsInfo {
            is_truncated,
            next_marker,
            next_version_idmarker,
            objects,
            prefixes,
        })
    }

    pub async fn walk_internal(
        self: Arc<Self>,
        rx: CancellationToken,
        bucket: &str,
        prefix: &str,
        result: Sender<ObjectInfoOrErr>,
        opts: WalkOptions,
    ) -> Result<()> {
        check_list_objs_args(bucket, prefix, &None)?;

        let (entry_tx, mut entry_rx) = mpsc::channel::<MetaCacheEntry>(100);
        let bucket_name = bucket.to_owned();
        let bucket_name_for_list = bucket_name.clone();

        let versioning_config = match get_versioning_config(&bucket_name).await {
            Ok((res, _)) => Some(res),
            Err(_) => None,
        };

        let result_task = tokio::spawn(
            async move {
                while let Some(entry) = entry_rx.recv().await {
                    if opts.latest_only {
                        let fi = match entry.to_fileinfo(&bucket_name) {
                            Ok(res) => res,
                            Err(err) => {
                                let item = ObjectInfoOrErr {
                                    item: None,
                                    err: Some(err.into()),
                                };
                                let _ = result.send(item).await;
                                return;
                            }
                        };

                        if let Some(filter) = opts.filter
                            && !filter(&fi)
                        {
                            continue;
                        }

                        let item = ObjectInfoOrErr {
                            item: Some(ObjectInfo::from_file_info(&fi, &bucket_name, &fi.name, {
                                if let Some(v) = &versioning_config {
                                    v.versioned(&fi.name)
                                } else {
                                    false
                                }
                            })),
                            err: None,
                        };
                        let _ = result.send(item).await;
                        continue;
                    }

                    let versions = match if opts.include_free_versions {
                        entry.file_info_versions_with_free_versions(&bucket_name)
                    } else {
                        entry.file_info_versions(&bucket_name)
                    } {
                        Ok(res) => res,
                        Err(err) => {
                            let item = ObjectInfoOrErr {
                                item: None,
                                err: Some(err.into()),
                            };
                            let _ = result.send(item).await;
                            return;
                        }
                    };

                    for fi in &versions.versions {
                        if let Some(filter) = opts.filter
                            && !filter(fi)
                        {
                            continue;
                        }

                        let item = ObjectInfoOrErr {
                            item: Some(ObjectInfo::from_file_info(fi, &bucket_name, &fi.name, {
                                if let Some(v) = &versioning_config {
                                    v.versioned(&fi.name)
                                } else {
                                    false
                                }
                            })),
                            err: None,
                        };
                        let _ = result.send(item).await;
                    }

                    if opts.include_free_versions {
                        for fi in &versions.free_versions {
                            if let Some(filter) = opts.filter
                                && !filter(fi)
                            {
                                continue;
                            }

                            let item = ObjectInfoOrErr {
                                item: Some(ObjectInfo::from_file_info(fi, &bucket_name, &fi.name, {
                                    if let Some(v) = &versioning_config {
                                        v.versioned(&fi.name)
                                    } else {
                                        false
                                    }
                                })),
                                err: None,
                            };
                            let _ = result.send(item).await;
                        }
                    }
                }
            }
            .instrument(tracing::Span::current()),
        );

        let limit = i32::try_from(opts.limit).unwrap_or(i32::MAX);
        let list_result = self
            .list_path(
                rx,
                ListPathOptions {
                    bucket: bucket_name_for_list,
                    prefix: prefix.to_owned(),
                    marker: opts.marker.clone(),
                    limit,
                    ask_disks: opts.ask_disks.clone(),
                    incl_deleted: true,
                    recursive: true,
                    versioned: true,
                    ..Default::default()
                },
                entry_tx,
            )
            .await;

        let _ = result_task.await;

        match list_result {
            Ok(()) => Ok(()),
            Err(err) => walk_result_from_set_errors(&[Some(err)]),
        }
    }

    pub async fn list_path_result(self: Arc<Self>, o: &ListPathOptions) -> Result<MetaCacheEntriesSortedResult> {
        check_list_objs_args(&o.bucket, &o.prefix, &o.marker)?;

        let list_path_started = std::time::Instant::now();

        let mut o = o.clone();
        o.marker = o.marker.filter(|v| v >= &o.prefix);

        if let Some(marker) = &o.marker
            && !o.prefix.is_empty()
            && !marker.starts_with(&o.prefix)
        {
            return Err(Error::Unexpected);
        }

        if o.limit == 0 {
            return Err(Error::Unexpected);
        }

        if o.prefix.starts_with(SLASH_SEPARATOR) {
            return Err(Error::Unexpected);
        }

        let slash_separator = Some(SLASH_SEPARATOR.to_owned());
        o.include_directories = o.separator == slash_separator;

        if (o.separator == slash_separator || o.separator.is_none()) && !o.recursive {
            o.recursive = o.separator != slash_separator;
            o.separator = slash_separator;
        } else {
            o.recursive = true;
        }

        o.parse_marker();

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
        let (err_tx, mut err_rx) = broadcast::channel::<Arc<Error>>(1);
        let (sender, recv) = mpsc::channel(o.limit as usize);

        let set = self.clone();
        let opts = o.clone();
        let cancel_rx1 = cancel.clone();
        let cancel_rx1_for_err = cancel_rx1.clone();
        let err_tx1 = err_tx.clone();
        let job1_context = log_context.clone();
        let job1 = tokio::spawn(
            async move {
                let mut opts = opts;
                opts.stop_disk_at_limit = true;
                if let Err(err) = set.list_path(cancel_rx1, opts, sender).await
                    && !cancel_rx1_for_err.is_cancelled()
                {
                    log_list_path_worker_error("set_disks", "list_path", &job1_context, &err);
                    let _ = err_tx1.send(Arc::new(err));
                }
            }
            .instrument(tracing::Span::current()),
        );

        let cancel_rx2 = cancel.clone();
        let (result_tx, mut result_rx) = mpsc::channel(1);
        let err_tx2 = err_tx.clone();
        let opts = o.clone();
        let job2_context = log_context.clone();
        let job2 = tokio::spawn(
            async move {
                match gather_results(cancel_rx2, opts, recv, result_tx).await {
                    Ok(GatherResultsState::LimitReached) => cancel.cancel(),
                    Ok(GatherResultsState::InputClosed) => {}
                    Err(err) => {
                        log_list_path_worker_error("set_disks", "gather_results", &job2_context, &err);
                        let _ = err_tx2.send(Arc::new(err));
                        cancel.cancel();
                    }
                }
            }
            .instrument(tracing::Span::current()),
        );

        let mut result = tokio::select! {
            res = err_rx.recv() => {
                match res {
                    Ok(err) => {
                        log_list_path_worker_error("set_disks", "worker_error", &log_context, err.as_ref());
                        MetaCacheEntriesSortedResult { entries: None, err: Some(err.as_ref().clone().into()) }
                    },
                    Err(err) => {
                        log_list_path_worker_error("set_disks", "error_channel_closed", &log_context, &err);
                        MetaCacheEntriesSortedResult { entries: None, err: Some(rustfs_filemeta::Error::other(err)) }
                    },
                }
            }
            Some(result) = result_rx.recv() => result,
        };

        join_all(vec![job1, job2]).await;

        if let Ok(err) = err_rx.try_recv() {
            log_list_path_worker_error("set_disks", "trailing_worker_error", &log_context, err.as_ref());
            result.err = Some(err.as_ref().clone().into());
        }

        if result.err.is_some() {
            log_list_path_finished("set_disks", &log_context, list_path_started.elapsed().as_secs_f64() * 1000.0, 0, true);
            return Ok(result);
        }

        if let Some(entries) = result.entries.as_mut() {
            entries.reuse = true;
            let truncated = !entries.entries().is_empty() || result.err.is_none();
            entries.o.0.truncate(o.limit as usize);
            if !o.transient && truncated {
                entries.list_id = if let Some(id) = o.id {
                    Some(id)
                } else {
                    Some(Uuid::new_v4().to_string())
                };
            }

            if !truncated {
                result.err = Some(Error::Unexpected.into());
            }
        }

        rustfs_io_metrics::record_stage_duration(
            "sets_list_objects_list_path",
            list_path_started.elapsed().as_secs_f64() * 1000.0,
        );

        log_list_path_finished(
            "set_disks",
            &log_context,
            list_path_started.elapsed().as_secs_f64() * 1000.0,
            result
                .entries
                .as_ref()
                .map(|entries| entries.entries().len())
                .unwrap_or_default(),
            result.err.is_some(),
        );

        Ok(result)
    }

    pub async fn list_path(&self, rx: CancellationToken, opts: ListPathOptions, sender: Sender<MetaCacheEntry>) -> Result<()> {
        let list_path_started = std::time::Instant::now();

        let (mut disks, infos, _) = self.get_online_disks_with_healing_and_info(true).await;

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
        ensure_non_empty_listing_disks(&opts.bucket, &opts.base_dir, &disks)?;

        let enforce_write_quorum = enforce_latest_listing_write_quorum(!opts.versioned, &opts.ask_disks);
        let write_quorum_parity = self.default_parity_count;
        let required_obj_quorum = latest_listing_required_object_quorum(
            listing_quorum,
            self.set_drive_count,
            write_quorum_parity,
            enforce_write_quorum,
        );
        ask_disks = expand_ask_disks_for_object_quorum(ask_disks, disks.len(), required_obj_quorum);
        let mut fallback_disks = Vec::new();

        if let Some(asked_disks) = positive_ask_disks(ask_disks)
            && disks.len() > asked_disks
        {
            let mut rand = rand::rng();
            disks.shuffle(&mut rand);

            fallback_disks = disks.split_off(asked_disks);
        }
        let fallback_disks = Arc::new(fallback_disks);
        let claim_tracker = FallbackClaimTracker::default();

        let bucket = opts.bucket.clone();
        let base_dir = opts.base_dir.clone();
        let latest_object_quorum =
            latest_listing_object_quorum(listing_quorum, self.set_drive_count, write_quorum_parity, enforce_write_quorum);
        let raw_min_disks = latest_listing_raw_min_disks(listing_quorum, latest_object_quorum, enforce_write_quorum);
        let resolver = list_metadata_resolution_params(bucket.clone(), listing_quorum, latest_object_quorum, opts.versioned);
        let agreed_resolver = resolver.clone();
        let partial_resolver = resolver.clone();
        let reader_disks = disks.len();

        debug!(
            bucket = %bucket,
            prefix = %base_dir,
            set_drive_count = self.set_drive_count,
            asked_disks = ask_disks,
            listing_quorum = listing_quorum,
            latest_object_quorum = latest_object_quorum,
            raw_min_disks = raw_min_disks,
            fallback_disks = fallback_disks.len(),
            limit = opts.limit,
            stop_disk_at_limit = opts.stop_disk_at_limit,
            "set_disks list_path selected listing quorum and fallback disks"
        );

        let limit = {
            if opts.limit > 0 && opts.stop_disk_at_limit {
                opts.limit + 4 + (opts.limit / 16)
            } else {
                0
            }
        };

        let tx1 = sender.clone();
        let tx2 = sender.clone();
        let cancel_for_send1 = rx.clone();
        let cancel_for_send2 = rx.clone();
        let supplement = ListingSupplement::new(
            ListingSupplementOptions {
                bucket: bucket.clone(),
                path: opts.base_dir.clone(),
                recursive: opts.recursive,
                filter_prefix: opts.filter_prefix.clone(),
                forward_to: opts.marker.clone(),
                per_disk_limit: limit,
                skip_total_timeout: false,
            },
            fallback_disks.clone(),
            claim_tracker.clone(),
        );
        let agreed_supplement = supplement.clone();
        let partial_supplement = supplement;

        let result = list_path_raw_with_claim_tracker(
            rx,
            ListPathRawOptions {
                disks: disks.iter().cloned().map(Some).collect(),
                fallback_disks: fallback_disks.iter().cloned().map(Some).collect(),
                bucket: opts.bucket,
                path: opts.base_dir,
                recursive: opts.recursive,
                filter_prefix: opts.filter_prefix,
                forward_to: opts.marker,
                min_disks: raw_min_disks,
                per_disk_limit: limit,
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    Box::pin({
                        let value = tx1.clone();
                        let cancel_token = cancel_for_send1.clone();
                        let resolver = agreed_resolver.clone();
                        let supplement = agreed_supplement.clone();
                        async move {
                            if cancel_token.is_cancelled() {
                                return;
                            }

                            let entry =
                                match resolve_agreed_listing_entry(entry, reader_disks, resolver.clone(), enforce_write_quorum) {
                                    ListingEntryResolution::Resolved(entry) => entry,
                                    ListingEntryResolution::NeedsSupplement(entry, fallback) => {
                                        if cancel_token.is_cancelled() {
                                            return;
                                        }

                                        let Some(entry) = resolve_agreed_listing_entry_with_supplement(
                                            entry,
                                            reader_disks,
                                            resolver,
                                            enforce_write_quorum,
                                            supplement,
                                        )
                                        .await
                                        .or(fallback) else {
                                            return;
                                        };
                                        entry
                                    }
                                    ListingEntryResolution::Rejected => return,
                                };

                            if let Err(err) = send_or_cancel(&cancel_token, &value, entry).await
                                && !cancel_token.is_cancelled()
                            {
                                error!("list_path send fail {:?}", err);
                            }
                        }
                    })
                })),
                partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<DiskError>]| {
                    Box::pin({
                        let value = tx2.clone();
                        let resolver = partial_resolver.clone();
                        let cancel_token = cancel_for_send2.clone();
                        let supplement = partial_supplement.clone();
                        async move {
                            if cancel_token.is_cancelled() {
                                return;
                            }

                            if let Some(entry) =
                                resolve_listing_entries_with_supplement(entries, resolver, enforce_write_quorum, supplement).await
                                && let Err(err) = send_or_cancel(&cancel_token, &value, entry).await
                                && !cancel_token.is_cancelled()
                            {
                                error!("list_path send fail {:?}", err);
                            }
                        }
                    })
                })),
                finished: None,
                ..Default::default()
            },
            claim_tracker,
        )
        .await;

        rustfs_io_metrics::record_stage_duration(
            "set_disks_list_objects_list_path",
            list_path_started.elapsed().as_secs_f64() * 1000.0,
        );

        if let Err(ref err) = result {
            debug!(
                bucket = %bucket,
                path = %base_dir,
                disk_count = disks.len(),
                fallback_disks = fallback_disks.len(),
                asked_disks = ask_disks,
                listing_quorum = listing_quorum,
                stop_disk_at_limit = opts.stop_disk_at_limit,
                limit = opts.limit,
                per_disk_limit = limit,
                elapsed_ms = list_path_started.elapsed().as_secs_f64() * 1000.0,
                list_error = %err,
                "set_disks list_path finished with error"
            );
        } else {
            debug!(
                bucket = %bucket,
                path = %base_dir,
                disk_count = disks.len(),
                fallback_disks = fallback_disks.len(),
                asked_disks = ask_disks,
                listing_quorum = listing_quorum,
                stop_disk_at_limit = opts.stop_disk_at_limit,
                limit = opts.limit,
                per_disk_limit = limit,
                elapsed_ms = list_path_started.elapsed().as_secs_f64() * 1000.0,
                "set_disks list_path finished"
            );
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
mod test {
    use super::{
        ENV_API_LIST_OBJECTS_INDEX_MODE, ENV_API_LIST_OBJECTS_INDEX_PROVIDER, ENV_API_LIST_OBJECTS_INDEX_PROVIDER_GENERATION,
        ENV_API_LIST_OBJECTS_INDEX_PROVIDER_PATH, ENV_API_LIST_OBJECTS_NAMESPACE_JOURNAL_PATH, ENV_API_LIST_OBJECTS_QUORUM,
        ENV_API_LIST_QUORUM, FallbackListingEntries, FallbackListingEntry, GatherResultsState, LIST_CURSOR_GENERATION_LIVE,
        LIST_OBJECTS_INDEX_PROVIDER_PERSISTENT_KEY_ONLY, LIST_OBJECTS_INDEX_PROVIDER_WALKER_KEY_ONLY, ListIndexFallbackReason,
        ListIndexLifecycle, ListIndexLifecycleState, ListIndexSourceDecision, ListMetadataAuthority, ListMetadataIndexGeneration,
        ListMetadataIndexHealth, ListMetadataIndexKeyLookup, ListMetadataIndexPage, ListMetadataIndexPageIterator,
        ListObjectsIndexProviderKind, ListObjectsIndexProviderState, ListPathOptions, ListPathRawOptions, ListSourceMode,
        ListingEntryResolution, ListingSupplement, ListingSupplementOptions, MAX_OBJECT_LIST, NamespaceMutationJournalBackend,
        NamespaceMutationJournalSnapshot, NamespaceMutationJournalStatus, PersistentKeyOnlyIndex, VerifiedIndexCandidateStats,
        VersionMarker, current_list_objects_mutation_sequence, enforce_latest_listing_write_quorum,
        expand_ask_disks_for_object_quorum, fallback_entries_for_object, gather_results, latest_listing_allow_agreed_objects,
        latest_listing_object_quorum, latest_listing_raw_min_disks, latest_listing_required_object_quorum,
        list_metadata_resolution_params, list_objects_from_verified_index_candidates,
        list_objects_from_verified_index_candidates_with_optional_stats, list_objects_from_verified_index_candidates_with_stats,
        list_objects_index_mode_from_env, list_objects_index_provider_from_env, list_objects_index_provider_state_from_env,
        list_objects_key_only_provider_health, list_objects_quorum_from_env, list_quorum_from_env,
        load_namespace_mutation_journal_state, load_persistent_key_only_index, max_keys_plus_one, merge_entry_channels,
        normalize_list_quorum, observe_list_objects_mutations_with_store, parse_namespace_mutation_journal_state,
        parse_persistent_key_only_index, parse_version_marker, persist_observed_list_objects_mutation,
        persistent_key_only_index_health, persistent_key_only_index_matches_provider, record_list_objects_index_opt_in_fallback,
        reset_list_objects_mutation_sequences_for_test, resolve_agreed_listing_entry, resolve_listing_entries,
        select_list_index_provider_source_mode, select_list_index_source_mode, send_or_cancel, version_marker_for_entries,
        walk_result_from_set_errors, write_namespace_mutation_journal_state, write_persistent_key_only_index,
    };
    use crate::cache_value::metacache_set::{FallbackClaimTracker, TestReaderBehavior, list_path_raw};
    use crate::disk::{DiskAPI, DiskOption, endpoint::Endpoint, error::DiskError, new_disk};
    use crate::error::StorageError;
    use crate::object_api::ObjectInfo;
    use rustfs_filemeta::{
        FileInfo, FileMeta, FileMetaVersion, MetaCacheEntries, MetaCacheEntriesSorted, MetaCacheEntry, MetaDeleteMarker,
        VersionType,
    };
    use std::collections::{HashMap, HashSet};
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    struct TestIndexGeneration {
        id: String,
    }

    impl ListMetadataIndexGeneration for TestIndexGeneration {
        fn generation_id(&self) -> &str {
            &self.id
        }
    }

    struct TestIndexHealth {
        reason: Option<ListIndexFallbackReason>,
    }

    impl ListMetadataIndexHealth for TestIndexHealth {
        fn fallback_reason(&self) -> Option<ListIndexFallbackReason> {
            self.reason
        }
    }

    struct TestIndexPage {
        mode: ListSourceMode,
        generation: TestIndexGeneration,
        keys: Vec<String>,
    }

    impl ListMetadataIndexPage for TestIndexPage {
        fn source_mode(&self) -> ListSourceMode {
            self.mode
        }

        fn generation(&self) -> &dyn ListMetadataIndexGeneration {
            &self.generation
        }

        fn keys(&self) -> &[String] {
            &self.keys
        }
    }

    struct TestIndexIterator {
        pages: std::vec::IntoIter<TestIndexPage>,
    }

    impl ListMetadataIndexPageIterator for TestIndexIterator {
        type Page = TestIndexPage;

        fn next_page(&mut self) -> Option<Self::Page> {
            self.pages.next()
        }
    }

    struct TestIndexLookup {
        decision: ListIndexSourceDecision,
    }

    impl ListMetadataIndexKeyLookup for TestIndexLookup {
        type Page = TestIndexPage;

        fn lookup_page(&self, _opts: &ListPathOptions) -> ListIndexSourceDecision {
            self.decision
        }
    }

    fn test_meta_entry(name: &str) -> MetaCacheEntry {
        MetaCacheEntry {
            name: name.to_owned(),
            ..Default::default()
        }
    }

    fn test_live_object_info(name: &str, etag: &str) -> ObjectInfo {
        let mut fi = FileInfo {
            name: name.to_owned(),
            size: 1,
            mod_time: Some(time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp")),
            ..Default::default()
        };
        fi.metadata.insert("etag".to_string(), etag.to_string());
        ObjectInfo::from_file_info(&fi, "bucket", name, false)
    }

    fn test_object_meta_entry(name: &str) -> MetaCacheEntry {
        let mut meta = FileMeta::new();
        meta.add_version(FileInfo {
            volume: "bucket".to_owned(),
            name: name.to_owned(),
            size: 1,
            mod_time: Some(time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp")),
            ..Default::default()
        })
        .expect("test metadata should accept object version");
        let metadata = meta.marshal_msg().expect("test metadata should marshal");

        MetaCacheEntry {
            name: name.to_owned(),
            metadata,
            cached: Some(meta),
            reusable: false,
        }
    }

    #[test]
    fn fallback_entries_for_object_filters_claimed_physical_disks() {
        let mut entries = FallbackListingEntries::new();
        entries.insert(
            "object".to_string(),
            vec![
                FallbackListingEntry {
                    endpoint: "disk-a".to_string(),
                    entry: test_meta_entry("object"),
                },
                FallbackListingEntry {
                    endpoint: "disk-b".to_string(),
                    entry: test_meta_entry("object"),
                },
            ],
        );
        let claimed = HashSet::from(["disk-a".to_string()]);

        let candidates = fallback_entries_for_object(&entries, "object", &claimed);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].as_ref().map(|entry| entry.name.as_str()), Some("object"));
    }

    #[test]
    fn listing_supplement_uses_page_cache_only_for_bounded_walks() {
        let bounded = ListingSupplement::new(
            ListingSupplementOptions {
                bucket: "bucket".to_string(),
                path: String::new(),
                recursive: true,
                filter_prefix: None,
                forward_to: None,
                per_disk_limit: 100,
                skip_total_timeout: true,
            },
            Arc::new(Vec::new()),
            FallbackClaimTracker::default(),
        );
        let unbounded = ListingSupplement::new(
            ListingSupplementOptions {
                bucket: "bucket".to_string(),
                path: String::new(),
                recursive: true,
                filter_prefix: None,
                forward_to: None,
                per_disk_limit: 0,
                skip_total_timeout: false,
            },
            Arc::new(Vec::new()),
            FallbackClaimTracker::default(),
        );

        assert!(bounded.entries.is_some());
        assert!(bounded.options.skip_total_timeout);
        assert!(unbounded.entries.is_none());
        assert!(!unbounded.options.skip_total_timeout);
    }

    #[tokio::test]
    async fn listing_supplement_unbounded_reads_object_metadata_from_fallback_disk() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let endpoint =
            Endpoint::try_from(tempdir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("local disk should be created");
        let mut fi = FileInfo::new("object", 1, 1);
        fi.volume = "bucket".to_owned();
        fi.name = "object".to_owned();
        fi.size = 1;
        fi.fresh = true;
        fi.erasure.index = 1;
        fi.mod_time = Some(time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp"));
        fi.metadata.insert("etag".to_owned(), "object-etag".to_owned());
        disk.write_metadata("", "bucket", "object", fi)
            .await
            .expect("test metadata should be written");

        let supplement = ListingSupplement::new(
            ListingSupplementOptions {
                bucket: "bucket".to_string(),
                path: String::new(),
                recursive: true,
                filter_prefix: None,
                forward_to: None,
                per_disk_limit: 0,
                skip_total_timeout: false,
            },
            Arc::new(vec![disk]),
            FallbackClaimTracker::default(),
        );

        let entries = supplement.entries_for("object").await;

        assert!(supplement.entries.is_none());
        assert_eq!(entries.len(), 1);
        let entry = entries
            .into_iter()
            .next()
            .and_then(|entry| entry)
            .expect("fallback object metadata should be returned");
        let info = entry.to_fileinfo("bucket").expect("fallback metadata should decode");
        assert_eq!(entry.name, "object");
        assert_eq!(info.metadata.get("etag").map(String::as_str), Some("object-etag"));
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
            fi.volume = "bucket".to_owned();
            fi.name = name.to_owned();
            let version_idx = u128::try_from(idx + 1).expect("test version index should fit u128");
            fi.version_id = Some(Uuid::from_u128(version_idx));
            fi.versioned = true;
            fi.size = 1;
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
        assert_eq!(result.entries.unwrap().entries().len(), 1);

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
        let entries = result.entries.unwrap();
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

    #[test]
    fn version_marker_is_applied_only_when_key_marker_entry_is_present() {
        let version_marker = Some(VersionMarker::Null);

        let listed_after_deleted_marker = sorted_entries(&["obj-b", "obj-c"]);
        assert_eq!(
            version_marker_for_entries(Some(&listed_after_deleted_marker), Some("obj-a"), version_marker),
            None
        );

        let listed_with_marker = sorted_entries(&["obj-a", "obj-b"]);
        assert_eq!(
            version_marker_for_entries(Some(&listed_with_marker), Some("obj-a"), version_marker),
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
        let entries = result.entries.unwrap();
        let names = entries
            .entries()
            .into_iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(names, ["obj-b"]);
        assert!(result.err.is_some());

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
        let entries = result.entries.unwrap();
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

        let base_marker = parsed.marker.clone().expect("marker should parse");
        let replay_marker = parsed.encode_marker(base_marker.as_str());
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
    fn list_quorum_from_env_defaults_to_strict() {
        temp_env::with_var_unset(ENV_API_LIST_QUORUM, || {
            assert_eq!(list_quorum_from_env(), "strict");
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_quorum_from_env_honors_supported_value() {
        temp_env::with_var(ENV_API_LIST_QUORUM, Some("auto"), || {
            assert_eq!(list_quorum_from_env(), "auto");
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_quorum_from_env_rejects_unknown_value() {
        temp_env::with_var(ENV_API_LIST_QUORUM, Some("unsafe"), || {
            assert_eq!(list_quorum_from_env(), "strict");
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_objects_quorum_from_env_defaults_to_optimal() {
        temp_env::with_var_unset(ENV_API_LIST_OBJECTS_QUORUM, || {
            assert_eq!(list_objects_quorum_from_env(), "optimal");
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_objects_quorum_from_env_honors_supported_value() {
        temp_env::with_var(ENV_API_LIST_OBJECTS_QUORUM, Some("strict"), || {
            assert_eq!(list_objects_quorum_from_env(), "strict");
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_objects_index_mode_from_env_defaults_to_walker() {
        temp_env::with_var_unset(ENV_API_LIST_OBJECTS_INDEX_MODE, || {
            assert_eq!(list_objects_index_mode_from_env(), None);
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_objects_index_mode_from_env_accepts_key_only_aliases() {
        temp_env::with_var(ENV_API_LIST_OBJECTS_INDEX_MODE, Some("key_only"), || {
            assert_eq!(list_objects_index_mode_from_env(), Some(ListSourceMode::IndexKeyOnly));
        });
        temp_env::with_var(ENV_API_LIST_OBJECTS_INDEX_MODE, Some("index_key_only"), || {
            assert_eq!(list_objects_index_mode_from_env(), Some(ListSourceMode::IndexKeyOnly));
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_objects_index_mode_from_env_accepts_verified_page_aliases() {
        temp_env::with_var(ENV_API_LIST_OBJECTS_INDEX_MODE, Some("verified_page"), || {
            assert_eq!(list_objects_index_mode_from_env(), Some(ListSourceMode::IndexVerifiedPage));
        });
        temp_env::with_var(ENV_API_LIST_OBJECTS_INDEX_MODE, Some("index_verified_page"), || {
            assert_eq!(list_objects_index_mode_from_env(), Some(ListSourceMode::IndexVerifiedPage));
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_objects_index_mode_from_env_rejects_metadata_fast() {
        temp_env::with_var(ENV_API_LIST_OBJECTS_INDEX_MODE, Some("index_metadata_fast"), || {
            assert_eq!(list_objects_index_mode_from_env(), None);
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_objects_index_provider_from_env_is_default_off() {
        temp_env::with_var_unset(ENV_API_LIST_OBJECTS_INDEX_PROVIDER, || {
            assert_eq!(list_objects_index_provider_from_env(), None);
            let health = list_objects_key_only_provider_health(list_objects_index_provider_from_env());
            assert_eq!(health.state, ListIndexLifecycleState::Disabled);
            assert_eq!(health.fallback_reason, Some(ListIndexFallbackReason::Disabled));
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_objects_index_provider_from_env_publishes_live_generation_when_enabled() {
        temp_env::with_var(
            ENV_API_LIST_OBJECTS_INDEX_PROVIDER,
            Some(LIST_OBJECTS_INDEX_PROVIDER_WALKER_KEY_ONLY),
            || {
                assert_eq!(list_objects_index_provider_from_env(), Some(ListObjectsIndexProviderKind::WalkerKeyOnly));
                let health = list_objects_key_only_provider_health(list_objects_index_provider_from_env());
                assert_eq!(health.state, ListIndexLifecycleState::Healthy);
                assert_eq!(health.active_generation.as_deref(), Some(LIST_CURSOR_GENERATION_LIVE));
                assert_eq!(health.fallback_reason, None);
            },
        );
    }

    #[test]
    #[serial_test::serial]
    fn list_objects_index_provider_from_env_accepts_persistent_key_only() {
        temp_env::with_var(
            ENV_API_LIST_OBJECTS_INDEX_PROVIDER,
            Some(LIST_OBJECTS_INDEX_PROVIDER_PERSISTENT_KEY_ONLY),
            || {
                assert_eq!(
                    list_objects_index_provider_from_env(),
                    Some(ListObjectsIndexProviderKind::PersistentKeyOnly)
                );
            },
        );
    }

    #[test]
    #[serial_test::serial]
    fn list_objects_index_provider_state_degrades_persistent_without_path() {
        temp_env::with_var(
            ENV_API_LIST_OBJECTS_INDEX_PROVIDER,
            Some(LIST_OBJECTS_INDEX_PROVIDER_PERSISTENT_KEY_ONLY),
            || {
                temp_env::with_var_unset(ENV_API_LIST_OBJECTS_INDEX_PROVIDER_PATH, || {
                    let provider = list_objects_index_provider_state_from_env().expect("persistent provider should be selected");
                    let health = provider.health_snapshot();

                    assert_eq!(provider.kind, ListObjectsIndexProviderKind::PersistentKeyOnly);
                    assert_eq!(provider.persistent_path, None);
                    assert_eq!(health.state, ListIndexLifecycleState::Degraded);
                    assert_eq!(health.fallback_reason, Some(ListIndexFallbackReason::Degraded));
                });
            },
        );
    }

    #[test]
    #[serial_test::serial]
    fn list_objects_index_provider_state_rebuilds_persistent_until_index_is_loaded() {
        let index_path = "/tmp/rustfs-listobjects-key-only.index";
        temp_env::with_var(
            ENV_API_LIST_OBJECTS_INDEX_PROVIDER,
            Some(LIST_OBJECTS_INDEX_PROVIDER_PERSISTENT_KEY_ONLY),
            || {
                temp_env::with_var(ENV_API_LIST_OBJECTS_INDEX_PROVIDER_PATH, Some(index_path), || {
                    temp_env::with_var(ENV_API_LIST_OBJECTS_INDEX_PROVIDER_GENERATION, Some("bench-20260706"), || {
                        let provider =
                            list_objects_index_provider_state_from_env().expect("persistent provider should be selected");
                        let health = provider.health_snapshot();

                        assert_eq!(provider.kind, ListObjectsIndexProviderKind::PersistentKeyOnly);
                        assert_eq!(provider.persistent_path.as_deref(), Some(std::path::Path::new(index_path)));
                        assert_eq!(provider.persistent_generation.as_deref(), Some("bench-20260706"));
                        assert_eq!(health.state, ListIndexLifecycleState::Rebuilding);
                        assert_eq!(health.staging_generation.as_deref(), Some("bench-20260706"));
                        assert_eq!(health.fallback_reason, Some(ListIndexFallbackReason::Rebuilding));
                    });
                });
            },
        );
    }

    #[test]
    fn parse_persistent_key_only_index_sorts_and_deduplicates_keys() {
        let index = parse_persistent_key_only_index(
            "# rustfs-listobjects-key-only-v1\n\
             # bucket=photos\n\
             # generation=bench-20260706\n\
             # checkpoint_high_water_mark=99\n\
             photos/c.jpg\n\
             photos/a.jpg\r\n\
             \n\
             photos/b.jpg\n\
             photos/a.jpg\n",
        );

        assert_eq!(index.bucket.as_deref(), Some("photos"));
        assert_eq!(index.generation, "bench-20260706");
        assert_eq!(index.checkpoint_high_water_mark, 99);
        assert_eq!(
            index.keys.as_slice(),
            vec![
                "photos/a.jpg".to_string(),
                "photos/b.jpg".to_string(),
                "photos/c.jpg".to_string()
            ]
        );
    }

    #[test]
    fn parse_namespace_mutation_journal_state_reads_bucket_and_high_water_mark() {
        let state = parse_namespace_mutation_journal_state(
            "# rustfs-listobjects-namespace-journal-v1\n\
             # bucket=photos\n\
             # high_water_mark=123\n\
             # status=healthy\n",
        )
        .expect("journal state should parse");

        assert_eq!(state.bucket.as_deref(), Some("photos"));
        assert_eq!(state.high_water_mark, 123);
        assert_eq!(state.status, NamespaceMutationJournalStatus::Healthy);
    }

    #[tokio::test]
    async fn namespace_mutation_journal_write_never_regresses() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let journal_root = tempdir.path().join("namespace-mutation-journal");

        assert_eq!(
            write_namespace_mutation_journal_state(
                NamespaceMutationJournalBackend::LocalPath(journal_root.clone()),
                "bucket",
                7,
                false,
            )
            .await
            .expect("journal sequence should be written")
            .high_water_mark,
            7
        );
        assert_eq!(
            write_namespace_mutation_journal_state(
                NamespaceMutationJournalBackend::LocalPath(journal_root.clone()),
                "bucket",
                3,
                false,
            )
            .await
            .expect("journal sequence should not regress")
            .high_water_mark,
            7
        );
    }

    #[test]
    #[serial_test::serial]
    fn observed_mutation_persists_namespace_journal_high_water() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let index_path = tempdir.path().join("persistent-key-only.index");
        let journal_root = tempdir.path().join("namespace-mutation-journal");
        let index_path = index_path.to_str().expect("index path should be utf8");
        let journal_root = journal_root.to_str().expect("journal path should be utf8");

        temp_env::with_var(
            ENV_API_LIST_OBJECTS_INDEX_PROVIDER,
            Some(LIST_OBJECTS_INDEX_PROVIDER_PERSISTENT_KEY_ONLY),
            || {
                temp_env::with_var(ENV_API_LIST_OBJECTS_INDEX_PROVIDER_PATH, Some(index_path), || {
                    temp_env::with_var(ENV_API_LIST_OBJECTS_NAMESPACE_JOURNAL_PATH, Some(journal_root), || {
                        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime should start");
                        runtime.block_on(async {
                            reset_list_objects_mutation_sequences_for_test().await;

                            persist_observed_list_objects_mutation(None, "bucket", 11).await;
                            let state = load_namespace_mutation_journal_state(
                                NamespaceMutationJournalBackend::LocalPath(std::path::PathBuf::from(journal_root)),
                                "bucket",
                            )
                            .await
                            .expect("journal state should load")
                            .expect("journal state should exist");

                            assert_eq!(state.high_water_mark, 11);
                            assert_eq!(state.status, NamespaceMutationJournalStatus::Healthy);
                            assert_eq!(current_list_objects_mutation_sequence("bucket").await, 11);
                        });
                    });
                });
            },
        );
    }

    #[tokio::test]
    async fn persistent_key_only_index_load_recovers_mutation_sequence_from_journal() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let index_path = tempdir.path().join("persistent-key-only.index");
        let journal_root = tempdir.path().join("namespace-mutation-journal");
        let keys = vec!["object-a".to_string(), "object-b".to_string()];

        write_persistent_key_only_index(None, &index_path, "bucket", "generation-42", 5, &keys)
            .await
            .expect("index should be written");
        write_namespace_mutation_journal_state(NamespaceMutationJournalBackend::LocalPath(journal_root), "bucket", 9, false)
            .await
            .expect("journal sequence should be written");
        reset_list_objects_mutation_sequences_for_test().await;

        let index = load_persistent_key_only_index(None, &index_path)
            .await
            .expect("index should load");

        assert_eq!(index.checkpoint_high_water_mark, 5);
        assert_eq!(current_list_objects_mutation_sequence("bucket").await, 9);
    }

    #[test]
    fn persistent_key_only_index_provider_match_rejects_configured_generation_mismatch() {
        let index = PersistentKeyOnlyIndex {
            bucket: Some("bucket".to_string()),
            generation: "generation-old".to_string(),
            checkpoint_high_water_mark: 42,
            keys: Arc::new(vec!["a".to_string(), "b".to_string()]),
        };
        let matching_provider = ListObjectsIndexProviderState::persistent_key_only(
            Some(PathBuf::from("/tmp/persistent-key-only.index")),
            Some("generation-old".to_string()),
        );
        let mismatching_provider = ListObjectsIndexProviderState::persistent_key_only(
            Some(PathBuf::from("/tmp/persistent-key-only.index")),
            Some("generation-new".to_string()),
        );

        assert!(persistent_key_only_index_matches_provider(&index, "bucket", &matching_provider));
        assert!(!persistent_key_only_index_matches_provider(&index, "bucket", &mismatching_provider));
        assert!(!persistent_key_only_index_matches_provider(&index, "other-bucket", &matching_provider));
    }

    #[test]
    fn persistent_key_only_index_health_uses_snapshot_generation_and_checkpoint() {
        let index = PersistentKeyOnlyIndex {
            bucket: Some("bucket".to_string()),
            generation: "generation-42".to_string(),
            checkpoint_high_water_mark: 42,
            keys: Arc::new(vec!["a".to_string(), "b".to_string()]),
        };

        let health = persistent_key_only_index_health(
            &index,
            NamespaceMutationJournalSnapshot {
                high_water_mark: 42,
                degraded: false,
            },
        );

        assert_eq!(health.state, ListIndexLifecycleState::Healthy);
        assert_eq!(health.active_generation.as_deref(), Some("generation-42"));
        assert_eq!(health.checkpoint_high_water_mark, 42);
        assert_eq!(health.mutation_high_water_mark, 42);
        assert_eq!(health.fallback_reason, None);
    }

    #[test]
    fn persistent_key_only_index_health_reports_lagging_mutation_checkpoint() {
        let index = PersistentKeyOnlyIndex {
            bucket: Some("bucket".to_string()),
            generation: "generation-42".to_string(),
            checkpoint_high_water_mark: 42,
            keys: Arc::new(vec!["a".to_string(), "b".to_string()]),
        };

        let health = persistent_key_only_index_health(
            &index,
            NamespaceMutationJournalSnapshot {
                high_water_mark: 43,
                degraded: false,
            },
        );

        assert_eq!(health.state, ListIndexLifecycleState::Lagging);
        assert_eq!(health.active_generation.as_deref(), Some("generation-42"));
        assert_eq!(health.checkpoint_high_water_mark, 42);
        assert_eq!(health.mutation_high_water_mark, 43);
        assert_eq!(health.mutation_lag, 1);
        assert_eq!(health.fallback_reason, Some(ListIndexFallbackReason::Lagging));
    }

    #[test]
    fn persistent_key_only_index_health_reports_degraded_journal() {
        let index = PersistentKeyOnlyIndex {
            bucket: Some("bucket".to_string()),
            generation: "generation-42".to_string(),
            checkpoint_high_water_mark: 42,
            keys: Arc::new(vec!["a".to_string(), "b".to_string()]),
        };

        let health = persistent_key_only_index_health(
            &index,
            NamespaceMutationJournalSnapshot {
                high_water_mark: 42,
                degraded: true,
            },
        );

        assert_eq!(health.state, ListIndexLifecycleState::Degraded);
        assert_eq!(health.fallback_reason, Some(ListIndexFallbackReason::Degraded));
    }

    #[tokio::test]
    async fn list_objects_mutation_sequence_advances_per_bucket() {
        reset_list_objects_mutation_sequences_for_test().await;

        assert_eq!(current_list_objects_mutation_sequence("bucket-a").await, 0);
        assert_eq!(observe_list_objects_mutations_with_store(None, "bucket-a", 1).await, Some(1));
        assert_eq!(observe_list_objects_mutations_with_store(None, "bucket-a", 2).await, Some(3));
        assert_eq!(current_list_objects_mutation_sequence("bucket-a").await, 3);
        assert_eq!(current_list_objects_mutation_sequence("bucket-b").await, 0);
        assert_eq!(observe_list_objects_mutations_with_store(None, "bucket-b", 0).await, None);
        assert_eq!(current_list_objects_mutation_sequence("bucket-b").await, 0);
    }

    #[test]
    fn list_objects_index_provider_state_uses_lifecycle_active_generation() {
        let provider = ListObjectsIndexProviderState::from_kind(ListObjectsIndexProviderKind::WalkerKeyOnly);
        let health = provider.health_snapshot();

        assert_eq!(provider.kind, ListObjectsIndexProviderKind::WalkerKeyOnly);
        assert_eq!(health.state, ListIndexLifecycleState::Healthy);
        assert_eq!(health.staging_generation, None);
        assert_eq!(health.active_generation.as_deref(), Some(LIST_CURSOR_GENERATION_LIVE));
        assert_eq!(
            select_list_index_source_mode(Some(ListSourceMode::IndexKeyOnly), health.active_generation.as_deref(), &health),
            ListIndexSourceDecision::UseIndex(ListSourceMode::IndexKeyOnly)
        );
    }

    #[test]
    fn list_objects_index_opt_in_fallback_accepts_incompatible_cursor_without_panicking() {
        record_list_objects_index_opt_in_fallback(
            &ListPathOptions {
                bucket: "bucket".to_string(),
                prefix: "photos/".to_string(),
                marker: Some(
                    "photos/2026/[rustfs_cache:v2,id:list-cache-id,src:index_verified_page,gen:generation-1]".to_string(),
                ),
                ..Default::default()
            },
            ListSourceMode::IndexKeyOnly,
        );
    }

    #[tokio::test]
    async fn list_objects_verified_index_candidates_use_live_metadata_only() {
        let candidates = vec![
            "photos/2026/a.jpg".to_string(),
            "photos/2026/b.jpg".to_string(),
            "photos/2026/c.jpg".to_string(),
        ];

        let result =
            list_objects_from_verified_index_candidates("photos/2026/", None, &None, 100, &candidates, |key| async move {
                Ok(Some(test_live_object_info(&key, "live-etag")))
            })
            .await
            .expect("verified candidate listing should succeed");

        assert_eq!(result.objects.len(), 3);
        assert!(
            result
                .objects
                .iter()
                .all(|object| object.etag.as_deref() == Some("live-etag"))
        );
        assert!(result.prefixes.is_empty());
        assert!(!result.is_truncated);
    }

    #[tokio::test]
    async fn list_objects_verified_index_candidates_apply_marker_and_delimiter_boundaries() {
        let candidates = vec![
            "photos/2025/old.jpg".to_string(),
            "photos/2026/a.jpg".to_string(),
            "photos/2026/archive/b.jpg".to_string(),
            "photos/2026/archive/c.jpg".to_string(),
            "photos/2026/d.jpg".to_string(),
        ];

        let result = list_objects_from_verified_index_candidates(
            "photos/2026/",
            Some("photos/2026/a.jpg"),
            &Some("/".to_string()),
            100,
            &candidates,
            |key| async move { Ok(Some(test_live_object_info(&key, "live-etag"))) },
        )
        .await
        .expect("verified candidate listing should succeed");

        assert_eq!(
            result.objects.iter().map(|object| object.name.as_str()).collect::<Vec<_>>(),
            vec!["photos/2026/d.jpg"]
        );
        assert_eq!(result.prefixes, vec!["photos/2026/archive/".to_string()]);
        assert!(!result.is_truncated);
    }

    #[tokio::test]
    async fn list_objects_verified_index_candidates_use_common_prefix_as_page_marker() {
        let candidates = vec![
            "photos/2026/archive/b.jpg".to_string(),
            "photos/2026/archive/c.jpg".to_string(),
            "photos/2026/d.jpg".to_string(),
        ];

        let result = list_objects_from_verified_index_candidates(
            "photos/2026/",
            None,
            &Some("/".to_string()),
            1,
            &candidates,
            |key| async move { Ok(Some(test_live_object_info(&key, "live-etag"))) },
        )
        .await
        .expect("verified candidate listing should succeed");

        assert!(result.objects.is_empty());
        assert_eq!(result.prefixes, vec!["photos/2026/archive/".to_string()]);
        assert!(result.is_truncated);
        assert_eq!(result.next_marker.as_deref(), Some("photos/2026/archive/"));
    }

    #[tokio::test]
    async fn list_objects_verified_index_candidates_drop_deleted_or_failed_live_verifications() {
        let candidates = vec!["photos/2026/a.jpg".to_string(), "photos/2026/deleted.jpg".to_string()];

        let result =
            list_objects_from_verified_index_candidates("photos/2026/", None, &None, 100, &candidates, |key| async move {
                if key.ends_with("deleted.jpg") {
                    Ok(None)
                } else {
                    Ok(Some(test_live_object_info(&key, "live-etag")))
                }
            })
            .await
            .expect("verified candidate listing should succeed");

        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.objects[0].name, "photos/2026/a.jpg");
    }

    #[tokio::test]
    async fn list_objects_verified_index_candidates_ignore_stale_delete_markers() {
        let candidates = vec![
            "photos/2026/live.jpg".to_string(),
            "photos/2026/stale-delete-marker.jpg".to_string(),
        ];

        let result =
            list_objects_from_verified_index_candidates("photos/2026/", None, &None, 100, &candidates, |key| async move {
                if key.ends_with("stale-delete-marker.jpg") {
                    Ok(None)
                } else {
                    Ok(Some(test_live_object_info(&key, "live-etag")))
                }
            })
            .await
            .expect("verified candidate listing should succeed");

        assert_eq!(
            result.objects.iter().map(|object| object.name.as_str()).collect::<Vec<_>>(),
            vec!["photos/2026/live.jpg"]
        );
    }

    #[tokio::test]
    async fn list_objects_verified_index_candidates_return_overwritten_live_metadata() {
        let candidates = vec!["photos/2026/overwritten.jpg".to_string()];

        let result =
            list_objects_from_verified_index_candidates("photos/2026/", None, &None, 100, &candidates, |key| async move {
                Ok(Some(test_live_object_info(&key, "new-live-etag")))
            })
            .await
            .expect("verified candidate listing should succeed");

        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.objects[0].etag.as_deref(), Some("new-live-etag"));
    }

    #[tokio::test]
    async fn list_objects_verified_index_candidates_include_completed_multipart_after_live_verify() {
        let candidates = vec!["photos/2026/multipart-complete.bin".to_string()];

        let result =
            list_objects_from_verified_index_candidates("photos/2026/", None, &None, 100, &candidates, |key| async move {
                Ok(Some(test_live_object_info(&key, "completed-multipart-etag")))
            })
            .await
            .expect("verified candidate listing should succeed");

        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.objects[0].name, "photos/2026/multipart-complete.bin");
        assert_eq!(result.objects[0].etag.as_deref(), Some("completed-multipart-etag"));
    }

    #[tokio::test]
    async fn list_objects_verified_index_candidates_report_verification_stats() {
        let candidates = vec![
            "photos/2025/old.jpg".to_string(),
            "photos/2026/a.jpg".to_string(),
            "photos/2026/archive/b.jpg".to_string(),
            "photos/2026/deleted.jpg".to_string(),
        ];

        let result = list_objects_from_verified_index_candidates_with_stats(
            "photos/2026/",
            Some("photos/2026/a.jpg"),
            &Some("/".to_string()),
            100,
            &candidates,
            |key| async move {
                if key.ends_with("deleted.jpg") {
                    Ok(None)
                } else {
                    Ok(Some(test_live_object_info(&key, "live-etag")))
                }
            },
        )
        .await
        .expect("verified candidate listing should succeed");

        assert!(result.info.objects.is_empty());
        assert_eq!(result.info.prefixes, vec!["photos/2026/archive/".to_string()]);
        assert_eq!(result.stats.candidate_keys, 4);
        assert_eq!(result.stats.skipped_keys, 2);
        assert_eq!(result.stats.common_prefixes, 1);
        assert_eq!(result.stats.live_verify_attempts, 1);
        assert_eq!(result.stats.live_verify_hits, 0);
        assert_eq!(result.stats.live_verify_misses, 1);
    }

    #[tokio::test]
    async fn list_objects_verified_index_candidates_skip_stats_when_disabled() {
        let candidates = vec!["photos/2026/a.jpg".to_string(), "photos/2026/deleted.jpg".to_string()];

        let result = list_objects_from_verified_index_candidates_with_optional_stats(
            "photos/2026/",
            None,
            &None,
            100,
            &candidates,
            false,
            |key| async move {
                if key.ends_with("deleted.jpg") {
                    Ok(None)
                } else {
                    Ok(Some(test_live_object_info(&key, "live-etag")))
                }
            },
        )
        .await
        .expect("verified candidate listing should succeed");

        assert_eq!(result.info.objects.len(), 1);
        assert_eq!(result.stats, VerifiedIndexCandidateStats::default());
    }

    #[test]
    fn list_index_provider_selection_rejects_generation_mismatch() {
        let mut lifecycle = ListIndexLifecycle::disabled(10);
        lifecycle.begin_rebuild("generation-2", 100);
        lifecycle.checkpoint_rebuild(100);
        assert!(lifecycle.publish_rebuild());
        let health = lifecycle.health_snapshot();

        let opts = ListPathOptions {
            marker: Some("photos/2026/a.jpg[rustfs_cache:v2,id:list-cache-id,src:index_key_only,gen:generation-1]".to_string()),
            ..Default::default()
        };

        assert_eq!(
            select_list_index_provider_source_mode(&opts, ListSourceMode::IndexKeyOnly, &health),
            ListIndexSourceDecision::FallbackToWalker(ListIndexFallbackReason::GenerationMismatch)
        );
    }

    #[test]
    fn list_index_provider_selection_falls_back_when_degraded() {
        let mut lifecycle = ListIndexLifecycle::disabled(10);
        lifecycle.begin_rebuild("generation-1", 100);
        lifecycle.checkpoint_rebuild(100);
        assert!(lifecycle.publish_rebuild());
        lifecycle.mark_degraded();
        let health = lifecycle.health_snapshot();

        assert_eq!(
            select_list_index_provider_source_mode(&ListPathOptions::default(), ListSourceMode::IndexKeyOnly, &health),
            ListIndexSourceDecision::FallbackToWalker(ListIndexFallbackReason::Degraded)
        );
    }

    #[test]
    fn list_metadata_resolution_params_limits_plain_listing_to_latest_version() {
        let resolver = list_metadata_resolution_params("bucket".to_string(), 2, 3, false);

        assert_eq!(resolver.dir_quorum, 3);
        assert_eq!(resolver.obj_quorum, 3);
        assert_eq!(resolver.bucket, "bucket");
        assert_eq!(resolver.requested_versions, 1);
    }

    #[test]
    fn list_metadata_resolution_params_keeps_all_versions_for_version_listing() {
        let resolver = list_metadata_resolution_params("bucket".to_string(), 3, 5, true);

        assert_eq!(resolver.dir_quorum, 3);
        assert_eq!(resolver.obj_quorum, 3);
        assert_eq!(resolver.bucket, "bucket");
        assert_eq!(resolver.requested_versions, 0);
    }

    #[test]
    fn latest_listing_object_quorum_uses_write_quorum_for_strict_latest_listing() {
        let required_quorum = latest_listing_required_object_quorum(2, 8, 4, true);
        let ask_disks = expand_ask_disks_for_object_quorum(4, 8, required_quorum);

        assert_eq!(required_quorum, 5);
        assert_eq!(ask_disks, 5);
        assert_eq!(latest_listing_object_quorum(2, 8, 4, true), 5);
    }

    #[test]
    fn latest_listing_object_quorum_calculates_low_parity_write_quorum() {
        let required_quorum = latest_listing_required_object_quorum(2, 8, 1, true);
        let ask_disks = expand_ask_disks_for_object_quorum(4, 8, required_quorum);

        assert_eq!(required_quorum, 7);
        assert_eq!(ask_disks, 7);
        assert_eq!(latest_listing_object_quorum(2, 8, 1, true), 7);
    }

    #[test]
    fn latest_listing_object_quorum_keeps_reduced_and_versioned_listing_quorum() {
        assert!(!enforce_latest_listing_write_quorum(true, "reduced"));
        assert!(!enforce_latest_listing_write_quorum(true, "disk"));
        assert!(enforce_latest_listing_write_quorum(true, "optimal"));
        assert!(!enforce_latest_listing_write_quorum(false, "optimal"));
        assert_eq!(latest_listing_required_object_quorum(1, 4, 2, false), 1);
        assert_eq!(latest_listing_object_quorum(1, 4, 2, false), 1);
        assert_eq!(latest_listing_required_object_quorum(2, 4, 2, false), 2);
        assert_eq!(latest_listing_object_quorum(2, 4, 2, false), 2);
        assert_eq!(expand_ask_disks_for_object_quorum(2, 4, 2), 2);
    }

    #[test]
    fn latest_listing_object_quorum_requires_write_quorum_when_degraded_cannot_satisfy_it() {
        let required_quorum = latest_listing_required_object_quorum(2, 8, 4, true);
        let ask_disks = expand_ask_disks_for_object_quorum(4, 4, required_quorum);

        assert_eq!(required_quorum, 5);
        assert_eq!(ask_disks, 4);
        assert_eq!(latest_listing_object_quorum(2, 8, 4, true), 5);
    }

    #[tokio::test]
    async fn latest_listing_agreed_path_requires_enough_readers_for_write_quorum() {
        let entry = test_object_meta_entry("object");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();
        let allow_agreed_objects = latest_listing_allow_agreed_objects(true, 4, 5);
        let raw_min_disks = latest_listing_raw_min_disks(2, 5, true);

        assert!(!allow_agreed_objects);
        assert_eq!(raw_min_disks, 5);
        let err = list_path_raw(
            CancellationToken::new(),
            ListPathRawOptions {
                disks: vec![None, None, None, None],
                min_disks: raw_min_disks,
                test_reader_behaviors: vec![TestReaderBehavior::Entries(vec![entry.clone()]); 4],
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    let seen = seen_clone.clone();
                    Box::pin(async move {
                        if !allow_agreed_objects && !entry.is_dir() {
                            return;
                        }
                        seen.lock().expect("seen mutex poisoned").push(entry.name);
                    })
                })),
                ..Default::default()
            },
        )
        .await
        .expect_err("latest listing should fail when selected readers cannot satisfy write quorum");

        assert_eq!(err, DiskError::ErasureReadQuorum);
        assert!(seen.lock().expect("seen mutex poisoned").is_empty());

        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();
        let allow_agreed_objects = latest_listing_allow_agreed_objects(true, 5, 5);
        let raw_min_disks = latest_listing_raw_min_disks(3, 5, true);

        assert!(allow_agreed_objects);
        assert_eq!(raw_min_disks, 5);
        list_path_raw(
            CancellationToken::new(),
            ListPathRawOptions {
                disks: vec![None, None, None, None, None],
                min_disks: raw_min_disks,
                test_reader_behaviors: vec![TestReaderBehavior::Entries(vec![entry]); 5],
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    let seen = seen_clone.clone();
                    Box::pin(async move {
                        if !allow_agreed_objects && !entry.is_dir() {
                            return;
                        }
                        seen.lock().expect("seen mutex poisoned").push(entry.name);
                    })
                })),
                ..Default::default()
            },
        )
        .await
        .expect("write-quorum all-agree listing should complete");

        assert_eq!(seen.lock().expect("seen mutex poisoned").as_slice(), &["object".to_string()]);
    }

    #[tokio::test]
    async fn latest_listing_agreed_path_reconciles_low_parity_partial_latest() {
        let old_mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let new_mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("valid timestamp");
        let entry = test_object_meta_entry_with_erasure_versions(
            "object",
            &[(old_mod_time, "old-etag", 4, 4), (new_mod_time, "new-etag", 7, 1)],
        );
        let fallback_old_entry = test_object_meta_entry_with_erasure_versions("object", &[(old_mod_time, "old-etag", 4, 4)]);
        let resolver = list_metadata_resolution_params("bucket".to_string(), 3, 5, false);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();

        list_path_raw(
            CancellationToken::new(),
            ListPathRawOptions {
                disks: vec![None, None, None, None, None],
                min_disks: 5,
                test_reader_behaviors: vec![TestReaderBehavior::Entries(vec![entry]); 5],
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    let seen = seen_clone.clone();
                    let resolver = resolver.clone();
                    let fallback_old_entry = fallback_old_entry.clone();
                    Box::pin(async move {
                        let entry = match resolve_agreed_listing_entry(entry, 5, resolver.clone(), true) {
                            ListingEntryResolution::Resolved(entry) => entry,
                            ListingEntryResolution::NeedsSupplement(entry, _) => {
                                let mut candidates = vec![Some(entry); 5];
                                candidates.extend(vec![Some(fallback_old_entry); 2]);
                                let Some(entry) = resolve_listing_entries(MetaCacheEntries(candidates), resolver, true) else {
                                    return;
                                };
                                entry
                            }
                            ListingEntryResolution::Rejected => return,
                        };
                        let info = entry.to_fileinfo("bucket").expect("resolved entry should decode");
                        seen.lock().expect("seen mutex poisoned").push((
                            entry.name,
                            info.mod_time,
                            info.metadata.get("etag").cloned(),
                        ));
                    })
                })),
                ..Default::default()
            },
        )
        .await
        .expect("all-agree listing should complete");

        assert_eq!(
            seen.lock().expect("seen mutex poisoned").as_slice(),
            &[("object".to_string(), Some(old_mod_time), Some("old-etag".to_string()))]
        );
    }

    #[tokio::test]
    async fn latest_listing_agreed_path_falls_back_to_previous_version_without_supplement() {
        let old_mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let new_mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("valid timestamp");
        let entry = test_object_meta_entry_with_erasure_versions(
            "object",
            &[(old_mod_time, "old-etag", 4, 4), (new_mod_time, "new-etag", 7, 1)],
        );
        let resolver = list_metadata_resolution_params("bucket".to_string(), 3, 5, false);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();

        list_path_raw(
            CancellationToken::new(),
            ListPathRawOptions {
                disks: vec![None, None, None, None, None],
                min_disks: 5,
                test_reader_behaviors: vec![TestReaderBehavior::Entries(vec![entry]); 5],
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    let seen = seen_clone.clone();
                    let resolver = resolver.clone();
                    Box::pin(async move {
                        let entry = match resolve_agreed_listing_entry(entry, 5, resolver, true) {
                            ListingEntryResolution::Resolved(entry) => entry,
                            ListingEntryResolution::NeedsSupplement(_, Some(entry)) => entry,
                            ListingEntryResolution::NeedsSupplement(_, None) | ListingEntryResolution::Rejected => return,
                        };
                        let info = entry.to_fileinfo("bucket").expect("resolved entry should decode");
                        seen.lock().expect("seen mutex poisoned").push((
                            entry.name,
                            info.mod_time,
                            info.metadata.get("etag").cloned(),
                        ));
                    })
                })),
                ..Default::default()
            },
        )
        .await
        .expect("all-agree listing should complete");

        assert_eq!(
            seen.lock().expect("seen mutex poisoned").as_slice(),
            &[("object".to_string(), Some(old_mod_time), Some("old-etag".to_string()))]
        );
    }

    #[tokio::test]
    async fn latest_listing_agreed_path_supplements_committed_low_parity_latest() {
        let new_mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("valid timestamp");
        let entry = test_object_meta_entry_with_erasure_versions("object", &[(new_mod_time, "new-etag", 7, 1)]);
        let fallback_entry = entry.clone();
        let resolver = list_metadata_resolution_params("bucket".to_string(), 3, 5, false);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();

        list_path_raw(
            CancellationToken::new(),
            ListPathRawOptions {
                disks: vec![None, None, None, None, None],
                min_disks: 5,
                test_reader_behaviors: vec![TestReaderBehavior::Entries(vec![entry]); 5],
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    let seen = seen_clone.clone();
                    let resolver = resolver.clone();
                    let fallback_entry = fallback_entry.clone();
                    Box::pin(async move {
                        let ListingEntryResolution::NeedsSupplement(entry, _) =
                            resolve_agreed_listing_entry(entry, 5, resolver.clone(), true)
                        else {
                            return;
                        };
                        let mut candidates = vec![Some(entry); 5];
                        candidates.extend(vec![Some(fallback_entry); 2]);
                        let resolved = resolve_listing_entries(MetaCacheEntries(candidates), resolver, true)
                            .expect("supplemented low-parity latest should satisfy object write quorum");
                        let info = resolved.to_fileinfo("bucket").expect("resolved entry should decode");
                        seen.lock().expect("seen mutex poisoned").push((
                            resolved.name,
                            info.mod_time,
                            info.metadata.get("etag").cloned(),
                        ));
                    })
                })),
                ..Default::default()
            },
        )
        .await
        .expect("all-agree listing should complete");

        assert_eq!(
            seen.lock().expect("seen mutex poisoned").as_slice(),
            &[("object".to_string(), Some(new_mod_time), Some("new-etag".to_string()))]
        );
    }

    #[tokio::test]
    async fn latest_listing_agreed_directory_requires_write_quorum() {
        let entry = test_dir_meta_entry("prefix/");
        let raw_min_disks = latest_listing_raw_min_disks(2, 5, true);

        let err = list_path_raw(
            CancellationToken::new(),
            ListPathRawOptions {
                disks: vec![None, None, None, None],
                min_disks: raw_min_disks,
                test_reader_behaviors: vec![TestReaderBehavior::Entries(vec![entry]); 4],
                agreed: Some(Box::new(move |_| Box::pin(async {}))),
                ..Default::default()
            },
        )
        .await
        .expect_err("latest listing should not expose prefixes below write quorum");

        assert_eq!(err, DiskError::ErasureReadQuorum);
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
        let mut opts = ListPathOptions {
            id: Some("list-cache-id".to_string()),
            pool_idx: Some(3),
            set_idx: Some(7),
            ..Default::default()
        };

        let marker = opts.encode_marker("photos/2026/image.jpg");
        let expected_marker = format!(
            "photos/2026/image.jpg[rustfs_cache:{},id:list-cache-id,p:3,s:7,src:walker,gen:live]",
            super::MARKER_TAG_VERSION
        );
        assert_eq!(marker, expected_marker);

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
    fn list_source_modes_expose_metadata_authority_boundaries() {
        assert_eq!(ListSourceMode::Walker.metadata_authority(), ListMetadataAuthority::LiveXlMeta);
        assert_eq!(
            ListSourceMode::IndexKeyOnly.metadata_authority(),
            ListMetadataAuthority::LiveVerifiedIndexCandidate
        );
        assert_eq!(
            ListSourceMode::IndexVerifiedPage.metadata_authority(),
            ListMetadataAuthority::LiveVerifiedIndexCandidate
        );
        assert_eq!(
            ListSourceMode::IndexMetadataFast.metadata_authority(),
            ListMetadataAuthority::IndexSnapshotEventuallyConsistent
        );

        assert!(ListSourceMode::IndexKeyOnly.requires_live_metadata_verification());
        assert!(ListSourceMode::IndexVerifiedPage.requires_live_metadata_verification());
        assert!(!ListSourceMode::IndexMetadataFast.requires_live_metadata_verification());
        assert!(!ListSourceMode::IndexMetadataFast.can_satisfy_strong_listing());
    }

    #[test]
    fn list_index_source_selection_falls_back_until_request_is_healthy_and_versioned() {
        let healthy = TestIndexHealth { reason: None };
        let unhealthy = TestIndexHealth {
            reason: Some(ListIndexFallbackReason::Unhealthy),
        };

        assert_eq!(
            select_list_index_source_mode(None, Some("generation-1"), &healthy),
            ListIndexSourceDecision::FallbackToWalker(ListIndexFallbackReason::Disabled)
        );
        assert_eq!(
            select_list_index_source_mode(Some(ListSourceMode::Walker), Some("generation-1"), &healthy),
            ListIndexSourceDecision::FallbackToWalker(ListIndexFallbackReason::Disabled)
        );
        assert_eq!(
            select_list_index_source_mode(Some(ListSourceMode::IndexKeyOnly), None, &healthy),
            ListIndexSourceDecision::FallbackToWalker(ListIndexFallbackReason::MissingGeneration)
        );
        assert_eq!(
            select_list_index_source_mode(Some(ListSourceMode::IndexKeyOnly), Some(""), &healthy),
            ListIndexSourceDecision::FallbackToWalker(ListIndexFallbackReason::MissingGeneration)
        );
        assert_eq!(
            select_list_index_source_mode(Some(ListSourceMode::IndexKeyOnly), Some("generation-1"), &unhealthy),
            ListIndexSourceDecision::FallbackToWalker(ListIndexFallbackReason::Unhealthy)
        );
        assert_eq!(
            select_list_index_source_mode(Some(ListSourceMode::IndexVerifiedPage), Some("generation-1"), &healthy),
            ListIndexSourceDecision::UseIndex(ListSourceMode::IndexVerifiedPage)
        );
    }

    #[test]
    fn list_index_traits_keep_page_iteration_and_metadata_authority_explicit() {
        let page = TestIndexPage {
            mode: ListSourceMode::IndexMetadataFast,
            generation: TestIndexGeneration {
                id: "snapshot-7".to_string(),
            },
            keys: vec!["photos/2026/image.jpg".to_string()],
        };
        let mut iterator = TestIndexIterator {
            pages: vec![page].into_iter(),
        };
        let page = iterator.next_page().expect("test iterator should produce one page");

        assert_eq!(page.keys(), &["photos/2026/image.jpg".to_string()]);
        assert_eq!(page.generation().generation_id(), "snapshot-7");
        assert_eq!(page.metadata_authority(), ListMetadataAuthority::IndexSnapshotEventuallyConsistent);
        assert!(!page.source_mode().can_satisfy_strong_listing());
    }

    #[test]
    fn list_index_key_lookup_trait_returns_explicit_fallback_decision() {
        let lookup = TestIndexLookup {
            decision: ListIndexSourceDecision::FallbackToWalker(ListIndexFallbackReason::UnsupportedRequest),
        };

        assert_eq!(
            lookup.lookup_page(&ListPathOptions::default()),
            ListIndexSourceDecision::FallbackToWalker(ListIndexFallbackReason::UnsupportedRequest)
        );
    }

    #[test]
    fn list_index_lifecycle_rebuild_requires_publish_before_health() {
        let mut lifecycle = ListIndexLifecycle::disabled(10);

        lifecycle.begin_rebuild("generation-1", 100);
        lifecycle.checkpoint_rebuild(80);

        let snapshot = lifecycle.health_snapshot();
        assert_eq!(snapshot.state, ListIndexLifecycleState::Rebuilding);
        assert_eq!(snapshot.active_generation, None);
        assert_eq!(snapshot.staging_generation.as_deref(), Some("generation-1"));
        assert_eq!(snapshot.checkpoint_high_water_mark, 80);
        assert_eq!(snapshot.fallback_reason, Some(ListIndexFallbackReason::Rebuilding));

        assert!(lifecycle.publish_rebuild());
        let snapshot = lifecycle.health_snapshot();
        assert_eq!(snapshot.state, ListIndexLifecycleState::Healthy);
        assert_eq!(snapshot.active_generation.as_deref(), Some("generation-1"));
        assert_eq!(snapshot.staging_generation, None);
        assert_eq!(snapshot.fallback_reason, None);
    }

    #[test]
    fn list_index_lifecycle_restart_never_trusts_unpublished_staging_generation() {
        let mut lifecycle = ListIndexLifecycle::disabled(10);
        lifecycle.begin_rebuild("generation-1", 100);
        lifecycle.checkpoint_rebuild(90);

        let recovered = lifecycle.recover_after_restart();
        let snapshot = recovered.health_snapshot();

        assert_eq!(snapshot.state, ListIndexLifecycleState::Disabled);
        assert_eq!(snapshot.active_generation, None);
        assert_eq!(snapshot.staging_generation, None);
        assert_eq!(snapshot.fallback_reason, Some(ListIndexFallbackReason::Disabled));
    }

    #[test]
    fn list_index_lifecycle_restart_keeps_only_last_published_generation() {
        let mut lifecycle = ListIndexLifecycle::disabled(10);
        lifecycle.begin_rebuild("generation-1", 100);
        lifecycle.checkpoint_rebuild(100);
        assert!(lifecycle.publish_rebuild());

        lifecycle.begin_rebuild("generation-2", 125);
        lifecycle.checkpoint_rebuild(120);

        let recovered = lifecycle.recover_after_restart();
        let snapshot = recovered.health_snapshot();

        assert_eq!(snapshot.state, ListIndexLifecycleState::Healthy);
        assert_eq!(snapshot.active_generation.as_deref(), Some("generation-1"));
        assert_eq!(snapshot.staging_generation, None);
        assert_eq!(snapshot.fallback_reason, None);
    }

    #[test]
    fn list_index_lifecycle_reports_lagging_and_fallback_reason() {
        let mut lifecycle = ListIndexLifecycle::disabled(10);
        lifecycle.begin_rebuild("generation-1", 100);
        lifecycle.checkpoint_rebuild(100);
        assert!(lifecycle.publish_rebuild());

        lifecycle.observe_mutation_high_water_mark(115);
        let snapshot = lifecycle.health_snapshot();

        assert_eq!(snapshot.state, ListIndexLifecycleState::Lagging);
        assert_eq!(snapshot.mutation_lag, 15);
        assert_eq!(snapshot.fallback_reason, Some(ListIndexFallbackReason::Lagging));
        assert!(!snapshot.is_healthy());
    }

    #[test]
    fn list_index_lifecycle_reports_degraded_and_corrupt_as_unhealthy() {
        let mut lifecycle = ListIndexLifecycle::disabled(10);
        lifecycle.begin_rebuild("generation-1", 100);
        lifecycle.checkpoint_rebuild(100);
        assert!(lifecycle.publish_rebuild());

        lifecycle.mark_degraded();
        let degraded = lifecycle.health_snapshot();
        assert_eq!(degraded.state, ListIndexLifecycleState::Degraded);
        assert_eq!(degraded.fallback_reason, Some(ListIndexFallbackReason::Degraded));
        assert!(!degraded.is_healthy());

        lifecycle.mark_corrupt();
        let corrupt = lifecycle.health_snapshot();
        assert_eq!(corrupt.state, ListIndexLifecycleState::Corrupt);
        assert_eq!(corrupt.fallback_reason, Some(ListIndexFallbackReason::Corrupt));
        assert!(!corrupt.is_healthy());
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
    fn walk_result_from_set_errors_returns_non_eof_error() {
        let err = walk_result_from_set_errors(&[Some(StorageError::Unexpected), Some(StorageError::FileAccessDenied)])
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

    #[test]
    fn walk_result_from_set_errors_ignores_only_unexpected_and_successes() {
        walk_result_from_set_errors(&[None, Some(StorageError::Unexpected)])
            .expect("successful sets and unexpected EOF-style markers should not fail the walk");
    }

    // use std::sync::Arc;

    // use crate::cache_value::metacache_set::list_path_raw;
    // use crate::cache_value::metacache_set::ListPathRawOptions;
    // use crate::disk::endpoint::Endpoint;
    // use crate::disk::error::is_err_eof;
    // use crate::disk::format::FormatV3;
    // use crate::disk::new_disk;
    // use crate::disk::DiskAPI;
    // use crate::disk::DiskOption;
    // use crate::disk::MetaCacheEntries;
    // use crate::disk::MetaCacheEntry;
    // use crate::disk::WalkDirOptions;
    // use crate::layout::endpoints::EndpointServerPools;
    // use crate::error::Error;
    // use crate::metacache::writer::MetacacheReader;
    // use crate::set_disk::SetDisks;
    // use crate::store::list_objects::ListPathOptions;
    // use crate::store::list_objects::WalkOptions;
    // use crate::store::list_objects::WalkVersionsSortOrder;
    // use futures::future::join_all;
    // use rustfs_lock::namespace_lock::NsLockMap;
    // use tokio::sync::broadcast;
    // use tokio::sync::mpsc;
    // use tokio::sync::RwLock;
    // use uuid::Uuid;

    // #[tokio::test]
    // async fn test_walk_dir() {
    //     let mut ep = Endpoint::try_from("/Users/weisd/project/weisd/s3-rustfs/target/volume/test").unwrap();
    //     ep.pool_idx = 0;
    //     ep.set_idx = 0;
    //     ep.disk_idx = 0;
    //     ep.is_local = true;

    //     let disk = new_disk(&ep, &DiskOption::default()).await.expect("init disk fail");

    //     // let disk = match LocalDisk::new(&ep, false).await {
    //     //     Ok(res) => res,
    //     //     Err(err) => {
    //     //         println!("LocalDisk::new err {:?}", err);
    //     //         return;
    //     //     }
    //     // };

    //     let (rd, mut wr) = tokio::io::duplex(64);

    //     let job = tokio::spawn(async move {
    //         let opts = WalkDirOptions {
    //             bucket: "dada".to_owned(),
    //             base_dir: "".to_owned(),
    //             recursive: true,
    //             ..Default::default()
    //         };

    //         println!("walk opts {:?}", opts);
    //         if let Err(err) = disk.walk_dir(opts, &mut wr).await {
    //             println!("walk_dir err {:?}", err);
    //         }
    //     });

    //     let job2 = tokio::spawn(async move {
    //         let mut mrd = MetacacheReader::new(rd);

    //         loop {
    //             match mrd.peek().await {
    //                 Ok(res) => {
    //                     if let Some(info) = res {
    //                         println!("info {:?}", info.name)
    //                     } else {
    //                         break;
    //                     }
    //                 }
    //                 Err(err) => {
    //                     if is_err_eof(&err) {
    //                         break;
    //                     }

    //                     println!("get err {:?}", err);
    //                     break;
    //                 }
    //             }
    //         }
    //     });
    //     join_all(vec![job, job2]).await;
    // }

    // #[tokio::test]
    // async fn test_list_path_raw() {
    //     let mut ep = Endpoint::try_from("/Users/weisd/project/weisd/s3-rustfs/target/volume/test").unwrap();
    //     ep.pool_idx = 0;
    //     ep.set_idx = 0;
    //     ep.disk_idx = 0;
    //     ep.is_local = true;

    //     let disk = new_disk(&ep, &DiskOption::default()).await.expect("init disk fail");

    //     // let disk = match LocalDisk::new(&ep, false).await {
    //     //     Ok(res) => res,
    //     //     Err(err) => {
    //     //         println!("LocalDisk::new err {:?}", err);
    //     //         return;
    //     //     }
    //     // };

    //     let (_, rx) = broadcast::channel(1);
    //     let bucket = "dada".to_owned();
    //     let forward_to = None;
    //     let disks = vec![Some(disk)];
    //     let fallback_disks = Vec::new();

    //     list_path_raw(
    //         rx,
    //         ListPathRawOptions {
    //             disks,
    //             fallback_disks,
    //             bucket,
    //             path: "".to_owned(),
    //             recursice: true,
    //             forward_to,
    //             min_disks: 1,
    //             report_not_found: false,
    //             agreed: Some(Box::new(move |entry: MetaCacheEntry| {
    //                 Box::pin(async move { println!("get entry: {}", entry.name) })
    //             })),
    //             partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<Error>]| {
    //                 Box::pin(async move { println!("get entries: {:?}", entries) })
    //             })),
    //             finished: None,
    //             ..Default::default()
    //         },
    //     )
    //     .await
    //     .unwrap();
    // }

    // #[tokio::test]
    // async fn test_set_list_path() {
    //     let mut ep = Endpoint::try_from("/Users/weisd/project/weisd/s3-rustfs/target/volume/test").unwrap();
    //     ep.pool_idx = 0;
    //     ep.set_idx = 0;
    //     ep.disk_idx = 0;
    //     ep.is_local = true;

    //     let disk = new_disk(&ep, &DiskOption::default()).await.expect("init disk fail");
    //     let _ = disk.set_disk_id(Some(Uuid::new_v4())).await;

    //     let set = SetDisks {
    //         lockers: Vec::new(),
    //         locker_owner: String::new(),
    //         ns_mutex: Arc::new(RwLock::new(NsLockMap::new(false))),
    //         disks: RwLock::new(vec![Some(disk)]),
    //         set_endpoints: Vec::new(),
    //         set_drive_count: 1,
    //         default_parity_count: 0,
    //         set_index: 0,
    //         pool_index: 0,
    //         format: FormatV3::new(1, 1),
    //     };

    //     let (_tx, rx) = broadcast::channel(1);

    //     let bucket = "dada".to_owned();

    //     let opts = ListPathOptions {
    //         bucket,
    //         recursive: true,
    //         ..Default::default()
    //     };

    //     let (sender, mut recv) = mpsc::channel(10);

    //     set.list_path(rx, opts, sender).await.unwrap();

    //     while let Some(entry) = recv.recv().await {
    //         println!("get entry {:?}", entry.name)
    //     }
    // }

    // #[tokio::test]
    //walk() {
    //     let server_address = "localhost:9000";

    //     let (endpoint_pools, _setup_type) = EndpointServerPools::from_volumes(
    //         server_address,
    //         vec!["/Users/weisd/project/weisd/s3-rustfs/target/volume/test".to_string()],
    //     )
    //     .unwrap();

    //     let store = ECStore::new(server_address.to_string(), endpoint_pools.clone())
    //         .await
    //         .unwrap();

    //     let (_tx, rx) = broadcast::channel(1);

    //     let bucket = "dada".to_owned();
    //     let opts = ListPathOptions {
    //         bucket,
    //         recursive: true,
    //         ..Default::default()
    //     };

    //     let (sender, mut recv) = mpsc::channel(10);

    //     store.list_merged(rx, opts, sender).await.unwrap();

    //     while let Some(entry) = recv.recv().await {
    //         println!("get entry {:?}", entry.name)
    //     }
    // }

    // #[tokio::test]
    // async fn test_list_path() {
    //     let server_address = "localhost:9000";

    //     let (endpoint_pools, _setup_type) = EndpointServerPools::from_volumes(
    //         server_address,
    //         vec!["/Users/weisd/project/weisd/s3-rustfs/target/volume/test".to_string()],
    //     )
    //     .unwrap();

    //     let store = ECStore::new(server_address.to_string(), endpoint_pools.clone())
    //         .await
    //         .unwrap();

    //     let bucket = "dada".to_owned();
    //     let opts = ListPathOptions {
    //         bucket,
    //         recursive: true,
    //         limit: 100,

    //         ..Default::default()
    //     };

    //     let ret = store.list_path(&opts).await.unwrap();
    //     println!("ret {:?}", ret);
    // }

    // #[tokio::test]
    // async fn test_list_objects_v2() {
    //     let server_address = "localhost:9000";

    //     let (endpoint_pools, _setup_type) = EndpointServerPools::from_volumes(
    //         server_address,
    //         vec!["/Users/weisd/project/weisd/s3-rustfs/target/volume/test".to_string()],
    //     )
    //     .unwrap();

    //     let store = ECStore::new(server_address.to_string(), endpoint_pools.clone())
    //         .await
    //         .unwrap();

    //     let ret = store.list_objects_v2("data", "", "", "", 100, false, "").await.unwrap();
    //     println!("ret {:?}", ret);
    // }

    // #[tokio::test]
    // async fn test_walk() {
    //     let server_address = "localhost:9000";

    //     let (endpoint_pools, _setup_type) = EndpointServerPools::from_volumes(
    //         server_address,
    //         vec!["/Users/weisd/project/weisd/s3-rustfs/target/volume/test".to_string()],
    //     )
    //     .unwrap();

    //     let store = ECStore::new(server_address.to_string(), endpoint_pools.clone())
    //         .await
    //         .unwrap();

    //     ECStore::init(store.clone()).await.unwrap();

    //     let (_tx, rx) = broadcast::channel(1);

    //     let bucket = ".rustfs.sys";
    //     let prefix = "config/iam/sts/";

    //     let (sender, mut recv) = mpsc::channel(10);

    //     let opts = WalkOptions::default();

    //     store.walk(rx, bucket, prefix, sender, opts).await.unwrap();

    //     while let Some(entry) = recv.recv().await {
    //         println!("get entry {:?}", entry)
    //     }
    // }

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
    async fn merge_entry_channels_documents_candidate_metadata_authority_risk() {
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
            !merged.is_latest_delete_marker(),
            "current merge consumes candidate metadata bytes; future index-backed strong modes must live-verify metadata instead"
        );
        assert!(
            matches!(timeout(Duration::from_secs(1), out_rx.recv()).await, Ok(None)),
            "merge should not emit a duplicate entry for the same key"
        );

        handle
            .await
            .expect("merge task should not panic")
            .expect("merge task should succeed");
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
