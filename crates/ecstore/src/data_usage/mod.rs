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

// #730: scanner/data-usage state is partially migrated and still owns staged cache helpers.

pub mod local_snapshot;

use crate::storage_api_contracts::{
    bucket::{BucketOperations as _, BucketOptions},
    list::{ListOperations as _, StorageListObjectVersionsInfo},
    object::{EcstoreObjectIO, HTTPPreconditions, ObjectOperations as _},
};
use crate::{
    bucket::{metadata_sys::get_replication_config, versioning::VersioningApi as _, versioning_sys::BucketVersioningSys},
    config::com::{read_config, read_config_preserve_empty},
    disk::{DiskAPI, RUSTFS_META_BUCKET},
    error::{Error, classify_system_path_failure_reason},
    object_api::{ObjectInfo, ObjectOptions, PutObjReader},
    runtime::sources as runtime_sources,
    store::{ECStore, list_objects::list_marker_key},
};
pub use local_snapshot::{LocalUsageSnapshot, read_snapshot as read_local_snapshot, snapshot_path};
use rustfs_data_usage::{
    BucketTargetUsageInfo, BucketUsageInfo, CompressionTotalInfo, DATA_USAGE_OBJECT_NAME, DATA_USAGE_OBSERVED_OBJECT_NAME,
    DataUsageCache, DataUsageInfo, DiskUsageStatus, LEGACY_DATA_USAGE_OBJECT_NAME, SizeHistogram, VersionsHistogram,
    observed_data_usage_is_newer,
};
use rustfs_io_metrics::record_system_path_failure;
use rustfs_utils::path::SLASH_SEPARATOR;
use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    future::Future,
    sync::{
        Arc, LazyLock, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::fs;
use tokio::sync::{Mutex as TokioMutex, RwLock};
use tracing::{debug, error, info, instrument};

// Data usage storage constants
pub const DATA_USAGE_ROOT: &str = SLASH_SEPARATOR;
const DATA_COMPRESSION_TOTAL_NAME: &str = ".compression.json";
pub const DATA_USAGE_CACHE_NAME: &str = ".usage-cache.bin";
const DATA_USAGE_CACHE_TTL_SECS: u64 = 30;
const LIVE_BUCKET_USAGE_MAX_ENTRIES: u64 = 1024;
const DATA_USAGE_REMOVE_CAS_RETRIES: usize = 3;

#[derive(Debug, Clone)]
struct CachedBucketUsage {
    usage: BucketUsageInfo,
    authoritative: bool,
    refreshed_at: SystemTime,
    usage_updated_at: SystemTime,
    // Set by request-path mutations until a scanner snapshot catches up to the same core counts.
    dirty: bool,
    // Set when a newer scanner snapshot was observed but did not include the dirty counts yet.
    stale_snapshot_pending: bool,
    // First complete scanner generation observed after an unknown-baseline
    // mutation. A strictly later generation is required before the mutation
    // evidence can be discarded.
    pending_scanner_position: Option<(u64, u64)>,
    // Deletes are visible to admin immediately, but quota admission keeps
    // them pending until a complete scanner generation reconciles the set.
    // This marker intentionally remains process-local: the delete request
    // updates this overlay before the scanner writes a durable snapshot. If
    // the process restarts first, loading the persisted complete snapshot
    // restores the pre-reconciliation (larger) baseline, which is
    // conservative for quota admission. A persisted post-delete snapshot is
    // necessarily a complete scanner reconciliation and therefore creates a
    // fresh cache entry with no pending hold.
    pending_negative_delta: u64,
}

type UsageMemoryCache = Arc<RwLock<HashMap<String, CachedBucketUsage>>>;
type CacheUpdating = Arc<RwLock<bool>>;
type LiveBucketUsageCache = moka::future::Cache<String, BucketUsageInfo>;

static USAGE_MEMORY_CACHE: OnceLock<UsageMemoryCache> = OnceLock::new();
static USAGE_CACHE_UPDATING: OnceLock<CacheUpdating> = OnceLock::new();
static LIVE_BUCKET_USAGE_CACHE: OnceLock<LiveBucketUsageCache> = OnceLock::new();
static USAGE_MEMORY_GENERATION: AtomicU64 = AtomicU64::new(0);

/// Best-available persisted usage for `bucket` when no authoritative source
/// exists yet (issue #5716): after an upgrade from a pre-v2 release the only
/// persisted usage data is the legacy `.usage.json`, which is demoted to
/// non-authoritative, and the authoritative caches stay empty until the
/// scanner's first complete cycle lands. Quota admission degrades to the
/// pre-discard per-bucket sizes retained on the cached snapshot instead of
/// failing every write closed.
///
/// The baseline is static between snapshot loads — live writes do not advance
/// it — so hard-quota enforcement during the degraded window is advisory: the
/// overrun is bounded only by the writes issued before the next complete
/// scanner cycle replaces the baseline with authoritative usage. That is
/// strictly tighter than beta.11 (usage treated as 0) and strictly more
/// available than a blanket 503. The fallback applies to any window without
/// authoritative usage, not only pre-v2 upgrades; the values always come from
/// the last persisted scanner output. Loads go through the TTL-bounded
/// snapshot cache, so the quota path adds at most one backend read per
/// [`DATA_USAGE_CACHE_TTL_SECS`] window. Returns `None` for buckets absent
/// from every persisted snapshot — those still fail closed.
pub async fn lookup_degraded_bucket_usage_baseline(store: Arc<ECStore>, bucket: &str) -> Option<u64> {
    let ttl = Duration::from_secs(DATA_USAGE_CACHE_TTL_SECS);
    {
        let cache = data_usage_snapshot_cache().read().await;
        if let Some(cached) = cache
            .as_ref()
            .filter(|cached| tokio::time::Instant::now().duration_since(cached.loaded_at) < ttl)
        {
            return cached.degraded_baseline.get(bucket).copied();
        }
    }

    // Stale or empty cache: refresh through the TTL-bounded loader. A failed
    // refresh carries the previous baseline forward, so quota admission keeps
    // its last grounded values through a backend read outage.
    let _ = load_data_usage_from_backend_cached(store).await;
    let cache = data_usage_snapshot_cache().read().await;
    cache
        .as_ref()
        .and_then(|cached| cached.degraded_baseline.get(bucket).copied())
}

/// Cached copy of the last persisted data usage snapshot, served to admin
/// endpoints for up to `DATA_USAGE_CACHE_TTL_SECS` between backend reads.
#[derive(Debug, Clone)]
struct CachedDataUsageSnapshot {
    info: Option<DataUsageInfo>,
    loaded_at: tokio::time::Instant,
    /// Pre-discard per-bucket sizes from the same load, retained even when the
    /// snapshot is incomplete and its bucket data is discarded. Consumed only
    /// by [`lookup_degraded_bucket_usage_baseline`] for quota admission.
    degraded_baseline: HashMap<String, u64>,
}

impl CachedDataUsageSnapshot {
    fn result(&self) -> Result<DataUsageInfo, Error> {
        self.info
            .clone()
            .ok_or_else(|| Error::other("data usage snapshot load recently failed"))
    }
}

fn fresh_cached_data_usage_snapshot(
    cache: &Option<CachedDataUsageSnapshot>,
    now: tokio::time::Instant,
    ttl: Duration,
) -> Option<Result<DataUsageInfo, Error>> {
    cache
        .as_ref()
        .filter(|cached| now.duration_since(cached.loaded_at) < ttl)
        .map(CachedDataUsageSnapshot::result)
}

fn cache_data_usage_snapshot_result(
    cache: &mut Option<CachedDataUsageSnapshot>,
    result: Result<(DataUsageInfo, HashMap<String, u64>), Error>,
    loaded_at: tokio::time::Instant,
    refresh_generation: u64,
    current_generation: u64,
) -> Option<Result<DataUsageInfo, Error>> {
    if current_generation != refresh_generation {
        return None;
    }

    Some(match result {
        Ok((info, degraded_baseline)) => {
            *cache = Some(CachedDataUsageSnapshot {
                info: Some(info.clone()),
                loaded_at,
                degraded_baseline,
            });
            Ok(info)
        }
        Err(e) => {
            // Keep the previous baseline through a failed refresh: quota
            // admission must not lose its last grounded values because one
            // backend read errored.
            let degraded_baseline = cache.take().map(|cached| cached.degraded_baseline).unwrap_or_default();
            *cache = Some(CachedDataUsageSnapshot {
                info: None,
                loaded_at,
                degraded_baseline,
            });
            Err(e)
        }
    })
}

type DataUsageSnapshotCache = Arc<RwLock<Option<CachedDataUsageSnapshot>>>;

static DATA_USAGE_SNAPSHOT_CACHE: OnceLock<DataUsageSnapshotCache> = OnceLock::new();
static DATA_USAGE_SNAPSHOT_REFRESH: OnceLock<Arc<TokioMutex<()>>> = OnceLock::new();
static DATA_USAGE_SNAPSHOT_GENERATION: AtomicU64 = AtomicU64::new(0);
static ADMIN_DATA_USAGE_SNAPSHOT_CACHE: OnceLock<DataUsageSnapshotCache> = OnceLock::new();
static ADMIN_DATA_USAGE_SNAPSHOT_REFRESH: OnceLock<Arc<TokioMutex<()>>> = OnceLock::new();
static ADMIN_DATA_USAGE_SNAPSHOT_GENERATION: AtomicU64 = AtomicU64::new(0);

// Always-on revert detector for rustfs/backlog#1306: one relaxed increment per
// full-bucket version listing is negligible and lets tests prove that admin
// request paths never trigger live listings.
static LIVE_BUCKET_USAGE_COMPUTATIONS: AtomicU64 = AtomicU64::new(0);

/// Number of live full-bucket usage computations performed by this process.
pub fn live_bucket_usage_computations() -> u64 {
    LIVE_BUCKET_USAGE_COMPUTATIONS.load(Ordering::Relaxed)
}

/// Deferred persist thresholds for compression totals: persist after this many
/// operations recorded, but no more often than the min interval.
const COMPRESSION_PERSIST_BATCH_SIZE: u64 = 100;
const COMPRESSION_PERSIST_MIN_INTERVAL: Duration = Duration::from_secs(30);

/// In-memory compression accumulator with debounced persistence state.
#[derive(Debug, Clone)]
struct CompressionTotalState {
    info: CompressionTotalInfo,
    /// Operations recorded since the last persist attempt.
    ops_since_persist: u64,
    /// When the last persist was attempted (used for min-interval gating).
    last_persist: tokio::time::Instant,
    /// When `true`, recording is skipped (embedded mode without observability).
    inited: bool,
}

impl Default for CompressionTotalState {
    fn default() -> Self {
        Self {
            info: CompressionTotalInfo::default(),
            ops_since_persist: 0,
            last_persist: tokio::time::Instant::now(),
            inited: false,
        }
    }
}

static COMPRESSION_TOTAL_MEMORY_CACHE: LazyLock<Arc<RwLock<Option<CompressionTotalState>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(Some(CompressionTotalState::default()))));

fn memory_cache() -> &'static UsageMemoryCache {
    USAGE_MEMORY_CACHE.get_or_init(|| Arc::new(RwLock::new(HashMap::new())))
}

fn cache_updating() -> &'static CacheUpdating {
    USAGE_CACHE_UPDATING.get_or_init(|| Arc::new(RwLock::new(false)))
}

fn data_usage_snapshot_cache() -> &'static DataUsageSnapshotCache {
    DATA_USAGE_SNAPSHOT_CACHE.get_or_init(|| Arc::new(RwLock::new(None)))
}

fn data_usage_snapshot_generation() -> u64 {
    DATA_USAGE_SNAPSHOT_GENERATION.load(Ordering::Acquire)
}

fn admin_data_usage_snapshot_cache() -> &'static DataUsageSnapshotCache {
    ADMIN_DATA_USAGE_SNAPSHOT_CACHE.get_or_init(|| Arc::new(RwLock::new(None)))
}

fn admin_data_usage_snapshot_generation() -> u64 {
    ADMIN_DATA_USAGE_SNAPSHOT_GENERATION.load(Ordering::Acquire)
}

fn clear_data_usage_snapshot_cache(cache: &mut Option<CachedDataUsageSnapshot>) {
    DATA_USAGE_SNAPSHOT_GENERATION.fetch_add(1, Ordering::AcqRel);
    *cache = None;
}

fn clear_admin_data_usage_snapshot_cache(cache: &mut Option<CachedDataUsageSnapshot>) {
    ADMIN_DATA_USAGE_SNAPSHOT_GENERATION.fetch_add(1, Ordering::AcqRel);
    *cache = None;
}

fn live_bucket_usage_cache() -> &'static LiveBucketUsageCache {
    LIVE_BUCKET_USAGE_CACHE.get_or_init(|| {
        moka::future::Cache::builder()
            .max_capacity(LIVE_BUCKET_USAGE_MAX_ENTRIES)
            .build()
    })
}

fn usage_memory_generation() -> u64 {
    USAGE_MEMORY_GENERATION.load(Ordering::Acquire)
}

// Data usage storage paths
lazy_static::lazy_static! {
    pub static ref DATA_USAGE_BUCKET: String = format!("{}{}{}",
        crate::disk::RUSTFS_META_BUCKET,
        SLASH_SEPARATOR,
        crate::disk::BUCKET_META_PREFIX
    );
    pub static ref DATA_USAGE_OBJ_NAME_PATH: String = format!("{}{}{}",
        crate::disk::BUCKET_META_PREFIX,
        SLASH_SEPARATOR,
        DATA_USAGE_OBJECT_NAME
    );
    pub static ref DATA_USAGE_OBSERVED_OBJ_NAME_PATH: String = format!("{}{}{}",
        crate::disk::BUCKET_META_PREFIX,
        SLASH_SEPARATOR,
        DATA_USAGE_OBSERVED_OBJECT_NAME
    );
    static ref DATA_USAGE_OBJ_BACKUP_PATH: String = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
    static ref LEGACY_DATA_USAGE_OBJ_NAME_PATH: String = format!("{}{}{}",
        crate::disk::BUCKET_META_PREFIX,
        SLASH_SEPARATOR,
        LEGACY_DATA_USAGE_OBJECT_NAME
    );
    static ref LEGACY_DATA_USAGE_OBJ_BACKUP_PATH: String = format!("{}.bkp", LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str());
    pub static ref DATA_COMPRESSION_TOTAL_NAME_PATH: String = format!("{}{}{}",
        crate::disk::BUCKET_META_PREFIX,
        SLASH_SEPARATOR,
        DATA_COMPRESSION_TOTAL_NAME
    );
}

/// Decide whether an incoming usage snapshot must be skipped as stale, given the local
/// wall clock `now`. Mirrors `stale_data_usage_update_reason` in
/// `crates/scanner/src/scanner.rs` — keep the two consistent.
///
/// If the persisted `existing.last_update` is future-dated beyond
/// [`rustfs_data_usage::USAGE_LAST_UPDATE_FUTURE_TOLERANCE`] (clock step-back or a
/// slower-clock scanner leader), it is untrustworthy: the save is allowed so usage
/// stats cannot freeze forever.
fn stale_data_usage_persist_reason(incoming: &DataUsageInfo, existing: &DataUsageInfo, now: SystemTime) -> Option<&'static str> {
    match (incoming.last_update, existing.last_update) {
        (Some(new_ts), Some(existing_ts))
            if new_ts <= existing_ts && !rustfs_data_usage::usage_last_update_is_untrusted_future(existing_ts, now) =>
        {
            Some("older_or_equal_last_update")
        }
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UsageSnapshotSource {
    Primary,
    Backup,
    LegacyPrimary,
    LegacyBackup,
    Missing,
}

impl UsageSnapshotSource {
    fn is_authoritative(self) -> bool {
        matches!(self, Self::Primary | Self::Backup)
    }

    fn is_backup(self) -> bool {
        matches!(self, Self::Backup | Self::LegacyBackup)
    }
}

fn stale_data_usage_persist_reason_for_source(
    incoming: &DataUsageInfo,
    existing: &DataUsageInfo,
    source: UsageSnapshotSource,
    now: SystemTime,
) -> Option<&'static str> {
    let reason = stale_data_usage_persist_reason(incoming, existing, now);
    if source.is_backup() && incoming.last_update == existing.last_update {
        None
    } else {
        reason
    }
}

/// Store data usage info to backend storage
#[instrument(skip(store))]
pub async fn store_data_usage_in_backend(data_usage_info: DataUsageInfo, store: Arc<ECStore>) -> Result<(), Error> {
    if data_usage_info.usage_snapshot_converged == Some(false) {
        return Err(Error::other(
            "nonconverged data usage observations cannot replace the quota-authoritative snapshot",
        ));
    }
    let Some(expected_publication_epoch) = store.scanner_data_usage_publication_epoch().await else {
        return Err(Error::other("data usage publication is blocked by data movement"));
    };
    // Prevent older data from overwriting newer persisted stats
    let existing_snapshot = load_data_usage_snapshot(store.clone()).await;
    if let Ok((existing, source)) = existing_snapshot
        && source.is_authoritative()
        && let Some(reason) = stale_data_usage_persist_reason_for_source(&data_usage_info, &existing, source, SystemTime::now())
    {
        info!(
            "Skip persisting data usage ({reason}): incoming last_update {:?} <= existing {:?}",
            data_usage_info.last_update, existing.last_update
        );
        return Ok(());
    }

    save_data_usage_in_backend(data_usage_info, store, expected_publication_epoch).await
}

async fn save_data_usage_in_backend(
    data_usage_info: DataUsageInfo,
    store: Arc<ECStore>,
    expected_publication_epoch: u64,
) -> Result<(), Error> {
    let data =
        serde_json::to_vec(&data_usage_info).map_err(|e| Error::other(format!("Failed to serialize data usage info: {e}")))?;

    // Save to backend using the same mechanism as original code
    let Some((publication_guard, publication_epoch)) = store.scanner_data_usage_publication_admission_guard().await else {
        return Err(Error::other("data usage publication is blocked by data movement"));
    };
    if publication_epoch != expected_publication_epoch {
        return Err(Error::other("data usage publication epoch changed before save"));
    }
    crate::config::com::save_config(store.clone(), &DATA_USAGE_OBJ_NAME_PATH, data)
        .await
        .map_err(Error::other)?;
    drop(publication_guard);

    cleanup_observed_data_usage_after_authoritative_save_with_publication(store.as_ref(), &data_usage_info, Some(store.as_ref()))
        .await;

    // Invalidate the cached snapshot so readers observe the new save on their
    // next request instead of waiting out the remaining TTL. The next cached
    // read reloads through `load_data_usage_from_backend`, keeping its
    // backward-compatibility post-processing.
    invalidate_data_usage_snapshot_cache().await;

    Ok(())
}

#[async_trait::async_trait]
trait ObservedDataUsageSnapshotCleanup {
    async fn delete_observed_data_usage_snapshot(&self, revision: &str) -> Result<(), Error>;
}

#[async_trait::async_trait]
impl ObservedDataUsageSnapshotCleanup for ECStore {
    async fn delete_observed_data_usage_snapshot(&self, revision: &str) -> Result<(), Error> {
        self.delete_object(
            RUSTFS_META_BUCKET,
            DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(),
            ObjectOptions {
                delete_prefix: true,
                delete_prefix_object: true,
                http_preconditions: Some(HTTPPreconditions {
                    if_match: Some(revision.to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
        .map(|_| ())
    }
}

async fn cleanup_observed_data_usage_after_authoritative_save_with_publication<S>(
    store: &S,
    authoritative: &DataUsageInfo,
    publication_store: Option<&ECStore>,
) where
    S: EcstoreObjectIO + ObservedDataUsageSnapshotCleanup + ?Sized,
{
    let observed_read_epoch = match publication_store {
        Some(publication_store) => {
            let Some(epoch) = publication_store.scanner_data_usage_publication_epoch().await else {
                return;
            };
            Some(epoch)
        }
        None => None,
    };
    let observed_snapshot = load_data_usage_for_bucket_removal(store, DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str()).await;
    let (observed, revision) = match observed_snapshot {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => return,
        Err(err) => {
            record_usage_snapshot_failure(
                "read_observed_before_authoritative_cleanup",
                DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(),
                &err,
            );
            return;
        }
    };
    if observed_data_usage_is_newer(&observed, authoritative) {
        return;
    }

    let publication_guard = match publication_store {
        Some(publication_store) => {
            let Some((guard, publication_epoch)) = publication_store.scanner_data_usage_publication_admission_guard().await
            else {
                return;
            };
            if observed_read_epoch.is_some_and(|expected| expected != publication_epoch) {
                return;
            }
            Some(guard)
        }
        None => None,
    };
    match store.delete_observed_data_usage_snapshot(&revision).await {
        Ok(()) | Err(Error::ConfigNotFound | Error::FileNotFound | Error::ObjectNotFound(_, _) | Error::PreconditionFailed) => {}
        Err(err) => {
            record_usage_snapshot_failure(
                "delete_observed_after_authoritative_save",
                DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(),
                &err,
            );
        }
    }
    drop(publication_guard);
}

#[cfg(test)]
async fn cleanup_observed_data_usage_after_authoritative_save<S>(store: &S, authoritative: &DataUsageInfo)
where
    S: EcstoreObjectIO + ObservedDataUsageSnapshotCleanup + ?Sized,
{
    cleanup_observed_data_usage_after_authoritative_save_with_publication(store, authoritative, None).await;
}

fn set_buckets_count_from_usage(data_usage_info: &mut DataUsageInfo) {
    data_usage_info.buckets_count = u64::try_from(data_usage_info.buckets_usage.len()).unwrap_or(u64::MAX);
}

fn remove_bucket_usage_from_info(data_usage_info: &mut DataUsageInfo, bucket: &str) -> bool {
    if bucket.is_empty() {
        return false;
    }

    let removed_usage = data_usage_info.buckets_usage.remove(bucket).is_some();
    let removed_size = data_usage_info.bucket_sizes.remove(bucket).is_some();

    if !removed_usage && !removed_size {
        return false;
    }

    set_buckets_count_from_usage(data_usage_info);
    data_usage_info.calculate_totals();
    true
}

async fn clear_bucket_usage_memory(bucket: &str, guard: Option<&rustfs_lock::NamespaceLockGuard>) -> Result<(), Error> {
    if bucket.is_empty() {
        return Ok(());
    }

    let mut cache = memory_cache().write().await;
    ensure_bucket_namespace_guard(guard, bucket, "data usage memory cleanup")?;
    cache.remove(bucket);
    Ok(())
}

pub async fn remove_bucket_usage_from_backend(store: Arc<ECStore>, bucket: &str) -> Result<(), Error> {
    remove_bucket_usage_for_namespace_change(store.as_ref(), bucket).await
}

pub(crate) async fn remove_bucket_usage_for_namespace_change(store: &ECStore, bucket: &str) -> Result<(), Error> {
    prepare_bucket_usage_for_namespace_change(bucket, None).await?;
    remove_bucket_usage_from_backend_with_guard_fenced(store, bucket, None).await
}

pub(crate) async fn prepare_bucket_usage_for_namespace_change(
    bucket: &str,
    guard: Option<&rustfs_lock::NamespaceLockGuard>,
) -> Result<(), Error> {
    ensure_bucket_namespace_guard(guard, bucket, "data usage cache cleanup")?;
    let _ = USAGE_MEMORY_GENERATION.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| Some(current.saturating_add(1)));
    live_bucket_usage_cache().invalidate(bucket).await;
    clear_bucket_usage_memory(bucket, guard).await?;

    let mut snapshot_cache = data_usage_snapshot_cache().write().await;
    ensure_bucket_namespace_guard(guard, bucket, "data usage snapshot cache cleanup")?;
    clear_data_usage_snapshot_cache(&mut snapshot_cache);
    drop(snapshot_cache);

    let mut admin_snapshot_cache = admin_data_usage_snapshot_cache().write().await;
    ensure_bucket_namespace_guard(guard, bucket, "admin data usage snapshot cache cleanup")?;
    clear_admin_data_usage_snapshot_cache(&mut admin_snapshot_cache);
    Ok(())
}

#[cfg(test)]
pub(crate) async fn remove_bucket_usage_from_backend_with_guard<S>(
    store: &S,
    bucket: &str,
    guard: Option<&rustfs_lock::NamespaceLockGuard>,
) -> Result<(), Error>
where
    S: EcstoreObjectIO + ?Sized,
{
    let result = remove_bucket_usage_from_backend_with_store_and_guard(store, bucket, guard).await;
    invalidate_bucket_usage_snapshot_caches(guard, bucket).await?;
    result
}

pub(crate) async fn remove_bucket_usage_from_backend_with_guard_fenced(
    store: &ECStore,
    bucket: &str,
    guard: Option<&rustfs_lock::NamespaceLockGuard>,
) -> Result<(), Error> {
    let result = remove_bucket_usage_from_backend_with_store_and_guard_and_publication(store, bucket, guard, Some(store)).await;
    invalidate_bucket_usage_snapshot_caches(guard, bucket).await?;
    result
}

async fn invalidate_bucket_usage_snapshot_caches(
    guard: Option<&rustfs_lock::NamespaceLockGuard>,
    bucket: &str,
) -> Result<(), Error> {
    let mut snapshot_cache = data_usage_snapshot_cache().write().await;
    ensure_bucket_namespace_guard(guard, bucket, "data usage snapshot cache invalidation")?;
    clear_data_usage_snapshot_cache(&mut snapshot_cache);
    drop(snapshot_cache);
    let mut admin_snapshot_cache = admin_data_usage_snapshot_cache().write().await;
    ensure_bucket_namespace_guard(guard, bucket, "admin data usage snapshot cache invalidation")?;
    clear_admin_data_usage_snapshot_cache(&mut admin_snapshot_cache);
    Ok(())
}

async fn load_data_usage_for_bucket_removal<S>(store: &S, object: &str) -> Result<Option<(DataUsageInfo, String)>, Error>
where
    S: EcstoreObjectIO + ?Sized,
{
    let mut reader = match store
        .get_object_reader(RUSTFS_META_BUCKET, object, None, http::HeaderMap::new(), &ObjectOptions::default())
        .await
    {
        Ok(reader) => reader,
        Err(Error::FileNotFound | Error::ObjectNotFound(_, _) | Error::ConfigNotFound) => return Ok(None),
        Err(err) => return Err(err),
    };
    let revision = reader
        .object_info
        .etag
        .as_deref()
        .filter(|etag| !etag.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| Error::other("data usage snapshot has no ETag"))?;
    let mut data_usage_info = parse_usage_snapshot(&reader.read_all().await?)?;
    populate_backward_compatible_usage_maps(&mut data_usage_info);
    validate_complete_usage_snapshot(&mut data_usage_info);
    Ok(Some((data_usage_info, revision)))
}

fn data_usage_contains_bucket(data_usage_info: &DataUsageInfo, bucket: &str) -> bool {
    data_usage_info.buckets_usage.contains_key(bucket) || data_usage_info.bucket_sizes.contains_key(bucket)
}

fn advance_data_usage_last_update(data_usage_info: &mut DataUsageInfo) {
    let now = SystemTime::now();
    data_usage_info.last_update = Some(match data_usage_info.last_update {
        Some(last_update) if last_update >= now => last_update.checked_add(Duration::from_nanos(1)).unwrap_or(now),
        _ => now,
    });
}

#[cfg(test)]
async fn remove_bucket_usage_from_backend_with_store<S>(store: &S, bucket: &str) -> Result<(), Error>
where
    S: EcstoreObjectIO + ?Sized,
{
    remove_bucket_usage_from_backend_with_store_and_guard(store, bucket, None).await
}

fn ensure_bucket_namespace_guard(
    guard: Option<&rustfs_lock::NamespaceLockGuard>,
    bucket: &str,
    operation: &'static str,
) -> Result<(), Error> {
    if guard.is_some_and(rustfs_lock::NamespaceLockGuard::is_lock_lost) {
        return Err(Error::other(format!("bucket namespace lock was lost before {operation}: {bucket}")));
    }
    Ok(())
}

#[cfg(test)]
async fn remove_bucket_usage_from_backend_with_store_and_guard<S>(
    store: &S,
    bucket: &str,
    guard: Option<&rustfs_lock::NamespaceLockGuard>,
) -> Result<(), Error>
where
    S: EcstoreObjectIO + ?Sized,
{
    remove_bucket_usage_from_backend_with_store_and_guard_and_publication(store, bucket, guard, None).await
}

async fn remove_bucket_usage_from_backend_with_store_and_guard_and_publication<S>(
    store: &S,
    bucket: &str,
    guard: Option<&rustfs_lock::NamespaceLockGuard>,
    publication_store: Option<&ECStore>,
) -> Result<(), Error>
where
    S: EcstoreObjectIO + ?Sized,
{
    ensure_bucket_namespace_guard(guard, bucket, "data usage primary cleanup")?;
    let primary_seed_epoch = match publication_store {
        Some(publication_store) => Some(
            publication_store
                .scanner_data_usage_publication_epoch()
                .await
                .ok_or_else(|| Error::other("data usage publication is blocked by data movement"))?,
        ),
        None => None,
    };
    let primary_seed = load_data_usage_seed_for_missing_primary(store).await?;
    remove_bucket_usage_from_object_with_retries_and_publication(
        store,
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        bucket,
        DATA_USAGE_REMOVE_CAS_RETRIES,
        primary_seed.as_ref(),
        guard,
        publication_store.map(|store| (store, primary_seed_epoch)),
    )
    .await?;

    ensure_bucket_namespace_guard(guard, bucket, "data usage backup cleanup")?;
    let backup_seed_epoch = match publication_store {
        Some(publication_store) => Some(
            publication_store
                .scanner_data_usage_publication_epoch()
                .await
                .ok_or_else(|| Error::other("data usage publication is blocked by data movement"))?,
        ),
        None => None,
    };
    let backup_seed = load_data_usage_for_bucket_removal(store, DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await?
        .map(|(data_usage_info, _)| data_usage_info)
        .or_else(|| primary_seed.clone());
    remove_bucket_usage_from_object_with_retries_and_publication(
        store,
        DATA_USAGE_OBJ_BACKUP_PATH.as_str(),
        bucket,
        DATA_USAGE_REMOVE_CAS_RETRIES,
        backup_seed.as_ref(),
        guard,
        publication_store.map(|store| (store, backup_seed_epoch)),
    )
    .await?;

    ensure_bucket_namespace_guard(guard, bucket, "observed data usage cleanup")?;
    if let Err(err) = remove_bucket_usage_from_object_with_retries_and_publication(
        store,
        DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(),
        bucket,
        DATA_USAGE_REMOVE_CAS_RETRIES,
        None,
        guard,
        publication_store.map(|store| (store, None)),
    )
    .await
    {
        // The authoritative timestamp was already advanced above, so admin
        // selection rejects this observation even if optional cleanup fails.
        // Never make an admin-only freshness artifact block DeleteBucket.
        record_usage_snapshot_failure("remove_bucket_from_observed", DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(), &err);
    }

    for object in [
        LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str(),
        LEGACY_DATA_USAGE_OBJ_BACKUP_PATH.as_str(),
    ] {
        remove_bucket_usage_from_object_with_retries_and_publication(
            store,
            object,
            bucket,
            DATA_USAGE_REMOVE_CAS_RETRIES,
            None,
            guard,
            publication_store.map(|store| (store, None)),
        )
        .await?;
    }
    Ok(())
}

async fn load_data_usage_seed_for_missing_primary<S>(store: &S) -> Result<Option<DataUsageInfo>, Error>
where
    S: EcstoreObjectIO + ?Sized,
{
    for (object, authoritative) in [
        (DATA_USAGE_OBJ_BACKUP_PATH.as_str(), true),
        (LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str(), false),
        (LEGACY_DATA_USAGE_OBJ_BACKUP_PATH.as_str(), false),
    ] {
        if let Some((mut data_usage_info, _)) = load_data_usage_for_bucket_removal(store, object).await? {
            if !authoritative {
                data_usage_info.usage_snapshot_complete = false;
            }
            return Ok(Some(data_usage_info));
        }
    }
    Ok(None)
}

#[cfg(test)]
async fn remove_bucket_usage_from_object_with_retries<S>(
    store: &S,
    object: &str,
    bucket: &str,
    cas_retries: usize,
    missing_seed: Option<&DataUsageInfo>,
    guard: Option<&rustfs_lock::NamespaceLockGuard>,
) -> Result<(), Error>
where
    S: EcstoreObjectIO + ?Sized,
{
    remove_bucket_usage_from_object_with_retries_and_publication(store, object, bucket, cas_retries, missing_seed, guard, None)
        .await
}

async fn remove_bucket_usage_from_object_with_retries_and_publication<S>(
    store: &S,
    object: &str,
    bucket: &str,
    cas_retries: usize,
    missing_seed: Option<&DataUsageInfo>,
    guard: Option<&rustfs_lock::NamespaceLockGuard>,
    publication: Option<(&ECStore, Option<u64>)>,
) -> Result<(), Error>
where
    S: EcstoreObjectIO + ?Sized,
{
    for attempt in 0..=cas_retries {
        ensure_bucket_namespace_guard(guard, bucket, "data usage snapshot cleanup")?;
        let read_epoch = match publication {
            Some((publication_store, expected_publication_epoch)) => {
                let epoch = publication_store
                    .scanner_data_usage_publication_epoch()
                    .await
                    .ok_or_else(|| Error::other("data usage publication is blocked by data movement"))?;
                if expected_publication_epoch.is_some_and(|expected| expected != epoch) {
                    return Err(Error::other("data usage publication epoch changed before snapshot read"));
                }
                Some(epoch)
            }
            None => None,
        };
        let loaded_snapshot = load_data_usage_for_bucket_removal(store, object).await?;
        let (mut data_usage_info, revision) = match loaded_snapshot {
            Some((data_usage_info, revision)) => (data_usage_info, Some(revision)),
            None => match missing_seed {
                Some(data_usage_info) => (data_usage_info.clone(), None),
                None => return Ok(()),
            },
        };
        remove_bucket_usage_from_info(&mut data_usage_info, bucket);
        advance_data_usage_last_update(&mut data_usage_info);

        let data = serde_json::to_vec(&data_usage_info)
            .map_err(|err| Error::other(format!("Failed to serialize data usage info: {err}")))?;
        let mut put_data = PutObjReader::from_vec(data);
        let http_preconditions = match revision.as_ref() {
            Some(revision) => HTTPPreconditions {
                if_match: Some(revision.clone()),
                ..Default::default()
            },
            None => HTTPPreconditions {
                if_none_match: Some("*".to_string()),
                ..Default::default()
            },
        };
        ensure_bucket_namespace_guard(guard, bucket, "data usage snapshot commit")?;
        let publication_guard = match publication {
            Some((publication_store, expected_publication_epoch)) => {
                let Some((guard, publication_epoch)) = publication_store.scanner_data_usage_publication_admission_guard().await
                else {
                    return Err(Error::other("data usage publication is blocked by data movement"));
                };
                if expected_publication_epoch
                    .or(read_epoch)
                    .is_some_and(|expected| expected != publication_epoch)
                {
                    return Err(Error::other("data usage publication epoch changed before snapshot commit"));
                }
                Some(guard)
            }
            None => None,
        };
        let save_result = store
            .put_object(
                RUSTFS_META_BUCKET,
                object,
                &mut put_data,
                &ObjectOptions {
                    max_parity: true,
                    http_preconditions: Some(http_preconditions),
                    ..Default::default()
                },
            )
            .await;
        drop(publication_guard);
        match save_result {
            Ok(_) => return Ok(()),
            Err(err) => {
                if let Some((observed, observed_revision)) = load_data_usage_for_bucket_removal(store, object).await? {
                    let revision_advanced = revision.as_ref().is_none_or(|revision| revision != &observed_revision);
                    if revision_advanced && !data_usage_contains_bucket(&observed, bucket) {
                        return Ok(());
                    }
                    if attempt < cas_retries
                        && (err == Error::PreconditionFailed || revision.as_ref() != Some(&observed_revision))
                    {
                        ensure_bucket_namespace_guard(guard, bucket, "data usage snapshot retry")?;
                        continue;
                    }
                } else if attempt < cas_retries && err == Error::PreconditionFailed {
                    continue;
                }
                return Err(err);
            }
        }
    }

    Err(Error::other("data usage bucket removal CAS retries exhausted"))
}

fn record_usage_snapshot_failure(operation: &'static str, object: &str, e: &Error) {
    let reason = classify_system_path_failure_reason(e);
    record_system_path_failure("data_usage", operation, reason);
    error!(
        event = "data_usage_snapshot_load_failed",
        component = "ecstore",
        subsystem = "data_usage",
        state = "read_failed",
        operation,
        reason,
        object = %object,
        error = %e,
        "data usage snapshot load failed"
    );
}

fn record_usage_snapshot_decode_failure(operation: &'static str, object: &str, e: &Error) {
    const REASON: &str = "decode_error";
    record_system_path_failure("data_usage", operation, REASON);
    error!(
        event = "data_usage_snapshot_load_failed",
        component = "ecstore",
        subsystem = "data_usage",
        state = "decode_failed",
        operation,
        reason = REASON,
        object = %object,
        error = %e,
        "data usage snapshot load failed"
    );
}

fn parse_usage_snapshot(buf: &[u8]) -> Result<DataUsageInfo, Error> {
    serde_json::from_slice(buf).map_err(|e| {
        Error::other(format!(
            "Failed to deserialize data usage info: {:?} at line {} column {}",
            e.classify(),
            e.line(),
            e.column()
        ))
    })
}

/// Decide which usage snapshot to serve: a primary usage object or its
/// `.bkp` backup. The backup future is polled only when the primary is unusable,
/// so the healthy path performs a single read.
///
/// `Ok(DataUsageInfo::default())` is returned only when BOTH the primary and
/// the backup are `ConfigNotFound` — a genuine "no snapshot yet". A real
/// primary failure with a missing backup propagates as an error instead of
/// being rendered as confirmed all-zero stats.
async fn resolve_loaded_snapshot_pair_with_source(
    primary: Result<Vec<u8>, Error>,
    backup: impl Future<Output = Result<Vec<u8>, Error>>,
    primary_path: &str,
    backup_path: &str,
) -> Result<(DataUsageInfo, UsageSnapshotSource), Error> {
    let primary_failure = match primary {
        Ok(buf) => match parse_usage_snapshot(&buf) {
            Ok(info) => return Ok((info, UsageSnapshotSource::Primary)),
            Err(e) => {
                record_usage_snapshot_decode_failure("parse_primary", primary_path, &e);
                e
            }
        },
        Err(e) => {
            if e != Error::ConfigNotFound {
                record_usage_snapshot_failure("read_primary", primary_path, &e);
            }
            e
        }
    };
    match backup.await {
        Ok(buf) => match parse_usage_snapshot(&buf) {
            Ok(info) => Ok((info, UsageSnapshotSource::Backup)),
            Err(e) => {
                record_usage_snapshot_decode_failure("parse_backup", backup_path, &e);
                Err(e)
            }
        },
        Err(Error::ConfigNotFound) if primary_failure == Error::ConfigNotFound => {
            Ok((DataUsageInfo::default(), UsageSnapshotSource::Missing))
        }
        Err(Error::ConfigNotFound) => Err(primary_failure),
        Err(e) => {
            record_usage_snapshot_failure("read_backup", backup_path, &e);
            Err(e)
        }
    }
}

#[allow(
    dead_code,
    reason = "primary/backup snapshot fallback asserted by this file's tests (backlog#1823)"
)]
async fn resolve_loaded_snapshot(
    primary: Result<Vec<u8>, Error>,
    backup: impl Future<Output = Result<Vec<u8>, Error>>,
) -> Result<DataUsageInfo, Error> {
    Ok(resolve_loaded_snapshot_pair_with_source(
        primary,
        backup,
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        DATA_USAGE_OBJ_BACKUP_PATH.as_str(),
    )
    .await?
    .0)
}

#[cfg(test)]
async fn resolve_loaded_snapshot_with_source(
    primary: Result<Vec<u8>, Error>,
    backup: impl Future<Output = Result<Vec<u8>, Error>>,
) -> Result<(DataUsageInfo, UsageSnapshotSource), Error> {
    resolve_loaded_snapshot_pair_with_source(
        primary,
        backup,
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        DATA_USAGE_OBJ_BACKUP_PATH.as_str(),
    )
    .await
}

async fn load_data_usage_snapshot_from_store<S: EcstoreObjectIO>(
    store: Arc<S>,
) -> Result<(DataUsageInfo, UsageSnapshotSource), Error> {
    let primary = read_config_preserve_empty(store.clone(), &DATA_USAGE_OBJ_NAME_PATH).await;
    let authoritative = resolve_loaded_snapshot_pair_with_source(
        primary,
        {
            let store = store.clone();
            async move { read_config_preserve_empty(store, &DATA_USAGE_OBJ_BACKUP_PATH).await }
        },
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        DATA_USAGE_OBJ_BACKUP_PATH.as_str(),
    )
    .await?;
    if authoritative.1 != UsageSnapshotSource::Missing {
        return Ok(authoritative);
    }

    let legacy_primary = read_config_preserve_empty(store.clone(), &LEGACY_DATA_USAGE_OBJ_NAME_PATH).await;
    let legacy = resolve_loaded_snapshot_pair_with_source(
        legacy_primary,
        async move { read_config_preserve_empty(store, &LEGACY_DATA_USAGE_OBJ_BACKUP_PATH).await },
        LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str(),
        LEGACY_DATA_USAGE_OBJ_BACKUP_PATH.as_str(),
    )
    .await?;
    Ok((
        legacy.0,
        match legacy.1 {
            UsageSnapshotSource::Primary => UsageSnapshotSource::LegacyPrimary,
            UsageSnapshotSource::Backup => UsageSnapshotSource::LegacyBackup,
            source => source,
        },
    ))
}

async fn load_data_usage_snapshot(store: Arc<ECStore>) -> Result<(DataUsageInfo, UsageSnapshotSource), Error> {
    load_data_usage_snapshot_from_store(store).await
}

/// Load data usage info from backend storage
#[instrument(skip(store))]
pub async fn load_data_usage_from_backend(store: Arc<ECStore>) -> Result<DataUsageInfo, Error> {
    Ok(load_data_usage_from_backend_with_baseline(store).await?.0)
}

/// Like [`load_data_usage_from_backend`], but also returns the pre-discard
/// per-bucket sizes so the cached loader can retain them as the degraded
/// quota-admission baseline (issue #5716).
async fn load_data_usage_from_backend_with_baseline(store: Arc<ECStore>) -> Result<(DataUsageInfo, HashMap<String, u64>), Error> {
    let (data_usage_info, source) = load_data_usage_snapshot(store).await?;
    Ok(normalize_loaded_data_usage(data_usage_info, source.is_authoritative()).await)
}

async fn load_observed_data_usage_snapshot(store: Arc<ECStore>) -> Option<DataUsageInfo> {
    let data = match read_config_preserve_empty(store, &DATA_USAGE_OBSERVED_OBJ_NAME_PATH).await {
        Ok(data) => data,
        Err(Error::ConfigNotFound) => return None,
        Err(err) => {
            record_usage_snapshot_failure("read_observed", DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(), &err);
            return None;
        }
    };

    match parse_usage_snapshot(&data) {
        Ok(info)
            if info.usage_snapshot_converged == Some(false)
                && (info.is_complete_bucket_usage_snapshot() || info.is_valid_partial_snapshot()) =>
        {
            Some(info)
        }
        Ok(_) => {
            error!(
                event = "data_usage_snapshot_load_failed",
                component = "ecstore",
                subsystem = "data_usage",
                state = "invalid_observed_snapshot",
                object = %DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(),
                "observed data usage snapshot was not a structurally complete nonconverged view"
            );
            None
        }
        Err(err) => {
            record_usage_snapshot_decode_failure("parse_observed", DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(), &err);
            None
        }
    }
}

fn select_admin_data_usage_snapshot(
    mut authoritative: DataUsageInfo,
    authoritative_format: bool,
    observed: Option<DataUsageInfo>,
) -> (DataUsageInfo, bool) {
    if authoritative_format
        && authoritative.is_complete_bucket_usage_snapshot()
        && authoritative.usage_snapshot_converged.is_none()
    {
        authoritative.usage_snapshot_converged = Some(true);
    }
    match observed {
        Some(observed) if observed_data_usage_is_newer(&observed, &authoritative) => (observed, true),
        _ => (authoritative, authoritative_format),
    }
}

async fn load_admin_data_usage_from_backend(store: Arc<ECStore>) -> Result<DataUsageInfo, Error> {
    let (authoritative, source) = load_data_usage_snapshot(store.clone()).await?;
    let observed = load_observed_data_usage_snapshot(store).await;
    let (selected, selected_is_current_format) =
        select_admin_data_usage_snapshot(authoritative, source.is_authoritative(), observed);
    Ok(normalize_loaded_data_usage(selected, selected_is_current_format).await.0)
}

fn discard_incomplete_bucket_usage(data_usage_info: &mut DataUsageInfo) {
    if !data_usage_info.is_complete_bucket_usage_snapshot() && !data_usage_info.usage_snapshot_partial {
        data_usage_info.usage_snapshot_complete = false;
        data_usage_info.buckets_usage.clear();
        data_usage_info.bucket_sizes.clear();
        data_usage_info.buckets_count = 0;
        data_usage_info.calculate_totals();
    }
}

fn validate_complete_usage_snapshot(data_usage_info: &mut DataUsageInfo) {
    if data_usage_info.usage_snapshot_complete && !data_usage_info.is_complete_bucket_usage_snapshot() {
        data_usage_info.usage_snapshot_complete = false;
    }
}

fn populate_backward_compatible_usage_maps(data_usage_info: &mut DataUsageInfo) {
    if data_usage_info.buckets_usage.is_empty() {
        data_usage_info.buckets_usage = data_usage_info
            .bucket_sizes
            .iter()
            .map(|(bucket, &size)| {
                (
                    bucket.clone(),
                    BucketUsageInfo {
                        size,
                        ..Default::default()
                    },
                )
            })
            .collect();
    }

    if data_usage_info.bucket_sizes.is_empty() {
        data_usage_info.bucket_sizes = data_usage_info
            .buckets_usage
            .iter()
            .map(|(bucket, bui)| (bucket.clone(), bui.size))
            .collect();
    }
}

/// Returns the normalized snapshot plus the pre-discard per-bucket sizes: the
/// degraded quota-admission baseline captured before an incomplete snapshot
/// drops its bucket data (issue #5716).
async fn normalize_loaded_data_usage(
    mut data_usage_info: DataUsageInfo,
    authoritative_format: bool,
) -> (DataUsageInfo, HashMap<String, u64>) {
    info!("Loaded data usage info from backend with {} buckets", data_usage_info.buckets_count);

    if !authoritative_format {
        data_usage_info.usage_snapshot_complete = false;
    }
    populate_backward_compatible_usage_maps(&mut data_usage_info);
    validate_complete_usage_snapshot(&mut data_usage_info);
    let degraded_baseline = data_usage_info.bucket_sizes.clone();
    discard_incomplete_bucket_usage(&mut data_usage_info);

    // Handle replication info
    for (bucket, bui) in &data_usage_info.buckets_usage {
        if (bui.replicated_size_v1 > 0
            || bui.replication_failed_count_v1 > 0
            || bui.replication_failed_size_v1 > 0
            || bui.replication_pending_count_v1 > 0)
            && let Ok((cfg, _)) = get_replication_config(bucket).await
            && !cfg.role.is_empty()
        {
            data_usage_info.replication_info.insert(
                cfg.role.clone(),
                BucketTargetUsageInfo {
                    replication_failed_size: bui.replication_failed_size_v1,
                    replication_failed_count: bui.replication_failed_count_v1,
                    replicated_size: bui.replicated_size_v1,
                    replication_pending_count: bui.replication_pending_count_v1,
                    replication_pending_size: bui.replication_pending_size_v1,
                    ..Default::default()
                },
            );
        }
    }

    (data_usage_info, degraded_baseline)
}

/// Load the persisted data usage snapshot through a small in-process cache.
///
/// Admin read endpoints call this on every request; the cache bounds backend
/// reads (and the associated JSON parse and INFO log) to once per
/// `DATA_USAGE_CACHE_TTL_SECS` per process. `save_data_usage_in_backend`
/// invalidates the cache so a fresh scanner save is visible immediately.
pub async fn load_data_usage_from_backend_cached(store: Arc<ECStore>) -> Result<DataUsageInfo, Error> {
    let ttl = Duration::from_secs(DATA_USAGE_CACHE_TTL_SECS);

    loop {
        {
            let cache = data_usage_snapshot_cache().read().await;
            if let Some(result) = fresh_cached_data_usage_snapshot(&cache, tokio::time::Instant::now(), ttl) {
                return result;
            }
        }

        // Serialize refreshes without holding the cache lock across backend I/O.
        let refresh_guard = DATA_USAGE_SNAPSHOT_REFRESH
            .get_or_init(|| Arc::new(TokioMutex::new(())))
            .lock()
            .await;
        {
            let cache = data_usage_snapshot_cache().read().await;
            if let Some(result) = fresh_cached_data_usage_snapshot(&cache, tokio::time::Instant::now(), ttl) {
                return result;
            }
        }

        let refresh_generation = data_usage_snapshot_generation();
        let result = load_data_usage_from_backend_with_baseline(store.clone()).await;
        let loaded_at = tokio::time::Instant::now();
        let mut cache = data_usage_snapshot_cache().write().await;
        if let Some(result) =
            cache_data_usage_snapshot_result(&mut cache, result, loaded_at, refresh_generation, data_usage_snapshot_generation())
        {
            return result;
        }
        drop(cache);
        drop(refresh_guard);
    }
}

/// Load the freshest structurally complete snapshot for authenticated admin
/// observability. A scan raced by namespace activity may be selected here, but
/// never by [`load_data_usage_from_backend_cached`], which remains the
/// converged source for quota admission.
pub async fn load_admin_data_usage_from_backend_cached(store: Arc<ECStore>) -> Result<DataUsageInfo, Error> {
    let ttl = Duration::from_secs(DATA_USAGE_CACHE_TTL_SECS);

    loop {
        {
            let cache = admin_data_usage_snapshot_cache().read().await;
            if let Some(result) = fresh_cached_data_usage_snapshot(&cache, tokio::time::Instant::now(), ttl) {
                return result;
            }
        }

        let refresh_guard = ADMIN_DATA_USAGE_SNAPSHOT_REFRESH
            .get_or_init(|| Arc::new(TokioMutex::new(())))
            .lock()
            .await;
        {
            let cache = admin_data_usage_snapshot_cache().read().await;
            if let Some(result) = fresh_cached_data_usage_snapshot(&cache, tokio::time::Instant::now(), ttl) {
                return result;
            }
        }

        let refresh_generation = admin_data_usage_snapshot_generation();
        let result = load_admin_data_usage_from_backend(store.clone())
            .await
            .map(|info| (info, HashMap::new()));
        let loaded_at = tokio::time::Instant::now();
        let mut cache = admin_data_usage_snapshot_cache().write().await;
        if let Some(result) = cache_data_usage_snapshot_result(
            &mut cache,
            result,
            loaded_at,
            refresh_generation,
            admin_data_usage_snapshot_generation(),
        ) {
            return result;
        }
        drop(cache);
        drop(refresh_guard);
    }
}

/// Invalidate the process-local persisted usage snapshot cache after a durable
/// scanner write.
pub async fn invalidate_data_usage_snapshot_cache() {
    let mut cache = data_usage_snapshot_cache().write().await;
    clear_data_usage_snapshot_cache(&mut cache);

    let mut admin_cache = admin_data_usage_snapshot_cache().write().await;
    clear_admin_data_usage_snapshot_cache(&mut admin_cache);
}

/// Invalidate only the admin/console view after an observational save. Quota
/// admission continues to use the independently cached converged snapshot.
pub async fn invalidate_admin_data_usage_snapshot_cache() {
    let mut cache = admin_data_usage_snapshot_cache().write().await;
    clear_admin_data_usage_snapshot_cache(&mut cache);
}

/// Aggregate usage information from local disk snapshots.
#[allow(
    dead_code,
    reason = "reached only through aggregate_local_snapshots, which has no caller (backlog#1823)"
)]
fn merge_snapshot(aggregated: &mut DataUsageInfo, mut snapshot: LocalUsageSnapshot, latest_update: &mut Option<SystemTime>) {
    if let Some(update) = snapshot.last_update
        && latest_update.is_none_or(|current| update > current)
    {
        *latest_update = Some(update);
    }

    snapshot.recompute_totals();

    aggregated.objects_total_count = aggregated.objects_total_count.saturating_add(snapshot.objects_total_count);
    aggregated.versions_total_count = aggregated.versions_total_count.saturating_add(snapshot.versions_total_count);
    aggregated.delete_markers_total_count = aggregated
        .delete_markers_total_count
        .saturating_add(snapshot.delete_markers_total_count);
    aggregated.objects_total_size = aggregated.objects_total_size.saturating_add(snapshot.objects_total_size);

    for (bucket, usage) in snapshot.buckets_usage.into_iter() {
        let bucket_size = usage.size;
        match aggregated.buckets_usage.entry(bucket.clone()) {
            Entry::Occupied(mut entry) => entry.get_mut().merge(&usage),
            Entry::Vacant(entry) => {
                entry.insert(usage.clone());
            }
        }

        aggregated
            .bucket_sizes
            .entry(bucket)
            .and_modify(|size| *size = size.saturating_add(bucket_size))
            .or_insert(bucket_size);
    }
}

#[allow(
    dead_code,
    reason = "entry point of the local usage-snapshot feature, which has had no caller since it landed in #5307 (backlog#1823)"
)]
pub async fn aggregate_local_snapshots(store: Arc<ECStore>) -> Result<(Vec<DiskUsageStatus>, DataUsageInfo), Error> {
    let mut aggregated = DataUsageInfo::default();
    let mut latest_update: Option<SystemTime> = None;
    let mut statuses: Vec<DiskUsageStatus> = Vec::new();
    let mut processed_disks: HashSet<String> = HashSet::new();

    for (pool_idx, pool) in store.pools.iter().enumerate() {
        for set_disks in pool.disk_set.iter() {
            let disk_entries = {
                let guard = set_disks.disks.read().await;
                guard.clone()
            };

            for (disk_index, disk_opt) in disk_entries.into_iter().enumerate() {
                let Some(disk) = disk_opt else {
                    continue;
                };

                if !disk.is_local() {
                    continue;
                }

                let disk_id = match disk.get_disk_id().await.map_err(Error::from)? {
                    Some(id) => id.to_string(),
                    None => continue,
                };

                let root = disk.path();
                let disk_key = format!("{}|{}", disk.endpoint(), root.display());

                // Skip if we've already processed this physical disk
                if !processed_disks.insert(disk_key.clone()) {
                    continue;
                }

                let mut status = DiskUsageStatus {
                    disk_id: disk_id.clone(),
                    pool_index: Some(pool_idx),
                    set_index: Some(set_disks.set_index),
                    disk_index: Some(disk_index),
                    last_update: None,
                    snapshot_exists: false,
                };

                let snapshot_result = read_local_snapshot(root.as_path(), &disk_id).await;

                // If a snapshot is corrupted or unreadable, skip it but keep processing others
                if let Err(err) = &snapshot_result {
                    info!(
                        "Failed to read data usage snapshot for disk {} (pool {}, set {}, disk {}): {}",
                        disk_id, pool_idx, set_disks.set_index, disk_index, err
                    );
                    // Best-effort cleanup so next scan can rebuild a fresh snapshot instead of repeatedly failing
                    let snapshot_file = snapshot_path(root.as_path(), &disk_id);
                    if let Err(remove_err) = fs::remove_file(&snapshot_file).await
                        && remove_err.kind() != std::io::ErrorKind::NotFound
                    {
                        info!("Failed to remove corrupted snapshot {:?}: {}", snapshot_file, remove_err);
                    }
                }

                if let Ok(Some(mut snapshot)) = snapshot_result {
                    status.last_update = snapshot.last_update;
                    status.snapshot_exists = true;

                    if snapshot.meta.disk_id.is_empty() {
                        snapshot.meta.disk_id = disk_id.clone();
                    }
                    if snapshot.meta.pool_index.is_none() {
                        snapshot.meta.pool_index = Some(pool_idx);
                    }
                    if snapshot.meta.set_index.is_none() {
                        snapshot.meta.set_index = Some(set_disks.set_index);
                    }
                    if snapshot.meta.disk_index.is_none() {
                        snapshot.meta.disk_index = Some(disk_index);
                    }

                    merge_snapshot(&mut aggregated, snapshot, &mut latest_update);
                }

                statuses.push(status);
            }
        }
    }

    aggregated.buckets_count = aggregated.buckets_usage.len() as u64;
    aggregated.last_update = latest_update;
    aggregated.disk_usage_status = statuses.clone();

    Ok((statuses, aggregated))
}

/// Calculate accurate bucket usage statistics by enumerating objects through the object layer.
#[derive(Default)]
struct BucketUsageAccumulator {
    current_object_name: Option<String>,
    // FileMeta caps versions per object, so replay detection remains bounded.
    current_object_versions: HashSet<Option<[u8; 16]>>,
    current_live_versions: u64,
    objects_count: u64,
    versions_count: u64,
    total_size: u64,
    delete_markers: u64,
    size_histogram: SizeHistogram,
    versions_histogram: VersionsHistogram,
}

impl BucketUsageAccumulator {
    fn record(&mut self, bucket: &str, object: &ObjectInfo) -> Result<(), Error> {
        if object.is_dir {
            return Ok(());
        }

        if self
            .current_object_name
            .as_deref()
            .is_some_and(|current_name| object.name.as_str() != current_name)
        {
            self.finish_current_object();
        }

        record_version_listing_entry(
            bucket,
            &mut self.current_object_name,
            &mut self.current_object_versions,
            &object.name,
            object.version_id.as_ref().map(|version_id| version_id.as_bytes()),
        )?;

        if object.delete_marker {
            self.delete_markers = self.delete_markers.saturating_add(1);
            return Ok(());
        }

        let object_size = quota_object_size(object)?;
        self.current_live_versions = self.current_live_versions.saturating_add(1);
        self.size_histogram.add(object_size);
        self.total_size = self.total_size.saturating_add(object_size);
        self.versions_count = self.versions_count.saturating_add(1);
        Ok(())
    }

    fn finish_current_object(&mut self) {
        if self.current_live_versions > 0 {
            self.objects_count = self.objects_count.saturating_add(1);
            self.versions_histogram.add(self.current_live_versions);
        }
        self.current_live_versions = 0;
    }

    fn finish(mut self) -> BucketUsageInfo {
        self.finish_current_object();
        BucketUsageInfo {
            size: self.total_size,
            objects_count: self.objects_count,
            versions_count: self.versions_count,
            delete_markers_count: self.delete_markers,
            object_size_histogram: self.size_histogram.to_map(),
            object_versions_histogram: self.versions_histogram.to_map(),
            ..Default::default()
        }
    }
}

pub fn quota_object_size(object: &ObjectInfo) -> Result<u64, Error> {
    // A compressed object may carry -1 while the transformed size is unknown
    // (legacy streaming sentinel). In that case the persisted physical size
    // is still a valid accounting floor; every other negative value is corrupt.
    // An explicit negative `actual-size` metadata value is corrupt, however:
    // the sentinel is only valid in the in-memory/object-part field written by
    // the legacy streaming path, not as a persisted declared size.
    let compressed = object.is_compressed();
    if object.actual_size < -1 || (object.actual_size == -1 && !compressed) {
        return Err(Error::PartMissingOrCorrupt);
    }
    if object
        .parts
        .iter()
        .any(|part| part.actual_size < -1 || (part.actual_size < 0 && !compressed))
    {
        return Err(Error::PartMissingOrCorrupt);
    }
    let declared_actual_size = rustfs_utils::http::get_str(&object.user_defined, rustfs_utils::http::SUFFIX_ACTUAL_SIZE)
        .filter(|value| !value.is_empty());
    if declared_actual_size
        .as_deref()
        .and_then(|value| value.parse::<i64>().ok())
        .is_some_and(|size| size < 0)
    {
        return Err(Error::PartMissingOrCorrupt);
    }
    let logical_size = match object.get_actual_size().map_err(Error::other)? {
        size if size == -1 && compressed && declared_actual_size.is_none() => None,
        size if size >= 0 => Some(u64::try_from(size).map_err(|_| Error::PartMissingOrCorrupt)?),
        _ => return Err(Error::PartMissingOrCorrupt),
    };
    let persisted_part_size = if object.parts.is_empty() {
        u64::try_from(object.size).map_err(|_| Error::PartMissingOrCorrupt)?
    } else {
        object.parts.iter().try_fold(0_u64, |total, part| {
            // Compressed streaming objects persist -1 when the transformed
            // part size is unknown. The physical part size remains a valid
            // quota floor; reject only non-negative values that overflow.
            let actual_size = if part.actual_size == -1 {
                0
            } else {
                u64::try_from(part.actual_size).map_err(|_| Error::PartMissingOrCorrupt)?
            };
            let part_size = actual_size.max(u64::try_from(part.size).map_err(|_| Error::PartMissingOrCorrupt)?);
            total.checked_add(part_size).ok_or(Error::PartMissingOrCorrupt)
        })?
    };
    Ok(logical_size.unwrap_or(0).max(persisted_part_size))
}

type UsageVersionPage = StorageListObjectVersionsInfo<ObjectInfo>;

pub async fn compute_bucket_usage(store: Arc<ECStore>, bucket_name: &str) -> Result<BucketUsageInfo, Error> {
    LIVE_BUCKET_USAGE_COMPUTATIONS.fetch_add(1, Ordering::Relaxed);
    let bucket = bucket_name.to_string();
    compute_bucket_usage_with_pages(bucket_name, move |marker, version_marker| {
        let store = Arc::clone(&store);
        let bucket = bucket.clone();
        async move {
            store
                .list_object_versions(&bucket, "", marker, version_marker, None, 1000)
                .await
        }
    })
    .await
}

async fn compute_bucket_usage_with_pages<F, Fut>(bucket_name: &str, mut fetch_page: F) -> Result<BucketUsageInfo, Error>
where
    F: FnMut(Option<String>, Option<String>) -> Fut,
    Fut: Future<Output = Result<UsageVersionPage, Error>>,
{
    let mut marker: Option<String> = None;
    let mut version_marker: Option<String> = None;
    let mut usage = BucketUsageAccumulator::default();

    loop {
        let result = fetch_page(marker.clone(), version_marker.clone()).await?;

        let page_entries = result.objects.len();
        for object in result.objects.iter() {
            usage.record(bucket_name, object)?;
        }

        if !result.is_truncated {
            break;
        }
        ensure_truncated_version_page_has_entries(bucket_name, page_entries)?;

        advance_version_listing_cursor(
            bucket_name,
            &mut marker,
            &mut version_marker,
            result.next_marker,
            result.next_version_idmarker,
        )?;
    }

    Ok(usage.finish())
}

fn ensure_truncated_version_page_has_entries(bucket: &str, page_entries: usize) -> Result<(), Error> {
    if page_entries == 0 {
        return Err(Error::other(format!("bucket {bucket} version listing returned an empty truncated page")));
    }
    Ok(())
}

fn record_version_listing_entry(
    bucket: &str,
    current_object_name: &mut Option<String>,
    current_object_versions: &mut HashSet<Option<[u8; 16]>>,
    object_name: &str,
    version_id: Option<&[u8; 16]>,
) -> Result<(), Error> {
    match current_object_name.as_deref() {
        Some(current_name) if object_name < current_name => {
            return Err(Error::other(format!("bucket {bucket} version listing returned an out-of-order object")));
        }
        Some(current_name) if object_name > current_name => {
            current_object_versions.clear();
            *current_object_name = Some(object_name.to_string());
        }
        None => *current_object_name = Some(object_name.to_string()),
        Some(_) => {}
    }

    if !current_object_versions.insert(version_id.copied()) {
        return Err(Error::other(format!(
            "bucket {bucket} version listing returned a repeated object version"
        )));
    }
    Ok(())
}

fn advance_version_listing_cursor(
    bucket: &str,
    marker: &mut Option<String>,
    version_marker: &mut Option<String>,
    next_marker: Option<String>,
    next_version_marker: Option<String>,
) -> Result<(), Error> {
    let next_marker = next_marker
        .filter(|next_marker| !next_marker.is_empty())
        .ok_or_else(|| Error::other(format!("bucket {bucket} version listing was truncated without a key marker")))?;
    let next_version_marker = next_version_marker
        .filter(|next_version_marker| !next_version_marker.is_empty())
        .ok_or_else(|| Error::other(format!("bucket {bucket} version listing was truncated without a version marker")))?;
    let current_key_marker = marker.as_deref().map(list_marker_key);
    let next_key_marker = list_marker_key(&next_marker);
    if current_key_marker == Some(next_key_marker) && version_marker.as_deref() == Some(next_version_marker.as_str()) {
        return Err(Error::other(format!(
            "bucket {bucket} version listing returned a repeated continuation marker"
        )));
    }
    if current_key_marker.is_some_and(|marker| next_key_marker < marker) {
        return Err(Error::other(format!("bucket {bucket} version listing returned a regressing key marker")));
    }
    *marker = Some(next_marker);
    *version_marker = Some(next_version_marker);
    Ok(())
}

async fn coalesce_live_bucket_usage<F>(bucket: String, init: F) -> Result<BucketUsageInfo, Error>
where
    F: Future<Output = Result<BucketUsageInfo, Error>> + Send + 'static,
{
    let result = live_bucket_usage_cache().try_get_with(bucket.clone(), init).await;
    live_bucket_usage_cache().invalidate(&bucket).await;
    result.map_err(|err| Error::other(err.to_string()))
}

fn apply_live_bucket_usage_to_response(data_usage_info: &mut DataUsageInfo, bucket: &str, usage: &BucketUsageInfo) {
    let inserted_bucket = !data_usage_info.buckets_usage.contains_key(bucket);
    data_usage_info.bucket_sizes.insert(bucket.to_string(), usage.size);
    data_usage_info.buckets_usage.insert(bucket.to_string(), usage.clone());
    if inserted_bucket {
        data_usage_info.usage_snapshot_complete = false;
    }
    set_buckets_count_from_usage(data_usage_info);
    data_usage_info.calculate_totals();
}

pub async fn refresh_bucket_usage_from_object_layer(
    store: Arc<ECStore>,
    data_usage_info: &mut DataUsageInfo,
    bucket: &str,
) -> Result<BucketUsageInfo, Error> {
    let bucket_name = bucket.to_string();
    let usage =
        coalesce_live_bucket_usage(bucket_name.clone(), async move { compute_bucket_usage(store, &bucket_name).await }).await?;
    // Request-time listings are not linearizable with writes on other nodes.
    // Keep the live result response-local instead of promoting it into the quota cache.
    apply_live_bucket_usage_to_response(data_usage_info, bucket, &usage);
    Ok(usage)
}

pub async fn refresh_versioned_bucket_usage_from_object_layer(store: Arc<ECStore>, data_usage_info: &mut DataUsageInfo) {
    let listed_bucket_names = match store
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
    {
        Ok(buckets) => buckets.into_iter().map(|bucket| bucket.name).collect::<Vec<_>>(),
        Err(err) => {
            debug!(error = %err, "failed to list buckets while refreshing versioned bucket usage");
            Vec::new()
        }
    };
    let mut buckets = data_usage_info.buckets_usage.keys().cloned().collect::<HashSet<String>>();
    buckets.extend(listed_bucket_names.into_iter().filter(|bucket| !bucket.is_empty()));
    let mut buckets = buckets.into_iter().collect::<Vec<_>>();
    buckets.sort();

    for bucket in buckets {
        let Ok(versioning) = BucketVersioningSys::get(&bucket).await else {
            continue;
        };
        if !versioning.enabled() && !versioning.suspended() {
            continue;
        }
        if let Err(err) = refresh_bucket_usage_from_object_layer(store.clone(), data_usage_info, &bucket).await {
            debug!(
                bucket = %bucket,
                error = %err,
                "failed to refresh versioned bucket usage from object layer"
            );
        }
    }
}

async fn ensure_bucket_usage_cached(bucket: &str) {
    let cache = memory_cache().read().await;
    if cache.contains_key(bucket) {
        return;
    }
    drop(cache);

    update_usage_cache_if_needed().await;
}

fn cached_bucket_usage_from_backend(usage: BucketUsageInfo, updated_at: SystemTime, authoritative: bool) -> CachedBucketUsage {
    CachedBucketUsage {
        usage,
        authoritative,
        refreshed_at: SystemTime::now(),
        usage_updated_at: updated_at,
        dirty: false,
        stale_snapshot_pending: false,
        pending_scanner_position: None,
        pending_negative_delta: 0,
    }
}

fn cached_bucket_usage_now(usage: BucketUsageInfo) -> CachedBucketUsage {
    let now = SystemTime::now();
    CachedBucketUsage {
        usage,
        authoritative: false,
        refreshed_at: now,
        usage_updated_at: now,
        dirty: false,
        stale_snapshot_pending: false,
        pending_scanner_position: None,
        pending_negative_delta: 0,
    }
}

fn data_usage_info_updated_at(data_usage_info: &DataUsageInfo) -> SystemTime {
    data_usage_info.last_update.unwrap_or(SystemTime::UNIX_EPOCH)
}

fn bucket_usage_counts_match(left: &BucketUsageInfo, right: &BucketUsageInfo) -> bool {
    left.size == right.size
        && left.objects_count == right.objects_count
        && left.versions_count == right.versions_count
        && left.delete_markers_count == right.delete_markers_count
}

fn preserve_unknown_dirty_usage(
    existing: &CachedBucketUsage,
    snapshot_position: Option<(u64, u64)>,
    snapshot_update: SystemTime,
) -> Option<CachedBucketUsage> {
    if existing.authoritative || !existing.dirty {
        return None;
    }
    if existing
        .pending_scanner_position
        .zip(snapshot_position)
        .is_some_and(|(previous, current)| current.0 > previous.0 || (current.0 == previous.0 && current.1 > previous.1))
    {
        return None;
    }

    let mut preserved = existing.clone();
    preserved.stale_snapshot_pending = true;
    if preserved.pending_scanner_position.is_none() && snapshot_update >= existing.usage_updated_at {
        preserved.pending_scanner_position = snapshot_position;
    }
    Some(preserved)
}

#[cfg(any(test, feature = "test-util"))]
async fn replace_bucket_usage_memory_from_authoritative(bucket: &str, usage: BucketUsageInfo, refresh_started_at: SystemTime) {
    let mut cache = memory_cache().write().await;
    if let Some(existing) = cache.get(bucket)
        && existing.usage_updated_at > refresh_started_at
    {
        return;
    }

    cache.insert(bucket.to_string(), cached_bucket_usage_from_backend(usage, refresh_started_at, true));
}

#[cfg(feature = "test-util")]
pub async fn seed_bucket_usage_memory_for_test(bucket: &str, size: u64) {
    replace_bucket_usage_memory_from_authoritative(
        bucket,
        BucketUsageInfo {
            size,
            ..Default::default()
        },
        SystemTime::now(),
    )
    .await;
}

/// Fast in-memory update for immediate quota and admin usage consistency.
pub async fn record_bucket_object_write_memory(bucket: &str, previous_current_size: Option<u64>, new_size: u64) {
    record_bucket_object_write_memory_inner(bucket, previous_current_size, new_size, false).await;
}

/// Fast in-memory update for versioned object writes.
pub async fn record_bucket_object_version_write_memory(bucket: &str, previous_current_size: Option<u64>, new_size: u64) {
    record_bucket_object_write_memory_inner(bucket, previous_current_size, new_size, true).await;
}

async fn record_bucket_object_write_memory_inner(
    bucket: &str,
    previous_current_size: Option<u64>,
    new_size: u64,
    creates_new_version: bool,
) {
    ensure_bucket_usage_cached(bucket).await;

    let mut cache = memory_cache().write().await;
    let entry = cache
        .entry(bucket.to_string())
        .or_insert_with(|| cached_bucket_usage_now(BucketUsageInfo::default()));

    if creates_new_version {
        entry.usage.size = entry.usage.size.saturating_add(new_size);
        if previous_current_size.is_none() {
            entry.usage.objects_count = entry.usage.objects_count.saturating_add(1);
        }
        entry.usage.versions_count = entry.usage.versions_count.saturating_add(1);
    } else {
        match previous_current_size {
            Some(previous_size) => {
                entry.usage.size = entry.usage.size.saturating_sub(previous_size).saturating_add(new_size);
            }
            None => {
                entry.usage.size = entry.usage.size.saturating_add(new_size);
                entry.usage.objects_count = entry.usage.objects_count.saturating_add(1);
                entry.usage.versions_count = entry.usage.versions_count.saturating_add(1);
            }
        }
    }

    let now = SystemTime::now();
    entry.refreshed_at = now;
    entry.usage_updated_at = now;
    entry.dirty = true;
    entry.stale_snapshot_pending = false;
    entry.pending_scanner_position = None;
}

/// Degraded in-memory update for an object write whose previous current size
/// could not be determined (rustfs/backlog#1009: the pre-PUT lookup was
/// skipped and the rename_data backfill came back unknown — mixed-version
/// peers or sub-quorum metadata divergence). Applies only the components that
/// are correct regardless of the previous state: the new bytes always count,
/// and a versioned write always adds a version. objects_count (and the
/// non-versioned overwrite's old-size subtraction) are left to the next
/// scanner refresh, which replaces this cache with authoritative numbers.
pub async fn record_bucket_object_write_unknown_previous_memory(bucket: &str, new_size: u64, creates_new_version: bool) {
    ensure_bucket_usage_cached(bucket).await;

    let mut cache = memory_cache().write().await;
    let entry = cache
        .entry(bucket.to_string())
        .or_insert_with(|| cached_bucket_usage_now(BucketUsageInfo::default()));

    entry.usage.size = entry.usage.size.saturating_add(new_size);
    if creates_new_version {
        entry.usage.versions_count = entry.usage.versions_count.saturating_add(1);
    }

    let now = SystemTime::now();
    entry.refreshed_at = now;
    entry.usage_updated_at = now;
    entry.dirty = true;
    entry.stale_snapshot_pending = false;
    entry.pending_scanner_position = None;
}

/// Fast in-memory update for successful object deletes.
pub async fn record_bucket_object_delete_memory(bucket: &str, deleted_size: u64, removed_current_object: bool) {
    ensure_bucket_usage_cached(bucket).await;

    let mut cache = memory_cache().write().await;
    let entry = cache
        .entry(bucket.to_string())
        .or_insert_with(|| cached_bucket_usage_now(BucketUsageInfo::default()));

    entry.usage.size = entry.usage.size.saturating_sub(deleted_size);
    entry.pending_negative_delta = entry.pending_negative_delta.saturating_add(deleted_size);
    if removed_current_object {
        entry.usage.objects_count = entry.usage.objects_count.saturating_sub(1);
        entry.usage.versions_count = entry.usage.versions_count.saturating_sub(1);
    }

    let now = SystemTime::now();
    entry.refreshed_at = now;
    entry.usage_updated_at = now;
    entry.dirty = true;
    entry.stale_snapshot_pending = false;
    entry.pending_scanner_position = None;
}

/// Fast in-memory update for successful delete marker creation.
pub async fn record_bucket_delete_marker_memory(bucket: &str) {
    ensure_bucket_usage_cached(bucket).await;

    let mut cache = memory_cache().write().await;
    let entry = cache
        .entry(bucket.to_string())
        .or_insert_with(|| cached_bucket_usage_now(BucketUsageInfo::default()));

    entry.usage.delete_markers_count = entry.usage.delete_markers_count.saturating_add(1);

    let now = SystemTime::now();
    entry.refreshed_at = now;
    entry.usage_updated_at = now;
    entry.dirty = true;
    entry.stale_snapshot_pending = false;
    entry.pending_scanner_position = None;
}

/// Get bucket usage from the authoritative cache for this topology.
async fn get_persisted_bucket_usage(bucket: &str) -> Option<u64> {
    let store = runtime_sources::object_store_handle()?;
    let info = load_data_usage_from_backend_cached(store).await.ok()?;
    info.buckets_usage.get(bucket).map(|usage| usage.size)
}

pub async fn get_bucket_usage_memory(bucket: &str) -> Option<u64> {
    // A process-local mutation cache is authoritative only when every request
    // is handled by this process. In a distributed deployment it contains an
    // absolute count derived from one node's requests, so applying it as the
    // cluster total can replace a complete snapshot with roughly one node's
    // share of the bucket.
    if runtime_sources::setup_is_dist_erasure().await {
        return get_persisted_bucket_usage(bucket).await;
    }

    update_usage_cache_if_needed().await;

    let cache = memory_cache().read().await;
    cache
        .get(bucket)
        .filter(|cached| cached.authoritative)
        .map(|cached| cached.usage.size.saturating_add(cached.pending_negative_delta))
}

async fn update_usage_cache_if_needed() {
    let ttl = Duration::from_secs(DATA_USAGE_CACHE_TTL_SECS);
    let double_ttl = ttl * 2;
    let now = SystemTime::now();

    let cache = memory_cache().read().await;
    let earliest_timestamp = cache.values().map(|cached| cached.refreshed_at).min();
    drop(cache);

    let age = match earliest_timestamp {
        Some(ts) => now.duration_since(ts).unwrap_or_default(),
        None => double_ttl,
    };

    if age < ttl {
        return;
    }

    let mut updating = cache_updating().write().await;
    if age < double_ttl {
        if *updating {
            return;
        }
        *updating = true;
        drop(updating);

        let updating_clone = (*cache_updating()).clone();
        let refresh_generation = usage_memory_generation();
        tokio::spawn(async move {
            if let Some(store) = runtime_sources::object_store_handle()
                && let Ok(data_usage_info) = load_data_usage_from_backend(store).await
            {
                replace_bucket_usage_memory_from_info_if_generation(&data_usage_info, Some(refresh_generation)).await;
            }
            let mut updating = updating_clone.write().await;
            *updating = false;
        });
        return;
    }

    for retry in 0..10 {
        if !*updating {
            break;
        }
        drop(updating);
        let delay = Duration::from_millis(1 << retry);
        tokio::time::sleep(delay).await;
        updating = cache_updating().write().await;
    }

    *updating = true;
    drop(updating);

    let refresh_generation = usage_memory_generation();
    if let Some(store) = runtime_sources::object_store_handle()
        && let Ok(data_usage_info) = load_data_usage_from_backend(store).await
    {
        replace_bucket_usage_memory_from_info_if_generation(&data_usage_info, Some(refresh_generation)).await;
    }

    let mut updating = cache_updating().write().await;
    *updating = false;
}

pub async fn replace_bucket_usage_memory_from_info(data_usage_info: &DataUsageInfo) {
    replace_bucket_usage_memory_from_info_if_generation(data_usage_info, None).await;
}

async fn replace_bucket_usage_memory_from_info_if_generation(data_usage_info: &DataUsageInfo, expected_generation: Option<u64>) {
    if !data_usage_info.is_complete_bucket_usage_snapshot() {
        return;
    }

    let usage_updated_at = data_usage_info_updated_at(data_usage_info);
    let snapshot_position = data_usage_info.scanner_epoch.zip(data_usage_info.scanner_cycle);
    let mut next_cache = HashMap::with_capacity(data_usage_info.buckets_usage.len());
    for (bucket, bucket_usage) in data_usage_info.buckets_usage.iter() {
        next_cache.insert(
            bucket.clone(),
            cached_bucket_usage_from_backend(bucket_usage.clone(), usage_updated_at, true),
        );
    }

    let mut cache = memory_cache().write().await;
    if expected_generation.is_some_and(|expected| usage_memory_generation() != expected) {
        return;
    }
    for (bucket, existing) in cache.iter() {
        match next_cache.entry(bucket.clone()) {
            Entry::Occupied(mut candidate) => {
                if let Some(preserved) = preserve_unknown_dirty_usage(existing, snapshot_position, usage_updated_at) {
                    candidate.insert(preserved);
                    continue;
                }
                if existing.authoritative && existing.usage_updated_at > usage_updated_at {
                    candidate.insert(existing.clone());
                    continue;
                }
                if existing.authoritative && existing.dirty && !bucket_usage_counts_match(&existing.usage, &candidate.get().usage)
                {
                    // A scanner snapshot can be saved after newer writes but still miss them if it listed the bucket earlier.
                    let mut preserved = existing.clone();
                    preserved.stale_snapshot_pending = true;
                    candidate.insert(preserved);
                }
            }
            Entry::Vacant(candidate) => {
                if let Some(preserved) = preserve_unknown_dirty_usage(existing, snapshot_position, usage_updated_at) {
                    candidate.insert(preserved);
                } else if existing.authoritative && existing.usage_updated_at > usage_updated_at {
                    candidate.insert(existing.clone());
                } else if existing.authoritative && existing.dirty {
                    let mut preserved = existing.clone();
                    preserved.stale_snapshot_pending = true;
                    candidate.insert(preserved);
                }
            }
        }
    }

    *cache = next_cache;
}

async fn apply_bucket_usage_memory_overlay_if_authoritative(data_usage_info: &mut DataUsageInfo, authoritative: bool) {
    if !authoritative {
        return;
    }

    let cache = memory_cache().read().await;
    if cache.is_empty() {
        return;
    }

    let persisted_update = data_usage_info.last_update;
    let mut changed = false;
    let mut added_bucket = false;

    for (bucket, cached) in cache.iter() {
        if !cached.authoritative {
            if cached.dirty {
                data_usage_info.usage_snapshot_complete = false;
            }
            continue;
        }
        if !cached.stale_snapshot_pending && persisted_update.is_some_and(|persisted| cached.usage_updated_at <= persisted) {
            continue;
        }

        added_bucket |= !data_usage_info.buckets_usage.contains_key(bucket);
        data_usage_info.buckets_usage.insert(bucket.clone(), cached.usage.clone());
        data_usage_info.bucket_sizes.insert(bucket.clone(), cached.usage.size);
        changed = true;
    }

    if changed {
        if added_bucket {
            data_usage_info.usage_snapshot_complete = false;
        }
        data_usage_info.buckets_count = data_usage_info.buckets_usage.len() as u64;
        data_usage_info.calculate_totals();
    }
}

pub async fn apply_bucket_usage_memory_overlay(data_usage_info: &mut DataUsageInfo) {
    let authoritative = !runtime_sources::setup_is_dist_erasure().await;
    apply_bucket_usage_memory_overlay_if_authoritative(data_usage_info, authoritative).await;
}

// Helper functions for DataUsageCache operations

/// How many times `load_data_usage_cache` tries a read that failed for a
/// transient reason before giving up.
const DATA_USAGE_CACHE_LOAD_ATTEMPTS: usize = 5;
const DATA_USAGE_CACHE_LOAD_BASE_DELAY: std::time::Duration = std::time::Duration::from_millis(100);
const DATA_USAGE_CACHE_LOAD_MAX_DELAY: std::time::Duration = std::time::Duration::from_millis(1_000);

/// Result of one attempt at reading a data-usage cache object.
enum DataUsageCacheRead {
    Loaded(DataUsageCache),
    /// The object is definitively not there, so retrying cannot turn the read
    /// into a hit.
    Absent,
}

/// True when the error means the cache object does not exist, as opposed to a
/// transient failure that is worth another attempt.
///
/// `SetDisks::get_object_reader` runs its failures through `to_object_err`,
/// which rewrites `FileNotFound` to `ObjectNotFound` and `VolumeNotFound` to
/// `BucketNotFound`, so those are the variants that actually arrive here. The
/// raw pair is matched too because callers reading through a different layer
/// can still surface it.
fn is_data_usage_cache_absent(err: &Error) -> bool {
    matches!(
        err,
        Error::FileNotFound | Error::VolumeNotFound | Error::ObjectNotFound(..) | Error::BucketNotFound(..)
    )
}

async fn read_data_usage_cache_object<S>(store: &S, key: &str) -> crate::error::Result<DataUsageCacheRead>
where
    S: EcstoreObjectIO,
{
    use crate::disk::RUSTFS_META_BUCKET;
    use crate::object_api::ObjectOptions;
    use http::HeaderMap;

    match store
        .get_object_reader(
            RUSTFS_META_BUCKET,
            key,
            None,
            HeaderMap::new(),
            &ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
    {
        // A cache object that fails to decode is treated as absent rather than
        // as an error: a corrupt cache should not stall the caller, and the
        // next scanner pass rewrites it.
        Ok(mut reader) => Ok(DataUsageCache::unmarshal(&reader.read_all().await?)
            .map(DataUsageCacheRead::Loaded)
            .unwrap_or(DataUsageCacheRead::Absent)),
        Err(err) if is_data_usage_cache_absent(&err) => Ok(DataUsageCacheRead::Absent),
        Err(err) => Err(err),
    }
}

/// Load a data-usage cache, preferring the prefixed key and falling back to the
/// legacy unprefixed one.
///
/// A cache that is absent under both keys yields an empty cache; a transient
/// read failure is retried with capped, jittered backoff and surfaces as an
/// error once the attempts are exhausted.
pub(crate) async fn load_data_usage_cache<S>(store: &S, name: &str) -> crate::error::Result<DataUsageCache>
where
    S: EcstoreObjectIO,
{
    use crate::disk::BUCKET_META_PREFIX;
    use std::path::Path;

    let prefixed = Path::new(BUCKET_META_PREFIX).join(name);
    let prefixed = prefixed
        .to_str()
        .ok_or_else(|| Error::other("data usage cache path is not valid UTF-8"))?
        .to_owned();

    rustfs_utils::retry::retry_with_backoff(
        || async {
            match read_data_usage_cache_object(store, &prefixed).await? {
                DataUsageCacheRead::Loaded(cache) => Ok(cache),
                DataUsageCacheRead::Absent => match read_data_usage_cache_object(store, name).await? {
                    DataUsageCacheRead::Loaded(cache) => Ok(cache),
                    DataUsageCacheRead::Absent => Ok(DataUsageCache::default()),
                },
            }
        },
        DATA_USAGE_CACHE_LOAD_ATTEMPTS,
        DATA_USAGE_CACHE_LOAD_BASE_DELAY,
        DATA_USAGE_CACHE_LOAD_MAX_DELAY,
    )
    .await
}

/// Persist the current in-memory compression total to the backend.
/// Resets the debounce counter so the next auto-persist won't fire
/// immediately after this manual flush (intended for shutdown paths).
/// Storage will not be triggered when uninitialized.
pub async fn store_compression_total_in_backend() {
    let snapshot = {
        let mut guard = COMPRESSION_TOTAL_MEMORY_CACHE.write().await;
        let Some(state) = guard.as_mut() else {
            return;
        };
        if !state.inited {
            return;
        }
        state.ops_since_persist = 0;
        state.last_persist = tokio::time::Instant::now();
        state.info.clone()
    };
    try_flush_compression_total(&snapshot).await;
}

/// Load compression total info from backend storage
#[instrument(skip(store))]
pub async fn load_compression_total_from_backend(store: Arc<ECStore>) -> Result<CompressionTotalInfo, Error> {
    let buf: Vec<u8> = match read_config(store.clone(), &DATA_COMPRESSION_TOTAL_NAME_PATH).await {
        Ok(data) => data,
        Err(e) => {
            if e == Error::ConfigNotFound {
                return Ok(CompressionTotalInfo::default());
            }
            let reason = classify_system_path_failure_reason(&e);
            record_system_path_failure("compression_total", "read_primary", reason);
            error!(
                path_kind = "compression_total",
                operation = "read_primary",
                reason,
                object = %DATA_COMPRESSION_TOTAL_NAME_PATH.as_str(),
                error = %e,
                "system path read failed"
            );
            return Err(Error::other(e));
        }
    };
    let compression_total: CompressionTotalInfo =
        serde_json::from_slice(&buf).map_err(|e| Error::other(format!("Failed to deserialize compression total info: {e}")))?;

    info!(
        "Loaded compression total info: original={}, compressed={}, operations={}",
        compression_total.original_bytes_total,
        compression_total.compressed_bytes_total,
        compression_total.compression_operations_total
    );

    Ok(compression_total)
}

pub async fn load_compression_total_from_memory() -> Option<CompressionTotalInfo> {
    COMPRESSION_TOTAL_MEMORY_CACHE.read().await.as_ref().map(|s| s.info.clone())
}

/// Record a compression operation. Accumulates totals in memory and triggers a
/// background persist to backend after every `COMPRESSION_PERSIST_BATCH_SIZE`
/// operations, gated by a minimum interval to avoid excessive IO under high
/// throughput.
pub async fn record_compression_total_memory(original_size: u64, compressed_size: u64) {
    let mut guard = COMPRESSION_TOTAL_MEMORY_CACHE.write().await;
    let Some(state) = guard.as_mut() else {
        error!("compression total memory cache not initialized, discarding record");
        return;
    };
    // Compression totals are only recorded while this cache is initialized.
    if !state.inited {
        return;
    }
    state.info.original_bytes_total = state.info.original_bytes_total.saturating_add(original_size);
    state.info.compressed_bytes_total = state.info.compressed_bytes_total.saturating_add(compressed_size);
    state.info.compression_operations_total = state.info.compression_operations_total.saturating_add(1);
    state.ops_since_persist = state.ops_since_persist.saturating_add(1);
    if state.ops_since_persist >= COMPRESSION_PERSIST_BATCH_SIZE
        || state.last_persist.elapsed() >= COMPRESSION_PERSIST_MIN_INTERVAL
    {
        state.ops_since_persist = 0;
        state.last_persist = tokio::time::Instant::now();
        // Fire-and-forget: hold no lock during IO
        let info = state.info.clone();
        drop(guard);
        tokio::spawn(async move {
            try_flush_compression_total(&info).await;
        });
    }
}

/// Persist the given `CompressionTotalInfo` snapshot to backend storage.
/// Used by both the debounce path and the manual flush path.
async fn try_flush_compression_total(info: &CompressionTotalInfo) {
    let Some(store) = runtime_sources::object_store_handle() else {
        error!("object store not initialized, skipping compression total persist");
        return;
    };
    match serde_json::to_vec(info) {
        Ok(data) => {
            if let Err(e) = crate::config::com::save_config(store.clone(), &DATA_COMPRESSION_TOTAL_NAME_PATH, data).await {
                error!("Failed to persist compression total to backend: {}", e);
            }
        }
        Err(e) => {
            error!("Failed to serialize compression total info: {}", e);
        }
    }
}

/// Initialize the compression total memory cache from backend storage.
/// Should be called at startup after the store is available if compression is enabled.
/// Compression totals are only recorded while this cache is initialized, so this must
/// be called before any compression operations are recorded.
#[instrument(skip(store))]
pub async fn init_compression_total_memory_from_backend(store: Arc<ECStore>) {
    let info = match load_compression_total_from_backend(store).await {
        Ok(info) => info,
        Err(e) => {
            // If load fails (e.g. ConfigNotFound or corrupt backup), start from zero.
            info!("Failed to init compression total from backend, starting from zero: {}", e);
            CompressionTotalInfo::default()
        }
    };
    *COMPRESSION_TOTAL_MEMORY_CACHE.write().await = Some(CompressionTotalState {
        info,
        ops_since_persist: 0,
        last_persist: tokio::time::Instant::now(),
        inited: true,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::endpoints::EndpointServerPools;
    use crate::runtime::instance::InstanceContext;
    use crate::storage_api_contracts::object::ObjectIO as _;
    use rustfs_data_usage::BucketUsageInfo;
    use rustfs_lock::{LocalClient, LockRequest, LockType, NamespaceLock, ObjectKey};
    use serial_test::serial;
    use std::io::Cursor;
    use std::sync::Arc;
    use tokio::{io::AsyncReadExt, sync::Mutex};

    #[derive(Debug, Default)]
    struct UsageCasState {
        object: Option<(Vec<u8>, u64)>,
        backup_object: Option<(Vec<u8>, u64)>,
        observed_object: Option<(Vec<u8>, u64)>,
        legacy_object: Option<(Vec<u8>, u64)>,
        legacy_backup_object: Option<(Vec<u8>, u64)>,
        interleaving_snapshot: Option<Vec<u8>>,
        interleaving_backup_snapshot: Option<Vec<u8>>,
        interleaving_legacy_snapshot: Option<Vec<u8>>,
        interleaving_legacy_backup_snapshot: Option<Vec<u8>>,
        error_after_commit_put: Option<usize>,
        advance_time_on_put: Option<Duration>,
        advance_time_after_get: Option<(UsageObjectSlot, Duration)>,
        advance_publication_epoch_after_get: Option<(UsageObjectSlot, Arc<InstanceContext>)>,
        advance_time_before_put: Option<(usize, Duration)>,
        advance_time_after_put: Option<(usize, Duration)>,
        put_count: usize,
    }

    #[derive(Debug, Default)]
    struct UsageCasStore {
        state: Mutex<UsageCasState>,
    }

    #[async_trait::async_trait]
    impl ObservedDataUsageSnapshotCleanup for UsageCasStore {
        async fn delete_observed_data_usage_snapshot(&self, revision: &str) -> Result<(), Error> {
            let mut state = self.state.lock().await;
            let current = state.observed_object.as_ref().ok_or(Error::FileNotFound)?.1;
            if revision != format!("usage-{current}") {
                return Err(Error::PreconditionFailed);
            }
            state.observed_object = None;
            Ok(())
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum UsageObjectSlot {
        Primary,
        Backup,
        Observed,
        LegacyPrimary,
        LegacyBackup,
    }

    #[async_trait::async_trait]
    impl crate::storage_api_contracts::object::ObjectIO for UsageCasStore {
        type Error = Error;
        type RangeSpec = crate::storage_api_contracts::range::HTTPRangeSpec;
        type HeaderMap = http::HeaderMap;
        type ObjectOptions = ObjectOptions;
        type ObjectInfo = ObjectInfo;
        type GetObjectReader = crate::object_api::GetObjectReader;
        type PutObjectReader = PutObjReader;

        async fn get_object_reader(
            &self,
            bucket: &str,
            object: &str,
            _range: Option<Self::RangeSpec>,
            _h: Self::HeaderMap,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::GetObjectReader, Self::Error> {
            if bucket != RUSTFS_META_BUCKET {
                return Err(Error::FileNotFound);
            }
            let slot = match object {
                object if object == DATA_USAGE_OBJ_NAME_PATH.as_str() => UsageObjectSlot::Primary,
                object if object == DATA_USAGE_OBJ_BACKUP_PATH.as_str() => UsageObjectSlot::Backup,
                object if object == DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str() => UsageObjectSlot::Observed,
                object if object == LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str() => UsageObjectSlot::LegacyPrimary,
                object if object == LEGACY_DATA_USAGE_OBJ_BACKUP_PATH.as_str() => UsageObjectSlot::LegacyBackup,
                _ => return Err(Error::FileNotFound),
            };
            let mut state = self.state.lock().await;
            let stored = match slot {
                UsageObjectSlot::Primary => &state.object,
                UsageObjectSlot::Backup => &state.backup_object,
                UsageObjectSlot::Observed => &state.observed_object,
                UsageObjectSlot::LegacyPrimary => &state.legacy_object,
                UsageObjectSlot::LegacyBackup => &state.legacy_backup_object,
            };
            let (data, revision) = stored.clone().ok_or(Error::FileNotFound)?;
            let advance = match state.advance_time_after_get {
                Some((expected_slot, duration)) if expected_slot == slot => {
                    state.advance_time_after_get = None;
                    Some(duration)
                }
                _ => None,
            };
            let advance_publication_epoch = match state.advance_publication_epoch_after_get {
                Some((expected_slot, ref ctx)) if expected_slot == slot => {
                    let ctx = Arc::clone(ctx);
                    state.advance_publication_epoch_after_get = None;
                    Some(ctx)
                }
                _ => None,
            };
            drop(state);
            if let Some(duration) = advance {
                tokio::time::advance(duration).await;
            }
            if let Some(ctx) = advance_publication_epoch {
                ctx.advance_data_movement_operation_epoch();
            }
            Ok(crate::object_api::GetObjectReader {
                stream: Box::new(Cursor::new(data)),
                object_info: ObjectInfo {
                    etag: Some(format!("usage-{revision}")),
                    ..Default::default()
                },
                buffered_body: None,
                body_source: Default::default(),
            })
        }

        async fn put_object(
            &self,
            bucket: &str,
            object: &str,
            data: &mut Self::PutObjectReader,
            opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
            if bucket != RUSTFS_META_BUCKET {
                return Err(Error::FileNotFound);
            }
            let slot = match object {
                object if object == DATA_USAGE_OBJ_NAME_PATH.as_str() => UsageObjectSlot::Primary,
                object if object == DATA_USAGE_OBJ_BACKUP_PATH.as_str() => UsageObjectSlot::Backup,
                object if object == DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str() => UsageObjectSlot::Observed,
                object if object == LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str() => UsageObjectSlot::LegacyPrimary,
                object if object == LEGACY_DATA_USAGE_OBJ_BACKUP_PATH.as_str() => UsageObjectSlot::LegacyBackup,
                _ => return Err(Error::FileNotFound),
            };
            let mut buf = Vec::new();
            data.stream.read_to_end(&mut buf).await?;

            let mut state = self.state.lock().await;
            state.put_count += 1;
            let put_count = state.put_count;
            if let Some(duration) = state.advance_time_on_put.take() {
                drop(state);
                tokio::time::advance(duration).await;
                state = self.state.lock().await;
            }
            if let Some((expected_put, duration)) = state.advance_time_before_put
                && expected_put == put_count
            {
                state.advance_time_before_put = None;
                drop(state);
                tokio::time::advance(duration).await;
                state = self.state.lock().await;
            }
            if slot == UsageObjectSlot::Primary
                && let Some(interleaving) = state.interleaving_snapshot.take()
            {
                let revision = state.object.as_ref().map_or(1, |(_, revision)| revision + 1);
                state.object = Some((interleaving, revision));
            }
            if slot == UsageObjectSlot::Backup
                && let Some(interleaving) = state.interleaving_backup_snapshot.take()
            {
                let revision = state.backup_object.as_ref().map_or(1, |(_, revision)| revision + 1);
                state.backup_object = Some((interleaving, revision));
            }
            if slot == UsageObjectSlot::Observed {
                return Err(Error::other("observed test fixture writes are injected directly"));
            }
            if slot == UsageObjectSlot::LegacyPrimary
                && let Some(interleaving) = state.interleaving_legacy_snapshot.take()
            {
                let revision = state.legacy_object.as_ref().map_or(1, |(_, revision)| revision + 1);
                state.legacy_object = Some((interleaving, revision));
            }
            if slot == UsageObjectSlot::LegacyBackup
                && let Some(interleaving) = state.interleaving_legacy_backup_snapshot.take()
            {
                let revision = state.legacy_backup_object.as_ref().map_or(1, |(_, revision)| revision + 1);
                state.legacy_backup_object = Some((interleaving, revision));
            }
            let current_revision = match slot {
                UsageObjectSlot::Primary => state.object.as_ref(),
                UsageObjectSlot::Backup => state.backup_object.as_ref(),
                UsageObjectSlot::Observed => state.observed_object.as_ref(),
                UsageObjectSlot::LegacyPrimary => state.legacy_object.as_ref(),
                UsageObjectSlot::LegacyBackup => state.legacy_backup_object.as_ref(),
            }
            .map(|(_, revision)| *revision);
            if let Some(preconditions) = &opts.http_preconditions {
                if preconditions
                    .if_none_match
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty())
                    && current_revision.is_some()
                {
                    return Err(Error::PreconditionFailed);
                }
                if let Some(expected) = preconditions
                    .if_match
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                {
                    let actual = current_revision.map(|revision| format!("usage-{revision}"));
                    if actual.as_deref() != Some(expected.trim_matches('"')) {
                        return Err(Error::PreconditionFailed);
                    }
                }
            }

            let revision = current_revision.unwrap_or_default() + 1;
            match slot {
                UsageObjectSlot::Primary => state.object = Some((buf, revision)),
                UsageObjectSlot::Backup => state.backup_object = Some((buf, revision)),
                UsageObjectSlot::Observed => state.observed_object = Some((buf, revision)),
                UsageObjectSlot::LegacyPrimary => state.legacy_object = Some((buf, revision)),
                UsageObjectSlot::LegacyBackup => state.legacy_backup_object = Some((buf, revision)),
            }
            if state.error_after_commit_put == Some(put_count) {
                return Err(Error::other("injected post-commit failure"));
            }
            if let Some((expected_put, duration)) = state.advance_time_after_put
                && expected_put == put_count
            {
                state.advance_time_after_put = None;
                drop(state);
                tokio::time::advance(duration).await;
            }
            Ok(ObjectInfo {
                etag: Some(format!("usage-{revision}")),
                ..Default::default()
            })
        }
    }

    /// Minimal ObjectIO backing `load_data_usage_cache` tests: records the keys
    /// read and fails the first N reads with a transient (non-absence) error.
    #[derive(Debug, Default)]
    struct UsageCacheReadStore {
        transient_failures: Mutex<usize>,
        reads: Mutex<Vec<String>>,
    }

    impl UsageCacheReadStore {
        fn failing_first(n: usize) -> Self {
            Self {
                transient_failures: Mutex::new(n),
                reads: Mutex::new(Vec::new()),
            }
        }

        async fn read_keys(&self) -> Vec<String> {
            self.reads.lock().await.clone()
        }
    }

    #[async_trait::async_trait]
    impl crate::storage_api_contracts::object::ObjectIO for UsageCacheReadStore {
        type Error = Error;
        type RangeSpec = crate::storage_api_contracts::range::HTTPRangeSpec;
        type HeaderMap = http::HeaderMap;
        type ObjectOptions = ObjectOptions;
        type ObjectInfo = ObjectInfo;
        type GetObjectReader = crate::object_api::GetObjectReader;
        type PutObjectReader = PutObjReader;

        async fn get_object_reader(
            &self,
            _bucket: &str,
            object: &str,
            _range: Option<Self::RangeSpec>,
            _h: Self::HeaderMap,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::GetObjectReader, Self::Error> {
            self.reads.lock().await.push(object.to_string());
            let mut remaining = self.transient_failures.lock().await;
            if *remaining > 0 {
                *remaining -= 1;
                return Err(Error::other("transient read failure"));
            }
            // `SetDisks::get_object_reader` reports a missing object through
            // `to_object_err`, so the absence that reaches the caller is
            // `ObjectNotFound`, not the raw `FileNotFound`.
            Err(Error::ObjectNotFound(RUSTFS_META_BUCKET.to_string(), object.to_string()))
        }

        async fn put_object(
            &self,
            _bucket: &str,
            _object: &str,
            _data: &mut Self::PutObjectReader,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
            unimplemented!("load_data_usage_cache never writes")
        }
    }

    fn prefixed_usage_key(name: &str) -> String {
        std::path::Path::new(crate::disk::BUCKET_META_PREFIX)
            .join(name)
            .to_str()
            .expect("utf-8 path")
            .to_string()
    }

    fn build_publication_store(ctx: Arc<InstanceContext>) -> Arc<ECStore> {
        let endpoint_pools = EndpointServerPools::default();
        Arc::new(ECStore {
            id: uuid::Uuid::new_v4(),
            disk_map: HashMap::new(),
            pools: Vec::new(),
            peer_sys: crate::cluster::rpc::S3PeerSys::new_with_instance_ctx(&endpoint_pools, ctx.clone()),
            pool_meta: RwLock::new(crate::core::pools::PoolMeta::default()),
            rebalance_meta: RwLock::new(None),
            decommission_cancelers: RwLock::new(Vec::new()),
            start_gate: TokioMutex::new(()),
            pool_meta_save_gate: TokioMutex::new(()),
            ctx,
            bucket_fence_registry: Arc::default(),
        })
    }

    #[test]
    fn data_usage_cache_absence_covers_the_variants_that_actually_arrive() {
        // `to_object_err` rewrites the raw storage variants before they reach
        // `load_data_usage_cache`; classifying only the raw pair would treat a
        // missing cache as a transient failure and retry it.
        assert!(is_data_usage_cache_absent(&Error::ObjectNotFound(
            "bucket".to_string(),
            "object".to_string()
        )));
        assert!(is_data_usage_cache_absent(&Error::BucketNotFound("bucket".to_string())));
        assert!(is_data_usage_cache_absent(&Error::FileNotFound));
        assert!(is_data_usage_cache_absent(&Error::VolumeNotFound));

        assert!(!is_data_usage_cache_absent(&Error::other("transient read failure")));
        assert!(!is_data_usage_cache_absent(&Error::DiskNotFound));
    }

    #[tokio::test]
    async fn load_data_usage_cache_treats_absence_as_an_empty_cache_without_retrying() {
        let name = "usage-cache";
        let store = UsageCacheReadStore::default();

        let cache = load_data_usage_cache(&store, name).await.expect("absence is not an error");

        assert!(cache.cache.is_empty());
        assert_eq!(
            store.read_keys().await,
            vec![prefixed_usage_key(name), name.to_string()],
            "the prefixed key is tried first, then the legacy one, and neither absence is retried"
        );
    }

    #[tokio::test]
    async fn load_data_usage_cache_retries_a_transient_failure() {
        let name = "usage-cache";
        // Two transient failures, then the object reads as absent.
        let store = UsageCacheReadStore::failing_first(2);

        let cache = load_data_usage_cache(&store, name)
            .await
            .expect("retry should reach the absent read");

        assert!(cache.cache.is_empty());
        assert_eq!(
            store.read_keys().await,
            vec![
                prefixed_usage_key(name),
                prefixed_usage_key(name),
                prefixed_usage_key(name),
                name.to_string(),
            ],
            "a transient failure retries the prefixed read rather than falling through"
        );
    }

    #[tokio::test]
    async fn load_data_usage_cache_surfaces_a_persistent_failure() {
        let name = "usage-cache";
        let store = UsageCacheReadStore::failing_first(usize::MAX);

        let err = load_data_usage_cache(&store, name)
            .await
            .expect_err("an exhausted retry must not be reported as an empty cache");

        assert!(err.to_string().contains("transient read failure"));
        assert_eq!(
            store.read_keys().await.len(),
            DATA_USAGE_CACHE_LOAD_ATTEMPTS,
            "every attempt is used before giving up"
        );
    }

    async fn clear_usage_memory_cache_for_test() {
        memory_cache().write().await.clear();
        *cache_updating().write().await = false;
    }

    fn data_usage_info_for_test(bucket: &str, objects_count: u64, size: u64, last_update: SystemTime) -> DataUsageInfo {
        let mut info = DataUsageInfo {
            last_update: Some(last_update),
            ..Default::default()
        };
        info.buckets_usage.insert(
            bucket.to_string(),
            BucketUsageInfo {
                objects_count,
                versions_count: objects_count,
                size,
                ..Default::default()
            },
        );
        info.usage_snapshot_complete = true;
        info.bucket_sizes.insert(bucket.to_string(), size);
        info.buckets_count = info.buckets_usage.len() as u64;
        info.calculate_totals();
        info
    }

    fn usage_with_last_update(last_update: Option<SystemTime>) -> DataUsageInfo {
        DataUsageInfo {
            last_update,
            ..Default::default()
        }
    }

    #[test]
    fn stale_data_usage_persist_reason_allows_newer_incoming() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let incoming = usage_with_last_update(Some(now));
        let existing = usage_with_last_update(Some(now - Duration::from_secs(60)));
        assert_eq!(stale_data_usage_persist_reason(&incoming, &existing, now), None);
    }

    #[test]
    fn stale_data_usage_persist_reason_skips_older_or_equal_incoming() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let existing = usage_with_last_update(Some(now - Duration::from_secs(60)));

        let older = usage_with_last_update(Some(now - Duration::from_secs(120)));
        assert_eq!(
            stale_data_usage_persist_reason(&older, &existing, now),
            Some("older_or_equal_last_update")
        );

        let equal = usage_with_last_update(existing.last_update);
        assert_eq!(
            stale_data_usage_persist_reason(&equal, &existing, now),
            Some("older_or_equal_last_update")
        );
    }

    #[test]
    fn stale_data_usage_persist_reason_allows_save_when_existing_is_future_dated() {
        // Existing snapshot timestamp beyond the clock tolerance is untrustworthy
        // (clock step-back / slower-clock leader): the save must be allowed even
        // though incoming <= existing, otherwise usage stats freeze forever.
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let existing =
            usage_with_last_update(Some(now + rustfs_data_usage::USAGE_LAST_UPDATE_FUTURE_TOLERANCE + Duration::from_secs(1)));
        let incoming = usage_with_last_update(Some(now));
        assert_eq!(stale_data_usage_persist_reason(&incoming, &existing, now), None);
    }

    #[test]
    fn stale_data_usage_persist_reason_skips_at_exact_tolerance_boundary() {
        // Exactly at now + tolerance is still within the trusted window.
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let existing = usage_with_last_update(Some(now + rustfs_data_usage::USAGE_LAST_UPDATE_FUTURE_TOLERANCE));
        let incoming = usage_with_last_update(Some(now));
        assert_eq!(
            stale_data_usage_persist_reason(&incoming, &existing, now),
            Some("older_or_equal_last_update")
        );
    }

    #[test]
    fn stale_data_usage_persist_reason_preserves_none_handling() {
        // This call site (unlike the scanner sibling) allows saves when either
        // timestamp is missing — pin that behavior.
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);

        let incoming_none = usage_with_last_update(None);
        let existing_some = usage_with_last_update(Some(now - Duration::from_secs(60)));
        assert_eq!(stale_data_usage_persist_reason(&incoming_none, &existing_some, now), None);

        let incoming_some = usage_with_last_update(Some(now));
        let existing_none = usage_with_last_update(None);
        assert_eq!(stale_data_usage_persist_reason(&incoming_some, &existing_none, now), None);

        let both_none = usage_with_last_update(None);
        assert_eq!(stale_data_usage_persist_reason(&both_none, &usage_with_last_update(None), now), None);
    }

    #[test]
    fn backup_stale_guard_allows_equal_snapshot_to_repair_primary() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let existing = usage_with_last_update(Some(now - Duration::from_secs(60)));
        let equal = usage_with_last_update(existing.last_update);
        let older = usage_with_last_update(Some(now - Duration::from_secs(120)));

        assert_eq!(
            stale_data_usage_persist_reason_for_source(&equal, &existing, UsageSnapshotSource::Backup, now),
            None
        );
        assert_eq!(
            stale_data_usage_persist_reason_for_source(&older, &existing, UsageSnapshotSource::Backup, now),
            Some("older_or_equal_last_update")
        );
    }

    fn snapshot_bytes(bucket: &str) -> Vec<u8> {
        let info = data_usage_info_for_test(bucket, 3, 42, SystemTime::UNIX_EPOCH);
        serde_json::to_vec(&info).expect("test snapshot must serialize")
    }

    #[test]
    fn data_usage_backup_path_appends_suffix_to_primary() {
        assert_eq!(DATA_USAGE_OBJ_BACKUP_PATH.as_str(), format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str()));
    }

    fn assert_snapshot_bucket(info: &DataUsageInfo, bucket: &str) {
        assert!(
            info.buckets_usage.contains_key(bucket),
            "expected snapshot for bucket {bucket}, got buckets {:?}",
            info.buckets_usage.keys().collect::<Vec<_>>()
        );
    }

    #[test]
    fn parse_snapshot_error_does_not_include_payload_value() {
        let err =
            parse_usage_snapshot(br#"{"buckets_count":"secret-marker"}"#).expect_err("an invalid field type must fail decoding");
        assert!(!err.to_string().contains("secret-marker"));
    }

    #[tokio::test]
    async fn legacy_snapshot_bucket_usage_is_unknown_without_completeness_marker() {
        let mut legacy = data_usage_info_for_test("control", 10, 10_285, SystemTime::UNIX_EPOCH);
        legacy.usage_snapshot_complete = true;
        legacy.buckets_usage.insert("large".to_string(), BucketUsageInfo::default());
        legacy.bucket_sizes.insert("large".to_string(), 0);
        legacy.buckets_count = 2;

        let (normalized, degraded_baseline) = normalize_loaded_data_usage(legacy, false).await;

        assert_eq!(normalized.buckets_count, 0);
        assert!(normalized.buckets_usage.is_empty());
        assert!(normalized.bucket_sizes.is_empty());
        assert_eq!(normalized.objects_total_count, 0);
        assert_eq!(normalized.objects_total_size, 0);
        assert!(!normalized.usage_snapshot_complete);
        // Issue #5716: the discarded sizes must survive as the degraded
        // quota-admission baseline.
        assert_eq!(degraded_baseline.get("control").copied(), Some(10_285));
        assert_eq!(degraded_baseline.get("large").copied(), Some(0));
    }

    #[test]
    fn legacy_snapshot_json_defaults_completeness_to_unknown() {
        let info = data_usage_info_for_test("legacy", 1, 42, SystemTime::UNIX_EPOCH);
        let mut value = serde_json::to_value(info).expect("serialize data usage fixture");
        let object = value.as_object_mut().expect("data usage fixture should be a JSON object");
        object.remove("usage_snapshot_complete");

        let decoded: DataUsageInfo = serde_json::from_value(value).expect("deserialize legacy data usage fixture");

        assert!(!decoded.usage_snapshot_complete);
    }

    #[test]
    fn complete_snapshot_json_is_additive_for_legacy_readers() {
        #[derive(serde::Deserialize)]
        struct LegacyUsageReader {
            buckets_count: u64,
        }

        let info = data_usage_info_for_test("current", 1, 42, SystemTime::UNIX_EPOCH);
        let encoded = serde_json::to_vec(&info).expect("serialize current data usage fixture");
        let legacy: LegacyUsageReader =
            serde_json::from_slice(&encoded).expect("legacy JSON reader should ignore additive fields");

        assert_eq!(legacy.buckets_count, 1);
    }

    #[tokio::test]
    async fn complete_snapshot_preserves_confirmed_empty_bucket() {
        let mut info = data_usage_info_for_test("control", 10, 10_285, SystemTime::UNIX_EPOCH);
        info.buckets_usage.insert("empty".to_string(), BucketUsageInfo::default());
        info.buckets_count = 2;

        let (normalized, _) = normalize_loaded_data_usage(info, true).await;

        assert_eq!(normalized.buckets_count, 2);
        assert!(normalized.usage_snapshot_complete);
        assert_eq!(
            normalized
                .buckets_usage
                .get("control")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((10, 10_285))
        );
        assert_eq!(
            normalized
                .buckets_usage
                .get("empty")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((0, 0))
        );
    }

    #[tokio::test]
    async fn malformed_complete_snapshot_is_rejected_as_a_whole() {
        let mut info = data_usage_info_for_test("control", 10, 10_285, SystemTime::UNIX_EPOCH);
        info.buckets_usage.insert(
            "partial".to_string(),
            BucketUsageInfo {
                objects_count: 1_502,
                versions_count: 1_502,
                size: 196_870_144,
                ..Default::default()
            },
        );
        info.bucket_sizes.insert("partial".to_string(), 196_870_144);
        info.buckets_count = 1;

        let (normalized, _) = normalize_loaded_data_usage(info, true).await;

        assert_eq!(normalized.buckets_count, 0);
        assert!(!normalized.buckets_usage.contains_key("control"));
        assert!(!normalized.buckets_usage.contains_key("partial"));
        assert!(!normalized.bucket_sizes.contains_key("partial"));
        assert!(!normalized.usage_snapshot_complete);
    }

    #[tokio::test]
    async fn completeness_marker_does_not_fabricate_a_missing_bucket() {
        let mut info = data_usage_info_for_test("control", 10, 10_285, SystemTime::UNIX_EPOCH);
        info.buckets_count = 2;

        assert!(!data_usage_contains_bucket(&info, "missing"));
        let (normalized, _) = normalize_loaded_data_usage(info, true).await;

        assert!(!normalized.usage_snapshot_complete);
        assert!(normalized.buckets_usage.is_empty());
        assert!(!normalized.buckets_usage.contains_key("missing"));
    }

    #[tokio::test]
    async fn complete_empty_snapshot_remains_authoritative() {
        let (normalized, _) = normalize_loaded_data_usage(
            DataUsageInfo {
                last_update: Some(SystemTime::UNIX_EPOCH),
                usage_snapshot_complete: true,
                ..Default::default()
            },
            true,
        )
        .await;

        assert!(normalized.usage_snapshot_complete);
        assert_eq!(normalized.buckets_count, 0);
        assert!(normalized.buckets_usage.is_empty());
    }

    #[test]
    fn admin_snapshot_selection_requires_the_current_authoritative_baseline() {
        let authoritative = DataUsageInfo {
            last_update: Some(SystemTime::UNIX_EPOCH),
            scanner_epoch: Some(4),
            scanner_cycle: Some(10),
            usage_snapshot_complete: true,
            ..Default::default()
        };
        let observed = DataUsageInfo {
            last_update: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1)),
            scanner_epoch: Some(4),
            scanner_cycle: Some(11),
            usage_snapshot_complete: true,
            usage_snapshot_converged: Some(false),
            usage_snapshot_authoritative_baseline: Some(authoritative.snapshot_identity()),
            ..Default::default()
        };

        let (selected, _) = select_admin_data_usage_snapshot(authoritative.clone(), true, Some(observed.clone()));
        assert_eq!(selected.usage_snapshot_converged, Some(false));

        let mut namespace_changed = authoritative;
        namespace_changed.last_update = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(2));
        let (selected, _) = select_admin_data_usage_snapshot(namespace_changed, true, Some(observed));
        assert_eq!(selected.usage_snapshot_converged, Some(true));
    }

    #[test]
    fn persisted_authoritative_stalls_but_memory_overlay_remains_visible() {
        let authoritative = DataUsageInfo {
            last_update: Some(SystemTime::UNIX_EPOCH),
            scanner_epoch: Some(4),
            scanner_cycle: Some(10),
            usage_snapshot_complete: true,
            ..Default::default()
        };
        let mut partial = authoritative.clone();
        partial.last_update = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1));
        partial.scanner_cycle = Some(11);
        partial.usage_snapshot_complete = false;
        partial.usage_snapshot_partial = true;
        partial.usage_snapshot_converged = Some(false);
        partial.usage_snapshot_authoritative_baseline = Some(authoritative.snapshot_identity());
        partial.usage_snapshot_set_states = vec![rustfs_data_usage::DataUsageSnapshotSetState {
            pool_index: 0,
            set_index: 0,
            scanner_cycle: Some(10),
            scanner_epoch: Some(4),
            scan_plan_digest: Some([1; 32]),
            complete: false,
            tombstone: false,
        }];
        partial.buckets_usage.insert(
            "bucket".to_string(),
            BucketUsageInfo {
                size: 100,
                ..Default::default()
            },
        );
        partial.buckets_count = 1;

        let (selected, _) = select_admin_data_usage_snapshot(authoritative, true, Some(partial));
        assert!(selected.usage_snapshot_partial);
        assert_eq!(selected.buckets_usage.get("bucket").map(|usage| usage.size), Some(100));
    }

    #[tokio::test]
    async fn authoritative_save_cleanup_removes_observed_snapshot_best_effort() {
        let store = UsageCasStore::default();
        let authoritative = data_usage_info_for_test("bucket", 1, 10, SystemTime::UNIX_EPOCH + Duration::from_secs(2));
        let stale_observed = DataUsageInfo {
            last_update: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1)),
            scanner_epoch: Some(4),
            scanner_cycle: Some(10),
            usage_snapshot_complete: true,
            usage_snapshot_converged: Some(false),
            ..Default::default()
        };
        store.state.lock().await.observed_object =
            Some((serde_json::to_vec(&stale_observed).expect("observed snapshot should encode"), 1));

        cleanup_observed_data_usage_after_authoritative_save(&store, &authoritative).await;
        assert!(store.state.lock().await.observed_object.is_none());

        cleanup_observed_data_usage_after_authoritative_save(&store, &authoritative).await;
        assert!(store.state.lock().await.observed_object.is_none());

        let newer_observed = DataUsageInfo {
            last_update: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(3)),
            scanner_epoch: Some(4),
            scanner_cycle: Some(11),
            usage_snapshot_complete: true,
            usage_snapshot_converged: Some(false),
            usage_snapshot_authoritative_baseline: Some(authoritative.snapshot_identity()),
            ..Default::default()
        };
        store.state.lock().await.observed_object =
            Some((serde_json::to_vec(&newer_observed).expect("observed snapshot should encode"), 2));

        cleanup_observed_data_usage_after_authoritative_save(&store, &authoritative).await;
        assert!(store.state.lock().await.observed_object.is_some());
    }

    #[test]
    #[serial]
    fn cached_snapshot_failure_is_reused_until_ttl_expires() {
        let loaded_at = tokio::time::Instant::now();
        let mut cache = None;
        let refresh_generation = data_usage_snapshot_generation();

        let first = cache_data_usage_snapshot_result(
            &mut cache,
            Err(Error::ErasureReadQuorum),
            loaded_at,
            refresh_generation,
            data_usage_snapshot_generation(),
        )
        .expect("an uninterrupted refresh should populate the cache");
        assert!(matches!(first, Err(Error::ErasureReadQuorum)));

        let cached = fresh_cached_data_usage_snapshot(&cache, loaded_at + Duration::from_secs(1), Duration::from_secs(30))
            .expect("the failed load must remain cached within the TTL");
        assert!(cached.is_err());

        assert!(fresh_cached_data_usage_snapshot(&cache, loaded_at + Duration::from_secs(30), Duration::from_secs(30)).is_none());
    }

    #[test]
    #[serial]
    fn cached_snapshot_success_is_reused_until_ttl_expires() {
        let loaded_at = tokio::time::Instant::now();
        let expected = data_usage_info_for_test("bucket", 3, 42, SystemTime::UNIX_EPOCH);
        let mut cache = None;
        let refresh_generation = data_usage_snapshot_generation();

        let first = cache_data_usage_snapshot_result(
            &mut cache,
            Ok((expected, HashMap::new())),
            loaded_at,
            refresh_generation,
            data_usage_snapshot_generation(),
        )
        .expect("an uninterrupted refresh should populate the cache")
        .expect("successful load must be returned");
        assert_snapshot_bucket(&first, "bucket");

        let cached = fresh_cached_data_usage_snapshot(&cache, loaded_at + Duration::from_secs(1), Duration::from_secs(30))
            .expect("successful load must remain cached within the TTL")
            .expect("cached success must remain successful");
        assert_snapshot_bucket(&cached, "bucket");
    }

    #[test]
    #[serial]
    fn cache_invalidation_during_refresh_prevents_stale_snapshot_resurrection() {
        let loaded_at = tokio::time::Instant::now();
        let refresh_generation = data_usage_snapshot_generation();
        let mut cache = Some(CachedDataUsageSnapshot {
            info: Some(data_usage_info_for_test("stale", 1, 42, SystemTime::UNIX_EPOCH)),
            loaded_at,
            degraded_baseline: HashMap::new(),
        });
        clear_data_usage_snapshot_cache(&mut cache);

        let stale_result = cache_data_usage_snapshot_result(
            &mut cache,
            Ok((data_usage_info_for_test("stale", 1, 42, SystemTime::UNIX_EPOCH), HashMap::new())),
            loaded_at,
            refresh_generation,
            data_usage_snapshot_generation(),
        );

        assert!(stale_result.is_none());
        assert!(
            cache.is_none(),
            "an in-flight load must not repopulate a cache invalidated after it started"
        );
    }

    #[tokio::test]
    async fn resolve_snapshot_primary_ok_is_used_without_backup_read() {
        let backup_read = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let backup_read_probe = Arc::clone(&backup_read);

        let info = resolve_loaded_snapshot(Ok(snapshot_bytes("primary-bucket")), async move {
            backup_read_probe.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(snapshot_bytes("backup-bucket"))
        })
        .await
        .expect("healthy primary snapshot must load");

        assert_snapshot_bucket(&info, "primary-bucket");
        assert!(
            !backup_read.load(std::sync::atomic::Ordering::SeqCst),
            "backup must not be read when the primary parses"
        );
    }

    #[tokio::test]
    async fn resolve_snapshot_primary_corrupt_falls_back_to_backup() {
        let (info, source) =
            resolve_loaded_snapshot_with_source(Ok(b"{not json".to_vec()), async { Ok(snapshot_bytes("backup-bucket")) })
                .await
                .expect("backup snapshot must be served when the primary is corrupt");

        assert_snapshot_bucket(&info, "backup-bucket");
        assert_eq!(source, UsageSnapshotSource::Backup);
    }

    #[tokio::test]
    async fn resolve_snapshot_primary_corrupt_backup_corrupt_is_error() {
        let err = resolve_loaded_snapshot(Ok(b"{not json".to_vec()), async { Ok(b"also not json".to_vec()) })
            .await
            .expect_err("two corrupt snapshots must not produce stats");
        assert!(err.to_string().contains("deserialize"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn resolve_snapshot_primary_corrupt_backup_missing_is_error() {
        let err = resolve_loaded_snapshot(Ok(b"{not json".to_vec()), async { Err(Error::ConfigNotFound) })
            .await
            .expect_err("a corrupt primary without a backup must not produce stats");
        assert!(err.to_string().contains("deserialize"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn resolve_snapshot_primary_empty_backup_missing_is_error() {
        let err = resolve_loaded_snapshot(Ok(Vec::new()), async { Err(Error::ConfigNotFound) })
            .await
            .expect_err("an empty primary object is corrupt, not an absent snapshot");
        assert!(err.to_string().contains("deserialize"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn resolve_snapshot_primary_read_error_falls_back_to_backup() {
        let info = resolve_loaded_snapshot(Err(Error::ErasureReadQuorum), async { Ok(snapshot_bytes("backup-bucket")) })
            .await
            .expect("backup snapshot must be served when the primary read fails");

        assert_snapshot_bucket(&info, "backup-bucket");
    }

    #[tokio::test]
    async fn resolve_snapshot_primary_real_error_backup_missing_is_error_not_zeros() {
        let err = resolve_loaded_snapshot(Err(Error::ErasureReadQuorum), async { Err(Error::ConfigNotFound) })
            .await
            .expect_err("primary corruption must not be rendered as all-zero stats");
        assert!(matches!(err, Error::ErasureReadQuorum), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn resolve_snapshot_primary_missing_uses_backup() {
        let info = resolve_loaded_snapshot(Err(Error::ConfigNotFound), async { Ok(snapshot_bytes("backup-bucket")) })
            .await
            .expect("an existing backup must be used when the primary is missing");
        assert_snapshot_bucket(&info, "backup-bucket");
    }

    #[tokio::test]
    async fn resolve_snapshot_backup_read_error_is_preserved() {
        let err = resolve_loaded_snapshot(Err(Error::ConfigNotFound), async { Err(Error::ErasureReadQuorum) })
            .await
            .expect_err("a backup read failure must be returned");
        assert!(matches!(err, Error::ErasureReadQuorum), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn resolve_snapshot_both_missing_is_default() {
        let info = resolve_loaded_snapshot(Err(Error::ConfigNotFound), async { Err(Error::ConfigNotFound) })
            .await
            .expect("no snapshot yet must resolve to empty stats");
        assert_eq!(info.buckets_count, 0);
        assert!(info.buckets_usage.is_empty());
    }

    #[tokio::test]
    async fn authoritative_usage_loader_prefers_v2_over_legacy_input() {
        let store = Arc::new(UsageCasStore {
            state: Mutex::new(UsageCasState {
                legacy_object: Some((snapshot_bytes("legacy-bucket"), 1)),
                ..Default::default()
            }),
        });

        let (legacy, source) = load_data_usage_snapshot_from_store(store.clone())
            .await
            .expect("a legacy snapshot should seed an upgrade when newer formats are absent");
        assert_snapshot_bucket(&legacy, "legacy-bucket");
        assert_eq!(source, UsageSnapshotSource::LegacyPrimary);

        store.state.lock().await.object = Some((snapshot_bytes("v2-bucket"), 2));
        let (authoritative, source) = load_data_usage_snapshot_from_store(store.clone())
            .await
            .expect("a v2 snapshot should supersede the legacy migration input");
        assert_snapshot_bucket(&authoritative, "v2-bucket");
        assert_eq!(source, UsageSnapshotSource::Primary);

        store.state.lock().await.object = Some((b"corrupt-v2".to_vec(), 3));
        let err = load_data_usage_snapshot_from_store(store)
            .await
            .expect_err("corrupt v2 state must not fall back to a legacy writer");
        assert!(err.to_string().contains("deserialize"));
    }

    #[tokio::test]
    async fn compute_usage_preserves_same_object_across_1000_entry_page_boundary() {
        let first_page = UsageVersionPage {
            is_truncated: true,
            next_marker: Some("object-a".to_string()),
            next_version_idmarker: Some(uuid::Uuid::from_u128(1000).to_string()),
            objects: (1..=1000_u128)
                .map(|version| ObjectInfo {
                    name: "object-a".to_string(),
                    size: 1,
                    version_id: Some(uuid::Uuid::from_u128(version)),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let second_page = UsageVersionPage {
            objects: vec![
                ObjectInfo {
                    name: "object-a".to_string(),
                    size: 1,
                    version_id: Some(uuid::Uuid::from_u128(1001)),
                    ..Default::default()
                },
                ObjectInfo {
                    name: "object-a".to_string(),
                    version_id: Some(uuid::Uuid::from_u128(1002)),
                    delete_marker: true,
                    ..Default::default()
                },
                ObjectInfo {
                    name: "object-b".to_string(),
                    size: 2,
                    version_id: Some(uuid::Uuid::from_u128(1003)),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let pages = Arc::new(std::sync::Mutex::new(std::collections::VecDeque::from([first_page, second_page])));
        let fetch_pages = Arc::clone(&pages);

        let usage = compute_bucket_usage_with_pages("bucket-a", move |marker, version_marker| {
            let page = fetch_pages
                .lock()
                .expect("page queue lock should not be poisoned")
                .pop_front()
                .expect("pagination must not request an unexpected page");
            if page.is_truncated {
                assert_eq!((marker, version_marker), (None, None));
            } else {
                let expected_version_marker = uuid::Uuid::from_u128(1000).to_string();
                assert_eq!(marker.as_deref(), Some("object-a"));
                assert_eq!(version_marker.as_deref(), Some(expected_version_marker.as_str()));
            }
            async move { Ok(page) }
        })
        .await
        .expect("two-page usage aggregation should succeed");

        assert!(pages.lock().expect("page queue lock should not be poisoned").is_empty());
        assert_eq!(usage.objects_count, 2);
        assert_eq!(usage.versions_count, 1002);
        assert_eq!(usage.delete_markers_count, 1);
        assert_eq!(usage.size, 1003);
        assert_eq!(usage.object_versions_histogram.get("SINGLE_VERSION"), Some(&1));
        assert_eq!(usage.object_versions_histogram.get("BETWEEN_1000_AND_10000"), Some(&1));
    }

    #[test]
    fn bucket_usage_uses_the_larger_of_logical_and_physical_size() {
        let mut metadata = HashMap::new();
        rustfs_utils::http::insert_str(
            &mut metadata,
            rustfs_utils::http::SUFFIX_COMPRESSION,
            "klauspost/compress/s2".to_string(),
        );
        rustfs_utils::http::insert_str(&mut metadata, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, "4096".to_string());
        let object = ObjectInfo {
            name: "compressed".to_string(),
            size: 128,
            user_defined: Arc::new(metadata),
            ..Default::default()
        };
        let mut usage = BucketUsageAccumulator::default();
        usage
            .record("bucket", &object)
            .expect("valid compressed metadata should be counted");
        assert_eq!(usage.finish().size, 4096);

        let mut framed_metadata = HashMap::new();
        rustfs_utils::http::insert_str(
            &mut framed_metadata,
            rustfs_utils::http::SUFFIX_COMPRESSION,
            "klauspost/compress/s2".to_string(),
        );
        rustfs_utils::http::insert_str(&mut framed_metadata, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, "1".to_string());
        let framed = ObjectInfo {
            name: "framed".to_string(),
            size: 17,
            user_defined: Arc::new(framed_metadata),
            ..Default::default()
        };
        assert_eq!(quota_object_size(&framed).expect("physical framing must remain quota-accounted"), 17);

        let legacy_compressed_part = ObjectInfo {
            name: "legacy-compressed-part".to_string(),
            size: 1,
            user_defined: Arc::new((*framed.user_defined).clone()),
            parts: Arc::new(vec![rustfs_filemeta::ObjectPartInfo {
                size: 1,
                actual_size: -1,
                ..Default::default()
            }]),
            ..Default::default()
        };
        assert_eq!(
            quota_object_size(&legacy_compressed_part).expect("unknown compressed part size is a valid sentinel"),
            1
        );

        let uncompressed_negative_part = ObjectInfo {
            name: "uncompressed-negative-part".to_string(),
            size: 1,
            parts: Arc::new(vec![rustfs_filemeta::ObjectPartInfo {
                size: 1,
                actual_size: -1,
                ..Default::default()
            }]),
            ..Default::default()
        };
        assert!(matches!(quota_object_size(&uncompressed_negative_part), Err(Error::PartMissingOrCorrupt)));

        let mut corrupt_metadata = (*object.user_defined).clone();
        rustfs_utils::http::insert_str(&mut corrupt_metadata, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, "-1".to_string());
        let corrupt = ObjectInfo {
            user_defined: Arc::new(corrupt_metadata),
            ..object
        };
        assert!(matches!(quota_object_size(&corrupt), Err(Error::PartMissingOrCorrupt)));

        let mut poisoned_metadata = HashMap::new();
        rustfs_utils::http::insert_str(
            &mut poisoned_metadata,
            rustfs_utils::http::SUFFIX_COMPRESSION,
            "klauspost/compress/s2".to_string(),
        );
        rustfs_utils::http::insert_str(&mut poisoned_metadata, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, "1".to_string());
        let poisoned = ObjectInfo {
            name: "legacy-swift-metadata".to_string(),
            size: 4096,
            user_defined: Arc::new(poisoned_metadata),
            parts: Arc::new(vec![rustfs_filemeta::ObjectPartInfo {
                size: 4096,
                actual_size: 4096,
                ..Default::default()
            }]),
            ..Default::default()
        };
        assert_eq!(
            quota_object_size(&poisoned).expect("persisted part accounting must bound legacy user metadata"),
            4096
        );
    }

    #[test]
    fn quota_object_size_accepts_compressed_unknown_actual_size_sentinel() {
        let mut metadata = HashMap::new();
        rustfs_utils::http::insert_str(
            &mut metadata,
            rustfs_utils::http::SUFFIX_COMPRESSION,
            "klauspost/compress/s2".to_string(),
        );
        let object = ObjectInfo {
            size: 400,
            actual_size: -1,
            user_defined: Arc::new(metadata),
            ..Default::default()
        };

        assert_eq!(quota_object_size(&object).expect("compressed sentinel is valid"), 400);
    }

    #[test]
    fn quota_object_size_rejects_compressed_part_sum_overflow() {
        let mut metadata = HashMap::new();
        rustfs_utils::http::insert_str(
            &mut metadata,
            rustfs_utils::http::SUFFIX_COMPRESSION,
            "klauspost/compress/s2".to_string(),
        );
        let object = ObjectInfo {
            size: 1,
            user_defined: Arc::new(metadata),
            parts: Arc::new(vec![
                rustfs_filemeta::ObjectPartInfo {
                    actual_size: i64::MAX,
                    ..Default::default()
                },
                rustfs_filemeta::ObjectPartInfo {
                    actual_size: 1,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        assert!(matches!(quota_object_size(&object), Err(Error::Io(_))));
    }

    #[test]
    fn quota_object_size_rejects_negative_values_other_than_the_compressed_sentinel() {
        let mut metadata = HashMap::new();
        rustfs_utils::http::insert_str(
            &mut metadata,
            rustfs_utils::http::SUFFIX_COMPRESSION,
            "klauspost/compress/s2".to_string(),
        );
        let corrupt_object = ObjectInfo {
            size: 400,
            actual_size: -2,
            user_defined: Arc::new(metadata.clone()),
            ..Default::default()
        };
        assert!(matches!(quota_object_size(&corrupt_object), Err(Error::PartMissingOrCorrupt)));

        let corrupt_part = ObjectInfo {
            size: 400,
            user_defined: Arc::new(metadata),
            parts: Arc::new(vec![rustfs_filemeta::ObjectPartInfo {
                size: 400,
                actual_size: -2,
                ..Default::default()
            }]),
            ..Default::default()
        };
        assert!(matches!(quota_object_size(&corrupt_part), Err(Error::PartMissingOrCorrupt)));
    }

    #[tokio::test]
    #[serial]
    async fn live_bucket_usage_refreshes_are_coalesced_only_while_in_flight() {
        const BUCKET: &str = "coalesced-live-usage-test";
        live_bucket_usage_cache().invalidate(BUCKET).await;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut tasks = Vec::new();

        for _ in 0..8 {
            let calls = Arc::clone(&calls);
            tasks.push(tokio::spawn(coalesce_live_bucket_usage(BUCKET.to_string(), async move {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(25)).await;
                Ok(BucketUsageInfo {
                    objects_count: 7,
                    size: 42,
                    ..Default::default()
                })
            })));
        }

        for task in tasks {
            let usage = task
                .await
                .expect("coalesced refresh task should not panic")
                .expect("coalesced refresh should succeed");
            assert_eq!((usage.objects_count, usage.size), (7, 42));
        }
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        let calls_after_batch = Arc::clone(&calls);
        coalesce_live_bucket_usage(BUCKET.to_string(), async move {
            calls_after_batch.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(BucketUsageInfo::default())
        })
        .await
        .expect("a later refresh should run after the in-flight entry is removed");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        live_bucket_usage_cache().invalidate(BUCKET).await;
    }

    #[tokio::test]
    #[serial]
    async fn live_usage_updates_response_without_replacing_quota_memory() {
        clear_usage_memory_cache_for_test().await;
        let persisted = data_usage_info_for_test("bucket-a", 100, 1_000, SystemTime::now());
        replace_bucket_usage_memory_from_info(&persisted).await;
        let mut response = persisted.clone();

        apply_live_bucket_usage_to_response(&mut response, "bucket-a", &BucketUsageInfo::default());

        assert_eq!(response.buckets_usage.get("bucket-a").map(|usage| usage.objects_count), Some(0));
        assert_eq!(get_bucket_usage_memory("bucket-a").await, Some(1_000));
    }

    #[test]
    fn version_listing_cursor_rejects_incomplete_or_non_advancing_pages() {
        let mut marker = Some("object-a".to_string());
        let mut version_marker = Some("version-a".to_string());

        assert!(
            advance_version_listing_cursor("bucket-a", &mut marker, &mut version_marker, None, Some("version-b".to_string()),)
                .is_err()
        );
        assert!(
            advance_version_listing_cursor("bucket-a", &mut marker, &mut version_marker, Some("object-a".to_string()), None,)
                .is_err()
        );
        assert!(
            advance_version_listing_cursor(
                "bucket-a",
                &mut marker,
                &mut version_marker,
                Some("object-a".to_string()),
                Some("version-a".to_string()),
            )
            .is_err()
        );
        assert!(ensure_truncated_version_page_has_entries("bucket-a", 0).is_err());
        ensure_truncated_version_page_has_entries("bucket-a", 1).expect("a truncated page with an entry can advance");

        let mut tagged_marker = Some("object-a[rustfs_cache:v2,id:old]".to_string());
        let mut tagged_version_marker = Some("version-a".to_string());
        assert!(
            advance_version_listing_cursor(
                "bucket-a",
                &mut tagged_marker,
                &mut tagged_version_marker,
                Some("object-a[rustfs_cache:v2,id:new]".to_string()),
                Some("version-a".to_string()),
            )
            .is_err(),
            "different cache tags for the same logical marker must not count as progress"
        );
    }

    #[test]
    fn version_listing_rejects_replayed_and_out_of_order_entries() {
        let version_a = uuid::Uuid::from_u128(1);
        let version_b = uuid::Uuid::from_u128(2);
        let mut current_object = None;
        let mut current_versions = HashSet::new();

        record_version_listing_entry(
            "bucket-a",
            &mut current_object,
            &mut current_versions,
            "object-b",
            Some(version_a.as_bytes()),
        )
        .expect("first version should be accepted");
        assert!(
            record_version_listing_entry(
                "bucket-a",
                &mut current_object,
                &mut current_versions,
                "object-b",
                Some(version_a.as_bytes()),
            )
            .is_err()
        );
        record_version_listing_entry(
            "bucket-a",
            &mut current_object,
            &mut current_versions,
            "object-c",
            Some(version_b.as_bytes()),
        )
        .expect("a lexicographically later object should reset version history");
        assert!(
            record_version_listing_entry(
                "bucket-a",
                &mut current_object,
                &mut current_versions,
                "object-a",
                Some(version_a.as_bytes()),
            )
            .is_err()
        );
    }

    fn aggregate_for_test(
        inputs: Vec<(DiskUsageStatus, Result<Option<LocalUsageSnapshot>, Error>)>,
    ) -> (Vec<DiskUsageStatus>, DataUsageInfo) {
        let mut aggregated = DataUsageInfo::default();
        let mut latest_update: Option<SystemTime> = None;
        let mut statuses = Vec::new();

        for (mut status, snapshot_result) in inputs {
            if let Ok(Some(snapshot)) = snapshot_result {
                status.snapshot_exists = true;
                status.last_update = snapshot.last_update;
                merge_snapshot(&mut aggregated, snapshot, &mut latest_update);
            }
            statuses.push(status);
        }

        aggregated.buckets_count = aggregated.buckets_usage.len() as u64;
        aggregated.last_update = latest_update;
        aggregated.disk_usage_status = statuses.clone();

        (statuses, aggregated)
    }

    #[test]
    fn aggregate_skips_corrupted_snapshot_and_preserves_other_disks() {
        let mut good_snapshot = LocalUsageSnapshot::new(local_snapshot::LocalUsageSnapshotMeta {
            disk_id: "good-disk".to_string(),
            pool_index: Some(0),
            set_index: Some(0),
            disk_index: Some(0),
        });
        good_snapshot.last_update = Some(SystemTime::now());
        good_snapshot.buckets_usage.insert(
            "bucket-a".to_string(),
            BucketUsageInfo {
                objects_count: 3,
                versions_count: 3,
                size: 42,
                ..Default::default()
            },
        );
        good_snapshot.recompute_totals();

        let bad_snapshot_err: Result<Option<LocalUsageSnapshot>, Error> = Err(Error::other("corrupted snapshot payload"));

        let inputs = vec![
            (
                DiskUsageStatus {
                    disk_id: "bad-disk".to_string(),
                    pool_index: Some(0),
                    set_index: Some(0),
                    disk_index: Some(1),
                    last_update: None,
                    snapshot_exists: false,
                },
                bad_snapshot_err,
            ),
            (
                DiskUsageStatus {
                    disk_id: "good-disk".to_string(),
                    pool_index: Some(0),
                    set_index: Some(0),
                    disk_index: Some(0),
                    last_update: None,
                    snapshot_exists: false,
                },
                Ok(Some(good_snapshot)),
            ),
        ];

        let (statuses, aggregated) = aggregate_for_test(inputs);

        // Bad disk stays non-existent, good disk is marked present
        let bad_status = statuses.iter().find(|s| s.disk_id == "bad-disk").unwrap();
        assert!(!bad_status.snapshot_exists);
        let good_status = statuses.iter().find(|s| s.disk_id == "good-disk").unwrap();
        assert!(good_status.snapshot_exists);

        // Aggregated data is from good snapshot only
        assert_eq!(aggregated.objects_total_count, 3);
        assert_eq!(aggregated.objects_total_size, 42);
        assert_eq!(aggregated.buckets_count, 1);
        assert_eq!(aggregated.buckets_usage.get("bucket-a").map(|b| (b.objects_count, b.size)), Some((3, 42)));
    }

    #[tokio::test]
    #[serial]
    async fn memory_overlay_reflects_recent_delete_before_scanner_persists() {
        clear_usage_memory_cache_for_test().await;

        let persisted = data_usage_info_for_test("bucket-a", 1, 42, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_object_delete_memory("bucket-a", 42, true).await;

        let mut response = persisted.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 0);
        assert_eq!(response.objects_total_size, 0);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((0, 0))
        );
    }

    #[tokio::test]
    #[serial]
    async fn distributed_usage_does_not_apply_a_process_local_absolute_count() {
        clear_usage_memory_cache_for_test().await;

        let persisted = data_usage_info_for_test("bucket-a", 100, 1_000, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_object_delete_memory("bucket-a", 42, true).await;

        let mut distributed_response = persisted.clone();
        apply_bucket_usage_memory_overlay_if_authoritative(&mut distributed_response, false).await;
        assert_eq!(
            distributed_response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((100, 1_000))
        );

        let mut single_process_response = persisted;
        apply_bucket_usage_memory_overlay_if_authoritative(&mut single_process_response, true).await;
        assert_eq!(
            single_process_response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((99, 958))
        );
    }

    #[tokio::test]
    #[serial]
    async fn memory_overlay_does_not_publish_delta_from_unknown_baseline() {
        clear_usage_memory_cache_for_test().await;

        let bucket = "unknown-baseline";
        memory_cache().write().await.insert(
            bucket.to_string(),
            cached_bucket_usage_from_backend(BucketUsageInfo::default(), SystemTime::UNIX_EPOCH, false),
        );
        record_bucket_object_write_memory(bucket, None, 42).await;

        let mut response = DataUsageInfo::default();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert!(!response.buckets_usage.contains_key(bucket));
        assert_eq!(get_bucket_usage_memory(bucket).await, None);
    }

    #[tokio::test]
    #[serial]
    async fn unknown_dirty_usage_requires_a_followup_scanner_generation() {
        clear_usage_memory_cache_for_test().await;

        let bucket = "unknown-baseline";
        memory_cache().write().await.insert(
            bucket.to_string(),
            cached_bucket_usage_from_backend(BucketUsageInfo::default(), SystemTime::UNIX_EPOCH, false),
        );
        record_bucket_object_write_memory(bucket, None, 42).await;
        let mutation_update = memory_cache()
            .read()
            .await
            .get(bucket)
            .expect("unknown usage mutation should remain cached")
            .usage_updated_at;

        let mut first_snapshot = data_usage_info_for_test(bucket, 10, 420, mutation_update + Duration::from_nanos(1));
        first_snapshot.scanner_epoch = Some(7);
        first_snapshot.scanner_cycle = Some(10);
        replace_bucket_usage_memory_from_info(&first_snapshot).await;

        let mut first_response = first_snapshot.clone();
        apply_bucket_usage_memory_overlay_if_authoritative(&mut first_response, true).await;
        assert!(
            !first_response.usage_snapshot_complete,
            "the first scanner generation after an unknown mutation cannot prove that it observed the mutation"
        );
        assert_eq!(
            first_response
                .buckets_usage
                .get(bucket)
                .map(|usage| (usage.objects_count, usage.size)),
            Some((10, 420)),
            "unknown memory deltas must not be overlaid as absolute usage"
        );

        replace_bucket_usage_memory_from_info(&first_snapshot).await;
        let mut repeated_response = first_snapshot.clone();
        apply_bucket_usage_memory_overlay_if_authoritative(&mut repeated_response, true).await;
        assert!(
            !repeated_response.usage_snapshot_complete,
            "reloading the same scanner generation must not clear unknown mutation evidence"
        );

        let mut successor = data_usage_info_for_test(bucket, 11, 462, mutation_update + Duration::from_nanos(2));
        successor.scanner_epoch = Some(7);
        successor.scanner_cycle = Some(11);
        replace_bucket_usage_memory_from_info(&successor).await;

        let mut successor_response = successor;
        apply_bucket_usage_memory_overlay_if_authoritative(&mut successor_response, true).await;
        assert!(successor_response.is_complete_bucket_usage_snapshot());
        assert_eq!(
            successor_response
                .buckets_usage
                .get(bucket)
                .map(|usage| (usage.objects_count, usage.size)),
            Some((11, 462))
        );
    }

    #[tokio::test]
    #[serial]
    async fn a_new_unknown_mutation_restarts_the_scanner_generation_fence() {
        clear_usage_memory_cache_for_test().await;

        let bucket = "unknown-baseline";
        memory_cache().write().await.insert(
            bucket.to_string(),
            cached_bucket_usage_from_backend(BucketUsageInfo::default(), SystemTime::UNIX_EPOCH, false),
        );
        record_bucket_object_write_memory(bucket, None, 42).await;
        let first_mutation_update = memory_cache()
            .read()
            .await
            .get(bucket)
            .expect("unknown usage mutation should remain cached")
            .usage_updated_at;

        let mut first_snapshot = data_usage_info_for_test(bucket, 10, 420, first_mutation_update + Duration::from_nanos(1));
        first_snapshot.scanner_epoch = Some(7);
        first_snapshot.scanner_cycle = Some(10);
        replace_bucket_usage_memory_from_info(&first_snapshot).await;

        record_bucket_object_write_memory(bucket, None, 42).await;
        let second_mutation_update = memory_cache()
            .read()
            .await
            .get(bucket)
            .expect("second unknown usage mutation should remain cached")
            .usage_updated_at;
        let mut next_snapshot = data_usage_info_for_test(bucket, 11, 462, second_mutation_update + Duration::from_nanos(1));
        next_snapshot.scanner_epoch = Some(7);
        next_snapshot.scanner_cycle = Some(11);
        replace_bucket_usage_memory_from_info(&next_snapshot).await;

        let mut next_response = next_snapshot.clone();
        apply_bucket_usage_memory_overlay_if_authoritative(&mut next_response, true).await;
        assert!(
            !next_response.usage_snapshot_complete,
            "a mutation after the first observation must require another scanner generation"
        );

        let mut final_snapshot = data_usage_info_for_test(bucket, 12, 504, second_mutation_update + Duration::from_nanos(2));
        final_snapshot.scanner_epoch = Some(7);
        final_snapshot.scanner_cycle = Some(12);
        replace_bucket_usage_memory_from_info(&final_snapshot).await;

        let mut final_response = final_snapshot;
        apply_bucket_usage_memory_overlay_if_authoritative(&mut final_response, true).await;
        assert!(final_response.is_complete_bucket_usage_snapshot());
    }

    #[test]
    fn remove_bucket_usage_from_info_drops_bucket_and_recomputes_totals() {
        let last_update = SystemTime::now();
        let mut info = data_usage_info_for_test("bucket-a", 2, 84, last_update);
        info.buckets_usage.insert(
            "bucket-b".to_string(),
            BucketUsageInfo {
                objects_count: 3,
                versions_count: 3,
                size: 126,
                ..Default::default()
            },
        );
        info.bucket_sizes.insert("bucket-b".to_string(), 126);
        info.buckets_count = 2;
        info.calculate_totals();

        assert!(remove_bucket_usage_from_info(&mut info, "bucket-a"));

        assert_eq!(info.buckets_count, 1);
        assert_eq!(info.objects_total_count, 3);
        assert_eq!(info.objects_total_size, 126);
        assert_eq!(info.last_update, Some(last_update));
        assert!(!info.buckets_usage.contains_key("bucket-a"));
        assert!(!info.bucket_sizes.contains_key("bucket-a"));
        assert_eq!(
            info.buckets_usage
                .get("bucket-b")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((3, 126))
        );
    }

    #[tokio::test]
    async fn remove_bucket_usage_retries_against_newer_scanner_snapshot() {
        let now = SystemTime::now();
        let mut initial = data_usage_info_for_test("bucket-a", 2, 84, now - Duration::from_secs(10));
        initial.scanner_epoch = Some(8);
        initial.scanner_cycle = Some(12);
        let mut scanner_winner = data_usage_info_for_test("bucket-a", 4, 168, now);
        scanner_winner.scanner_epoch = Some(9);
        scanner_winner.scanner_cycle = Some(13);
        scanner_winner.buckets_usage.insert(
            "bucket-c".to_string(),
            BucketUsageInfo {
                objects_count: 5,
                versions_count: 5,
                size: 210,
                ..Default::default()
            },
        );
        scanner_winner.bucket_sizes.insert("bucket-c".to_string(), 210);
        scanner_winner.buckets_count = 2;
        scanner_winner.calculate_totals();
        let store = Arc::new(UsageCasStore {
            state: Mutex::new(UsageCasState {
                object: Some((serde_json::to_vec(&initial).expect("initial usage snapshot should encode"), 1)),
                interleaving_snapshot: Some(serde_json::to_vec(&scanner_winner).expect("scanner winner snapshot should encode")),
                ..Default::default()
            }),
        });

        remove_bucket_usage_from_backend_with_store(store.as_ref(), "bucket-a")
            .await
            .expect("bucket removal should reconcile the scanner CAS winner");

        let state = store.state.lock().await;
        let saved = serde_json::from_slice::<DataUsageInfo>(&state.object.as_ref().expect("usage snapshot should remain").0)
            .expect("saved usage snapshot should decode");
        assert_eq!(state.put_count, 3);
        assert!(saved.last_update.is_some_and(|last_update| last_update > now));
        assert_eq!(saved.scanner_epoch, Some(9));
        assert_eq!(saved.scanner_cycle, Some(13));
        assert_eq!(saved.buckets_count, 1);
        assert_eq!(saved.objects_total_count, 5);
        assert_eq!(saved.objects_total_size, 210);
        assert!(!saved.buckets_usage.contains_key("bucket-a"));
        assert_eq!(
            saved
                .buckets_usage
                .get("bucket-c")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((5, 210))
        );
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn remove_bucket_usage_stops_retry_after_namespace_lock_loss() {
        let ttl = Duration::from_millis(20);
        let lock = NamespaceLock::new("usage-cleanup-lock-loss-test".to_string(), Arc::new(LocalClient::new()));
        let request = LockRequest::new(ObjectKey::new("bucket-a", ""), LockType::Exclusive, "cleanup-owner")
            .with_acquire_timeout(Duration::from_secs(1))
            .with_ttl(ttl)
            .with_refresh_interval(ttl);
        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("namespace lock acquisition should not fail")
            .expect("namespace lock should be acquired");

        let now = SystemTime::now();
        let initial = data_usage_info_for_test("bucket-a", 2, 84, now - Duration::from_secs(10));
        let scanner_winner = data_usage_info_for_test("bucket-a", 4, 168, now);
        let store = Arc::new(UsageCasStore {
            state: Mutex::new(UsageCasState {
                object: Some((serde_json::to_vec(&initial).expect("initial usage snapshot should encode"), 1)),
                interleaving_snapshot: Some(serde_json::to_vec(&scanner_winner).expect("scanner winner should encode")),
                advance_time_on_put: Some(ttl + Duration::from_millis(1)),
                ..Default::default()
            }),
        });

        let err = remove_bucket_usage_from_backend_with_store_and_guard(store.as_ref(), "bucket-a", Some(&guard))
            .await
            .expect_err("namespace lock loss must stop the cleanup CAS retry");
        assert!(err.to_string().contains("namespace lock was lost"));

        let state = store.state.lock().await;
        assert_eq!(state.put_count, 1, "the stale cleanup must not issue a second CAS write");
        let saved = serde_json::from_slice::<DataUsageInfo>(&state.object.as_ref().expect("scanner winner should remain").0)
            .expect("scanner winner should decode");
        assert_eq!(saved.buckets_usage.get("bucket-a").map(|usage| usage.objects_count), Some(4));
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn remove_bucket_usage_stops_before_put_after_namespace_lock_loss() {
        let ttl = Duration::from_millis(20);
        let lock = NamespaceLock::new("usage-pre-put-lock-loss-test".to_string(), Arc::new(LocalClient::new()));
        let request = LockRequest::new(ObjectKey::new("bucket-a", ""), LockType::Exclusive, "cleanup-owner")
            .with_acquire_timeout(Duration::from_secs(1))
            .with_ttl(ttl)
            .with_refresh_interval(ttl);
        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("namespace lock acquisition should not fail")
            .expect("namespace lock should be acquired");

        let initial = data_usage_info_for_test("bucket-a", 2, 84, SystemTime::now());
        let encoded = serde_json::to_vec(&initial).expect("initial usage snapshot should encode");
        let store = Arc::new(UsageCasStore {
            state: Mutex::new(UsageCasState {
                object: Some((encoded, 1)),
                advance_time_after_get: Some((UsageObjectSlot::Primary, ttl + Duration::from_millis(1))),
                ..Default::default()
            }),
        });

        let err = remove_bucket_usage_from_backend_with_store_and_guard(store.as_ref(), "bucket-a", Some(&guard))
            .await
            .expect_err("namespace lock loss after the snapshot read must fence the write");
        assert!(err.to_string().contains("namespace lock was lost"));

        let state = store.state.lock().await;
        assert_eq!(state.put_count, 0, "a cleanup that lost its lease must not start a snapshot write");
        let saved = serde_json::from_slice::<DataUsageInfo>(&state.object.as_ref().expect("usage snapshot should remain").0)
            .expect("usage snapshot should decode");
        assert!(saved.buckets_usage.contains_key("bucket-a"));
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn remove_bucket_usage_stops_before_backup_after_namespace_lock_loss() {
        let ttl = Duration::from_millis(20);
        let lock = NamespaceLock::new("usage-backup-lock-loss-test".to_string(), Arc::new(LocalClient::new()));
        let request = LockRequest::new(ObjectKey::new("bucket-a", ""), LockType::Exclusive, "cleanup-owner")
            .with_acquire_timeout(Duration::from_secs(1))
            .with_ttl(ttl)
            .with_refresh_interval(ttl);
        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("namespace lock acquisition should not fail")
            .expect("namespace lock should be acquired");

        let snapshot = data_usage_info_for_test("bucket-a", 2, 84, SystemTime::now());
        let encoded = serde_json::to_vec(&snapshot).expect("usage snapshot should encode");
        let store = Arc::new(UsageCasStore {
            state: Mutex::new(UsageCasState {
                object: Some((encoded.clone(), 1)),
                backup_object: Some((encoded, 1)),
                advance_time_after_put: Some((1, ttl + Duration::from_millis(1))),
                ..Default::default()
            }),
        });

        let err = remove_bucket_usage_from_backend_with_store_and_guard(store.as_ref(), "bucket-a", Some(&guard))
            .await
            .expect_err("namespace lock loss must stop cleanup before the backup write");
        assert!(err.to_string().contains("namespace lock was lost"));

        let state = store.state.lock().await;
        assert_eq!(state.put_count, 1, "the cleanup must not write the backup after lock loss");
        let backup = serde_json::from_slice::<DataUsageInfo>(
            &state.backup_object.as_ref().expect("new-generation backup should remain").0,
        )
        .expect("backup should decode");
        assert_eq!(backup.buckets_usage.get("bucket-a").map(|usage| usage.objects_count), Some(2));
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn remove_bucket_usage_stops_backup_retry_after_namespace_lock_loss() {
        let ttl = Duration::from_millis(20);
        let lock = NamespaceLock::new("usage-backup-retry-lock-loss-test".to_string(), Arc::new(LocalClient::new()));
        let request = LockRequest::new(ObjectKey::new("bucket-a", ""), LockType::Exclusive, "cleanup-owner")
            .with_acquire_timeout(Duration::from_secs(1))
            .with_ttl(ttl)
            .with_refresh_interval(ttl);
        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("namespace lock acquisition should not fail")
            .expect("namespace lock should be acquired");

        let now = SystemTime::now();
        let initial = data_usage_info_for_test("bucket-a", 2, 84, now - Duration::from_secs(10));
        let scanner_winner = data_usage_info_for_test("bucket-a", 4, 168, now);
        let encoded = serde_json::to_vec(&initial).expect("initial usage snapshot should encode");
        let store = Arc::new(UsageCasStore {
            state: Mutex::new(UsageCasState {
                object: Some((encoded.clone(), 1)),
                backup_object: Some((encoded, 1)),
                interleaving_backup_snapshot: Some(serde_json::to_vec(&scanner_winner).expect("scanner winner should encode")),
                advance_time_before_put: Some((2, ttl + Duration::from_millis(1))),
                ..Default::default()
            }),
        });

        let err = remove_bucket_usage_from_backend_with_store_and_guard(store.as_ref(), "bucket-a", Some(&guard))
            .await
            .expect_err("namespace lock loss must stop the backup CAS retry");
        assert!(err.to_string().contains("namespace lock was lost"));

        let state = store.state.lock().await;
        assert_eq!(state.put_count, 2, "the stale cleanup must not issue a second backup CAS write");
        let backup = serde_json::from_slice::<DataUsageInfo>(
            &state.backup_object.as_ref().expect("scanner winner backup should remain").0,
        )
        .expect("backup should decode");
        assert_eq!(backup.buckets_usage.get("bucket-a").map(|usage| usage.objects_count), Some(4));
    }

    async fn assert_legacy_cleanup_stops_retry_after_lock_loss(backup: bool) {
        let ttl = Duration::from_millis(20);
        let lock = NamespaceLock::new(
            format!("usage-legacy-{}-retry-lock-loss-test", if backup { "backup" } else { "primary" }),
            Arc::new(LocalClient::new()),
        );
        let request = LockRequest::new(ObjectKey::new("bucket-a", ""), LockType::Exclusive, "cleanup-owner")
            .with_acquire_timeout(Duration::from_secs(1))
            .with_ttl(ttl)
            .with_refresh_interval(ttl);
        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("namespace lock acquisition should not fail")
            .expect("namespace lock should be acquired");

        let now = SystemTime::now();
        let initial = data_usage_info_for_test("bucket-a", 2, 84, now - Duration::from_secs(10));
        let scanner_winner = data_usage_info_for_test("bucket-a", 4, 168, now);
        let encoded = serde_json::to_vec(&initial).expect("initial usage snapshot should encode");
        let winner = serde_json::to_vec(&scanner_winner).expect("scanner winner should encode");
        let conflict_put = if backup { 4 } else { 3 };
        let store = Arc::new(UsageCasStore {
            state: Mutex::new(UsageCasState {
                object: Some((encoded.clone(), 1)),
                backup_object: Some((encoded.clone(), 1)),
                legacy_object: Some((encoded.clone(), 1)),
                legacy_backup_object: Some((encoded, 1)),
                interleaving_legacy_snapshot: (!backup).then_some(winner.clone()),
                interleaving_legacy_backup_snapshot: backup.then_some(winner),
                advance_time_before_put: Some((conflict_put, ttl + Duration::from_millis(1))),
                ..Default::default()
            }),
        });

        let err = remove_bucket_usage_from_backend_with_store_and_guard(store.as_ref(), "bucket-a", Some(&guard))
            .await
            .expect_err("namespace lock loss must stop the legacy snapshot CAS retry");
        assert!(err.to_string().contains("namespace lock was lost"));

        let state = store.state.lock().await;
        assert_eq!(
            state.put_count, conflict_put,
            "the stale cleanup must not retry the conflicted legacy snapshot"
        );
        let saved = if backup {
            &state
                .legacy_backup_object
                .as_ref()
                .expect("scanner winner legacy backup should remain")
                .0
        } else {
            &state
                .legacy_object
                .as_ref()
                .expect("scanner winner legacy primary should remain")
                .0
        };
        let saved = serde_json::from_slice::<DataUsageInfo>(saved).expect("scanner winner should decode");
        assert_eq!(saved.buckets_usage.get("bucket-a").map(|usage| usage.objects_count), Some(4));
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn remove_bucket_usage_stops_legacy_retries_after_namespace_lock_loss() {
        assert_legacy_cleanup_stops_retry_after_lock_loss(false).await;
        assert_legacy_cleanup_stops_retry_after_lock_loss(true).await;
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn prepare_bucket_usage_preserves_successor_memory_after_lock_loss() {
        const BUCKET: &str = "successor-bucket";
        let ttl = Duration::from_millis(20);
        let lock = NamespaceLock::new("usage-memory-lock-loss-test".to_string(), Arc::new(LocalClient::new()));
        let request = LockRequest::new(ObjectKey::new(BUCKET, ""), LockType::Exclusive, "cleanup-owner")
            .with_acquire_timeout(Duration::from_secs(1))
            .with_ttl(ttl)
            .with_refresh_interval(ttl);
        let guard = Arc::new(
            lock.acquire_guard(&request)
                .await
                .expect("namespace lock acquisition should not fail")
                .expect("namespace lock should be acquired"),
        );

        let mut cache = memory_cache().write().await;
        cache.insert(
            BUCKET.to_string(),
            cached_bucket_usage_now(BucketUsageInfo {
                objects_count: 7,
                versions_count: 7,
                size: 294,
                ..Default::default()
            }),
        );
        let guard_for_prepare = guard.clone();
        let prepare =
            tokio::spawn(
                async move { prepare_bucket_usage_for_namespace_change(BUCKET, Some(guard_for_prepare.as_ref())).await },
            );
        tokio::task::yield_now().await;

        tokio::time::advance(ttl + Duration::from_millis(1)).await;
        drop(cache);
        let err = prepare
            .await
            .expect("prepare task should join")
            .expect_err("lock loss while waiting for the cache must stop cleanup");
        assert!(err.to_string().contains("namespace lock was lost"));

        let mut cache = memory_cache().write().await;
        assert_eq!(
            cache.get(BUCKET).map(|entry| entry.usage.objects_count),
            Some(7),
            "fresh successor memory usage must not be erased after namespace lock loss"
        );
        cache.remove(BUCKET);
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn prepare_bucket_usage_preserves_successor_snapshot_cache_after_lock_loss() {
        const BUCKET: &str = "successor-snapshot-bucket";
        let ttl = Duration::from_millis(20);
        let lock = NamespaceLock::new("usage-snapshot-lock-loss-test".to_string(), Arc::new(LocalClient::new()));
        let request = LockRequest::new(ObjectKey::new(BUCKET, ""), LockType::Exclusive, "cleanup-owner")
            .with_acquire_timeout(Duration::from_secs(1))
            .with_ttl(ttl)
            .with_refresh_interval(ttl);
        let guard = Arc::new(
            lock.acquire_guard(&request)
                .await
                .expect("namespace lock acquisition should not fail")
                .expect("namespace lock should be acquired"),
        );
        let successor = data_usage_info_for_test(BUCKET, 7, 294, SystemTime::now());
        let mut snapshot_cache = data_usage_snapshot_cache().write().await;
        *snapshot_cache = Some(CachedDataUsageSnapshot {
            info: Some(successor),
            loaded_at: tokio::time::Instant::now(),
            degraded_baseline: HashMap::new(),
        });
        memory_cache()
            .write()
            .await
            .insert(BUCKET.to_string(), cached_bucket_usage_now(BucketUsageInfo::default()));

        let guard_for_prepare = guard.clone();
        let prepare =
            tokio::spawn(
                async move { prepare_bucket_usage_for_namespace_change(BUCKET, Some(guard_for_prepare.as_ref())).await },
            );
        loop {
            if !memory_cache().read().await.contains_key(BUCKET) {
                break;
            }
            tokio::task::yield_now().await;
        }

        tokio::time::advance(ttl + Duration::from_millis(1)).await;
        drop(snapshot_cache);
        let err = prepare
            .await
            .expect("prepare task should join")
            .expect_err("lock loss while waiting for the snapshot cache must stop cleanup");
        assert!(err.to_string().contains("namespace lock was lost"));

        let snapshot_cache = data_usage_snapshot_cache().read().await;
        assert_eq!(
            snapshot_cache
                .as_ref()
                .and_then(|cached| cached.info.as_ref())
                .and_then(|info| info.buckets_usage.get(BUCKET))
                .map(|usage| usage.objects_count),
            Some(7),
            "fresh successor snapshot cache must not be erased after namespace lock loss"
        );
        drop(snapshot_cache);
        invalidate_data_usage_snapshot_cache().await;
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn remove_bucket_usage_preserves_successor_snapshot_cache_after_lock_loss() {
        const BUCKET: &str = "successor-final-snapshot-bucket";
        let ttl = Duration::from_millis(20);
        let lock = NamespaceLock::new("usage-final-snapshot-lock-loss-test".to_string(), Arc::new(LocalClient::new()));
        let request = LockRequest::new(ObjectKey::new(BUCKET, ""), LockType::Exclusive, "cleanup-owner")
            .with_acquire_timeout(Duration::from_secs(1))
            .with_ttl(ttl)
            .with_refresh_interval(ttl);
        let guard = Arc::new(
            lock.acquire_guard(&request)
                .await
                .expect("namespace lock acquisition should not fail")
                .expect("namespace lock should be acquired"),
        );
        let snapshot = data_usage_info_for_test(BUCKET, 2, 84, SystemTime::now());
        let encoded = serde_json::to_vec(&snapshot).expect("usage snapshot should encode");
        let store = Arc::new(UsageCasStore {
            state: Mutex::new(UsageCasState {
                object: Some((encoded.clone(), 1)),
                backup_object: Some((encoded, 1)),
                ..Default::default()
            }),
        });
        let successor = data_usage_info_for_test(BUCKET, 7, 294, SystemTime::now());
        let mut snapshot_cache = data_usage_snapshot_cache().write().await;
        *snapshot_cache = Some(CachedDataUsageSnapshot {
            info: Some(successor),
            loaded_at: tokio::time::Instant::now(),
            degraded_baseline: HashMap::new(),
        });

        let store_for_cleanup = store.clone();
        let guard_for_cleanup = guard.clone();
        let cleanup = tokio::spawn(async move {
            remove_bucket_usage_from_backend_with_guard(store_for_cleanup.as_ref(), BUCKET, Some(guard_for_cleanup.as_ref()))
                .await
        });
        loop {
            if store.state.lock().await.put_count >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }

        tokio::time::advance(ttl + Duration::from_millis(1)).await;
        drop(snapshot_cache);
        let err = cleanup
            .await
            .expect("cleanup task should join")
            .expect_err("lock loss while waiting for final cache invalidation must stop cleanup");
        assert!(err.to_string().contains("namespace lock was lost"));

        let snapshot_cache = data_usage_snapshot_cache().read().await;
        assert_eq!(
            snapshot_cache
                .as_ref()
                .and_then(|cached| cached.info.as_ref())
                .and_then(|info| info.buckets_usage.get(BUCKET))
                .map(|usage| usage.objects_count),
            Some(7),
            "fresh successor snapshot cache must not be erased by final invalidation after lock loss"
        );
        drop(snapshot_cache);
        invalidate_data_usage_snapshot_cache().await;
    }

    #[tokio::test]
    #[serial]
    async fn remove_bucket_usage_invalidates_snapshot_cache_after_backend_error() {
        const BUCKET: &str = "backend-error-cache-bucket";
        let store = UsageCasStore {
            state: Mutex::new(UsageCasState {
                object: Some((b"{".to_vec(), 1)),
                ..Default::default()
            }),
        };
        let stale = data_usage_info_for_test(BUCKET, 7, 294, SystemTime::now());
        *data_usage_snapshot_cache().write().await = Some(CachedDataUsageSnapshot {
            info: Some(stale),
            loaded_at: tokio::time::Instant::now(),
            degraded_baseline: HashMap::new(),
        });

        remove_bucket_usage_from_backend_with_guard(&store, BUCKET, None)
            .await
            .expect_err("the malformed backend snapshot should be reported");

        assert!(
            data_usage_snapshot_cache().read().await.is_none(),
            "a failed cleanup must still invalidate cached predecessor usage"
        );
    }

    #[tokio::test]
    async fn remove_bucket_usage_writes_fences_when_bucket_is_already_absent() {
        let last_update = SystemTime::now() - Duration::from_secs(10);
        let snapshot = data_usage_info_for_test("bucket-b", 3, 126, last_update);
        let encoded = serde_json::to_vec(&snapshot).expect("usage snapshot should encode");
        let store = Arc::new(UsageCasStore {
            state: Mutex::new(UsageCasState {
                object: Some((encoded.clone(), 1)),
                backup_object: Some((encoded, 1)),
                ..Default::default()
            }),
        });

        remove_bucket_usage_from_backend_with_store(store.as_ref(), "bucket-a")
            .await
            .expect("bucket removal should fence both snapshots even when the bucket is absent");

        let state = store.state.lock().await;
        assert_eq!(state.object.as_ref().map(|(_, revision)| *revision), Some(2));
        assert_eq!(state.backup_object.as_ref().map(|(_, revision)| *revision), Some(2));
        for (data, _) in [state.object.as_ref(), state.backup_object.as_ref()].into_iter().flatten() {
            let saved = serde_json::from_slice::<DataUsageInfo>(data).expect("saved usage snapshot should decode");
            assert!(saved.last_update.is_some_and(|updated| updated > last_update));
            assert_eq!(saved.buckets_usage.get("bucket-b").map(|usage| usage.objects_count), Some(3));
        }
    }

    #[tokio::test]
    async fn remove_bucket_usage_does_not_synthesize_authoritative_snapshot_when_all_missing() {
        let store = Arc::new(UsageCasStore::default());

        remove_bucket_usage_from_backend_with_store(store.as_ref(), "bucket-a")
            .await
            .expect("bucket removal should remain a no-op without a usage baseline");

        let state = store.state.lock().await;
        assert_eq!(state.put_count, 0);
        assert!(state.object.is_none());
        assert!(state.backup_object.is_none());
    }

    #[tokio::test]
    async fn remove_bucket_usage_migrates_legacy_snapshot_without_hiding_other_buckets() {
        let mut legacy = data_usage_info_for_test("bucket-a", 2, 84, SystemTime::now());
        legacy.buckets_usage.insert(
            "bucket-b".to_string(),
            BucketUsageInfo {
                objects_count: 3,
                versions_count: 3,
                size: 126,
                ..Default::default()
            },
        );
        legacy.bucket_sizes.insert("bucket-b".to_string(), 126);
        legacy.usage_snapshot_complete = false;
        legacy.buckets_count = 2;
        legacy.calculate_totals();
        let store = Arc::new(UsageCasStore {
            state: Mutex::new(UsageCasState {
                legacy_object: Some((serde_json::to_vec(&legacy).expect("legacy usage snapshot should encode"), 1)),
                ..Default::default()
            }),
        });

        remove_bucket_usage_from_backend_with_store(store.as_ref(), "bucket-a")
            .await
            .expect("bucket removal should migrate the legacy usage baseline");

        let state = store.state.lock().await;
        assert_eq!(state.put_count, 3);
        for (data, _) in [
            state.object.as_ref(),
            state.backup_object.as_ref(),
            state.legacy_object.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            let saved = serde_json::from_slice::<DataUsageInfo>(data).expect("saved usage snapshot should decode");
            assert!(!data_usage_contains_bucket(&saved, "bucket-a"));
            assert_eq!(
                saved
                    .buckets_usage
                    .get("bucket-b")
                    .map(|usage| (usage.objects_count, usage.size)),
                Some((3, 126))
            );
        }
        for (data, _) in [state.object.as_ref(), state.backup_object.as_ref()].into_iter().flatten() {
            let saved = serde_json::from_slice::<DataUsageInfo>(data).expect("migrated usage snapshot should decode");
            assert!(
                !saved.usage_snapshot_complete,
                "legacy usage must not be promoted to an authoritative snapshot"
            );
        }
    }

    #[tokio::test]
    async fn remove_bucket_usage_seeds_missing_backup_from_updated_primary() {
        let mut primary = data_usage_info_for_test("bucket-a", 2, 84, SystemTime::now());
        primary.buckets_usage.insert(
            "bucket-b".to_string(),
            BucketUsageInfo {
                objects_count: 3,
                versions_count: 3,
                size: 126,
                ..Default::default()
            },
        );
        primary.bucket_sizes.insert("bucket-b".to_string(), 126);
        primary.buckets_count = 2;
        primary.calculate_totals();
        let store = Arc::new(UsageCasStore {
            state: Mutex::new(UsageCasState {
                object: Some((serde_json::to_vec(&primary).expect("primary usage snapshot should encode"), 1)),
                ..Default::default()
            }),
        });

        remove_bucket_usage_from_backend_with_store(store.as_ref(), "bucket-a")
            .await
            .expect("bucket removal should seed a missing backup from the updated primary");

        let state = store.state.lock().await;
        assert_eq!(state.put_count, 2);
        for (data, _) in [state.object.as_ref(), state.backup_object.as_ref()].into_iter().flatten() {
            let saved = serde_json::from_slice::<DataUsageInfo>(data).expect("saved usage snapshot should decode");
            assert!(!data_usage_contains_bucket(&saved, "bucket-a"));
            assert_eq!(
                saved
                    .buckets_usage
                    .get("bucket-b")
                    .map(|usage| (usage.objects_count, usage.size)),
                Some((3, 126))
            );
        }
    }

    #[tokio::test]
    async fn remove_bucket_usage_fences_reject_stale_primary_and_backup_writes() {
        let stale = data_usage_info_for_test("bucket-a", 2, 84, SystemTime::now() - Duration::from_secs(10));
        let stale_data = serde_json::to_vec(&stale).expect("stale usage snapshot should encode");
        let store = Arc::new(UsageCasStore {
            state: Mutex::new(UsageCasState {
                object: Some((stale_data.clone(), 1)),
                ..Default::default()
            }),
        });

        remove_bucket_usage_from_backend_with_store(store.as_ref(), "bucket-a")
            .await
            .expect("bucket removal should write both usage fences");

        let mut stale_primary = PutObjReader::from_vec(stale_data.clone());
        let primary_err = store
            .put_object(
                RUSTFS_META_BUCKET,
                DATA_USAGE_OBJ_NAME_PATH.as_str(),
                &mut stale_primary,
                &ObjectOptions {
                    http_preconditions: Some(HTTPPreconditions {
                        if_match: Some("usage-1".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .expect_err("the pre-delete primary baseline must be fenced");
        assert_eq!(primary_err, Error::PreconditionFailed);

        let mut stale_backup = PutObjReader::from_vec(stale_data);
        let backup_err = store
            .put_object(
                RUSTFS_META_BUCKET,
                DATA_USAGE_OBJ_BACKUP_PATH.as_str(),
                &mut stale_backup,
                &ObjectOptions {
                    http_preconditions: Some(HTTPPreconditions {
                        if_none_match: Some("*".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .expect_err("the missing pre-delete backup baseline must be fenced");
        assert_eq!(backup_err, Error::PreconditionFailed);
    }

    #[tokio::test]
    async fn remove_bucket_usage_rejects_movement_epoch_flip_between_read_and_commit() {
        let ctx = Arc::new(InstanceContext::new());
        let publication_store = build_publication_store(ctx.clone());
        let snapshot = data_usage_info_for_test("bucket-a", 2, 84, SystemTime::now());
        let store = Arc::new(UsageCasStore {
            state: Mutex::new(UsageCasState {
                object: Some((serde_json::to_vec(&snapshot).expect("usage snapshot should encode"), 1)),
                advance_publication_epoch_after_get: Some((UsageObjectSlot::Primary, ctx)),
                ..Default::default()
            }),
        });
        let expected_epoch = publication_store
            .scanner_data_usage_publication_epoch()
            .await
            .expect("idle publication store should admit the initial read");

        let err = remove_bucket_usage_from_object_with_retries_and_publication(
            store.as_ref(),
            DATA_USAGE_OBJ_NAME_PATH.as_str(),
            "bucket-a",
            0,
            None,
            None,
            Some((publication_store.as_ref(), Some(expected_epoch))),
        )
        .await
        .expect_err("a movement epoch flip during the read must fence the commit");

        assert!(err.to_string().contains("epoch changed"));
        assert_eq!(store.state.lock().await.put_count, 0);
    }

    #[tokio::test]
    async fn remove_bucket_usage_confirms_ambiguous_committed_final_attempt() {
        let initial = data_usage_info_for_test("bucket-a", 2, 84, SystemTime::now());
        let store = Arc::new(UsageCasStore {
            state: Mutex::new(UsageCasState {
                object: Some((serde_json::to_vec(&initial).expect("initial usage snapshot should encode"), 1)),
                error_after_commit_put: Some(1),
                ..Default::default()
            }),
        });

        remove_bucket_usage_from_object_with_retries(
            store.as_ref(),
            DATA_USAGE_OBJ_NAME_PATH.as_str(),
            "bucket-a",
            0,
            None,
            None,
        )
        .await
        .expect("final-attempt read-back should confirm the ambiguous committed removal");

        let state = store.state.lock().await;
        let saved = serde_json::from_slice::<DataUsageInfo>(&state.object.as_ref().expect("usage snapshot should remain").0)
            .expect("saved usage snapshot should decode");
        assert_eq!(state.put_count, 1);
        assert!(!saved.buckets_usage.contains_key("bucket-a"));
        assert!(!saved.bucket_sizes.contains_key("bucket-a"));
    }

    #[tokio::test]
    async fn remove_bucket_usage_updates_primary_and_backup_snapshots() {
        let now = SystemTime::now();
        let primary = data_usage_info_for_test("bucket-a", 2, 84, now);
        let mut backup = primary.clone();
        backup.buckets_usage.insert(
            "bucket-b".to_string(),
            BucketUsageInfo {
                objects_count: 3,
                versions_count: 3,
                size: 126,
                ..Default::default()
            },
        );
        backup.bucket_sizes.insert("bucket-b".to_string(), 126);
        backup.buckets_count = 2;
        backup.calculate_totals();
        let store = Arc::new(UsageCasStore {
            state: Mutex::new(UsageCasState {
                object: Some((serde_json::to_vec(&primary).expect("primary usage snapshot should encode"), 1)),
                backup_object: Some((serde_json::to_vec(&backup).expect("backup usage snapshot should encode"), 1)),
                ..Default::default()
            }),
        });

        remove_bucket_usage_from_backend_with_store(store.as_ref(), "bucket-a")
            .await
            .expect("bucket removal should update both usage snapshots");

        let state = store.state.lock().await;
        let primary = serde_json::from_slice::<DataUsageInfo>(&state.object.as_ref().expect("primary snapshot should remain").0)
            .expect("primary snapshot should decode");
        let backup =
            serde_json::from_slice::<DataUsageInfo>(&state.backup_object.as_ref().expect("backup snapshot should remain").0)
                .expect("backup snapshot should decode");
        assert_eq!(state.put_count, 2);
        assert!(!data_usage_contains_bucket(&primary, "bucket-a"));
        assert!(!data_usage_contains_bucket(&backup, "bucket-a"));
        assert_eq!(
            backup
                .buckets_usage
                .get("bucket-b")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((3, 126))
        );
    }

    #[tokio::test]
    #[serial]
    async fn clear_bucket_usage_memory_prevents_deleted_bucket_overlay() {
        clear_usage_memory_cache_for_test().await;

        let persisted = data_usage_info_for_test("bucket-a", 1, 42, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_object_delete_memory("bucket-a", 42, true).await;
        clear_bucket_usage_memory("bucket-a", None)
            .await
            .expect("bucket usage memory cleanup should succeed");

        let mut response = DataUsageInfo {
            last_update: Some(SystemTime::now()),
            ..Default::default()
        };
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.buckets_count, 0);
        assert_eq!(response.objects_total_count, 0);
        assert!(!response.buckets_usage.contains_key("bucket-a"));
        assert!(!response.bucket_sizes.contains_key("bucket-a"));
    }

    #[tokio::test]
    #[serial]
    async fn memory_overlay_preserves_object_count_for_overwrite() {
        clear_usage_memory_cache_for_test().await;

        let persisted = data_usage_info_for_test("bucket-a", 1, 10, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_object_write_memory("bucket-a", Some(10), 20).await;

        let mut response = persisted.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 1);
        assert_eq!(response.objects_total_size, 20);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((1, 20))
        );
    }

    #[tokio::test]
    #[serial]
    async fn partial_usage_is_observational_not_authoritative_for_quota() {
        clear_usage_memory_cache_for_test().await;

        let mut partial = data_usage_info_for_test("bucket-a", 10, 100, SystemTime::now());
        partial.usage_snapshot_complete = false;
        partial.usage_snapshot_partial = true;
        replace_bucket_usage_memory_from_info(&partial).await;

        assert_eq!(get_bucket_usage_memory("bucket-a").await, None);
    }

    #[tokio::test]
    #[serial]
    async fn stale_quota_uses_complete_baseline_plus_positive_deltas() {
        clear_usage_memory_cache_for_test().await;

        let baseline = data_usage_info_for_test("bucket-a", 1, 100, SystemTime::now());
        replace_bucket_usage_memory_from_info(&baseline).await;
        record_bucket_object_write_memory("bucket-a", None, 25).await;

        assert_eq!(get_bucket_usage_memory("bucket-a").await, Some(125));
    }

    #[tokio::test]
    #[serial]
    async fn negative_delta_waits_for_set_reconciliation() {
        clear_usage_memory_cache_for_test().await;

        let baseline = data_usage_info_for_test("bucket-a", 1, 100, SystemTime::UNIX_EPOCH + Duration::from_secs(100));
        replace_bucket_usage_memory_from_info(&baseline).await;
        record_bucket_object_delete_memory("bucket-a", 25, true).await;

        assert_eq!(get_bucket_usage_memory("bucket-a").await, Some(100));

        // Simulate a process restart: the request-path overlay is gone, but
        // the persisted authoritative snapshot is still the pre-reconciliation
        // baseline. Quota must remain conservative until a complete scanner
        // result proves the delete.
        clear_usage_memory_cache_for_test().await;
        replace_bucket_usage_memory_from_info(&baseline).await;
        assert_eq!(get_bucket_usage_memory("bucket-a").await, Some(100));

        let reconciled = data_usage_info_for_test("bucket-a", 0, 75, SystemTime::UNIX_EPOCH + Duration::from_secs(101));
        replace_bucket_usage_memory_from_info(&reconciled).await;
        assert_eq!(get_bucket_usage_memory("bucket-a").await, Some(75));
    }

    #[tokio::test]
    #[serial]
    async fn memory_overlay_counts_versioned_overwrite_as_new_version() {
        clear_usage_memory_cache_for_test().await;

        let persisted = data_usage_info_for_test("bucket-a", 1, 10, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_object_version_write_memory("bucket-a", Some(10), 20).await;

        let mut response = persisted.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 1);
        assert_eq!(response.versions_total_count, 2);
        assert_eq!(response.objects_total_size, 30);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.versions_count, usage.size)),
            Some((1, 2, 30))
        );
    }

    /// rustfs/backlog#1009: with an unknown previous state, only the
    /// always-correct components are recorded — bytes are added and a
    /// versioned write adds a version, but objects_count never moves.
    #[tokio::test]
    #[serial]
    async fn memory_overlay_unknown_previous_adds_size_only_for_unversioned_write() {
        clear_usage_memory_cache_for_test().await;

        let persisted = data_usage_info_for_test("bucket-a", 1, 10, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_object_write_unknown_previous_memory("bucket-a", 20, false).await;

        let mut response = persisted.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 1);
        assert_eq!(response.versions_total_count, 1);
        assert_eq!(response.objects_total_size, 30);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.versions_count, usage.size)),
            Some((1, 1, 30))
        );
    }

    #[tokio::test]
    #[serial]
    async fn memory_overlay_unknown_previous_adds_size_and_version_for_versioned_write() {
        clear_usage_memory_cache_for_test().await;

        let persisted = data_usage_info_for_test("bucket-a", 1, 10, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_object_write_unknown_previous_memory("bucket-a", 20, true).await;

        let mut response = persisted.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 1);
        assert_eq!(response.versions_total_count, 2);
        assert_eq!(response.objects_total_size, 30);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.versions_count, usage.size)),
            Some((1, 2, 30))
        );
    }

    #[tokio::test]
    #[serial]
    async fn memory_overlay_records_delete_marker_without_removing_versions() {
        clear_usage_memory_cache_for_test().await;

        let persisted = data_usage_info_for_test("bucket-a", 2, 30, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_delete_marker_memory("bucket-a").await;

        let mut response = persisted.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 2);
        assert_eq!(response.versions_total_count, 2);
        assert_eq!(response.delete_markers_total_count, 1);
        assert_eq!(response.objects_total_size, 30);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| { (usage.objects_count, usage.versions_count, usage.delete_markers_count, usage.size,) }),
            Some((2, 2, 1, 30))
        );
    }

    #[tokio::test]
    #[serial]
    async fn authoritative_versioned_refresh_replaces_stale_dirty_memory() {
        clear_usage_memory_cache_for_test().await;

        let old_persisted = data_usage_info_for_test("bucket-a", 0, 0, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&old_persisted).await;
        record_bucket_object_write_memory("bucket-a", None, 15).await;

        let authoritative = BucketUsageInfo {
            objects_count: 1,
            versions_count: 1,
            delete_markers_count: 1,
            size: 10,
            ..Default::default()
        };
        replace_bucket_usage_memory_from_authoritative("bucket-a", authoritative.clone(), SystemTime::now()).await;

        let mut response = old_persisted.clone();
        response.buckets_usage.insert("bucket-a".to_string(), authoritative);
        response.bucket_sizes.insert("bucket-a".to_string(), 10);
        response.calculate_totals();

        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 1);
        assert_eq!(response.versions_total_count, 1);
        assert_eq!(response.delete_markers_total_count, 1);
        assert_eq!(response.objects_total_size, 10);
        assert_eq!(
            response.buckets_usage.get("bucket-a").map(|usage| (
                usage.objects_count,
                usage.versions_count,
                usage.delete_markers_count,
                usage.size
            )),
            Some((1, 1, 1, 10))
        );
    }

    #[tokio::test]
    #[serial]
    async fn authoritative_versioned_refresh_preserves_newer_dirty_memory() {
        clear_usage_memory_cache_for_test().await;

        let old_persisted = data_usage_info_for_test("bucket-a", 0, 0, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&old_persisted).await;
        let refresh_started = SystemTime::now() - Duration::from_secs(1);
        record_bucket_object_write_memory("bucket-a", None, 15).await;

        replace_bucket_usage_memory_from_authoritative(
            "bucket-a",
            BucketUsageInfo {
                objects_count: 1,
                versions_count: 1,
                delete_markers_count: 1,
                size: 10,
                ..Default::default()
            },
            refresh_started,
        )
        .await;

        let mut response = old_persisted.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 1);
        assert_eq!(response.versions_total_count, 1);
        assert_eq!(response.delete_markers_total_count, 0);
        assert_eq!(response.objects_total_size, 15);
        assert_eq!(
            response.buckets_usage.get("bucket-a").map(|usage| (
                usage.objects_count,
                usage.versions_count,
                usage.delete_markers_count,
                usage.size
            )),
            Some((1, 1, 0, 15))
        );
    }

    #[tokio::test]
    #[serial]
    async fn scanner_sync_preserves_dirty_delete_marker_with_later_snapshot() {
        clear_usage_memory_cache_for_test().await;

        let now = SystemTime::now();
        let old_persisted = data_usage_info_for_test("bucket-a", 2, 30, now - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&old_persisted).await;
        record_bucket_delete_marker_memory("bucket-a").await;

        let scanner_without_marker = data_usage_info_for_test("bucket-a", 2, 30, now + Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&scanner_without_marker).await;

        let mut response = scanner_without_marker.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 2);
        assert_eq!(response.versions_total_count, 2);
        assert_eq!(response.delete_markers_total_count, 1);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| { (usage.objects_count, usage.versions_count, usage.delete_markers_count, usage.size,) }),
            Some((2, 2, 1, 30))
        );
    }

    #[tokio::test]
    #[serial]
    async fn memory_overlay_does_not_replace_newer_persisted_usage() {
        clear_usage_memory_cache_for_test().await;

        let now = SystemTime::now();
        let old_persisted = data_usage_info_for_test("bucket-a", 1, 42, now - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&old_persisted).await;
        record_bucket_object_delete_memory("bucket-a", 42, true).await;

        let mut newer_persisted = data_usage_info_for_test("bucket-a", 2, 84, now + Duration::from_secs(10));
        apply_bucket_usage_memory_overlay(&mut newer_persisted).await;

        assert_eq!(newer_persisted.objects_total_count, 2);
        assert_eq!(newer_persisted.objects_total_size, 84);
        assert_eq!(
            newer_persisted
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((2, 84))
        );
    }

    #[tokio::test]
    #[serial]
    async fn scanner_snapshot_can_reduce_object_layer_refreshed_memory_usage() {
        clear_usage_memory_cache_for_test().await;

        let now = SystemTime::now();
        replace_bucket_usage_memory_from_authoritative(
            "bucket-a",
            BucketUsageInfo {
                objects_count: 100,
                versions_count: 100,
                size: 1_000,
                ..Default::default()
            },
            now,
        )
        .await;

        let scanner_decrease = data_usage_info_for_test("bucket-a", 60, 600, now + Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&scanner_decrease).await;

        let mut response = scanner_decrease.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 60);
        assert_eq!(response.objects_total_size, 600);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((60, 600))
        );
    }

    #[tokio::test]
    #[serial]
    async fn scanner_backend_snapshot_can_reduce_backend_memory_usage() {
        clear_usage_memory_cache_for_test().await;

        let now = SystemTime::now();
        let complete_snapshot = data_usage_info_for_test("bucket-a", 100, 1_000, now);
        replace_bucket_usage_memory_from_info(&complete_snapshot).await;

        let scanner_decrease = data_usage_info_for_test("bucket-a", 60, 600, now + Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&scanner_decrease).await;

        let mut response = scanner_decrease.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 60);
        assert_eq!(response.objects_total_size, 600);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((60, 600))
        );
    }

    #[tokio::test]
    #[serial]
    async fn authoritative_refresh_can_reduce_memory_usage() {
        clear_usage_memory_cache_for_test().await;

        let now = SystemTime::now();
        let complete_snapshot = data_usage_info_for_test("bucket-a", 100, 1_000, now);
        replace_bucket_usage_memory_from_info(&complete_snapshot).await;

        replace_bucket_usage_memory_from_authoritative(
            "bucket-a",
            BucketUsageInfo {
                objects_count: 60,
                versions_count: 60,
                size: 600,
                ..Default::default()
            },
            now + Duration::from_secs(10),
        )
        .await;

        let mut response = data_usage_info_for_test("bucket-a", 60, 600, now + Duration::from_secs(10));
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 60);
        assert_eq!(response.objects_total_size, 600);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((60, 600))
        );
    }

    #[tokio::test]
    #[serial]
    async fn scanner_sync_preserves_newer_memory_update() {
        clear_usage_memory_cache_for_test().await;

        let now = SystemTime::now();
        let old_persisted = data_usage_info_for_test("bucket-a", 1, 42, now - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&old_persisted).await;
        record_bucket_object_delete_memory("bucket-a", 42, true).await;

        let scanner_snapshot = data_usage_info_for_test("bucket-a", 1, 42, now - Duration::from_secs(5));
        replace_bucket_usage_memory_from_info(&scanner_snapshot).await;

        let mut response = scanner_snapshot.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 0);
        assert_eq!(response.objects_total_size, 0);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((0, 0))
        );
    }

    #[tokio::test]
    #[serial]
    async fn scanner_sync_preserves_dirty_memory_update_with_later_partial_snapshot() {
        clear_usage_memory_cache_for_test().await;

        let now = SystemTime::now();
        let old_persisted = data_usage_info_for_test("bucket-a", 900, 9_000, now - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&old_persisted).await;

        for _ in 0..100 {
            record_bucket_object_write_memory("bucket-a", None, 10).await;
        }

        let scanner_partial = data_usage_info_for_test("bucket-a", 950, 9_500, now + Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&scanner_partial).await;

        let mut response = scanner_partial.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 1000);
        assert_eq!(response.objects_total_size, 10_000);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((1000, 10_000))
        );
    }

    // --- CompressionTotalState tests ---

    /// Reset the compression total cache to a known state for isolated tests.
    async fn reset_compression_cache(state: CompressionTotalState) {
        *COMPRESSION_TOTAL_MEMORY_CACHE.write().await = Some(state);
    }

    #[test]
    fn compression_state_default_values() {
        let state = CompressionTotalState::default();
        assert!(!state.inited, "default state should not be inited");
        assert_eq!(state.ops_since_persist, 0);
        assert_eq!(state.info.original_bytes_total, 0);
        assert_eq!(state.info.compressed_bytes_total, 0);
        assert_eq!(state.info.compression_operations_total, 0);
    }

    #[tokio::test]
    #[serial]
    async fn record_compression_skips_when_not_inited() {
        let mut pre_state = CompressionTotalState::default();
        // Set known values that should remain unchanged when inited=false
        pre_state.info.original_bytes_total = 42;
        pre_state.info.compressed_bytes_total = 21;
        pre_state.info.compression_operations_total = 7;
        pre_state.ops_since_persist = 5;
        // inited is already false from default()

        reset_compression_cache(pre_state.clone()).await;

        // Call with sizes that would be accumulated if inited were true
        record_compression_total_memory(100, 50).await;

        let guard = COMPRESSION_TOTAL_MEMORY_CACHE.read().await;
        let state = guard.as_ref().expect("cache should contain a state");
        assert!(!state.inited);
        assert_eq!(state.info.original_bytes_total, 42, "original should be unchanged");
        assert_eq!(state.info.compressed_bytes_total, 21, "compressed should be unchanged");
        assert_eq!(state.info.compression_operations_total, 7, "operations should be unchanged");
        assert_eq!(state.ops_since_persist, 5, "ops_since_persist should be unchanged");
    }

    #[tokio::test]
    #[serial]
    async fn record_compression_accumulates_totals() {
        let state = CompressionTotalState {
            info: CompressionTotalInfo::default(),
            ops_since_persist: 0,
            last_persist: tokio::time::Instant::now(),
            inited: true,
        };

        reset_compression_cache(state).await;

        for _ in 0..3 {
            record_compression_total_memory(100, 50).await;
        }

        let guard = COMPRESSION_TOTAL_MEMORY_CACHE.read().await;
        let state = guard.as_ref().expect("cache should contain a state");
        assert!(state.inited);
        assert_eq!(state.info.original_bytes_total, 300);
        assert_eq!(state.info.compressed_bytes_total, 150);
        assert_eq!(state.info.compression_operations_total, 3);
        assert_eq!(state.ops_since_persist, 3);
    }

    #[tokio::test]
    #[serial]
    async fn record_compression_triggers_persist_on_batch_full() {
        // Simulate 99 previous records, so the next (100th) hits the batch threshold.
        let state = CompressionTotalState {
            info: CompressionTotalInfo {
                original_bytes_total: 9900,
                compressed_bytes_total: 4950,
                compression_operations_total: 99,
            },
            ops_since_persist: 99,
            last_persist: tokio::time::Instant::now(),
            inited: true,
        };

        reset_compression_cache(state).await;

        // 100th record — should trigger the debounce flush.
        record_compression_total_memory(100, 50).await;

        let guard = COMPRESSION_TOTAL_MEMORY_CACHE.read().await;
        let state = guard.as_ref().expect("cache should contain a state");

        // Totals are correctly accumulated (not reset by the flush).
        assert_eq!(state.info.original_bytes_total, 10_000); // 9900 + 100
        assert_eq!(state.info.compressed_bytes_total, 5_000); // 4950 + 50
        assert_eq!(state.info.compression_operations_total, 100); // 99 + 1

        // Debounce counter is reset after the persist trigger.
        assert_eq!(state.ops_since_persist, 0);

        // last_persist was refreshed; it should be very recent.
        assert!(
            state.last_persist.elapsed() < Duration::from_secs(3),
            "last_persist should have been refreshed to now"
        );

        // try_flush_compression_total is spawned asynchronously here.
        // In a unit test, runtime_sources::object_store_handle() returns None,
        // so the spawned task will hit the "object store not initialized" error
        // path and return gracefully. The state mutation (counter reset + totals
        // accumulation) is the critical behaviour verified above.
    }
}
