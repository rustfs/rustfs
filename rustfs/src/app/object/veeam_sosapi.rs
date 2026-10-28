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

//! Veeam SOSAPI virtual objects.
//!
//! Authenticated GET and HEAD of two exact bucket-root keys return generated
//! XML. The objects are not stored: no `xl.meta`, version, or Object Lock
//! interaction. Mutations of those keys are rejected while the feature is on.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use super::super::storage_api::s3::{S3Error, S3ErrorCode, S3Result};
use bytes::Bytes;
use md5::Digest as _;
use md5::Md5;
use moka::{future::Cache as AsyncCache, sync::Cache as SyncCache};
use time::OffsetDateTime;
use tracing::debug;

use super::super::storage_api::object_usecase::data_usage::load_bucket_usage_from_backend_cached;
use super::super::storage_api::object_usecase::{ECStore, StorageObjectInfo as ObjectInfo, check_preconditions};
use super::HTTPRangeSpec;
use super::shared::{LOG_COMPONENT_APP, LOG_SUBSYSTEM_OBJECT};
use crate::app::storage_api::admin_usecase::contract::StorageAdminApi;
use crate::app::storage_api::object_usecase::bucket::quota::{BucketQuota, QuotaType};
use http::HeaderMap;
use uuid::Uuid;

pub(super) const SYSTEM_KEY: &str = ".system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml";
pub(super) const CAPACITY_KEY: &str = ".system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml";
pub(super) const CONTENT_TYPE: &str = "application/xml";

const CAPACITY_XML_CACHE_TTL: Duration = Duration::from_secs(12);
const CAPACITY_HISTORY_IDLE: Duration = Duration::from_secs(3600);
const CAPACITY_CACHE_ENTRIES: u64 = 1024;
const CLUSTER_CACHE_ENTRIES: u64 = 16;
const MAX_BUCKET_REFRESHES: usize = 64;
const MAX_CLUSTER_REFRESHES: usize = 16;
const EVENT_USAGE_MISSING: &str = "veeam_sosapi_usage_missing";
const EVENT_QUOTA_UNAVAILABLE: &str = "veeam_sosapi_quota_unavailable";

static LOGGED_USAGE_MISSING: AtomicBool = AtomicBool::new(false);
static LOGGED_QUOTA_UNAVAILABLE: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SosapiKind {
    System,
    Capacity,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SosapiReadError {
    InvalidRange,
    PartNumber,
}

pub(super) struct SosapiRepresentation {
    pub body: Bytes,
    pub content_length: i64,
    pub content_range: Option<String>,
    pub etag_hex: String,
    pub last_modified: OffsetDateTime,
}

pub(super) struct CapacityInputs {
    pub used_bucket: u64,
    pub hard_quota: Option<u64>,
    pub capacity_cluster: u64,
    pub free_cluster: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CapacityFigures {
    pub capacity: u64,
    pub used: u64,
    pub available: u64,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct CapacityCacheKey {
    store_id: Uuid,
    bucket: String,
    incarnation: Uuid,
}

#[derive(Clone)]
struct CachedXml {
    bytes: Bytes,
    etag_hex: String,
    modified_at: OffsetDateTime,
}

// Bucket refreshes may wait for a cluster permit; cluster probes never acquire a bucket permit.
struct CapacityCaches {
    current: AsyncCache<CapacityCacheKey, CachedXml>,
    history: SyncCache<CapacityCacheKey, CachedXml>,
    refresh_permits: tokio::sync::Semaphore,
}

impl CapacityCaches {
    fn new() -> Self {
        Self {
            current: AsyncCache::builder()
                .max_capacity(CAPACITY_CACHE_ENTRIES)
                .time_to_live(CAPACITY_XML_CACHE_TTL)
                .build(),
            history: SyncCache::builder()
                .max_capacity(CAPACITY_CACHE_ENTRIES)
                .time_to_idle(CAPACITY_HISTORY_IDLE)
                .build(),
            refresh_permits: tokio::sync::Semaphore::new(MAX_BUCKET_REFRESHES),
        }
    }
}

struct StableXml {
    bytes: Bytes,
    etag_hex: String,
    modified_at: OffsetDateTime,
}

pub(super) fn match_sosapi_key(key: &str) -> Option<SosapiKind> {
    match key {
        SYSTEM_KEY => Some(SosapiKind::System),
        CAPACITY_KEY => Some(SosapiKind::Capacity),
        _ => None,
    }
}

pub(super) fn veeam_sosapi_enabled() -> bool {
    rustfs_utils::get_env_bool(rustfs_config::ENV_VEEAM_SOSAPI_ENABLE, rustfs_config::DEFAULT_VEEAM_SOSAPI_ENABLE)
}

/// SOSAPI key whose virtual read should run. `None` when the key is ordinary
/// or the operator turned the feature off.
pub(super) fn handled_kind(key: &str) -> Option<SosapiKind> {
    let kind = match_sosapi_key(key)?;
    veeam_sosapi_enabled().then_some(kind)
}

/// True only for an exact SOSAPI key while the feature is enabled.
///
/// Callers use this on the mutation hot path: a miss is two string compares
/// and does not read the object store.
pub(super) fn mutation_blocked(key: &str) -> bool {
    match_sosapi_key(key).is_some() && veeam_sosapi_enabled()
}

pub(super) fn capacity_figures(inputs: CapacityInputs) -> CapacityFigures {
    let used_bucket = inputs.used_bucket;
    let capacity = inputs
        .hard_quota
        .filter(|quota| *quota > 0)
        .unwrap_or(inputs.capacity_cluster);
    let used = used_bucket.min(capacity);
    let available = capacity.saturating_sub(used).min(inputs.free_cluster);
    CapacityFigures {
        capacity,
        used,
        available,
    }
}

/// Full generated document. Range and part-number checks happen after read
/// preconditions so a failing `If-Match` stays 412.
async fn generated(store: &Arc<ECStore>, bucket: &str, kind: SosapiKind) -> S3Result<(Bytes, String, OffsetDateTime)> {
    match kind {
        SosapiKind::System => {
            let stable = system_xml_stable();
            Ok((stable.bytes.clone(), stable.etag_hex.clone(), stable.modified_at))
        }
        SosapiKind::Capacity => cached_or_build_capacity(store, bucket).await,
    }
}

pub(super) async fn read_representation(
    store: &Arc<ECStore>,
    bucket: &str,
    kind: SosapiKind,
    headers: &HeaderMap,
    part_number: Option<usize>,
    range: Option<&HTTPRangeSpec>,
) -> S3Result<SosapiRepresentation> {
    let (full, etag_hex, last_modified) = generated(store, bucket, kind).await?;
    check_preconditions(
        headers,
        &ObjectInfo {
            etag: Some(etag_hex.clone()),
            mod_time: Some(last_modified),
            ..Default::default()
        },
    )?;
    representation_from(full, etag_hex, last_modified, part_number, range).map_err(|err| match err {
        SosapiReadError::InvalidRange => {
            S3Error::with_message(S3ErrorCode::InvalidRange, "The requested range is not satisfiable")
        }
        SosapiReadError::PartNumber => {
            S3Error::with_message(S3ErrorCode::InvalidArgument, "partNumber is not supported for Veeam SOSAPI objects")
        }
    })
}

fn representation_from(
    full: Bytes,
    etag_hex: String,
    last_modified: OffsetDateTime,
    part_number: Option<usize>,
    range: Option<&HTTPRangeSpec>,
) -> Result<SosapiRepresentation, SosapiReadError> {
    if part_number.is_some() {
        return Err(SosapiReadError::PartNumber);
    }
    apply_range(full, etag_hex, last_modified, range)
}

fn apply_range(
    full: Bytes,
    etag_hex: String,
    last_modified: OffsetDateTime,
    range: Option<&HTTPRangeSpec>,
) -> Result<SosapiRepresentation, SosapiReadError> {
    let total = i64::try_from(full.len()).unwrap_or(i64::MAX);
    let Some(range) = range else {
        return Ok(SosapiRepresentation {
            content_length: total,
            content_range: None,
            body: full,
            etag_hex,
            last_modified,
        });
    };
    let (start, length) = range.get_offset_length(total).map_err(|_| SosapiReadError::InvalidRange)?;
    let length_usize = usize::try_from(length).map_err(|_| SosapiReadError::InvalidRange)?;
    let end = start.checked_add(length_usize).ok_or(SosapiReadError::InvalidRange)?;
    if end > full.len() {
        return Err(SosapiReadError::InvalidRange);
    }
    let start_i64 = i64::try_from(start).map_err(|_| SosapiReadError::InvalidRange)?;
    let end_inclusive = start_i64 + length - 1;
    Ok(SosapiRepresentation {
        body: full.slice(start..end),
        content_length: length,
        content_range: Some(format!("bytes {start_i64}-{end_inclusive}/{total}")),
        etag_hex,
        last_modified,
    })
}

fn system_xml_stable() -> &'static StableXml {
    static BODY: OnceLock<StableXml> = OnceLock::new();
    BODY.get_or_init(|| {
        let bytes = Bytes::from(system_xml(env!("CARGO_PKG_VERSION")));
        let etag_hex = md5_hex(&bytes);
        StableXml {
            bytes,
            etag_hex,
            modified_at: OffsetDateTime::from_unix_timestamp(crate::version::build::BUILD_TIMESTAMP)
                .unwrap_or(OffsetDateTime::UNIX_EPOCH),
        }
    })
}

fn system_xml(version: &str) -> String {
    let version = xml_escape_text(version);
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<SystemInfo>\n  <ProtocolVersion>\"1.0\"</ProtocolVersion>\n  <ModelName>\"RustFS {version}\"</ModelName>\n  <ProtocolCapabilities>\n    <CapacityInfo>true</CapacityInfo>\n    <UploadSessions>false</UploadSessions>\n    <IAMSTS>false</IAMSTS>\n  </ProtocolCapabilities>\n</SystemInfo>\n"
    )
}

fn capacity_xml(figures: &CapacityFigures) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CapacityInfo>\n  <Capacity>{}</Capacity>\n  <Available>{}</Available>\n  <Used>{}</Used>\n</CapacityInfo>\n",
        figures.capacity, figures.available, figures.used,
    )
}

/// Escape XML text. Quotes stay as ASCII quotes: Veeam's parser requires that
/// spelling inside `ProtocolVersion` and `ModelName`.
fn xml_escape_text(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn md5_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Md5::digest(bytes);
    let mut out = String::with_capacity(32);
    for byte in digest {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

fn capacity_caches() -> &'static CapacityCaches {
    static CACHES: OnceLock<CapacityCaches> = OnceLock::new();
    CACHES.get_or_init(CapacityCaches::new)
}

async fn cached_or_build_capacity(store: &Arc<ECStore>, bucket: &str) -> S3Result<(Bytes, String, OffsetDateTime)> {
    let incarnation = store
        .bucket_incarnation_id_from_disk(bucket)
        .await
        .map_err(|_| capacity_unavailable())?;
    let key = CapacityCacheKey {
        store_id: store.id,
        bucket: bucket.to_owned(),
        incarnation,
    };
    cached_or_load_capacity(capacity_caches(), &key, || async {
        let inputs = load_capacity_inputs(store, bucket, incarnation).await?;
        if store
            .bucket_incarnation_id_from_disk(bucket)
            .await
            .map_err(|_| capacity_unavailable())?
            != incarnation
        {
            return Err(capacity_unavailable());
        }
        Ok(capacity_figures(inputs))
    })
    .await
}

async fn cached_or_load_capacity<F, Fut>(
    caches: &CapacityCaches,
    key: &CapacityCacheKey,
    load: F,
) -> S3Result<(Bytes, String, OffsetDateTime)>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = S3Result<CapacityFigures>>,
{
    let value = caches
        .current
        .try_get_with_by_ref(key, async {
            let _permit = caches.refresh_permits.acquire().await.map_err(|_| capacity_unavailable())?;
            let bytes = Bytes::from(capacity_xml(&load().await?));
            let etag_hex = md5_hex(&bytes);
            let modified_at = caches
                .history
                .get(key)
                .filter(|entry| entry.bytes == bytes)
                .map_or_else(OffsetDateTime::now_utc, |entry| entry.modified_at);
            let value = CachedXml {
                bytes,
                etag_hex,
                modified_at,
            };
            caches.history.insert(key.clone(), value.clone());
            Ok::<_, S3Error>(value)
        })
        .await
        .map_err(|_| capacity_unavailable())?;
    Ok((value.bytes, value.etag_hex, value.modified_at))
}

fn capacity_unavailable() -> S3Error {
    S3Error::with_message(S3ErrorCode::ServiceUnavailable, "Veeam SOSAPI capacity is temporarily unavailable")
}

async fn load_capacity_inputs(store: &Arc<ECStore>, bucket: &str, incarnation: Uuid) -> S3Result<CapacityInputs> {
    let (quota, quota_incarnation, _) = store.bucket_quota_and_incarnation_from_disk(bucket).await.map_err(|_| {
        log_quota_unavailable_once(bucket);
        capacity_unavailable()
    })?;
    if quota_incarnation != incarnation {
        return Err(capacity_unavailable());
    }
    let used_bucket = load_bucket_usage_from_backend_cached(Arc::clone(store), bucket, incarnation)
        .await
        .ok()
        .flatten()
        .ok_or_else(|| {
            log_usage_missing_once(bucket);
            capacity_unavailable()
        })?;
    let (capacity_cluster, free_cluster) = cached_cluster_capacity(store).await?;
    Ok(CapacityInputs {
        used_bucket,
        hard_quota: quota.as_ref().and_then(hard_quota_bytes),
        capacity_cluster,
        free_cluster,
    })
}

fn hard_quota_bytes(quota: &BucketQuota) -> Option<u64> {
    match quota.quota_type {
        QuotaType::Hard => quota.quota.filter(|size| *size > 0),
    }
}

struct ClusterCapacityCache {
    current: AsyncCache<Uuid, (u64, u64)>,
    refresh_permits: tokio::sync::Semaphore,
}

impl ClusterCapacityCache {
    fn new() -> Self {
        Self {
            current: AsyncCache::builder()
                .max_capacity(CLUSTER_CACHE_ENTRIES)
                .time_to_live(CAPACITY_XML_CACHE_TTL)
                .build(),
            refresh_permits: tokio::sync::Semaphore::new(MAX_CLUSTER_REFRESHES),
        }
    }
}

fn cluster_capacity_cache() -> &'static ClusterCapacityCache {
    static CACHE: OnceLock<ClusterCapacityCache> = OnceLock::new();
    CACHE.get_or_init(ClusterCapacityCache::new)
}

async fn cached_cluster_capacity(store: &Arc<ECStore>) -> S3Result<(u64, u64)> {
    cached_cluster_capacity_with(cluster_capacity_cache(), store.id, || async {
        let info = StorageAdminApi::storage_info(store.as_ref()).await;
        if !cluster_capacity_observations_are_live(&info) {
            return Err(capacity_unavailable());
        }
        conservative_cluster_capacity(&info).ok_or_else(capacity_unavailable)
    })
    .await
}

fn cluster_capacity_observations_are_live(info: &rustfs_madmin::StorageInfo) -> bool {
    !info.disks.is_empty()
        && !info.observations.is_empty()
        && info
            .observations
            .iter()
            .all(|observation| !observation.cached && observation.status == rustfs_madmin::StorageInfoProbeStatus::Succeeded)
        && info.disks.iter().all(|disk| {
            (disk.state.eq_ignore_ascii_case("ok") || disk.state.eq_ignore_ascii_case("online"))
                && disk.capacity_observation_source.as_deref() == Some("live_probe")
        })
}

async fn cached_cluster_capacity_with<F, Fut>(cache: &ClusterCapacityCache, store_id: Uuid, load: F) -> S3Result<(u64, u64)>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = S3Result<(u64, u64)>>,
{
    cache
        .current
        .try_get_with(store_id, async {
            let _permit = cache.refresh_permits.acquire().await.map_err(|_| capacity_unavailable())?;
            load().await
        })
        .await
        .map_err(|_| capacity_unavailable())
}

/// A set can consume only the space available on each of its shard disks.
/// This remains an estimate when multiple shard paths share a backing device.
fn conservative_cluster_capacity(info: &rustfs_madmin::StorageInfo) -> Option<(u64, u64)> {
    let backend = &info.backend;
    let pools = backend.drives_per_set.len();
    if pools == 0
        || backend.total_sets.len() != pools
        || backend.standard_sc_data.len() != pools
        || backend.standard_sc_parities.len() != pools
    {
        return None;
    }
    let expected = backend
        .drives_per_set
        .iter()
        .zip(&backend.total_sets)
        .try_fold(0usize, |sum, (&width, &sets)| sum.checked_add(width.checked_mul(sets)?))?;
    if expected != info.disks.len() {
        return None;
    }
    let mut seen = HashSet::with_capacity(expected);
    let mut sets: HashMap<(usize, usize), (u64, u64, usize)> = HashMap::new();
    for disk in &info.disks {
        let pool = usize::try_from(disk.pool_index).ok()?;
        let set = usize::try_from(disk.set_index).ok()?;
        let index = usize::try_from(disk.disk_index).ok()?;
        let width = *backend.drives_per_set.get(pool)?;
        if width == 0 || set >= *backend.total_sets.get(pool)? || index >= width || !seen.insert((pool, set, index)) {
            return None;
        }
        let entry = sets.entry((pool, set)).or_insert((u64::MAX, u64::MAX, 0));
        entry.0 = entry.0.min(disk.total_space);
        entry.1 = entry.1.min(disk.available_space);
        entry.2 = entry.2.checked_add(1)?;
    }
    let mut total = 0u64;
    let mut free = 0u64;
    for ((pool, _), (min_total, min_free, count)) in sets {
        let width = backend.drives_per_set[pool];
        let data = backend.standard_sc_data[pool];
        if count != width
            || data == 0
            || data.checked_add(backend.standard_sc_parities[pool]) != Some(width)
            || min_total == 0
            || min_free > min_total
        {
            return None;
        }
        let data = u64::try_from(data).ok()?;
        total = total.checked_add(min_total.checked_mul(data)?)?;
        free = free.checked_add(min_free.checked_mul(data)?)?;
    }
    Some((total, free))
}

fn log_usage_missing_once(bucket: &str) {
    if LOGGED_USAGE_MISSING
        .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
    {
        debug!(
            event = EVENT_USAGE_MISSING,
            component = LOG_COMPONENT_APP,
            subsystem = LOG_SUBSYSTEM_OBJECT,
            state = "snapshot_missing",
            bucket = %bucket,
            "Veeam SOSAPI capacity is unavailable because the data-usage snapshot is missing"
        );
    }
}

fn log_quota_unavailable_once(bucket: &str) {
    if LOGGED_QUOTA_UNAVAILABLE
        .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
    {
        debug!(
            event = EVENT_QUOTA_UNAVAILABLE,
            component = LOG_COMPONENT_APP,
            subsystem = LOG_SUBSYSTEM_OBJECT,
            state = "quota_unavailable",
            bucket = %bucket,
            "Veeam SOSAPI capacity is unavailable because the bucket quota could not be read"
        );
    }
}

#[cfg(test)]
mod tests;
