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
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use super::super::storage_api::s3::{S3Error, S3ErrorCode, S3Result};
use bytes::Bytes;
use md5::Digest as _;
use md5::Md5;
use moka::sync::Cache;
use time::OffsetDateTime;
use tracing::debug;

use super::super::storage_api::object_usecase::ECStore;
use super::super::storage_api::object_usecase::data_usage::load_bucket_usage_from_backend_cached;
use super::HTTPRangeSpec;
use super::shared::{LOG_COMPONENT_APP, LOG_SUBSYSTEM_OBJECT};
use crate::app::storage_api::admin_usecase::contract::StorageAdminApi;
use crate::app::storage_api::object_usecase::bucket::quota::{BucketQuota, QuotaType};
use uuid::Uuid;

pub(super) const SYSTEM_KEY: &str = ".system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml";
pub(super) const CAPACITY_KEY: &str = ".system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml";
pub(super) const CONTENT_TYPE: &str = "application/xml";

const CAPACITY_XML_CACHE_TTL: Duration = Duration::from_secs(12);
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
pub(super) enum SosapiReadError {
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

#[derive(Clone)]
struct CapacityCacheEntry {
    store_id: Uuid,
    bucket_incarnation: Uuid,
    bytes: Bytes,
    etag_hex: String,
    generated_at: OffsetDateTime,
    stored_at: Instant,
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
pub(super) async fn generated(store: &Arc<ECStore>, bucket: &str, kind: SosapiKind) -> S3Result<(Bytes, String, OffsetDateTime)> {
    match kind {
        SosapiKind::System => {
            let stable = system_xml_stable();
            Ok((stable.bytes.clone(), stable.etag_hex.clone(), stable.modified_at))
        }
        SosapiKind::Capacity => cached_or_build_capacity(store, bucket).await,
    }
}

pub(super) fn representation_from(
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

fn capacity_cache() -> &'static Cache<String, CapacityCacheEntry> {
    static CACHE: OnceLock<Cache<String, CapacityCacheEntry>> = OnceLock::new();
    CACHE.get_or_init(|| {
        Cache::builder()
            .max_capacity(1024)
            .time_to_idle(Duration::from_secs(3600))
            .build()
    })
}

fn capacity_refresh_lock(bucket: &str) -> &'static tokio::sync::Mutex<()> {
    static LOCKS: OnceLock<[tokio::sync::Mutex<()>; 64]> = OnceLock::new();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bucket.hash(&mut hasher);
    &LOCKS.get_or_init(|| std::array::from_fn(|_| tokio::sync::Mutex::new(())))[(hasher.finish() % 64) as usize]
}

fn cached_capacity(bucket: &str, store_id: Uuid, incarnation: Uuid) -> Option<(Bytes, String, OffsetDateTime)> {
    let entry = capacity_cache().get(bucket)?;
    if entry.store_id != store_id
        || entry.bucket_incarnation != incarnation
        || entry.stored_at.elapsed() >= CAPACITY_XML_CACHE_TTL
    {
        return None;
    }
    Some((entry.bytes, entry.etag_hex, entry.generated_at))
}

async fn cached_or_build_capacity(store: &Arc<ECStore>, bucket: &str) -> S3Result<(Bytes, String, OffsetDateTime)> {
    let incarnation = store
        .bucket_incarnation_id_from_disk(bucket)
        .await
        .map_err(|_| capacity_unavailable())?;
    cached_or_load_capacity(bucket, store.id, incarnation, || async {
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
    bucket: &str,
    store_id: Uuid,
    incarnation: Uuid,
    load: F,
) -> S3Result<(Bytes, String, OffsetDateTime)>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = S3Result<CapacityFigures>>,
{
    if let Some(hit) = cached_capacity(bucket, store_id, incarnation) {
        return Ok(hit);
    }
    let _refresh = capacity_refresh_lock(bucket).lock().await;
    if let Some(hit) = cached_capacity(bucket, store_id, incarnation) {
        return Ok(hit);
    }
    let bytes = Bytes::from(capacity_xml(&load().await?));
    let etag_hex = md5_hex(&bytes);
    let generated_at = capacity_cache()
        .get(bucket)
        .filter(|entry| entry.store_id == store_id && entry.bucket_incarnation == incarnation && entry.bytes == bytes)
        .map_or_else(OffsetDateTime::now_utc, |entry| entry.generated_at);
    capacity_cache().insert(
        bucket.to_owned(),
        CapacityCacheEntry {
            store_id,
            bucket_incarnation: incarnation,
            bytes: bytes.clone(),
            etag_hex: etag_hex.clone(),
            generated_at,
            stored_at: Instant::now(),
        },
    );
    Ok((bytes, etag_hex, generated_at))
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

struct ClusterCapacity {
    store_id: Uuid,
    total: u64,
    free: u64,
    stored_at: Instant,
}

async fn cached_cluster_capacity(store: &Arc<ECStore>) -> S3Result<(u64, u64)> {
    static CACHE: OnceLock<tokio::sync::Mutex<Option<ClusterCapacity>>> = OnceLock::new();
    cached_cluster_capacity_with(CACHE.get_or_init(|| tokio::sync::Mutex::new(None)), store.id, || async {
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

async fn cached_cluster_capacity_with<F, Fut>(
    cache: &tokio::sync::Mutex<Option<ClusterCapacity>>,
    store_id: Uuid,
    load: F,
) -> S3Result<(u64, u64)>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = S3Result<(u64, u64)>>,
{
    let mut cached = cache.lock().await;
    if let Some(entry) = cached
        .as_ref()
        .filter(|entry| entry.store_id == store_id && entry.stored_at.elapsed() < CAPACITY_XML_CACHE_TTL)
    {
        return Ok((entry.total, entry.free));
    }
    let (total, free) = load().await?;
    *cached = Some(ClusterCapacity {
        store_id,
        total,
        free,
        stored_at: Instant::now(),
    });
    Ok((total, free))
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
mod tests {
    use super::*;

    fn element_text<'a>(xml: &'a str, element: &str) -> &'a str {
        let start = format!("<{element}>");
        let end = format!("</{element}>");
        let start_idx = xml.find(&start).unwrap_or_else(|| panic!("missing {element}")) + start.len();
        let rest = &xml[start_idx..];
        let end_idx = rest.find(&end).unwrap_or_else(|| panic!("unclosed {element}"));
        &rest[..end_idx]
    }

    #[test]
    fn matches_only_the_exact_sosapi_keys() {
        assert_eq!(match_sosapi_key(SYSTEM_KEY), Some(SosapiKind::System));
        assert_eq!(match_sosapi_key(CAPACITY_KEY), Some(SosapiKind::Capacity));
        assert_eq!(match_sosapi_key(&format!("{SYSTEM_KEY}/")), None);
        assert_eq!(match_sosapi_key(".system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/"), None);
        assert_eq!(match_sosapi_key("Veeam/Backup/job.dat"), None);
        assert_eq!(match_sosapi_key(&format!("prefix/{SYSTEM_KEY}")), None);
    }

    #[test]
    #[serial_test::serial]
    fn flag_off_leaves_the_keys_unhandled() {
        temp_env::with_var(rustfs_config::ENV_VEEAM_SOSAPI_ENABLE, Some("false"), || {
            assert!(handled_kind(SYSTEM_KEY).is_none());
            assert!(!mutation_blocked(CAPACITY_KEY));
        });
        temp_env::with_var_unset(rustfs_config::ENV_VEEAM_SOSAPI_ENABLE, || {
            assert_eq!(handled_kind(SYSTEM_KEY), Some(SosapiKind::System));
            assert!(mutation_blocked(CAPACITY_KEY));
        });
    }

    #[test]
    fn system_xml_keeps_minio_quote_spelling_and_omits_optional_blocks() {
        let xml = system_xml("1.0.2");
        assert_eq!(element_text(&xml, "ProtocolVersion"), "\"1.0\"");
        assert_eq!(element_text(&xml, "ModelName"), "\"RustFS 1.0.2\"");
        assert_eq!(element_text(&xml, "CapacityInfo"), "true");
        assert_eq!(element_text(&xml, "UploadSessions"), "false");
        assert_eq!(element_text(&xml, "IAMSTS"), "false");
        assert!(xml.contains("<ProtocolCapabilities>"));
        assert!(!xml.contains("SystemRecommendations"));
        assert!(!xml.contains("APIEndpoints"));
        assert!(!xml.contains("&quot;"));
        let escaped = system_xml("1<2>&");
        assert!(escaped.contains("\"RustFS 1&lt;2&gt;&amp;\""));
    }

    #[test]
    fn capacity_math_clamps_used_and_available() {
        let quota = capacity_figures(CapacityInputs {
            used_bucket: 250,
            hard_quota: Some(1_000),
            capacity_cluster: 9_000,
            free_cluster: 8_000,
        });
        assert_eq!(
            quota,
            CapacityFigures {
                capacity: 1_000,
                used: 250,
                available: 750,
            }
        );

        let cluster = capacity_figures(CapacityInputs {
            used_bucket: 100,
            hard_quota: None,
            capacity_cluster: 5_000,
            free_cluster: 4_000,
        });
        assert_eq!(cluster.capacity, 5_000);
        assert_eq!(cluster.used, 100);
        assert_eq!(cluster.available, 4_000);

        let saturated = capacity_figures(CapacityInputs {
            used_bucket: 9_000,
            hard_quota: Some(1_000),
            capacity_cluster: 50_000,
            free_cluster: 40_000,
        });
        assert_eq!(saturated.used, 1_000);
        assert_eq!(saturated.available, 0);

        let free_clamp = capacity_figures(CapacityInputs {
            used_bucket: 100,
            hard_quota: Some(10_000),
            capacity_cluster: 50_000,
            free_cluster: 50,
        });
        assert_eq!(free_clamp.available, 50);
        assert!(free_clamp.available <= free_clamp.capacity - free_clamp.used);

        let zero_quota = capacity_figures(CapacityInputs {
            used_bucket: 10,
            hard_quota: Some(0),
            capacity_cluster: 400,
            free_cluster: 300,
        });
        assert_eq!(zero_quota.capacity, 400);
    }

    #[tokio::test]
    async fn concurrent_capacity_refreshes_share_one_load_and_preserve_modification_time() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let bucket = format!("veeam-cache-{}", super::super::Uuid::new_v4());
        let store_id = Uuid::new_v4();
        let incarnation = Uuid::new_v4();
        let calls = AtomicUsize::new(0);
        let figures = || CapacityFigures {
            capacity: 100,
            used: 20,
            available: 80,
        };
        let load = || async {
            calls.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            Ok(figures())
        };
        let (first, second) = tokio::join!(
            cached_or_load_capacity(&bucket, store_id, incarnation, load),
            cached_or_load_capacity(&bucket, store_id, incarnation, load)
        );
        let first = first.expect("first refresh");
        let second = second.expect("coalesced refresh");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(first, second);

        let mut expired = capacity_cache().get(&bucket).expect("cached entry");
        expired.stored_at = Instant::now() - CAPACITY_XML_CACHE_TTL;
        capacity_cache().insert(bucket.clone(), expired);
        let unchanged = cached_or_load_capacity(&bucket, store_id, incarnation, load)
            .await
            .expect("refresh unchanged content");
        assert_eq!(first, unchanged);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let replacement = Uuid::new_v4();
        let replacement_value = cached_or_load_capacity(&bucket, store_id, replacement, load)
            .await
            .expect("new incarnation");
        assert_eq!(calls.load(Ordering::SeqCst), 3, "new incarnation must not hit the old cache");
        assert_eq!(replacement_value.0, first.0);
        let different_store = cached_or_load_capacity(&bucket, Uuid::new_v4(), replacement, load)
            .await
            .expect("other store");
        assert_eq!(calls.load(Ordering::SeqCst), 4, "other stores must not share cached values");
        assert_eq!(different_store.0, first.0);
        let _ = cached_or_load_capacity(&bucket, store_id, incarnation, load)
            .await
            .expect("restore test identity");

        let mut expired = capacity_cache().get(&bucket).expect("refreshed entry");
        expired.stored_at = Instant::now() - CAPACITY_XML_CACHE_TTL;
        capacity_cache().insert(bucket.clone(), expired);
        let changed = cached_or_load_capacity(&bucket, store_id, incarnation, || async {
            Ok(CapacityFigures {
                capacity: 100,
                used: 30,
                available: 70,
            })
        })
        .await
        .expect("refresh changed content");
        assert_ne!(first.1, changed.1);
        capacity_cache().invalidate(&bucket);
    }

    #[test]
    fn cluster_capacity_rejects_missing_or_stale_observations() {
        let mut info = rustfs_madmin::StorageInfo {
            disks: vec![rustfs_madmin::Disk {
                state: "ok".into(),
                capacity_observation_source: Some("live_probe".into()),
                ..Default::default()
            }],
            observations: vec![rustfs_madmin::StorageInfoObservation {
                status: rustfs_madmin::StorageInfoProbeStatus::Succeeded,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(cluster_capacity_observations_are_live(&info));
        info.disks[0].capacity_observation_source = Some("snapshot".into());
        assert!(!cluster_capacity_observations_are_live(&info));
        info.disks[0].capacity_observation_source = Some("live_probe".into());
        info.observations[0].status = rustfs_madmin::StorageInfoProbeStatus::Failed;
        assert!(!cluster_capacity_observations_are_live(&info));
        info.observations.clear();
        assert!(!cluster_capacity_observations_are_live(&info));
    }

    #[test]
    fn cluster_capacity_uses_the_smallest_disk_in_each_erasure_set() {
        let mut info = rustfs_madmin::StorageInfo::default();
        info.backend.drives_per_set = vec![4];
        info.backend.total_sets = vec![1];
        info.backend.standard_sc_data = vec![2];
        info.backend.standard_sc_parities = vec![2];
        info.disks = [100, 100, 100, 10]
            .into_iter()
            .enumerate()
            .map(|(index, free)| rustfs_madmin::Disk {
                pool_index: 0,
                set_index: 0,
                disk_index: i32::try_from(index).unwrap(),
                total_space: 200,
                available_space: free,
                ..Default::default()
            })
            .collect();
        assert_eq!(conservative_cluster_capacity(&info), Some((400, 20)));
        info.disks[3].disk_index = 0;
        assert_eq!(conservative_cluster_capacity(&info), None, "duplicate drives cannot certify capacity");
        info.disks.pop();
        assert_eq!(conservative_cluster_capacity(&info), None, "missing drives cannot certify capacity");
    }

    #[test]
    fn cluster_capacity_handles_heterogeneous_pools_and_rejects_overflow() {
        let mut info = rustfs_madmin::StorageInfo::default();
        info.backend.drives_per_set = vec![2, 1];
        info.backend.total_sets = vec![1, 2];
        info.backend.standard_sc_data = vec![1, 1];
        info.backend.standard_sc_parities = vec![1, 0];
        info.disks = [
            (0, 0, 0, 100, 80),
            (0, 0, 1, 120, 50),
            (1, 0, 0, 200, 90),
            (1, 1, 0, 300, 150),
        ]
        .into_iter()
        .map(|(pool, set, index, total, free)| rustfs_madmin::Disk {
            pool_index: pool,
            set_index: set,
            disk_index: index,
            total_space: total,
            available_space: free,
            ..Default::default()
        })
        .collect();
        assert_eq!(conservative_cluster_capacity(&info), Some((600, 290)));
        info.disks[2].total_space = u64::MAX;
        info.disks[3].total_space = u64::MAX;
        assert_eq!(conservative_cluster_capacity(&info), None);
    }

    #[tokio::test]
    async fn cluster_capacity_refresh_is_shared_and_store_scoped() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let cache = tokio::sync::Mutex::new(None);
        let calls = AtomicUsize::new(0);
        let store = Uuid::new_v4();
        let load = || async {
            calls.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            Ok((100, 80))
        };
        let (a, b) = tokio::join!(
            cached_cluster_capacity_with(&cache, store, load),
            cached_cluster_capacity_with(&cache, store, load)
        );
        assert_eq!((a.unwrap(), b.unwrap()), ((100, 80), (100, 80)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(cached_cluster_capacity_with(&cache, Uuid::new_v4(), load).await.unwrap(), (100, 80));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let failed_store = Uuid::new_v4();
        let failed = cached_cluster_capacity_with(&cache, failed_store, || async { Err(capacity_unavailable()) }).await;
        assert_eq!(failed.unwrap_err().code(), &S3ErrorCode::ServiceUnavailable);
        assert_eq!(cached_cluster_capacity_with(&cache, failed_store, load).await.unwrap(), (100, 80));
        assert_eq!(calls.load(Ordering::SeqCst), 3, "failed probes must not poison the cache");
    }

    #[tokio::test]
    async fn cancelled_or_failed_capacity_refresh_can_be_retried() {
        let bucket = format!("veeam-cancel-{}", super::super::Uuid::new_v4());
        let started = Arc::new(tokio::sync::Notify::new());
        let store_id = Uuid::new_v4();
        let incarnation = Uuid::new_v4();
        let task_bucket = bucket.clone();
        let task_started = Arc::clone(&started);
        let task = tokio::spawn(async move {
            cached_or_load_capacity(&task_bucket, store_id, incarnation, || async {
                task_started.notify_one();
                std::future::pending::<()>().await;
                Ok(CapacityFigures {
                    capacity: 1,
                    used: 0,
                    available: 1,
                })
            })
            .await
        });
        started.notified().await;
        task.abort();
        assert!(task.await.expect_err("refresh was cancelled").is_cancelled());
        let failed = cached_or_load_capacity(&bucket, store_id, incarnation, || async { Err(capacity_unavailable()) }).await;
        assert_eq!(failed.expect_err("failure is returned").code(), &S3ErrorCode::ServiceUnavailable);
        let recovered = tokio::time::timeout(
            Duration::from_secs(1),
            cached_or_load_capacity(&bucket, store_id, incarnation, || async {
                Ok(CapacityFigures {
                    capacity: 1,
                    used: 0,
                    available: 1,
                })
            }),
        )
        .await
        .expect("cancelled refresh released the lock")
        .expect("retry succeeds");
        assert!(!recovered.0.is_empty());
        capacity_cache().invalidate(&bucket);
    }

    #[test]
    fn etag_is_md5_of_the_full_body_and_range_slices_it() {
        let xml = capacity_xml(&CapacityFigures {
            capacity: 10,
            used: 4,
            available: 3,
        });
        let bytes = Bytes::from(xml.clone());
        let etag = md5_hex(&bytes);
        assert_eq!(etag.len(), 32);
        let full = apply_range(bytes.clone(), etag.clone(), OffsetDateTime::UNIX_EPOCH, None).expect("full read");
        assert_eq!(full.body.as_ref(), xml.as_bytes());
        assert!(full.content_range.is_none());
        assert_eq!(full.etag_hex, etag);
        assert_eq!(full.content_length, i64::try_from(xml.len()).unwrap_or(i64::MAX));

        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 0,
            end: 3,
        };
        let sliced = apply_range(bytes.clone(), etag.clone(), OffsetDateTime::UNIX_EPOCH, Some(&range)).expect("range");
        assert_eq!(sliced.body.as_ref(), &xml.as_bytes()[..4]);
        assert_eq!(sliced.content_length, 4);
        assert_eq!(sliced.content_range.as_deref(), Some(format!("bytes 0-3/{}", xml.len()).as_str()));
        assert_eq!(sliced.etag_hex, etag);

        let past_end = HTTPRangeSpec {
            is_suffix_length: false,
            start: i64::try_from(xml.len()).unwrap_or(i64::MAX),
            end: -1,
        };
        assert_eq!(
            apply_range(bytes, etag, OffsetDateTime::UNIX_EPOCH, Some(&past_end)).err(),
            Some(SosapiReadError::InvalidRange)
        );
    }

    #[test]
    #[serial_test::serial]
    fn veeam_sosapi_http_contract() {
        crate::app::gating_test_env::run_large_stack_test("veeam-sosapi-http", || async {
            temp_env::async_with_vars([(rustfs_config::ENV_VEEAM_SOSAPI_ENABLE, None::<&str>)], async {
                use crate::app::runtime_sources::{AppContext, ServerContextSlot};
                use crate::app::storage_api::s3::{Body, S3ServiceBuilder, SimpleAuth};
                use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};
                use crate::app::storage_api::test::contract::list::ListOperations as _;
                use crate::app::storage_api::test::contract::object::ObjectOperations as _;
                use http::Method;
                use http_body_util::BodyExt;

                let (paths, store) = crate::app::gating_test_env::shared_gating_ecstore_and_disk_paths().await;
                let _paths = paths;
                let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
                let context = Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
                let slot = ServerContextSlot::new();
                assert!(slot.install(context));
                let fs = super::super::FS::with_server_ctx(slot);
                let owner = S3ServiceBuilder::new(fs.clone()).build();
                let mut anonymous_builder = S3ServiceBuilder::new(fs.clone());
                anonymous_builder.set_auth(SimpleAuth::from_single("test-access", "test-secret"));
                anonymous_builder.set_access(fs.clone());
                let anonymous = anonymous_builder.build();
                let mut bad_auth_builder = S3ServiceBuilder::new(fs.clone());
                bad_auth_builder.set_auth(SimpleAuth::from_single("test-access", "test-secret"));
                bad_auth_builder.set_access(fs);
                let bad_auth = bad_auth_builder.build();

                let bucket = format!("veeam-sosapi-{}", super::super::Uuid::new_v4());
                store
                    .make_bucket(&bucket, &MakeBucketOptions::default())
                    .await
                    .expect("create sosapi bucket");

                let owner_call = |method: Method, uri: String, headers: Vec<(&str, String)>, body: Bytes| {
                    let mut builder = http::Request::builder().method(method).uri(uri).header("host", "localhost");
                    for (name, value) in headers {
                        builder = builder.header(name, value);
                    }
                    builder
                        .extension(crate::storage::access::ReqInfo {
                            cred: Some(rustfs_credentials::Credentials::default()),
                            is_owner: true,
                            ..Default::default()
                        })
                        .body(Body::from(body))
                        .expect("HTTP request")
                };

                let system_uri = format!("/{bucket}/{SYSTEM_KEY}");
                let capacity_uri = format!("/{bucket}/{CAPACITY_KEY}");

                let response = owner
                    .call(owner_call(Method::GET, system_uri.clone(), Vec::new(), Bytes::new()))
                    .await
                    .expect("GET system.xml");
                assert_eq!(response.status(), http::StatusCode::OK);
                assert_eq!(response.headers()["content-type"], "application/xml");
                assert!(response.headers().get("etag").is_some());
                assert!(response.headers().get("last-modified").is_some());
                let system_etag = response.headers()["etag"].to_str().expect("etag").to_string();
                let system_modified = response.headers()["last-modified"].to_str().expect("last modified").to_string();
                let system_body = BodyExt::collect(response.into_body())
                    .await
                    .expect("system body")
                    .to_bytes();
                let system_xml = std::str::from_utf8(&system_body).expect("utf-8");
                assert!(system_xml.contains("<ProtocolVersion>\"1.0\"</ProtocolVersion>"));
                assert!(system_xml.contains(&format!("<ModelName>\"RustFS {}\"</ModelName>", env!("CARGO_PKG_VERSION"))));
                assert!(!system_xml.contains("SystemRecommendations"));
                assert!(!system_xml.contains("APIEndpoints"));

                let head = owner
                    .call(owner_call(Method::HEAD, system_uri.clone(), Vec::new(), Bytes::new()))
                    .await
                    .expect("HEAD system.xml");
                assert_eq!(head.status(), http::StatusCode::OK);
                assert_eq!(
                    head.headers()["content-length"],
                    system_body.len().to_string().as_str()
                );
                assert_eq!(head.headers()["etag"].to_str().expect("head etag"), system_etag.as_str());
                assert_eq!(head.headers()["content-type"], "application/xml");

                for method in [Method::GET, Method::HEAD] {
                    let unchanged = owner.call(owner_call(method, system_uri.clone(),
                        vec![("if-modified-since", system_modified.clone())], Bytes::new()))
                        .await.expect("unchanged system XML");
                    assert_eq!(unchanged.status(), http::StatusCode::NOT_MODIFIED);
                }

                let ranged = owner
                    .call(owner_call(
                        Method::GET,
                        system_uri.clone(),
                        vec![("range", "bytes=0-3".to_string())],
                        Bytes::new(),
                    ))
                    .await
                    .expect("ranged GET");
                assert_eq!(ranged.status(), http::StatusCode::PARTIAL_CONTENT);
                assert_eq!(ranged.headers()["etag"].to_str().expect("range etag"), system_etag.as_str());
                let content_range = ranged.headers()["content-range"].to_str().expect("content-range");
                assert!(content_range.starts_with("bytes 0-3/"), "{content_range}");
                let ranged_body = BodyExt::collect(ranged.into_body()).await.expect("range body").to_bytes();
                assert_eq!(&ranged_body[..], &system_body[..4]);

                for method in [Method::GET, Method::HEAD] {
                    let missing_version = owner.call(owner_call(
                        method, format!("{system_uri}?versionId=11111111-1111-4111-8111-111111111111"), Vec::new(), Bytes::new()
                    )).await.expect("versioned read");
                    assert_eq!(missing_version.status(), http::StatusCode::NOT_FOUND);
                }

                let unknown_capacity = owner.call(owner_call(Method::GET, capacity_uri.clone(), Vec::new(), Bytes::new()))
                    .await.expect("unknown capacity");
                assert_eq!(unknown_capacity.status(), http::StatusCode::SERVICE_UNAVAILABLE);
                let mut snapshot = rustfs_data_usage::DataUsageInfo {
                    last_update: Some(std::time::SystemTime::now() + Duration::from_secs(1)),
                    buckets_count: 1,
                    usage_snapshot_complete: true,
                    ..Default::default()
                };
                let bucket_incarnation = store.bucket_incarnation_id_from_disk(&bucket).await.expect("bucket incarnation");
                snapshot.usage_snapshot_converged = Some(true);
                snapshot.scanner_cycle = Some(1);
                snapshot.scanner_epoch = Some(1);
                snapshot.buckets_usage.insert(bucket.clone(), rustfs_data_usage::BucketUsageInfo {
                    bucket_incarnation: Some(bucket_incarnation.to_string()), ..Default::default()
                });
                snapshot.bucket_sizes.insert(bucket.clone(), 0);
                crate::app::storage_api::test::data_usage::store_data_usage_in_backend(snapshot.clone(), Arc::clone(&store))
                    .await.expect("seed capacity snapshot");

                let capacity = owner
                    .call(owner_call(Method::GET, capacity_uri.clone(), Vec::new(), Bytes::new()))
                    .await
                    .expect("GET capacity.xml");
                assert_eq!(capacity.status(), http::StatusCode::OK);
                assert_eq!(capacity.headers()["content-type"], "application/xml");
                let capacity_etag = capacity.headers()["etag"].to_str().expect("capacity etag").to_string();
                let capacity_body = BodyExt::collect(capacity.into_body())
                    .await
                    .expect("capacity body")
                    .to_bytes();
                let capacity_xml = std::str::from_utf8(&capacity_body).expect("utf-8");
                let capacity_value: u64 = element_text(capacity_xml, "Capacity").parse().expect("capacity");
                let available_value: u64 = element_text(capacity_xml, "Available").parse().expect("available");
                let used_value: u64 = element_text(capacity_xml, "Used").parse().expect("used");
                assert!(used_value <= capacity_value);
                assert!(available_value <= capacity_value.saturating_sub(used_value));

                let other_etag = "\"00000000000000000000000000000000\"";
                let past = OffsetDateTime::UNIX_EPOCH
                    .format(&super::super::RFC1123)
                    .expect("past http date");
                let future = (OffsetDateTime::UNIX_EPOCH + time::Duration::days(40_000))
                    .format(&super::super::RFC1123)
                    .expect("future http date");
                let precondition_cases = [
                    (Method::GET, system_uri.as_str(), "if-match", system_etag.as_str(), http::StatusCode::OK),
                    (Method::HEAD, system_uri.as_str(), "if-match", system_etag.as_str(), http::StatusCode::OK),
                    (Method::GET, system_uri.as_str(), "if-match", other_etag, http::StatusCode::PRECONDITION_FAILED),
                    (Method::HEAD, system_uri.as_str(), "if-match", other_etag, http::StatusCode::PRECONDITION_FAILED),
                    (Method::GET, system_uri.as_str(), "if-none-match", system_etag.as_str(), http::StatusCode::NOT_MODIFIED),
                    (Method::HEAD, system_uri.as_str(), "if-none-match", system_etag.as_str(), http::StatusCode::NOT_MODIFIED),
                    (Method::GET, system_uri.as_str(), "if-none-match", other_etag, http::StatusCode::OK),
                    (Method::HEAD, system_uri.as_str(), "if-none-match", other_etag, http::StatusCode::OK),
                    (Method::GET, capacity_uri.as_str(), "if-match", capacity_etag.as_str(), http::StatusCode::OK),
                    (Method::HEAD, capacity_uri.as_str(), "if-match", capacity_etag.as_str(), http::StatusCode::OK),
                    (Method::GET, capacity_uri.as_str(), "if-match", other_etag, http::StatusCode::PRECONDITION_FAILED),
                    (Method::HEAD, capacity_uri.as_str(), "if-match", other_etag, http::StatusCode::PRECONDITION_FAILED),
                    (Method::GET, capacity_uri.as_str(), "if-none-match", capacity_etag.as_str(), http::StatusCode::NOT_MODIFIED),
                    (Method::HEAD, capacity_uri.as_str(), "if-none-match", capacity_etag.as_str(), http::StatusCode::NOT_MODIFIED),
                    (Method::GET, capacity_uri.as_str(), "if-none-match", other_etag, http::StatusCode::OK),
                    (Method::HEAD, capacity_uri.as_str(), "if-none-match", other_etag, http::StatusCode::OK),
                    (Method::GET, system_uri.as_str(), "if-modified-since", past.as_str(), http::StatusCode::OK),
                    (Method::HEAD, system_uri.as_str(), "if-modified-since", past.as_str(), http::StatusCode::OK),
                    (Method::GET, system_uri.as_str(), "if-modified-since", future.as_str(), http::StatusCode::NOT_MODIFIED),
                    (Method::HEAD, system_uri.as_str(), "if-modified-since", future.as_str(), http::StatusCode::NOT_MODIFIED),
                    (Method::GET, system_uri.as_str(), "if-unmodified-since", future.as_str(), http::StatusCode::OK),
                    (Method::HEAD, system_uri.as_str(), "if-unmodified-since", future.as_str(), http::StatusCode::OK),
                    (Method::GET, system_uri.as_str(), "if-unmodified-since", past.as_str(), http::StatusCode::PRECONDITION_FAILED),
                    (Method::HEAD, system_uri.as_str(), "if-unmodified-since", past.as_str(), http::StatusCode::PRECONDITION_FAILED),
                    (Method::GET, capacity_uri.as_str(), "if-modified-since", past.as_str(), http::StatusCode::OK),
                    (Method::HEAD, capacity_uri.as_str(), "if-modified-since", past.as_str(), http::StatusCode::OK),
                    (Method::GET, capacity_uri.as_str(), "if-modified-since", future.as_str(), http::StatusCode::NOT_MODIFIED),
                    (Method::HEAD, capacity_uri.as_str(), "if-modified-since", future.as_str(), http::StatusCode::NOT_MODIFIED),
                    (Method::GET, capacity_uri.as_str(), "if-unmodified-since", future.as_str(), http::StatusCode::OK),
                    (Method::HEAD, capacity_uri.as_str(), "if-unmodified-since", future.as_str(), http::StatusCode::OK),
                    (Method::GET, capacity_uri.as_str(), "if-unmodified-since", past.as_str(), http::StatusCode::PRECONDITION_FAILED),
                    (Method::HEAD, capacity_uri.as_str(), "if-unmodified-since", past.as_str(), http::StatusCode::PRECONDITION_FAILED),
                ];
                for (method, uri, header, value, expected) in precondition_cases {
                    let response = owner
                        .call(owner_call(
                            method.clone(),
                            uri.to_string(),
                            vec![(header, value.to_string())],
                            Bytes::new(),
                        ))
                        .await
                        .unwrap_or_else(|err| panic!("{method} {uri} {header} failed: {err}"));
                    let status = response.status();
                    let response_etag = response
                        .headers()
                        .get(http::header::ETAG)
                        .and_then(|etag| etag.to_str().ok())
                        .map(str::to_string);
                    let body = BodyExt::collect(response.into_body()).await.expect("precondition body").to_bytes();
                    let body_text = String::from_utf8_lossy(&body);
                    assert_eq!(status, expected, "{method} {uri} {header}={value} body={body_text}");
                    let expected_etag = if uri.ends_with(SYSTEM_KEY) { &system_etag } else { &capacity_etag };
                    match expected {
                        http::StatusCode::OK if method == Method::GET => {
                            assert!(body_text.contains("<?xml"), "{method} {uri} {header} body={body_text}");
                        }
                        http::StatusCode::NOT_MODIFIED => {
                            assert_eq!(response_etag.as_deref(), Some(expected_etag.as_str()));
                            assert!(!body_text.contains("ProtocolVersion"), "{body_text}");
                            assert!(!body_text.contains("<Capacity>"), "{body_text}");
                        }
                        http::StatusCode::PRECONDITION_FAILED if method == Method::GET => {
                            assert!(body_text.contains("PreconditionFailed"), "{body_text}");
                            assert!(!body_text.contains("ProtocolVersion"), "{body_text}");
                        }
                        _ => {}
                    }
                }
                let ranged_match = owner
                    .call(owner_call(
                        Method::GET,
                        system_uri.clone(),
                        vec![
                            ("if-match", system_etag.clone()),
                            ("range", "bytes=0-3".to_string()),
                        ],
                        Bytes::new(),
                    ))
                    .await
                    .expect("matching If-Match still honors range");
                assert_eq!(ranged_match.status(), http::StatusCode::PARTIAL_CONTENT);
                let part_and_match = owner
                    .call(owner_call(
                        Method::GET,
                        format!("{system_uri}?partNumber=1"),
                        vec![("if-match", other_etag.to_string())],
                        Bytes::new(),
                    ))
                    .await
                    .expect("failed If-Match precedes partNumber");
                assert_eq!(part_and_match.status(), http::StatusCode::PRECONDITION_FAILED);

                let metadata = crate::app::storage_api::test::get_global_bucket_metadata_sys().expect("bucket metadata");
                let mut checker = crate::app::storage_api::test::bucket::quota::checker::QuotaChecker::new(Arc::clone(&metadata));
                let quota_bucket = format!("veeam-sosapi-quota-{}", super::super::Uuid::new_v4());
                store
                    .make_bucket(&quota_bucket, &MakeBucketOptions::default())
                    .await
                    .expect("quota bucket");
                checker
                    .set_quota_config(
                        &quota_bucket,
                        crate::app::storage_api::object_usecase::bucket::quota::BucketQuota::new(Some(1_000_000)),
                    )
                    .await
                    .expect("set quota on fresh bucket");
                snapshot.last_update = Some(std::time::SystemTime::now() + Duration::from_secs(2));
                snapshot.buckets_count = 2;
                let quota_incarnation = store.bucket_incarnation_id_from_disk(&quota_bucket).await.expect("quota bucket incarnation");
                snapshot.buckets_usage.insert(quota_bucket.clone(), rustfs_data_usage::BucketUsageInfo {
                    bucket_incarnation: Some(quota_incarnation.to_string()), ..Default::default()
                });
                snapshot.bucket_sizes.insert(quota_bucket.clone(), 0);
                crate::app::storage_api::test::data_usage::store_data_usage_in_backend(snapshot.clone(), Arc::clone(&store))
                    .await.expect("seed quota bucket usage");
                let quota_response = owner
                    .call(owner_call(
                        Method::GET,
                        format!("/{quota_bucket}/{CAPACITY_KEY}"),
                        Vec::new(),
                        Bytes::new(),
                    ))
                    .await
                    .expect("quota capacity");
                assert_eq!(quota_response.status(), http::StatusCode::OK);
                let quota_xml = BodyExt::collect(quota_response.into_body())
                    .await
                    .expect("quota body")
                    .to_bytes();
                let quota_xml = std::str::from_utf8(&quota_xml).expect("utf-8");
                assert_eq!(element_text(quota_xml, "Capacity"), "1000000");
                let quota_used: u64 = element_text(quota_xml, "Used").parse().expect("quota used");
                let quota_available: u64 = element_text(quota_xml, "Available").parse().expect("quota available");
                assert!(quota_used <= 1_000_000);
                assert!(quota_available <= 1_000_000 - quota_used);
                let (mut corrupt_quota, _) = metadata.read().await.get_config(&quota_bucket).await.expect("load quota metadata");
                let original_quota = Arc::clone(&corrupt_quota);
                Arc::make_mut(&mut corrupt_quota).quota_config_json = b"{invalid-json".to_vec();
                metadata.read().await.set(quota_bucket.clone(), corrupt_quota).await;
                capacity_cache().invalidate(&quota_bucket);
                for method in [Method::GET, Method::HEAD] {
                    let response = owner.call(owner_call(
                        method, format!("/{quota_bucket}/{CAPACITY_KEY}"), Vec::new(), Bytes::new()
                    )).await.expect("invalid quota response");
                    assert_eq!(response.status(), http::StatusCode::OK, "stale in-memory quota must not override authoritative metadata");
                }
                metadata.read().await.set(quota_bucket.clone(), original_quota).await;

                let recycled = format!("veeam-recycled-{}", Uuid::new_v4());
                store.make_bucket(&recycled, &MakeBucketOptions::default()).await.expect("create recycled bucket");
                let old_id = store.bucket_incarnation_id_from_disk(&recycled).await.expect("old identity");
                let (old_metadata, _) = metadata.read().await.get_config(&recycled).await.expect("old cached metadata");
                snapshot.last_update = Some(std::time::SystemTime::now() + Duration::from_secs(3));
                snapshot.buckets_count = 3;
                snapshot.buckets_usage.insert(recycled.clone(), rustfs_data_usage::BucketUsageInfo {
                    size: 123, bucket_incarnation: Some(old_id.to_string()), ..Default::default()
                });
                snapshot.bucket_sizes.insert(recycled.clone(), 123);
                crate::app::storage_api::test::data_usage::store_data_usage_in_backend(snapshot.clone(), Arc::clone(&store))
                    .await.expect("seed old incarnation snapshot");
                let recycled_uri = format!("/{recycled}/{CAPACITY_KEY}");
                let old = owner.call(owner_call(Method::GET, recycled_uri.clone(), Vec::new(), Bytes::new())).await.expect("old capacity");
                assert_eq!(old.status(), http::StatusCode::OK);
                let old_body = BodyExt::collect(old.into_body()).await.expect("old capacity body").to_bytes();
                assert_eq!(element_text(std::str::from_utf8(&old_body).unwrap(), "Used"), "123");
                store.delete_bucket(&recycled, &DeleteBucketOptions::default()).await.expect("delete recycled bucket");
                store.make_bucket(&recycled, &MakeBucketOptions::default()).await.expect("recreate bucket");
                let new_id = store.bucket_incarnation_id_from_disk(&recycled).await.expect("new identity");
                assert_ne!(old_id, new_id);
                let (new_metadata, _) = metadata.read().await.get_config(&recycled).await.expect("new cached metadata");
                metadata.read().await.set(recycled.clone(), old_metadata).await;
                // Simulate a stale peer snapshot after the new bucket exists.
                snapshot.last_update = Some(std::time::SystemTime::now() + Duration::from_secs(4));
                crate::app::storage_api::test::data_usage::store_data_usage_in_backend(snapshot.clone(), Arc::clone(&store))
                    .await.expect("seed stale incarnation snapshot");
                let stale = owner.call(owner_call(Method::GET, recycled_uri.clone(), Vec::new(), Bytes::new())).await.expect("stale capacity");
                assert_eq!(stale.status(), http::StatusCode::SERVICE_UNAVAILABLE);
                snapshot.last_update = Some(std::time::SystemTime::now() + Duration::from_secs(5));
                snapshot.buckets_usage.get_mut(&recycled).unwrap().bucket_incarnation = Some(new_id.to_string());
                snapshot.buckets_usage.get_mut(&recycled).unwrap().size = 17;
                snapshot.bucket_sizes.insert(recycled.clone(), 17);
                crate::app::storage_api::test::data_usage::store_data_usage_in_backend(snapshot, Arc::clone(&store))
                    .await.expect("seed new incarnation snapshot");
                let fresh = owner.call(owner_call(Method::GET, recycled_uri, Vec::new(), Bytes::new())).await.expect("new capacity");
                assert_eq!(fresh.status(), http::StatusCode::OK);
                let fresh_body = BodyExt::collect(fresh.into_body()).await.expect("new capacity body").to_bytes();
                assert_eq!(element_text(std::str::from_utf8(&fresh_body).unwrap(), "Used"), "17");
                metadata.read().await.set(recycled.clone(), new_metadata).await;

                let stored = store
                    .get_object_info(&bucket, SYSTEM_KEY, &super::super::ObjectOptions::default())
                    .await;
                assert!(stored.is_err(), "virtual GET must not create xl.meta");

                let mutations = [
                    (Method::PUT, system_uri.clone(), Vec::new(), Bytes::from_static(b"nope")),
                    (Method::DELETE, system_uri.clone(), Vec::new(), Bytes::new()),
                    (
                        Method::PUT,
                        capacity_uri.clone(),
                        vec![("x-amz-copy-source", format!("/{bucket}/other"))],
                        Bytes::new(),
                    ),
                    (Method::POST, format!("{system_uri}?uploads"), Vec::new(), Bytes::new()),
                    (
                        Method::PUT,
                        format!("{system_uri}?partNumber=1&uploadId=abc"),
                        Vec::new(),
                        Bytes::from_static(b"part"),
                    ),
                    (
                        Method::POST,
                        format!("{system_uri}?uploadId=abc"),
                        Vec::new(),
                        Bytes::from_static(
                            b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"abc\"</ETag></Part></CompleteMultipartUpload>",
                        ),
                    ),
                    (Method::DELETE, format!("{system_uri}?uploadId=abc"), Vec::new(), Bytes::new()),
                    (Method::GET, format!("{system_uri}?uploadId=abc"), Vec::new(), Bytes::new()),
                    (Method::GET, format!("{system_uri}?tagging"), Vec::new(), Bytes::new()),
                    (
                        Method::PUT,
                        format!("{system_uri}?tagging"),
                        Vec::new(),
                        Bytes::from_static(
                            b"<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>",
                        ),
                    ),
                    (Method::DELETE, format!("{system_uri}?tagging"), Vec::new(), Bytes::new()),
                    (Method::GET, format!("{system_uri}?retention"), Vec::new(), Bytes::new()),
                    (
                        Method::PUT,
                        format!("{system_uri}?retention"),
                        Vec::new(),
                        Bytes::from_static(
                            b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2030-01-01T00:00:00.000Z</RetainUntilDate></Retention>",
                        ),
                    ),
                    (Method::GET, format!("{system_uri}?legal-hold"), Vec::new(), Bytes::new()),
                    (
                        Method::PUT,
                        format!("{system_uri}?legal-hold"),
                        Vec::new(),
                        Bytes::from_static(b"<LegalHold><Status>ON</Status></LegalHold>"),
                    ),
                    (
                        Method::POST,
                        format!("{system_uri}?restore"),
                        Vec::new(),
                        Bytes::from_static(b"<RestoreRequest><Days>1</Days></RestoreRequest>"),
                    ),
                ];
                for (method, uri, headers, body) in mutations {
                    let response = owner
                        .call(owner_call(method.clone(), uri.clone(), headers, body))
                        .await
                        .unwrap_or_else(|err| panic!("{method} {uri} failed: {err}"));
                    let status = response.status();
                    let error_body = BodyExt::collect(response.into_body())
                        .await
                        .expect("mutation body")
                        .to_bytes();
                    let error_xml = String::from_utf8_lossy(&error_body);
                    assert_eq!(
                        status,
                        http::StatusCode::METHOD_NOT_ALLOWED,
                        "{method} {uri} body={error_xml}"
                    );
                    assert!(error_xml.contains("MethodNotAllowed"), "{method} {uri} body={error_xml}");
                }
                assert!(
                    store
                        .get_object_info(&bucket, SYSTEM_KEY, &super::super::ObjectOptions::default())
                        .await
                        .is_err()
                );

                let normal_key = "Veeam/Backup/job.dat";
                let normal = owner
                    .call(owner_call(
                        Method::PUT,
                        format!("/{bucket}/{normal_key}"),
                        Vec::new(),
                        Bytes::from_static(b"backup-bytes"),
                    ))
                    .await
                    .expect("normal put");
                assert_eq!(normal.status(), http::StatusCode::OK);
                let normal_get = owner
                    .call(owner_call(
                        Method::GET,
                        format!("/{bucket}/{normal_key}"),
                        Vec::new(),
                        Bytes::new(),
                    ))
                    .await
                    .expect("normal get");
                assert_eq!(normal_get.status(), http::StatusCode::OK);
                let normal_body = BodyExt::collect(normal_get.into_body()).await.expect("normal body").to_bytes();
                assert_eq!(&normal_body[..], b"backup-bytes");

                let locked = format!("veeam-sosapi-lock-{}", super::super::Uuid::new_v4());
                store
                    .make_bucket(
                        &locked,
                        &MakeBucketOptions {
                            lock_enabled: true,
                            versioning_enabled: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("object lock bucket");
                let locked_get = owner
                    .call(owner_call(
                        Method::GET,
                        format!("/{locked}/{SYSTEM_KEY}"),
                        Vec::new(),
                        Bytes::new(),
                    ))
                    .await
                    .expect("locked get");
                assert_eq!(locked_get.status(), http::StatusCode::OK);
                let locked_put = owner
                    .call(owner_call(
                        Method::PUT,
                        format!("/{locked}/{SYSTEM_KEY}"),
                        Vec::new(),
                        Bytes::from_static(b"locked"),
                    ))
                    .await
                    .expect("locked put");
                assert_eq!(locked_put.status(), http::StatusCode::METHOD_NOT_ALLOWED);
                let versions = Arc::clone(&store)
                    .list_object_versions(&locked, SYSTEM_KEY, None, None, None, 1000)
                    .await
                    .expect("list versions");
                assert!(versions.objects.is_empty());

                let anon = anonymous
                    .call(
                        http::Request::builder()
                            .method(Method::GET)
                            .uri(system_uri.clone())
                            .header("host", "localhost")
                            .body(Body::empty())
                            .expect("anon request"),
                    )
                    .await
                    .expect("anonymous GET");
                let anon_status = anon.status();
                let anon_body = BodyExt::collect(anon.into_body()).await.expect("anon body").to_bytes();
                let anon_xml = String::from_utf8_lossy(&anon_body);
                assert_eq!(anon_status, http::StatusCode::FORBIDDEN, "{anon_xml}");
                assert!(!anon_xml.contains("ProtocolVersion"), "{anon_xml}");

                let now = OffsetDateTime::now_utc();
                let scope_date = format!("{:04}{:02}{:02}", now.year(), u8::from(now.month()), now.day());
                let amz_date = format!("{scope_date}T{:02}{:02}{:02}Z", now.hour(), now.minute(), now.second());
                let bad = bad_auth
                    .call(
                        http::Request::builder()
                            .method(Method::GET)
                            .uri(system_uri.clone())
                            .header("host", "localhost")
                            .header("x-amz-date", &amz_date)
                            .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
                            .header(
                                "authorization",
                                format!(
                                    "AWS4-HMAC-SHA256 Credential=bad/{scope_date}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=0000000000000000000000000000000000000000000000000000000000000000"
                                ),
                            )
                            .body(Body::empty())
                            .expect("bad auth request"),
                    )
                    .await
                    .expect("bad auth GET");
                let bad_status = bad.status();
                let bad_body = BodyExt::collect(bad.into_body()).await.expect("bad body").to_bytes();
                let bad_xml = String::from_utf8_lossy(&bad_body);
                assert_eq!(bad_status, http::StatusCode::FORBIDDEN, "{bad_xml}");
                assert!(!bad_xml.contains("ProtocolVersion"), "{bad_xml}");

                let missing = owner
                    .call(owner_call(
                        Method::PUT,
                        format!("/missing-sosapi-bucket/{SYSTEM_KEY}"),
                        Vec::new(),
                        Bytes::from_static(b"x"),
                    ))
                    .await
                    .expect("missing bucket");
                assert_eq!(missing.status(), http::StatusCode::NOT_FOUND);

                let shadow = format!("veeam-sosapi-shadow-{}", super::super::Uuid::new_v4());
                store
                    .make_bucket(&shadow, &MakeBucketOptions::default())
                    .await
                    .expect("shadow bucket");
                let shadow_uri = format!("/{shadow}/{SYSTEM_KEY}");
                temp_env::async_with_vars([(rustfs_config::ENV_VEEAM_SOSAPI_ENABLE, Some("false"))], async {
                    let stored = owner
                        .call(owner_call(
                            Method::PUT,
                            shadow_uri.clone(),
                            Vec::new(),
                            Bytes::from_static(b"shadow-bytes"),
                        ))
                        .await
                        .expect("store shadow");
                    assert_eq!(stored.status(), http::StatusCode::OK, "kill switch must allow PUT");
                    let missing_virtual = owner
                        .call(owner_call(
                            Method::GET,
                            format!("/{bucket}/{CAPACITY_KEY}"),
                            Vec::new(),
                            Bytes::new(),
                        ))
                        .await
                        .expect("disabled capacity");
                    assert_eq!(missing_virtual.status(), http::StatusCode::NOT_FOUND);
                })
                .await;

                for method in [Method::GET, Method::HEAD] {
                    let response = owner.call(owner_call(
                        method.clone(), format!("{shadow_uri}?versionId=null"), Vec::new(), Bytes::new()
                    )).await.expect("explicit stored version");
                    assert_eq!(response.status(), http::StatusCode::OK);
                    if method == Method::GET {
                        let body = BodyExt::collect(response.into_body()).await.expect("stored version body").to_bytes();
                        assert_eq!(body, Bytes::from_static(b"shadow-bytes"));
                    }
                }

                let virtual_wins = owner
                    .call(owner_call(Method::GET, shadow_uri.clone(), Vec::new(), Bytes::new()))
                    .await
                    .expect("virtual wins");
                assert_eq!(virtual_wins.status(), http::StatusCode::OK);
                let virtual_body = BodyExt::collect(virtual_wins.into_body())
                    .await
                    .expect("virtual body")
                    .to_bytes();
                assert!(std::str::from_utf8(&virtual_body).unwrap_or("").contains("ProtocolVersion"));
                assert!(!virtual_body.windows(12).any(|window| window == b"shadow-bytes"));
                let blocked_delete = owner
                    .call(owner_call(Method::DELETE, shadow_uri.clone(), Vec::new(), Bytes::new()))
                    .await
                    .expect("blocked delete");
                assert_eq!(blocked_delete.status(), http::StatusCode::METHOD_NOT_ALLOWED);
                assert!(
                    store
                        .get_object_info(&shadow, SYSTEM_KEY, &super::super::ObjectOptions::default())
                        .await
                        .is_ok(),
                    "enabled DELETE must leave the pre-existing object in place"
                );

                temp_env::async_with_vars([(rustfs_config::ENV_VEEAM_SOSAPI_ENABLE, Some("false"))], async {
                    let removed = owner
                        .call(owner_call(Method::DELETE, shadow_uri.clone(), Vec::new(), Bytes::new()))
                        .await
                        .expect("delete shadow");
                    assert_eq!(removed.status(), http::StatusCode::NO_CONTENT);
                })
                .await;
                assert!(
                    store
                        .get_object_info(&shadow, SYSTEM_KEY, &super::super::ObjectOptions::default())
                        .await
                        .is_err()
                );
            })
            .await;
        });
    }
}
