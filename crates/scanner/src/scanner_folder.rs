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

use std::collections::{HashMap, HashSet};
use std::fs::FileType;
use std::io::ErrorKind;
use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, Instant, SystemTime};

use crate::ReplTargetSizeSummary;
use crate::data_usage_define::{
    DATA_USAGE_SCAN_CHECKPOINT_VERSION, DataUsageCache, DataUsageCacheInfo, DataUsageEntry, DataUsageHash, DataUsageHashMap,
    DataUsageScanCheckpoint, DataUsageScanCheckpointReason, PendingScannerHeal, PendingScannerHealKind, ScannerSizeSummaryExt,
    SizeReconciliationEntry, SizeSummary, hash_path,
};
use crate::error::ScannerError;
use crate::runtime_config::{
    scanner_alert_excess_folders, scanner_alert_excess_version_size, scanner_alert_excess_versions, scanner_yield_every_n_objects,
};
use crate::scanner_budget::{ScannerCycleBudget, ScannerCycleBudgetReason};
use crate::scanner_io::{
    SCANNER_SKIP_FILE_ERROR, ScannerIODisk as _, is_scanner_metadata_corrupt_error, is_scanner_metadata_transient_error,
};
use crate::sleeper::DynamicSleeper;
use crate::storage_api::owner::{EcstoreEventArgs, ecstore_send_event};
use metrics::{counter, describe_counter};
use rustfs_common::heal_channel::{
    HEAL_DELETE_DANGLING, HealAdmissionDropReason, HealAdmissionResult, HealChannelPriority, HealChannelRequest,
    HealRequestSource, HealScanMode, send_heal_request_with_admission,
};
use rustfs_common::metrics::{
    CloseDiskGuard, IlmAction, Metric, Metrics, ScannerReplicationRepairKind, ScannerSourceWorkUpdate, ScannerWorkSource,
    UpdateCurrentPathFn, current_path_updater, global_metrics,
};
use rustfs_common::trace_bus::{TraceEvent, TraceFunc, TraceKind, trace_emit, trace_subscriber_count};
use rustfs_filemeta::{
    MAX_META_CACHE_HEAL_CANDIDATES, MAX_META_CACHE_HEAL_TRUNCATED_OBJECTS, MetaCacheEntries, MetaCacheEntry,
    MetaCacheHealCandidateKind,
};
use rustfs_utils::path::{SLASH_SEPARATOR, path_join_buf};
use s3s::dto::{BucketLifecycleConfiguration, ObjectLockConfiguration, VersioningConfiguration};
use time::OffsetDateTime;
use tokio::select;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::{
    Disk, DiskError, DiskInfoOptions, Evaluator, Event, LcEventSrc, ListPathRawOptions, ObjectOpts, ReplicationConfig,
    ReplicationHealObject, ReplicationQueueAdmission, ReplicationStatusType, STORAGE_FORMAT_FILE, ScannerDiskExt as _,
    ScannerLifecycleConfigExt as _, ScannerVersioningConfigExt as _, StorageError, apply_expiry_rule, apply_transition_rule,
    enqueue_runtime_newer_noncurrent, is_reserved_or_invalid_bucket, list_path_raw, path2_bucket_object,
    path2_bucket_object_with_base_path, queue_replication_heal, scanner_is_erasure,
    scanner_replication_config_for_lifecycle_eval,
};
use crate::{ScannerObjectInfo as ObjectInfo, ScannerObjectToDelete as ObjectToDelete};

const LOG_COMPONENT_SCANNER: &str = "scanner";
const LOG_SUBSYSTEM_FOLDER: &str = "folder";
const LOG_SUBSYSTEM_LIFECYCLE: &str = "lifecycle";
const LOG_SUBSYSTEM_HEAL: &str = "heal";
const EVENT_SCANNER_FOLDER_STATE: &str = "scanner_folder_state";
const EVENT_SCANNER_METADATA_CORRUPT: &str = "scanner_metadata_corrupt";
const EVENT_SCANNER_LIFECYCLE_ACTION: &str = "scanner_lifecycle_action";
const EVENT_SCANNER_HEAL_ADMISSION: &str = "scanner_heal_admission";
const EVENT_SCANNER_ALERT_STATE: &str = "scanner_alert_state";

const DATA_USAGE_UPDATE_DIR_CYCLES: u32 = 16;
const DATA_SCANNER_COMPACT_LEAST_OBJECT: usize = 500;
const DATA_SCANNER_COMPACT_AT_CHILDREN: usize = 10000;
const DATA_SCANNER_COMPACT_AT_FOLDERS: usize = DATA_SCANNER_COMPACT_AT_CHILDREN / 4;
const DATA_SCANNER_FORCE_COMPACT_AT_FOLDERS: usize = 250_000;
const SCANNER_LIST_PATH_RAW_STALL_TIMEOUT: Duration = Duration::from_secs(60);
const SCANNER_ENTRY_PROGRESS_BATCH: u64 = 32;
const SCANNER_ENTRY_PROGRESS_INTERVAL: Duration = Duration::from_secs(30);
// Erasure data directories contain direct part.N files; keep namespace probes bounded.
const ERASURE_DATA_DIR_PROBE_ENTRY_LIMIT: usize = 64;
const DEFAULT_HEAL_OBJECT_SELECT_PROB: u32 = 1024;
const ENV_DATA_USAGE_UPDATE_DIR_CYCLES: &str = "RUSTFS_DATA_USAGE_UPDATE_DIR_CYCLES";
const ENV_HEAL_OBJECT_SELECT_PROB: &str = "RUSTFS_HEAL_OBJECT_SELECT_PROB";
const ENV_SCANNER_DEEP_VERIFY_COOLDOWN_SECS: &str = "RUSTFS_SCANNER_DEEP_VERIFY_COOLDOWN_SECS";
const ENV_FAILED_OBJECT_TTL_SECS: &str = "RUSTFS_DATA_USAGE_FAILED_OBJECT_TTL_SECS";
const ENV_FAILED_OBJECTS_MAX: &str = "RUSTFS_DATA_USAGE_FAILED_OBJECTS_MAX";
const DEFAULT_FAILED_OBJECT_TTL_SECS: u32 = 86_400;
const DEFAULT_FAILED_OBJECTS_MAX: u32 = 10_000;
const DEFAULT_SCANNER_DEEP_VERIFY_COOLDOWN_SECS: u64 = 60;
const METRIC_SCANNER_EXCESS_OBJECT_VERSIONS_TOTAL: &str = "rustfs_scanner_excess_object_versions_total";
const METRIC_SCANNER_EXCESS_OBJECT_VERSION_SIZE_TOTAL: &str = "rustfs_scanner_excess_object_version_size_total";
const METRIC_SCANNER_EXCESS_FOLDERS_TOTAL: &str = "rustfs_scanner_excess_folders_total";
const METRIC_SCANNER_PENDING_HEAL_PRUNE_TOTAL: &str = "rustfs_scanner_pending_heal_prune_total";
const METRIC_SCANNER_PENDING_HEAL_MALFORMED_TOTAL: &str = "rustfs_scanner_pending_heal_malformed_total";
const METRIC_SCANNER_HEAL_DISCOVERY_CANDIDATES_TOTAL: &str = "rustfs_scanner_heal_discovery_candidates_total";
const METRIC_SCANNER_HEAL_DISCOVERY_SUB_QUORUM_TOTAL: &str = "rustfs_scanner_heal_discovery_sub_quorum_total";
const METRIC_SCANNER_HEAL_DISCOVERY_UNVERIFIED_TOTAL: &str = "rustfs_scanner_heal_discovery_unverified_total";
const METRIC_SCANNER_HEAL_DISCOVERY_QUEUED_TOTAL: &str = "rustfs_scanner_heal_discovery_queued_total";
const METRIC_SCANNER_HEAL_DISCOVERY_TRUNCATED_TOTAL: &str = "rustfs_scanner_heal_discovery_truncated_total";
const MAX_PENDING_SCANNER_HEAL_RETRIES_PER_BUCKET: usize = 128;
const MAX_SIZE_RECONCILIATION_ENTRIES_PER_BUCKET: usize = 10_000;
const MAX_SIZE_RECONCILIATION_BYTES_PER_BUCKET: usize = 8 * 1024 * 1024;
const MAX_SIZE_RECONCILIATION_AGE_SECS: u64 = 7 * 24 * 60 * 60;

// --- scanner excess alerts as S3 notification events (rustfs/backlog#1868) --
//
// The excess-versions / excess-version-size / excess-folders alerts were
// metrics-and-logs only; subscribers (consoles, external auditors) had no way
// to hear them. MinIO emits s3:ObjectManyVersions / s3:ObjectLargeVersions /
// s3:PrefixManyFolders for the same conditions — RustFS carries those as
// EventName::Scanner* with the wire names below. Without a cooldown a single
// over-threshold object would re-emit on every scan cycle (~a minute), so
// emissions are edge-held per (kind, bucket, object) for 24h.

/// `s3:Scanner:ManyVersions` (MinIO `s3:ObjectManyVersions`).
pub const EVENT_SCANNER_MANY_VERSIONS: &str = "s3:Scanner:ManyVersions";
/// `s3:Scanner:LargeVersions` (MinIO `s3:ObjectLargeVersions`).
pub const EVENT_SCANNER_LARGE_VERSIONS: &str = "s3:Scanner:LargeVersions";
/// `s3:Scanner:BigPrefix` (MinIO `s3:PrefixManyFolders`).
pub const EVENT_SCANNER_BIG_PREFIX: &str = "s3:Scanner:BigPrefix";
const ENV_SCANNER_ALERT_COOLDOWN_SECS: &str = "RUSTFS_SCANNER_ALERT_COOLDOWN_SECS";
const DEFAULT_SCANNER_ALERT_COOLDOWN_SECS: u64 = 86_400;
/// Hard cap on distinct cooldown keys; a pathological number of over-threshold
/// objects clears the map wholesale instead of growing without bound (the
/// worst case is one re-emission per still-hot key per scan cycle).
const MAX_SCANNER_ALERT_COOLDOWN_KEYS: usize = 4096;

/// Distinct alert kinds sharing one cooldown map.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum ScannerAlertKind {
    ManyVersions,
    LargeVersions,
    BigPrefix,
}

type ScannerAlertCooldownKey = (ScannerAlertKind, String, String);
type ScannerAlertCooldownMap = HashMap<ScannerAlertCooldownKey, Instant>;

static SCANNER_ALERT_EMISSION_COOLDOWN: Mutex<Option<ScannerAlertCooldownMap>> = Mutex::new(None);

fn scanner_alert_cooldown() -> Duration {
    let raw = std::env::var(ENV_SCANNER_ALERT_COOLDOWN_SECS)
        .ok()
        .and_then(|v| v.parse::<u64>().ok());
    Duration::from_secs(raw.unwrap_or(DEFAULT_SCANNER_ALERT_COOLDOWN_SECS))
}

/// Edge-held emission gate: returns `true` (and records the cooldown) only
/// when this (kind, bucket, object) last fired longer than the cooldown ago —
/// or never. Metrics and logs stay level-triggered every cycle; only the
/// notification events are held back.
fn scanner_alert_emission_allows(kind: ScannerAlertKind, bucket: &str, object: &str, cooldown: Duration) -> bool {
    let key = (kind, bucket.to_string(), object.to_string());
    let mut guard = SCANNER_ALERT_EMISSION_COOLDOWN
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let guard = guard.get_or_insert_with(ScannerAlertCooldownMap::new);
    let now = Instant::now();
    // Expired entries leave first; the cap is still exceeded only when live
    // keys alone overflow it, in which case a wholesale clear trades one
    // extra emission per hot key for a hard memory bound.
    if guard.len() >= MAX_SCANNER_ALERT_COOLDOWN_KEYS {
        guard.retain(|_, fired_at| now.duration_since(*fired_at) < cooldown);
        if guard.len() >= MAX_SCANNER_ALERT_COOLDOWN_KEYS {
            guard.clear();
        }
    }
    match guard.get(&key) {
        Some(fired_at) if now.duration_since(*fired_at) < cooldown => false,
        _ => {
            guard.insert(key, now);
            true
        }
    }
}

/// Emit a scanner alert as an S3 notification event through the standard
/// dispatch pipeline. Fire-and-forget: the notify layer owns delivery,
/// retry, and target filtering; the scanner never waits on it.
fn emit_scanner_alert_event(event_name: &str, bucket: &str, object: &str, size: i64, details: &[(&str, String)]) {
    let mut req_params = HashMap::with_capacity(details.len());
    for (key, value) in details {
        req_params.insert((*key).to_string(), value.clone());
    }
    ecstore_send_event(EcstoreEventArgs {
        event_name: event_name.to_string(),
        bucket_name: bucket.to_string(),
        object: crate::ScannerObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            size,
            ..Default::default()
        },
        req_params,
        user_agent: "Scanner".to_string(),
        ..Default::default()
    });
}
const MAX_PENDING_SCANNER_HEALS_PER_BUCKET: usize = 10_000;
const MAX_PENDING_SCANNER_HEAL_AGE_SECS: u64 = 24 * 60 * 60;

static SCANNER_ALERT_METRICS_ONCE: Once = Once::new();

#[cfg(test)]
type ListPathRawTimeoutSnapshot = (bool, Option<Duration>, Option<Duration>);

fn scanner_abandoned_child_list_options() -> ListPathRawOptions {
    // A complete heal walk scales with bucket size and may legitimately take
    // longer than a fixed wall-clock budget. Keep the total duration unbounded;
    // Retain the scanner's per-read stall budget and keep cancellation controlled
    // by the scanner cycle token.
    ListPathRawOptions {
        skip_walkdir_total_timeout: true,
        walkdir_stall_timeout: Some(SCANNER_LIST_PATH_RAW_STALL_TIMEOUT),
        ..Default::default()
    }
}

pub fn data_usage_update_dir_cycles() -> u32 {
    rustfs_utils::get_env_u32(ENV_DATA_USAGE_UPDATE_DIR_CYCLES, DATA_USAGE_UPDATE_DIR_CYCLES)
}

pub fn heal_object_select_prob() -> u32 {
    rustfs_utils::get_env_u32(ENV_HEAL_OBJECT_SELECT_PROB, DEFAULT_HEAL_OBJECT_SELECT_PROB)
}

fn deep_verify_cooldown() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        ENV_SCANNER_DEEP_VERIFY_COOLDOWN_SECS,
        DEFAULT_SCANNER_DEEP_VERIFY_COOLDOWN_SECS,
    ))
}

fn object_is_within_deep_verify_cooldown(mod_time: Option<OffsetDateTime>, now: OffsetDateTime, cooldown: Duration) -> bool {
    let Some(mod_time) = mod_time else {
        return false;
    };
    let Ok(cooldown) = time::Duration::try_from(cooldown) else {
        return false;
    };
    mod_time > now - cooldown
}

fn effective_object_heal_scan_mode(heal_bitrot: bool, mod_time: Option<OffsetDateTime>, now: OffsetDateTime) -> HealScanMode {
    if !heal_bitrot {
        return HealScanMode::Normal;
    }
    if object_is_within_deep_verify_cooldown(mod_time, now, deep_verify_cooldown()) {
        HealScanMode::Normal
    } else {
        HealScanMode::Deep
    }
}

fn ensure_scanner_alert_metrics_registered() {
    SCANNER_ALERT_METRICS_ONCE.call_once(|| {
        describe_counter!(
            METRIC_SCANNER_EXCESS_OBJECT_VERSIONS_TOTAL,
            "Total scanner alerts for objects with too many retained versions."
        );
        describe_counter!(
            METRIC_SCANNER_EXCESS_OBJECT_VERSION_SIZE_TOTAL,
            "Total scanner alerts for objects whose retained versions exceed the cumulative size threshold."
        );
        describe_counter!(
            METRIC_SCANNER_EXCESS_FOLDERS_TOTAL,
            "Total scanner alerts for folders with too many direct subfolders."
        );
    });
}

fn scanner_excess_versions_threshold() -> u64 {
    scanner_alert_excess_versions()
}

fn scanner_excess_version_size_threshold() -> u64 {
    scanner_alert_excess_version_size()
}

fn scanner_excess_folders_threshold() -> u64 {
    scanner_alert_excess_folders()
}

fn should_yield_after_object(object_count: u64, yield_every: u64) -> bool {
    yield_every > 0 && object_count.is_multiple_of(yield_every)
}

const SCANNER_FAILED_OBJECT_LOG_INITIAL_LIMIT: usize = 16;
const SCANNER_FAILED_OBJECT_LOG_EVERY: usize = 1024;

fn should_log_failed_object(failed_objects: usize) -> bool {
    failed_objects <= SCANNER_FAILED_OBJECT_LOG_INITIAL_LIMIT || failed_objects.is_multiple_of(SCANNER_FAILED_OBJECT_LOG_EVERY)
}

fn record_scanner_ilm_action_if_queued(metrics: &Metrics, action: IlmAction, count: u64, queued: bool) -> bool {
    if queued {
        metrics.record_scanner_lifecycle_action(action, count);
    }
    queued
}

fn scanner_replication_work_update(admission: ReplicationQueueAdmission) -> ScannerSourceWorkUpdate {
    match admission {
        ReplicationQueueAdmission::Queued => ScannerSourceWorkUpdate::queued(1),
        ReplicationQueueAdmission::Missed => ScannerSourceWorkUpdate::missed(1),
        ReplicationQueueAdmission::Skipped => ScannerSourceWorkUpdate {
            skipped: 1,
            ..Default::default()
        },
    }
}

fn scanner_replication_repair_kind(roi: &ReplicationHealObject) -> Option<ScannerReplicationRepairKind> {
    if roi.is_empty_identity() {
        return None;
    }

    if roi.is_existing_object_repair() {
        Some(ScannerReplicationRepairKind::BucketExistingObject)
    } else if roi.has_version_purge_status() {
        Some(ScannerReplicationRepairKind::BucketVersionPurge)
    } else if roi.delete_marker {
        Some(ScannerReplicationRepairKind::BucketDeleteMarker)
    } else {
        Some(ScannerReplicationRepairKind::BucketObject)
    }
}

fn record_scanner_replication_admission(metrics: &Metrics, roi: &ReplicationHealObject, admission: ReplicationQueueAdmission) {
    let work = scanner_replication_work_update(admission);
    metrics.record_scanner_source_work(ScannerWorkSource::BucketReplication, work);
    if let Some(kind) = scanner_replication_repair_kind(roi) {
        metrics.record_scanner_replication_repair_work(kind, work);
    }
}

fn scanner_heal_source(scan_mode: HealScanMode) -> ScannerWorkSource {
    match scan_mode {
        HealScanMode::Deep => ScannerWorkSource::Bitrot,
        HealScanMode::Unknown | HealScanMode::Normal => ScannerWorkSource::Heal,
    }
}

fn record_scanner_heal_admission(metrics: &Metrics, scan_mode: HealScanMode, admission: Result<HealAdmissionResult, ()>) -> bool {
    let (work, admitted) = match admission {
        Ok(HealAdmissionResult::Accepted) => (ScannerSourceWorkUpdate::queued(1), true),
        Ok(HealAdmissionResult::Merged) => (
            ScannerSourceWorkUpdate {
                skipped: 1,
                ..Default::default()
            },
            true,
        ),
        Ok(HealAdmissionResult::Full | HealAdmissionResult::Dropped(_)) | Err(_) => (ScannerSourceWorkUpdate::missed(1), false),
    };
    metrics.record_scanner_source_work(scanner_heal_source(scan_mode), work);
    admitted
}

#[derive(Clone, Copy)]
struct PendingScannerAccounting<'a> {
    object: &'a ObjectInfo,
    retained_size: i64,
    expired_size: i64,
}

impl PendingScannerAccounting<'_> {
    fn apply(self, size_summary: &mut SizeSummary, cumulative_size: &mut i64, queued: bool) {
        let size = if queued { self.expired_size } else { self.retained_size };
        size_summary.actions_accounting(self.object, size, self.retained_size);
        *cumulative_size = cumulative_size.saturating_add(size);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FolderResumeMatch {
    Exact,
    Descendant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FolderResumeOrder {
    NoHint,
    Used,
    Stale,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FolderScanSource {
    New,
    Existing,
}

#[derive(Clone, Debug)]
struct QueuedFolder {
    folder: CachedFolder,
    source: FolderScanSource,
}

fn folder_resume_match(folder_name: &str, resume_after: &str) -> Option<FolderResumeMatch> {
    if resume_after == folder_name {
        return Some(FolderResumeMatch::Exact);
    }
    resume_after
        .strip_prefix(folder_name)
        .filter(|suffix| suffix.starts_with(SLASH_SEPARATOR))
        .map(|_| FolderResumeMatch::Descendant)
}

fn order_items_for_resume<T, F>(items: &mut [T], resume_after: Option<&str>, name: F) -> FolderResumeOrder
where
    F: Fn(&T) -> &str,
{
    items.sort_by(|left, right| name(left).cmp(name(right)));

    let Some(resume_after) = resume_after.filter(|resume_after| !resume_after.is_empty()) else {
        return FolderResumeOrder::NoHint;
    };

    let Some((resume_index, resume_match)) = items
        .iter()
        .enumerate()
        .find_map(|(index, item)| folder_resume_match(name(item), resume_after).map(|resume_match| (index, resume_match)))
    else {
        return FolderResumeOrder::Stale;
    };

    let rotate_by = match resume_match {
        FolderResumeMatch::Exact => resume_index + 1,
        FolderResumeMatch::Descendant => resume_index,
    };
    if rotate_by < items.len() {
        items.rotate_left(rotate_by);
    }
    FolderResumeOrder::Used
}

#[cfg(test)]
fn order_folders_for_resume(folders: &mut [CachedFolder], resume_after: Option<&str>) -> FolderResumeOrder {
    order_items_for_resume(folders, resume_after, |folder| folder.name.as_str())
}

fn order_queued_folders_for_resume(folders: &mut [QueuedFolder], resume_after: Option<&str>) -> FolderResumeOrder {
    order_items_for_resume(folders, resume_after, |folder| folder.folder.name.as_str())
}

fn checkpoint_reason_from_budget(reason: Option<ScannerCycleBudgetReason>) -> DataUsageScanCheckpointReason {
    match reason {
        Some(ScannerCycleBudgetReason::Runtime) => DataUsageScanCheckpointReason::Runtime,
        Some(ScannerCycleBudgetReason::Objects) => DataUsageScanCheckpointReason::Objects,
        Some(ScannerCycleBudgetReason::Directories) => DataUsageScanCheckpointReason::Directories,
        None => DataUsageScanCheckpointReason::Unknown,
    }
}

fn data_usage_entry_has_progress(entry: &DataUsageEntry) -> bool {
    data_usage_root_has_progress(entry)
}

fn set_scan_checkpoint(cache: &mut DataUsageCache, reason: DataUsageScanCheckpointReason) {
    let resume_after = cache.info.scan_resume_after.clone().or_else(|| {
        cache
            .info
            .scan_checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.resume_after.clone())
    });

    if let Some(resume_after) = resume_after {
        let checkpoint = DataUsageScanCheckpoint::new(resume_after, reason);
        global_metrics().record_scanner_checkpoint_set(
            checkpoint.version,
            checkpoint.resume_after.clone(),
            checkpoint.reason.as_str(),
        );
        cache.info.scan_checkpoint = Some(checkpoint);
    } else {
        cache.info.scan_checkpoint = None;
    }
}

fn should_alert_excessive_versions(remaining_versions: usize, cumulative_size: i64) -> (bool, bool) {
    let too_many_versions = remaining_versions as u64 >= scanner_excess_versions_threshold();
    let too_large_versions = cumulative_size > 0 && cumulative_size as u64 >= scanner_excess_version_size_threshold();
    (too_many_versions, too_large_versions)
}

fn non_negative_i64_to_u64(value: i64) -> u64 {
    value.max(0) as u64
}

fn trace_start_instant() -> Option<Instant> {
    (trace_subscriber_count() > 0).then(Instant::now)
}

fn emit_scanner_folder_trace(root: &str, folder: &str, objects: u64, started_at: Option<Instant>, state: &'static str) {
    let Some(started_at) = started_at else {
        return;
    };

    trace_emit(|| {
        let (bucket, prefix) = path2_bucket_object_with_base_path(root, folder);
        TraceEvent::new(TraceKind::Scanner, TraceFunc::ScannerFolder)
            .with_bucket(bucket)
            .with_object(prefix)
            .with_duration(started_at.elapsed())
            .with_attr("state", state)
            .with_attr("objects", objects)
    });
}

fn emit_scanner_ilm_action_trace(
    bucket: &str,
    object: &str,
    action: IlmAction,
    count: u64,
    queued: bool,
    started_at: Option<Instant>,
) {
    let Some(started_at) = started_at else {
        return;
    };

    let state = if queued { "queued" } else { "not_queued" };
    trace_emit(|| {
        TraceEvent::new(TraceKind::Scanner, TraceFunc::ScannerIlmAction)
            .with_bucket(bucket)
            .with_object(object)
            .with_duration(started_at.elapsed())
            .with_attr("state", state)
            .with_attr("action", action.as_str())
            .with_attr("count", count)
            .with_attr("queued", queued)
    });
}

struct ScannerHealCandidateTraceContext {
    bucket: String,
    object: Option<String>,
    version_id: Option<String>,
    scan_mode: Option<HealScanMode>,
    started_at: Instant,
}

fn scanner_heal_candidate_trace_context(request: &HealChannelRequest) -> Option<ScannerHealCandidateTraceContext> {
    let started_at = trace_start_instant()?;
    Some(ScannerHealCandidateTraceContext {
        bucket: request.bucket.clone(),
        object: request.object_prefix.clone(),
        version_id: request.object_version_id.clone(),
        scan_mode: request.scan_mode,
        started_at,
    })
}

struct ScannerHealCandidateTrace<'a> {
    candidate_type: &'static str,
    bucket: &'a str,
    object: Option<&'a str>,
    version_id: Option<&'a str>,
    priority: HealChannelPriority,
    scan_mode: Option<HealScanMode>,
    result: Result<HealAdmissionResult, &'a str>,
    started_at: Instant,
}

fn emit_scanner_heal_candidate_trace(trace: ScannerHealCandidateTrace<'_>) {
    trace_emit(|| {
        let (state, admission, error) = match trace.result {
            Ok(result) if result.is_admitted() => ("admitted", describe_heal_admission(result), None),
            Ok(result) => ("not_admitted", describe_heal_admission(result), None),
            Err(error) => ("submit_failed", "channel_error".to_string(), Some(error)),
        };
        let mut event = TraceEvent::new(TraceKind::Scanner, TraceFunc::ScannerHealCandidate)
            .with_bucket(trace.bucket)
            .with_duration(trace.started_at.elapsed())
            .with_attr("state", state)
            .with_attr("candidate_type", trace.candidate_type)
            .with_attr("priority", heal_priority_label(trace.priority))
            .with_attr("admission", admission);

        if let Some(object) = trace.object {
            event = event.with_object(object);
        }
        if let Some(version_id) = trace.version_id {
            event = event.with_attr("version_id", version_id);
        }
        if let Some(scan_mode) = trace.scan_mode {
            event = event.with_attr("scan_mode", scan_mode.as_str());
        }
        if let Some(error) = error {
            event = event.with_attr("error", error);
        }

        event
    });
}

fn apply_scanner_size_summary(into: &mut DataUsageEntry, summary: &SizeSummary) {
    into.size = into.size.saturating_add(summary.total_size);
    into.versions = into.versions.saturating_add(summary.versions);
    into.delete_markers = into.delete_markers.saturating_add(summary.delete_markers);
    into.obj_sizes.add(u64::try_from(summary.total_size).unwrap_or(u64::MAX));
    into.obj_versions.add(u64::try_from(summary.versions).unwrap_or(u64::MAX));

    let replication_stats = into.replication_stats.get_or_insert_with(Default::default);
    replication_stats.replica_size = replication_stats
        .replica_size
        .saturating_add(non_negative_i64_to_u64(summary.replica_size));
    replication_stats.replica_count = replication_stats
        .replica_count
        .saturating_add(u64::try_from(summary.replica_count).unwrap_or(u64::MAX));

    for (arn, st) in &summary.repl_target_stats {
        let tgt_stat = replication_stats.targets.entry(arn.clone()).or_default();
        tgt_stat.pending_size = tgt_stat.pending_size.saturating_add(non_negative_i64_to_u64(st.pending_size));
        tgt_stat.failed_size = tgt_stat.failed_size.saturating_add(non_negative_i64_to_u64(st.failed_size));
        tgt_stat.replicated_size = tgt_stat
            .replicated_size
            .saturating_add(non_negative_i64_to_u64(st.replicated_size));
        tgt_stat.replicated_count = tgt_stat
            .replicated_count
            .saturating_add(u64::try_from(st.replicated_count).unwrap_or(u64::MAX));
        tgt_stat.failed_count = tgt_stat
            .failed_count
            .saturating_add(u64::try_from(st.failed_count).unwrap_or(u64::MAX));
        tgt_stat.pending_count = tgt_stat
            .pending_count
            .saturating_add(u64::try_from(st.pending_count).unwrap_or(u64::MAX));
    }

    into.add_tier_sizes(&summary.tier_stats);
}

fn data_usage_root_has_progress(root: &DataUsageEntry) -> bool {
    !root.children.is_empty()
        || root.size > 0
        || root.objects > 0
        || root.versions > 0
        || root.delete_markers > 0
        || root.failed_objects > 0
        || root.replication_stats.is_some()
}

fn partial_cache_is_useful(root: &DataUsageEntry, pending_heals_changed: bool) -> bool {
    data_usage_root_has_progress(root) || pending_heals_changed
}

/// Folder scanner for scanning directory structures
pub struct FolderScanner {
    root: String,
    old_cache: DataUsageCache,
    new_cache: DataUsageCache,
    update_cache: DataUsageCache,

    data_usage_scanner_debug: bool,
    heal_object_select: u32,
    scan_mode: HealScanMode,
    is_erasure_mode: bool,

    failed_object_ttl_secs: u64,
    failed_objects_max: usize,

    sleeper: DynamicSleeper,
    // should_heal: Arc<dyn Fn() -> bool + Send + Sync>,
    disks: Vec<Arc<Disk>>,
    disks_quorum: usize,

    updates: Option<mpsc::Sender<DataUsageEntry>>,
    last_update: SystemTime,

    update_current_path: UpdateCurrentPathFn,

    budget: Arc<ScannerCycleBudget>,
    skip_heal: Arc<std::sync::atomic::AtomicBool>,
    local_disk: Arc<Disk>,
    pending_heals_changed: bool,
    pending_size_reconciliation_keys: HashSet<String>,
    pending_size_reconciliation_scopes: HashSet<String>,
    pending_size_reconciliation_truncated: bool,
    #[cfg(test)]
    list_path_raw_options_observer: Option<mpsc::UnboundedSender<ListPathRawTimeoutSnapshot>>,
}

fn size_reconciliation_entry_bytes(entry: &SizeReconciliationEntry) -> usize {
    entry.key.len()
        + entry.bucket.len()
        + entry.object.len()
        + entry.version_id.as_deref().map_or(0, str::len)
        + entry.generation.as_deref().map_or(0, str::len)
        + entry.reason.len()
        + std::mem::size_of::<u64>()
        + std::mem::size_of::<u32>()
}

fn size_reconciliation_scope_key(bucket: &str, object: &str) -> String {
    format!("{}:{}|{}:{}", bucket.len(), bucket, object.len(), object)
}

fn prune_size_reconciliation(info: &mut DataUsageCacheInfo, now: u64) {
    info.size_reconciliation.retain(|key, entry| {
        if entry.first_seen == 0 || entry.first_seen > now {
            entry.first_seen = now;
        }
        key == &entry.key
            && entry.key.len() <= 4096
            && entry.bucket.len() <= 512
            && entry.object.len() <= 512
            && entry.version_id.as_deref().is_none_or(|value| value.len() <= 64)
            && entry.generation.as_deref().is_none_or(|value| value.len() <= 64)
            && entry.reason.len() <= 64
            && now.saturating_sub(entry.first_seen) <= MAX_SIZE_RECONCILIATION_AGE_SECS
    });

    while info.size_reconciliation.len() > MAX_SIZE_RECONCILIATION_ENTRIES_PER_BUCKET
        || info
            .size_reconciliation
            .values()
            .map(size_reconciliation_entry_bytes)
            .sum::<usize>()
            > MAX_SIZE_RECONCILIATION_BYTES_PER_BUCKET
    {
        let oldest = info
            .size_reconciliation
            .iter()
            .min_by(|(left_key, left), (right_key, right)| {
                left.first_seen.cmp(&right.first_seen).then_with(|| left_key.cmp(right_key))
            })
            .map(|(key, _)| key.clone());
        let Some(oldest) = oldest else {
            break;
        };
        info.size_reconciliation.remove(&oldest);
    }
}

impl FolderScanner {
    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn should_skip_failed(&self, path: &str) -> bool {
        let ttl = self.failed_object_ttl_secs;
        if ttl == 0 {
            return false;
        }

        let Some(last_failed) = self.new_cache.info.failed_objects.get(path) else {
            return false;
        };

        let now = Self::now_secs();
        now.saturating_sub(*last_failed) < ttl
    }

    fn record_failed(&mut self, path: &str) {
        let ttl = self.failed_object_ttl_secs;
        if ttl == 0 {
            return;
        }

        let now = Self::now_secs();
        self.new_cache.info.failed_objects.insert(path.to_string(), now);

        let max_entries = self.failed_objects_max;
        if max_entries > 0 && self.new_cache.info.failed_objects.len() > max_entries {
            self.prune_failed_objects(now, ttl);
        }
    }

    fn prune_failed_objects_cache(&mut self) {
        let ttl = self.failed_object_ttl_secs;
        if ttl == 0 {
            return;
        }

        let now = Self::now_secs();
        self.prune_failed_objects(now, ttl);
    }

    fn prune_failed_objects(&mut self, now: u64, ttl: u64) {
        let max_entries = self.failed_objects_max;
        let failed = &mut self.new_cache.info.failed_objects;
        if failed.is_empty() {
            return;
        }

        failed.retain(|_, ts| now.saturating_sub(*ts) < ttl);

        if max_entries == 0 {
            return;
        }

        if failed.len() <= max_entries {
            return;
        }

        let mut entries: Vec<(String, u64)> = failed.iter().map(|(k, v)| (k.clone(), *v)).collect();
        entries.sort_by(|(k1, ts1), (k2, ts2)| ts1.cmp(ts2).then_with(|| k1.cmp(k2)));

        let remove_count = failed.len().saturating_sub(max_entries);
        for (key, _) in entries.into_iter().take(remove_count) {
            failed.remove(&key);
        }
    }

    /// Apply the per-object size-resolution ledger updates in one place. The
    /// scanner cache is the durable boundary; both working copies are updated
    /// so an incremental publication cannot lose a debt or its resolution.
    fn apply_size_reconciliation(&mut self, summary: &SizeSummary) {
        let now = Self::now_secs();
        self.pending_size_reconciliation_keys
            .extend(summary.size_reconciliation.iter().map(|entry| entry.key.clone()));
        self.pending_size_reconciliation_scopes.extend(
            summary
                .reconciliation_scopes
                .iter()
                .map(|scope| size_reconciliation_scope_key(&scope.bucket, &scope.object)),
        );
        self.pending_size_reconciliation_truncated |= summary.size_reconciliation_truncated;

        for info in [&mut self.new_cache.info, &mut self.update_cache.info] {
            for incoming in &summary.size_reconciliation {
                if let Some(existing) = info.size_reconciliation.get_mut(&incoming.key) {
                    existing.reason = incoming.reason.clone();
                    existing.physical_size = incoming.physical_size;
                    existing.generation = incoming.generation.clone();
                    existing.version_id = incoming.version_id.clone();
                    existing.attempts = existing.attempts.saturating_add(1);
                    continue;
                }

                if size_reconciliation_entry_bytes(incoming) > MAX_SIZE_RECONCILIATION_BYTES_PER_BUCKET {
                    continue;
                }

                let mut entry = incoming.clone();
                entry.first_seen = now;
                entry.attempts = 1;
                info.size_reconciliation.insert(entry.key.clone(), entry);
            }
        }
    }

    fn finish_size_reconciliation_batch(&mut self) {
        let now = Self::now_secs();
        let current_keys = std::mem::take(&mut self.pending_size_reconciliation_keys);
        let scopes = std::mem::take(&mut self.pending_size_reconciliation_scopes);
        let truncated = std::mem::replace(&mut self.pending_size_reconciliation_truncated, false);

        for info in [&mut self.new_cache.info, &mut self.update_cache.info] {
            if !truncated {
                info.size_reconciliation.retain(|key, entry| {
                    !scopes.contains(&size_reconciliation_scope_key(&entry.bucket, &entry.object)) || current_keys.contains(key)
                });
            }
            prune_size_reconciliation(info, now);
        }
    }

    fn record_scan_resume_hint(&mut self, folder: &str) {
        self.new_cache.info.scan_resume_after = Some(folder.to_string());
        self.update_cache.info.scan_resume_after = Some(folder.to_string());
        let checkpoint = DataUsageScanCheckpoint::new(folder.to_string(), DataUsageScanCheckpointReason::Unknown);
        global_metrics().record_scanner_checkpoint_set(
            checkpoint.version,
            checkpoint.resume_after.clone(),
            checkpoint.reason.as_str(),
        );
        self.new_cache.info.scan_checkpoint = Some(checkpoint.clone());
        self.update_cache.info.scan_checkpoint = Some(checkpoint);
    }

    fn record_scan_resume_hint_if_not_ancestor(&mut self, folder: &str) {
        let keep_existing = self
            .new_cache
            .info
            .scan_resume_after
            .as_deref()
            .is_some_and(|existing| matches!(folder_resume_match(folder, existing), Some(FolderResumeMatch::Descendant)));
        if !keep_existing {
            self.record_scan_resume_hint(folder);
        }
    }

    fn carry_forward_old_children(&mut self, parent_hash: &DataUsageHash, entry: &mut DataUsageEntry) {
        if entry.compacted {
            // Compacted entries store child totals directly; child links would be flattened twice.
            return;
        }

        let Some(old_entry) = self.old_cache.cache.get(&parent_hash.key()) else {
            return;
        };

        let old_children = old_entry.children.iter().cloned().collect::<Vec<_>>();
        for child in old_children {
            if entry.children.contains(&child) {
                continue;
            }
            if !self.old_cache.cache.contains_key(&child) {
                continue;
            }

            let child_hash = DataUsageHash(child.clone());
            self.new_cache
                .copy_with_children(&self.old_cache, &child_hash, &Some(parent_hash.clone()));
            entry.children.insert(child);
        }
    }

    async fn preserve_partial_child_progress(
        &mut self,
        parent: &Option<DataUsageHash>,
        child_hash: &DataUsageHash,
        parent_entry: &mut DataUsageEntry,
        child_entry: &DataUsageEntry,
    ) {
        if data_usage_entry_has_progress(child_entry) {
            let mut child_entry = child_entry.clone();
            self.carry_forward_old_children(child_hash, &mut child_entry);
            self.record_scan_resume_hint_if_not_ancestor(&child_hash.key());
            parent_entry.add_child(child_hash);
            self.new_cache.replace_hashed(child_hash, parent, &child_entry);
            self.update_cache.delete_recursive(child_hash);
            self.update_cache.copy_with_children(&self.new_cache, child_hash, parent);
            self.send_update().await;
        }
    }

    fn alert_excessive_folders(&self, folder: &str, total_folders: usize) {
        let threshold = scanner_excess_folders_threshold();
        if u64::try_from(total_folders).unwrap_or(u64::MAX) <= threshold {
            return;
        }

        ensure_scanner_alert_metrics_registered();
        global_metrics().record_scanner_source_executed(ScannerWorkSource::Alerts, 1);
        counter!(
            METRIC_SCANNER_EXCESS_FOLDERS_TOTAL,
            "root" => self.root.clone()
        )
        .increment(1);
        if scanner_alert_emission_allows(ScannerAlertKind::BigPrefix, &self.root, folder, scanner_alert_cooldown()) {
            emit_scanner_alert_event(
                EVENT_SCANNER_BIG_PREFIX,
                &self.root,
                folder,
                0,
                &[("folders", total_folders.to_string()), ("threshold", threshold.to_string())],
            );
        }
        warn!(
            target: "rustfs::scanner::folder",
            event = EVENT_SCANNER_ALERT_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_FOLDER,
            root = %self.root,
            folder,
            folders = total_folders,
            threshold,
            state = "excess_folders",
            "Scanner alert recorded excessive direct subfolders"
        );
    }

    pub async fn should_heal(&self) -> bool {
        if self.skip_heal.load(std::sync::atomic::Ordering::Relaxed) {
            return false;
        }
        if self.heal_object_select == 0 {
            return false;
        }

        if self
            .local_disk
            .disk_info(&DiskInfoOptions::default())
            .await
            .unwrap_or_default()
            .healing
        {
            self.skip_heal.store(true, std::sync::atomic::Ordering::Relaxed);
            return false;
        }

        true
    }

    async fn send_required_scanner_heal_request(
        &mut self,
        kind: PendingScannerHealKind,
        bucket: String,
        object: Option<String>,
        version_id: Option<String>,
        request: HealChannelRequest,
    ) -> Result<HealAdmissionResult, ScannerError> {
        let candidate_type = pending_scanner_heal_candidate_type(kind);
        let priority = request.priority;
        let scan_mode = request.scan_mode.unwrap_or(self.scan_mode);
        let result = match send_scanner_heal_request(candidate_type, request).await {
            Ok(result) => result,
            Err(err) => {
                self.update_pending_scanner_heal_after_admission(
                    kind,
                    &bucket,
                    object.as_deref(),
                    version_id.as_deref(),
                    scan_mode,
                    HealAdmissionResult::Full,
                );
                error!(
                    target: "rustfs::scanner::folder",
                    event = EVENT_SCANNER_HEAL_ADMISSION,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_HEAL,
                    candidate_type,
                    bucket = %bucket,
                    object = object.as_deref().unwrap_or(""),
                    priority = heal_priority_label(priority),
                    state = "heal_channel_error",
                    error = %err,
                    "Scanner deferred heal request after channel error"
                );
                return Ok(HealAdmissionResult::Full);
            }
        };
        self.update_pending_scanner_heal_after_admission(
            kind,
            &bucket,
            object.as_deref(),
            version_id.as_deref(),
            scan_mode,
            result,
        );
        if result.is_admitted() {
            return Ok(result);
        }

        record_high_priority_heal_escalation(candidate_type, priority, result);
        let admission_error =
            build_high_priority_heal_admission_error(candidate_type, &bucket, object.as_deref(), priority, result);
        error!(
            target: "rustfs::scanner::folder",
            event = EVENT_SCANNER_HEAL_ADMISSION,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_HEAL,
            candidate_type,
            bucket = %bucket,
            object = object.as_deref().unwrap_or(""),
            priority = heal_priority_label(priority),
            admission = result.result_label(),
            reason = result.reason_label(),
            error = %admission_error,
            state = "high_priority_not_admitted",
            "Scanner high-priority heal admission failed"
        );
        Ok(result)
    }

    pub fn set_heal_object_select(&mut self, prob: u32) {
        self.heal_object_select = prob;
    }

    /// Set debug mode
    pub fn set_debug(&mut self, debug: bool) {
        self.data_usage_scanner_debug = debug;
    }

    /// Send update if enough time has passed
    /// Should be called on a regular basis when the new_cache contains more recent total than previously.
    /// May or may not send an update upstream.
    fn should_send_update(&self) -> bool {
        if self.updates.is_none() {
            return false;
        }

        let elapsed = self.last_update.elapsed().unwrap_or(Duration::from_secs(0));
        elapsed >= Duration::from_secs(60)
    }

    pub async fn send_update(&mut self) {
        // Send at most an update every minute.
        if !self.should_send_update() {
            return;
        }

        if let Some(flat) = self.update_cache.size_recursive(&self.new_cache.info.name)
            && let Some(ref updates) = self.updates
        {
            // Try to send without blocking
            if let Err(e) = updates.send(flat.clone()).await {
                error!(
                    target: "rustfs::scanner::folder",
                    event = EVENT_SCANNER_FOLDER_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_FOLDER,
                    root = %self.new_cache.info.name,
                    state = "update_send_failed",
                    error = %e,
                    "Scanner folder update send failed"
                );
            }
            self.last_update = SystemTime::now();
        }
    }

    async fn send_update_for_entry(&mut self, hash: &DataUsageHash, parent: &Option<DataUsageHash>, entry: &DataUsageEntry) {
        if !self.should_send_update() {
            return;
        }

        self.update_cache.replace_hashed(hash, parent, entry);
        self.send_update().await;
    }

    /// Scan a folder recursively
    /// Files found in the folders will be added to new_cache.
    #[allow(clippy::never_loop)]
    #[allow(unused_assignments)]
    pub async fn scan_folder(
        &mut self,
        ctx: CancellationToken,
        folder: CachedFolder,
        into: &mut DataUsageEntry,
    ) -> Result<(), ScannerError> {
        let done_folder = Metrics::time(Metric::ScanFolder);
        let trace_started_at = trace_start_instant();

        if ctx.is_cancelled() {
            return Err(ScannerError::Other("Operation cancelled".to_string()));
        }
        if !self.budget.try_start_directory() {
            return Err(ScannerError::Other("Operation cancelled".to_string()));
        }

        let this_hash = hash_path(&folder.name);
        // Store initial compaction state.
        let was_compacted = into.compacted;

        loop {
            if ctx.is_cancelled() {
                return Err(ScannerError::Other("Operation cancelled".to_string()));
            }

            self.prune_failed_objects_cache();

            let mut abandoned_children: DataUsageHashMap = HashSet::new();
            if !into.compacted {
                abandoned_children = self.old_cache.find_children_copy(this_hash.clone());
            }

            debug!(
                target: "rustfs::scanner::folder",
                event = EVENT_SCANNER_FOLDER_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_FOLDER,
                root = %self.root,
                folder = %folder.name,
                state = "scan_started",
                "Scanner folder state updated"
            );
            let (_, prefix) = path2_bucket_object_with_base_path(&self.root, &folder.name);

            let active_life_cycle = if self
                .old_cache
                .info
                .lifecycle
                .as_ref()
                .is_some_and(|v| v.has_active_rules(&prefix))
            {
                self.old_cache.info.lifecycle.clone()
            } else {
                None
            };

            let active_replication = if self
                .old_cache
                .info
                .replication
                .as_ref()
                .is_some_and(|v| v.has_active_rules(&prefix, true))
            {
                self.old_cache.info.replication.clone()
            } else {
                None
            };
            let active_object_lock = self.old_cache.info.object_lock.clone();

            self.sleeper.sleep_folder().await;

            let mut existing_folders: Vec<CachedFolder> = Vec::new();
            let mut new_folders: Vec<CachedFolder> = Vec::new();
            let mut found_object_metadata = false;
            let mut erasure_data_directory_candidates: Vec<(CachedFolder, bool, String)> = Vec::new();
            let mut object_count: u64 = 0;
            let yield_every_objects = scanner_yield_every_n_objects();

            let dir_path = path_join_buf(&[&self.root, &folder.name]);

            debug!(
                target: "rustfs::scanner::folder",
                event = EVENT_SCANNER_FOLDER_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_FOLDER,
                dir_path = ?dir_path,
                state = "dir_open",
                "Scanner folder state updated"
            );

            let mut dir_reader = match tokio::fs::read_dir(&dir_path).await {
                Ok(dir_reader) => dir_reader,
                Err(e) => return Err(ScannerError::Io(e)),
            };
            let mut pending_entry_progress = 0_u64;
            let mut last_entry_progress = Instant::now();

            loop {
                let entry = match dir_reader.next_entry().await {
                    Ok(Some(entry)) => entry,
                    Ok(None) => break,
                    Err(e) if e.kind() == ErrorKind::NotFound => {
                        debug!(
                            target: "rustfs::scanner::folder",
                            event = EVENT_SCANNER_FOLDER_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_FOLDER,
                            dir_path = %dir_path,
                            state = "dir_missing_during_iteration",
                            error = %e,
                            "Scanner folder state updated"
                        );
                        break;
                    }
                    Err(e) if e.kind() == ErrorKind::NotADirectory => {
                        warn!(
                            target: "rustfs::scanner::folder",
                            event = EVENT_SCANNER_FOLDER_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_FOLDER,
                            dir_path = %dir_path,
                            state = "dir_became_non_directory",
                            error = %e,
                            "Scanner folder state updated"
                        );
                        break;
                    }
                    Err(e) => return Err(ScannerError::Io(e)),
                };
                pending_entry_progress = pending_entry_progress.saturating_add(1);
                if pending_entry_progress >= SCANNER_ENTRY_PROGRESS_BATCH
                    || last_entry_progress.elapsed() >= SCANNER_ENTRY_PROGRESS_INTERVAL
                {
                    self.budget.record_entries_visited(pending_entry_progress);
                    pending_entry_progress = 0;
                    last_entry_progress = Instant::now();
                }
                let file_name = entry.file_name().to_string_lossy().to_string();
                if file_name.is_empty() || file_name == "." || file_name == ".." {
                    continue;
                }
                let is_storage_format_entry = file_name == STORAGE_FORMAT_FILE;

                let file_path = entry.path().to_string_lossy().to_string();

                let trim_dir_name = file_path.strip_prefix(&dir_path).unwrap_or(&file_path);

                let entry_name = path_join_buf(&[&folder.name, trim_dir_name]);

                if entry_name.is_empty() || entry_name == folder.name {
                    continue;
                }

                // Ignore entries that disappeared during traversal or hit symlink
                // loops, but propagate other walk errors.
                let mut entry_type = match entry.file_type().await {
                    Ok(entry_type) => entry_type,
                    Err(e) if e.kind() == ErrorKind::NotFound => {
                        debug!(
                            target: "rustfs::scanner::folder",
                            event = EVENT_SCANNER_FOLDER_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_FOLDER,
                            entry = %entry_name,
                            state = "entry_missing_before_type_lookup",
                            error = %e,
                            "Scanner folder state updated"
                        );
                        continue;
                    }
                    Err(e) if e.kind() == ErrorKind::TooManyLinks => {
                        warn!(
                            target: "rustfs::scanner::folder",
                            event = EVENT_SCANNER_FOLDER_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_FOLDER,
                            entry = %entry_name,
                            state = "entry_symlink_loop_before_type_lookup",
                            error = %e,
                            "Scanner folder state updated"
                        );
                        continue;
                    }
                    Err(e) => return Err(ScannerError::Io(e)),
                };

                // Metadata presence establishes an erasure object boundary;
                // parsing failures still belong to accounting and healing. A
                // directory named `xl.meta` remains a valid namespace prefix,
                // and symlinks are classified after resolving their target.
                if is_storage_format_entry && !entry_type.is_dir() && !entry_type.is_symlink() {
                    found_object_metadata = true;
                }

                if entry_type.is_symlink() {
                    let metadata = match tokio::fs::metadata(&file_path).await {
                        Ok(metadata) => metadata,
                        Err(e) if e.kind() == ErrorKind::NotFound => {
                            debug!(
                                target: "rustfs::scanner::folder",
                                event = EVENT_SCANNER_FOLDER_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_FOLDER,
                                file_path = %file_path,
                                state = "symlink_target_missing_before_metadata",
                                error = %e,
                                "Scanner folder state updated"
                            );
                            continue;
                        }
                        Err(e) if e.kind() == ErrorKind::TooManyLinks => {
                            warn!(
                                target: "rustfs::scanner::folder",
                                event = EVENT_SCANNER_FOLDER_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_FOLDER,
                                file_path = %file_path,
                                state = "symlink_target_loop_before_metadata",
                                error = %e,
                                "Scanner folder state updated"
                            );
                            continue;
                        }
                        Err(e) => return Err(ScannerError::Io(e)),
                    };

                    if metadata.is_dir() {
                        warn!(
                            target: "rustfs::scanner::folder",
                            event = EVENT_SCANNER_FOLDER_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_FOLDER,
                            file_path = %file_path,
                            state = "symlink_directory_ignored",
                            "Scanner folder state updated"
                        );
                        continue;
                    }

                    entry_type = metadata.file_type();
                    if is_storage_format_entry {
                        found_object_metadata = true;
                    }
                }

                // ok

                let (bucket, prefix) = path2_bucket_object_with_base_path(self.root.as_str(), &entry_name);
                if bucket.is_empty() {
                    break;
                }

                if is_reserved_or_invalid_bucket(&bucket, false) {
                    break;
                }

                if ctx.is_cancelled() {
                    break;
                }

                if entry_type.is_dir() {
                    let h = hash_path(&entry_name);

                    if h == this_hash {
                        continue;
                    }

                    let exists = self.old_cache.cache.contains_key(&h.key());

                    let this = CachedFolder {
                        name: entry_name.clone(),
                        parent: Some(this_hash.clone()),
                        object_heal_prob_div: folder.object_heal_prob_div,
                    };

                    if self.is_erasure_mode && uuid::Uuid::parse_str(&file_name).is_ok_and(|data_dir_id| !data_dir_id.is_nil()) {
                        erasure_data_directory_candidates.push((this, exists, file_path));
                        continue;
                    }

                    abandoned_children.remove(&h.key());

                    if exists {
                        existing_folders.push(this);
                        self.update_cache
                            .copy_with_children(&self.old_cache, &h, &Some(this_hash.clone()));
                    } else {
                        new_folders.push(this);
                    }
                    continue;
                }

                let timer = self.sleeper.timer();

                let heal_enabled = this_hash.mod_alt(
                    self.old_cache.info.next_cycle as u32 / folder.object_heal_prob_div,
                    self.heal_object_select / folder.object_heal_prob_div,
                ) && self.should_heal().await;

                let mut item = ScannerItem {
                    path: file_path,
                    bucket,
                    prefix: rustfs_utils::path::dir(&prefix),
                    object_name: file_name,
                    lifecycle: active_life_cycle.clone(),
                    object_lock: active_object_lock.clone(),
                    replication: active_replication.clone(),
                    heal_enabled,
                    heal_bitrot: self.scan_mode == HealScanMode::Deep,
                    debug: self.data_usage_scanner_debug,
                    file_type: entry_type,
                };

                // If this path is already known as failed, just skip it.
                // We intentionally do NOT call `record_failed` or bump `failed_objects` here,
                // because the failure was recorded when the original error occurred
                // (e.g. in the get_size error branch below). This branch only accounts
                // for subsequent skips of already-failed paths.
                if self.should_skip_failed(&item.path) {
                    continue;
                }

                let sz = match self.local_disk.get_size(item.clone()).await {
                    Ok(sz) => sz,
                    Err(e) => {
                        let failure_action = classify_get_size_failure(&item, &e);

                        if failure_action != GetSizeFailureAction::Skip {
                            // Track failed objects to prevent infinite retry loops
                            into.failed_objects += 1;
                            self.record_failed(&item.path);

                            if should_log_failed_object(into.failed_objects) {
                                if let GetSizeFailureAction::HealMetadata { object } = &failure_action {
                                    error!(
                                        target: "rustfs::scanner::folder",
                                        event = EVENT_SCANNER_METADATA_CORRUPT,
                                        component = LOG_COMPONENT_SCANNER,
                                        subsystem = LOG_SUBSYSTEM_FOLDER,
                                        drive = %self.local_disk.path().display(),
                                        bucket = %item.bucket,
                                        object = %object,
                                        metadata_path = %item.path,
                                        failed_objects = into.failed_objects,
                                        state = "metadata_corrupt",
                                        error = %e,
                                        "Scanner detected corrupt object metadata"
                                    );
                                } else {
                                    warn!(
                                        target: "rustfs::scanner::folder",
                                        event = EVENT_SCANNER_FOLDER_STATE,
                                        component = LOG_COMPONENT_SCANNER,
                                        subsystem = LOG_SUBSYSTEM_FOLDER,
                                        path = %item.path,
                                        failed_objects = into.failed_objects,
                                        state = "get_size_failed",
                                        error = %e,
                                        "Scanner folder failed to get object size"
                                    );
                                }
                            }
                        }

                        if let GetSizeFailureAction::HealMetadata { object } = failure_action {
                            // Single-flight (backlog#1894 axis A) — the
                            // recording mode and its guarantees are pinned by
                            // corrupt_metadata_recording below.
                            let mrf_result = rustfs_common::mrf_channel::try_send_mrf_intent_typed(
                                rustfs_common::mrf_channel::MrfKind::MetadataCorruption,
                                &item.bucket,
                                &object,
                                None,
                                None,
                            );
                            match corrupt_metadata_recording(mrf_result) {
                                CorruptMetadataRecording::LedgerOnly => {
                                    // Recorded as Full (retry-later): admission
                                    // for this target happens in the MRF
                                    // consumer, not in the manager's queue here.
                                    self.update_pending_scanner_heal_after_admission(
                                        PendingScannerHealKind::Object,
                                        &item.bucket,
                                        Some(&object),
                                        None,
                                        self.scan_mode,
                                        HealAdmissionResult::Full,
                                    );
                                }
                                CorruptMetadataRecording::ImmediateAndLedger => {
                                    self.send_required_scanner_heal_request(
                                        PendingScannerHealKind::Object,
                                        item.bucket.clone(),
                                        Some(object.clone()),
                                        None,
                                        build_object_heal_request(
                                            item.bucket.clone(),
                                            object.clone(),
                                            None,
                                            self.scan_mode,
                                            HealChannelPriority::High,
                                        ),
                                    )
                                    .await?;
                                }
                            }
                        }

                        timer.sleep().await;
                        continue;
                    }
                };

                found_object_metadata = true;

                item.transform_meta_dir();

                abandoned_children.remove(&path_join_buf(&[&item.bucket, &item.object_path()]));

                apply_scanner_size_summary(into, &sz);
                self.apply_size_reconciliation(&sz);
                into.objects += 1;
                object_count += 1;
                self.budget.record_object_scanned();

                timer.sleep().await;

                if ctx.is_cancelled() {
                    return Err(ScannerError::Other("Operation cancelled".to_string()));
                }

                if should_yield_after_object(object_count, yield_every_objects) {
                    self.send_update_for_entry(&this_hash, &folder.parent, into).await;
                    let yield_start = Instant::now();
                    tokio::task::yield_now().await;
                    global_metrics().record_scanner_yield(yield_start.elapsed());
                }
            }
            self.budget.record_entries_visited(pending_entry_progress);

            let mut found_erasure_data_directory = false;
            if self.is_erasure_mode && !found_object_metadata {
                for (_, _, path) in &erasure_data_directory_candidates {
                    if contains_erasure_part_file(path).await? {
                        found_erasure_data_directory = true;
                        break;
                    }
                }
            }

            if !found_object_metadata && !found_erasure_data_directory {
                for (candidate, exists, _) in erasure_data_directory_candidates {
                    let h = hash_path(&candidate.name);
                    abandoned_children.remove(&h.key());
                    if exists {
                        self.update_cache.copy_with_children(&self.old_cache, &h, &candidate.parent);
                        existing_folders.push(candidate);
                    } else {
                        new_folders.push(candidate);
                    }
                }
            }

            if self.is_erasure_mode && found_erasure_data_directory && !found_object_metadata {
                found_object_metadata = true;
                let metadata_path = path_join_buf(&[&dir_path, STORAGE_FORMAT_FILE]);

                if !self.should_skip_failed(&metadata_path) {
                    into.failed_objects = into.failed_objects.saturating_add(1);
                    self.record_failed(&metadata_path);

                    let failed_cache_entries = self.new_cache.info.failed_objects.len();
                    if failed_cache_entries > 0 && should_log_failed_object(failed_cache_entries) {
                        warn!(
                            target: "rustfs::scanner::folder",
                            event = EVENT_SCANNER_FOLDER_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_FOLDER,
                            path = %metadata_path,
                            failed_objects = failed_cache_entries,
                            state = "object_metadata_missing",
                            "Scanner found erasure object data without metadata"
                        );
                    }

                    let (bucket, object) = path2_bucket_object_with_base_path(&self.root, &folder.name);
                    if !bucket.is_empty() && !object.is_empty() {
                        self.send_required_scanner_heal_request(
                            PendingScannerHealKind::Object,
                            bucket.clone(),
                            Some(object.clone()),
                            None,
                            build_object_heal_request(bucket, object, None, self.scan_mode, HealChannelPriority::High),
                        )
                        .await?;
                    }
                }
            }

            if ctx.is_cancelled() {
                return Err(ScannerError::Other("Operation cancelled".to_string()));
            }

            if found_object_metadata && self.is_erasure_mode {
                // If we found an object in erasure mode, we skip subdirs (only datadirs)...
                debug!(
                    target: "rustfs::scanner::folder",
                    event = EVENT_SCANNER_FOLDER_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_FOLDER,
                    folder = %folder.name,
                    state = "erasure_object_found",
                    "Scanner folder descent stopped after erasure object"
                );
                break;
            }

            // If we have many subfolders, compact ourself.
            let should_compact = (self.new_cache.info.name != folder.name
                && existing_folders.len() + new_folders.len() >= DATA_SCANNER_COMPACT_AT_FOLDERS)
                || existing_folders.len() + new_folders.len() >= DATA_SCANNER_FORCE_COMPACT_AT_FOLDERS;

            let total_folders = existing_folders.len() + new_folders.len();
            self.alert_excessive_folders(&folder.name, total_folders);

            if !into.compacted && should_compact {
                into.compacted = true;
                new_folders.append(&mut existing_folders);

                existing_folders.clear();

                if self.data_usage_scanner_debug {
                    debug!(
                        target: "rustfs::scanner::folder",
                        event = EVENT_SCANNER_FOLDER_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_FOLDER,
                        folder = %folder.name,
                        entry_count = new_folders.len(),
                        state = "preemptive_compaction",
                        "Scanner folder switched to compacted mode"
                    );
                }
            }

            if !into.compacted {
                for folder_item in &existing_folders {
                    let h = hash_path(&folder_item.name);
                    self.update_cache.copy_with_children(&self.old_cache, &h, &folder_item.parent);
                }
            }

            let is_scan_root = folder.name == self.old_cache.info.name;
            let scan_checkpoint = self.old_cache.info.scan_checkpoint.as_ref();
            let checkpoint_resume_after = scan_checkpoint.and_then(|checkpoint| {
                if is_scan_root {
                    global_metrics().record_scanner_checkpoint_set(
                        checkpoint.version,
                        checkpoint.resume_after.clone(),
                        checkpoint.reason.as_str(),
                    );
                }
                if checkpoint.version != DATA_USAGE_SCAN_CHECKPOINT_VERSION || checkpoint.resume_after.is_empty() {
                    if is_scan_root {
                        global_metrics().record_scanner_checkpoint_ignored();
                    }
                    None
                } else {
                    Some(checkpoint.resume_after.as_str())
                }
            });
            let checkpoint_tracks_child_order = checkpoint_resume_after
                .and_then(|resume_after| folder_resume_match(&folder.name, resume_after))
                .is_some_and(|resume_match| matches!(resume_match, FolderResumeMatch::Descendant));
            let scan_resume_after = checkpoint_resume_after.or(self.old_cache.info.scan_resume_after.as_deref());
            let mut queued_folders = Vec::with_capacity(new_folders.len() + existing_folders.len());
            queued_folders.extend(new_folders.into_iter().map(|folder| QueuedFolder {
                folder,
                source: FolderScanSource::New,
            }));
            queued_folders.extend(existing_folders.into_iter().map(|folder| QueuedFolder {
                folder,
                source: FolderScanSource::Existing,
            }));
            let has_queued_folders = !queued_folders.is_empty();
            let resume_order = order_queued_folders_for_resume(&mut queued_folders, scan_resume_after);
            if checkpoint_tracks_child_order && has_queued_folders {
                match resume_order {
                    FolderResumeOrder::Used => global_metrics().record_scanner_checkpoint_used(),
                    FolderResumeOrder::Stale => global_metrics().record_scanner_checkpoint_stale(),
                    FolderResumeOrder::NoHint => {}
                }
            }

            // Scan child folders in the combined resume order.
            for queued_folder in queued_folders {
                if ctx.is_cancelled() {
                    return Err(ScannerError::Other("Operation cancelled".to_string()));
                }

                let mut folder_item = queued_folder.folder;
                let h = hash_path(&folder_item.name);

                match queued_folder.source {
                    FolderScanSource::New => {
                        // Add new folders to the update tree so totals update for these.
                        if !into.compacted {
                            let mut found_any = false;
                            let mut parent = this_hash.clone();
                            let update_cache_name_hash = hash_path(&self.update_cache.info.name);

                            while parent != update_cache_name_hash {
                                let parent_key = parent.key();
                                let e = self.update_cache.find(&parent_key);
                                if e.is_none_or(|v| v.compacted) {
                                    found_any = true;
                                    break;
                                }
                                if let Some(next) = self.update_cache.search_parent(&parent) {
                                    parent = next;
                                } else {
                                    found_any = true;
                                    break;
                                }
                            }
                            if !found_any {
                                // Add non-compacted empty entry.
                                self.update_cache
                                    .replace_hashed(&h, &Some(this_hash.clone()), &DataUsageEntry::default());
                            }
                        }
                    }
                    FolderScanSource::Existing => {
                        if !into.compacted && self.old_cache.is_compacted(&h) {
                            let next_cycle = self.old_cache.info.next_cycle as u32;
                            if !h.mod_(next_cycle, data_usage_update_dir_cycles()) {
                                // Transfer and add as child...
                                self.new_cache.copy_with_children(&self.old_cache, &h, &folder_item.parent);
                                into.add_child(&h);
                                self.record_scan_resume_hint(&folder_item.name);
                                continue;
                            }

                            folder_item.object_heal_prob_div = data_usage_update_dir_cycles();
                        }
                    }
                }

                (self.update_current_path)(&folder_item.name).await;

                if into.compacted {
                    // In compacted mode child totals are accumulated directly into the parent entry.
                    let fut = Box::pin(self.scan_folder(ctx.clone(), folder_item.clone(), into));
                    fut.await.map_err(|e| ScannerError::Other(e.to_string()))?;
                    self.record_scan_resume_hint(&folder_item.name);
                    self.send_update_for_entry(&this_hash, &folder.parent, into).await;
                    tokio::task::yield_now().await;
                } else {
                    let mut dst = DataUsageEntry::default();

                    // Use Box::pin for recursive async call
                    let fut = Box::pin(self.scan_folder(ctx.clone(), folder_item.clone(), &mut dst));
                    if let Err(e) = fut.await {
                        if ctx.is_cancelled() {
                            self.preserve_partial_child_progress(&folder_item.parent, &h, into, &dst)
                                .await;
                            return Err(e);
                        }
                        warn!(
                            target: "rustfs::scanner::folder",
                            event = EVENT_SCANNER_FOLDER_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_FOLDER,
                            folder = %folder.name,
                            child_folder = %folder_item.name,
                            state = "child_scan_failed",
                            error = %e,
                            "Scanner child folder scan failed"
                        );
                        continue;
                    }
                    tokio::task::yield_now().await;

                    into.add_child(&h);
                    self.record_scan_resume_hint(&folder_item.name);
                    // We scanned a folder, optionally send update.
                    self.update_cache.delete_recursive(&h);
                    self.update_cache.copy_with_children(&self.new_cache, &h, &folder_item.parent);
                    self.send_update().await;
                }

                if queued_folder.source == FolderScanSource::New
                    && !into.compacted
                    && self.update_cache.find(&this_hash.key()).is_some_and(|v| !v.compacted)
                {
                    self.update_cache.delete_recursive(&h);
                    self.update_cache
                        .copy_with_children(&self.new_cache, &h, &Some(this_hash.clone()));
                }
            }

            // Scan for healing
            if abandoned_children.is_empty() || !self.should_heal().await {
                debug!(
                    target: "rustfs::scanner::folder",
                    event = EVENT_SCANNER_FOLDER_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_FOLDER,
                    folder = %folder.name,
                    state = "heal_skip_no_abandoned_children",
                    "Scanner folder skipped heal scan for abandoned children"
                );
                // If we are not heal scanning, return now.
                break;
            }

            if self.disks.is_empty() || self.disks_quorum == 0 {
                debug!(
                    target: "rustfs::scanner::folder",
                    event = EVENT_SCANNER_FOLDER_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_FOLDER,
                    folder = %folder.name,
                    disks = self.disks.len(),
                    quorum = self.disks_quorum,
                    state = "heal_skip_no_quorum",
                    "Scanner folder skipped heal scan because quorum is unavailable"
                );
                break;
            }

            let mut previous_bucket = String::new();
            for name in abandoned_children {
                if !self.should_heal().await {
                    break;
                }

                let (bucket, prefix) = path2_bucket_object(name.as_str());

                if bucket != previous_bucket {
                    self.send_required_scanner_heal_request(
                        PendingScannerHealKind::Bucket,
                        bucket.clone(),
                        None,
                        None,
                        build_bucket_heal_request(bucket.clone(), HealChannelPriority::High),
                    )
                    .await?;
                    previous_bucket = bucket.clone();
                }

                let child_ctx = ctx.child_token();

                let (agreed_tx, mut agreed_rx) = mpsc::channel::<String>(1);
                let (partial_tx, mut partial_rx) = mpsc::channel::<MetaCacheEntries>(1);
                let (finished_tx, mut finished_rx) = mpsc::channel::<Vec<Option<DiskError>>>(1);

                let disks = self.disks.iter().cloned().map(Some).collect();
                let disks_quorum = self.disks_quorum;
                let bucket_clone = bucket.clone();
                let prefix_clone = prefix.clone();
                let child_ctx_clone = child_ctx.clone();
                #[cfg(test)]
                let list_path_raw_options_observer = self.list_path_raw_options_observer.clone();

                tokio::spawn(async move {
                    let options = ListPathRawOptions {
                        disks,
                        bucket: bucket_clone.clone(),
                        path: prefix_clone.clone(),
                        recursive: true,
                        report_not_found: true,
                        min_disks: disks_quorum,
                        agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                            let entry_name = entry.name.clone();
                            let agreed_tx = agreed_tx.clone();
                            Box::pin(async move {
                                if let Err(e) = agreed_tx.send(entry_name).await {
                                    error!(
                                        target: "rustfs::scanner::folder",
                                        event = EVENT_SCANNER_FOLDER_STATE,
                                        component = LOG_COMPONENT_SCANNER,
                                        subsystem = LOG_SUBSYSTEM_FOLDER,
                                        entry = %entry.name,
                                        state = "list_path_agreed_send_failed",
                                        error = %e,
                                        "Scanner list_path_raw agreed callback failed"
                                    );
                                }
                            })
                        })),
                        partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<DiskError>]| {
                            let partial_tx = partial_tx.clone();
                            Box::pin(async move {
                                if let Err(e) = partial_tx.send(entries).await {
                                    error!(
                                        target: "rustfs::scanner::folder",
                                        event = EVENT_SCANNER_FOLDER_STATE,
                                        component = LOG_COMPONENT_SCANNER,
                                        subsystem = LOG_SUBSYSTEM_FOLDER,
                                        state = "list_path_partial_send_failed",
                                        error = %e,
                                        "Scanner list_path_raw partial callback failed"
                                    );
                                }
                            })
                        })),
                        finished: Some(Box::new(move |errs: &[Option<DiskError>]| {
                            let finished_tx = finished_tx.clone();
                            let errs_clone = errs.to_vec();
                            Box::pin(async move {
                                if let Err(e) = finished_tx.send(errs_clone).await {
                                    error!(
                                        target: "rustfs::scanner::folder",
                                        event = EVENT_SCANNER_FOLDER_STATE,
                                        component = LOG_COMPONENT_SCANNER,
                                        subsystem = LOG_SUBSYSTEM_FOLDER,
                                        state = "list_path_finished_send_failed",
                                        error = %e,
                                        "Scanner list_path_raw finished callback failed"
                                    );
                                }
                            })
                        })),
                        ..scanner_abandoned_child_list_options()
                    };
                    #[cfg(test)]
                    if let Some(observer) = list_path_raw_options_observer {
                        let _ = observer.send((
                            options.skip_walkdir_total_timeout,
                            options.walkdir_timeout,
                            options.walkdir_stall_timeout,
                        ));
                    }
                    if let Err(e) = list_path_raw(child_ctx_clone.clone(), options).await {
                        if is_missing_path_disk_error(&e) {
                            debug!(
                                target: "rustfs::scanner::folder",
                                event = EVENT_SCANNER_FOLDER_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_FOLDER,
                                bucket = %bucket_clone,
                                prefix = %prefix_clone,
                                state = "list_path_missing",
                                error = %e,
                                "Scanner list_path_raw missing path skipped"
                            );
                        } else {
                            error!(
                                target: "rustfs::scanner::folder",
                                event = EVENT_SCANNER_FOLDER_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_FOLDER,
                                bucket = %bucket_clone,
                                prefix = %prefix_clone,
                                state = "list_path_failed",
                                error = %e,
                                "Scanner list_path_raw failed"
                            );
                        }
                    }
                });

                let mut found_objects = false;
                let mut agreed_closed = false;
                let mut partial_closed = false;
                let mut finished_closed = false;
                let mut seen_heal_candidates: HashSet<(String, Option<String>, MetaCacheHealCandidateKind)> = HashSet::new();
                let mut seen_truncated_objects: HashSet<String> = HashSet::new();

                loop {
                    if agreed_closed && partial_closed && finished_closed {
                        break;
                    }

                    select! {
                        entry_name = agreed_rx.recv(), if !agreed_closed => {
                            let Some(entry_name) = entry_name else {
                                agreed_closed = true;
                                continue;
                            };
                            (self.update_current_path)(&entry_name).await;
                        }
                        entries = partial_rx.recv(), if !partial_closed => {
                            let Some(entries) = entries else {
                                partial_closed = true;
                                continue;
                            };
                            if !self.should_heal().await {
                                child_ctx.cancel();
                                break;
                            }

                            let discovery = entries.discover_heal_candidates(&bucket, MAX_META_CACHE_HEAL_CANDIDATES);
                            counter!(METRIC_SCANNER_HEAL_DISCOVERY_CANDIDATES_TOTAL)
                                .increment(u64::try_from(discovery.candidates.len()).unwrap_or(u64::MAX));
                            counter!(METRIC_SCANNER_HEAL_DISCOVERY_SUB_QUORUM_TOTAL).increment(
                                u64::try_from(
                                    discovery
                                        .candidates
                                        .iter()
                                        .filter(|candidate| candidate.replica_count < disks_quorum)
                                        .count(),
                                )
                                .unwrap_or(u64::MAX),
                            );
                            counter!(METRIC_SCANNER_HEAL_DISCOVERY_UNVERIFIED_TOTAL).increment(
                                u64::try_from(discovery.unverified_count).unwrap_or(u64::MAX),
                            );
                            if discovery.truncated {
                                counter!(METRIC_SCANNER_HEAL_DISCOVERY_TRUNCATED_TOTAL).increment(1);
                            }

                            for candidate in discovery.candidates {
                                let sub_quorum_candidate = candidate.replica_count < disks_quorum;
                                let version_id = candidate.validated_version().map(|id| id.to_string());
                                let identity = (candidate.object.clone(), version_id.clone(), candidate.kind.clone());
                                if seen_heal_candidates.len() >= MAX_META_CACHE_HEAL_CANDIDATES
                                    && !seen_heal_candidates.contains(&identity)
                                {
                                    continue;
                                }
                                if !seen_heal_candidates.insert(identity) {
                                    continue;
                                }
                                let request = if candidate.is_unversioned() {
                                    build_non_destructive_object_heal_request(
                                        bucket.clone(),
                                        candidate.object.clone(),
                                        self.scan_mode,
                                        HealChannelPriority::High,
                                    )
                                } else {
                                    build_object_heal_request(
                                        bucket.clone(),
                                        candidate.object.clone(),
                                        version_id.clone(),
                                        self.scan_mode,
                                        HealChannelPriority::High,
                                    )
                                };
                                (self.update_current_path)(&candidate.object).await;
                                let admission = self.send_required_scanner_heal_request(
                                    PendingScannerHealKind::Object,
                                    bucket.clone(),
                                    Some(candidate.object.clone()),
                                    version_id.clone(),
                                    request,
                                )
                                .await?;
                                if admission.is_admitted() {
                                    counter!(METRIC_SCANNER_HEAL_DISCOVERY_QUEUED_TOTAL).increment(1);
                                } else if sub_quorum_candidate {
                                    self.mark_pending_scanner_heal_reason(
                                        PendingScannerHealKind::Object,
                                        &bucket,
                                        Some(&candidate.object),
                                        version_id.as_deref(),
                                        "sub_quorum_metadata",
                                    );
                                }
                                found_objects = true;
                            }

                            // Candidates beyond the main cap remain exact
                            // version requests; never downgrade them to a
                            // latest-version (version_id=None) heal.
                            for candidate in discovery.truncated_candidates {
                                let version_id = candidate.validated_version().map(|id| id.to_string());
                                let identity = (candidate.object.clone(), version_id.clone(), candidate.kind.clone());
                                if seen_truncated_objects.len() >= MAX_META_CACHE_HEAL_TRUNCATED_OBJECTS
                                    && !seen_truncated_objects.contains(&candidate.object)
                                {
                                    continue;
                                }
                                seen_truncated_objects.insert(candidate.object.clone());
                                if !seen_heal_candidates.insert(identity) {
                                    continue;
                                }
                                let request = build_object_heal_request(
                                    bucket.clone(),
                                    candidate.object.clone(),
                                    version_id.clone(),
                                    self.scan_mode,
                                    HealChannelPriority::High,
                                );
                                (self.update_current_path)(&candidate.object).await;
                                let admission = self
                                    .send_required_scanner_heal_request(
                                        PendingScannerHealKind::Object,
                                        bucket.clone(),
                                        Some(candidate.object.clone()),
                                        version_id,
                                        request,
                                    )
                                    .await?;
                                if admission.is_admitted() {
                                    counter!(METRIC_SCANNER_HEAL_DISCOVERY_QUEUED_TOTAL).increment(1);
                                }
                                found_objects = true;
                            }


                        }
                        errs = finished_rx.recv(), if !finished_closed => {
                            let Some(errs) = errs else {
                                finished_closed = true;
                                continue;
                            };
                            if disk_errors_are_only_missing_paths(&errs) {
                                debug!(
                                    target: "rustfs::scanner::folder",
                                    event = EVENT_SCANNER_FOLDER_STATE,
                                    component = LOG_COMPONENT_SCANNER,
                                    subsystem = LOG_SUBSYSTEM_FOLDER,
                                    state = "list_path_finished_missing_paths",
                                    errors = ?errs,
                                    "Scanner list_path_raw finished with missing paths"
                                );
                            } else {
                                error!(
                                    target: "rustfs::scanner::folder",
                                    event = EVENT_SCANNER_FOLDER_STATE,
                                    component = LOG_COMPONENT_SCANNER,
                                    subsystem = LOG_SUBSYSTEM_FOLDER,
                                    state = "list_path_finished_with_errors",
                                    errors = ?errs,
                                    "Scanner list_path_raw finished with disk errors"
                                );
                            }
                            child_ctx.cancel();
                        }
                        _ = child_ctx.cancelled() => {
                            break;
                        }
                    }
                }

                if found_objects {
                    let folder_item = CachedFolder {
                        name: name.clone(),
                        parent: Some(this_hash.clone()),
                        object_heal_prob_div: 1,
                    };

                    if into.compacted {
                        // In compacted mode child totals are accumulated directly into the parent entry.
                        let fut = Box::pin(self.scan_folder(ctx.clone(), folder_item.clone(), into));
                        fut.await.map_err(|e| ScannerError::Other(e.to_string()))?;
                        self.send_update_for_entry(&this_hash, &folder.parent, into).await;
                        tokio::task::yield_now().await;
                    } else {
                        let mut dst = DataUsageEntry::default();
                        let h = hash_path(&folder_item.name);

                        // Use Box::pin for recursive async call
                        let fut = Box::pin(self.scan_folder(ctx.clone(), folder_item.clone(), &mut dst));
                        if let Err(e) = fut.await {
                            if ctx.is_cancelled() {
                                self.preserve_partial_child_progress(&folder_item.parent, &h, into, &dst)
                                    .await;
                                return Err(e);
                            }
                            warn!(
                                target: "rustfs::scanner::folder",
                                event = EVENT_SCANNER_FOLDER_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_FOLDER,
                                folder = %folder.name,
                                child_folder = %folder_item.name,
                                state = "heal_child_scan_failed",
                                error = %e,
                                "Scanner heal child folder scan failed"
                            );
                            continue;
                        }
                        tokio::task::yield_now().await;

                        into.add_child(&h);
                        // We scanned a folder, optionally send update.
                        self.update_cache.delete_recursive(&h);
                        self.update_cache.copy_with_children(&self.new_cache, &h, &folder_item.parent);
                        self.send_update().await;
                    }
                }
            }

            break;
        }

        if !was_compacted {
            self.new_cache.replace_hashed(&this_hash, &folder.parent, into);
        }

        if !into.compacted
            && self.new_cache.info.name != folder.name
            && let Some(mut flat) = self.new_cache.size_recursive(&this_hash.key())
        {
            flat.compacted = true;
            let mut should_compact = false;

            if flat.objects < DATA_SCANNER_COMPACT_LEAST_OBJECT {
                should_compact = true;
            } else {
                // Compact if we only have objects as children...
                should_compact = true;
                for k in &into.children {
                    if let Some(v) = self.new_cache.cache.get(k)
                        && (!v.children.is_empty() || v.objects > 1)
                    {
                        should_compact = false;
                        break;
                    }
                }
            }

            if should_compact {
                self.new_cache.delete_recursive(&this_hash);
                self.new_cache.replace_hashed(&this_hash, &folder.parent, &flat);
            }
        }

        // Compact if too many children...
        if !into.compacted {
            let done_compact = Metrics::time(Metric::CompactFolder);
            self.new_cache.reduce_children_of(
                &this_hash,
                DATA_SCANNER_COMPACT_AT_CHILDREN,
                self.new_cache.info.name != folder.name,
            );
            done_compact();
        }

        if self.update_cache.cache.contains_key(&this_hash.key()) && !was_compacted {
            // Replace if existed before.
            if let Some(flat) = self.new_cache.size_recursive(&this_hash.key()) {
                self.update_cache.delete_recursive(&this_hash);
                self.update_cache.replace_hashed(&this_hash, &folder.parent, &flat);
            }
        }

        self.finish_size_reconciliation_batch();
        done_folder();
        let scanned_objects = u64::try_from(into.objects).unwrap_or(u64::MAX);
        emit_scanner_folder_trace(&self.root, &folder.name, scanned_objects, trace_started_at, "completed");

        Ok(())
    }

    pub fn as_mut_new_cache(&mut self) -> &mut DataUsageCache {
        &mut self.new_cache
    }
}

/// Scan a data folder
/// This function scans the basepath+cache.info.name and returns an updated cache.
/// The returned cache will always be valid, but may not be updated from the existing.
/// Throttling between operations is controlled by the provided [`DynamicSleeper`].
/// If the supplied context is canceled the function will return at the first chance.
#[allow(clippy::too_many_arguments)]
pub async fn scan_data_folder(
    ctx: CancellationToken,
    budget: Arc<ScannerCycleBudget>,
    disks: Vec<Arc<Disk>>,
    local_disk: Arc<Disk>,
    cache: DataUsageCache,
    updates: Option<mpsc::Sender<DataUsageEntry>>,
    scan_mode: HealScanMode,
    sleeper: DynamicSleeper,
) -> Result<DataUsageCache, ScannerError> {
    use crate::data_usage_define::DATA_USAGE_ROOT;

    // Check that we're not trying to scan the root
    if cache.info.name.is_empty() || cache.info.name == DATA_USAGE_ROOT {
        return Err(ScannerError::Other("internal error: root scan attempted".to_string()));
    }

    // Get disk path
    let base_path = local_disk.path().to_string_lossy().to_string();

    let (update_current_path, close_disk) = current_path_updater(&base_path, &cache.info.name).await;
    let mut close_disk_guard = CloseDiskGuard::new(close_disk);

    // Create skip_heal flag
    let is_erasure_mode = scanner_is_erasure().await;
    let skip_heal = Arc::new(std::sync::atomic::AtomicBool::new(!is_erasure_mode || cache.info.skip_healing));

    // Create heal_object_select flag
    let heal_object_select = if is_erasure_mode && !cache.info.skip_healing {
        heal_object_select_prob()
    } else {
        0
    };

    let disks_quorum = disks.len() / 2;

    let failed_object_ttl = rustfs_utils::get_env_u32(ENV_FAILED_OBJECT_TTL_SECS, DEFAULT_FAILED_OBJECT_TTL_SECS) as u64;
    let failed_objects_max = rustfs_utils::get_env_u32(ENV_FAILED_OBJECTS_MAX, DEFAULT_FAILED_OBJECTS_MAX) as usize;

    // Create folder scanner
    let mut scanner = FolderScanner {
        root: base_path,
        old_cache: cache.clone(),
        new_cache: DataUsageCache {
            info: cache.info.clone(),
            ..Default::default()
        },
        update_cache: DataUsageCache {
            info: cache.info.clone(),
            ..Default::default()
        },
        data_usage_scanner_debug: false,
        heal_object_select,
        scan_mode,
        is_erasure_mode,
        failed_object_ttl_secs: failed_object_ttl,
        failed_objects_max,
        sleeper,
        disks,
        disks_quorum,
        updates,
        last_update: SystemTime::UNIX_EPOCH,
        update_current_path,
        budget: budget.clone(),
        skip_heal,
        local_disk,
        pending_heals_changed: false,
        pending_size_reconciliation_keys: HashSet::new(),
        pending_size_reconciliation_scopes: HashSet::new(),
        pending_size_reconciliation_truncated: false,
        #[cfg(test)]
        list_path_raw_options_observer: None,
    };

    let now = FolderScanner::now_secs();
    prune_size_reconciliation(&mut scanner.new_cache.info, now);
    prune_size_reconciliation(&mut scanner.update_cache.info, now);

    // Check if context is cancelled
    if ctx.is_cancelled() {
        return Err(ScannerError::Other("Operation cancelled".to_string()));
    }

    scanner.retry_pending_scanner_heals().await?;

    // Read top level in bucket
    let mut root = DataUsageEntry::default();
    let folder = CachedFolder {
        name: cache.info.name.clone(),
        parent: None,
        object_heal_prob_div: 1,
    };

    // Scan the folder
    match scanner.scan_folder(ctx.clone(), folder, &mut root).await {
        Ok(()) => {
            // Get the new cache and finalize it
            let new_cache = scanner.as_mut_new_cache();
            new_cache.force_compact(DATA_SCANNER_COMPACT_AT_CHILDREN);
            new_cache.info.last_update = Some(SystemTime::now());
            new_cache.info.next_cycle = cache.info.next_cycle;
            let unresolved_objects = root.failed_objects > 0
                || !new_cache.info.failed_objects.is_empty()
                || !new_cache.info.size_reconciliation.is_empty();
            new_cache.info.snapshot_complete = !unresolved_objects;
            let had_scan_checkpoint = cache.info.scan_checkpoint.is_some() || new_cache.info.scan_checkpoint.is_some();
            new_cache.info.scan_resume_after = None;
            new_cache.info.scan_checkpoint = None;
            if had_scan_checkpoint {
                global_metrics().record_scanner_checkpoint_cleared();
            }

            close_disk_guard.close().await;
            if unresolved_objects {
                Err(ScannerError::PartialCache(Box::new(new_cache.clone())))
            } else {
                Ok(new_cache.clone())
            }
        }
        Err(e) => {
            if ctx.is_cancelled() {
                let root_hash = hash_path(&cache.info.name);
                let root_has_progress = data_usage_root_has_progress(&root);
                let pending_heals_changed = scanner.pending_heals_changed;
                if root_has_progress {
                    scanner.carry_forward_old_children(&root_hash, &mut root);
                }
                let new_cache = scanner.as_mut_new_cache();
                if root_has_progress {
                    new_cache.replace_hashed(&root_hash, &None, &root);
                }
                if partial_cache_is_useful(&root, pending_heals_changed) || !new_cache.info.size_reconciliation.is_empty() {
                    if new_cache.root().is_some() {
                        new_cache.force_compact(DATA_SCANNER_COMPACT_AT_CHILDREN);
                    }
                    new_cache.info.last_update = Some(SystemTime::now());
                    new_cache.info.next_cycle = cache.info.next_cycle;
                    new_cache.info.snapshot_complete = false;
                    if root_has_progress {
                        set_scan_checkpoint(new_cache, checkpoint_reason_from_budget(budget.reason()));
                    }
                    close_disk_guard.close().await;
                    return Err(ScannerError::PartialCache(Box::new(new_cache.clone())));
                }
            }
            if matches!(&e, ScannerError::Io(io) if io.kind() == ErrorKind::NotFound) {
                let mut partial_cache = scanner.old_cache.clone();
                partial_cache.info.last_update = Some(SystemTime::now());
                partial_cache.info.next_cycle = cache.info.next_cycle;
                partial_cache.info.snapshot_complete = false;
                close_disk_guard.close().await;
                return Err(ScannerError::NamespaceNotFoundCache(Box::new(partial_cache)));
            }
            close_disk_guard.close().await;
            // No useful information, return original cache
            Err(e)
        }
    }
}

mod item_actions;
mod ledger;

use item_actions::*;
pub use item_actions::{GetSizeFn, ScannerItem};
use ledger::*;

#[cfg(test)]
mod tests;
