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

use std::collections::HashSet;
use std::fs::FileType;
use std::io::ErrorKind;
use std::sync::{Arc, Once};
use std::time::{Duration, Instant, SystemTime};

use crate::ReplTargetSizeSummary;
use crate::data_usage_define::{
    DATA_USAGE_SCAN_CHECKPOINT_VERSION, DataUsageCache, DataUsageEntry, DataUsageHash, DataUsageHashMap, DataUsageScanCheckpoint,
    DataUsageScanCheckpointReason, SizeSummary, hash_path,
};
use crate::error::ScannerError;
use crate::runtime_config::{
    scanner_alert_excess_folders, scanner_alert_excess_version_size, scanner_alert_excess_versions, scanner_yield_every_n_objects,
};
use crate::scanner_budget::{ScannerCycleBudget, ScannerCycleBudgetReason};
use crate::scanner_io::{SCANNER_SKIP_FILE_ERROR, ScannerIODisk as _};
use crate::sleeper::DynamicSleeper;
use metrics::{counter, describe_counter};
use rustfs_common::heal_channel::{
    HEAL_DELETE_DANGLING, HealAdmissionResult, HealChannelPriority, HealChannelRequest, HealRequestSource, HealScanMode,
    send_heal_request_with_admission,
};
use rustfs_common::metrics::{
    IlmAction, Metric, Metrics, ScannerReplicationRepairKind, ScannerSourceWorkUpdate, ScannerWorkSource, UpdateCurrentPathFn,
    current_path_updater, global_metrics,
};
use rustfs_filemeta::{
    MetaCacheEntries, MetaCacheEntry, MetadataResolutionParams, ReplicateObjectInfo, ReplicationStatusType, ReplicationType,
};
use rustfs_utils::path::{SLASH_SEPARATOR, path_join_buf};
use s3s::dto::{BucketLifecycleConfiguration, ObjectLockConfiguration};
use time::OffsetDateTime;
use tokio::select;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::{
    BucketVersioningSys, Disk, DiskError, DiskInfoOptions, Evaluator, Event, LcEventSrc, ListPathRawOptions, ObjectOpts,
    ReplicationConfig, ReplicationQueueAdmission, ScannerDiskExt as _, ScannerLifecycleConfigExt as _,
    ScannerReplicationConfigExt as _, ScannerVersioningConfigExt as _, StorageError, apply_expiry_rule, apply_transition_rule,
    enqueue_global_newer_noncurrent, is_erasure, is_reserved_or_invalid_bucket, list_path_raw, path2_bucket_object,
    path2_bucket_object_with_base_path, queue_replication_heal_internal,
};
use crate::{ScannerObjectInfo as ObjectInfo, ScannerObjectToDelete as ObjectToDelete};

const LOG_COMPONENT_SCANNER: &str = "scanner";
const LOG_SUBSYSTEM_FOLDER: &str = "folder";
const LOG_SUBSYSTEM_LIFECYCLE: &str = "lifecycle";
const LOG_SUBSYSTEM_HEAL: &str = "heal";
const EVENT_SCANNER_FOLDER_STATE: &str = "scanner_folder_state";
const EVENT_SCANNER_LIFECYCLE_ACTION: &str = "scanner_lifecycle_action";
const EVENT_SCANNER_HEAL_ADMISSION: &str = "scanner_heal_admission";
const EVENT_SCANNER_ALERT_STATE: &str = "scanner_alert_state";
const EVENT_SCANNER_COMPAT_STATE: &str = "scanner_compat_state";

const DATA_USAGE_UPDATE_DIR_CYCLES: u32 = 16;
const DATA_SCANNER_COMPACT_LEAST_OBJECT: usize = 500;
const DATA_SCANNER_COMPACT_AT_CHILDREN: usize = 10000;
const DATA_SCANNER_COMPACT_AT_FOLDERS: usize = DATA_SCANNER_COMPACT_AT_CHILDREN / 4;
const DATA_SCANNER_FORCE_COMPACT_AT_FOLDERS: usize = 250_000;
const DEFAULT_HEAL_OBJECT_SELECT_PROB: u32 = 1024;
const ENV_DATA_USAGE_UPDATE_DIR_CYCLES: &str = "RUSTFS_DATA_USAGE_UPDATE_DIR_CYCLES";
const ENV_HEAL_OBJECT_SELECT_PROB: &str = "RUSTFS_HEAL_OBJECT_SELECT_PROB";
const ENV_SCANNER_DEEP_VERIFY_COOLDOWN_SECS: &str = "RUSTFS_SCANNER_DEEP_VERIFY_COOLDOWN_SECS";
const ENV_FAILED_OBJECT_TTL_SECS: &str = "RUSTFS_DATA_USAGE_FAILED_OBJECT_TTL_SECS";
const ENV_FAILED_OBJECTS_MAX: &str = "RUSTFS_DATA_USAGE_FAILED_OBJECTS_MAX";
const DEFAULT_FAILED_OBJECT_TTL_SECS: u32 = 86_400;
const DEFAULT_FAILED_OBJECTS_MAX: u32 = 10_000;
const DEFAULT_SCANNER_DEEP_VERIFY_COOLDOWN_SECS: u64 = 60;
const METRIC_SCANNER_INLINE_HEAL_TOTAL: &str = "rustfs_scanner_inline_heal_total";
const METRIC_SCANNER_EXCESS_OBJECT_VERSIONS_TOTAL: &str = "rustfs_scanner_excess_object_versions_total";
const METRIC_SCANNER_EXCESS_OBJECT_VERSION_SIZE_TOTAL: &str = "rustfs_scanner_excess_object_version_size_total";
const METRIC_SCANNER_EXCESS_FOLDERS_TOTAL: &str = "rustfs_scanner_excess_folders_total";

static SCANNER_INLINE_HEAL_WARN_ONCE: Once = Once::new();
static SCANNER_INLINE_HEAL_METRICS_ONCE: Once = Once::new();
static SCANNER_ALERT_METRICS_ONCE: Once = Once::new();

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

fn scanner_inline_heal_enabled() -> bool {
    scanner_inline_heal_enabled_from_value(std::env::var(rustfs_config::ENV_SCANNER_INLINE_HEAL_ENABLE).ok().as_deref())
}

fn scanner_inline_heal_enabled_from_value(value: Option<&str>) -> bool {
    match value {
        Some(value) => matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "on" | "yes"),
        None => rustfs_config::DEFAULT_SCANNER_INLINE_HEAL_ENABLE,
    }
}

fn ensure_scanner_inline_heal_metric_registered() {
    SCANNER_INLINE_HEAL_METRICS_ONCE.call_once(|| {
        describe_counter!(
            METRIC_SCANNER_INLINE_HEAL_TOTAL,
            "Total number of inline heal operations executed directly by scanner."
        );
        counter!(METRIC_SCANNER_INLINE_HEAL_TOTAL).increment(0);
    });
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

fn scanner_replication_repair_kind(roi: &ReplicateObjectInfo) -> Option<ScannerReplicationRepairKind> {
    if roi.bucket.is_empty() && roi.name.is_empty() {
        return None;
    }

    if roi.op_type == ReplicationType::ExistingObject || roi.existing_obj_resync.must_resync() {
        Some(ScannerReplicationRepairKind::BucketExistingObject)
    } else if !roi.version_purge_status.is_empty() {
        Some(ScannerReplicationRepairKind::BucketVersionPurge)
    } else if roi.delete_marker {
        Some(ScannerReplicationRepairKind::BucketDeleteMarker)
    } else {
        Some(ScannerReplicationRepairKind::BucketObject)
    }
}

fn record_scanner_replication_admission(metrics: &Metrics, roi: &ReplicateObjectInfo, admission: ReplicationQueueAdmission) {
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
        *cumulative_size += size;
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

fn warn_inline_heal_compat_requested() {
    if !scanner_inline_heal_enabled() {
        return;
    }

    SCANNER_INLINE_HEAL_WARN_ONCE.call_once(|| {
        warn!(
            target: "rustfs::scanner::folder",
            event = EVENT_SCANNER_COMPAT_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_HEAL,
            env = rustfs_config::ENV_SCANNER_INLINE_HEAL_ENABLE,
            state = "inline_heal_rollback_unsupported",
            "Scanner inline-heal rollback is unsupported; using async heal admission"
        );
    });
}

fn non_negative_i64_to_u64(value: i64) -> u64 {
    value.max(0) as u64
}

fn apply_scanner_size_summary(into: &mut DataUsageEntry, summary: &SizeSummary) {
    into.size += summary.total_size;
    into.versions += summary.versions;
    into.delete_markers += summary.delete_markers;
    into.obj_sizes.add(summary.total_size as u64);
    into.obj_versions.add(summary.versions as u64);

    let replication_stats = into.replication_stats.get_or_insert_with(Default::default);
    replication_stats.replica_size += non_negative_i64_to_u64(summary.replica_size);
    replication_stats.replica_count += summary.replica_count as u64;

    for (arn, st) in &summary.repl_target_stats {
        let tgt_stat = replication_stats.targets.entry(arn.clone()).or_default();
        tgt_stat.pending_size += non_negative_i64_to_u64(st.pending_size);
        tgt_stat.failed_size += non_negative_i64_to_u64(st.failed_size);
        tgt_stat.replicated_size += non_negative_i64_to_u64(st.replicated_size);
        tgt_stat.replicated_count += st.replicated_count as u64;
        tgt_stat.failed_count += st.failed_count as u64;
        tgt_stat.pending_count += st.pending_count as u64;
    }
}

/// Cached folder information for scanning
#[derive(Clone, Debug)]
pub struct CachedFolder {
    pub name: String,
    pub parent: Option<DataUsageHash>,
    pub object_heal_prob_div: u32,
}

/// Type alias for get size function
pub type GetSizeFn = Box<dyn Fn(ScannerItem) -> Result<SizeSummary, StorageError> + Send + Sync>;

fn build_bucket_heal_request(bucket: String, priority: HealChannelPriority) -> HealChannelRequest {
    HealChannelRequest {
        bucket,
        priority,
        source: HealRequestSource::Scanner,
        ..Default::default()
    }
}

fn build_object_heal_request(
    bucket: String,
    object: String,
    version_id: Option<String>,
    scan_mode: HealScanMode,
    priority: HealChannelPriority,
) -> HealChannelRequest {
    HealChannelRequest {
        bucket,
        object_prefix: Some(object),
        object_version_id: version_id,
        priority,
        scan_mode: Some(scan_mode),
        remove_corrupted: Some(HEAL_DELETE_DANGLING),
        source: HealRequestSource::Scanner,
        ..Default::default()
    }
}

fn resolve_object_heal_entry(entries: &MetaCacheEntries, resolver: MetadataResolutionParams) -> Option<MetaCacheEntry> {
    if let Some(entry) = entries.resolve(resolver) {
        return Some(entry);
    }

    let (entry, _) = entries.first_found();
    entry.filter(|entry| !entry.name.ends_with(SLASH_SEPARATOR))
}

fn heal_priority_label(priority: HealChannelPriority) -> &'static str {
    match priority {
        HealChannelPriority::Low => "low",
        HealChannelPriority::Normal => "normal",
        HealChannelPriority::High => "high",
        HealChannelPriority::Critical => "critical",
    }
}

fn describe_heal_admission(result: HealAdmissionResult) -> String {
    match result {
        HealAdmissionResult::Accepted | HealAdmissionResult::Merged => result.result_label().to_string(),
        HealAdmissionResult::Full => "queue_full".to_string(),
        HealAdmissionResult::Dropped(reason) => format!("dropped:{}", reason.as_str()),
    }
}

fn record_high_priority_heal_escalation(
    candidate_type: &'static str,
    priority: HealChannelPriority,
    result: HealAdmissionResult,
) {
    counter!(
        "rustfs_heal_candidate_priority_reject_total",
        "type" => candidate_type.to_string(),
        "priority" => heal_priority_label(priority).to_string(),
        "result" => result.result_label().to_string(),
        "reason" => result.reason_label().to_string()
    )
    .increment(1);
}

fn build_high_priority_heal_admission_error(
    candidate_type: &'static str,
    bucket: &str,
    object: Option<&str>,
    priority: HealChannelPriority,
    result: HealAdmissionResult,
) -> ScannerError {
    let object_text = object.map(|object| format!(", object='{object}'")).unwrap_or_default();
    ScannerError::Other(format!(
        "high-priority heal request was not admitted: type={candidate_type}, bucket='{bucket}'{object_text}, priority={}, admission={}",
        heal_priority_label(priority),
        describe_heal_admission(result)
    ))
}

fn record_heal_candidate_admission(candidate_type: &'static str, priority: HealChannelPriority, result: HealAdmissionResult) {
    counter!(
        "rustfs_heal_candidate_enqueue_total",
        "type" => candidate_type.to_string(),
        "priority" => heal_priority_label(priority).to_string(),
        "result" => result.result_label().to_string()
    )
    .increment(1);

    if matches!(result, HealAdmissionResult::Merged) {
        counter!(
            "rustfs_heal_candidate_merge_total",
            "type" => candidate_type.to_string()
        )
        .increment(1);
    }

    if let HealAdmissionResult::Dropped(reason) = result {
        counter!(
            "rustfs_heal_candidate_drop_total",
            "type" => candidate_type.to_string(),
            "reason" => reason.as_str().to_string()
        )
        .increment(1);
    }
}

async fn send_scanner_heal_request(
    candidate_type: &'static str,
    request: HealChannelRequest,
) -> Result<HealAdmissionResult, ScannerError> {
    let priority = request.priority;
    match send_heal_request_with_admission(request).await {
        Ok(result) => {
            record_heal_candidate_admission(candidate_type, priority, result);
            Ok(result)
        }
        Err(err) => {
            counter!(
                "rustfs_heal_candidate_enqueue_total",
                "type" => candidate_type.to_string(),
                "priority" => heal_priority_label(priority).to_string(),
                "result" => "channel_error".to_string()
            )
            .increment(1);
            Err(ScannerError::Other(err))
        }
    }
}

async fn send_required_scanner_heal_request(
    candidate_type: &'static str,
    bucket: &str,
    object: Option<&str>,
    request: HealChannelRequest,
) -> Result<(), ScannerError> {
    let priority = request.priority;
    let result = send_scanner_heal_request(candidate_type, request).await?;
    if result.is_admitted() {
        return Ok(());
    }

    record_high_priority_heal_escalation(candidate_type, priority, result);
    error!(
        target: "rustfs::scanner::folder",
        event = EVENT_SCANNER_HEAL_ADMISSION,
        component = LOG_COMPONENT_SCANNER,
        subsystem = LOG_SUBSYSTEM_HEAL,
        candidate_type,
        bucket,
        object = object.unwrap_or(""),
        priority = heal_priority_label(priority),
        admission = result.result_label(),
        reason = result.reason_label(),
        state = "high_priority_not_admitted",
        "Scanner high-priority heal admission failed"
    );
    Err(build_high_priority_heal_admission_error(candidate_type, bucket, object, priority, result))
}

/// Scanner item representing a file during scanning
#[derive(Clone, Debug)]
pub struct ScannerItem {
    pub path: String,
    pub bucket: String,
    pub prefix: String,
    pub object_name: String,
    pub file_type: FileType,
    pub lifecycle: Option<Arc<BucketLifecycleConfiguration>>,
    pub replication: Option<Arc<ReplicationConfig>>,
    pub heal_enabled: bool,
    pub heal_bitrot: bool,
    pub debug: bool,
}

impl ScannerItem {
    /// Get the object path (prefix + object_name)
    pub fn object_path(&self) -> String {
        if self.prefix.is_empty() {
            self.object_name.clone()
        } else {
            path_join_buf(&[&self.prefix, &self.object_name])
        }
    }

    /// Transform meta directory by splitting prefix and extracting object name
    /// This converts a directory path like "bucket/dir1/dir2/file" to prefix="bucket/dir1/dir2" and object_name="file"
    pub fn transform_meta_dir(&mut self) {
        let prefix = self.prefix.clone(); // Clone to avoid borrow checker issues
        let split: Vec<&str> = prefix.split(SLASH_SEPARATOR).collect();

        if split.len() > 1 {
            let prefix_parts: Vec<&str> = split[..split.len() - 1].to_vec();
            self.prefix = path_join_buf(&prefix_parts);
        } else {
            self.prefix = String::new();
        }

        // Object name is the last element
        self.object_name = split.last().unwrap_or(&"").to_string();
    }

    pub async fn apply_actions(
        &mut self,
        object_infos: Vec<ObjectInfo>,
        lock_retention: Option<Arc<ObjectLockConfiguration>>,
        size_summary: &mut SizeSummary,
    ) {
        if object_infos.is_empty() {
            debug!(
                target: "rustfs::scanner::folder",
                event = EVENT_SCANNER_LIFECYCLE_ACTION,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                object_path = %self.object_path(),
                state = "no_object_versions",
                "Scanner lifecycle action skipped"
            );
            return;
        }
        debug!(
            target: "rustfs::scanner::folder",
            event = EVENT_SCANNER_LIFECYCLE_ACTION,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
            object_path = %self.object_path(),
            state = "started",
            "Scanner lifecycle evaluation started"
        );

        let versioning_config = match BucketVersioningSys::get(&self.bucket).await {
            Ok(versioning_config) => versioning_config,
            Err(_) => {
                warn!(
                    target: "rustfs::scanner::folder",
                    event = EVENT_SCANNER_LIFECYCLE_ACTION,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    bucket = %self.bucket,
                    state = "versioning_lookup_failed",
                    "Scanner lifecycle action skipped"
                );
                return;
            }
        };

        let Some(lifecycle) = self.lifecycle.as_ref() else {
            let mut cumulative_size = 0;
            for oi in object_infos.iter() {
                let actual_size = match oi.get_actual_size() {
                    Ok(size) => size,
                    Err(_) => {
                        warn!(
                            target: "rustfs::scanner::folder",
                            event = EVENT_SCANNER_LIFECYCLE_ACTION,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                            bucket = %self.bucket,
                            object = %oi.name,
                            state = "size_lookup_failed",
                            "Scanner lifecycle action used fallback size"
                        );
                        continue;
                    }
                };

                let size = self.heal_actions(oi, actual_size, size_summary).await;

                size_summary.actions_accounting(oi, size, actual_size);

                cumulative_size += size;
            }

            self.alert_excessive_versions(object_infos.len(), cumulative_size);

            debug!(
                target: "rustfs::scanner::folder",
                event = EVENT_SCANNER_LIFECYCLE_ACTION,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                object_path = %self.object_path(),
                state = "no_lifecycle_config",
                "Scanner lifecycle action finished without lifecycle rules"
            );
            return;
        };

        let object_opts = object_infos
            .iter()
            .map(ObjectOpts::from_object_info)
            .collect::<Vec<ObjectOpts>>();

        let events = match Evaluator::new(lifecycle.clone())
            .with_lock_retention(lock_retention)
            .with_replication_config(self.replication.clone())
            .eval(&object_opts)
            .await
        {
            Ok(events) => events,
            Err(e) => {
                warn!(
                    target: "rustfs::scanner::folder",
                    event = EVENT_SCANNER_LIFECYCLE_ACTION,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    object_path = %self.object_path(),
                    state = "evaluate_failed",
                    error = %e,
                    "Scanner lifecycle action evaluation failed"
                );
                return;
            }
        };
        let mut to_delete_objs: Vec<ObjectToDelete> = Vec::new();
        let mut noncurrent_events: Vec<Event> = Vec::new();
        let mut noncurrent_accounting: Vec<PendingScannerAccounting<'_>> = Vec::new();
        let mut cumulative_size = 0;
        let mut remaining_versions = object_infos.len();
        'eventLoop: {
            for (i, event) in events.iter().enumerate() {
                let oi = &object_infos[i];
                let actual_size = match oi.get_actual_size() {
                    Ok(size) => size,
                    Err(_) => {
                        warn!(
                            target: "rustfs::scanner::folder",
                            event = EVENT_SCANNER_LIFECYCLE_ACTION,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                            bucket = %self.bucket,
                            object = %oi.name,
                            state = "size_lookup_failed",
                            "Scanner lifecycle action used fallback size"
                        );
                        0
                    }
                };

                let mut size = actual_size;
                let mut account_now = true;

                match event.action {
                    IlmAction::DeleteAllVersionsAction | IlmAction::DelMarkerDeleteAllVersionsAction => {
                        debug!(
                            target: "rustfs::scanner::folder",
                            event = EVENT_SCANNER_LIFECYCLE_ACTION,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                            bucket = %self.bucket,
                            object = %oi.name,
                            action = %event.action,
                            state = "apply_expiry_rule",
                            "Scanner lifecycle action dispatched"
                        );
                        let done_ilm = Metrics::time_ilm(event.action);
                        let queued = apply_expiry_rule(event, &LcEventSrc::Scanner, oi).await;
                        if record_scanner_ilm_action_if_queued(global_metrics(), event.action, 1, queued) {
                            done_ilm(1)();
                            remaining_versions = 0;
                        } else {
                            PendingScannerAccounting {
                                object: oi,
                                retained_size: actual_size,
                                expired_size: 0,
                            }
                            .apply(size_summary, &mut cumulative_size, false);
                            for retained in object_infos.iter().skip(i + 1) {
                                let retained_size = match retained.get_actual_size() {
                                    Ok(size) => size,
                                    Err(_) => {
                                        warn!(
                                            target: "rustfs::scanner::folder",
                                            event = EVENT_SCANNER_LIFECYCLE_ACTION,
                                            component = LOG_COMPONENT_SCANNER,
                                            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                                            bucket = %self.bucket,
                                            object = %retained.name,
                                            state = "size_lookup_failed",
                                            "Scanner lifecycle action used fallback size"
                                        );
                                        0
                                    }
                                };
                                PendingScannerAccounting {
                                    object: retained,
                                    retained_size,
                                    expired_size: 0,
                                }
                                .apply(size_summary, &mut cumulative_size, false);
                            }
                        }
                        break 'eventLoop;
                    }

                    IlmAction::DeleteAction | IlmAction::DeleteRestoredAction | IlmAction::DeleteRestoredVersionAction => {
                        debug!(
                            target: "rustfs::scanner::folder",
                            event = EVENT_SCANNER_LIFECYCLE_ACTION,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                            bucket = %self.bucket,
                            object = %oi.name,
                            action = %event.action,
                            state = "apply_expiry_rule",
                            "Scanner lifecycle action dispatched"
                        );
                        let done_ilm = Metrics::time_ilm(event.action);
                        let queued = apply_expiry_rule(event, &LcEventSrc::Scanner, oi).await;
                        if record_scanner_ilm_action_if_queued(global_metrics(), event.action, 1, queued) {
                            done_ilm(1)();
                            if !versioning_config.prefix_enabled(&self.object_path()) && event.action == IlmAction::DeleteAction {
                                remaining_versions -= 1;
                                size = 0;
                            }
                        }
                    }
                    IlmAction::DeleteVersionAction => {
                        if let Some(opt) = object_opts.get(i) {
                            to_delete_objs.push(ObjectToDelete {
                                object_name: opt.name.clone(),
                                version_id: opt.version_id,
                                ..Default::default()
                            });
                            noncurrent_accounting.push(PendingScannerAccounting {
                                object: oi,
                                retained_size: actual_size,
                                expired_size: 0,
                            });
                            account_now = false;
                        }
                        noncurrent_events.push(event.clone());
                    }
                    IlmAction::TransitionAction | IlmAction::TransitionVersionAction => {
                        debug!(
                            target: "rustfs::scanner::folder",
                            event = EVENT_SCANNER_LIFECYCLE_ACTION,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                            bucket = %self.bucket,
                            object = %oi.name,
                            action = %event.action,
                            state = "apply_transition_rule",
                            "Scanner lifecycle action dispatched"
                        );
                        let done_ilm = Metrics::time_ilm(event.action);
                        let queued = apply_transition_rule(event, &LcEventSrc::Scanner, oi).await;
                        if record_scanner_ilm_action_if_queued(global_metrics(), event.action, 1, queued) {
                            done_ilm(1)();
                        }
                    }

                    IlmAction::NoneAction | IlmAction::ActionCount => {
                        size = self.heal_actions(oi, actual_size, size_summary).await;
                    }
                }

                if account_now {
                    size_summary.actions_accounting(oi, size, actual_size);
                    cumulative_size += size;
                }
            }
        }

        if !to_delete_objs.is_empty()
            && let Some(event) = noncurrent_events.first().cloned()
        {
            let action = event.action;
            let count = u64::try_from(to_delete_objs.len()).unwrap_or(u64::MAX);
            let done_ilm = Metrics::time_ilm(action);
            let queued = enqueue_global_newer_noncurrent(&self.bucket, to_delete_objs, event, &LcEventSrc::Scanner).await;
            if record_scanner_ilm_action_if_queued(global_metrics(), action, count, queued) {
                done_ilm(count)();
                remaining_versions = remaining_versions.saturating_sub(noncurrent_accounting.len());
            }
            for pending in noncurrent_accounting {
                pending.apply(size_summary, &mut cumulative_size, queued);
            }
        }
        self.alert_excessive_versions(remaining_versions, cumulative_size);
    }

    async fn heal_actions(&mut self, oi: &ObjectInfo, actual_size: i64, size_summary: &mut SizeSummary) -> i64 {
        if self.heal_enabled {
            warn_inline_heal_compat_requested();
            self.enqueue_heal(oi).await;
        }

        self.heal_replication(oi, size_summary).await;

        actual_size
    }

    async fn heal_replication(&mut self, oi: &ObjectInfo, size_summary: &mut SizeSummary) {
        if oi.version_id.is_none_or(|v| v.is_nil()) {
            return;
        }

        let Some(replication) = self.replication.clone() else {
            return;
        };

        let done_replication = Metrics::time(Metric::CheckReplication);
        let replication_result = queue_replication_heal_internal(&oi.bucket, oi.clone(), (*replication).clone(), 0).await;
        done_replication();
        let roi = replication_result.object_info;
        record_scanner_replication_admission(global_metrics(), &roi, replication_result.admission);
        if !Self::should_account_replication_stats(oi) {
            return;
        }

        for (arn, target_status) in roi.target_statuses.iter() {
            if !size_summary.repl_target_stats.contains_key(arn.as_str()) {
                size_summary
                    .repl_target_stats
                    .insert(arn.clone(), ReplTargetSizeSummary::default());
            }

            if let Some(repl_target_size_summary) = size_summary.repl_target_stats.get_mut(arn.as_str()) {
                match target_status {
                    ReplicationStatusType::Pending => {
                        repl_target_size_summary.pending_size += roi.size;
                        repl_target_size_summary.pending_count += 1;
                        size_summary.pending_size += roi.size;
                        size_summary.pending_count += 1;
                    }
                    ReplicationStatusType::Failed => {
                        repl_target_size_summary.failed_size += roi.size;
                        repl_target_size_summary.failed_count += 1;
                        size_summary.failed_size += roi.size;
                        size_summary.failed_count += 1;
                    }
                    ReplicationStatusType::Completed | ReplicationStatusType::CompletedLegacy => {
                        repl_target_size_summary.replicated_size += roi.size;
                        repl_target_size_summary.replicated_count += 1;
                        size_summary.replicated_size += roi.size;
                        size_summary.replicated_count += 1;
                    }
                    _ => {}
                }
            }
        }

        if oi.replication_status == ReplicationStatusType::Replica {
            size_summary.replica_size += roi.size;
            size_summary.replica_count += 1;
        }
    }

    fn should_account_replication_stats(oi: &ObjectInfo) -> bool {
        !oi.delete_marker && oi.version_purge_status.is_empty()
    }

    async fn enqueue_heal(&mut self, oi: &ObjectInfo) {
        let done_heal = Metrics::time(Metric::HealAbandonedObject);
        debug!(
            target: "rustfs::scanner::folder",
            event = EVENT_SCANNER_HEAL_ADMISSION,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_HEAL,
            bucket = %self.bucket,
            object = %self.object_path(),
            version_id = %oi.version_id.unwrap_or_default(),
            state = "request_started",
            "Scanner heal admission started"
        );

        let now = OffsetDateTime::now_utc();
        let scan_mode = effective_object_heal_scan_mode(self.heal_bitrot, oi.mod_time, now);
        if self.heal_bitrot && scan_mode != HealScanMode::Deep {
            let cooldown = deep_verify_cooldown();
            let age_secs = oi.mod_time.map(|mod_time| {
                let age = now - mod_time;
                age.whole_seconds().max(0)
            });
            debug!(
                target: "rustfs::scanner::folder",
                event = EVENT_SCANNER_HEAL_ADMISSION,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_HEAL,
                bucket = %self.bucket,
                object = %self.object_path(),
                version_id = %oi.version_id.unwrap_or_default(),
                object_age_secs = age_secs.unwrap_or_default(),
                cooldown_secs = cooldown.as_secs(),
                original_scan_mode = %HealScanMode::Deep.as_str(),
                effective_scan_mode = %scan_mode.as_str(),
                state = "downgraded_to_normal",
                "Scanner heal deep scan downgraded"
            );
        }

        let result = send_scanner_heal_request(
            "object",
            build_object_heal_request(
                self.bucket.clone(),
                self.object_path(),
                oi.version_id
                    .and_then(|v| if v.is_nil() { None } else { Some(v.to_string()) }),
                scan_mode,
                HealChannelPriority::Low,
            ),
        )
        .await;

        let admission = result.as_ref().copied().map_err(|_| ());
        let admitted = record_scanner_heal_admission(global_metrics(), scan_mode, admission);
        match result {
            Ok(HealAdmissionResult::Accepted | HealAdmissionResult::Merged) => {}
            Ok(result @ (HealAdmissionResult::Full | HealAdmissionResult::Dropped(_))) => {
                warn!(
                    target: "rustfs::scanner::folder",
                    event = EVENT_SCANNER_HEAL_ADMISSION,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_HEAL,
                    bucket = %self.bucket,
                    object = %self.object_path(),
                    admission = %describe_heal_admission(result),
                    state = "not_admitted",
                    "Scanner heal admission rejected low-priority request"
                );
            }
            Err(e) => warn!(
                target: "rustfs::scanner::folder",
                event = EVENT_SCANNER_HEAL_ADMISSION,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_HEAL,
                bucket = %self.bucket,
                object = %self.object_path(),
                state = "submit_failed",
                error = %e,
                "Scanner heal admission submission failed"
            ),
        }
        if admitted {
            done_heal();
        }
    }

    fn alert_excessive_versions(&self, remaining_versions: usize, cumulative_size: i64) {
        ensure_scanner_alert_metrics_registered();
        let (too_many_versions, too_large_versions) = should_alert_excessive_versions(remaining_versions, cumulative_size);
        if too_many_versions {
            global_metrics().record_scanner_source_executed(ScannerWorkSource::Alerts, 1);
            counter!(
                METRIC_SCANNER_EXCESS_OBJECT_VERSIONS_TOTAL,
                "bucket" => self.bucket.clone()
            )
            .increment(1);
            warn!(
                target: "rustfs::scanner::folder",
                event = EVENT_SCANNER_ALERT_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_FOLDER,
                bucket = %self.bucket,
                object = %self.object_path(),
                versions = remaining_versions,
                threshold = scanner_excess_versions_threshold(),
                state = "excess_versions",
                "Scanner alert recorded excessive retained versions"
            );
        }
        if too_large_versions {
            global_metrics().record_scanner_source_executed(ScannerWorkSource::Alerts, 1);
            counter!(
                METRIC_SCANNER_EXCESS_OBJECT_VERSION_SIZE_TOTAL,
                "bucket" => self.bucket.clone()
            )
            .increment(1);
            warn!(
                target: "rustfs::scanner::folder",
                event = EVENT_SCANNER_ALERT_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_FOLDER,
                bucket = %self.bucket,
                object = %self.object_path(),
                versions = remaining_versions,
                cumulative_size,
                threshold = scanner_excess_version_size_threshold(),
                state = "excess_version_size",
                "Scanner alert recorded excessive retained version size"
            );
        }
    }
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

    /// Set heal object select probability
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

            let active_replication =
                if self.old_cache.info.replication.as_ref().is_some_and(|v| {
                    !v.is_empty() && v.config.as_ref().is_some_and(|config| config.has_active_rules(&prefix, true))
                }) {
                    self.old_cache.info.replication.clone()
                } else {
                    None
                };

            self.sleeper.sleep_folder().await;

            let mut existing_folders: Vec<CachedFolder> = Vec::new();
            let mut new_folders: Vec<CachedFolder> = Vec::new();
            let mut found_objects = false;
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
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    warn!(
                        target: "rustfs::scanner::folder",
                        event = EVENT_SCANNER_FOLDER_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_FOLDER,
                        dir_path = %dir_path,
                        state = "dir_missing_before_read",
                        error = %e,
                        "Scanner folder state updated"
                    );
                    return Ok(());
                }
                Err(e) => return Err(ScannerError::Io(e)),
            };

            loop {
                let entry = match dir_reader.next_entry().await {
                    Ok(Some(entry)) => entry,
                    Ok(None) => break,
                    Err(e) if e.kind() == ErrorKind::NotFound => {
                        warn!(
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
                let file_name = entry.file_name().to_string_lossy().to_string();
                if file_name.is_empty() || file_name == "." || file_name == ".." {
                    continue;
                }

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
                        warn!(
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

                if entry_type.is_symlink() {
                    let metadata = match tokio::fs::metadata(&file_path).await {
                        Ok(metadata) => metadata,
                        Err(e) if e.kind() == ErrorKind::NotFound => {
                            warn!(
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
                        let is_skip_file = matches!(e, StorageError::Io(ref io) if io.to_string() == SCANNER_SKIP_FILE_ERROR);

                        if !is_skip_file {
                            // Track failed objects to prevent infinite retry loops
                            into.failed_objects += 1;
                            self.record_failed(&item.path);

                            if should_log_failed_object(into.failed_objects) {
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

                        timer.sleep().await;
                        continue;
                    }
                };

                found_objects = true;

                item.transform_meta_dir();

                abandoned_children.remove(&path_join_buf(&[&item.bucket, &item.object_path()]));

                apply_scanner_size_summary(into, &sz);
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

            if ctx.is_cancelled() {
                return Err(ScannerError::Other("Operation cancelled".to_string()));
            }

            if found_objects && is_erasure().await {
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

                    let h = DataUsageHash(folder_item.name.clone());
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

            let mut resolver = MetadataResolutionParams {
                dir_quorum: self.disks_quorum,
                obj_quorum: self.disks_quorum,
                bucket: "".to_string(),
                strict: false,
                ..Default::default()
            };

            for name in abandoned_children {
                if !self.should_heal().await {
                    break;
                }

                let (bucket, prefix) = path2_bucket_object(name.as_str());

                if bucket != resolver.bucket {
                    send_required_scanner_heal_request(
                        "bucket",
                        &bucket,
                        None,
                        build_bucket_heal_request(bucket.clone(), HealChannelPriority::High),
                    )
                    .await?;
                }

                resolver.bucket = bucket.clone();

                let child_ctx = ctx.child_token();

                let (agreed_tx, mut agreed_rx) = mpsc::channel::<String>(1);
                let (partial_tx, mut partial_rx) = mpsc::channel::<MetaCacheEntries>(1);
                let (finished_tx, mut finished_rx) = mpsc::channel::<Vec<Option<DiskError>>>(1);

                let disks = self.disks.iter().cloned().map(Some).collect();
                let disks_quorum = self.disks_quorum;
                let bucket_clone = bucket.clone();
                let prefix_clone = prefix.clone();
                let child_ctx_clone = child_ctx.clone();

                tokio::spawn(async move {
                    if let Err(e) = list_path_raw(
                        child_ctx_clone.clone(),
                        ListPathRawOptions {
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
                            ..Default::default()
                        },
                    )
                    .await
                    {
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
                });

                let mut found_objects = false;
                let mut agreed_closed = false;
                let mut partial_closed = false;
                let mut finished_closed = false;

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

                            let Some(entry) = resolve_object_heal_entry(&entries, resolver.clone()) else {
                                continue;
                            };

                            (self.update_current_path)(&entry.name).await;

                            if entry.is_dir() {
                                continue;
                            }

                            let fivs = match entry.file_info_versions(&bucket) {
                                Ok(fivs) => fivs,
                                Err(e) => {
                                    error!(
                                        target: "rustfs::scanner::folder",
                                        event = EVENT_SCANNER_FOLDER_STATE,
                                        component = LOG_COMPONENT_SCANNER,
                                        subsystem = LOG_SUBSYSTEM_FOLDER,
                                        bucket = %bucket,
                                        entry = %entry.name,
                                        state = "file_info_versions_failed",
                                        error = %e,
                                        "Scanner list_path_raw failed to resolve file versions"
                                    );
                                    send_required_scanner_heal_request(
                                        "object",
                                        &bucket,
                                        Some(&entry.name),
                                        build_object_heal_request(
                                            bucket.clone(),
                                            entry.name.clone(),
                                            None,
                                            self.scan_mode,
                                            HealChannelPriority::High,
                                        ),
                                    )
                                    .await?;
                                    found_objects = true;
                                    continue;
                                }
                            };

                            for fiv in fivs.versions {
                                send_required_scanner_heal_request(
                                    "object",
                                    &bucket,
                                    Some(&entry.name),
                                    build_object_heal_request(
                                        bucket.clone(),
                                        entry.name.clone(),
                                        fiv.version_id.and_then(|v| if v.is_nil() { None } else { Some(v.to_string()) }),
                                        self.scan_mode,
                                        HealChannelPriority::High,
                                    ),
                                )
                                .await?;
                                found_objects = true;
                            }


                        }
                        errs = finished_rx.recv(), if !finished_closed => {
                            let Some(errs) = errs else {
                                finished_closed = true;
                                continue;
                            };
                            error!(
                                target: "rustfs::scanner::folder",
                                event = EVENT_SCANNER_FOLDER_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_FOLDER,
                                state = "list_path_finished_with_errors",
                                errors = ?errs,
                                "Scanner list_path_raw finished with disk errors"
                            );
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

                        // Use Box::pin for recursive async call
                        let fut = Box::pin(self.scan_folder(ctx.clone(), folder_item.clone(), &mut dst));
                        if let Err(e) = fut.await {
                            if ctx.is_cancelled() {
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

                        let h = DataUsageHash(folder_item.name.clone());
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

        done_folder();

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

    ensure_scanner_inline_heal_metric_registered();

    // Check that we're not trying to scan the root
    if cache.info.name.is_empty() || cache.info.name == DATA_USAGE_ROOT {
        return Err(ScannerError::Other("internal error: root scan attempted".to_string()));
    }

    // Get disk path
    let base_path = local_disk.path().to_string_lossy().to_string();

    let (update_current_path, close_disk) = current_path_updater(&base_path, &cache.info.name);

    // Create skip_heal flag
    let is_erasure_mode = is_erasure().await;
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
    };

    // Check if context is cancelled
    if ctx.is_cancelled() {
        return Err(ScannerError::Other("Operation cancelled".to_string()));
    }

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
            let had_scan_checkpoint = cache.info.scan_checkpoint.is_some() || new_cache.info.scan_checkpoint.is_some();
            new_cache.info.scan_resume_after = None;
            new_cache.info.scan_checkpoint = None;
            if had_scan_checkpoint {
                global_metrics().record_scanner_checkpoint_cleared();
            }

            close_disk().await;
            Ok(new_cache.clone())
        }
        Err(e) => {
            if ctx.is_cancelled() {
                let root_has_progress = !root.children.is_empty()
                    || root.size > 0
                    || root.objects > 0
                    || root.versions > 0
                    || root.delete_markers > 0
                    || root.failed_objects > 0
                    || root.replication_stats.is_some();
                let new_cache = scanner.as_mut_new_cache();
                if root_has_progress {
                    new_cache.replace_hashed(&hash_path(&cache.info.name), &None, &root);
                }
                if new_cache.root().is_some() {
                    new_cache.force_compact(DATA_SCANNER_COMPACT_AT_CHILDREN);
                    new_cache.info.last_update = Some(SystemTime::now());
                    new_cache.info.next_cycle = cache.info.next_cycle;
                    set_scan_checkpoint(new_cache, checkpoint_reason_from_budget(budget.reason()));
                    close_disk().await;
                    return Err(ScannerError::PartialCache(Box::new(new_cache.clone())));
                }
            }
            close_disk().await;
            // No useful information, return original cache
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::SCANNER_SLEEPER;

    use super::*;
    use crate::{DiskOption, Endpoint, new_disk};
    use rustfs_filemeta::{
        FileInfo, FileMeta, ReplicateObjectInfo, ReplicationType, ResyncDecision, ResyncTargetDecision, VersionPurgeStatusType,
    };
    use serial_test::serial;
    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::sync::atomic::AtomicBool;
    use temp_env::{with_var, with_var_unset};
    use uuid::Uuid;

    async fn build_test_scanner() -> (FolderScanner, std::path::PathBuf) {
        let temp_dir = std::env::temp_dir().join(format!("rustfs-scanner-test-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&temp_dir)
            .await
            .expect("failed to create test directory");

        let endpoint = Endpoint::try_from(temp_dir.to_string_lossy().as_ref()).expect("failed to create endpoint");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("failed to create disk");

        let update_current_path: UpdateCurrentPathFn = Arc::new(|_: &str| Box::pin(async {}));

        let scanner = FolderScanner {
            root: temp_dir.to_string_lossy().to_string(),
            old_cache: DataUsageCache::default(),
            new_cache: DataUsageCache::default(),
            update_cache: DataUsageCache::default(),
            data_usage_scanner_debug: false,
            heal_object_select: 0,
            scan_mode: HealScanMode::Normal,
            failed_object_ttl_secs: u64::MAX,
            failed_objects_max: usize::MAX,
            sleeper: SCANNER_SLEEPER.clone(),
            disks: Vec::new(),
            disks_quorum: 0,
            updates: None,
            last_update: SystemTime::UNIX_EPOCH,
            update_current_path,
            budget: ScannerCycleBudget::new(&CancellationToken::new(), Default::default()),
            skip_heal: Arc::new(AtomicBool::new(false)),
            local_disk: disk,
        };

        (scanner, temp_dir)
    }

    struct TestGuard {
        temp_dir: Option<std::path::PathBuf>,
    }

    impl TestGuard {
        fn new(ttl: u64, max: usize, scanner: &mut FolderScanner, temp_dir: std::path::PathBuf) -> Self {
            scanner.failed_object_ttl_secs = ttl;
            scanner.failed_objects_max = max;
            Self {
                temp_dir: Some(temp_dir),
            }
        }
    }

    impl Drop for TestGuard {
        fn drop(&mut self) {
            if let Some(temp_dir) = self.temp_dir.take() {
                let _ = std::fs::remove_dir_all(&temp_dir);
            }
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_should_skip_failed_respects_ttl() {
        let (mut scanner, temp_dir) = build_test_scanner().await;
        let _guard = TestGuard::new(60, 100, &mut scanner, temp_dir);
        let now = FolderScanner::now_secs();

        scanner
            .new_cache
            .info
            .failed_objects
            .insert("recent".to_string(), now.saturating_sub(10));
        scanner
            .new_cache
            .info
            .failed_objects
            .insert("expired".to_string(), now.saturating_sub(120));

        assert!(scanner.should_skip_failed("recent"));
        assert!(!scanner.should_skip_failed("expired"));
    }

    #[tokio::test]
    #[serial]
    async fn test_record_failed_ttl_zero_noop() {
        let (mut scanner, temp_dir) = build_test_scanner().await;
        let _guard = TestGuard::new(0, 100, &mut scanner, temp_dir);

        scanner.record_failed("path1");
        assert!(scanner.new_cache.info.failed_objects.is_empty());

        let now = FolderScanner::now_secs();
        scanner.new_cache.info.failed_objects.insert("path2".to_string(), now);
        assert!(!scanner.should_skip_failed("path2"));
    }

    #[test]
    fn test_should_account_replication_stats_only_for_live_object_versions() {
        let live = ObjectInfo::default();
        assert!(ScannerItem::should_account_replication_stats(&live));

        let delete_marker = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        assert!(!ScannerItem::should_account_replication_stats(&delete_marker));

        let purge_version = ObjectInfo {
            version_purge_status: VersionPurgeStatusType::Pending,
            ..Default::default()
        };
        assert!(!ScannerItem::should_account_replication_stats(&purge_version));
    }

    #[tokio::test]
    async fn test_scanner_ilm_action_accounting_requires_enqueue_success() {
        let metrics = Metrics::new();
        let _start = metrics.start_scan_cycle_work();

        record_scanner_ilm_action_if_queued(&metrics, IlmAction::DeleteAction, 2, false);
        let report = metrics.report().await;
        let lifecycle = report
            .source_work
            .iter()
            .find(|work| work.source == ScannerWorkSource::Lifecycle.as_str())
            .expect("lifecycle source work should be visible");
        assert_eq!(lifecycle.executed, 0);
        assert_eq!(report.current_cycle_lifecycle_expiry_actions, 0);

        record_scanner_ilm_action_if_queued(&metrics, IlmAction::DeleteAction, 3, true);
        let report = metrics.report().await;
        let lifecycle = report
            .source_work
            .iter()
            .find(|work| work.source == ScannerWorkSource::Lifecycle.as_str())
            .expect("lifecycle source work should be visible");
        assert_eq!(lifecycle.executed, 3);
        assert_eq!(report.current_cycle_lifecycle_expiry_actions, 3);
        assert_eq!(report.current_cycle_lifecycle_transition_actions, 0);

        record_scanner_ilm_action_if_queued(&metrics, IlmAction::TransitionAction, 4, true);
        let report = metrics.report().await;
        let lifecycle = report
            .source_work
            .iter()
            .find(|work| work.source == ScannerWorkSource::Lifecycle.as_str())
            .expect("lifecycle source work should be visible");
        assert_eq!(lifecycle.executed, 7);
        assert_eq!(report.current_cycle_lifecycle_expiry_actions, 3);
        assert_eq!(report.current_cycle_lifecycle_transition_actions, 4);
    }

    #[test]
    fn test_pending_scanner_accounting_requires_enqueue_success() {
        let object = ObjectInfo {
            size: 10,
            version_id: Some(uuid::Uuid::new_v4()),
            ..Default::default()
        };
        let pending = PendingScannerAccounting {
            object: &object,
            retained_size: 10,
            expired_size: 0,
        };

        let mut failed_summary = SizeSummary::default();
        let mut failed_cumulative_size = 0;
        pending.apply(&mut failed_summary, &mut failed_cumulative_size, false);
        assert_eq!(failed_summary.versions, 1);
        assert_eq!(failed_summary.total_size, 10);
        assert_eq!(failed_cumulative_size, 10);

        let mut queued_summary = SizeSummary::default();
        let mut queued_cumulative_size = 0;
        pending.apply(&mut queued_summary, &mut queued_cumulative_size, true);
        assert_eq!(queued_summary.versions, 0);
        assert_eq!(queued_summary.total_size, 0);
        assert_eq!(queued_cumulative_size, 0);
    }

    #[tokio::test]
    async fn test_scanner_replication_admission_accounting_maps_source_work() {
        let metrics = Metrics::new();
        let object = ReplicateObjectInfo {
            bucket: "bucket-a".to_string(),
            name: "object-a".to_string(),
            ..Default::default()
        };

        record_scanner_replication_admission(&metrics, &object, ReplicationQueueAdmission::Skipped);
        record_scanner_replication_admission(&metrics, &object, ReplicationQueueAdmission::Queued);
        record_scanner_replication_admission(&metrics, &object, ReplicationQueueAdmission::Missed);

        let report = metrics.report().await;
        let replication = report
            .source_work
            .iter()
            .find(|work| work.source == ScannerWorkSource::BucketReplication.as_str())
            .expect("bucket replication source work should be visible");

        assert_eq!(replication.skipped, 1);
        assert_eq!(replication.queued, 1);
        assert_eq!(replication.missed, 1);

        let object_repair = report
            .replication_repair
            .iter()
            .find(|repair| repair.source == ScannerWorkSource::BucketReplication.as_str() && repair.kind == "object")
            .expect("bucket object repair work should be visible");
        assert_eq!(object_repair.scanner_role, "repair_admission");
        assert_eq!(object_repair.execution_owner, "bucket_replication_queue");
        assert_eq!(object_repair.skipped, 1);
        assert_eq!(object_repair.queued, 1);
        assert_eq!(object_repair.missed, 1);
    }

    #[test]
    fn test_scanner_replication_repair_kind_maps_bucket_variants() {
        let object = ReplicateObjectInfo {
            bucket: "bucket-a".to_string(),
            name: "object-a".to_string(),
            replication_status: ReplicationStatusType::Pending,
            ..Default::default()
        };
        assert_eq!(scanner_replication_repair_kind(&object), Some(ScannerReplicationRepairKind::BucketObject));

        let delete_marker = ReplicateObjectInfo {
            bucket: "bucket-a".to_string(),
            name: "delete-marker-a".to_string(),
            delete_marker: true,
            replication_status: ReplicationStatusType::Failed,
            ..Default::default()
        };
        assert_eq!(
            scanner_replication_repair_kind(&delete_marker),
            Some(ScannerReplicationRepairKind::BucketDeleteMarker)
        );

        let version_purge = ReplicateObjectInfo {
            bucket: "bucket-a".to_string(),
            name: "version-purge-a".to_string(),
            version_purge_status: VersionPurgeStatusType::Pending,
            ..Default::default()
        };
        assert_eq!(
            scanner_replication_repair_kind(&version_purge),
            Some(ScannerReplicationRepairKind::BucketVersionPurge)
        );

        let existing_object = ReplicateObjectInfo {
            bucket: "bucket-a".to_string(),
            name: "existing-object-a".to_string(),
            op_type: ReplicationType::ExistingObject,
            ..Default::default()
        };
        assert_eq!(
            scanner_replication_repair_kind(&existing_object),
            Some(ScannerReplicationRepairKind::BucketExistingObject)
        );

        let mut existing_obj_resync = ResyncDecision::new();
        existing_obj_resync.targets.insert(
            "arn:minio:replication:::target".to_string(),
            ResyncTargetDecision {
                replicate: true,
                ..Default::default()
            },
        );
        let existing_delete_marker = ReplicateObjectInfo {
            bucket: "bucket-a".to_string(),
            name: "existing-delete-marker-a".to_string(),
            delete_marker: true,
            replication_status: ReplicationStatusType::Completed,
            existing_obj_resync,
            ..Default::default()
        };
        assert_eq!(
            scanner_replication_repair_kind(&existing_delete_marker),
            Some(ScannerReplicationRepairKind::BucketExistingObject)
        );

        assert_eq!(scanner_replication_repair_kind(&ReplicateObjectInfo::default()), None);
    }

    #[tokio::test]
    async fn test_scanner_heal_admission_accounting_maps_normal_scan_to_heal() {
        let metrics = Metrics::new();

        record_scanner_heal_admission(&metrics, HealScanMode::Normal, Ok(HealAdmissionResult::Accepted));
        record_scanner_heal_admission(&metrics, HealScanMode::Normal, Ok(HealAdmissionResult::Merged));
        record_scanner_heal_admission(&metrics, HealScanMode::Normal, Ok(HealAdmissionResult::Full));
        record_scanner_heal_admission(
            &metrics,
            HealScanMode::Normal,
            Ok(HealAdmissionResult::Dropped(
                rustfs_common::heal_channel::HealAdmissionDropReason::QueueFull,
            )),
        );
        record_scanner_heal_admission(&metrics, HealScanMode::Normal, Err(()));

        let report = metrics.report().await;
        let heal = report
            .source_work
            .iter()
            .find(|work| work.source == ScannerWorkSource::Heal.as_str())
            .expect("heal source work should be visible");
        let bitrot = report
            .source_work
            .iter()
            .find(|work| work.source == ScannerWorkSource::Bitrot.as_str())
            .expect("bitrot source work should be visible");

        assert_eq!(heal.queued, 1);
        assert_eq!(heal.skipped, 1);
        assert_eq!(heal.missed, 3);
        assert_eq!(heal.executed, 0);
        assert_eq!(bitrot.queued + bitrot.skipped + bitrot.missed + bitrot.executed, 0);
    }

    #[tokio::test]
    async fn test_scanner_heal_admission_accounting_maps_deep_scan_to_bitrot() {
        let metrics = Metrics::new();

        record_scanner_heal_admission(&metrics, HealScanMode::Deep, Ok(HealAdmissionResult::Accepted));
        record_scanner_heal_admission(&metrics, HealScanMode::Deep, Ok(HealAdmissionResult::Merged));
        record_scanner_heal_admission(&metrics, HealScanMode::Deep, Ok(HealAdmissionResult::Full));

        let report = metrics.report().await;
        let heal = report
            .source_work
            .iter()
            .find(|work| work.source == ScannerWorkSource::Heal.as_str())
            .expect("heal source work should be visible");
        let bitrot = report
            .source_work
            .iter()
            .find(|work| work.source == ScannerWorkSource::Bitrot.as_str())
            .expect("bitrot source work should be visible");

        assert_eq!(bitrot.queued, 1);
        assert_eq!(bitrot.skipped, 1);
        assert_eq!(bitrot.missed, 1);
        assert_eq!(bitrot.executed, 0);
        assert_eq!(heal.queued + heal.skipped + heal.missed + heal.executed, 0);
    }

    #[test]
    #[serial]
    fn test_excessive_version_alert_thresholds_use_env() {
        with_var(rustfs_config::ENV_SCANNER_ALERT_EXCESS_VERSIONS, Some("3"), || {
            with_var(rustfs_config::ENV_SCANNER_ALERT_EXCESS_VERSION_SIZE, Some("100"), || {
                crate::runtime_config::refresh_scanner_runtime_config_for_tests();
                assert_eq!(should_alert_excessive_versions(2, 99), (false, false));
                assert_eq!(should_alert_excessive_versions(3, 99), (true, false));
                assert_eq!(should_alert_excessive_versions(2, 100), (false, true));
                assert_eq!(should_alert_excessive_versions(3, 100), (true, true));
            });
        });
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
    }

    #[test]
    #[serial]
    fn test_excessive_folders_threshold_uses_env() {
        with_var(rustfs_config::ENV_SCANNER_ALERT_EXCESS_FOLDERS, Some("3"), || {
            crate::runtime_config::refresh_scanner_runtime_config_for_tests();
            assert_eq!(scanner_excess_folders_threshold(), 3);
        });
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
    }

    #[test]
    #[serial]
    fn test_excessive_folders_threshold_default_supports_pbs_layout() {
        with_var_unset(rustfs_config::ENV_SCANNER_ALERT_EXCESS_FOLDERS, || {
            crate::runtime_config::refresh_scanner_runtime_config_for_tests();
            assert_eq!(scanner_excess_folders_threshold(), 65_538);
        });
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
    }

    #[test]
    #[serial]
    fn test_scanner_yield_every_n_objects_uses_env() {
        with_var(rustfs_config::ENV_SCANNER_YIELD_EVERY_N_OBJECTS, Some("32"), || {
            crate::runtime_config::refresh_scanner_runtime_config_for_tests();
            assert_eq!(scanner_yield_every_n_objects(), 32);
        });
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
    }

    #[test]
    #[serial]
    fn test_scanner_yield_every_n_objects_uses_default() {
        with_var_unset(rustfs_config::ENV_SCANNER_YIELD_EVERY_N_OBJECTS, || {
            crate::runtime_config::refresh_scanner_runtime_config_for_tests();
            assert_eq!(scanner_yield_every_n_objects(), rustfs_config::DEFAULT_SCANNER_YIELD_EVERY_N_OBJECTS);
        });
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
    }

    #[test]
    fn test_should_yield_after_object_respects_interval_and_disable() {
        assert!(!should_yield_after_object(127, 128));
        assert!(should_yield_after_object(128, 128));
        assert!(!should_yield_after_object(128, 0));
    }

    #[test]
    fn test_checkpoint_reason_from_budget_maps_all_budget_reasons() {
        assert_eq!(
            checkpoint_reason_from_budget(Some(crate::scanner_budget::ScannerCycleBudgetReason::Runtime)),
            crate::data_usage_define::DataUsageScanCheckpointReason::Runtime
        );
        assert_eq!(
            checkpoint_reason_from_budget(Some(crate::scanner_budget::ScannerCycleBudgetReason::Objects)),
            crate::data_usage_define::DataUsageScanCheckpointReason::Objects
        );
        assert_eq!(
            checkpoint_reason_from_budget(Some(crate::scanner_budget::ScannerCycleBudgetReason::Directories)),
            crate::data_usage_define::DataUsageScanCheckpointReason::Directories
        );
        assert_eq!(
            checkpoint_reason_from_budget(None),
            crate::data_usage_define::DataUsageScanCheckpointReason::Unknown
        );
    }

    #[test]
    fn test_order_folders_for_resume_rotates_after_exact_resume_hint() {
        let mut folders = vec![
            CachedFolder {
                name: "bucket/child-c".to_string(),
                parent: None,
                object_heal_prob_div: 1,
            },
            CachedFolder {
                name: "bucket/child-a".to_string(),
                parent: None,
                object_heal_prob_div: 1,
            },
            CachedFolder {
                name: "bucket/child-b".to_string(),
                parent: None,
                object_heal_prob_div: 1,
            },
        ];

        let outcome = order_folders_for_resume(&mut folders, Some("bucket/child-b"));

        let names = folders.into_iter().map(|folder| folder.name).collect::<Vec<_>>();
        assert_eq!(outcome, FolderResumeOrder::Used);
        assert_eq!(
            names,
            vec![
                "bucket/child-c".to_string(),
                "bucket/child-a".to_string(),
                "bucket/child-b".to_string()
            ]
        );
    }

    #[test]
    fn test_order_folders_for_resume_prioritizes_descendant_resume_hint() {
        let mut folders = vec![
            CachedFolder {
                name: "bucket/child-c".to_string(),
                parent: None,
                object_heal_prob_div: 1,
            },
            CachedFolder {
                name: "bucket/child-a".to_string(),
                parent: None,
                object_heal_prob_div: 1,
            },
            CachedFolder {
                name: "bucket/child-b".to_string(),
                parent: None,
                object_heal_prob_div: 1,
            },
        ];

        let outcome = order_folders_for_resume(&mut folders, Some("bucket/child-b/grandchild"));

        let names = folders.into_iter().map(|folder| folder.name).collect::<Vec<_>>();
        assert_eq!(outcome, FolderResumeOrder::Used);
        assert_eq!(
            names,
            vec![
                "bucket/child-b".to_string(),
                "bucket/child-c".to_string(),
                "bucket/child-a".to_string()
            ]
        );
    }

    #[test]
    fn test_order_folders_for_resume_reports_stale_hint() {
        let mut folders = vec![
            CachedFolder {
                name: "bucket/child-c".to_string(),
                parent: None,
                object_heal_prob_div: 1,
            },
            CachedFolder {
                name: "bucket/child-a".to_string(),
                parent: None,
                object_heal_prob_div: 1,
            },
        ];

        let outcome = order_folders_for_resume(&mut folders, Some("bucket/child-b"));

        let names = folders.into_iter().map(|folder| folder.name).collect::<Vec<_>>();
        assert_eq!(outcome, FolderResumeOrder::Stale);
        assert_eq!(names, vec!["bucket/child-a".to_string(), "bucket/child-c".to_string()]);
    }

    #[tokio::test]
    #[serial]
    async fn test_record_failed_prunes_to_max_entries() {
        let (mut scanner, temp_dir) = build_test_scanner().await;
        let _guard = TestGuard::new(1000, 2, &mut scanner, temp_dir);
        let now = FolderScanner::now_secs();

        scanner
            .new_cache
            .info
            .failed_objects
            .insert("old1".to_string(), now.saturating_sub(50));
        scanner
            .new_cache
            .info
            .failed_objects
            .insert("old2".to_string(), now.saturating_sub(40));
        scanner
            .new_cache
            .info
            .failed_objects
            .insert("old3".to_string(), now.saturating_sub(30));

        scanner.record_failed("new");

        assert_eq!(scanner.new_cache.info.failed_objects.len(), 2);
        assert!(scanner.new_cache.info.failed_objects.contains_key("new"));
        assert!(scanner.new_cache.info.failed_objects.contains_key("old3"));
        assert!(!scanner.new_cache.info.failed_objects.contains_key("old1"));
        assert!(!scanner.new_cache.info.failed_objects.contains_key("old2"));
    }

    #[tokio::test]
    #[serial]
    async fn test_prune_failed_objects_cache_drops_expired() {
        let (mut scanner, temp_dir) = build_test_scanner().await;
        let _guard = TestGuard::new(5, 10, &mut scanner, temp_dir);
        let now = FolderScanner::now_secs();

        scanner
            .new_cache
            .info
            .failed_objects
            .insert("expired".to_string(), now.saturating_sub(10));
        scanner
            .new_cache
            .info
            .failed_objects
            .insert("fresh".to_string(), now.saturating_sub(2));

        scanner.prune_failed_objects_cache();

        assert_eq!(scanner.new_cache.info.failed_objects.len(), 1);
        assert!(scanner.new_cache.info.failed_objects.contains_key("fresh"));
    }

    #[tokio::test]
    #[serial]
    async fn test_prune_failed_objects_max_zero_keeps_fresh() {
        let (mut scanner, temp_dir) = build_test_scanner().await;
        let _guard = TestGuard::new(60, 0, &mut scanner, temp_dir);
        let now = FolderScanner::now_secs();

        scanner
            .new_cache
            .info
            .failed_objects
            .insert("fresh1".to_string(), now.saturating_sub(5));
        scanner
            .new_cache
            .info
            .failed_objects
            .insert("fresh2".to_string(), now.saturating_sub(10));
        scanner
            .new_cache
            .info
            .failed_objects
            .insert("expired".to_string(), now.saturating_sub(120));

        scanner.prune_failed_objects_cache();

        assert_eq!(scanner.new_cache.info.failed_objects.len(), 2);
        assert!(scanner.new_cache.info.failed_objects.contains_key("fresh1"));
        assert!(scanner.new_cache.info.failed_objects.contains_key("fresh2"));
        assert!(!scanner.new_cache.info.failed_objects.contains_key("expired"));
    }

    #[test]
    fn test_scanner_inline_heal_enabled_defaults_to_false() {
        assert!(!scanner_inline_heal_enabled_from_value(None));
    }

    #[test]
    fn test_scanner_inline_heal_enabled_reads_env_override() {
        assert!(scanner_inline_heal_enabled_from_value(Some("true")));
        assert!(scanner_inline_heal_enabled_from_value(Some("YES")));
        assert!(scanner_inline_heal_enabled_from_value(Some("1")));
        assert!(!scanner_inline_heal_enabled_from_value(Some("false")));
    }

    #[test]
    fn test_build_object_heal_request_omits_nil_version_id() {
        let request = build_object_heal_request(
            "bucket".to_string(),
            "path/to/object".to_string(),
            None,
            HealScanMode::Deep,
            HealChannelPriority::Low,
        );

        assert_eq!(request.bucket, "bucket");
        assert_eq!(request.object_prefix.as_deref(), Some("path/to/object"));
        assert!(request.object_version_id.is_none());
        assert_eq!(request.scan_mode, Some(HealScanMode::Deep));
        assert_eq!(request.priority, HealChannelPriority::Low);
        assert_eq!(request.source, HealRequestSource::Scanner);
        assert_eq!(request.remove_corrupted, Some(HEAL_DELETE_DANGLING));
    }

    fn metadata_for_object(bucket: &str, object: &str) -> Vec<u8> {
        let mut meta = FileMeta::new();
        meta.add_version(FileInfo {
            volume: bucket.to_string(),
            name: object.to_string(),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        })
        .expect("test metadata version should be accepted");
        meta.marshal_msg().expect("test metadata should marshal")
    }

    fn test_metadata_resolver(bucket: &str) -> MetadataResolutionParams {
        MetadataResolutionParams {
            bucket: bucket.to_string(),
            dir_quorum: 2,
            obj_quorum: 2,
            ..Default::default()
        }
    }

    #[test]
    fn test_resolve_object_heal_entry_allows_plain_unresolved_fallback() {
        let entries = MetaCacheEntries(vec![Some(MetaCacheEntry {
            name: "object".to_string(),
            metadata: vec![1, 2, 3],
            ..Default::default()
        })]);

        let entry = resolve_object_heal_entry(&entries, test_metadata_resolver("bucket"))
            .expect("plain object fallback should be eligible for heal");

        assert_eq!(entry.name, "object");
    }

    #[test]
    fn test_resolve_object_heal_entry_skips_unresolved_trailing_slash_fallback() {
        let entries = MetaCacheEntries(vec![Some(MetaCacheEntry {
            name: "object/".to_string(),
            metadata: vec![1, 2, 3],
            ..Default::default()
        })]);

        assert!(
            resolve_object_heal_entry(&entries, test_metadata_resolver("bucket")).is_none(),
            "unresolved trailing-slash fallback must not be submitted as an object heal"
        );
    }

    #[test]
    fn test_resolve_object_heal_entry_preserves_resolved_trailing_slash_object() {
        let metadata = metadata_for_object("bucket", "object/");
        let entries = MetaCacheEntries(vec![
            Some(MetaCacheEntry {
                name: "object/".to_string(),
                metadata: metadata.clone(),
                ..Default::default()
            }),
            Some(MetaCacheEntry {
                name: "object/".to_string(),
                metadata,
                ..Default::default()
            }),
        ]);

        let entry = resolve_object_heal_entry(&entries, test_metadata_resolver("bucket"))
            .expect("resolved trailing-slash object should remain eligible");

        assert_eq!(entry.name, "object/");
        assert!(entry.is_object_dir());
    }

    #[test]
    fn test_effective_object_heal_scan_mode_keeps_normal_when_bitrot_disabled() {
        let now = OffsetDateTime::now_utc();
        assert_eq!(effective_object_heal_scan_mode(false, Some(now), now), HealScanMode::Normal);
    }

    #[test]
    fn test_effective_object_heal_scan_mode_downgrades_recent_object_to_normal() {
        let now = OffsetDateTime::now_utc();
        let recent = now - time::Duration::seconds(5);
        assert_eq!(effective_object_heal_scan_mode(true, Some(recent), now), HealScanMode::Normal);
    }

    #[test]
    fn test_effective_object_heal_scan_mode_keeps_old_object_deep() {
        let now = OffsetDateTime::now_utc();
        let old = now - time::Duration::seconds((DEFAULT_SCANNER_DEEP_VERIFY_COOLDOWN_SECS as i64) + 5);
        assert_eq!(effective_object_heal_scan_mode(true, Some(old), now), HealScanMode::Deep);
    }

    #[test]
    fn test_heal_priority_label_matches_priority_names() {
        assert_eq!(heal_priority_label(HealChannelPriority::Low), "low");
        assert_eq!(heal_priority_label(HealChannelPriority::Normal), "normal");
        assert_eq!(heal_priority_label(HealChannelPriority::High), "high");
        assert_eq!(heal_priority_label(HealChannelPriority::Critical), "critical");
    }

    #[test]
    fn test_describe_heal_admission_formats_unadmitted_results() {
        assert_eq!(describe_heal_admission(HealAdmissionResult::Accepted), "accepted");
        assert_eq!(describe_heal_admission(HealAdmissionResult::Merged), "merged");
        assert_eq!(describe_heal_admission(HealAdmissionResult::Full), "queue_full");
        assert_eq!(
            describe_heal_admission(HealAdmissionResult::Dropped(
                rustfs_common::heal_channel::HealAdmissionDropReason::QueueFull
            )),
            "dropped:queue_full"
        );
    }

    #[test]
    fn test_build_high_priority_heal_admission_error_contains_context() {
        let err = build_high_priority_heal_admission_error(
            "object",
            "bucket-a",
            Some("path/to/object"),
            HealChannelPriority::High,
            HealAdmissionResult::Full,
        );

        let err_text = err.to_string();
        assert!(err_text.contains("type=object"));
        assert!(err_text.contains("bucket='bucket-a'"));
        assert!(err_text.contains("object='path/to/object'"));
        assert!(err_text.contains("priority=high"));
        assert!(err_text.contains("admission=queue_full"));
    }

    #[tokio::test]
    async fn test_heal_actions_returns_actual_size_without_inline_heal() {
        let temp_dir = std::env::temp_dir();
        let file_type = std::fs::metadata(&temp_dir).unwrap().file_type();

        let mut item = ScannerItem {
            path: temp_dir.join("object").to_string_lossy().to_string(),
            bucket: "bucket".to_string(),
            prefix: "".to_string(),
            object_name: "object".to_string(),
            file_type,
            lifecycle: None,
            replication: None,
            heal_enabled: true,
            heal_bitrot: true,
            debug: false,
        };
        let object_info = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            ..Default::default()
        };
        let mut size_summary = SizeSummary::default();

        let size = item.heal_actions(&object_info, 123, &mut size_summary).await;
        assert_eq!(size, 123);
    }

    #[tokio::test]
    #[serial]
    #[cfg(unix)]
    async fn test_scan_folder_skips_unreadable_child_directory() {
        let (mut scanner, temp_dir) = build_test_scanner().await;
        let _guard = TestGuard::new(60, 0, &mut scanner, temp_dir.clone());

        let bucket_dir = temp_dir.join("bucket");
        let good_dir = bucket_dir.join("good");
        let bad_dir = bucket_dir.join("bad");

        std::fs::create_dir_all(&good_dir).expect("failed to create good dir");
        std::fs::create_dir_all(&bad_dir).expect("failed to create bad dir");
        std::fs::set_permissions(&bad_dir, std::fs::Permissions::from_mode(0o000)).expect("failed to remove bad dir permissions");

        scanner.old_cache.info.name = "bucket".to_string();
        scanner.new_cache.info.name = "bucket".to_string();
        scanner.update_cache.info.name = "bucket".to_string();

        let folder = CachedFolder {
            name: "bucket".to_string(),
            parent: None,
            object_heal_prob_div: 1,
        };

        let mut into = DataUsageEntry::default();
        let result = scanner.scan_folder(CancellationToken::new(), folder, &mut into).await;

        std::fs::set_permissions(&bad_dir, std::fs::Permissions::from_mode(0o755))
            .expect("failed to restore bad dir permissions");

        assert!(result.is_ok(), "expected unreadable child directory to be skipped");
    }

    #[tokio::test]
    #[serial]
    async fn test_scan_folder_exits_when_abandoned_child_listing_finishes() {
        let (mut scanner, temp_dir) = build_test_scanner().await;
        let _guard = TestGuard::new(60, 100, &mut scanner, temp_dir.clone());
        let _heal_responder = rustfs_common::heal_channel::init_heal_channel().ok().map(|mut heal_rx| {
            tokio::spawn(async move {
                while let Some(command) = heal_rx.recv().await {
                    if let rustfs_common::heal_channel::HealChannelCommand::Start { response_tx, .. } = command {
                        let _ = response_tx.send(Ok(HealAdmissionResult::Accepted));
                    }
                }
            })
        });

        let bucket = "src-archive";
        tokio::fs::create_dir_all(temp_dir.join(bucket))
            .await
            .expect("failed to create bucket directory");

        let mut disks = vec![scanner.local_disk.clone()];
        for disk_name in ["disk2", "disk3", "disk4"] {
            let disk_root = temp_dir.join(disk_name);
            tokio::fs::create_dir_all(disk_root.join(bucket))
                .await
                .expect("failed to create extra disk bucket directory");
            let endpoint =
                Endpoint::try_from(disk_root.to_string_lossy().as_ref()).expect("failed to create extra disk endpoint");
            let disk = new_disk(
                &endpoint,
                &DiskOption {
                    cleanup: false,
                    health_check: false,
                },
            )
            .await
            .expect("failed to create extra disk");
            disks.push(disk);
        }

        scanner.heal_object_select = 1;
        scanner.disks = disks;
        scanner.disks_quorum = 2;
        scanner.old_cache.replace(
            "src-archive/snapshots/37b3f20d941e2f5e6d99114d9bb2f3e67a8a2e5c9c4c5a1b0d6e7f8091a2b3c4",
            bucket,
            DataUsageEntry {
                objects: 1,
                ..Default::default()
            },
        );

        let mut into = DataUsageEntry::default();
        let folder = CachedFolder {
            name: bucket.to_string(),
            parent: None,
            object_heal_prob_div: 1,
        };

        tokio::time::timeout(
            Duration::from_millis(200),
            scanner.scan_folder(CancellationToken::new(), folder, &mut into),
        )
        .await
        .expect("scan_folder should not hang after list_path_raw finishes")
        .expect("scan_folder should finish successfully");
    }

    #[tokio::test]
    #[serial]
    async fn test_scan_folder_directory_budget_cancels_after_limit() {
        let (mut scanner, temp_dir) = build_test_scanner().await;
        let _guard = TestGuard::new(60, 100, &mut scanner, temp_dir.clone());

        let bucket_dir = temp_dir.join("bucket");
        tokio::fs::create_dir_all(bucket_dir.join("child"))
            .await
            .expect("failed to create child directory");

        scanner.old_cache.info.name = "bucket".to_string();
        scanner.new_cache.info.name = "bucket".to_string();
        scanner.update_cache.info.name = "bucket".to_string();

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            crate::scanner_budget::ScannerCycleBudgetConfig {
                max_directories: Some(1),
                ..Default::default()
            },
        );
        let ctx = budget.token();
        scanner.budget = budget.clone();

        let folder = CachedFolder {
            name: "bucket".to_string(),
            parent: None,
            object_heal_prob_div: 1,
        };

        let mut into = DataUsageEntry::default();
        let result = scanner.scan_folder(ctx, folder, &mut into).await;

        assert!(result.is_err(), "directory budget cancellation should make the scan partial");
        assert!(budget.budget_elapsed());
        assert_eq!(budget.reason(), Some(crate::scanner_budget::ScannerCycleBudgetReason::Directories));
        assert!(budget.token().is_cancelled());
    }

    #[tokio::test]
    #[serial]
    async fn test_scan_folder_compacted_parent_sends_partial_update() {
        let (mut scanner, temp_dir) = build_test_scanner().await;
        let _guard = TestGuard::new(60, 100, &mut scanner, temp_dir.clone());

        let bucket_dir = temp_dir.join("bucket");
        tokio::fs::create_dir_all(bucket_dir.join("child"))
            .await
            .expect("failed to create child directory");

        scanner.old_cache.info.name = "bucket".to_string();
        scanner.new_cache.info.name = "bucket".to_string();
        scanner.update_cache.info.name = "bucket".to_string();
        scanner.last_update = SystemTime::UNIX_EPOCH;

        let (tx, mut rx) = mpsc::channel(1);
        scanner.updates = Some(tx);

        let folder = CachedFolder {
            name: "bucket".to_string(),
            parent: None,
            object_heal_prob_div: 1,
        };
        let mut into = DataUsageEntry {
            compacted: true,
            ..Default::default()
        };

        scanner
            .scan_folder(CancellationToken::new(), folder, &mut into)
            .await
            .expect("compacted scan should finish successfully");

        let update = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("compacted scan should send a partial update")
            .expect("partial update channel should remain open");

        assert!(update.compacted, "partial update should preserve compacted state");
    }

    #[tokio::test]
    #[serial]
    async fn test_scan_data_folder_returns_partial_cache_on_budget_cancel() {
        let (mut scanner, temp_dir) = build_test_scanner().await;
        let _guard = TestGuard::new(60, 100, &mut scanner, temp_dir.clone());

        let bucket_dir = temp_dir.join("bucket");
        tokio::fs::create_dir_all(bucket_dir.join("child-a"))
            .await
            .expect("failed to create first child directory");
        tokio::fs::create_dir_all(bucket_dir.join("child-b"))
            .await
            .expect("failed to create second child directory");

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            crate::scanner_budget::ScannerCycleBudgetConfig {
                max_directories: Some(2),
                ..Default::default()
            },
        );
        let cache = DataUsageCache {
            info: crate::data_usage_define::DataUsageCacheInfo {
                name: "bucket".to_string(),
                next_cycle: 7,
                ..Default::default()
            },
            ..Default::default()
        };

        let result = scan_data_folder(
            budget.token(),
            budget.clone(),
            vec![scanner.local_disk.clone()],
            scanner.local_disk.clone(),
            cache,
            None,
            HealScanMode::Normal,
            SCANNER_SLEEPER.clone(),
        )
        .await;

        let partial_cache = match result {
            Err(ScannerError::PartialCache(partial_cache)) => partial_cache,
            other => panic!("expected partial cache after directory budget cancellation, got {other:?}"),
        };

        assert!(partial_cache.info.last_update.is_some());
        assert_eq!(partial_cache.info.next_cycle, 7);
        assert!(partial_cache.root().is_some(), "partial cache should keep completed scan progress");
        assert!(budget.budget_elapsed());
        assert_eq!(budget.reason(), Some(crate::scanner_budget::ScannerCycleBudgetReason::Directories));
    }

    #[tokio::test]
    #[serial]
    async fn test_scan_data_folder_reports_invalid_checkpoint_ignored_once() {
        let (scanner, temp_dir) = build_test_scanner().await;
        let _guard = TestGuard {
            temp_dir: Some(temp_dir.clone()),
        };

        tokio::fs::create_dir_all(temp_dir.join("bucket").join("child-a").join("grandchild"))
            .await
            .expect("failed to create nested child directory");

        let before = global_metrics().report().await.scan_checkpoint_ignored;
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&parent, Default::default());
        let cache = DataUsageCache {
            info: crate::data_usage_define::DataUsageCacheInfo {
                name: "bucket".to_string(),
                scan_checkpoint: Some(crate::data_usage_define::DataUsageScanCheckpoint {
                    version: crate::data_usage_define::DATA_USAGE_SCAN_CHECKPOINT_VERSION + 1,
                    resume_after: "bucket/child-a".to_string(),
                    reason: crate::data_usage_define::DataUsageScanCheckpointReason::Unknown,
                }),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = scan_data_folder(
            budget.token(),
            budget.clone(),
            vec![scanner.local_disk.clone()],
            scanner.local_disk.clone(),
            cache,
            None,
            HealScanMode::Normal,
            SCANNER_SLEEPER.clone(),
        )
        .await;

        assert!(result.is_ok(), "scan should complete with an ignored checkpoint");
        let after = global_metrics().report().await.scan_checkpoint_ignored;
        assert_eq!(after.saturating_sub(before), 1);
    }

    #[tokio::test]
    #[serial]
    async fn test_scan_data_folder_resume_hint_prioritizes_next_existing_folder() {
        let (scanner, temp_dir) = build_test_scanner().await;
        let _guard = TestGuard {
            temp_dir: Some(temp_dir.clone()),
        };

        let bucket_dir = temp_dir.join("bucket");
        for child in ["child-a", "child-b", "child-c"] {
            tokio::fs::create_dir_all(bucket_dir.join(child))
                .await
                .expect("failed to create child directory");
        }

        let root_hash = hash_path("bucket");
        let mut cache = DataUsageCache {
            info: crate::data_usage_define::DataUsageCacheInfo {
                name: "bucket".to_string(),
                next_cycle: 9,
                scan_resume_after: Some("bucket/child-a".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        cache.replace_hashed(&root_hash, &None, &DataUsageEntry::default());
        for child in ["child-a", "child-b", "child-c"] {
            cache.replace_hashed(
                &hash_path(&format!("bucket/{child}")),
                &Some(root_hash.clone()),
                &DataUsageEntry::default(),
            );
        }

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            crate::scanner_budget::ScannerCycleBudgetConfig {
                max_directories: Some(2),
                ..Default::default()
            },
        );

        let result = scan_data_folder(
            budget.token(),
            budget.clone(),
            vec![scanner.local_disk.clone()],
            scanner.local_disk.clone(),
            cache,
            None,
            HealScanMode::Normal,
            SCANNER_SLEEPER.clone(),
        )
        .await;

        let partial_cache = match result {
            Err(ScannerError::PartialCache(partial_cache)) => partial_cache,
            other => panic!("expected partial cache after directory budget cancellation, got {other:?}"),
        };

        assert_eq!(partial_cache.info.scan_resume_after.as_deref(), Some("bucket/child-b"));
        let checkpoint = partial_cache.info.scan_checkpoint.as_ref().expect("partial scan checkpoint");
        assert_eq!(checkpoint.version, crate::data_usage_define::DATA_USAGE_SCAN_CHECKPOINT_VERSION);
        assert_eq!(checkpoint.resume_after, "bucket/child-b");
        assert_eq!(checkpoint.reason, crate::data_usage_define::DataUsageScanCheckpointReason::Directories);
        assert!(
            partial_cache
                .root()
                .is_some_and(|root| root.children.contains(&hash_path("bucket/child-b").key()))
        );
        assert_eq!(partial_cache.info.next_cycle, 9);
        assert_eq!(budget.reason(), Some(crate::scanner_budget::ScannerCycleBudgetReason::Directories));
    }

    #[tokio::test]
    #[serial]
    async fn test_scan_data_folder_resume_hint_orders_across_new_and_existing_folders() {
        let (scanner, temp_dir) = build_test_scanner().await;
        let _guard = TestGuard {
            temp_dir: Some(temp_dir.clone()),
        };

        let bucket_dir = temp_dir.join("bucket");
        for child in ["child-a", "child-b", "child-c", "child-d"] {
            tokio::fs::create_dir_all(bucket_dir.join(child))
                .await
                .expect("failed to create child directory");
        }

        let root_hash = hash_path("bucket");
        let mut cache = DataUsageCache {
            info: crate::data_usage_define::DataUsageCacheInfo {
                name: "bucket".to_string(),
                next_cycle: 9,
                scan_resume_after: Some("bucket/child-b".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        cache.replace_hashed(&root_hash, &None, &DataUsageEntry::default());
        for child in ["child-b", "child-c"] {
            cache.replace_hashed(
                &hash_path(&format!("bucket/{child}")),
                &Some(root_hash.clone()),
                &DataUsageEntry::default(),
            );
        }

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            crate::scanner_budget::ScannerCycleBudgetConfig {
                max_directories: Some(2),
                ..Default::default()
            },
        );

        let result = scan_data_folder(
            budget.token(),
            budget.clone(),
            vec![scanner.local_disk.clone()],
            scanner.local_disk.clone(),
            cache,
            None,
            HealScanMode::Normal,
            SCANNER_SLEEPER.clone(),
        )
        .await;

        let partial_cache = match result {
            Err(ScannerError::PartialCache(partial_cache)) => partial_cache,
            other => panic!("expected partial cache after directory budget cancellation, got {other:?}"),
        };

        assert_eq!(partial_cache.info.scan_resume_after.as_deref(), Some("bucket/child-c"));
        assert!(
            partial_cache
                .root()
                .is_some_and(|root| root.children.contains(&hash_path("bucket/child-c").key()))
        );
        assert_eq!(budget.reason(), Some(crate::scanner_budget::ScannerCycleBudgetReason::Directories));
    }

    #[tokio::test]
    #[serial]
    async fn test_scan_data_folder_success_clears_resume_hint() {
        let (scanner, temp_dir) = build_test_scanner().await;
        let _guard = TestGuard {
            temp_dir: Some(temp_dir.clone()),
        };

        tokio::fs::create_dir_all(temp_dir.join("bucket").join("child-a"))
            .await
            .expect("failed to create child directory");

        let cache = DataUsageCache {
            info: crate::data_usage_define::DataUsageCacheInfo {
                name: "bucket".to_string(),
                next_cycle: 11,
                scan_resume_after: Some("bucket/child-a".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&parent, Default::default());

        let result = scan_data_folder(
            budget.token(),
            budget,
            vec![scanner.local_disk.clone()],
            scanner.local_disk.clone(),
            cache,
            None,
            HealScanMode::Normal,
            SCANNER_SLEEPER.clone(),
        )
        .await
        .expect("scan should complete successfully");

        assert!(result.info.scan_resume_after.is_none());
        assert!(result.info.scan_checkpoint.is_none());
        assert_eq!(result.info.next_cycle, 11);
    }

    #[tokio::test]
    #[serial]
    #[cfg(unix)]
    async fn test_scan_folder_ignores_symlinked_child_directory() {
        let (mut scanner, temp_dir) = build_test_scanner().await;
        let _guard = TestGuard::new(60, 0, &mut scanner, temp_dir.clone());

        let bucket_dir = temp_dir.join("bucket");
        let target_dir = bucket_dir.join("target");
        let link_dir = bucket_dir.join("link");

        std::fs::create_dir_all(&target_dir).expect("failed to create target dir");
        symlink(&target_dir, &link_dir).expect("failed to create symlinked dir");

        scanner.old_cache.info.name = "bucket".to_string();
        scanner.new_cache.info.name = "bucket".to_string();
        scanner.update_cache.info.name = "bucket".to_string();

        let folder = CachedFolder {
            name: "bucket".to_string(),
            parent: None,
            object_heal_prob_div: 1,
        };

        let mut into = DataUsageEntry::default();
        let result = scanner.scan_folder(CancellationToken::new(), folder, &mut into).await;

        assert!(result.is_ok(), "expected symlinked child directory to be ignored");
        assert_eq!(into.failed_objects, 0, "expected ignored symlink not to count as a failed object");
    }

    #[test]
    fn test_should_log_failed_object_samples_after_initial_limit() {
        assert!(should_log_failed_object(1));
        assert!(should_log_failed_object(SCANNER_FAILED_OBJECT_LOG_INITIAL_LIMIT));
        assert!(!should_log_failed_object(SCANNER_FAILED_OBJECT_LOG_INITIAL_LIMIT + 1));
        assert!(should_log_failed_object(SCANNER_FAILED_OBJECT_LOG_EVERY));
        assert!(!should_log_failed_object(SCANNER_FAILED_OBJECT_LOG_EVERY + 1));
        assert!(should_log_failed_object(SCANNER_FAILED_OBJECT_LOG_EVERY * 2));
    }
}
