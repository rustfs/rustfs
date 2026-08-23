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

use crate::heal::{
    progress::{HealProgress, HealStatistics},
    resume::{ReplacementPhase, ResumeGc, ResumeManager, ResumeState, ResumeUtils},
    storage::HealStorageAPI,
    task::{HealOptions, HealPriority, HealRequest, HealTask, HealTaskStatus, HealType, demote_to_debug_when},
};
use crate::{Error, Result};
use metrics::{counter, gauge};
use rustfs_common::heal_channel::{HealAdmissionDropReason, HealAdmissionReceipt, HealAdmissionResult, HealRequestSource};
use rustfs_concurrency::{AdmissionState, WorkloadAdmissionSnapshotProvider, WorkloadClass};
use rustfs_madmin::heal_commands::HealResultItem;
#[cfg(test)]
use std::sync::LazyLock;
use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard},
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{Mutex, Notify, RwLock},
    time::{interval, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::{DiskError, Endpoint, HealDiskExt as _, local_disk_map_read};

const KEEP_HEAL_TASK_STATUS_DURATION: Duration = Duration::from_secs(10 * 60);
const DISPLACED_HEAL_REASON: &str = "reason=displaced; retry_hint=submit_again";
const LOG_COMPONENT_HEAL: &str = "heal";
const LOG_SUBSYSTEM_DISK_SCANNER: &str = "disk_scanner";
const LOG_SUBSYSTEM_MANAGER: &str = "manager";
const EVENT_HEAL_AUTO_SCAN_STATE: &str = "heal_auto_scan_state";
const EVENT_HEAL_AUTO_SCAN_DISK: &str = "heal_auto_scan_disk";
const EVENT_HEAL_AUTO_SCAN_ENQUEUE: &str = "heal_auto_scan_enqueue";
const EVENT_HEAL_MANAGER_STATE: &str = "heal_manager_state";
const EVENT_HEAL_QUEUE_ADMISSION: &str = "heal_queue_admission";
const EVENT_HEAL_MAINLINE_THROTTLE: &str = "heal_mainline_throttle";
const EVENT_HEAL_SCHEDULER_STATE: &str = "heal_scheduler_state";
const EVENT_HEAL_QUEUE_STATE: &str = "heal_queue_state";
const EVENT_HEAL_UNCLEAN_SHUTDOWN: &str = "heal_unclean_shutdown";
const EVENT_HEAL_RESUME_GC: &str = "heal_resume_gc";
const LEGACY_ROOT_HEAL_PATH: &str = ".";
const MAX_RECOVERABLE_HEAL_RETRIES: u32 = 3;
const MAX_RECOVERABLE_HEAL_RETRY_DELAY: Duration = Duration::from_secs(30);
const RESUME_GC_INTERVAL: Duration = Duration::from_secs(60 * 60);

// Admission/scheduler outcomes for per-object requests (Object/Metadata/
// ECDecode) log via demote_to_debug_when! — MRF, autoheal, and scanner
// recovery loops submit those per object, so a full queue or a retry storm
// would otherwise emit one warn! per object (rustfs/rustfs#5716). The
// `rustfs_heal_admission_total` metric and the `heal_queue_state` backlog
// event keep the aggregate signal at operator-visible levels.

#[cfg(test)]
struct RetryOwnershipTestHook {
    task_id: String,
    active_to_retrying_reached: Notify,
    active_to_retrying_release: Notify,
    retrying_to_queue_reached: Notify,
    retrying_to_queue_release: Notify,
}

#[cfg(test)]
static RETRY_OWNERSHIP_TEST_HOOK: LazyLock<Mutex<Option<Arc<RetryOwnershipTestHook>>>> = LazyLock::new(|| Mutex::new(None));

#[cfg(test)]
struct DuplicateAdmissionTestHook {
    request_id: String,
    active_lock_reached: Notify,
    active_lock_release: Notify,
}

#[cfg(test)]
static DUPLICATE_ADMISSION_TEST_HOOK: LazyLock<Mutex<Option<Arc<DuplicateAdmissionTestHook>>>> =
    LazyLock::new(|| Mutex::new(None));

#[cfg(test)]
async fn pause_retry_ownership_transition(task_id: &str, to_queue: bool) {
    let hook = RETRY_OWNERSHIP_TEST_HOOK.lock().await.clone();
    let Some(hook) = hook.filter(|hook| hook.task_id == task_id) else {
        return;
    };
    if to_queue {
        hook.retrying_to_queue_reached.notify_one();
        hook.retrying_to_queue_release.notified().await;
    } else {
        hook.active_to_retrying_reached.notify_one();
        hook.active_to_retrying_release.notified().await;
    }
}

#[cfg(test)]
async fn pause_duplicate_admission_after_active_lock(request_id: &str) {
    let hook = DUPLICATE_ADMISSION_TEST_HOOK.lock().await.clone();
    let Some(hook) = hook.filter(|hook| hook.request_id == request_id) else {
        return;
    };
    hook.active_lock_reached.notify_one();
    hook.active_lock_release.notified().await;
}

type WorkloadSnapshotProviderRef = Arc<dyn WorkloadAdmissionSnapshotProvider + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct MrfRepairNoticeTarget {
    bucket: Arc<str>,
    object: Arc<str>,
    version_id: Option<[u8; 16]>,
    kind: rustfs_common::mrf_channel::MrfKind,
    scope: Option<rustfs_common::mrf_channel::MrfScope>,
    lease: Option<rustfs_common::mrf_channel::MrfIngressLease>,
}

#[derive(Debug, Clone)]
struct HealAdmissionDecision {
    result: HealAdmissionResult,
    displaced_request: Option<HealRequest>,
}

impl HealAdmissionDecision {
    const fn new(result: HealAdmissionResult) -> Self {
        Self {
            result,
            displaced_request: None,
        }
    }

    fn accepted_with_displacement(displaced_request: HealRequest) -> Self {
        Self {
            result: HealAdmissionResult::Accepted,
            displaced_request: Some(displaced_request),
        }
    }

    fn displaced_task_id(&self) -> Option<&str> {
        self.displaced_request.as_ref().map(|request| request.id.as_str())
    }
}

fn lock_mrf_repair_notice_targets(
    registry: &StdMutex<HashMap<String, Vec<MrfRepairNoticeTarget>>>,
) -> StdMutexGuard<'_, HashMap<String, Vec<MrfRepairNoticeTarget>>> {
    match registry.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn lock_displaced_terminals(
    registry: &StdMutex<HashMap<String, Arc<CompletedHealStatus>>>,
) -> StdMutexGuard<'_, HashMap<String, Arc<CompletedHealStatus>>> {
    match registry.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn record_displaced_terminal(
    registry: &StdMutex<HashMap<String, Arc<CompletedHealStatus>>>,
    request: &HealRequest,
) -> Arc<CompletedHealStatus> {
    let terminal = Arc::new(CompletedHealStatus {
        heal_type: request.heal_type.clone(),
        status: HealTaskStatus::Failed {
            error: format!("heal task displaced by a higher-priority request ({DISPLACED_HEAL_REASON})"),
        },
        result_items_truncated: false,
        completed_at: SystemTime::now(),
        seqed_items: Vec::new(),
        next_seq: 0,
        min_seq: 0,
    });
    let mut terminals = lock_displaced_terminals(registry);
    prune_completed_heal_statuses(&mut terminals);
    terminals.insert(request.id.clone(), Arc::clone(&terminal));
    terminal
}

async fn remove_displaced_task_aliases(
    aliases: &Arc<Mutex<HashMap<String, HealTaskAlias>>>,
    terminals: &StdMutex<HashMap<String, Arc<CompletedHealStatus>>>,
    task_id: &str,
    terminal: &Arc<CompletedHealStatus>,
) {
    let mut aliases = aliases.lock().await;
    let alias_ids = aliases
        .iter()
        .filter_map(|(alias_id, alias)| (alias.task_id == task_id).then_some(alias_id.clone()))
        .collect::<Vec<_>>();
    let mut displaced_terminals = lock_displaced_terminals(terminals);
    prune_completed_heal_statuses(&mut displaced_terminals);
    for alias_id in alias_ids {
        displaced_terminals.insert(alias_id, Arc::clone(terminal));
    }
    aliases.retain(|alias_id, alias| alias_id != task_id && alias.task_id != task_id);
}

async fn remove_task_aliases_for_task(registry: &Arc<Mutex<HashMap<String, HealTaskAlias>>>, task_id: &str) {
    registry
        .lock()
        .await
        .retain(|alias_id, alias| alias_id != task_id && alias.task_id != task_id);
}

#[derive(Debug, Clone)]
pub struct HealTaskReport {
    pub status: HealTaskStatus,
    pub result_items: Vec<HealResultItem>,
    pub result_items_truncated: bool,
    pub progress: Option<HealProgress>,
    /// Cursor for incremental consumption: sequence number of the next item
    /// to be produced. `0` on reports from sources without sequencing.
    pub next_seq: u64,
    /// Oldest sequence still retained (`0` together with `next_seq` when
    /// sequencing is unavailable).
    pub min_seq: u64,
}

/// Report from a live task, honoring the client's incremental cursor.
async fn active_task_report(task: &HealTask, since: Option<u64>) -> HealTaskReport {
    let window = task.get_result_items_since(since).await;
    HealTaskReport {
        status: task.get_status().await,
        result_items: window.items,
        // The legacy flag stays set once anything was evicted; a lagging
        // incremental cursor additionally marks this response truncated so
        // the client knows to restart from `min_seq`.
        result_items_truncated: task.result_items_truncated() || window.lagged,
        progress: Some(task.get_progress().await),
        next_seq: window.next_seq,
        min_seq: window.min_seq,
    }
}

fn empty_task_report(status: HealTaskStatus) -> HealTaskReport {
    HealTaskReport {
        status,
        result_items: Vec::new(),
        result_items_truncated: false,
        progress: None,
        next_seq: 0,
        min_seq: 0,
    }
}

fn completed_task_report(completed: &CompletedHealStatus, since: Option<u64>) -> HealTaskReport {
    let mut lagged = false;
    let result_items = match since {
        None => completed.seqed_items.iter().map(|(_, item)| item.clone()).collect(),
        Some(cursor) => {
            if cursor + 1 < completed.min_seq {
                lagged = true;
            }
            completed
                .seqed_items
                .iter()
                .filter(|(seq, _)| *seq > cursor)
                .map(|(_, item)| item.clone())
                .collect()
        }
    };
    HealTaskReport {
        status: completed.status.clone(),
        result_items,
        result_items_truncated: completed.result_items_truncated || lagged,
        progress: None,
        next_seq: completed.next_seq,
        min_seq: completed.min_seq,
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HealPriorityCounts {
    pub low: u64,
    pub normal: u64,
    pub high: u64,
    pub urgent: u64,
}

impl HealPriorityCounts {
    fn increment(&mut self, priority: HealPriority) {
        match priority {
            HealPriority::Low => self.low += 1,
            HealPriority::Normal => self.normal += 1,
            HealPriority::High => self.high += 1,
            HealPriority::Urgent => self.urgent += 1,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HealSourceCounts {
    pub scanner: u64,
    pub admin: u64,
    pub auto_heal: u64,
    pub internal: u64,
    pub read_repair: u64,
    #[serde(default)]
    pub mrf: u64,
}

impl HealSourceCounts {
    fn increment(&mut self, source: HealRequestSource) {
        match source {
            HealRequestSource::Scanner => self.scanner += 1,
            HealRequestSource::Admin => self.admin += 1,
            HealRequestSource::AutoHeal => self.auto_heal += 1,
            HealRequestSource::Internal => self.internal += 1,
            HealRequestSource::ReadRepair => self.read_repair += 1,
            HealRequestSource::Mrf => self.mrf += 1,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HealOperationsSnapshot {
    pub queue_length: u64,
    pub active_tasks: u64,
    pub retrying_tasks: u64,
    pub queued_by_priority: HealPriorityCounts,
    pub active_by_priority: HealPriorityCounts,
    pub retrying_by_priority: HealPriorityCounts,
    pub queued_by_source: HealSourceCounts,
    pub active_by_source: HealSourceCounts,
    pub retrying_by_source: HealSourceCounts,
}

fn usize_to_u64_saturated(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn heal_type_matches_path(heal_type: &HealType, heal_path: &str) -> bool {
    let heal_path = heal_path.trim_matches('/');
    if heal_path.is_empty() || heal_path == LEGACY_ROOT_HEAL_PATH {
        return matches!(heal_type, HealType::Cluster);
    }

    match heal_type {
        HealType::Cluster => false,
        HealType::Object { bucket, object, .. }
        | HealType::Metadata { bucket, object }
        | HealType::ECDecode { bucket, object, .. } => heal_path_matches_bucket_child(heal_path, bucket, object),
        HealType::Bucket { bucket } => heal_path == bucket,
        HealType::Prefix { bucket, prefix } => heal_path_matches_bucket_child(heal_path, bucket, prefix),
        HealType::ErasureSet { set_disk_id, .. } => heal_path == set_disk_id,
    }
}

fn heal_path_matches_bucket_child(heal_path: &str, bucket: &str, child: &str) -> bool {
    heal_path == bucket || heal_path == format!("{bucket}/{child}").trim_matches('/')
}

fn publish_active_heal_count(active_heals: &HashMap<String, Arc<HealTask>>) {
    crate::set_heal_active_tasks(active_heals.len());
}

fn publish_heal_queue_length(queue: &PriorityHealQueue) {
    crate::set_heal_queue_length(queue.len());
}

fn active_heal_for_dedup_key(active_heals: &HashMap<String, Arc<HealTask>>, key: &str) -> Option<(String, HealType)> {
    active_heals
        .iter()
        .find(|(_, task)| PriorityHealQueue::make_dedup_key_for_type(&task.heal_type) == key)
        .map(|(task_id, task)| (task_id.clone(), task.heal_type.clone()))
}

fn retrying_heal_for_dedup_key(retrying_heals: &HashMap<String, RetryingHeal>, key: &str) -> Option<(String, HealType)> {
    retrying_heals
        .iter()
        .find(|(_, retrying)| PriorityHealQueue::make_dedup_key(&retrying.request) == key)
        .map(|(task_id, retrying)| (task_id.clone(), retrying.request.heal_type.clone()))
}

fn completed_status_is_retrying(status: &HealTaskStatus) -> bool {
    matches!(status, HealTaskStatus::Retrying { .. })
}

fn retry_budget_for_result(task: &HealTask, result: &Result<()>) -> Option<(Duration, String)> {
    let Err(err) = result else {
        return None;
    };
    if task.retry_attempts >= MAX_RECOVERABLE_HEAL_RETRIES {
        return None;
    }
    if task.has_batch_failure() {
        return None;
    }

    let error = err.to_string();
    if !err.is_recoverable_heal() {
        return None;
    }

    let retry_attempt = task.retry_attempts.saturating_add(1);
    let delay = recoverable_heal_retry_delay(retry_attempt);
    Some((delay, error))
}

#[cfg(test)]
fn retry_request_for_result(task: &HealTask, result: &Result<()>) -> Option<(HealRequest, Duration, String)> {
    let (delay, error) = retry_budget_for_result(task, result)?;
    Some((task.retry_request(), delay, error))
}

async fn retry_request_for_result_with_budget(task: &HealTask, result: &Result<()>) -> Option<(HealRequest, Duration, String)> {
    let (delay, error) = retry_budget_for_result(task, result)?;
    let request = match task.retry_request_with_remaining_timeout().await {
        Ok(request) => request,
        Err(err) => {
            debug!(
                target: "rustfs::heal::manager",
                event = EVENT_HEAL_QUEUE_ADMISSION,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_MANAGER,
                task_id = %task.id,
                error = %err,
                result = "retry_budget_exhausted",
                "Heal retry admission decided"
            );
            return None;
        }
    };
    Some((request, delay, error))
}

fn recoverable_heal_retry_delay(retry_attempt: u32) -> Duration {
    let retry_attempt = retry_attempt.clamp(1, 5);
    let delay = Duration::from_secs(2_u64.saturating_pow(retry_attempt));
    delay.min(MAX_RECOVERABLE_HEAL_RETRY_DELAY)
}

/// Heal config
/// HS-06 admin overlap policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HealOverlapPolicy {
    /// Default: overlapping admin starts merge into the existing task
    /// (today's dedup semantics).
    #[default]
    Merge,
    /// Return a typed already-running / overlapping-paths rejection like
    /// madmin's ErrHealAlreadyRunning / ErrHealOverlappingPaths.
    MinioError,
}

/// Path view of a heal type for overlap comparison: a bucket plus a
/// prefix/object path inside it (`None` bucket = cluster-wide, overlaps
/// everything).
fn heal_type_path_view(heal_type: &HealType) -> (Option<&str>, &str) {
    match heal_type {
        HealType::Cluster => (None, ""),
        HealType::Bucket { bucket } => (Some(bucket), ""),
        HealType::Prefix { bucket, prefix } => (Some(bucket), prefix),
        HealType::Object { bucket, object, .. }
        | HealType::Metadata { bucket, object }
        | HealType::ECDecode { bucket, object, .. } => (Some(bucket), object),
        // Erasure-set heal: the set id is the overlap dimension.
        HealType::ErasureSet { set_disk_id, .. } => (Some("\u{0}set"), set_disk_id),
    }
}

/// How two heal paths relate for the admin overlap check (HS-06).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OverlapVerdict {
    /// Distinct targets: no conflict.
    Disjoint,
    /// Same target: an identical heal is already in flight.
    SameTarget,
    /// One target contains the other.
    Overlapping,
}

fn prefix_paths_overlap(a: &str, b: &str) -> OverlapVerdict {
    if a == b {
        return OverlapVerdict::SameTarget;
    }
    if a.is_empty() || b.is_empty() || a.starts_with(b) || b.starts_with(a) {
        return OverlapVerdict::Overlapping;
    }
    OverlapVerdict::Disjoint
}

fn heal_types_overlap(left: &HealType, right: &HealType) -> OverlapVerdict {
    let (left_bucket, left_path) = heal_type_path_view(left);
    let (right_bucket, right_path) = heal_type_path_view(right);
    match (left_bucket, right_bucket) {
        // Cluster-wide overlaps everything (but an exact cluster match is
        // SameTarget).
        (None, _) | (_, None) => {
            if matches!(left, HealType::Cluster) && matches!(right, HealType::Cluster) {
                OverlapVerdict::SameTarget
            } else {
                OverlapVerdict::Overlapping
            }
        }
        (Some(lb), Some(rb)) => {
            if lb != rb {
                return OverlapVerdict::Disjoint;
            }
            prefix_paths_overlap(left_path, right_path)
        }
    }
}

#[derive(Debug, Clone)]
pub struct HealConfig {
    /// Whether to enable auto heal
    pub enable_auto_heal: bool,
    /// Heal interval
    pub heal_interval: Duration,
    /// Maximum concurrent heal tasks
    pub max_concurrent_heals: usize,
    /// Maximum concurrent heal tasks allowed for a single erasure set
    pub max_concurrent_per_set: usize,
    /// Aggregate task execution timeout across recoverable retries
    pub task_timeout: Duration,
    /// Queue size
    pub queue_size: usize,
    /// Whether duplicate low-priority requests should merge into an existing queued request.
    pub low_priority_merge_enable: bool,
    /// Whether low-priority requests may be dropped when the queue is full.
    pub low_priority_drop_when_full: bool,
    /// Whether notify-driven scheduler wakeups are enabled.
    pub event_driven_scheduler_enable: bool,
    /// How admin heal starts behave on path overlap (HS-06): merge into the
    /// existing task (default) or return a typed already-running rejection.
    pub overlap_policy: HealOverlapPolicy,
    /// Whether per-set bulkhead scheduling is enabled.
    pub set_bulkhead_enable: bool,
    /// Whether erasure-set page parallelism is enabled.
    pub page_parallel_enable: bool,
    /// Whether foreground read pressure can delay best-effort heal task starts.
    pub mainline_throttle_enable: bool,
    /// Foreground read permit utilization percentage that delays best-effort heal starts.
    pub mainline_read_utilization_high_percent: usize,
    /// Foreground write utilization percentage that delays best-effort heal starts.
    pub mainline_write_utilization_high_percent: usize,
    /// Delay before rechecking foreground pressure after delaying heal starts.
    pub mainline_max_sleep: Duration,
}

impl Default for HealConfig {
    fn default() -> Self {
        let queue_size: usize =
            rustfs_utils::get_env_usize(rustfs_config::ENV_HEAL_QUEUE_SIZE, rustfs_config::DEFAULT_HEAL_QUEUE_SIZE);
        let heal_interval = Duration::from_secs(rustfs_utils::get_env_u64(
            rustfs_config::ENV_HEAL_INTERVAL_SECS,
            rustfs_config::DEFAULT_HEAL_INTERVAL_SECS,
        ));
        let enable_auto_heal =
            rustfs_utils::get_env_bool(rustfs_config::ENV_HEAL_AUTO_HEAL_ENABLE, rustfs_config::DEFAULT_HEAL_AUTO_HEAL_ENABLE);
        let task_timeout = Duration::from_secs(rustfs_utils::get_env_u64(
            rustfs_config::ENV_HEAL_TASK_TIMEOUT_SECS,
            rustfs_config::DEFAULT_HEAL_TASK_TIMEOUT_SECS,
        ));
        let max_concurrent_heals = rustfs_utils::get_env_usize(
            rustfs_config::ENV_HEAL_MAX_CONCURRENT_HEALS,
            rustfs_config::DEFAULT_HEAL_MAX_CONCURRENT_HEALS,
        );
        let max_concurrent_per_set = rustfs_utils::get_env_usize(
            rustfs_config::ENV_HEAL_MAX_CONCURRENT_PER_SET,
            rustfs_config::DEFAULT_HEAL_MAX_CONCURRENT_PER_SET,
        );
        let low_priority_merge_enable = rustfs_utils::get_env_bool(
            rustfs_config::ENV_HEAL_LOW_PRIORITY_MERGE_ENABLE,
            rustfs_config::DEFAULT_HEAL_LOW_PRIORITY_MERGE_ENABLE,
        );
        let low_priority_drop_when_full = rustfs_utils::get_env_bool(
            rustfs_config::ENV_HEAL_LOW_PRIORITY_DROP_WHEN_FULL,
            rustfs_config::DEFAULT_HEAL_LOW_PRIORITY_DROP_WHEN_FULL,
        );
        let event_driven_scheduler_enable = rustfs_utils::get_env_bool(
            rustfs_config::ENV_HEAL_EVENT_DRIVEN_SCHEDULER_ENABLE,
            rustfs_config::DEFAULT_HEAL_EVENT_DRIVEN_SCHEDULER_ENABLE,
        );
        let overlap_policy =
            match rustfs_utils::get_env_str(rustfs_config::ENV_HEAL_OVERLAP_POLICY, rustfs_config::DEFAULT_HEAL_OVERLAP_POLICY)
                .to_lowercase()
                .as_str()
            {
                "minio_error" => HealOverlapPolicy::MinioError,
                _ => HealOverlapPolicy::Merge,
            };
        let set_bulkhead_enable = rustfs_utils::get_env_bool(
            rustfs_config::ENV_HEAL_SET_BULKHEAD_ENABLE,
            rustfs_config::DEFAULT_HEAL_SET_BULKHEAD_ENABLE,
        );
        let page_parallel_enable = rustfs_utils::get_env_bool(
            rustfs_config::ENV_HEAL_PAGE_PARALLEL_ENABLE,
            rustfs_config::DEFAULT_HEAL_PAGE_PARALLEL_ENABLE,
        );
        let mainline_throttle_enable = rustfs_utils::get_env_bool(
            rustfs_config::ENV_HEAL_MAINLINE_THROTTLE_ENABLE,
            rustfs_config::DEFAULT_HEAL_MAINLINE_THROTTLE_ENABLE,
        );
        let mainline_read_utilization_high_percent = rustfs_utils::get_env_usize(
            rustfs_config::ENV_HEAL_MAINLINE_READ_UTILIZATION_HIGH_PERCENT,
            rustfs_config::DEFAULT_HEAL_MAINLINE_READ_UTILIZATION_HIGH_PERCENT,
        )
        .min(100);
        let mainline_write_utilization_high_percent = rustfs_utils::get_env_usize(
            rustfs_config::ENV_HEAL_MAINLINE_WRITE_UTILIZATION_HIGH_PERCENT,
            rustfs_config::DEFAULT_HEAL_MAINLINE_WRITE_UTILIZATION_HIGH_PERCENT,
        )
        .min(100);
        let mainline_max_sleep = Duration::from_millis(rustfs_utils::get_env_u64(
            rustfs_config::ENV_HEAL_MAINLINE_MAX_SLEEP_MS,
            rustfs_config::DEFAULT_HEAL_MAINLINE_MAX_SLEEP_MS,
        ));
        Self {
            enable_auto_heal,
            heal_interval,        // 10 seconds
            max_concurrent_heals, // max 4,
            max_concurrent_per_set: std::cmp::min(max_concurrent_heals.max(1), max_concurrent_per_set.max(1)),
            task_timeout, // 5 minutes
            queue_size,
            low_priority_merge_enable,
            low_priority_drop_when_full,
            event_driven_scheduler_enable,
            overlap_policy,
            set_bulkhead_enable,
            page_parallel_enable,
            mainline_throttle_enable,
            mainline_read_utilization_high_percent,
            mainline_write_utilization_high_percent,
            mainline_max_sleep,
        }
    }
}

/// Heal state
#[derive(Debug, Default)]
pub struct HealState {
    /// Whether running
    pub is_running: bool,
    /// Current heal cycle
    pub current_cycle: u64,
    /// Last heal time
    pub last_heal_time: Option<SystemTime>,
    /// Total healed objects
    pub total_healed_objects: u64,
    /// Total heal failures
    pub total_heal_failures: u64,
    /// Current active heal tasks
    pub active_heal_count: usize,
}

/// Heal manager
pub struct HealManager {
    /// Heal config
    config: Arc<RwLock<HealConfig>>,
    /// Heal state
    state: Arc<RwLock<HealState>>,
    /// Active heal tasks
    active_heals: Arc<Mutex<HashMap<String, Arc<HealTask>>>>,
    /// Heal queue (priority-based)
    heal_queue: Arc<Mutex<PriorityHealQueue>>,
    /// Recently completed heal statuses retained for status queries. Values
    /// are shared so the lookup helper can hand a completed entry to a
    /// caller without cloning the retained result window.
    completed_heals: Arc<Mutex<HashMap<String, Arc<CompletedHealStatus>>>>,
    /// Terminals for requests removed by priority displacement. An Accepted
    /// task ID remains queryable for the same process lifetime and the normal
    /// ten-minute status TTL; clients should treat `reason=displaced` as a
    /// terminal result and submit a fresh request. This sidecar is synchronous
    /// so admission can publish the terminal while the queue transition is
    /// still under its lock, without awaiting another tokio lock. Queue state
    /// is process-local, so this guarantee does not extend across restart.
    displaced_terminals: Arc<StdMutex<HashMap<String, Arc<CompletedHealStatus>>>>,
    /// Client tokens merged into an existing task id.
    task_aliases: Arc<Mutex<HashMap<String, HealTaskAlias>>>,
    /// Heal tasks waiting for a retry backoff to expire.
    retrying_heals: Arc<Mutex<HashMap<String, RetryingHeal>>>,
    /// MRF repaired-event targets keyed by canonical heal task id. Admission
    /// only registers ownership; the scheduler emits the event after a real
    /// successful completion.
    mrf_repair_notice_targets: Arc<StdMutex<HashMap<String, Vec<MrfRepairNoticeTarget>>>>,
    /// Surviving disks that hold durable replacement intents, keyed by task ID.
    /// This is rebuilt from durable state after restart and never crosses the
    /// public request boundary.
    replacement_recovery_anchors: Arc<std::sync::Mutex<HashMap<String, String>>>,
    /// Set IDs whose durable replacement metadata is corrupt or conflicting.
    replacement_recovery_blocked_sets: Arc<std::sync::Mutex<HashSet<String>>>,
    /// Storage layer interface
    storage: Arc<dyn HealStorageAPI>,
    /// Cancel token
    cancel_token: CancellationToken,
    /// Statistics
    statistics: Arc<RwLock<HealStatistics>>,
    /// Scheduler wake-up notifier for event-driven dispatch
    notify: Arc<Notify>,
    /// Optional runtime workload snapshot provider used to protect foreground data-plane work.
    workload_provider: Option<WorkloadSnapshotProviderRef>,
}

/// Where a task-id lookup resolved. The variants carry the resolved state
/// so both the status and the report adapters can consume one shared
/// cascade without re-locking.
enum TaskStateLookup {
    Active(Arc<HealTask>),
    Retrying(HealTaskStatus),
    Completed(Arc<CompletedHealStatus>),
    Queued,
    NotFound,
}

struct HealQueueContext<'a> {
    heal_queue: &'a Arc<Mutex<PriorityHealQueue>>,
    active_heals: &'a Arc<Mutex<HashMap<String, Arc<HealTask>>>>,
    completed_heals: &'a Arc<Mutex<HashMap<String, Arc<CompletedHealStatus>>>>,
    displaced_terminals: &'a Arc<StdMutex<HashMap<String, Arc<CompletedHealStatus>>>>,
    task_aliases: &'a Arc<Mutex<HashMap<String, HealTaskAlias>>>,
    retrying_heals: &'a Arc<Mutex<HashMap<String, RetryingHeal>>>,
    mrf_repair_notice_targets: &'a Arc<StdMutex<HashMap<String, Vec<MrfRepairNoticeTarget>>>>,
    replacement_recovery_anchors: &'a Arc<std::sync::Mutex<HashMap<String, String>>>,
    config: &'a Arc<RwLock<HealConfig>>,
    statistics: &'a Arc<RwLock<HealStatistics>>,
    storage: &'a Arc<dyn HealStorageAPI>,
    notify: &'a Arc<Notify>,
    cancel_token: &'a CancellationToken,
    workload_provider: &'a Option<WorkloadSnapshotProviderRef>,
}

impl HealManager {
    fn classify_full_admission(request: &HealRequest, config: &HealConfig) -> HealAdmissionResult {
        let best_effort_source = matches!(
            request.source,
            HealRequestSource::Scanner | HealRequestSource::AutoHeal | HealRequestSource::ReadRepair
        );
        if best_effort_source || (request.priority == HealPriority::Low && config.low_priority_drop_when_full) {
            HealAdmissionResult::Dropped(HealAdmissionDropReason::QueueFull)
        } else {
            HealAdmissionResult::Full
        }
    }

    fn queue_usage_pct(queue_len: usize, queue_capacity: usize) -> usize {
        queue_len.saturating_mul(100).checked_div(queue_capacity).unwrap_or(0)
    }

    fn classify_pressure_admission(
        request: &HealRequest,
        queue_len: usize,
        queue_capacity: usize,
    ) -> Option<HealAdmissionResult> {
        if request.force_start || queue_capacity == 0 {
            return None;
        }

        let queue_usage_pct = Self::queue_usage_pct(queue_len, queue_capacity);
        if queue_usage_pct < 80 {
            return None;
        }

        match request.source {
            HealRequestSource::ReadRepair => Some(HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped)),
            HealRequestSource::Scanner if request.priority == HealPriority::Low => {
                Some(HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped))
            }
            HealRequestSource::AutoHeal if queue_usage_pct >= 95 => {
                Some(HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped))
            }
            _ => None,
        }
    }

    fn duplicate_admission_for_request(request: &HealRequest, config: &HealConfig) -> HealAdmissionResult {
        if request.priority == HealPriority::Low && !config.low_priority_merge_enable {
            HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped)
        } else {
            HealAdmissionResult::Merged
        }
    }

    fn can_displace_queued_work(request: &HealRequest) -> bool {
        matches!(request.source, HealRequestSource::Admin | HealRequestSource::Internal)
    }

    fn request_bypasses_mainline_throttle(request: &HealRequest) -> bool {
        request.force_start
            || matches!(request.source, HealRequestSource::Admin | HealRequestSource::Internal)
            || matches!(request.priority, HealPriority::High | HealPriority::Urgent)
    }

    fn mainline_throttle_active(
        config: &HealConfig,
        provider: &Option<WorkloadSnapshotProviderRef>,
    ) -> Option<ForegroundPressure> {
        if !config.mainline_throttle_enable
            || (config.mainline_read_utilization_high_percent == 0 && config.mainline_write_utilization_high_percent == 0)
        {
            return None;
        }

        let provider = provider.as_ref()?;
        let snapshot = provider.workload_admission_snapshot();
        [
            (WorkloadClass::ForegroundRead, config.mainline_read_utilization_high_percent),
            (WorkloadClass::ForegroundWrite, config.mainline_write_utilization_high_percent),
        ]
        .into_iter()
        .filter_map(|(class, threshold_pct)| {
            if threshold_pct == 0 {
                return None;
            }

            let entry = snapshot.get(class)?;
            let usage_pct = if matches!(entry.state, AdmissionState::Saturated) {
                100
            } else {
                let limit = entry.limit?;
                if limit == 0 {
                    return None;
                }
                entry
                    .active
                    .unwrap_or(0)
                    .saturating_mul(100)
                    .checked_div(limit)
                    .unwrap_or(100)
            };

            (usage_pct >= threshold_pct).then_some(ForegroundPressure {
                class,
                usage_pct,
                threshold_pct,
            })
        })
        .max_by_key(|pressure| pressure.usage_pct)
    }

    fn schedule_mainline_throttle_recheck(notify: Arc<Notify>, delay: Duration) {
        if delay.is_zero() {
            notify.notify_one();
            return;
        }

        tokio::spawn(async move {
            sleep(delay).await;
            notify.notify_one();
        });
    }

    fn record_mainline_throttle_delay(pressure: ForegroundPressure, config: &HealConfig) {
        counter!(
            "rustfs_heal_mainline_throttle_total",
            "source" => "background",
            "result" => "delayed",
            "reason" => pressure.reason()
        )
        .increment(1);
        debug!(
            target: "rustfs::heal::manager",
            event = EVENT_HEAL_MAINLINE_THROTTLE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_MANAGER,
            state = "delayed",
            reason = pressure.reason(),
            workload_class = pressure.class.as_str(),
            foreground_usage_pct = pressure.usage_pct,
            threshold_pct = pressure.threshold_pct,
            recheck_delay_ms = config.mainline_max_sleep.as_millis(),
            "Heal scheduler delayed background work under foreground pressure"
        );
    }

    fn record_admission_metric(source: HealRequestSource, admission: HealAdmissionResult, context: &'static str) {
        counter!(
            "rustfs_heal_admission_total",
            "source" => source.as_str().to_string(),
            "result" => admission.result_label().to_string(),
            "reason" => admission.reason_label().to_string(),
            "context" => context.to_string()
        )
        .increment(1);
    }

    fn remove_mrf_repair_notice_targets_for_task(&self, task_id: &str) {
        let targets = lock_mrf_repair_notice_targets(&self.mrf_repair_notice_targets).remove(task_id);
        if let Some(targets) = targets {
            for target in targets {
                rustfs_common::mrf_channel::release_mrf_identity(
                    target.kind,
                    &target.bucket,
                    &target.object,
                    target.version_id,
                    target.scope,
                    target.lease,
                );
            }
        }
    }

    fn insert_mrf_repair_notice_target(
        registry: &mut HashMap<String, Vec<MrfRepairNoticeTarget>>,
        task_id: &str,
        target: MrfRepairNoticeTarget,
    ) {
        let task_targets = registry.entry(task_id.to_string()).or_default();
        if !task_targets.contains(&target) {
            task_targets.push(target);
        }
    }

    fn admit_request_to_queue(
        queue: &mut PriorityHealQueue,
        request: HealRequest,
        config: &HealConfig,
        context: &'static str,
    ) -> HealAdmissionDecision {
        let queue_len = queue.len();
        publish_heal_queue_length(queue);
        let queue_capacity = config.queue_size;
        let per_object_request = request.heal_type.is_per_object();

        if queue_len >= queue_capacity && !request.force_start {
            if Self::can_displace_queued_work(&request) && queue.can_displace_lower_priority(request.priority) {
                let request_id = request.id.clone();
                let priority = request.priority;
                let source = request.source;
                if let Some(displaced) = queue.push_displacing_lower_priority(request) {
                    publish_heal_queue_length(queue);
                    Self::record_admission_metric(source, HealAdmissionResult::Accepted, context);
                    demote_to_debug_when!(per_object_request, warn, target: "rustfs::heal::manager", {
                        event = EVENT_HEAL_QUEUE_ADMISSION,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_MANAGER,
                        request_id = %request_id,
                        priority = ?priority,
                        source = source.as_str(),
                        context,
                        displaced_request_id = %displaced.id,
                        displaced_priority = ?displaced.priority,
                        queue_len,
                        queue_capacity,
                        result = "accepted_by_displacement",
                        "Heal queue request accepted by displacement"
                    });
                    return HealAdmissionDecision::accepted_with_displacement(displaced);
                }

                demote_to_debug_when!(per_object_request, warn, target: "rustfs::heal::manager", {
                    event = EVENT_HEAL_QUEUE_ADMISSION,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_MANAGER,
                    request_id = %request_id,
                    priority = ?priority,
                    source = source.as_str(),
                    context,
                    queue_len,
                    queue_capacity,
                    result = "full_no_displacement_candidate",
                    "Heal queue request rejected without displacement"
                });
                Self::record_admission_metric(source, HealAdmissionResult::Full, context);
                return HealAdmissionDecision::new(HealAdmissionResult::Full);
            }

            let admission = Self::classify_full_admission(&request, config);
            Self::record_admission_metric(request.source, admission, context);
            match admission {
                HealAdmissionResult::Dropped(reason) => {
                    demote_to_debug_when!(per_object_request, warn, target: "rustfs::heal::manager", {
                        event = EVENT_HEAL_QUEUE_ADMISSION,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_MANAGER,
                        request_id = %request.id,
                        priority = ?request.priority,
                        source = request.source.as_str(),
                        context,
                        queue_len,
                        queue_capacity,
                        reason = reason.as_str(),
                        result = "dropped_full",
                        "Heal queue request dropped"
                    });
                }
                HealAdmissionResult::Full => {
                    demote_to_debug_when!(per_object_request, warn, target: "rustfs::heal::manager", {
                        event = EVENT_HEAL_QUEUE_ADMISSION,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_MANAGER,
                        request_id = %request.id,
                        priority = ?request.priority,
                        source = request.source.as_str(),
                        context,
                        queue_len,
                        queue_capacity,
                        result = "rejected_full",
                        "Heal queue request rejected"
                    });
                }
                HealAdmissionResult::Accepted | HealAdmissionResult::Merged => {}
            }
            return HealAdmissionDecision::new(admission);
        }

        if let Some(admission) = Self::classify_pressure_admission(&request, queue_len, queue_capacity) {
            Self::record_admission_metric(request.source, admission, context);
            if let HealAdmissionResult::Dropped(reason) = admission {
                debug!(
                    target: "rustfs::heal::manager",
                    event = EVENT_HEAL_QUEUE_ADMISSION,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_MANAGER,
                    request_id = %request.id,
                    priority = ?request.priority,
                    source = request.source.as_str(),
                    context,
                    queue_len,
                    queue_capacity,
                    queue_usage_pct = Self::queue_usage_pct(queue_len, queue_capacity),
                    reason = reason.as_str(),
                    result = "dropped_pressure",
                    "Heal queue request dropped under pressure"
                );
            }
            return HealAdmissionDecision::new(admission);
        }

        if queue_capacity > 0 {
            let capacity_threshold = queue_capacity.saturating_mul(80) / 100;
            if queue_len >= capacity_threshold {
                debug!(
                    target: "rustfs::heal::manager",
                    event = EVENT_HEAL_QUEUE_ADMISSION,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_MANAGER,
                    queue_len,
                    queue_capacity,
                    queue_usage_pct = Self::queue_usage_pct(queue_len, queue_capacity),
                    context,
                    result = "queue_pressure_high",
                    "Heal queue pressure high"
                );
            }
        }

        let request_id = request.id.clone();
        let priority = request.priority;
        let source = request.source;

        match queue.push(request) {
            QueuePushOutcome::Accepted => {
                publish_heal_queue_length(queue);
                Self::record_admission_metric(source, HealAdmissionResult::Accepted, context);
                if matches!(priority, HealPriority::High | HealPriority::Urgent)
                    && tracing::enabled!(target: "rustfs::heal::manager", tracing::Level::DEBUG)
                {
                    let stats = queue.get_priority_stats();
                    debug!(
                        target: "rustfs::heal::manager",
                        event = EVENT_HEAL_QUEUE_ADMISSION,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_MANAGER,
                        request_id = %request_id,
                        priority = ?priority,
                        source = source.as_str(),
                        context,
                        queue_len = queue_len + 1,
                        urgent = *stats.get(&HealPriority::Urgent).unwrap_or(&0),
                        high = *stats.get(&HealPriority::High).unwrap_or(&0),
                        normal = *stats.get(&HealPriority::Normal).unwrap_or(&0),
                        low = *stats.get(&HealPriority::Low).unwrap_or(&0),
                        result = "accepted",
                        "Heal queue snapshot recorded"
                    );
                }
                debug!(
                    target: "rustfs::heal::manager",
                    event = EVENT_HEAL_QUEUE_ADMISSION,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_MANAGER,
                    request_id = %request_id,
                    priority = ?priority,
                    source = source.as_str(),
                    context,
                    queue_len = queue_len + 1,
                    result = "accepted",
                    "Heal queue request accepted"
                );
                HealAdmissionDecision::new(HealAdmissionResult::Accepted)
            }
            QueuePushOutcome::Merged => {
                Self::record_admission_metric(source, HealAdmissionResult::Merged, context);
                debug!(
                    target: "rustfs::heal::manager",
                    event = EVENT_HEAL_QUEUE_ADMISSION,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_MANAGER,
                    request_id = %request_id,
                    priority = ?priority,
                    source = source.as_str(),
                    context,
                    queue_len,
                    result = "merged_duplicate",
                    "Heal queue request merged"
                );
                HealAdmissionDecision::new(HealAdmissionResult::Merged)
            }
        }
    }

    async fn insert_task_alias(&self, alias_id: &str, task_id: &str) {
        if alias_id == task_id {
            return;
        }

        self.task_aliases.lock().await.insert(
            alias_id.to_string(),
            HealTaskAlias {
                task_id: task_id.to_string(),
            },
        );
    }

    async fn canonical_task_id(&self, task_id: &str) -> String {
        self.task_aliases
            .lock()
            .await
            .get(task_id)
            .map(|alias| alias.task_id.clone())
            .unwrap_or_else(|| task_id.to_string())
    }

    async fn remove_aliases_for_task(&self, task_id: &str) {
        remove_task_aliases_for_task(&self.task_aliases, task_id).await;
    }

    fn block_replacement_recovery_set(&self, set_disk_id: &str) {
        self.replacement_recovery_blocked_sets
            .lock()
            .expect("replacement recovery blocked set lock poisoned")
            .insert(set_disk_id.to_string());
    }

    fn replacement_recovery_set_is_blocked(&self, set_disk_id: &str) -> bool {
        self.replacement_recovery_blocked_sets
            .lock()
            .expect("replacement recovery blocked set lock poisoned")
            .contains(set_disk_id)
    }

    async fn validate_replacement_recovery_records(disk: &crate::heal::DiskStore) -> Result<()> {
        ResumeUtils::migrate_legacy_replacement_records(disk).await?;
        for task_id in ResumeUtils::get_replacement_intent_tasks(disk).await? {
            ResumeManager::load_replacement_intent(disk.clone(), &task_id).await?;
        }
        Ok(())
    }

    /// Start the bounded resume-state inspector.  Destructive GC remains
    /// disabled until the durable owner/CAS contract from backlog#1927 is
    /// available; this task therefore cannot remove an active or stale file.
    async fn start_resume_gc(&self) {
        let cancel = self.cancel_token.clone();
        tokio::spawn(async move {
            let mut gc_by_disk = HashMap::<String, ResumeGc>::new();
            let mut ticker = interval(RESUME_GC_INTERVAL);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = ticker.tick() => {
                        let disks = {
                            let local_disk_map = local_disk_map_read().await;
                            local_disk_map.values().flatten().cloned().collect::<Vec<_>>()
                        };
                        for disk in disks {
                            let disk_key = disk.endpoint().to_string();
                            let gc = gc_by_disk.entry(disk_key).or_default();
                            tokio::select! {
                                _ = cancel.cancelled() => return,
                                result = gc.inspect_disk(&disk) => {
                                    if let Err(error) = result {
                                        warn!(
                                            target: "rustfs::heal::manager",
                                            event = EVENT_HEAL_RESUME_GC,
                                            component = LOG_COMPONENT_HEAL,
                                            subsystem = LOG_SUBSYSTEM_MANAGER,
                                            state = "inspect_failed",
                                            endpoint = %disk.endpoint(),
                                            error = %error,
                                            "Heal resume GC inspection failed"
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    /// Create new HealManager
    pub fn new(storage: Arc<dyn HealStorageAPI>, config: Option<HealConfig>) -> Self {
        Self::new_with_workload_provider(storage, config, None)
    }

    /// Create new HealManager with an optional workload admission snapshot provider.
    pub fn new_with_workload_provider(
        storage: Arc<dyn HealStorageAPI>,
        config: Option<HealConfig>,
        workload_provider: Option<WorkloadSnapshotProviderRef>,
    ) -> Self {
        let config = config.unwrap_or_default();
        Self {
            config: Arc::new(RwLock::new(config)),
            state: Arc::new(RwLock::new(HealState::default())),
            active_heals: Arc::new(Mutex::new(HashMap::new())),
            heal_queue: Arc::new(Mutex::new(PriorityHealQueue::new())),
            completed_heals: Arc::new(Mutex::new(HashMap::new())),
            displaced_terminals: Arc::new(StdMutex::new(HashMap::new())),
            task_aliases: Arc::new(Mutex::new(HashMap::new())),
            retrying_heals: Arc::new(Mutex::new(HashMap::new())),
            mrf_repair_notice_targets: Arc::new(StdMutex::new(HashMap::new())),
            replacement_recovery_anchors: Arc::new(std::sync::Mutex::new(HashMap::new())),
            replacement_recovery_blocked_sets: Arc::new(std::sync::Mutex::new(HashSet::new())),
            storage,
            cancel_token: CancellationToken::new(),
            statistics: Arc::new(RwLock::new(HealStatistics::new())),
            notify: Arc::new(Notify::new()),
            workload_provider,
        }
    }

    /// Start HealManager
    pub async fn start(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if state.is_running {
            warn!(
                target: "rustfs::heal::manager",
                event = EVENT_HEAL_MANAGER_STATE,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_MANAGER,
                state = "already_running",
                "Heal manager already running"
            );
            return Ok(());
        }
        state.is_running = true;
        drop(state);

        info!(
            target: "rustfs::heal::manager",
            event = EVENT_HEAL_MANAGER_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_MANAGER,
            state = "starting",
            "Heal manager starting"
        );

        // start scheduler
        self.start_scheduler().await?;

        // Recover durable replacement intents before the scanner can admit a
        // competing task for the same set.
        self.process_unclean_shutdown().await;

        // Inspect resume artifacts in a bounded, fail-closed background task.
        self.start_resume_gc().await;

        // start auto disk scanner to heal unformatted disks
        if self.config.read().await.enable_auto_heal {
            self.start_auto_disk_scanner().await?;
        } else {
            info!(
                target: "rustfs::heal::manager",
                event = EVENT_HEAL_AUTO_SCAN_STATE,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                state = "disabled",
                "Heal auto disk scanner disabled"
            );
        }

        info!(
            target: "rustfs::heal::manager",
            event = EVENT_HEAL_MANAGER_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_MANAGER,
            state = "running",
            "Heal manager started"
        );
        Ok(())
    }

    /// Stop HealManager
    pub async fn stop(&self) -> Result<()> {
        info!(
            target: "rustfs::heal::manager",
            event = EVENT_HEAL_MANAGER_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_MANAGER,
            state = "stopping",
            "Heal manager stopping"
        );

        // cancel all tasks
        self.cancel_token.cancel();

        // wait for all tasks to complete
        let mut active_heals = self.active_heals.lock().await;
        for task in active_heals.values() {
            if let Err(e) = task.cancel().await {
                warn!(
                    target: "rustfs::heal::manager",
                    event = EVENT_HEAL_MANAGER_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_MANAGER,
                    state = "task_cancel_failed",
                    task_id = %task.id,
                    error = %e,
                    "Heal active task cancellation failed"
                );
            }
        }
        active_heals.clear();
        publish_active_heal_count(&active_heals);
        self.completed_heals.lock().await.clear();
        // Do not let the synchronous guard live across the following async lock.
        {
            lock_displaced_terminals(&self.displaced_terminals).clear();
        }
        self.task_aliases.lock().await.clear();
        self.retrying_heals.lock().await.clear();
        let mrf_targets = {
            let mut registry = lock_mrf_repair_notice_targets(&self.mrf_repair_notice_targets);
            registry.drain().flat_map(|(_, targets)| targets).collect::<Vec<_>>()
        };
        for target in mrf_targets {
            rustfs_common::mrf_channel::release_mrf_identity(
                target.kind,
                &target.bucket,
                &target.object,
                target.version_id,
                target.scope,
                target.lease,
            );
        }
        crate::set_heal_queue_length(0);

        // update state
        let mut state = self.state.write().await;
        state.is_running = false;

        info!(
            target: "rustfs::heal::manager",
            event = EVENT_HEAL_MANAGER_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_MANAGER,
            state = "stopped",
            "Heal manager stopped"
        );
        Ok(())
    }

    /// Submit heal request
    pub async fn submit_heal_request_with_receipt(&self, request: HealRequest) -> Result<HealAdmissionReceipt> {
        self.submit_heal_request_with_receipt_and_alias(request, false).await
    }

    pub(crate) async fn submit_heal_request_with_receipt_and_alias(
        &self,
        request: HealRequest,
        preserve_alias: bool,
    ) -> Result<HealAdmissionReceipt> {
        self.submit_heal_request_with_receipt_alias_and_mrf_notice(request, preserve_alias, None)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn submit_mrf_heal_request_with_receipt(
        &self,
        request: HealRequest,
        bucket: Arc<str>,
        object: Arc<str>,
        version_id: Option<[u8; 16]>,
    ) -> Result<HealAdmissionReceipt> {
        let kind = match &request.heal_type {
            HealType::Metadata { .. } => rustfs_common::mrf_channel::MrfKind::MetadataCorruption,
            HealType::ECDecode { .. } => rustfs_common::mrf_channel::MrfKind::DecodeFailure,
            _ => rustfs_common::mrf_channel::MrfKind::PartialWrite,
        };
        self.submit_mrf_heal_request_with_receipt_and_identity(request, bucket, object, version_id, kind, None, None)
            .await
    }

    pub(crate) async fn submit_mrf_heal_request_with_receipt_and_identity(
        &self,
        request: HealRequest,
        bucket: Arc<str>,
        object: Arc<str>,
        version_id: Option<[u8; 16]>,
        kind: rustfs_common::mrf_channel::MrfKind,
        scope: Option<rustfs_common::mrf_channel::MrfScope>,
        lease: Option<rustfs_common::mrf_channel::MrfIngressLease>,
    ) -> Result<HealAdmissionReceipt> {
        self.submit_heal_request_with_receipt_alias_and_mrf_notice(
            request,
            true,
            Some(MrfRepairNoticeTarget {
                bucket,
                object,
                version_id,
                kind,
                scope,
                lease,
            }),
        )
        .await
    }

    async fn submit_heal_request_with_receipt_alias_and_mrf_notice(
        &self,
        request: HealRequest,
        preserve_alias: bool,
        mrf_notice_target: Option<MrfRepairNoticeTarget>,
    ) -> Result<HealAdmissionReceipt> {
        // HS-06 forceStart semantics (admin only): MinIO stops the old task
        // first and then starts the new one. Cancel any active admin task
        // overlapping this request's path before entering admission, so the
        // fresh task is never merged into the one being replaced.
        if request.source == HealRequestSource::Admin && request.force_start {
            let overlapping: Vec<String> = {
                let active_heals = self.active_heals.lock().await;
                active_heals
                    .iter()
                    .filter(|(task_id, task)| {
                        task.source == HealRequestSource::Admin
                            && heal_types_overlap(&request.heal_type, &task.heal_type) != OverlapVerdict::Disjoint
                            && *task_id != &request.id
                    })
                    .map(|(task_id, _)| task_id.clone())
                    .collect()
            };
            for task_id in overlapping {
                match self.cancel_task(&task_id).await {
                    Ok(_) => info!(
                        target: "rustfs::heal::manager",
                        event = EVENT_HEAL_QUEUE_ADMISSION,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_MANAGER,
                        request_id = %request.id,
                        cancelled_task_id = %task_id,
                        result = "force_start_cancelled_overlap",
                        "Admin forceStart cancelled an overlapping heal task"
                    ),
                    Err(err) => warn!(
                        target: "rustfs::heal::manager",
                        event = EVENT_HEAL_QUEUE_ADMISSION,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_MANAGER,
                        request_id = %request.id,
                        cancelled_task_id = %task_id,
                        error = %err,
                        result = "force_start_cancel_failed",
                        "Admin forceStart failed to cancel an overlapping heal task"
                    ),
                }
            }
        }

        let config = self.config.read().await;
        let dedup_key = PriorityHealQueue::make_dedup_key(&request);

        // Match the scheduler's active -> queue order and keep retry ownership
        // in the same atomic view. Otherwise queue -> active and
        // active -> retrying transitions can slip between duplicate checks.
        let active_heals = self.active_heals.lock().await;
        #[cfg(test)]
        pause_duplicate_admission_after_active_lock(&request.id).await;
        let mut queue = self.heal_queue.lock().await;
        let retrying_heals = self.retrying_heals.lock().await;
        let duplicate = (!request.force_start).then(|| {
            active_heal_for_dedup_key(&active_heals, &dedup_key)
                .map(|(task_id, _)| (task_id, "active"))
                .or_else(|| {
                    queue
                        .queued_request_id_for_dedup_key(&dedup_key)
                        .map(|queued_id| (queued_id.to_string(), "queued"))
                })
                .or_else(|| retrying_heal_for_dedup_key(&retrying_heals, &dedup_key).map(|(task_id, _)| (task_id, "retrying")))
        });
        if let Some((merged_task_id, duplicate_state)) = duplicate.flatten() {
            // HS-06: under the minio_error overlap policy an exact duplicate
            // admin start reports the typed AlreadyRunning rejection instead
            // of the silent merge (MinIO's ErrHealAlreadyRunning).
            let admission =
                if request.source == HealRequestSource::Admin && config.overlap_policy == HealOverlapPolicy::MinioError {
                    HealAdmissionResult::Dropped(HealAdmissionDropReason::AlreadyRunning)
                } else {
                    Self::duplicate_admission_for_request(&request, &config)
                };
            if matches!(admission, HealAdmissionResult::Merged)
                && let Some(target) = mrf_notice_target
            {
                let mut targets = lock_mrf_repair_notice_targets(&self.mrf_repair_notice_targets);
                Self::insert_mrf_repair_notice_target(&mut targets, &merged_task_id, target);
            }
            drop(retrying_heals);
            drop(queue);
            drop(active_heals);
            Self::record_admission_metric(request.source, admission, "duplicate");

            match admission {
                HealAdmissionResult::Merged => {
                    if preserve_alias {
                        self.insert_task_alias(&request.id, &merged_task_id).await;
                    }
                    debug!(
                        target: "rustfs::heal::manager",
                        event = EVENT_HEAL_QUEUE_ADMISSION,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_MANAGER,
                        request_id = %request.id,
                        merged_task_id = %merged_task_id,
                        priority = ?request.priority,
                        duplicate_state,
                        result = "merged_duplicate",
                        "Heal queue admission decided"
                    );
                }
                HealAdmissionResult::Dropped(reason) => {
                    demote_to_debug_when!(request.heal_type.is_per_object(), warn, target: "rustfs::heal::manager", {
                        event = EVENT_HEAL_QUEUE_ADMISSION,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_MANAGER,
                        request_id = %request.id,
                        priority = ?request.priority,
                        reason = reason.as_str(),
                        duplicate_state,
                        result = "dropped_duplicate",
                        "Heal queue admission decided"
                    });
                }
                HealAdmissionResult::Accepted | HealAdmissionResult::Full => {}
            }

            return Ok(HealAdmissionReceipt {
                result: admission,
                task_id: merged_task_id,
            });
        }

        // HS-06 typed overlap rejection (admin only, minio_error policy):
        // paths containing or contained by an active/queued task reject with
        // AlreadyRunning / OverlappingPaths instead of merging. Exact
        // duplicates already merged above; scanner/autoheal/read-repair
        // sources never take this path.
        if request.source == HealRequestSource::Admin && config.overlap_policy == HealOverlapPolicy::MinioError {
            let mut rejection = None;
            for (task_id, task) in active_heals.iter() {
                match heal_types_overlap(&request.heal_type, &task.heal_type) {
                    OverlapVerdict::SameTarget => {
                        rejection = Some((HealAdmissionDropReason::AlreadyRunning, task_id.clone()));
                        break;
                    }
                    OverlapVerdict::Overlapping => {
                        rejection = Some((HealAdmissionDropReason::OverlappingPaths, task_id.clone()));
                    }
                    OverlapVerdict::Disjoint => {}
                }
            }
            if rejection.is_none() {
                for queued in queue.requests() {
                    match heal_types_overlap(&request.heal_type, &queued.heal_type) {
                        OverlapVerdict::SameTarget => {
                            rejection = Some((HealAdmissionDropReason::AlreadyRunning, queued.id.clone()));
                            break;
                        }
                        OverlapVerdict::Overlapping => {
                            rejection = Some((HealAdmissionDropReason::OverlappingPaths, queued.id.clone()));
                        }
                        OverlapVerdict::Disjoint => {}
                    }
                }
            }
            if let Some((reason, overlap_task_id)) = rejection {
                drop(retrying_heals);
                drop(queue);
                drop(active_heals);
                Self::record_admission_metric(request.source, HealAdmissionResult::Dropped(reason), "overlap_rejected");
                warn!(
                    target: "rustfs::heal::manager",
                    event = EVENT_HEAL_QUEUE_ADMISSION,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_MANAGER,
                    request_id = %request.id,
                    overlap_task_id = %overlap_task_id,
                    reason = reason.as_str(),
                    result = "overlap_rejected",
                    "Admin heal start rejected by overlap policy"
                );
                return Ok(HealAdmissionReceipt {
                    result: HealAdmissionResult::Dropped(reason),
                    task_id: overlap_task_id,
                });
            }
        }

        let mut task_id = request.id.clone();
        let admission_decision = Self::admit_request_to_queue(&mut queue, request, &config, "submit");
        let admission = admission_decision.result;
        if admission == HealAdmissionResult::Merged
            && let Some(queued_id) = queue.queued_request_id_for_dedup_key(&dedup_key)
        {
            task_id = queued_id.to_owned();
        }
        let should_notify = matches!(admission, HealAdmissionResult::Accepted) && config.event_driven_scheduler_enable;
        let displaced_task_id = admission_decision.displaced_task_id().map(ToOwned::to_owned);
        let displaced_terminal = admission_decision
            .displaced_request
            .as_ref()
            .map(|request| record_displaced_terminal(&self.displaced_terminals, request));
        if matches!(admission, HealAdmissionResult::Accepted | HealAdmissionResult::Merged)
            && let Some(target) = mrf_notice_target
        {
            let mut targets = lock_mrf_repair_notice_targets(&self.mrf_repair_notice_targets);
            Self::insert_mrf_repair_notice_target(&mut targets, &task_id, target);
        }
        if let Some(displaced_task_id) = &displaced_task_id {
            self.remove_mrf_repair_notice_targets_for_task(displaced_task_id);
        }
        drop(retrying_heals);
        drop(queue);
        drop(active_heals);

        if let (Some(displaced_task_id), Some(displaced_terminal)) = (displaced_task_id, displaced_terminal) {
            // The queue has already removed the displaced request, so the
            // synchronous terminal sidecar was published before aliases and
            // MRF ownership are cleaned up.
            remove_displaced_task_aliases(&self.task_aliases, &self.displaced_terminals, &displaced_task_id, &displaced_terminal)
                .await;
        }

        if should_notify {
            self.notify.notify_one();
        }

        Ok(HealAdmissionReceipt {
            result: admission,
            task_id,
        })
    }

    /// Submit heal request.
    pub async fn submit_heal_request(&self, request: HealRequest) -> Result<HealAdmissionResult> {
        Ok(self.submit_heal_request_with_receipt_and_alias(request, true).await?.result)
    }

    /// Get task status
    /// Ordered task-state lookup shared by every status/report query. The
    /// map precedence mirrors the historical per-method cascades exactly:
    /// active, then retrying, then completed — where a completed entry in a
    /// retrying state outranks the queue so a retrying task reports
    /// Retrying, never Pending — then the queue, and finally a terminal
    /// completed entry. `heal_path` additionally constrains the map matches
    /// the way the `*_for_path` variants always have.
    async fn lookup_task_state(&self, canonical_task_id: &str, heal_path: Option<&str>) -> TaskStateLookup {
        let matches_path = |heal_type: &HealType| heal_path.is_none_or(|path| heal_type_matches_path(heal_type, path));

        {
            let active_heals = self.active_heals.lock().await;
            if let Some(task) = active_heals
                .get(canonical_task_id)
                .filter(|task| matches_path(&task.heal_type))
            {
                return TaskStateLookup::Active(Arc::clone(task));
            }
        }

        {
            let retrying_heals = self.retrying_heals.lock().await;
            if let Some(retrying) = retrying_heals
                .get(canonical_task_id)
                .filter(|retrying| matches_path(&retrying.request.heal_type))
            {
                return TaskStateLookup::Retrying(retrying.status());
            }
        }

        // One completed-map pass (single lock + prune): a retrying completion
        // returns immediately; a terminal completion is held back until the
        // queue has been checked, so queued work outranks it.
        let mut terminal_completed: Option<Arc<CompletedHealStatus>> = None;
        {
            let mut completed_heals = self.completed_heals.lock().await;
            prune_completed_heal_statuses(&mut completed_heals);
            if let Some(completed) = completed_heals.get(canonical_task_id).filter(|c| matches_path(&c.heal_type)) {
                if completed_status_is_retrying(&completed.status) {
                    return TaskStateLookup::Completed(Arc::clone(completed));
                }
                terminal_completed = Some(Arc::clone(completed));
            }
        }

        {
            let queue = self.heal_queue.lock().await;
            let queued = match heal_path {
                Some(path) => queue.contains_request_id_matching_path(canonical_task_id, path),
                None => queue.contains_request_id(canonical_task_id),
            };
            if queued {
                return TaskStateLookup::Queued;
            }
        }

        if terminal_completed.is_none() {
            let mut displaced_terminals = lock_displaced_terminals(&self.displaced_terminals);
            prune_completed_heal_statuses(&mut displaced_terminals);
            terminal_completed = displaced_terminals
                .get(canonical_task_id)
                .filter(|terminal| matches_path(&terminal.heal_type))
                .cloned();
        }

        match terminal_completed {
            Some(completed) => TaskStateLookup::Completed(completed),
            None => TaskStateLookup::NotFound,
        }
    }

    pub async fn get_task_status(&self, task_id: &str) -> Result<HealTaskStatus> {
        let canonical_task_id = self.canonical_task_id(task_id).await;
        match self.lookup_task_state(&canonical_task_id, None).await {
            TaskStateLookup::Active(task) => Ok(task.get_status().await),
            TaskStateLookup::Retrying(status) => Ok(status),
            TaskStateLookup::Completed(completed) => Ok(completed.status.clone()),
            TaskStateLookup::Queued => Ok(HealTaskStatus::Pending),
            TaskStateLookup::NotFound => Err(Error::TaskNotFound {
                task_id: task_id.to_string(),
            }),
        }
    }

    pub async fn get_task_report(&self, task_id: &str) -> Result<HealTaskReport> {
        self.get_task_report_since(task_id, None).await
    }

    /// Incremental variant of [`Self::get_task_report`] (HS-06): `since` is
    /// the client's last seen sequence number; `None` keeps the legacy
    /// full-snapshot semantics.
    pub async fn get_task_report_since(&self, task_id: &str, since: Option<u64>) -> Result<HealTaskReport> {
        let canonical_task_id = self.canonical_task_id(task_id).await;
        match self.lookup_task_state(&canonical_task_id, None).await {
            TaskStateLookup::Active(task) => Ok(active_task_report(&task, since).await),
            TaskStateLookup::Retrying(status) => Ok(empty_task_report(status)),
            TaskStateLookup::Completed(completed) => Ok(completed_task_report(&completed, since)),
            TaskStateLookup::Queued => Ok(empty_task_report(HealTaskStatus::Pending)),
            TaskStateLookup::NotFound => Err(Error::TaskNotFound {
                task_id: task_id.to_string(),
            }),
        }
    }

    pub async fn get_task_report_for_path(&self, heal_path: &str, task_id: &str) -> Result<HealTaskReport> {
        self.get_task_report_for_path_since(heal_path, task_id, None).await
    }

    /// Incremental variant of [`Self::get_task_report_for_path`] (HS-06).
    pub async fn get_task_report_for_path_since(
        &self,
        heal_path: &str,
        task_id: &str,
        since: Option<u64>,
    ) -> Result<HealTaskReport> {
        let canonical_task_id = self.canonical_task_id(task_id).await;
        match self.lookup_task_state(&canonical_task_id, Some(heal_path)).await {
            TaskStateLookup::Active(task) => Ok(active_task_report(&task, since).await),
            TaskStateLookup::Retrying(status) => Ok(empty_task_report(status)),
            TaskStateLookup::Completed(completed) => Ok(completed_task_report(&completed, since)),
            TaskStateLookup::Queued => Ok(empty_task_report(HealTaskStatus::Pending)),
            TaskStateLookup::NotFound => {
                if self.path_has_task(heal_path).await {
                    return Err(Error::InvalidClientToken);
                }
                Err(Error::TaskNotFound {
                    task_id: task_id.to_string(),
                })
            }
        }
    }

    /// Get task status for a path-bound client token.
    ///
    /// If the token is unknown but no task remains for the path, the caller can
    /// treat it as an already-finished sequence. If the path still has a live or
    /// recently completed task, a different token is invalid for that path.
    pub async fn get_task_status_for_path(&self, heal_path: &str, task_id: &str) -> Result<HealTaskStatus> {
        let canonical_task_id = self.canonical_task_id(task_id).await;
        match self.lookup_task_state(&canonical_task_id, Some(heal_path)).await {
            TaskStateLookup::Active(task) => Ok(task.get_status().await),
            TaskStateLookup::Retrying(status) => Ok(status),
            TaskStateLookup::Completed(completed) => Ok(completed.status.clone()),
            TaskStateLookup::Queued => Ok(HealTaskStatus::Pending),
            TaskStateLookup::NotFound => {
                if self.path_has_task(heal_path).await {
                    return Err(Error::InvalidClientToken);
                }
                Err(Error::TaskNotFound {
                    task_id: task_id.to_string(),
                })
            }
        }
    }

    async fn path_has_task(&self, heal_path: &str) -> bool {
        {
            let active_heals = self.active_heals.lock().await;
            if active_heals
                .values()
                .any(|task| heal_type_matches_path(&task.heal_type, heal_path))
            {
                return true;
            }
        }

        {
            let queue = self.heal_queue.lock().await;
            if queue.contains_matching(|request| heal_type_matches_path(&request.heal_type, heal_path)) {
                return true;
            }
        }

        {
            let retrying_heals = self.retrying_heals.lock().await;
            if retrying_heals
                .values()
                .any(|retrying| heal_type_matches_path(&retrying.request.heal_type, heal_path))
            {
                return true;
            }
        }

        let mut completed_heals = self.completed_heals.lock().await;
        prune_completed_heal_statuses(&mut completed_heals);
        if completed_heals
            .values()
            .any(|completed| heal_type_matches_path(&completed.heal_type, heal_path))
        {
            return true;
        }
        drop(completed_heals);

        let mut displaced_terminals = lock_displaced_terminals(&self.displaced_terminals);
        prune_completed_heal_statuses(&mut displaced_terminals);
        displaced_terminals
            .values()
            .any(|terminal| heal_type_matches_path(&terminal.heal_type, heal_path))
    }

    /// Get task progress
    pub async fn get_active_tasks_count(&self) -> usize {
        let active_heals = self.active_heals.lock().await;
        publish_active_heal_count(&active_heals);
        active_heals.len()
    }

    pub async fn get_task_progress(&self, task_id: &str) -> Result<HealProgress> {
        let canonical_task_id = self.canonical_task_id(task_id).await;
        let active_heals = self.active_heals.lock().await;
        if let Some(task) = active_heals.get(&canonical_task_id) {
            Ok(task.get_progress().await)
        } else {
            Err(Error::TaskNotFound {
                task_id: task_id.to_string(),
            })
        }
    }

    /// Cancel task
    pub async fn cancel_task(&self, task_id: &str) -> Result<()> {
        let canonical_task_id = self.canonical_task_id(task_id).await;
        {
            let mut active_heals = self.active_heals.lock().await;
            if let Some(task) = active_heals.get(&canonical_task_id) {
                task.cancel().await?;
                active_heals.remove(&canonical_task_id);
                publish_active_heal_count(&active_heals);
                info!(
                    target: "rustfs::heal::manager",
                    event = EVENT_HEAL_MANAGER_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_MANAGER,
                    task_id = %canonical_task_id,
                    state = "cancelled_active_task",
                    "Heal manager cancelled active task"
                );
                drop(active_heals);
                self.remove_aliases_for_task(&canonical_task_id).await;
                self.remove_mrf_repair_notice_targets_for_task(&canonical_task_id);
                return Ok(());
            }
        }

        {
            let mut retrying_heals = self.retrying_heals.lock().await;
            if let Some(retrying) = retrying_heals.remove(&canonical_task_id) {
                retrying.cancel_token.cancel();
                drop(retrying_heals);
                self.completed_heals.lock().await.remove(&canonical_task_id);
                self.remove_aliases_for_task(&canonical_task_id).await;
                self.remove_mrf_repair_notice_targets_for_task(&canonical_task_id);
                info!(
                    target: "rustfs::heal::manager",
                    event = EVENT_HEAL_MANAGER_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_MANAGER,
                    task_id = %canonical_task_id,
                    state = "cancelled_retrying_task",
                    "Heal manager cancelled retrying task"
                );
                return Ok(());
            }
        }

        let mut queue = self.heal_queue.lock().await;
        if queue.remove_request_id(&canonical_task_id).is_some() {
            publish_heal_queue_length(&queue);
            info!(
                target: "rustfs::heal::manager",
                event = EVENT_HEAL_MANAGER_STATE,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_MANAGER,
                task_id = %canonical_task_id,
                state = "cancelled_queued_task",
                "Heal manager cancelled queued task"
            );
            drop(queue);
            self.remove_aliases_for_task(&canonical_task_id).await;
            self.remove_mrf_repair_notice_targets_for_task(&canonical_task_id);
            return Ok(());
        }

        Err(Error::TaskNotFound {
            task_id: task_id.to_string(),
        })
    }

    /// Cancel all queued or active tasks matching a heal path.
    pub async fn cancel_tasks_for_path(&self, heal_path: &str) -> Result<usize> {
        let mut cancelled = 0usize;

        {
            let mut active_heals = self.active_heals.lock().await;
            let task_ids = active_heals
                .iter()
                .filter_map(|(task_id, task)| heal_type_matches_path(&task.heal_type, heal_path).then_some(task_id.clone()))
                .collect::<Vec<_>>();

            for task_id in &task_ids {
                if let Some(task) = active_heals.get(task_id) {
                    task.cancel().await?;
                }
                active_heals.remove(task_id);
                cancelled += 1;
            }

            if cancelled > 0 {
                publish_active_heal_count(&active_heals);
            }
            drop(active_heals);
            for task_id in &task_ids {
                self.remove_aliases_for_task(task_id).await;
                self.remove_mrf_repair_notice_targets_for_task(task_id);
            }
        }

        {
            let mut retrying_heals = self.retrying_heals.lock().await;
            let task_ids = retrying_heals
                .iter()
                .filter_map(|(task_id, retrying)| {
                    heal_type_matches_path(&retrying.request.heal_type, heal_path).then_some(task_id.clone())
                })
                .collect::<Vec<_>>();

            for task_id in &task_ids {
                if let Some(retrying) = retrying_heals.remove(task_id) {
                    retrying.cancel_token.cancel();
                    cancelled += 1;
                }
            }
            drop(retrying_heals);

            if !task_ids.is_empty() {
                let mut completed_heals = self.completed_heals.lock().await;
                for task_id in &task_ids {
                    completed_heals.remove(task_id);
                }
                drop(completed_heals);

                for task_id in &task_ids {
                    self.remove_aliases_for_task(task_id).await;
                    self.remove_mrf_repair_notice_targets_for_task(task_id);
                }
            }
        }

        let mut queue = self.heal_queue.lock().await;
        let queued_cancelled = queue.remove_matching(|request| heal_type_matches_path(&request.heal_type, heal_path));
        if !queued_cancelled.is_empty() {
            publish_heal_queue_length(&queue);
            cancelled += queued_cancelled.len();
        }
        drop(queue);
        for request in &queued_cancelled {
            self.remove_aliases_for_task(&request.id).await;
            self.remove_mrf_repair_notice_targets_for_task(&request.id);
        }

        if cancelled == 0 {
            return Err(Error::TaskNotFound {
                task_id: heal_path.to_string(),
            });
        }

        info!(
            target: "rustfs::heal::manager",
            event = EVENT_HEAL_MANAGER_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_MANAGER,
            heal_path,
            cancelled,
            state = "cancelled_path_tasks",
            "Heal manager cancelled tasks for path"
        );
        Ok(cancelled)
    }

    /// Get statistics
    pub async fn get_statistics(&self) -> HealStatistics {
        self.statistics.read().await.clone()
    }

    /// Get active task count
    pub async fn get_active_task_count(&self) -> usize {
        let active_heals = self.active_heals.lock().await;
        publish_active_heal_count(&active_heals);
        active_heals.len()
    }

    /// Get queue length
    pub async fn get_queue_length(&self) -> usize {
        let queue = self.heal_queue.lock().await;
        publish_heal_queue_length(&queue);
        queue.len()
    }

    pub async fn operations_snapshot(&self) -> HealOperationsSnapshot {
        // Match the scheduler's active -> queue order. Retry transitions also
        // acquire active before retrying, so the three sets form one snapshot.
        let active_heals = self.active_heals.lock().await;
        let queue = self.heal_queue.lock().await;
        let retrying_heals = self.retrying_heals.lock().await;
        let (queued_by_priority, queued_by_source) = queue.operation_counts();
        let mut active_by_priority = HealPriorityCounts::default();
        let mut active_by_source = HealSourceCounts::default();
        for task in active_heals.values() {
            active_by_priority.increment(task.priority);
            active_by_source.increment(task.source);
        }
        let mut retrying_by_priority = HealPriorityCounts::default();
        let mut retrying_by_source = HealSourceCounts::default();
        for retrying in retrying_heals.values() {
            retrying_by_priority.increment(retrying.request.priority);
            retrying_by_source.increment(retrying.request.source);
        }
        publish_active_heal_count(&active_heals);
        publish_heal_queue_length(&queue);

        HealOperationsSnapshot {
            queue_length: usize_to_u64_saturated(queue.len()),
            active_tasks: usize_to_u64_saturated(active_heals.len()),
            retrying_tasks: usize_to_u64_saturated(retrying_heals.len()),
            queued_by_priority,
            active_by_priority,
            retrying_by_priority,
            queued_by_source,
            active_by_source,
            retrying_by_source,
        }
    }

    pub async fn active_progress_snapshot(&self) -> Option<HealProgress> {
        let active_tasks = {
            let active_heals = self.active_heals.lock().await;
            active_heals.values().cloned().collect::<Vec<_>>()
        };
        if active_tasks.is_empty() {
            return None;
        }

        let mut progresses = Vec::with_capacity(active_tasks.len());
        for task in active_tasks {
            progresses.push(task.get_progress().await);
        }
        crate::heal::progress::aggregate_heal_progress(progresses)
    }
}

impl std::fmt::Debug for HealManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealManager")
            .field("config", &"<config>")
            .field("state", &"<state>")
            .field("active_heals_count", &"<active_heals>")
            .field("queue_length", &"<queue>")
            .finish()
    }
}

mod auto_scan;
mod queue;
mod scheduler;
mod unclean_shutdown;

use queue::*;
use scheduler::*;
use unclean_shutdown::*;

#[cfg(test)]
mod tests;
