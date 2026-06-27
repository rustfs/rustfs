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
    storage::HealStorageAPI,
    task::{HealOptions, HealPriority, HealRequest, HealTask, HealTaskStatus, HealType},
};
use crate::{Error, Result};
use metrics::{counter, gauge};
use rustfs_common::heal_channel::{HealAdmissionDropReason, HealAdmissionResult, HealRequestSource};
use rustfs_concurrency::{AdmissionState, WorkloadAdmissionSnapshotProvider, WorkloadClass};
use rustfs_madmin::heal_commands::HealResultItem;
use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{Mutex, Notify, RwLock},
    time::{interval, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::{DiskError, EcstoreError, GLOBAL_LOCAL_DISK_MAP, HealDiskExt as _};

const KEEP_HEAL_TASK_STATUS_DURATION: Duration = Duration::from_secs(10 * 60);
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
const MAX_RECOVERABLE_HEAL_RETRIES: u32 = 3;
const MAX_RECOVERABLE_HEAL_RETRY_DELAY: Duration = Duration::from_secs(30);

type WorkloadSnapshotProviderRef = Arc<dyn WorkloadAdmissionSnapshotProvider + Send + Sync>;

/// Priority queue wrapper for heal requests
/// Uses BinaryHeap for priority-based ordering while maintaining FIFO for same-priority items
#[derive(Debug)]
struct PriorityHealQueue {
    /// Heap of (priority, sequence, request) tuples
    heap: BinaryHeap<PriorityQueueItem>,
    /// Sequence counter for FIFO ordering within same priority
    sequence: u64,
    /// Deduplication key reference counts for queued requests
    dedup_keys: HashMap<String, usize>,
}

/// Wrapper for heap items to implement proper ordering
#[derive(Debug)]
struct PriorityQueueItem {
    priority: HealPriority,
    sequence: u64,
    request: HealRequest,
}

impl Eq for PriorityQueueItem {}

impl PartialEq for PriorityQueueItem {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

impl Ord for PriorityQueueItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // First compare by priority (higher priority first)
        match self.priority.cmp(&other.priority) {
            std::cmp::Ordering::Equal => {
                // If priorities are equal, use sequence for FIFO (lower sequence first)
                other.sequence.cmp(&self.sequence)
            }
            ordering => ordering,
        }
    }
}

impl PartialOrd for PriorityQueueItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueuePushOutcome {
    Accepted,
    Merged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ForegroundPressure {
    class: WorkloadClass,
    usage_pct: usize,
    threshold_pct: usize,
}

impl ForegroundPressure {
    const fn reason(self) -> &'static str {
        match self.class {
            WorkloadClass::ForegroundRead => "foreground_read_pressure",
            WorkloadClass::ForegroundWrite => "foreground_write_pressure",
            _ => "foreground_pressure",
        }
    }
}

#[derive(Debug, Clone)]
struct CompletedHealStatus {
    heal_type: HealType,
    status: HealTaskStatus,
    result_items: Vec<HealResultItem>,
    completed_at: SystemTime,
}

#[derive(Debug, Clone)]
struct HealTaskAlias {
    task_id: String,
}

#[derive(Debug, Clone)]
struct RetryingHeal {
    request: HealRequest,
    error: String,
    cancel_token: CancellationToken,
}

#[derive(Debug, Clone)]
pub struct HealTaskReport {
    pub status: HealTaskStatus,
    pub result_items: Vec<HealResultItem>,
    pub progress: Option<HealProgress>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealSourceCounts {
    pub scanner: u64,
    pub admin: u64,
    pub auto_heal: u64,
    pub internal: u64,
    pub read_repair: u64,
}

impl HealSourceCounts {
    fn increment(&mut self, source: HealRequestSource) {
        match source {
            HealRequestSource::Scanner => self.scanner += 1,
            HealRequestSource::Admin => self.admin += 1,
            HealRequestSource::AutoHeal => self.auto_heal += 1,
            HealRequestSource::Internal => self.internal += 1,
            HealRequestSource::ReadRepair => self.read_repair += 1,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealOperationsSnapshot {
    pub queue_length: u64,
    pub active_tasks: u64,
    pub queued_by_priority: HealPriorityCounts,
    pub active_by_priority: HealPriorityCounts,
    pub queued_by_source: HealSourceCounts,
    pub active_by_source: HealSourceCounts,
}

fn usize_to_u64_saturated(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

impl PriorityHealQueue {
    fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            sequence: 0,
            dedup_keys: HashMap::new(),
        }
    }

    fn len(&self) -> usize {
        self.heap.len()
    }

    fn pop_next(&mut self) -> Option<HealRequest> {
        self.heap.pop().map(|item| {
            let key = Self::make_dedup_key(&item.request);
            Self::decrement_or_remove_dedup_key(&mut self.dedup_keys, &key);
            item.request
        })
    }

    fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    fn push(&mut self, request: HealRequest) -> QueuePushOutcome {
        let key = Self::make_dedup_key(&request);

        // Check for duplicates unless the caller explicitly forces admission.
        if self.dedup_keys.contains_key(&key) && !request.force_start {
            return QueuePushOutcome::Merged;
        }
        // Track dedup keys for both normal and forced requests so queued forced work
        // also reserves the dedup key for later non-forced duplicates.
        *self.dedup_keys.entry(key).or_insert(0) += 1;
        self.sequence += 1;
        self.heap.push(PriorityQueueItem {
            priority: request.priority,
            sequence: self.sequence,
            request,
        });
        QueuePushOutcome::Accepted
    }

    fn can_displace_lower_priority(&self, priority: HealPriority) -> bool {
        self.heap.iter().any(|item| item.priority < priority)
    }

    fn push_displacing_lower_priority(&mut self, request: HealRequest) -> Option<HealRequest> {
        let mut retained = BinaryHeap::new();
        let mut displaced: Option<PriorityQueueItem> = None;

        while let Some(item) = self.heap.pop() {
            if item.priority < request.priority {
                let should_displace = displaced
                    .as_ref()
                    .map(|current| {
                        item.priority < current.priority
                            || (item.priority == current.priority && item.sequence > current.sequence)
                    })
                    .unwrap_or(true);
                if should_displace {
                    if let Some(current) = displaced.replace(item) {
                        retained.push(current);
                    }
                } else {
                    retained.push(item);
                }
            } else {
                retained.push(item);
            }
        }

        self.heap = retained;

        let displaced = displaced.map(|item| {
            let key = Self::make_dedup_key(&item.request);
            Self::decrement_or_remove_dedup_key(&mut self.dedup_keys, &key);
            item.request
        });

        if displaced.is_some() {
            debug_assert_eq!(self.push(request), QueuePushOutcome::Accepted);
        }

        displaced
    }

    /// Get statistics about queue contents by priority
    fn get_priority_stats(&self) -> HashMap<HealPriority, usize> {
        let mut stats = HashMap::new();
        for item in &self.heap {
            *stats.entry(item.priority).or_insert(0) += 1;
        }
        stats
    }

    fn operation_counts(&self) -> (HealPriorityCounts, HealSourceCounts) {
        let mut priority = HealPriorityCounts::default();
        let mut source = HealSourceCounts::default();
        for item in &self.heap {
            priority.increment(item.request.priority);
            source.increment(item.request.source);
        }
        (priority, source)
    }

    #[cfg(test)]
    fn pop(&mut self) -> Option<HealRequest> {
        self.heap.pop().map(|item| {
            let key = Self::make_dedup_key(&item.request);
            Self::decrement_or_remove_dedup_key(&mut self.dedup_keys, &key);
            item.request
        })
    }

    #[cfg(test)]
    fn pop_runnable<F>(&mut self, can_run: F) -> Option<HealRequest>
    where
        F: Fn(&HealRequest) -> bool,
    {
        self.pop_runnable_with_skips(can_run, |_| None).0
    }

    fn pop_runnable_with_skips<F, G>(&mut self, can_run: F, skip_label: G) -> (Option<HealRequest>, Vec<String>)
    where
        F: Fn(&HealRequest) -> bool,
        G: Fn(&HealRequest) -> Option<String>,
    {
        let mut deferred = Vec::new();
        let mut selected = None;
        let mut skipped = Vec::new();

        while let Some(item) = self.heap.pop() {
            if can_run(&item.request) {
                selected = Some(item);
                break;
            }
            if let Some(label) = skip_label(&item.request) {
                skipped.push(label);
            }
            deferred.push(item);
        }

        for item in deferred {
            self.heap.push(item);
        }

        (
            selected.map(|item| {
                let key = Self::make_dedup_key(&item.request);
                Self::decrement_or_remove_dedup_key(&mut self.dedup_keys, &key);
                item.request
            }),
            skipped,
        )
    }

    /// Create a deduplication key from a heal request
    fn make_dedup_key(request: &HealRequest) -> String {
        Self::make_dedup_key_for_type(&request.heal_type)
    }

    fn make_dedup_key_for_type(heal_type: &HealType) -> String {
        match heal_type {
            HealType::Cluster => "cluster".to_string(),
            HealType::Object {
                bucket,
                object,
                version_id,
            } => {
                format!("object:{}:{}:{}", bucket, object, version_id.as_deref().unwrap_or(""))
            }
            HealType::Bucket { bucket } => {
                format!("bucket:{bucket}")
            }
            HealType::Prefix { bucket, prefix } => {
                format!("prefix:{bucket}/{prefix}")
            }
            HealType::ErasureSet { set_disk_id, .. } => {
                format!("erasure_set:{set_disk_id}")
            }
            HealType::Metadata { bucket, object } => {
                format!("metadata:{bucket}:{object}")
            }
            HealType::MRF { meta_path } => {
                format!("mrf:{meta_path}")
            }
            HealType::ECDecode {
                bucket,
                object,
                version_id,
            } => {
                format!("ecdecode:{}:{}:{}", bucket, object, version_id.as_deref().unwrap_or(""))
            }
        }
    }

    fn decrement_or_remove_dedup_key(dedup_keys: &mut HashMap<String, usize>, key: &str) {
        if let Some(count) = dedup_keys.get_mut(key) {
            if *count <= 1 {
                dedup_keys.remove(key);
            } else {
                *count -= 1;
            }
        }
    }

    /// Check if a request with the same key already exists in the queue
    #[allow(dead_code)]
    fn contains_key(&self, request: &HealRequest) -> bool {
        let key = Self::make_dedup_key(request);
        self.dedup_keys.contains_key(&key)
    }

    /// Check if an erasure set heal request for a specific set_disk_id exists
    fn contains_erasure_set(&self, set_disk_id: &str) -> bool {
        let key = format!("erasure_set:{set_disk_id}");
        self.dedup_keys.contains_key(&key)
    }

    fn contains_request_id(&self, request_id: &str) -> bool {
        self.heap.iter().any(|item| item.request.id == request_id)
    }

    fn contains_request_id_matching_path(&self, request_id: &str, heal_path: &str) -> bool {
        self.heap
            .iter()
            .any(|item| item.request.id == request_id && heal_type_matches_path(&item.request.heal_type, heal_path))
    }

    fn request_for_dedup_key(&self, key: &str) -> Option<&HealRequest> {
        self.heap
            .iter()
            .find_map(|item| (Self::make_dedup_key(&item.request) == key).then_some(&item.request))
    }

    fn contains_matching<F>(&self, mut matches: F) -> bool
    where
        F: FnMut(&HealRequest) -> bool,
    {
        self.heap.iter().any(|item| matches(&item.request))
    }

    fn remove_request_id(&mut self, request_id: &str) -> Option<HealRequest> {
        let mut retained = BinaryHeap::new();
        let mut removed = None;

        while let Some(item) = self.heap.pop() {
            if removed.is_none() && item.request.id == request_id {
                let key = Self::make_dedup_key(&item.request);
                Self::decrement_or_remove_dedup_key(&mut self.dedup_keys, &key);
                removed = Some(item.request);
            } else {
                retained.push(item);
            }
        }

        self.heap = retained;
        removed
    }

    fn remove_matching<F>(&mut self, mut should_remove: F) -> Vec<HealRequest>
    where
        F: FnMut(&HealRequest) -> bool,
    {
        let mut retained = BinaryHeap::new();
        let mut removed = Vec::new();

        while let Some(item) = self.heap.pop() {
            if should_remove(&item.request) {
                let key = Self::make_dedup_key(&item.request);
                Self::decrement_or_remove_dedup_key(&mut self.dedup_keys, &key);
                removed.push(item.request);
            } else {
                retained.push(item);
            }
        }

        self.heap = retained;
        removed
    }
}

impl RetryingHeal {
    fn status(&self) -> HealTaskStatus {
        HealTaskStatus::Retrying {
            error: self.error.clone(),
            retry_attempt: self.request.retry_attempts,
        }
    }
}

fn heal_type_matches_path(heal_type: &HealType, heal_path: &str) -> bool {
    let heal_path = heal_path.trim_matches('/');
    if heal_path.is_empty() {
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
        HealType::MRF { meta_path } => heal_path == meta_path.trim_matches('/'),
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

fn active_heals_contains_dedup_key(active_heals: &HashMap<String, Arc<HealTask>>, key: &str) -> bool {
    active_heal_for_dedup_key(active_heals, key).is_some()
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

fn retry_request_for_result(task: &HealTask, result: &Result<()>) -> Option<(HealRequest, Duration, String)> {
    let Err(err) = result else {
        return None;
    };
    if task.retry_attempts >= MAX_RECOVERABLE_HEAL_RETRIES {
        return None;
    }

    let error = err.to_string();
    if !is_recoverable_heal_error(err, &error) {
        return None;
    }

    let request = task.retry_request();
    let delay = recoverable_heal_retry_delay(request.retry_attempts);
    Some((request, delay, error))
}

fn recoverable_heal_retry_delay(retry_attempt: u32) -> Duration {
    let retry_attempt = retry_attempt.clamp(1, 5);
    let delay = Duration::from_secs(2_u64.saturating_pow(retry_attempt));
    delay.min(MAX_RECOVERABLE_HEAL_RETRY_DELAY)
}

fn is_recoverable_heal_error(err: &Error, error: &str) -> bool {
    match err {
        Error::TaskCancelled => false,
        Error::TaskTimeout | Error::TransientSkip { .. } => true,
        Error::Storage(err) => is_recoverable_storage_heal_error(err) || is_recoverable_heal_error_message(error),
        Error::Disk(err) => is_recoverable_disk_heal_error(err) || is_recoverable_heal_error_message(error),
        Error::TaskExecutionFailed { .. } | Error::Io(_) | Error::IO(_) | Error::Anyhow(_) | Error::Other(_) => {
            is_recoverable_heal_error_message(error)
        }
        _ => false,
    }
}

fn is_recoverable_storage_heal_error(err: &EcstoreError) -> bool {
    err.is_quorum_error() || matches!(err, EcstoreError::SlowDown | EcstoreError::OperationCanceled | EcstoreError::Lock(_))
}

fn is_recoverable_disk_heal_error(err: &DiskError) -> bool {
    matches!(
        err,
        DiskError::ErasureReadQuorum
            | DiskError::ErasureWriteQuorum
            | DiskError::Timeout
            | DiskError::SourceStalled
            | DiskError::FaultyRemoteDisk
            | DiskError::FaultyDisk
    )
}

fn is_recoverable_heal_error_message(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    [
        "failed to acquire read lock",
        "lock acquisition failed",
        "lock acquisition timeout",
        "remote lock rpc timed out",
        "deadline has elapsed",
        "timed out",
        "transport error",
        "network error",
        "connection refused",
        "operation canceled",
        "quorum not reached",
    ]
    .iter()
    .any(|pattern| error.contains(pattern))
}

/// Heal config
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
    /// Task timeout
    pub task_timeout: Duration,
    /// Queue size
    pub queue_size: usize,
    /// Whether duplicate low-priority requests should merge into an existing queued request.
    pub low_priority_merge_enable: bool,
    /// Whether low-priority requests may be dropped when the queue is full.
    pub low_priority_drop_when_full: bool,
    /// Whether notify-driven scheduler wakeups are enabled.
    pub event_driven_scheduler_enable: bool,
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
    /// Recently completed heal statuses retained for status queries.
    completed_heals: Arc<Mutex<HashMap<String, CompletedHealStatus>>>,
    /// Client tokens merged into an existing task id.
    task_aliases: Arc<Mutex<HashMap<String, HealTaskAlias>>>,
    /// Heal tasks waiting for a retry backoff to expire.
    retrying_heals: Arc<Mutex<HashMap<String, RetryingHeal>>>,
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

struct HealQueueContext<'a> {
    heal_queue: &'a Arc<Mutex<PriorityHealQueue>>,
    active_heals: &'a Arc<Mutex<HashMap<String, Arc<HealTask>>>>,
    completed_heals: &'a Arc<Mutex<HashMap<String, CompletedHealStatus>>>,
    retrying_heals: &'a Arc<Mutex<HashMap<String, RetryingHeal>>>,
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

    fn admit_request_to_queue(
        queue: &mut PriorityHealQueue,
        request: HealRequest,
        config: &HealConfig,
        context: &'static str,
    ) -> HealAdmissionResult {
        let queue_len = queue.len();
        publish_heal_queue_length(queue);
        let queue_capacity = config.queue_size;

        if queue_len >= queue_capacity && !request.force_start {
            if Self::can_displace_queued_work(&request) && queue.can_displace_lower_priority(request.priority) {
                let request_id = request.id.clone();
                let priority = request.priority;
                let source = request.source;
                if let Some(displaced) = queue.push_displacing_lower_priority(request) {
                    publish_heal_queue_length(queue);
                    Self::record_admission_metric(source, HealAdmissionResult::Accepted, context);
                    warn!(
                        target: "rustfs::heal::manager",
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
                    );
                    return HealAdmissionResult::Accepted;
                }

                warn!(
                    target: "rustfs::heal::manager",
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
                );
                Self::record_admission_metric(source, HealAdmissionResult::Full, context);
                return HealAdmissionResult::Full;
            }

            let admission = Self::classify_full_admission(&request, config);
            Self::record_admission_metric(request.source, admission, context);
            match admission {
                HealAdmissionResult::Dropped(reason) => {
                    warn!(
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
                        reason = reason.as_str(),
                        result = "dropped_full",
                        "Heal queue request dropped"
                    );
                }
                HealAdmissionResult::Full => {
                    warn!(
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
                        result = "rejected_full",
                        "Heal queue request rejected"
                    );
                }
                HealAdmissionResult::Accepted | HealAdmissionResult::Merged => {}
            }
            return admission;
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
            return admission;
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
                if matches!(priority, HealPriority::High | HealPriority::Urgent) {
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
                HealAdmissionResult::Accepted
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
                HealAdmissionResult::Merged
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
        self.task_aliases
            .lock()
            .await
            .retain(|alias_id, alias| alias_id != task_id && alias.task_id != task_id);
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
            task_aliases: Arc::new(Mutex::new(HashMap::new())),
            retrying_heals: Arc::new(Mutex::new(HashMap::new())),
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
        self.task_aliases.lock().await.clear();
        self.retrying_heals.lock().await.clear();
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
    pub async fn submit_heal_request(&self, request: HealRequest) -> Result<HealAdmissionResult> {
        let config = self.config.read().await;
        let dedup_key = PriorityHealQueue::make_dedup_key(&request);

        // Keep this lock order aligned with the scheduler so a request cannot
        // slip between queued and active states while duplicate admission runs.
        let active_duplicate = {
            let active_heals = self.active_heals.lock().await;
            (!request.force_start)
                .then(|| active_heal_for_dedup_key(&active_heals, &dedup_key))
                .flatten()
        };
        if let Some((merged_task_id, _)) = active_duplicate {
            let admission = Self::duplicate_admission_for_request(&request, &config);

            match admission {
                HealAdmissionResult::Merged => {
                    self.insert_task_alias(&request.id, &merged_task_id).await;
                    info!(
                        target: "rustfs::heal::manager",
                        event = EVENT_HEAL_QUEUE_ADMISSION,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_MANAGER,
                        request_id = %request.id,
                        merged_task_id = %merged_task_id,
                        priority = ?request.priority,
                        result = "merged_active_duplicate",
                        "Heal queue admission decided"
                    );
                }
                HealAdmissionResult::Dropped(reason) => {
                    warn!(
                        target: "rustfs::heal::manager",
                        event = EVENT_HEAL_QUEUE_ADMISSION,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_MANAGER,
                        request_id = %request.id,
                        priority = ?request.priority,
                        reason = reason.as_str(),
                        result = "dropped_active_duplicate",
                        "Heal queue admission decided"
                    );
                }
                HealAdmissionResult::Accepted | HealAdmissionResult::Full => {}
            }

            return Ok(admission);
        }

        let retrying_duplicate = {
            let retrying_heals = self.retrying_heals.lock().await;
            (!request.force_start)
                .then(|| retrying_heal_for_dedup_key(&retrying_heals, &dedup_key))
                .flatten()
        };
        if let Some((merged_task_id, _)) = retrying_duplicate {
            let admission = Self::duplicate_admission_for_request(&request, &config);

            match admission {
                HealAdmissionResult::Merged => {
                    self.insert_task_alias(&request.id, &merged_task_id).await;
                    info!(
                        target: "rustfs::heal::manager",
                        event = EVENT_HEAL_QUEUE_ADMISSION,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_MANAGER,
                        request_id = %request.id,
                        merged_task_id = %merged_task_id,
                        priority = ?request.priority,
                        result = "merged_retrying_duplicate",
                        "Heal queue admission decided"
                    );
                }
                HealAdmissionResult::Dropped(reason) => {
                    warn!(
                        target: "rustfs::heal::manager",
                        event = EVENT_HEAL_QUEUE_ADMISSION,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_MANAGER,
                        request_id = %request.id,
                        priority = ?request.priority,
                        reason = reason.as_str(),
                        result = "dropped_retrying_duplicate",
                        "Heal queue admission decided"
                    );
                }
                HealAdmissionResult::Accepted | HealAdmissionResult::Full => {}
            }

            return Ok(admission);
        }

        let mut queue = self.heal_queue.lock().await;

        let queued_duplicate = (!request.force_start)
            .then(|| queue.request_for_dedup_key(&dedup_key).map(|queued| queued.id.clone()))
            .flatten();
        if let Some(merged_task_id) = queued_duplicate {
            let admission = Self::duplicate_admission_for_request(&request, &config);
            drop(queue);

            match admission {
                HealAdmissionResult::Merged => {
                    self.insert_task_alias(&request.id, &merged_task_id).await;
                    debug!(
                        target: "rustfs::heal::manager",
                        event = EVENT_HEAL_QUEUE_ADMISSION,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_MANAGER,
                        request_id = %request.id,
                        merged_task_id = %merged_task_id,
                        priority = ?request.priority,
                        result = "merged_duplicate",
                        "Heal queue request merged"
                    );
                }
                HealAdmissionResult::Dropped(reason) => {
                    warn!(
                        target: "rustfs::heal::manager",
                        event = EVENT_HEAL_QUEUE_ADMISSION,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_MANAGER,
                        request_id = %request.id,
                        priority = ?request.priority,
                        reason = reason.as_str(),
                        result = "dropped_duplicate",
                        "Heal queue request dropped"
                    );
                }
                HealAdmissionResult::Accepted | HealAdmissionResult::Full => {}
            }

            return Ok(admission);
        }

        let admission = Self::admit_request_to_queue(&mut queue, request, &config, "submit");
        let should_notify = matches!(admission, HealAdmissionResult::Accepted) && config.event_driven_scheduler_enable;
        drop(queue);

        if should_notify {
            self.notify.notify_one();
        }

        Ok(admission)
    }

    /// Get task status
    pub async fn get_task_status(&self, task_id: &str) -> Result<HealTaskStatus> {
        let canonical_task_id = self.canonical_task_id(task_id).await;
        {
            let active_heals = self.active_heals.lock().await;
            if let Some(task) = active_heals.get(&canonical_task_id) {
                return Ok(task.get_status().await);
            }
        }

        {
            let retrying_heals = self.retrying_heals.lock().await;
            if let Some(retrying) = retrying_heals.get(&canonical_task_id) {
                return Ok(retrying.status());
            }
        }

        {
            let mut completed_heals = self.completed_heals.lock().await;
            prune_completed_heal_statuses(&mut completed_heals);
            if let Some(completed) = completed_heals.get(&canonical_task_id)
                && completed_status_is_retrying(&completed.status)
            {
                return Ok(completed.status.clone());
            }
        }

        let queue = self.heal_queue.lock().await;
        if queue.contains_request_id(&canonical_task_id) {
            return Ok(HealTaskStatus::Pending);
        }
        drop(queue);

        let mut completed_heals = self.completed_heals.lock().await;
        prune_completed_heal_statuses(&mut completed_heals);
        if let Some(completed) = completed_heals.get(&canonical_task_id) {
            return Ok(completed.status.clone());
        }

        Err(Error::TaskNotFound {
            task_id: task_id.to_string(),
        })
    }

    pub async fn get_task_report(&self, task_id: &str) -> Result<HealTaskReport> {
        let canonical_task_id = self.canonical_task_id(task_id).await;
        {
            let active_heals = self.active_heals.lock().await;
            if let Some(task) = active_heals.get(&canonical_task_id) {
                return Ok(HealTaskReport {
                    status: task.get_status().await,
                    result_items: task.get_result_items().await,
                    progress: Some(task.get_progress().await),
                });
            }
        }

        {
            let retrying_heals = self.retrying_heals.lock().await;
            if let Some(retrying) = retrying_heals.get(&canonical_task_id) {
                return Ok(HealTaskReport {
                    status: retrying.status(),
                    result_items: Vec::new(),
                    progress: None,
                });
            }
        }

        {
            let mut completed_heals = self.completed_heals.lock().await;
            prune_completed_heal_statuses(&mut completed_heals);
            if let Some(completed) = completed_heals.get(&canonical_task_id)
                && completed_status_is_retrying(&completed.status)
            {
                return Ok(HealTaskReport {
                    status: completed.status.clone(),
                    result_items: completed.result_items.clone(),
                    progress: None,
                });
            }
        }

        {
            let queue = self.heal_queue.lock().await;
            if queue.contains_request_id(&canonical_task_id) {
                return Ok(HealTaskReport {
                    status: HealTaskStatus::Pending,
                    result_items: Vec::new(),
                    progress: None,
                });
            }
        }

        let mut completed_heals = self.completed_heals.lock().await;
        prune_completed_heal_statuses(&mut completed_heals);
        if let Some(completed) = completed_heals.get(&canonical_task_id) {
            return Ok(HealTaskReport {
                status: completed.status.clone(),
                result_items: completed.result_items.clone(),
                progress: None,
            });
        }

        Err(Error::TaskNotFound {
            task_id: task_id.to_string(),
        })
    }

    pub async fn get_task_report_for_path(&self, heal_path: &str, task_id: &str) -> Result<HealTaskReport> {
        let canonical_task_id = self.canonical_task_id(task_id).await;
        {
            let active_heals = self.active_heals.lock().await;
            if let Some(task) = active_heals.get(&canonical_task_id)
                && heal_type_matches_path(&task.heal_type, heal_path)
            {
                return Ok(HealTaskReport {
                    status: task.get_status().await,
                    result_items: task.get_result_items().await,
                    progress: Some(task.get_progress().await),
                });
            }
        }

        {
            let retrying_heals = self.retrying_heals.lock().await;
            if let Some(retrying) = retrying_heals.get(&canonical_task_id)
                && heal_type_matches_path(&retrying.request.heal_type, heal_path)
            {
                return Ok(HealTaskReport {
                    status: retrying.status(),
                    result_items: Vec::new(),
                    progress: None,
                });
            }
        }

        {
            let mut completed_heals = self.completed_heals.lock().await;
            prune_completed_heal_statuses(&mut completed_heals);
            if let Some(completed) = completed_heals.get(&canonical_task_id)
                && heal_type_matches_path(&completed.heal_type, heal_path)
                && completed_status_is_retrying(&completed.status)
            {
                return Ok(HealTaskReport {
                    status: completed.status.clone(),
                    result_items: completed.result_items.clone(),
                    progress: None,
                });
            }
        }

        {
            let queue = self.heal_queue.lock().await;
            if queue.contains_request_id_matching_path(&canonical_task_id, heal_path) {
                return Ok(HealTaskReport {
                    status: HealTaskStatus::Pending,
                    result_items: Vec::new(),
                    progress: None,
                });
            }
        }

        {
            let mut completed_heals = self.completed_heals.lock().await;
            prune_completed_heal_statuses(&mut completed_heals);
            if let Some(completed) = completed_heals.get(&canonical_task_id)
                && heal_type_matches_path(&completed.heal_type, heal_path)
            {
                return Ok(HealTaskReport {
                    status: completed.status.clone(),
                    result_items: completed.result_items.clone(),
                    progress: None,
                });
            }
        }

        if self.path_has_task(heal_path).await {
            return Err(Error::InvalidClientToken);
        }

        Err(Error::TaskNotFound {
            task_id: task_id.to_string(),
        })
    }

    /// Get task status for a path-bound client token.
    ///
    /// If the token is unknown but no task remains for the path, the caller can
    /// treat it as an already-finished sequence. If the path still has a live or
    /// recently completed task, a different token is invalid for that path.
    pub async fn get_task_status_for_path(&self, heal_path: &str, task_id: &str) -> Result<HealTaskStatus> {
        let canonical_task_id = self.canonical_task_id(task_id).await;
        {
            let active_heals = self.active_heals.lock().await;
            if let Some(task) = active_heals.get(&canonical_task_id)
                && heal_type_matches_path(&task.heal_type, heal_path)
            {
                return Ok(task.get_status().await);
            }
        }

        {
            let retrying_heals = self.retrying_heals.lock().await;
            if let Some(retrying) = retrying_heals.get(&canonical_task_id)
                && heal_type_matches_path(&retrying.request.heal_type, heal_path)
            {
                return Ok(retrying.status());
            }
        }

        {
            let mut completed_heals = self.completed_heals.lock().await;
            prune_completed_heal_statuses(&mut completed_heals);
            if let Some(completed) = completed_heals.get(&canonical_task_id)
                && heal_type_matches_path(&completed.heal_type, heal_path)
                && completed_status_is_retrying(&completed.status)
            {
                return Ok(completed.status.clone());
            }
        }

        {
            let queue = self.heal_queue.lock().await;
            if queue.contains_request_id_matching_path(&canonical_task_id, heal_path) {
                return Ok(HealTaskStatus::Pending);
            }
        }

        {
            let mut completed_heals = self.completed_heals.lock().await;
            prune_completed_heal_statuses(&mut completed_heals);
            if let Some(completed) = completed_heals.get(&canonical_task_id)
                && heal_type_matches_path(&completed.heal_type, heal_path)
            {
                return Ok(completed.status.clone());
            }
        }

        if self.path_has_task(heal_path).await {
            return Err(Error::InvalidClientToken);
        }

        Err(Error::TaskNotFound {
            task_id: task_id.to_string(),
        })
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
        completed_heals
            .values()
            .any(|completed| heal_type_matches_path(&completed.heal_type, heal_path))
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
        let (queue_length, queued_by_priority, queued_by_source) = {
            let queue = self.heal_queue.lock().await;
            let (priority, source) = queue.operation_counts();
            publish_heal_queue_length(&queue);
            (usize_to_u64_saturated(queue.len()), priority, source)
        };

        let (active_tasks, active_by_priority, active_by_source) = {
            let active_heals = self.active_heals.lock().await;
            let mut priority = HealPriorityCounts::default();
            let mut source = HealSourceCounts::default();
            for task in active_heals.values() {
                priority.increment(task.priority);
                source.increment(task.source);
            }
            publish_active_heal_count(&active_heals);
            (usize_to_u64_saturated(active_heals.len()), priority, source)
        };

        HealOperationsSnapshot {
            queue_length,
            active_tasks,
            queued_by_priority,
            active_by_priority,
            queued_by_source,
            active_by_source,
        }
    }

    pub async fn active_progress_snapshot(&self) -> Option<HealProgress> {
        let active_heals = self.active_heals.lock().await;
        if active_heals.is_empty() {
            return None;
        }

        let mut snapshot = HealProgress::default();
        for task in active_heals.values() {
            let progress = task.get_progress().await;
            snapshot.objects_scanned = snapshot.objects_scanned.saturating_add(progress.objects_scanned);
            snapshot.objects_healed = snapshot.objects_healed.saturating_add(progress.objects_healed);
            snapshot.objects_failed = snapshot.objects_failed.saturating_add(progress.objects_failed);
            snapshot.bytes_processed = snapshot.bytes_processed.saturating_add(progress.bytes_processed);
        }
        Some(snapshot)
    }

    /// Start scheduler
    async fn start_scheduler(&self) -> Result<()> {
        let config = self.config.clone();
        let heal_queue = self.heal_queue.clone();
        let active_heals = self.active_heals.clone();
        let completed_heals = self.completed_heals.clone();
        let retrying_heals = self.retrying_heals.clone();
        let cancel_token = self.cancel_token.clone();
        let statistics = self.statistics.clone();
        let storage = self.storage.clone();
        let notify = self.notify.clone();
        let workload_provider = self.workload_provider.clone();

        tokio::spawn(async move {
            let mut interval = interval(config.read().await.heal_interval);

            loop {
                let event_driven_scheduler_enable = config.read().await.event_driven_scheduler_enable;
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!(
                            target: "rustfs::heal::manager",
                            event = EVENT_HEAL_SCHEDULER_STATE,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_MANAGER,
                            state = "shutdown",
                            "Heal scheduler stopped"
                        );
                        break;
                    }
                    _ = notify.notified(), if event_driven_scheduler_enable => {
                        Self::process_heal_queue(HealQueueContext {
                            heal_queue: &heal_queue,
                            active_heals: &active_heals,
                            completed_heals: &completed_heals,
                            retrying_heals: &retrying_heals,
                            config: &config,
                            statistics: &statistics,
                            storage: &storage,
                            notify: &notify,
                            cancel_token: &cancel_token,
                            workload_provider: &workload_provider,
                        })
                        .await;
                    }
                    _ = interval.tick() => {
                        Self::process_heal_queue(HealQueueContext {
                            heal_queue: &heal_queue,
                            active_heals: &active_heals,
                            completed_heals: &completed_heals,
                            retrying_heals: &retrying_heals,
                            config: &config,
                            statistics: &statistics,
                            storage: &storage,
                            notify: &notify,
                            cancel_token: &cancel_token,
                            workload_provider: &workload_provider,
                        })
                        .await;
                    }
                }
            }
        });

        Ok(())
    }

    /// Start background task to auto scan local disks and enqueue erasure set heal requests
    async fn start_auto_disk_scanner(&self) -> Result<()> {
        let config = self.config.clone();
        let heal_queue = self.heal_queue.clone();
        let active_heals = self.active_heals.clone();
        let cancel_token = self.cancel_token.clone();
        let storage = self.storage.clone();
        let notify = self.notify.clone();
        let mut duration = {
            let config = config.read().await;
            config.heal_interval
        };
        if duration < Duration::from_secs(10) {
            duration = Duration::from_secs(10);
        }
        info!(
            target: "rustfs::heal::manager",
            event = EVENT_HEAL_AUTO_SCAN_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
            state = "started",
            interval = ?duration,
            "Heal auto disk scanner started"
        );

        tokio::spawn(async move {
            let mut interval = interval(duration);

            loop {
                let mut candidate_count = 0usize;
                let mut skipped_duplicate_count = 0usize;
                let mut skipped_invalid_count = 0usize;
                let mut enqueued_count = 0usize;
                let mut not_enqueued_count = 0usize;
                let mut dropped_count = 0usize;
                let mut full_count = 0usize;
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!(
                            target: "rustfs::heal::manager",
                            event = EVENT_HEAL_AUTO_SCAN_STATE,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                            state = "shutdown",
                            "Heal auto disk scanner stopped"
                        );
                        break;
                    }
                    _ = interval.tick() => {
                        // Build list of endpoints that need healing
                        let mut endpoints = Vec::new();
                        let mut seen_returning_sets = HashSet::new();
                        for (_, disk_opt) in GLOBAL_LOCAL_DISK_MAP.read().await.iter() {
                            if let Some(disk) = disk_opt {
                                let endpoint = disk.endpoint();
                                let runtime_state = disk.runtime_state();
                                let set_disk_id =
                                    crate::heal::utils::format_set_disk_id_from_i32(endpoint.pool_idx, endpoint.set_idx);

                                // detect unformatted disk via get_disk_id()
                                match disk.get_disk_id().await {
                                    Err(DiskError::UnformattedDisk) => {
                                        candidate_count += 1;
                                        debug!(
                                            target: "rustfs::heal::manager",
                                            event = EVENT_HEAL_AUTO_SCAN_DISK,
                                            component = LOG_COMPONENT_HEAL,
                                            subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                            endpoint = %endpoint,
                                            disk_state = "unformatted",
                                            "Heal auto-scan candidate detected"
                                        );
                                        endpoints.push(endpoint);
                                    }
                                    Err(e) => {
                                        warn!(
                                            target: "rustfs::heal::manager",
                                            event = EVENT_HEAL_AUTO_SCAN_DISK,
                                            component = LOG_COMPONENT_HEAL,
                                            subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                            endpoint = %endpoint,
                                            disk_state = "check_failed",
                                            error = ?e,
                                            "Heal auto-scan disk inspection failed"
                                        );
                                    }
                                    Ok(_) => {
                                        if runtime_state.as_str() == "returning"
                                            && let Some(set_disk_id) = set_disk_id
                                            && seen_returning_sets.insert(set_disk_id.clone())
                                        {
                                            candidate_count += 1;
                                            debug!(
                                                target: "rustfs::heal::manager",
                                                event = EVENT_HEAL_AUTO_SCAN_DISK,
                                                component = LOG_COMPONENT_HEAL,
                                                subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                                endpoint = %endpoint,
                                                set_disk_id,
                                                disk_state = "returning",
                                                "Heal auto-scan returning disk candidate detected"
                                            );
                                            endpoints.push(endpoint);
                                        }
                                    }
                                }
                            }
                        }

                        if endpoints.is_empty() {
                            debug!(
                                target: "rustfs::heal::manager",
                                event = EVENT_HEAL_AUTO_SCAN_STATE,
                                component = LOG_COMPONENT_HEAL,
                                subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                state = "idle",
                                "Heal auto disk scanner idle"
                            );
                            continue;
                        }

                        // Get bucket list for erasure set healing
                        let buckets = match storage.list_buckets().await {
                            Ok(buckets) => buckets.iter().map(|b| b.name.clone()).collect::<Vec<String>>(),
                            Err(e) => {
                                error!(
                                    target: "rustfs::heal::manager",
                                    event = EVENT_HEAL_AUTO_SCAN_STATE,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                    state = "bucket_list_failed",
                                    error = %e,
                                    "Heal auto-scan bucket listing failed"
                                );
                                continue;
                            }
                        };

                        // Create erasure set heal requests for each endpoint
                        for ep in endpoints {
                            let Some(set_disk_id) =
                                crate::heal::utils::format_set_disk_id_from_i32(ep.pool_idx, ep.set_idx)
                            else {
                                warn!(
                                    target: "rustfs::heal::manager",
                                    event = EVENT_HEAL_AUTO_SCAN_ENQUEUE,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                    endpoint = %ep,
                                    result = "skipped_invalid_set_disk_id",
                                    "Heal auto-scan enqueue skipped"
                                );
                                skipped_invalid_count += 1;
                                continue;
                            };
                            // skip if already queued or healing
                            // Use consistent lock order: queue first, then active_heals to avoid deadlock
                            let mut skip = false;
                            {
                                let queue = heal_queue.lock().await;
                                if queue.contains_erasure_set(&set_disk_id) {
                                    skip = true;
                                }
                            }
                            if !skip {
                                let active = active_heals.lock().await;
                                if active.values().any(|task| {
                                    matches!(
                                        &task.heal_type,
                                        crate::heal::task::HealType::ErasureSet { set_disk_id: active_id, .. }
                                        if active_id == &set_disk_id
                                    )
                                }) {
                                    skip = true;
                                }
                            }

                            if skip {
                                skipped_duplicate_count += 1;
                                debug!(
                                    target: "rustfs::heal::manager",
                                    event = EVENT_HEAL_AUTO_SCAN_ENQUEUE,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                    endpoint = %ep,
                                    set_disk_id,
                                    result = "skipped_duplicate",
                                    "Heal auto-scan duplicate skipped"
                                );
                                continue;
                            }

                            // enqueue erasure set heal request for this disk
                            let mut req = HealRequest::new(
                                HealType::ErasureSet {
                                    buckets: buckets.clone(),
                                    set_disk_id: set_disk_id.clone(),
                                },
                                HealOptions {
                                    timeout: None,
                                    ..HealOptions::default()
                                },
                                HealPriority::Low,
                            );
                            req.source = HealRequestSource::AutoHeal;
                            let config = config.read().await;
                            let mut queue = heal_queue.lock().await;
                            let admission = Self::admit_request_to_queue(&mut queue, req, &config, "auto_scan");
                            let should_notify =
                                matches!(admission, HealAdmissionResult::Accepted) && config.event_driven_scheduler_enable;
                            drop(queue);
                            drop(config);
                            if matches!(admission, HealAdmissionResult::Accepted) {
                                if should_notify {
                                    notify.notify_one();
                                }
                                enqueued_count += 1;
                                debug!(
                                    target: "rustfs::heal::manager",
                                    event = EVENT_HEAL_AUTO_SCAN_ENQUEUE,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                    endpoint = %ep,
                                    set_disk_id,
                                    bucket_count = buckets.len(),
                                    result = "enqueued",
                                    "Heal auto-scan task enqueued"
                                );
                            } else {
                                if matches!(admission, HealAdmissionResult::Merged) {
                                    skipped_duplicate_count += 1;
                                } else {
                                    not_enqueued_count += 1;
                                }
                                if matches!(admission, HealAdmissionResult::Full) {
                                    full_count += 1;
                                }
                                if matches!(admission, HealAdmissionResult::Dropped(_)) {
                                    dropped_count += 1;
                                }
                                debug!(
                                    target: "rustfs::heal::manager",
                                    event = EVENT_HEAL_AUTO_SCAN_ENQUEUE,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                    endpoint = %ep,
                                    set_disk_id,
                                    bucket_count = buckets.len(),
                                    admission = admission.result_label(),
                                    reason = admission.reason_label(),
                                    result = "not_enqueued",
                                    "Heal auto-scan task not enqueued"
                                );
                            }
                        }
                        info!(
                            target: "rustfs::heal::manager",
                            event = EVENT_HEAL_AUTO_SCAN_STATE,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                            state = "cycle_completed",
                            candidate_count,
                            enqueued_count,
                            not_enqueued_count,
                            dropped_count,
                            full_count,
                            skipped_duplicate_count,
                            skipped_invalid_count,
                            "Heal auto-scan cycle completed"
                        );
                    }
                }
            }
        });
        Ok(())
    }

    /// Process heal queue
    /// Processes multiple tasks per cycle when capacity allows and queue has high-priority items
    async fn process_heal_queue(context: HealQueueContext<'_>) {
        let HealQueueContext {
            heal_queue,
            active_heals,
            completed_heals,
            retrying_heals,
            config,
            statistics,
            storage,
            notify,
            cancel_token,
            workload_provider,
        } = context;

        let config = config.read().await;
        let mainline_pressure = Self::mainline_throttle_active(&config, workload_provider);
        let mut active_heals_guard = active_heals.lock().await;
        publish_active_heal_count(&active_heals_guard);

        // Check if new heal tasks can be started
        let active_count = active_heals_guard.len();
        if active_count >= config.max_concurrent_heals {
            return;
        }

        // Calculate how many tasks we can start this cycle
        let available_slots = config.max_concurrent_heals - active_count;

        let mut queue = heal_queue.lock().await;
        let queue_len = queue.len();
        publish_heal_queue_length(&queue);

        if queue_len == 0 {
            return;
        }

        let mut running_per_set = running_erasure_set_counts(&active_heals_guard);
        let mut tasks_started = 0usize;
        let mut delayed_by_mainline_throttle = false;

        for _ in 0..available_slots {
            let selected_request = if config.set_bulkhead_enable || mainline_pressure.is_some() {
                let max_concurrent_per_set = config.max_concurrent_per_set;
                let (selected_request, skipped_sets) = queue.pop_runnable_with_skips(
                    |request| {
                        let set_allowed = !config.set_bulkhead_enable
                            || can_schedule_request(request, &running_per_set, max_concurrent_per_set);
                        let mainline_allowed = mainline_pressure.is_none() || Self::request_bypasses_mainline_throttle(request);
                        set_allowed && mainline_allowed
                    },
                    |request| heal_request_set_key(request).map(|_| heal_request_set_metric_label(request)),
                );
                for skipped_set in skipped_sets {
                    record_scheduler_skip(&skipped_set);
                }
                selected_request
            } else {
                queue.pop_next()
            };

            if let Some(request) = selected_request {
                let task_priority = request.priority;
                let task_type_label = heal_request_type_label(&request).to_string();
                let task_set_label = heal_request_set_metric_label(&request);
                if config.set_bulkhead_enable
                    && let Some(set_key) = heal_request_set_key(&request)
                {
                    *running_per_set.entry(set_key).or_insert(0) += 1;
                }
                let task = Arc::new(HealTask::from_request(request, storage.clone()));
                let task_id = task.id.clone();
                active_heals_guard.insert(task_id.clone(), task.clone());
                publish_active_heal_count(&active_heals_guard);
                update_task_running_metric_for_task(&active_heals_guard, task.as_ref());
                let active_heals_clone = active_heals.clone();
                let heal_queue_clone = heal_queue.clone();
                let completed_heals_clone = completed_heals.clone();
                let retrying_heals_clone = retrying_heals.clone();
                let statistics_clone = statistics.clone();
                let notify_clone = notify.clone();
                let manager_cancel_token = cancel_token.clone();
                let task_type_label_for_spawn = task_type_label.clone();
                let task_set_label_for_spawn = task_set_label.clone();
                let config_for_spawn = config.clone();

                // start heal task
                tokio::spawn(async move {
                    debug!(
                        target: "rustfs::heal::manager",
                        event = EVENT_HEAL_SCHEDULER_STATE,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_MANAGER,
                        task_id,
                        priority = ?task_priority,
                        heal_type = %task_type_label_for_spawn,
                        set = %task_set_label_for_spawn,
                        state = "task_started",
                        "Heal scheduler task started"
                    );
                    let result = task.execute().await;
                    let retry_request = retry_request_for_result(task.as_ref(), &result);
                    match &result {
                        Ok(_) => {
                            debug!(
                                target: "rustfs::heal::manager",
                                event = EVENT_HEAL_SCHEDULER_STATE,
                                component = LOG_COMPONENT_HEAL,
                                subsystem = LOG_SUBSYSTEM_MANAGER,
                                task_id,
                                heal_type = %task_type_label_for_spawn,
                                set = %task_set_label_for_spawn,
                                state = "task_completed",
                                "Heal scheduler task completed"
                            );
                        }
                        Err(e) => {
                            let will_retry = retry_request.is_some();
                            if will_retry {
                                warn!(
                                    target: "rustfs::heal::manager",
                                    event = EVENT_HEAL_SCHEDULER_STATE,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_MANAGER,
                                    task_id,
                                    heal_type = %task_type_label_for_spawn,
                                    set = %task_set_label_for_spawn,
                                    state = "task_retrying",
                                    retry_attempt = task.retry_attempts.saturating_add(1),
                                    error = %e,
                                    "Heal scheduler task retrying"
                                );
                            } else {
                                error!(
                                    target: "rustfs::heal::manager",
                                    event = EVENT_HEAL_SCHEDULER_STATE,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_MANAGER,
                                    task_id,
                                    heal_type = %task_type_label_for_spawn,
                                    set = %task_set_label_for_spawn,
                                    state = "task_failed",
                                    error = %e,
                                    "Heal scheduler task failed"
                                );
                            }
                        }
                    }
                    let retry_request_for_status = retry_request.as_ref().map(|(request, _, error)| HealTaskStatus::Retrying {
                        error: error.clone(),
                        retry_attempt: request.retry_attempts,
                    });
                    let retry_request_for_queue = retry_request;
                    let mut active_heals_guard = active_heals_clone.lock().await;
                    if let Some(completed_task) = active_heals_guard.remove(&task_id) {
                        publish_active_heal_count(&active_heals_guard);
                        update_task_running_metric_for_task(&active_heals_guard, completed_task.as_ref());
                        let completed_status = if let Some(status) = retry_request_for_status {
                            status
                        } else {
                            completed_task.get_status().await
                        };
                        let completed_status_entry = CompletedHealStatus {
                            heal_type: completed_task.heal_type.clone(),
                            status: completed_status.clone(),
                            result_items: completed_task.get_result_items().await,
                            completed_at: SystemTime::now(),
                        };
                        let mut completed_heals_guard = completed_heals_clone.lock().await;
                        prune_completed_heal_statuses(&mut completed_heals_guard);
                        completed_heals_guard.insert(task_id.clone(), completed_status_entry);
                        // update statistics
                        let mut stats = statistics_clone.write().await;
                        match completed_status {
                            HealTaskStatus::Completed => {
                                stats.update_task_completion(true);
                            }
                            HealTaskStatus::Retrying { .. } => {}
                            _ => {
                                stats.update_task_completion(false);
                            }
                        }
                        stats.update_running_tasks(active_heals_guard.len() as u64);
                    }

                    if let Some((retry_request, retry_delay, retry_error)) = retry_request_for_queue {
                        let retry_request_id = retry_request.id.clone();
                        let retry_attempt = retry_request.retry_attempts;
                        let retry_key = PriorityHealQueue::make_dedup_key(&retry_request);
                        let retry_priority = retry_request.priority;
                        let retry_active_heals = active_heals_clone.clone();
                        let retry_heal_queue = heal_queue_clone.clone();
                        let retrying_heals_for_spawn = retrying_heals_clone.clone();
                        let retry_completed_heals = completed_heals_clone.clone();
                        let retry_notify = notify_clone.clone();
                        let retry_cancel_token = CancellationToken::new();
                        let retry_manager_cancel_token = manager_cancel_token.clone();
                        let retry_config = config_for_spawn.clone();
                        retrying_heals_clone.lock().await.insert(
                            retry_request_id.clone(),
                            RetryingHeal {
                                request: retry_request.clone(),
                                error: retry_error.clone(),
                                cancel_token: retry_cancel_token.clone(),
                            },
                        );
                        tokio::spawn(async move {
                            loop {
                                tokio::select! {
                                    _ = retry_cancel_token.cancelled() => {
                                        info!(
                                            target: "rustfs::heal::manager",
                                            event = EVENT_HEAL_QUEUE_ADMISSION,
                                            component = LOG_COMPONENT_HEAL,
                                            subsystem = LOG_SUBSYSTEM_MANAGER,
                                            request_id = %retry_request_id,
                                            priority = ?retry_priority,
                                            retry_attempt,
                                            result = "retry_cancelled",
                                            "Heal retry admission decided"
                                        );
                                        return;
                                    }
                                    _ = retry_manager_cancel_token.cancelled() => {
                                        retrying_heals_for_spawn.lock().await.remove(&retry_request_id);
                                        return;
                                    }
                                    _ = sleep(retry_delay) => {}
                                }

                                {
                                    let retrying_heals_guard = retrying_heals_for_spawn.lock().await;
                                    if !retrying_heals_guard.contains_key(&retry_request_id) {
                                        return;
                                    }
                                }

                                let active_duplicate = {
                                    let active_heals_guard = retry_active_heals.lock().await;
                                    active_heals_contains_dedup_key(&active_heals_guard, &retry_key)
                                };
                                if active_duplicate {
                                    retrying_heals_for_spawn.lock().await.remove(&retry_request_id);
                                    info!(
                                        target: "rustfs::heal::manager",
                                        event = EVENT_HEAL_QUEUE_ADMISSION,
                                        component = LOG_COMPONENT_HEAL,
                                        subsystem = LOG_SUBSYSTEM_MANAGER,
                                        request_id = %retry_request_id,
                                        priority = ?retry_priority,
                                        retry_attempt,
                                        result = "retry_merged_active_duplicate",
                                        "Heal retry admission decided"
                                    );
                                    return;
                                }

                                let mut queue = retry_heal_queue.lock().await;
                                let admission =
                                    Self::admit_request_to_queue(&mut queue, retry_request.clone(), &retry_config, "retry");
                                let should_notify = matches!(admission, HealAdmissionResult::Accepted)
                                    && retry_config.event_driven_scheduler_enable;
                                match admission {
                                    HealAdmissionResult::Accepted => {
                                        drop(queue);
                                        retrying_heals_for_spawn.lock().await.remove(&retry_request_id);
                                        retry_completed_heals.lock().await.remove(&retry_request_id);
                                        info!(
                                            target: "rustfs::heal::manager",
                                            event = EVENT_HEAL_QUEUE_ADMISSION,
                                            component = LOG_COMPONENT_HEAL,
                                            subsystem = LOG_SUBSYSTEM_MANAGER,
                                            request_id = %retry_request_id,
                                            priority = ?retry_priority,
                                            retry_attempt,
                                            retry_delay_ms = retry_delay.as_millis(),
                                            error = %retry_error,
                                            result = "retry_enqueued",
                                            "Heal retry admission decided"
                                        );
                                        if should_notify {
                                            retry_notify.notify_one();
                                        }
                                        return;
                                    }
                                    HealAdmissionResult::Merged => {
                                        drop(queue);
                                        retrying_heals_for_spawn.lock().await.remove(&retry_request_id);
                                        info!(
                                            target: "rustfs::heal::manager",
                                            event = EVENT_HEAL_QUEUE_ADMISSION,
                                            component = LOG_COMPONENT_HEAL,
                                            subsystem = LOG_SUBSYSTEM_MANAGER,
                                            request_id = %retry_request_id,
                                            priority = ?retry_priority,
                                            retry_attempt,
                                            result = "retry_merged_duplicate",
                                            "Heal retry admission decided"
                                        );
                                        return;
                                    }
                                    HealAdmissionResult::Full => {
                                        warn!(
                                            target: "rustfs::heal::manager",
                                            event = EVENT_HEAL_QUEUE_ADMISSION,
                                            component = LOG_COMPONENT_HEAL,
                                            subsystem = LOG_SUBSYSTEM_MANAGER,
                                            request_id = %retry_request_id,
                                            priority = ?retry_priority,
                                            retry_attempt,
                                            result = "retry_rejected_full",
                                            "Heal retry admission decided"
                                        );
                                    }
                                    HealAdmissionResult::Dropped(reason) => {
                                        debug!(
                                            target: "rustfs::heal::manager",
                                            event = EVENT_HEAL_QUEUE_ADMISSION,
                                            component = LOG_COMPONENT_HEAL,
                                            subsystem = LOG_SUBSYSTEM_MANAGER,
                                            request_id = %retry_request_id,
                                            priority = ?retry_priority,
                                            retry_attempt,
                                            reason = reason.as_str(),
                                            result = "retry_dropped",
                                            "Heal retry admission decided"
                                        );
                                    }
                                }
                            }
                        });
                    }
                    notify_clone.notify_one();
                });
                tasks_started += 1;
            } else {
                delayed_by_mainline_throttle = mainline_pressure.is_some();
                break;
            }
        }

        // Update statistics for all started tasks
        let mut stats = statistics.write().await;
        stats.total_tasks += tasks_started as u64;
        stats.update_running_tasks(active_heals_guard.len() as u64);
        publish_active_heal_count(&active_heals_guard);
        publish_heal_queue_length(&queue);

        if delayed_by_mainline_throttle && let Some(pressure) = mainline_pressure {
            Self::record_mainline_throttle_delay(pressure, &config);
            Self::schedule_mainline_throttle_recheck(notify.clone(), config.mainline_max_sleep);
        }

        // Log queue status if items remain
        if !queue.is_empty() {
            let remaining = queue.len();
            if remaining > 10 {
                info!(
                    target: "rustfs::heal::manager",
                    event = EVENT_HEAL_QUEUE_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_MANAGER,
                    queue_len = remaining,
                    active_tasks = active_heals_guard.len(),
                    state = "backlog_high",
                    "Heal queue backlog high"
                );
            }
        }
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

fn heal_request_set_key(request: &HealRequest) -> Option<String> {
    match &request.heal_type {
        HealType::ErasureSet { set_disk_id, .. } => Some(set_disk_id.clone()),
        _ => None,
    }
}

fn heal_request_type_label(request: &HealRequest) -> &'static str {
    match &request.heal_type {
        HealType::Cluster => "cluster",
        HealType::Object { .. } => "object",
        HealType::Bucket { .. } => "bucket",
        HealType::Prefix { .. } => "prefix",
        HealType::ErasureSet { .. } => "erasure_set",
        HealType::Metadata { .. } => "metadata",
        HealType::MRF { .. } => "mrf",
        HealType::ECDecode { .. } => "ec_decode",
    }
}

fn heal_request_set_metric_label(request: &HealRequest) -> String {
    heal_request_set_key(request).unwrap_or_else(|| match (request.options.pool_index, request.options.set_index) {
        (Some(pool), Some(set)) => format!("pool_{pool}_set_{set}"),
        _ => "global".to_string(),
    })
}

fn record_scheduler_skip(set_label: &str) {
    counter!(
        "rustfs_heal_scheduler_skip_total",
        "reason" => "set_limit".to_string(),
        "set" => set_label.to_string()
    )
    .increment(1);
}

fn update_task_running_metric_for_task(active_heals: &HashMap<String, Arc<HealTask>>, task: &HealTask) {
    let type_label = task.metric_type_label();
    let set_label = task.metric_set_label();
    let count = active_heals
        .values()
        .filter(|active_task| active_task.metric_type_label() == type_label && active_task.metric_set_label() == set_label)
        .count();

    gauge!(
        "rustfs_heal_task_running",
        "type" => type_label.to_string(),
        "set" => set_label.clone()
    )
    .set(count as f64);
}

fn running_erasure_set_counts(active_heals: &HashMap<String, Arc<HealTask>>) -> HashMap<String, usize> {
    let mut running = HashMap::new();
    for task in active_heals.values() {
        if let HealType::ErasureSet { set_disk_id, .. } = &task.heal_type {
            *running.entry(set_disk_id.clone()).or_insert(0) += 1;
        }
    }
    running
}

fn prune_completed_heal_statuses(completed_heals: &mut HashMap<String, CompletedHealStatus>) {
    let Ok(now) = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) else {
        return;
    };

    completed_heals.retain(|_, completed| {
        completed
            .completed_at
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|completed_at| now.saturating_sub(completed_at) <= KEEP_HEAL_TASK_STATUS_DURATION)
            .unwrap_or(false)
    });
}

fn can_schedule_request(request: &HealRequest, running_per_set: &HashMap<String, usize>, max_concurrent_per_set: usize) -> bool {
    match heal_request_set_key(request) {
        Some(set_key) => running_per_set.get(&set_key).copied().unwrap_or(0) < max_concurrent_per_set,
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heal::storage::{HealObjectInfo, HealStorageAPI};
    use crate::heal::task::{HealOptions, HealPriority, HealRequest, HealTask, HealType};
    use rustfs_common::heal_channel::{HealOpts, HealRequestSource};
    use rustfs_concurrency::{WorkloadAdmissionRegistrySnapshot, WorkloadAdmissionSnapshot};
    use rustfs_madmin::heal_commands::HealResultItem;

    use super::super::{DiskStore, Endpoint, storage_api::status::BucketInfo};

    #[derive(Debug)]
    struct FixedWorkloadProvider {
        class: WorkloadClass,
        active: usize,
        limit: usize,
        state: AdmissionState,
    }

    impl WorkloadAdmissionSnapshotProvider for FixedWorkloadProvider {
        fn workload_admission_snapshot(&self) -> WorkloadAdmissionRegistrySnapshot {
            WorkloadAdmissionRegistrySnapshot::new(vec![WorkloadAdmissionSnapshot::new(self.class, self.state).with_counts(
                Some(self.active),
                None,
                Some(self.limit),
            )])
        }
    }

    async fn process_manager_queue_once(manager: &HealManager) {
        HealManager::process_heal_queue(HealQueueContext {
            heal_queue: &manager.heal_queue,
            active_heals: &manager.active_heals,
            completed_heals: &manager.completed_heals,
            retrying_heals: &manager.retrying_heals,
            config: &manager.config,
            statistics: &manager.statistics,
            storage: &manager.storage,
            notify: &manager.notify,
            cancel_token: &manager.cancel_token,
            workload_provider: &manager.workload_provider,
        })
        .await;
    }

    struct MockStorage;

    #[async_trait::async_trait]
    impl HealStorageAPI for MockStorage {
        async fn get_object_meta(&self, _bucket: &str, _object: &str) -> Result<Option<HealObjectInfo>> {
            Ok(None)
        }

        async fn get_object_data(&self, _bucket: &str, _object: &str) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }

        async fn put_object_data(&self, _bucket: &str, _object: &str, _data: &[u8]) -> Result<()> {
            Ok(())
        }

        async fn delete_object(&self, _bucket: &str, _object: &str) -> Result<()> {
            Ok(())
        }

        async fn verify_object_integrity(&self, _bucket: &str, _object: &str) -> Result<bool> {
            Ok(true)
        }

        async fn ec_decode_rebuild(&self, _bucket: &str, _object: &str) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }

        async fn get_disk_status(&self, _endpoint: &Endpoint) -> Result<crate::heal::storage::DiskStatus> {
            Ok(crate::heal::storage::DiskStatus::Ok)
        }

        async fn format_disk(&self, _endpoint: &Endpoint) -> Result<()> {
            Ok(())
        }

        async fn get_bucket_info(&self, _bucket: &str) -> Result<Option<BucketInfo>> {
            Ok(None)
        }

        async fn heal_bucket_metadata(&self, _bucket: &str) -> Result<()> {
            Ok(())
        }

        async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
            Ok(Vec::new())
        }

        async fn object_exists(&self, _bucket: &str, _object: &str) -> Result<bool> {
            Ok(false)
        }

        async fn get_object_size(&self, _bucket: &str, _object: &str) -> Result<Option<u64>> {
            Ok(None)
        }

        async fn get_object_checksum(&self, _bucket: &str, _object: &str) -> Result<Option<String>> {
            Ok(None)
        }

        async fn heal_object(
            &self,
            _bucket: &str,
            _object: &str,
            _version_id: Option<&str>,
            _opts: &HealOpts,
        ) -> Result<(HealResultItem, Option<Error>)> {
            Ok((HealResultItem::default(), None))
        }

        async fn heal_bucket(&self, _bucket: &str, _opts: &HealOpts) -> Result<HealResultItem> {
            Ok(HealResultItem::default())
        }

        async fn heal_format(&self, _dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
            Ok((HealResultItem::default(), None))
        }

        async fn list_objects_for_heal(&self, _bucket: &str, _prefix: &str) -> Result<Vec<String>> {
            Ok(Vec::new())
        }

        async fn list_objects_for_heal_page(
            &self,
            _bucket: &str,
            _prefix: &str,
            _continuation_token: Option<&str>,
        ) -> Result<(Vec<String>, Option<String>, bool)> {
            Ok((Vec::new(), None, false))
        }

        async fn get_disk_for_resume(&self, _set_disk_id: &str) -> Result<DiskStore> {
            Err(Error::other("not implemented in tests"))
        }
    }

    fn bucket_request(bucket: &str, priority: HealPriority, source: HealRequestSource) -> HealRequest {
        let mut request = HealRequest::new(
            HealType::Bucket {
                bucket: bucket.to_string(),
            },
            HealOptions::default(),
            priority,
        );
        request.source = source;
        request
    }

    #[test]
    fn test_priority_queue_ordering() {
        let mut queue = PriorityHealQueue::new();

        // Add requests with different priorities
        let low_req = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket1".to_string(),
            },
            HealOptions::default(),
            HealPriority::Low,
        );

        let normal_req = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket2".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        let high_req = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket3".to_string(),
            },
            HealOptions::default(),
            HealPriority::High,
        );

        let urgent_req = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket4".to_string(),
            },
            HealOptions::default(),
            HealPriority::Urgent,
        );

        // Add in random order: low, high, normal, urgent
        assert_eq!(queue.push(low_req), QueuePushOutcome::Accepted);
        assert_eq!(queue.push(high_req), QueuePushOutcome::Accepted);
        assert_eq!(queue.push(normal_req), QueuePushOutcome::Accepted);
        assert_eq!(queue.push(urgent_req), QueuePushOutcome::Accepted);

        assert_eq!(queue.len(), 4);

        // Should pop in priority order: urgent, high, normal, low
        let popped1 = queue.pop().unwrap();
        assert_eq!(popped1.priority, HealPriority::Urgent);

        let popped2 = queue.pop().unwrap();
        assert_eq!(popped2.priority, HealPriority::High);

        let popped3 = queue.pop().unwrap();
        assert_eq!(popped3.priority, HealPriority::Normal);

        let popped4 = queue.pop().unwrap();
        assert_eq!(popped4.priority, HealPriority::Low);

        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn test_priority_queue_fifo_same_priority() {
        let mut queue = PriorityHealQueue::new();

        // Add multiple requests with same priority
        let req1 = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket1".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        let req2 = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket2".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        let req3 = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket3".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        let id1 = req1.id.clone();
        let id2 = req2.id.clone();
        let id3 = req3.id.clone();

        assert_eq!(queue.push(req1), QueuePushOutcome::Accepted);
        assert_eq!(queue.push(req2), QueuePushOutcome::Accepted);
        assert_eq!(queue.push(req3), QueuePushOutcome::Accepted);

        // Should maintain FIFO order for same priority
        let popped1 = queue.pop().unwrap();
        assert_eq!(popped1.id, id1);

        let popped2 = queue.pop().unwrap();
        assert_eq!(popped2.id, id2);

        let popped3 = queue.pop().unwrap();
        assert_eq!(popped3.id, id3);
    }

    #[test]
    fn test_priority_queue_deduplication() {
        let mut queue = PriorityHealQueue::new();

        let req1 = HealRequest::new(
            HealType::Object {
                bucket: "bucket1".to_string(),
                object: "object1".to_string(),
                version_id: None,
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        let req2 = HealRequest::new(
            HealType::Object {
                bucket: "bucket1".to_string(),
                object: "object1".to_string(),
                version_id: None,
            },
            HealOptions::default(),
            HealPriority::High,
        );

        // First request should be added
        assert_eq!(queue.push(req1), QueuePushOutcome::Accepted);
        assert_eq!(queue.len(), 1);

        // Second request with same object should be rejected (duplicate)
        assert_eq!(queue.push(req2), QueuePushOutcome::Merged);
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn test_priority_queue_contains_erasure_set() {
        let mut queue = PriorityHealQueue::new();

        let req = HealRequest::new(
            HealType::ErasureSet {
                buckets: vec!["bucket1".to_string()],
                set_disk_id: "pool_0_set_1".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        assert_eq!(queue.push(req), QueuePushOutcome::Accepted);
        assert!(queue.contains_erasure_set("pool_0_set_1"));
        assert!(!queue.contains_erasure_set("pool_0_set_2"));
    }

    #[test]
    fn test_priority_queue_dedup_key_generation() {
        // Test different heal types generate different keys
        let obj_req = HealRequest::new(
            HealType::Object {
                bucket: "bucket1".to_string(),
                object: "object1".to_string(),
                version_id: None,
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        let bucket_req = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket1".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        let erasure_req = HealRequest::new(
            HealType::ErasureSet {
                buckets: vec!["bucket1".to_string()],
                set_disk_id: "pool_0_set_1".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        let obj_key = PriorityHealQueue::make_dedup_key(&obj_req);
        let bucket_key = PriorityHealQueue::make_dedup_key(&bucket_req);
        let erasure_key = PriorityHealQueue::make_dedup_key(&erasure_req);

        // All keys should be different
        assert_ne!(obj_key, bucket_key);
        assert_ne!(obj_key, erasure_key);
        assert_ne!(bucket_key, erasure_key);

        assert!(obj_key.starts_with("object:"));
        assert!(bucket_key.starts_with("bucket:"));
        assert!(erasure_key.starts_with("erasure_set:"));
    }

    #[test]
    fn test_priority_queue_mixed_priorities_and_types() {
        let mut queue = PriorityHealQueue::new();

        // Add various requests
        let requests = vec![
            (
                HealType::Object {
                    bucket: "b1".to_string(),
                    object: "o1".to_string(),
                    version_id: None,
                },
                HealPriority::Low,
            ),
            (
                HealType::Bucket {
                    bucket: "b2".to_string(),
                },
                HealPriority::Urgent,
            ),
            (
                HealType::ErasureSet {
                    buckets: vec!["b3".to_string()],
                    set_disk_id: "pool_0_set_1".to_string(),
                },
                HealPriority::Normal,
            ),
            (
                HealType::Object {
                    bucket: "b4".to_string(),
                    object: "o4".to_string(),
                    version_id: None,
                },
                HealPriority::High,
            ),
        ];

        for (heal_type, priority) in requests {
            let req = HealRequest::new(heal_type, HealOptions::default(), priority);
            let outcome = queue.push(req);
            assert_eq!(outcome, QueuePushOutcome::Accepted);
        }

        assert_eq!(queue.len(), 4);

        // Check they come out in priority order
        let priorities: Vec<HealPriority> = (0..4).filter_map(|_| queue.pop().map(|r| r.priority)).collect();

        assert_eq!(
            priorities,
            vec![
                HealPriority::Urgent,
                HealPriority::High,
                HealPriority::Normal,
                HealPriority::Low,
            ]
        );
    }

    #[test]
    fn test_priority_queue_stats() {
        let mut queue = PriorityHealQueue::new();

        // Add requests with different priorities
        for _ in 0..3 {
            assert_eq!(
                queue.push(HealRequest::new(
                    HealType::Bucket {
                        bucket: format!("bucket-low-{}", queue.len()),
                    },
                    HealOptions::default(),
                    HealPriority::Low,
                )),
                QueuePushOutcome::Accepted
            );
        }

        for _ in 0..2 {
            assert_eq!(
                queue.push(HealRequest::new(
                    HealType::Bucket {
                        bucket: format!("bucket-normal-{}", queue.len()),
                    },
                    HealOptions::default(),
                    HealPriority::Normal,
                )),
                QueuePushOutcome::Accepted
            );
        }

        assert_eq!(
            queue.push(HealRequest::new(
                HealType::Bucket {
                    bucket: "bucket-high".to_string(),
                },
                HealOptions::default(),
                HealPriority::High,
            )),
            QueuePushOutcome::Accepted
        );

        let stats = queue.get_priority_stats();

        assert_eq!(*stats.get(&HealPriority::Low).unwrap_or(&0), 3);
        assert_eq!(*stats.get(&HealPriority::Normal).unwrap_or(&0), 2);
        assert_eq!(*stats.get(&HealPriority::High).unwrap_or(&0), 1);
        assert_eq!(*stats.get(&HealPriority::Urgent).unwrap_or(&0), 0);
    }

    #[test]
    fn test_priority_queue_is_empty() {
        let mut queue = PriorityHealQueue::new();

        assert!(queue.is_empty());

        assert_eq!(
            queue.push(HealRequest::new(
                HealType::Bucket {
                    bucket: "test".to_string(),
                },
                HealOptions::default(),
                HealPriority::Normal,
            )),
            QueuePushOutcome::Accepted
        );

        assert!(!queue.is_empty());

        queue.pop();

        assert!(queue.is_empty());
    }

    #[test]
    fn test_priority_queue_pop_runnable_skips_blocked_erasure_set() {
        let mut queue = PriorityHealQueue::new();

        let blocked = HealRequest::new(
            HealType::ErasureSet {
                buckets: vec!["bucket-a".to_string()],
                set_disk_id: "pool_0_set_1".to_string(),
            },
            HealOptions::default(),
            HealPriority::Urgent,
        );
        let runnable = HealRequest::new(
            HealType::ErasureSet {
                buckets: vec!["bucket-b".to_string()],
                set_disk_id: "pool_0_set_2".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        assert_eq!(queue.push(blocked), QueuePushOutcome::Accepted);
        assert_eq!(queue.push(runnable.clone()), QueuePushOutcome::Accepted);

        let mut running = HashMap::new();
        running.insert("pool_0_set_1".to_string(), 1);

        let popped = queue
            .pop_runnable(|request| can_schedule_request(request, &running, 1))
            .expect("should find runnable request");

        assert!(matches!(
            popped.heal_type,
            HealType::ErasureSet { ref set_disk_id, .. } if set_disk_id == "pool_0_set_2"
        ));
    }

    #[test]
    fn test_can_schedule_request_respects_per_set_limit() {
        let request = HealRequest::new(
            HealType::ErasureSet {
                buckets: vec!["bucket".to_string()],
                set_disk_id: "pool_0_set_1".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        let mut running = HashMap::new();
        running.insert("pool_0_set_1".to_string(), 1);

        assert!(!can_schedule_request(&request, &running, 1));
        assert!(can_schedule_request(&request, &running, 2));
    }

    #[tokio::test]
    async fn test_submit_heal_request_returns_merged_for_duplicate() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);

        let request = HealRequest::new(
            HealType::Object {
                bucket: "bucket".to_string(),
                object: "object".to_string(),
                version_id: None,
            },
            HealOptions::default(),
            HealPriority::Low,
        );

        assert_eq!(
            manager
                .submit_heal_request(request.clone())
                .await
                .expect("first request should be accepted"),
            HealAdmissionResult::Accepted
        );
        assert_eq!(
            manager
                .submit_heal_request(request)
                .await
                .expect("duplicate request should produce admission result"),
            HealAdmissionResult::Merged
        );
    }

    #[tokio::test]
    async fn test_submit_heal_request_returns_merged_for_active_duplicate() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage.clone(), None);
        let active_request = HealRequest::object("bucket".to_string(), "object".to_string(), None);
        let active_task = Arc::new(HealTask::from_request(active_request, storage));
        manager.active_heals.lock().await.insert(active_task.id.clone(), active_task);

        let duplicate_request = HealRequest::object("bucket".to_string(), "object".to_string(), None);

        assert_eq!(
            manager
                .submit_heal_request(duplicate_request)
                .await
                .expect("active duplicate should produce admission result"),
            HealAdmissionResult::Merged
        );
        assert_eq!(manager.get_queue_length().await, 0);
    }

    #[tokio::test]
    async fn test_active_duplicate_token_can_query_and_cancel_original_task() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage.clone(), None);
        let active_request = HealRequest::object("bucket".to_string(), "object".to_string(), None);
        let active_task = Arc::new(HealTask::from_request(active_request, storage));
        let active_task_id = active_task.id.clone();
        manager.active_heals.lock().await.insert(active_task_id.clone(), active_task);

        let duplicate_request = HealRequest::object("bucket".to_string(), "object".to_string(), None);
        let duplicate_task_id = duplicate_request.id.clone();

        assert_eq!(
            manager
                .submit_heal_request(duplicate_request)
                .await
                .expect("active duplicate should produce admission result"),
            HealAdmissionResult::Merged
        );
        assert_eq!(
            manager
                .get_task_status_for_path("bucket/object", &duplicate_task_id)
                .await
                .expect("duplicate token should query merged active task"),
            HealTaskStatus::Pending
        );

        manager
            .cancel_task(&duplicate_task_id)
            .await
            .expect("duplicate token should cancel merged active task");

        assert!(manager.active_heals.lock().await.get(&active_task_id).is_none());
        assert!(matches!(manager.get_task_status(&active_task_id).await, Err(Error::TaskNotFound { .. })));
    }

    #[tokio::test]
    async fn test_queued_duplicate_token_can_query_and_cancel_original_request() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);
        let original_request = HealRequest::object("bucket".to_string(), "object".to_string(), None);
        let original_task_id = original_request.id.clone();
        let duplicate_request = HealRequest::object("bucket".to_string(), "object".to_string(), None);
        let duplicate_task_id = duplicate_request.id.clone();

        assert_eq!(
            manager
                .submit_heal_request(original_request)
                .await
                .expect("original request should be accepted"),
            HealAdmissionResult::Accepted
        );
        assert_eq!(
            manager
                .submit_heal_request(duplicate_request)
                .await
                .expect("queued duplicate should produce admission result"),
            HealAdmissionResult::Merged
        );
        assert_eq!(
            manager
                .get_task_status_for_path("bucket/object", &duplicate_task_id)
                .await
                .expect("duplicate token should query merged queued task"),
            HealTaskStatus::Pending
        );

        manager
            .cancel_task(&duplicate_task_id)
            .await
            .expect("duplicate token should cancel merged queued request");

        assert!(matches!(
            manager.get_task_status(&original_task_id).await,
            Err(Error::TaskNotFound { .. })
        ));
    }

    #[test]
    fn test_retry_request_for_recoverable_lock_timeout() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let task = HealTask::from_request(HealRequest::object("bucket".to_string(), "object".to_string(), None), storage);
        let result = Err(Error::TaskExecutionFailed {
            message: "Failed to heal object bucket/object: Lock acquisition timeout".to_string(),
        });

        let (retry_request, retry_delay, retry_error) =
            retry_request_for_result(&task, &result).expect("lock timeout should be retryable");

        assert_eq!(retry_request.id, task.id);
        assert_eq!(retry_request.retry_attempts, 1);
        assert_eq!(retry_request.priority, task.priority);
        assert!(retry_delay > Duration::ZERO);
        assert!(retry_error.contains("Lock acquisition timeout"));
    }

    #[test]
    fn test_retry_request_for_typed_read_quorum_error() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let task = HealTask::from_request(HealRequest::object("bucket".to_string(), "object".to_string(), None), storage);
        let result = Err(Error::Storage(EcstoreError::InsufficientReadQuorum(
            "bucket".to_string(),
            "object".to_string(),
        )));

        let (retry_request, retry_delay, retry_error) =
            retry_request_for_result(&task, &result).expect("typed read quorum should be retryable");

        assert_eq!(retry_request.id, task.id);
        assert_eq!(retry_request.retry_attempts, 1);
        assert!(retry_delay > Duration::ZERO);
        assert!(retry_error.contains("Storage resources are insufficient"));
    }

    #[test]
    fn test_retry_request_for_typed_not_found_error_is_not_retryable() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let task = HealTask::from_request(HealRequest::object("bucket".to_string(), "object".to_string(), None), storage);
        let result = Err(Error::Storage(EcstoreError::ObjectNotFound("bucket".to_string(), "object".to_string())));

        assert!(retry_request_for_result(&task, &result).is_none());
    }

    #[test]
    fn test_retry_request_for_recoverable_error_stops_at_limit() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let mut request = HealRequest::object("bucket".to_string(), "object".to_string(), None);
        request.retry_attempts = MAX_RECOVERABLE_HEAL_RETRIES;
        let task = HealTask::from_request(request, storage);
        let result = Err(Error::TaskExecutionFailed {
            message: "Remote lock RPC timed out".to_string(),
        });

        assert!(retry_request_for_result(&task, &result).is_none());
    }

    #[test]
    fn test_heal_type_matches_path_normalizes_prefix_trailing_slash() {
        let heal_type = HealType::Prefix {
            bucket: "bucket".to_string(),
            prefix: "logs/".to_string(),
        };

        assert!(heal_type_matches_path(&heal_type, "bucket"));
        assert!(heal_type_matches_path(&heal_type, "bucket/logs"));
        assert!(heal_type_matches_path(&heal_type, "bucket/logs/"));
    }

    #[test]
    fn test_heal_type_matches_path_normalizes_object_trailing_slash() {
        let heal_type = HealType::Object {
            bucket: "bucket".to_string(),
            object: "object/".to_string(),
            version_id: None,
        };

        assert!(heal_type_matches_path(&heal_type, "bucket/object"));
        assert!(heal_type_matches_path(&heal_type, "bucket/object/"));
    }

    async fn insert_retrying_request(manager: &HealManager, request: HealRequest) -> CancellationToken {
        let task_id = request.id.clone();
        let cancel_token = CancellationToken::new();
        manager.retrying_heals.lock().await.insert(
            task_id.clone(),
            RetryingHeal {
                request: request.clone(),
                error: "Lock acquisition timeout".to_string(),
                cancel_token: cancel_token.clone(),
            },
        );
        manager.completed_heals.lock().await.insert(
            task_id,
            CompletedHealStatus {
                heal_type: request.heal_type,
                status: HealTaskStatus::Retrying {
                    error: "Lock acquisition timeout".to_string(),
                    retry_attempt: request.retry_attempts,
                },
                result_items: Vec::new(),
                completed_at: SystemTime::now(),
            },
        );
        cancel_token
    }

    #[tokio::test]
    async fn test_cancel_task_cancels_retrying_backoff() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);
        let mut request = HealRequest::bucket("bucket".to_string());
        request.retry_attempts = 1;
        let task_id = request.id.clone();
        let cancel_token = insert_retrying_request(&manager, request).await;

        assert!(matches!(
            manager
                .get_task_status(&task_id)
                .await
                .expect("retrying task should be queryable"),
            HealTaskStatus::Retrying { .. }
        ));

        manager
            .cancel_task(&task_id)
            .await
            .expect("retrying task should be cancellable by token");

        assert!(cancel_token.is_cancelled());
        assert!(manager.retrying_heals.lock().await.get(&task_id).is_none());
        assert!(matches!(manager.get_task_status(&task_id).await, Err(Error::TaskNotFound { .. })));
    }

    #[tokio::test]
    async fn test_cancel_tasks_for_path_cancels_retrying_backoff() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);
        let mut request = HealRequest::bucket("bucket".to_string());
        request.retry_attempts = 1;
        let task_id = request.id.clone();
        let cancel_token = insert_retrying_request(&manager, request).await;

        assert_eq!(
            manager
                .cancel_tasks_for_path("bucket")
                .await
                .expect("retrying task should be cancellable by path"),
            1
        );

        assert!(cancel_token.is_cancelled());
        assert!(manager.retrying_heals.lock().await.get(&task_id).is_none());
        assert!(matches!(manager.get_task_status(&task_id).await, Err(Error::TaskNotFound { .. })));
    }

    #[tokio::test]
    async fn test_cancel_tasks_for_empty_path_cancels_queued_cluster_only() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);

        let cluster_request = HealRequest::new(HealType::Cluster, HealOptions::default(), HealPriority::High);
        let cluster_request_id = cluster_request.id.clone();
        let bucket_request = HealRequest::bucket("bucket".to_string());
        let bucket_request_id = bucket_request.id.clone();

        manager
            .submit_heal_request(cluster_request)
            .await
            .expect("cluster request should be accepted");
        manager
            .submit_heal_request(bucket_request)
            .await
            .expect("bucket request should be accepted");

        assert_eq!(
            manager
                .cancel_tasks_for_path("")
                .await
                .expect("root path should cancel queued cluster task"),
            1
        );
        assert!(matches!(
            manager.get_task_status(&cluster_request_id).await,
            Err(Error::TaskNotFound { .. })
        ));
        assert_eq!(
            manager
                .get_task_status(&bucket_request_id)
                .await
                .expect("bucket request should not match root path"),
            HealTaskStatus::Pending
        );
    }

    #[tokio::test]
    async fn test_cancel_tasks_for_empty_path_cancels_active_cluster_only() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage.clone(), None);

        let cluster_request = HealRequest::new(HealType::Cluster, HealOptions::default(), HealPriority::High);
        let cluster_request_id = cluster_request.id.clone();
        let bucket_request = HealRequest::bucket("bucket".to_string());
        let bucket_request_id = bucket_request.id.clone();

        manager.active_heals.lock().await.insert(
            cluster_request_id.clone(),
            Arc::new(HealTask::from_request(cluster_request, storage.clone())),
        );
        manager
            .active_heals
            .lock()
            .await
            .insert(bucket_request_id.clone(), Arc::new(HealTask::from_request(bucket_request, storage)));

        assert_eq!(
            manager
                .cancel_tasks_for_path("")
                .await
                .expect("root path should cancel active cluster task"),
            1
        );
        assert!(manager.active_heals.lock().await.get(&cluster_request_id).is_none());
        assert!(manager.active_heals.lock().await.get(&bucket_request_id).is_some());
    }

    #[tokio::test]
    async fn test_cancel_tasks_for_empty_path_cancels_retrying_cluster_only() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);

        let mut cluster_request = HealRequest::new(HealType::Cluster, HealOptions::default(), HealPriority::High);
        cluster_request.retry_attempts = 1;
        let cluster_request_id = cluster_request.id.clone();
        let cluster_cancel_token = insert_retrying_request(&manager, cluster_request).await;

        let mut bucket_request = HealRequest::bucket("bucket".to_string());
        bucket_request.retry_attempts = 1;
        let bucket_request_id = bucket_request.id.clone();
        let bucket_cancel_token = insert_retrying_request(&manager, bucket_request).await;

        assert_eq!(
            manager
                .cancel_tasks_for_path("")
                .await
                .expect("root path should cancel retrying cluster task"),
            1
        );
        assert!(cluster_cancel_token.is_cancelled());
        assert!(!bucket_cancel_token.is_cancelled());
        assert!(manager.retrying_heals.lock().await.get(&cluster_request_id).is_none());
        assert!(manager.retrying_heals.lock().await.get(&bucket_request_id).is_some());
    }

    #[tokio::test]
    async fn test_retrying_duplicate_token_can_query_and_cancel_original_retry() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);
        let mut original_request = HealRequest::bucket("bucket".to_string());
        original_request.retry_attempts = 1;
        let original_task_id = original_request.id.clone();
        let cancel_token = insert_retrying_request(&manager, original_request).await;

        let duplicate_request = HealRequest::bucket("bucket".to_string());
        let duplicate_task_id = duplicate_request.id.clone();

        assert_eq!(
            manager
                .submit_heal_request(duplicate_request)
                .await
                .expect("retrying duplicate should produce admission result"),
            HealAdmissionResult::Merged
        );
        assert!(matches!(
            manager
                .get_task_status_for_path("bucket", &duplicate_task_id)
                .await
                .expect("duplicate token should query merged retrying task"),
            HealTaskStatus::Retrying { .. }
        ));

        manager
            .cancel_task(&duplicate_task_id)
            .await
            .expect("duplicate token should cancel merged retrying task");

        assert!(cancel_token.is_cancelled());
        assert!(manager.retrying_heals.lock().await.get(&original_task_id).is_none());
    }

    #[tokio::test]
    async fn test_get_task_status_reports_pending_for_queued_request() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);

        let request = HealRequest::bucket("bucket".to_string());
        let request_id = request.id.clone();

        assert_eq!(
            manager
                .submit_heal_request(request)
                .await
                .expect("request should be accepted"),
            HealAdmissionResult::Accepted
        );
        assert_eq!(
            manager
                .get_task_status(&request_id)
                .await
                .expect("queued request should have status"),
            HealTaskStatus::Pending
        );
    }

    #[tokio::test]
    async fn test_operations_snapshot_counts_queue_by_source_and_priority() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);

        let mut scanner_request = HealRequest::new(
            HealType::Object {
                bucket: "bucket-a".to_string(),
                object: "object-a".to_string(),
                version_id: None,
            },
            HealOptions::default(),
            HealPriority::Low,
        );
        scanner_request.source = HealRequestSource::Scanner;

        let mut admin_request = HealRequest::bucket("bucket-b".to_string());
        admin_request.priority = HealPriority::High;
        admin_request.source = HealRequestSource::Admin;

        let mut auto_request = HealRequest::new(
            HealType::ErasureSet {
                buckets: vec!["bucket-c".to_string()],
                set_disk_id: "0-0".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );
        auto_request.source = HealRequestSource::AutoHeal;

        manager
            .submit_heal_request(scanner_request)
            .await
            .expect("scanner request should be accepted");
        manager
            .submit_heal_request(admin_request)
            .await
            .expect("admin request should be accepted");
        manager
            .submit_heal_request(auto_request)
            .await
            .expect("auto request should be accepted");

        let snapshot = manager.operations_snapshot().await;

        assert_eq!(snapshot.queue_length, 3);
        assert_eq!(snapshot.active_tasks, 0);
        assert_eq!(snapshot.queued_by_priority.low, 1);
        assert_eq!(snapshot.queued_by_priority.normal, 1);
        assert_eq!(snapshot.queued_by_priority.high, 1);
        assert_eq!(snapshot.queued_by_priority.urgent, 0);
        assert_eq!(snapshot.queued_by_source.scanner, 1);
        assert_eq!(snapshot.queued_by_source.admin, 1);
        assert_eq!(snapshot.queued_by_source.auto_heal, 1);
        assert_eq!(snapshot.queued_by_source.internal, 0);
    }

    #[tokio::test]
    async fn test_operations_snapshot_counts_active_by_source_and_priority() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);

        let mut request = HealRequest::bucket("bucket-a".to_string());
        request.priority = HealPriority::High;
        request.source = HealRequestSource::Admin;
        let task = Arc::new(HealTask::from_request(request, manager.storage.clone()));
        let task_id = task.id.clone();

        manager.active_heals.lock().await.insert(task_id, task);

        let snapshot = manager.operations_snapshot().await;

        assert_eq!(snapshot.queue_length, 0);
        assert_eq!(snapshot.active_tasks, 1);
        assert_eq!(snapshot.active_by_priority.high, 1);
        assert_eq!(snapshot.active_by_source.admin, 1);
        assert_eq!(snapshot.active_by_source.scanner, 0);
    }

    #[tokio::test]
    async fn test_active_progress_snapshot_sums_active_task_progress() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);

        let first = Arc::new(HealTask::from_request(
            HealRequest::bucket("bucket-a".to_string()),
            manager.storage.clone(),
        ));
        {
            let mut progress = first.progress.write().await;
            progress.update_progress(7, 3, 1, 4096);
        }

        let second = Arc::new(HealTask::from_request(
            HealRequest::bucket("bucket-b".to_string()),
            manager.storage.clone(),
        ));
        {
            let mut progress = second.progress.write().await;
            progress.update_progress(11, 5, 2, 2048);
        }

        manager.active_heals.lock().await.insert(first.id.clone(), first);
        manager.active_heals.lock().await.insert(second.id.clone(), second);

        let progress = manager
            .active_progress_snapshot()
            .await
            .expect("active progress should exist");

        assert_eq!(progress.objects_scanned, 18);
        assert_eq!(progress.objects_healed, 8);
        assert_eq!(progress.objects_failed, 3);
        assert_eq!(progress.bytes_processed, 6144);
    }

    #[tokio::test]
    async fn test_get_task_status_for_path_rejects_wrong_token_when_path_is_active() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);

        manager
            .submit_heal_request(HealRequest::bucket("bucket".to_string()))
            .await
            .expect("request should be accepted");

        assert!(matches!(
            manager.get_task_status_for_path("bucket", "wrong-token").await,
            Err(Error::InvalidClientToken)
        ));
    }

    #[tokio::test]
    async fn test_get_task_status_for_path_rejects_token_from_other_active_path() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);

        let bucket_request = HealRequest::bucket("bucket".to_string());
        let other_request = HealRequest::bucket("other".to_string());
        let other_request_id = other_request.id.clone();

        manager
            .submit_heal_request(bucket_request)
            .await
            .expect("bucket request should be accepted");
        manager
            .submit_heal_request(other_request)
            .await
            .expect("other request should be accepted");

        assert!(matches!(
            manager.get_task_status_for_path("bucket", &other_request_id).await,
            Err(Error::InvalidClientToken)
        ));
    }

    #[tokio::test]
    async fn test_get_task_status_for_path_does_not_accept_token_from_inactive_path() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);

        let request = HealRequest::bucket("bucket".to_string());
        let request_id = request.id.clone();

        manager
            .submit_heal_request(request)
            .await
            .expect("request should be accepted");

        assert!(matches!(
            manager.get_task_status_for_path("other", &request_id).await,
            Err(Error::TaskNotFound { .. })
        ));
    }

    #[tokio::test]
    async fn test_get_task_status_for_path_returns_not_found_when_path_is_inactive() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);

        assert!(matches!(
            manager.get_task_status_for_path("bucket", "old-token").await,
            Err(Error::TaskNotFound { .. })
        ));
    }

    #[tokio::test]
    async fn test_get_task_status_for_empty_path_does_not_match_unrelated_tasks() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);

        let request = HealRequest::bucket("bucket".to_string());
        let request_id = request.id.clone();

        manager
            .submit_heal_request(request)
            .await
            .expect("request should be accepted");

        assert!(matches!(
            manager.get_task_status_for_path("", &request_id).await,
            Err(Error::TaskNotFound { .. })
        ));
        assert!(matches!(
            manager.get_task_status_for_path("", "wrong-token").await,
            Err(Error::TaskNotFound { .. })
        ));
    }

    #[tokio::test]
    async fn test_get_task_report_queries_queued_task_by_token_without_path() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);

        let request = HealRequest::new(
            HealType::ErasureSet {
                buckets: vec![],
                set_disk_id: "pool_0_set_1".to_string(),
            },
            HealOptions::default(),
            HealPriority::High,
        );
        let request_id = request.id.clone();

        manager
            .submit_heal_request(request)
            .await
            .expect("request should be accepted");

        let report = manager
            .get_task_report(&request_id)
            .await
            .expect("queued task should be queryable by token");

        assert_eq!(report.status, HealTaskStatus::Pending);
        assert!(report.result_items.is_empty());
    }

    #[tokio::test]
    async fn test_get_task_status_reads_recent_completed_status() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);

        manager.completed_heals.lock().await.insert(
            "completed-token".to_string(),
            CompletedHealStatus {
                heal_type: HealType::Bucket {
                    bucket: "bucket".to_string(),
                },
                status: HealTaskStatus::Completed,
                result_items: Vec::new(),
                completed_at: SystemTime::now(),
            },
        );

        assert_eq!(
            manager
                .get_task_status_for_path("bucket", "completed-token")
                .await
                .expect("recent completed task should be queryable"),
            HealTaskStatus::Completed
        );
    }

    #[tokio::test]
    async fn test_get_task_report_for_path_reads_completed_items() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);

        manager.completed_heals.lock().await.insert(
            "completed-token".to_string(),
            CompletedHealStatus {
                heal_type: HealType::Object {
                    bucket: "bucket".to_string(),
                    object: "object".to_string(),
                    version_id: None,
                },
                status: HealTaskStatus::Completed,
                result_items: vec![HealResultItem {
                    bucket: "bucket".to_string(),
                    object: "object".to_string(),
                    object_size: 1024,
                    ..Default::default()
                }],
                completed_at: SystemTime::now(),
            },
        );

        let report = manager
            .get_task_report_for_path("bucket/object", "completed-token")
            .await
            .expect("recent completed task report should be queryable");

        assert_eq!(report.status, HealTaskStatus::Completed);
        assert_eq!(report.result_items.len(), 1);
        assert_eq!(report.result_items[0].object_size, 1024);
    }

    #[tokio::test]
    async fn test_get_task_report_for_empty_path_does_not_match_unrelated_tasks() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);

        manager
            .submit_heal_request(HealRequest::bucket("bucket".to_string()))
            .await
            .expect("request should be accepted");

        assert!(matches!(
            manager.get_task_report_for_path("", "wrong-token").await,
            Err(Error::TaskNotFound { .. })
        ));
    }

    #[tokio::test]
    async fn test_cancel_task_removes_queued_request() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);

        let request = HealRequest::bucket("bucket".to_string());
        let request_id = request.id.clone();

        manager
            .submit_heal_request(request)
            .await
            .expect("request should be accepted");
        manager
            .cancel_task(&request_id)
            .await
            .expect("queued request should be cancelled");

        assert!(matches!(manager.get_task_status(&request_id).await, Err(Error::TaskNotFound { .. })));
    }

    #[tokio::test]
    async fn test_cancel_tasks_for_path_removes_matching_queued_requests() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);

        let bucket_request = HealRequest::bucket("bucket".to_string());
        let bucket_request_id = bucket_request.id.clone();
        let other_request = HealRequest::bucket("other".to_string());
        let other_request_id = other_request.id.clone();

        manager
            .submit_heal_request(bucket_request)
            .await
            .expect("bucket request should be accepted");
        manager
            .submit_heal_request(other_request)
            .await
            .expect("other request should be accepted");

        assert_eq!(
            manager
                .cancel_tasks_for_path("bucket")
                .await
                .expect("matching request should be cancelled"),
            1
        );
        assert!(matches!(
            manager.get_task_status(&bucket_request_id).await,
            Err(Error::TaskNotFound { .. })
        ));
        assert_eq!(
            manager
                .get_task_status(&other_request_id)
                .await
                .expect("unmatched request should remain queued"),
            HealTaskStatus::Pending
        );
    }

    #[tokio::test]
    async fn test_submit_heal_request_returns_merged_before_full_for_duplicate() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(
            storage,
            Some(HealConfig {
                queue_size: 1,
                ..HealConfig::default()
            }),
        );

        let request = HealRequest::new(
            HealType::Object {
                bucket: "bucket".to_string(),
                object: "object".to_string(),
                version_id: None,
            },
            HealOptions::default(),
            HealPriority::Low,
        );

        assert_eq!(
            manager
                .submit_heal_request(request.clone())
                .await
                .expect("first request should be accepted"),
            HealAdmissionResult::Accepted
        );
        assert_eq!(
            manager
                .submit_heal_request(request)
                .await
                .expect("duplicate request should merge even when queue is full"),
            HealAdmissionResult::Merged
        );
    }

    #[tokio::test]
    async fn test_submit_heal_request_returns_dropped_for_low_priority_when_full() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(
            storage,
            Some(HealConfig {
                queue_size: 1,
                low_priority_drop_when_full: true,
                ..HealConfig::default()
            }),
        );

        let accepted = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket-a".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );
        let dropped = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket-b".to_string(),
            },
            HealOptions::default(),
            HealPriority::Low,
        );

        assert_eq!(
            manager
                .submit_heal_request(accepted)
                .await
                .expect("first request should be accepted"),
            HealAdmissionResult::Accepted
        );
        assert_eq!(
            manager
                .submit_heal_request(dropped)
                .await
                .expect("low priority request should be dropped with explicit admission result"),
            HealAdmissionResult::Dropped(HealAdmissionDropReason::QueueFull)
        );
    }

    #[tokio::test]
    async fn test_submit_heal_request_returns_full_for_normal_priority_when_full() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(
            storage,
            Some(HealConfig {
                queue_size: 1,
                ..HealConfig::default()
            }),
        );

        let accepted = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket-a".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );
        let full = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket-b".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        assert_eq!(
            manager
                .submit_heal_request(accepted)
                .await
                .expect("first request should be accepted"),
            HealAdmissionResult::Accepted
        );
        assert_eq!(
            manager
                .submit_heal_request(full)
                .await
                .expect("normal priority request should surface full admission"),
            HealAdmissionResult::Full
        );
    }

    #[tokio::test]
    async fn test_high_priority_request_displaces_lower_priority_when_queue_full() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(
            storage,
            Some(HealConfig {
                queue_size: 1,
                ..HealConfig::default()
            }),
        );

        let low = HealRequest::new(
            HealType::Bucket {
                bucket: "background-bucket".to_string(),
            },
            HealOptions::default(),
            HealPriority::Low,
        );
        let low_id = low.id.clone();
        let high = HealRequest::new(
            HealType::Bucket {
                bucket: "manual-bucket".to_string(),
            },
            HealOptions::default(),
            HealPriority::High,
        );
        let high_id = high.id.clone();

        assert_eq!(
            manager
                .submit_heal_request(low)
                .await
                .expect("low priority request should be accepted first"),
            HealAdmissionResult::Accepted
        );
        assert_eq!(
            manager
                .submit_heal_request(high)
                .await
                .expect("high priority request should be admitted by displacing lower priority work"),
            HealAdmissionResult::Accepted
        );
        assert_eq!(manager.get_queue_length().await, 1);
        assert!(matches!(manager.get_task_status(&low_id).await, Err(Error::TaskNotFound { .. })));
        assert_eq!(
            manager
                .get_task_status(&high_id)
                .await
                .expect("high priority request should remain queued"),
            HealTaskStatus::Pending
        );
    }

    #[tokio::test]
    async fn test_submit_heal_request_drops_read_repair_under_pressure() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(
            storage,
            Some(HealConfig {
                queue_size: 10,
                ..HealConfig::default()
            }),
        );

        for index in 0..8 {
            assert_eq!(
                manager
                    .submit_heal_request(bucket_request(
                        &format!("queued-{index}"),
                        HealPriority::Normal,
                        HealRequestSource::Internal,
                    ))
                    .await
                    .expect("seed request should be accepted"),
                HealAdmissionResult::Accepted
            );
        }

        let admission = manager
            .submit_heal_request(bucket_request("read-repair", HealPriority::Normal, HealRequestSource::ReadRepair))
            .await
            .expect("read repair admission should return a result");

        assert_eq!(admission, HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped));
        assert_eq!(manager.get_queue_length().await, 8);
    }

    #[tokio::test]
    async fn test_submit_heal_request_drops_low_scanner_under_pressure() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(
            storage,
            Some(HealConfig {
                queue_size: 10,
                ..HealConfig::default()
            }),
        );

        for index in 0..8 {
            assert_eq!(
                manager
                    .submit_heal_request(bucket_request(
                        &format!("queued-{index}"),
                        HealPriority::Normal,
                        HealRequestSource::Internal,
                    ))
                    .await
                    .expect("seed request should be accepted"),
                HealAdmissionResult::Accepted
            );
        }

        let admission = manager
            .submit_heal_request(bucket_request("scanner", HealPriority::Low, HealRequestSource::Scanner))
            .await
            .expect("scanner admission should return a result");

        assert_eq!(admission, HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped));
        assert_eq!(manager.get_queue_length().await, 8);
    }

    #[tokio::test]
    async fn test_submit_heal_request_accepts_admin_high_under_pressure() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(
            storage,
            Some(HealConfig {
                queue_size: 10,
                ..HealConfig::default()
            }),
        );

        for index in 0..8 {
            assert_eq!(
                manager
                    .submit_heal_request(bucket_request(
                        &format!("queued-{index}"),
                        HealPriority::Normal,
                        HealRequestSource::Internal,
                    ))
                    .await
                    .expect("seed request should be accepted"),
                HealAdmissionResult::Accepted
            );
        }

        let admission = manager
            .submit_heal_request(bucket_request("admin", HealPriority::High, HealRequestSource::Admin))
            .await
            .expect("admin admission should return a result");

        assert_eq!(admission, HealAdmissionResult::Accepted);
        assert_eq!(manager.get_queue_length().await, 9);
    }

    #[tokio::test]
    async fn test_mainline_throttle_delays_background_heal_start() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let provider: WorkloadSnapshotProviderRef = Arc::new(FixedWorkloadProvider {
            class: WorkloadClass::ForegroundRead,
            active: 8,
            limit: 10,
            state: AdmissionState::Open,
        });
        let manager = HealManager::new_with_workload_provider(
            storage,
            Some(HealConfig {
                max_concurrent_heals: 1,
                mainline_throttle_enable: true,
                mainline_read_utilization_high_percent: 80,
                mainline_write_utilization_high_percent: 80,
                mainline_max_sleep: Duration::from_millis(1),
                ..HealConfig::default()
            }),
            Some(provider),
        );

        manager
            .submit_heal_request(bucket_request("read-repair", HealPriority::Normal, HealRequestSource::ReadRepair))
            .await
            .expect("read repair request should be queued");

        process_manager_queue_once(&manager).await;

        assert_eq!(manager.get_queue_length().await, 1);
        assert_eq!(manager.get_active_task_count().await, 0);
    }

    #[tokio::test]
    async fn test_mainline_throttle_delays_background_heal_start_under_write_pressure() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let provider: WorkloadSnapshotProviderRef = Arc::new(FixedWorkloadProvider {
            class: WorkloadClass::ForegroundWrite,
            active: 9,
            limit: 10,
            state: AdmissionState::Open,
        });
        let manager = HealManager::new_with_workload_provider(
            storage,
            Some(HealConfig {
                max_concurrent_heals: 1,
                mainline_throttle_enable: true,
                mainline_read_utilization_high_percent: 80,
                mainline_write_utilization_high_percent: 80,
                mainline_max_sleep: Duration::from_millis(1),
                ..HealConfig::default()
            }),
            Some(provider),
        );

        manager
            .submit_heal_request(bucket_request("read-repair", HealPriority::Normal, HealRequestSource::ReadRepair))
            .await
            .expect("read repair request should be queued");

        process_manager_queue_once(&manager).await;

        assert_eq!(manager.get_queue_length().await, 1);
        assert_eq!(manager.get_active_task_count().await, 0);
    }

    #[tokio::test]
    async fn test_mainline_throttle_allows_admin_high_start() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let provider: WorkloadSnapshotProviderRef = Arc::new(FixedWorkloadProvider {
            class: WorkloadClass::ForegroundRead,
            active: 10,
            limit: 10,
            state: AdmissionState::Saturated,
        });
        let manager = HealManager::new_with_workload_provider(
            storage,
            Some(HealConfig {
                max_concurrent_heals: 1,
                mainline_throttle_enable: true,
                mainline_read_utilization_high_percent: 80,
                mainline_write_utilization_high_percent: 80,
                mainline_max_sleep: Duration::from_millis(1),
                ..HealConfig::default()
            }),
            Some(provider),
        );

        manager
            .submit_heal_request(bucket_request("admin", HealPriority::High, HealRequestSource::Admin))
            .await
            .expect("admin request should be queued");

        process_manager_queue_once(&manager).await;

        assert_eq!(manager.get_queue_length().await, 0);
    }

    #[tokio::test]
    async fn test_force_start_bypasses_duplicate_and_full_admission() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(
            storage,
            Some(HealConfig {
                queue_size: 1,
                low_priority_drop_when_full: true,
                ..HealConfig::default()
            }),
        );

        let normal = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket".to_string(),
            },
            HealOptions::default(),
            HealPriority::Low,
        );
        let mut forced_duplicate = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket".to_string(),
            },
            HealOptions::default(),
            HealPriority::Low,
        );
        forced_duplicate.force_start = true;

        let subsequent_duplicate = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket".to_string(),
            },
            HealOptions::default(),
            HealPriority::Low,
        );

        assert_eq!(
            manager
                .submit_heal_request(normal)
                .await
                .expect("first request should be accepted"),
            HealAdmissionResult::Accepted
        );
        assert_eq!(
            manager
                .submit_heal_request(forced_duplicate)
                .await
                .expect("force start should bypass duplicate/full policy"),
            HealAdmissionResult::Accepted
        );
        assert_eq!(
            manager
                .submit_heal_request(subsequent_duplicate)
                .await
                .expect("subsequent non-force duplicate should be merged"),
            HealAdmissionResult::Merged
        );
    }

    #[tokio::test]
    async fn test_force_start_marks_dedup_key_for_future_duplicates() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(
            storage,
            Some(HealConfig {
                queue_size: 1,
                ..HealConfig::default()
            }),
        );

        let normal = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket".to_string(),
            },
            HealOptions::default(),
            HealPriority::Low,
        );
        let mut forced = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket".to_string(),
            },
            HealOptions::default(),
            HealPriority::Low,
        );
        forced.force_start = true;
        let duplicate = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket".to_string(),
            },
            HealOptions::default(),
            HealPriority::Low,
        );

        assert_eq!(
            manager
                .submit_heal_request(normal)
                .await
                .expect("first request should be accepted"),
            HealAdmissionResult::Accepted
        );
        assert_eq!(
            manager
                .submit_heal_request(forced)
                .await
                .expect("forced request should bypass duplicate/full admission"),
            HealAdmissionResult::Accepted
        );
        assert_eq!(
            manager
                .submit_heal_request(duplicate)
                .await
                .expect("non-forced duplicate should merge while forced request is queued"),
            HealAdmissionResult::Merged
        );
    }

    #[test]
    fn test_running_erasure_set_counts_groups_only_erasure_tasks() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let erasure_task = Arc::new(HealTask::from_request(
            HealRequest::new(
                HealType::ErasureSet {
                    buckets: vec!["bucket".to_string()],
                    set_disk_id: "pool_0_set_1".to_string(),
                },
                HealOptions::default(),
                HealPriority::Normal,
            ),
            storage.clone(),
        ));
        let object_task = Arc::new(HealTask::from_request(
            HealRequest::new(
                HealType::Object {
                    bucket: "bucket".to_string(),
                    object: "object".to_string(),
                    version_id: None,
                },
                HealOptions::default(),
                HealPriority::Normal,
            ),
            storage,
        ));

        let mut active = HashMap::new();
        active.insert(erasure_task.id.clone(), erasure_task);
        active.insert(object_task.id.clone(), object_task);

        let counts = running_erasure_set_counts(&active);
        assert_eq!(counts.get("pool_0_set_1"), Some(&1));
        assert_eq!(counts.len(), 1);
    }

    #[test]
    fn test_heal_config_respects_feature_flags() {
        temp_env::with_vars(
            [
                (rustfs_config::ENV_HEAL_EVENT_DRIVEN_SCHEDULER_ENABLE, Some("false")),
                (rustfs_config::ENV_HEAL_SET_BULKHEAD_ENABLE, Some("false")),
                (rustfs_config::ENV_HEAL_PAGE_PARALLEL_ENABLE, Some("false")),
            ],
            || {
                let config = HealConfig::default();
                assert!(!config.event_driven_scheduler_enable);
                assert!(!config.set_bulkhead_enable);
                assert!(!config.page_parallel_enable);
            },
        );
    }
}
