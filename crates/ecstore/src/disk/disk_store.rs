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

use crate::disk::{
    CheckPartsResp, DataDirDeleteStatus, DeleteOptions, DiskAPI, DiskError, DiskInfo, DiskInfoOptions, DiskLocation, Endpoint,
    Error, FileInfoVersions, MmapCopyStageMetrics, ReadMultipleReq, ReadMultipleResp, ReadOptions, RenameDataResp, Result,
    SnapshotLeaseToken, UpdateMetadataOpts, VolumeInfo, WalkDirOptions,
    health_state::{
        RuntimeDriveHealthState, classify_drive_recovery, get_drive_returning_probe_interval,
        get_drive_returning_success_threshold, get_drive_suspect_failure_threshold, record_drive_offline_duration,
        record_drive_recovery_class, record_drive_runtime_state, record_drive_state_transition,
    },
    local::{LocalDisk, ScanGuard},
};
use crate::runtime::sources as runtime_sources;
use bytes::Bytes;
use metrics::counter;
use rustfs_filemeta::{FileInfo, ObjectPartInfo, RawFileInfo};
use rustfs_madmin::{info_commands::DiskMetrics, metrics::TimedAction};
#[cfg(not(test))]
use std::sync::OnceLock;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, LazyLock, RwLock as StdRwLock,
        atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::RwLock, time};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

/// Disk health status constants
const DISK_HEALTH_OK: u32 = 0;
const DISK_HEALTH_FAULTY: u32 = 1;
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_DISK: &str = "disk";
const EVENT_DISK_HEALTH_CHECK_FAILED: &str = "disk_health_check_failed";
const EVENT_DISK_RECOVERY_PROBE_STATE: &str = "disk_recovery_probe_state";
const EVENT_DISK_TIMEOUT_POLICY_FALLBACK: &str = "disk_timeout_policy_fallback";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TimeoutHealthAction {
    MarkFailure,
    IgnoreFailure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DiskMetricMutation {
    None,
    Write,
    Delete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TimeoutHealthPolicy {
    MarkFailure,
    IgnoreScanner,
}

impl TimeoutHealthPolicy {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            rustfs_config::DRIVE_TIMEOUT_HEALTH_ACTION_MARK_FAILURE => Some(Self::MarkFailure),
            rustfs_config::DRIVE_TIMEOUT_HEALTH_ACTION_IGNORE_SCANNER => Some(Self::IgnoreScanner),
            _ => None,
        }
    }

    fn scanner_timeout_health_action(self) -> TimeoutHealthAction {
        match self {
            Self::MarkFailure => TimeoutHealthAction::MarkFailure,
            Self::IgnoreScanner => TimeoutHealthAction::IgnoreFailure,
        }
    }
}

pub const ENV_RUSTFS_DRIVE_ACTIVE_MONITORING: &str = "RUSTFS_DRIVE_ACTIVE_MONITORING";
pub const DEFAULT_RUSTFS_DRIVE_ACTIVE_MONITORING: bool = true;
pub const SKIP_IF_SUCCESS_BEFORE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DriveTimeoutProfile {
    Default,
    HighLatency,
}

impl DriveTimeoutProfile {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            rustfs_config::DRIVE_TIMEOUT_PROFILE_DEFAULT => Some(Self::Default),
            rustfs_config::DRIVE_TIMEOUT_PROFILE_HIGH_LATENCY => Some(Self::HighLatency),
            _ => None,
        }
    }
}

#[cfg(not(test))]
static DRIVE_TIMEOUT_PROFILE_CACHE: OnceLock<DriveTimeoutProfile> = OnceLock::new();
#[cfg(not(test))]
static DRIVE_TIMEOUT_HEALTH_POLICY_CACHE: OnceLock<TimeoutHealthPolicy> = OnceLock::new();

const DISK_OPERATION_NAMES: &[&str] = &[
    "read_metadata",
    "disk_info",
    "make_volume",
    "make_volumes",
    "list_volumes",
    "stat_volume",
    "delete_volume",
    "walk_dir",
    "delete_version",
    "delete_versions",
    "delete_paths",
    "acquire_snapshot_lease",
    "release_snapshot_lease",
    "renew_snapshot_lease",
    "delete_data_dir",
    "write_metadata",
    "update_metadata",
    "read_version",
    "read_xl",
    "rename_data",
    "list_dir",
    "read_file",
    "read_file_stream",
    "read_file_mmap_copy",
    "read_file_mmap_copy_with_metrics",
    "append_file",
    "create_file",
    "rename_file",
    "rename_part",
    "prepare_part_transaction",
    "settle_part_transaction",
    "delete",
    "verify_file",
    "check_parts",
    "read_parts",
    "read_multiple",
    "write_all",
    "compare_and_update_file",
    "read_all",
];

static DISK_OPERATION_INDEX: LazyLock<HashMap<&'static str, usize>> = LazyLock::new(|| {
    DISK_OPERATION_NAMES
        .iter()
        .copied()
        .enumerate()
        .map(|(index, name)| (name, index))
        .collect()
});

lazy_static::lazy_static! {
    static ref TEST_DATA: Bytes = Bytes::from(vec![42u8; 2048]);
    static ref TEST_BUCKET: String = ".rustfs.sys/tmp".to_string();
}

pub fn get_max_timeout_duration() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION,
        rustfs_config::DEFAULT_DRIVE_MAX_TIMEOUT_DURATION_SECS,
    ))
}

fn resolve_drive_timeout_profile_from_env() -> DriveTimeoutProfile {
    let raw = rustfs_utils::get_env_str(rustfs_config::ENV_DRIVE_TIMEOUT_PROFILE, rustfs_config::DEFAULT_DRIVE_TIMEOUT_PROFILE);
    if let Some(profile) = DriveTimeoutProfile::parse(&raw) {
        return profile;
    }
    warn!(
        event = EVENT_DISK_TIMEOUT_POLICY_FALLBACK,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_DISK,
        env = rustfs_config::ENV_DRIVE_TIMEOUT_PROFILE,
        value = %raw,
        default = rustfs_config::DEFAULT_DRIVE_TIMEOUT_PROFILE,
        reason = "invalid_timeout_profile",
        "Disk timeout policy fell back to default"
    );
    DriveTimeoutProfile::parse(rustfs_config::DEFAULT_DRIVE_TIMEOUT_PROFILE).unwrap_or(DriveTimeoutProfile::Default)
}

fn get_drive_timeout_profile() -> DriveTimeoutProfile {
    #[cfg(test)]
    {
        resolve_drive_timeout_profile_from_env()
    }
    #[cfg(not(test))]
    {
        *DRIVE_TIMEOUT_PROFILE_CACHE.get_or_init(resolve_drive_timeout_profile_from_env)
    }
}

fn get_drive_timeout_duration(env_key: &str, default_secs: u64, high_latency_secs: Option<u64>) -> Duration {
    let fallback_default = match (get_drive_timeout_profile(), high_latency_secs) {
        (DriveTimeoutProfile::HighLatency, Some(secs)) => secs,
        _ => default_secs,
    };
    Duration::from_secs(
        rustfs_utils::get_env_opt_u64_with_aliases(env_key, &[rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION])
            .unwrap_or(fallback_default),
    )
}

pub fn get_drive_metadata_timeout() -> Duration {
    get_drive_timeout_duration(
        rustfs_config::ENV_DRIVE_METADATA_TIMEOUT_SECS,
        rustfs_config::DEFAULT_DRIVE_METADATA_TIMEOUT_SECS,
        Some(rustfs_config::DRIVE_TIMEOUT_PROFILE_HIGH_LATENCY_SECS),
    )
}

pub fn get_drive_disk_info_timeout() -> Duration {
    get_drive_timeout_duration(
        rustfs_config::ENV_DRIVE_DISK_INFO_TIMEOUT_SECS,
        rustfs_config::DEFAULT_DRIVE_DISK_INFO_TIMEOUT_SECS,
        Some(rustfs_config::DRIVE_TIMEOUT_PROFILE_HIGH_LATENCY_SECS),
    )
}

pub fn get_drive_list_dir_timeout() -> Duration {
    get_drive_timeout_duration(
        rustfs_config::ENV_DRIVE_LIST_DIR_TIMEOUT_SECS,
        rustfs_config::DEFAULT_DRIVE_LIST_DIR_TIMEOUT_SECS,
        Some(rustfs_config::DRIVE_TIMEOUT_PROFILE_HIGH_LATENCY_SECS),
    )
}

pub(crate) trait DiskStoreRenameDataExt {
    async fn rename_data_borrowed(
        &self,
        src_volume: &str,
        src_path: &str,
        fi: &FileInfo,
        dst_volume: &str,
        dst_path: &str,
    ) -> Result<RenameDataResp>;
}

impl DiskStoreRenameDataExt for LocalDiskWrapper {
    async fn rename_data_borrowed(
        &self,
        src_volume: &str,
        src_path: &str,
        fi: &FileInfo,
        dst_volume: &str,
        dst_path: &str,
    ) -> Result<RenameDataResp> {
        self.track_disk_health_mutation(
            "rename_data",
            DiskMetricMutation::Write,
            || async {
                self.disk
                    .rename_data_borrowed(src_volume, src_path, fi, dst_volume, dst_path)
                    .await
            },
            get_max_timeout_duration(),
        )
        .await
    }
}

pub fn get_drive_walkdir_timeout() -> Duration {
    get_drive_timeout_duration(
        rustfs_config::ENV_DRIVE_WALKDIR_TIMEOUT_SECS,
        rustfs_config::DEFAULT_DRIVE_WALKDIR_TIMEOUT_SECS,
        Some(rustfs_config::DRIVE_TIMEOUT_PROFILE_HIGH_LATENCY_SECS),
    )
}

/// Per-read stall budget for a directory walk: a walk read is failed only if
/// the drive stops answering for this long, not for a walk simply taking a
/// while (see `with_walk_stall_deadline` in `disk/local.rs`).
///
/// Wide-directory tuning (rustfs/backlog#1216): because a whole-directory
/// enumeration (`list_dir` with `count = -1`) is bounded by this budget as one
/// unit, a very wide flat prefix (millions of immediate children) can make a
/// single `readdir` exceed the default on a healthy disk and fail ListObjects.
/// Deployments with such directories should raise
/// `RUSTFS_DRIVE_WALKDIR_STALL_TIMEOUT_SECS`, or select the high-latency
/// drive-timeout profile (which raises this default automatically), to widen
/// the budget without a code change.
pub fn get_drive_walkdir_stall_timeout() -> Duration {
    get_drive_timeout_duration(
        rustfs_config::ENV_DRIVE_WALKDIR_STALL_TIMEOUT_SECS,
        rustfs_config::DEFAULT_DRIVE_WALKDIR_STALL_TIMEOUT_SECS,
        Some(rustfs_config::DRIVE_TIMEOUT_PROFILE_HIGH_LATENCY_SECS),
    )
}

pub fn get_drive_walkdir_peek_timeout() -> Duration {
    let stall_timeout = get_drive_walkdir_stall_timeout();
    let configured = get_drive_timeout_duration(
        rustfs_config::ENV_DRIVE_WALKDIR_PEEK_TIMEOUT_SECS,
        rustfs_config::DEFAULT_DRIVE_WALKDIR_PEEK_TIMEOUT_SECS,
        Some(rustfs_config::DRIVE_TIMEOUT_PROFILE_HIGH_LATENCY_SECS.saturating_mul(2)),
    );
    configured.max(stall_timeout)
}

pub fn get_object_disk_read_timeout() -> Duration {
    get_drive_timeout_duration(
        rustfs_config::ENV_OBJECT_DISK_READ_TIMEOUT,
        rustfs_config::DEFAULT_OBJECT_DISK_READ_TIMEOUT,
        Some(rustfs_config::DRIVE_TIMEOUT_PROFILE_HIGH_LATENCY_SECS),
    )
}

/// Per-shard erasure write stall budget: a shard write (or shutdown) that makes
/// no forward progress for this long is failed and its disk dropped before
/// commit. Re-armed on every shard write, so it bounds a stall rather than the
/// whole transfer. `0` disables the deadline (wait indefinitely).
pub fn get_object_disk_write_stall_timeout() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_OBJECT_DISK_WRITE_STALL_TIMEOUT,
        rustfs_config::DEFAULT_OBJECT_DISK_WRITE_STALL_TIMEOUT,
    ))
}

/// Optional absolute per-object erasure write cap (administrator slow-drip
/// backstop). `0` (default) disables the cap; the per-shard stall timeout is the
/// primary guarantee.
pub fn get_object_disk_write_absolute_cap() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_OBJECT_DISK_WRITE_ABSOLUTE_CAP,
        rustfs_config::DEFAULT_OBJECT_DISK_WRITE_ABSOLUTE_CAP,
    ))
}

pub fn get_drive_active_check_interval() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_DRIVE_ACTIVE_CHECK_INTERVAL_SECS,
        rustfs_config::DEFAULT_DRIVE_ACTIVE_CHECK_INTERVAL_SECS,
    ))
}

pub fn get_drive_active_check_timeout() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS,
        rustfs_config::DEFAULT_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS,
    ))
}

fn resolve_drive_timeout_health_policy_from_env() -> TimeoutHealthPolicy {
    let raw = rustfs_utils::get_env_str(
        rustfs_config::ENV_DRIVE_TIMEOUT_HEALTH_ACTION,
        rustfs_config::DEFAULT_DRIVE_TIMEOUT_HEALTH_ACTION,
    );
    if let Some(policy) = TimeoutHealthPolicy::parse(&raw) {
        return policy;
    }
    warn!(
        event = EVENT_DISK_TIMEOUT_POLICY_FALLBACK,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_DISK,
        env = rustfs_config::ENV_DRIVE_TIMEOUT_HEALTH_ACTION,
        value = %raw,
        default = rustfs_config::DEFAULT_DRIVE_TIMEOUT_HEALTH_ACTION,
        reason = "invalid_health_action_policy",
        "Disk timeout policy fell back to default"
    );
    TimeoutHealthPolicy::parse(rustfs_config::DEFAULT_DRIVE_TIMEOUT_HEALTH_ACTION).unwrap_or(TimeoutHealthPolicy::MarkFailure)
}

fn get_drive_timeout_health_policy() -> TimeoutHealthPolicy {
    #[cfg(test)]
    {
        resolve_drive_timeout_health_policy_from_env()
    }
    #[cfg(not(test))]
    {
        *DRIVE_TIMEOUT_HEALTH_POLICY_CACHE.get_or_init(resolve_drive_timeout_health_policy_from_env)
    }
}

/// DiskHealthTracker tracks the health status of a disk.
/// Similar to Go's diskHealthTracker.
#[derive(Debug)]
pub struct DiskHealthTracker {
    /// Atomic timestamp of last successful operation
    pub last_success: AtomicI64,
    /// Atomic timestamp of last operation start
    pub last_started: AtomicI64,
    /// Atomic disk status (OK or Faulty)
    pub status: AtomicU32,
    /// Atomic number of waiting operations
    pub waiting: AtomicU32,
    /// Runtime drive health state
    pub runtime_state: AtomicU32,
    /// Consecutive failures while transitioning away from online
    pub consecutive_failures: AtomicU32,
    /// Consecutive successes while returning online
    pub consecutive_successes: AtomicU32,
    /// When the drive first left the online state
    pub offline_since_unix_secs: AtomicI64,
    /// Last runtime state transition timestamp
    pub last_transition_unix_secs: AtomicI64,
    /// Last successfully probed total space in bytes
    pub last_capacity_total: AtomicU64,
    /// Last successfully probed used space in bytes
    pub last_capacity_used: AtomicU64,
    /// Last successfully probed free space in bytes
    pub last_capacity_free: AtomicU64,
    /// Last successful capacity probe timestamp
    pub last_capacity_probe_unix_secs: AtomicI64,
    /// Authoritative atomically published runtime/status pair.
    state_snapshot: AtomicU64,
    transition_lock: std::sync::Mutex<()>,
}

fn pack_health_state(runtime_state: RuntimeDriveHealthState, status: u32) -> u64 {
    (u64::from(runtime_state as u32) << 32) | u64::from(status)
}

fn unpack_health_state(snapshot: u64) -> (RuntimeDriveHealthState, u32) {
    (RuntimeDriveHealthState::from_u32((snapshot >> 32) as u32), snapshot as u32)
}

#[derive(Debug)]
pub(crate) struct DiskHealthMetricEpoch {
    /// Preallocated per-operation metrics for built-in disk operation names.
    operation_metrics: Box<[DiskOperationMetricEntry]>,
    /// Fallback for tests or future extension operations outside DISK_OPERATION_NAMES.
    fallback_operation_metrics: StdRwLock<HashMap<&'static str, Arc<DiskOperationMetrics>>>,
    /// Caller API operations currently executing through the disk health wrapper.
    api_waiting: AtomicU32,
    /// Operations rejected because the disk was unavailable for the caller.
    total_errors_availability: AtomicU64,
    /// Operations that timed out in the disk health wrapper.
    total_errors_timeout: AtomicU64,
    /// Completed disk write mutations.
    total_writes: AtomicU64,
    /// Completed disk delete mutations.
    total_deletes: AtomicU64,
}

impl Default for DiskHealthMetricEpoch {
    fn default() -> Self {
        Self {
            operation_metrics: DISK_OPERATION_NAMES
                .iter()
                .copied()
                .map(|name| DiskOperationMetricEntry {
                    name,
                    metrics: DiskOperationMetrics::default(),
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            fallback_operation_metrics: StdRwLock::new(HashMap::new()),
            api_waiting: AtomicU32::new(0),
            total_errors_availability: AtomicU64::new(0),
            total_errors_timeout: AtomicU64::new(0),
            total_writes: AtomicU64::new(0),
            total_deletes: AtomicU64::new(0),
        }
    }
}

impl DiskHealthMetricEpoch {
    fn fallback_operation_metrics_read(
        &self,
    ) -> std::sync::RwLockReadGuard<'_, HashMap<&'static str, Arc<DiskOperationMetrics>>> {
        self.fallback_operation_metrics
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn fallback_operation_metrics_write(
        &self,
    ) -> std::sync::RwLockWriteGuard<'_, HashMap<&'static str, Arc<DiskOperationMetrics>>> {
        self.fallback_operation_metrics
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn predefined_operation_metric(&self, op: &'static str) -> Option<&DiskOperationMetrics> {
        DISK_OPERATION_INDEX
            .get(op)
            .and_then(|index| self.operation_metrics.get(*index))
            .map(|entry| &entry.metrics)
    }

    fn fallback_operation_metric(&self, op: &'static str) -> Arc<DiskOperationMetrics> {
        if let Some(metrics) = self.fallback_operation_metrics_read().get(op).cloned() {
            return metrics;
        }

        let mut operation_metrics = self.fallback_operation_metrics_write();
        operation_metrics.entry(op).or_default().clone()
    }

    fn record_operation_call(&self, op: &'static str) {
        if let Some(metrics) = self.predefined_operation_metric(op) {
            metrics.record_call_atomic();
        } else {
            self.fallback_operation_metric(op).record_call_atomic();
        }
    }

    fn record_operation_latency(&self, op: &'static str, elapsed: Duration) {
        let now_sec = current_unix_secs();
        if let Some(metrics) = self.predefined_operation_metric(op) {
            metrics.record_latency_atomic(now_sec, elapsed);
        } else {
            self.fallback_operation_metric(op).record_latency_atomic(now_sec, elapsed);
        }
    }

    fn record_availability_error(&self) {
        self.total_errors_availability.fetch_add(1, Ordering::Relaxed);
    }

    fn record_timeout_error(&self) {
        self.total_errors_timeout.fetch_add(1, Ordering::Relaxed);
    }

    fn record_mutation_success(&self, mutation: DiskMetricMutation) {
        match mutation {
            DiskMetricMutation::None => {}
            DiskMetricMutation::Write => {
                self.total_writes.fetch_add(1, Ordering::Relaxed);
            }
            DiskMetricMutation::Delete => {
                self.total_deletes.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn waiting_guard(&self) -> DiskMetricWaitingGuard<'_> {
        self.api_waiting.fetch_add(1, Ordering::Relaxed);
        DiskMetricWaitingGuard { metrics: self }
    }

    fn waiting_count(&self) -> u32 {
        self.api_waiting.load(Ordering::Relaxed)
    }

    fn metrics_snapshot(&self) -> DiskMetrics {
        let now_sec = current_unix_secs();
        let fallback_operation_metrics = self.fallback_operation_metrics_read();
        let mut last_minute = HashMap::with_capacity(self.operation_metrics.len() + fallback_operation_metrics.len());
        let mut api_calls = HashMap::with_capacity(self.operation_metrics.len() + fallback_operation_metrics.len());
        for entry in self.operation_metrics.iter() {
            Self::insert_operation_snapshot(entry.name, &entry.metrics, now_sec, &mut last_minute, &mut api_calls);
        }
        for (op, action) in fallback_operation_metrics.iter() {
            Self::insert_operation_snapshot(op, action, now_sec, &mut last_minute, &mut api_calls);
        }

        DiskMetrics {
            last_minute,
            api_calls,
            total_waiting: self.waiting_count(),
            total_errors_availability: self.total_errors_availability.load(Ordering::Relaxed),
            total_errors_timeout: self.total_errors_timeout.load(Ordering::Relaxed),
            total_writes: self.total_writes.load(Ordering::Relaxed),
            total_deletes: self.total_deletes.load(Ordering::Relaxed),
        }
    }

    fn insert_operation_snapshot(
        op: &str,
        action: &DiskOperationMetrics,
        now_sec: u64,
        last_minute: &mut HashMap<String, TimedAction>,
        api_calls: &mut HashMap<String, u64>,
    ) {
        let lifetime_calls = action.lifetime_calls.load(Ordering::Relaxed);
        if lifetime_calls > 0 {
            last_minute.insert(op.to_string(), action.last_minute_snapshot(now_sec));
            api_calls.insert(op.to_string(), lifetime_calls);
        }
    }
}

struct DiskMetricWaitingGuard<'a> {
    metrics: &'a DiskHealthMetricEpoch,
}

impl Drop for DiskMetricWaitingGuard<'_> {
    fn drop(&mut self) {
        self.metrics.api_waiting.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ReconnectDiskHealthState {
    pub(crate) health: Arc<DiskHealthTracker>,
    pub(crate) metrics: Arc<DiskHealthMetricEpoch>,
}

#[derive(Debug)]
struct DiskOperationMetricEntry {
    name: &'static str,
    metrics: DiskOperationMetrics,
}

#[derive(Debug)]
struct TimedActionSlot {
    version: AtomicU64,
    unix_sec: AtomicU64,
    count: AtomicU64,
    acc_time: AtomicU64,
}

impl Default for TimedActionSlot {
    fn default() -> Self {
        Self {
            version: AtomicU64::new(0),
            unix_sec: AtomicU64::new(0),
            count: AtomicU64::new(0),
            acc_time: AtomicU64::new(0),
        }
    }
}

#[derive(Debug)]
struct DiskOperationMetrics {
    lifetime_calls: AtomicU64,
    last_minute: Box<[TimedActionSlot]>,
}

impl Default for DiskOperationMetrics {
    fn default() -> Self {
        Self {
            lifetime_calls: AtomicU64::new(0),
            last_minute: std::iter::repeat_with(TimedActionSlot::default)
                .take(60)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }
}

impl DiskOperationMetrics {
    #[allow(
        dead_code,
        reason = "internal metrics recorder reached only from record() below (backlog#1823)"
    )]
    fn record_call(&mut self) {
        self.lifetime_calls.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(
        dead_code,
        reason = "internal metrics recorder reached only from record() below (backlog#1823)"
    )]
    fn record_latency(&mut self, now_sec: u64, elapsed: Duration) {
        self.record_latency_atomic(now_sec, elapsed);
    }

    #[allow(dead_code, reason = "metrics roll-up with no caller in this port (backlog#1823)")]
    fn record(&mut self, now_sec: u64, elapsed: Duration) {
        self.record_call();
        self.record_latency(now_sec, elapsed);
    }

    fn record_call_atomic(&self) {
        self.lifetime_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn record_latency_atomic(&self, now_sec: u64, elapsed: Duration) {
        let elapsed_nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        let slot = &self.last_minute[(now_sec % 60) as usize];
        loop {
            let version = slot.version.load(Ordering::Acquire);
            if !version.is_multiple_of(2) {
                std::hint::spin_loop();
                continue;
            }
            if slot
                .version
                .compare_exchange(version, version.wrapping_add(1), Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                if slot.unix_sec.load(Ordering::Acquire) != now_sec {
                    slot.count.store(0, Ordering::Relaxed);
                    slot.acc_time.store(0, Ordering::Relaxed);
                    slot.unix_sec.store(now_sec, Ordering::Release);
                }
                slot.count.fetch_add(1, Ordering::Relaxed);
                slot.acc_time.fetch_add(elapsed_nanos, Ordering::Relaxed);
                slot.version.store(version.wrapping_add(2), Ordering::Release);
                break;
            }
        }
    }

    fn last_minute_snapshot(&self, now_sec: u64) -> TimedAction {
        let mut snapshot = TimedAction::default();
        for slot in &self.last_minute {
            let version = slot.version.load(Ordering::Acquire);
            if !version.is_multiple_of(2) {
                continue;
            }
            let slot_sec = slot.unix_sec.load(Ordering::Acquire);
            let count = slot.count.load(Ordering::Acquire);
            let acc_time = slot.acc_time.load(Ordering::Acquire);
            if slot.version.load(Ordering::Acquire) == version && slot_sec <= now_sec && now_sec.saturating_sub(slot_sec) < 60 {
                snapshot.count = snapshot.count.saturating_add(count);
                snapshot.acc_time = snapshot.acc_time.saturating_add(acc_time);
            }
        }
        snapshot
    }
}

pub(crate) struct DiskHealthWaitingGuard<'a> {
    health: &'a DiskHealthTracker,
}

impl Drop for DiskHealthWaitingGuard<'_> {
    fn drop(&mut self) {
        self.health.decrement_waiting();
    }
}

impl DiskHealthTracker {
    /// Create a new disk health tracker
    pub fn new() -> Self {
        let now = current_unix_time();
        let now_nanos = unix_nanos(now);

        Self {
            last_success: AtomicI64::new(now_nanos),
            last_started: AtomicI64::new(now_nanos),
            status: AtomicU32::new(DISK_HEALTH_OK),
            waiting: AtomicU32::new(0),
            runtime_state: AtomicU32::new(RuntimeDriveHealthState::Online as u32),
            consecutive_failures: AtomicU32::new(0),
            consecutive_successes: AtomicU32::new(0),
            offline_since_unix_secs: AtomicI64::new(0),
            last_transition_unix_secs: AtomicI64::new(unix_secs_i64(now)),
            last_capacity_total: AtomicU64::new(0),
            last_capacity_used: AtomicU64::new(0),
            last_capacity_free: AtomicU64::new(0),
            last_capacity_probe_unix_secs: AtomicI64::new(0),
            state_snapshot: AtomicU64::new(pack_health_state(RuntimeDriveHealthState::Online, DISK_HEALTH_OK)),
            transition_lock: std::sync::Mutex::new(()),
        }
    }

    /// Log a successful operation
    pub fn log_success(&self) {
        self.last_success.store(current_unix_nanos(), Ordering::Relaxed);
    }

    pub fn record_capacity_probe(&self, total: u64, used: u64, free: u64) {
        self.last_capacity_total.store(total, Ordering::Release);
        self.last_capacity_used.store(used, Ordering::Release);
        self.last_capacity_free.store(free, Ordering::Release);
        self.last_capacity_probe_unix_secs
            .store(current_unix_secs() as i64, Ordering::Release);
    }

    pub(crate) fn metric_epoch_for_reconnect(&self) -> Self {
        Self::new()
    }

    pub fn last_capacity_snapshot(&self) -> Option<(u64, u64, u64, u64)> {
        let ts = self.last_capacity_probe_unix_secs.load(Ordering::Acquire);
        if ts <= 0 {
            return None;
        }

        Some((
            self.last_capacity_total.load(Ordering::Acquire),
            self.last_capacity_used.load(Ordering::Acquire),
            self.last_capacity_free.load(Ordering::Acquire),
            ts as u64,
        ))
    }

    /// Check if disk is faulty
    pub fn is_faulty(&self) -> bool {
        unpack_health_state(self.state_snapshot.load(Ordering::Acquire)).1 == DISK_HEALTH_FAULTY
    }

    fn publish_state(&self, runtime_state: RuntimeDriveHealthState, status: u32) {
        self.state_snapshot
            .store(pack_health_state(runtime_state, status), Ordering::Release);
        self.runtime_state.store(runtime_state as u32, Ordering::Release);
        self.status.store(status, Ordering::Release);
    }

    /// Set disk as faulty
    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
    pub fn set_faulty(&self) {
        let _guard = self.transition_lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        self.publish_state(RuntimeDriveHealthState::Offline, DISK_HEALTH_FAULTY);
    }

    /// Set disk as OK
    pub fn set_ok(&self) {
        let _guard = self.transition_lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        self.publish_state(RuntimeDriveHealthState::Online, DISK_HEALTH_OK);
    }

    #[cfg(test)]
    pub fn force_runtime_state_for_test(&self, state: RuntimeDriveHealthState) {
        let _guard = self.transition_lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let status = if state == RuntimeDriveHealthState::Offline {
            DISK_HEALTH_FAULTY
        } else {
            DISK_HEALTH_OK
        };
        self.publish_state(state, status);
    }

    pub fn swap_ok_to_faulty(&self) -> bool {
        let _guard = self.transition_lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let (_, status) = unpack_health_state(self.state_snapshot.load(Ordering::Acquire));
        if status != DISK_HEALTH_OK {
            return false;
        }
        self.publish_state(RuntimeDriveHealthState::Offline, DISK_HEALTH_FAULTY);
        true
    }

    pub fn runtime_state(&self) -> RuntimeDriveHealthState {
        unpack_health_state(self.state_snapshot.load(Ordering::Acquire)).0
    }

    pub fn offline_duration(&self) -> Option<Duration> {
        self.offline_duration_at(current_unix_secs())
    }

    fn offline_duration_at(&self, now: u64) -> Option<Duration> {
        let offline_since = self.offline_since_unix_secs.load(Ordering::Acquire);
        if offline_since <= 0 {
            return None;
        }
        Some(Duration::from_secs(now.saturating_sub(offline_since as u64)))
    }

    pub fn mark_failure(&self, endpoint: &Endpoint, reason: &'static str) -> bool {
        let _guard = self.transition_lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let current = self.runtime_state();
        let now = current_unix_secs();
        let next = match current {
            RuntimeDriveHealthState::Online => {
                self.consecutive_failures.store(1, Ordering::Release);
                self.consecutive_successes.store(0, Ordering::Release);
                self.offline_since_unix_secs
                    .compare_exchange(0, now as i64, Ordering::AcqRel, Ordering::Relaxed)
                    .ok();
                RuntimeDriveHealthState::Suspect
            }
            RuntimeDriveHealthState::Suspect => {
                let failures = self.consecutive_failures.fetch_add(1, Ordering::AcqRel) + 1;
                if failures >= get_drive_suspect_failure_threshold() {
                    RuntimeDriveHealthState::Offline
                } else {
                    RuntimeDriveHealthState::Suspect
                }
            }
            RuntimeDriveHealthState::Returning => {
                self.consecutive_failures.store(0, Ordering::Release);
                self.consecutive_successes.store(0, Ordering::Release);
                RuntimeDriveHealthState::Offline
            }
            RuntimeDriveHealthState::Offline => RuntimeDriveHealthState::Offline,
        };

        let became_offline = next == RuntimeDriveHealthState::Offline && current != RuntimeDriveHealthState::Offline;
        self.transition_state(endpoint, current, next, reason);
        became_offline
    }

    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
    pub fn mark_offline(&self, endpoint: &Endpoint, reason: &'static str) -> bool {
        let _guard = self.transition_lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let current = self.runtime_state();
        if current == RuntimeDriveHealthState::Offline {
            return false;
        }

        self.consecutive_successes.store(0, Ordering::Release);
        self.transition_state(endpoint, current, RuntimeDriveHealthState::Offline, reason);
        true
    }

    /// Clear faulty/offline state so a store-init format load retry can issue RPC again.
    ///
    /// Remote disks are marked faulty on timeout/network errors; the init loop retries with the
    /// same [`DiskStore`] handles, which would otherwise fail immediately at `is_faulty()`.
    pub fn reset_for_store_init_retry(&self, endpoint: &Endpoint) {
        self.reset_for_store_init_retry_at(endpoint, current_unix_time());
    }

    fn reset_for_store_init_retry_at(&self, endpoint: &Endpoint, now: Duration) {
        let _guard = self.transition_lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let now_nanos = unix_nanos(now);
        let now_secs = unix_secs_i64(now);
        self.publish_state(RuntimeDriveHealthState::Online, DISK_HEALTH_OK);
        self.consecutive_failures.store(0, Ordering::Release);
        self.consecutive_successes.store(0, Ordering::Release);
        self.offline_since_unix_secs.store(0, Ordering::Release);
        self.waiting.store(0, Ordering::Release);
        self.last_success.store(now_nanos, Ordering::Relaxed);
        self.last_started.store(now_nanos, Ordering::Relaxed);
        self.last_transition_unix_secs.store(now_secs, Ordering::Release);
        record_drive_runtime_state(endpoint, RuntimeDriveHealthState::Online);
    }

    pub fn mark_recovery_success(&self, endpoint: &Endpoint, reason: &'static str) -> bool {
        let _guard = self.transition_lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let current = self.runtime_state();
        let next = match current {
            RuntimeDriveHealthState::Online => RuntimeDriveHealthState::Online,
            RuntimeDriveHealthState::Suspect => RuntimeDriveHealthState::Online,
            RuntimeDriveHealthState::Offline => {
                self.consecutive_successes.store(1, Ordering::Release);
                RuntimeDriveHealthState::Returning
            }
            RuntimeDriveHealthState::Returning => {
                let successes = self.consecutive_successes.fetch_add(1, Ordering::AcqRel) + 1;
                if successes >= get_drive_returning_success_threshold() {
                    RuntimeDriveHealthState::Online
                } else {
                    RuntimeDriveHealthState::Returning
                }
            }
        };

        let became_online = next == RuntimeDriveHealthState::Online;
        if became_online {
            self.consecutive_failures.store(0, Ordering::Release);
            self.consecutive_successes.store(0, Ordering::Release);
        }
        self.transition_state(endpoint, current, next, reason);
        if became_online {
            self.log_success();
        }
        became_online
    }

    pub fn record_operation_success(&self, endpoint: &Endpoint, reason: &'static str) {
        if self.runtime_state() == RuntimeDriveHealthState::Online {
            self.log_success();
        } else {
            self.mark_recovery_success(endpoint, reason);
        }
    }

    fn transition_state(
        &self,
        endpoint: &Endpoint,
        current: RuntimeDriveHealthState,
        next: RuntimeDriveHealthState,
        reason: &'static str,
    ) {
        if current == next {
            return;
        }

        let current_status = unpack_health_state(self.state_snapshot.load(Ordering::Acquire)).1;
        let status = match next {
            RuntimeDriveHealthState::Offline => DISK_HEALTH_FAULTY,
            RuntimeDriveHealthState::Returning => current_status,
            RuntimeDriveHealthState::Online | RuntimeDriveHealthState::Suspect => DISK_HEALTH_OK,
        };
        self.publish_state(next, status);
        self.last_transition_unix_secs
            .store(current_unix_secs() as i64, Ordering::Release);

        if matches!(
            next,
            RuntimeDriveHealthState::Suspect | RuntimeDriveHealthState::Offline | RuntimeDriveHealthState::Returning
        ) && self.offline_since_unix_secs.load(Ordering::Acquire) == 0
        {
            self.offline_since_unix_secs
                .store(current_unix_secs() as i64, Ordering::Release);
        }

        if next == RuntimeDriveHealthState::Online {
            if let Some(duration) = self.offline_duration() {
                record_drive_offline_duration(endpoint, duration);
                record_drive_recovery_class(classify_drive_recovery(duration));
            }
            self.offline_since_unix_secs.store(0, Ordering::Release);
        } else if let Some(duration) = self.offline_duration() {
            record_drive_offline_duration(endpoint, duration);
        }

        record_drive_state_transition(endpoint, current, next, reason);
        record_drive_runtime_state(endpoint, next);
    }

    /// Increment waiting operations counter
    pub fn increment_waiting(&self) {
        self.waiting.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn waiting_guard(&self) -> DiskHealthWaitingGuard<'_> {
        self.increment_waiting();
        DiskHealthWaitingGuard { health: self }
    }

    /// Decrement waiting operations counter
    pub fn decrement_waiting(&self) {
        self.waiting.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get waiting operations count
    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
    pub fn waiting_count(&self) -> u32 {
        self.waiting.load(Ordering::Relaxed)
    }

    /// Get last success timestamp
    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
    pub fn last_success(&self) -> i64 {
        self.last_success.load(Ordering::Acquire)
    }
}

fn current_unix_secs() -> u64 {
    // Zero is reserved as "not recorded" by health timestamp atomics.
    current_unix_time().as_secs().max(1)
}

fn current_unix_nanos() -> i64 {
    unix_nanos(current_unix_time())
}

fn current_unix_time() -> Duration {
    unix_time_since_epoch(SystemTime::now())
}

fn unix_time_since_epoch(time: SystemTime) -> Duration {
    time.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO)
}

fn unix_nanos(time: Duration) -> i64 {
    i64::try_from(time.as_nanos()).unwrap_or(i64::MAX)
}

fn unix_secs_i64(time: Duration) -> i64 {
    i64::try_from(time.as_secs()).unwrap_or(i64::MAX)
}

fn elapsed_since(last_nanos: i64, now_nanos: i64) -> Duration {
    let elapsed_nanos = now_nanos.saturating_sub(last_nanos).max(0);
    Duration::from_nanos(u64::try_from(elapsed_nanos).unwrap_or(u64::MAX))
}

impl Default for DiskHealthTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// LocalDiskWrapper wraps a DiskStore with health tracking capabilities.
/// This is similar to Go's xlStorageDiskIDCheck.
#[derive(Debug, Clone)]
pub struct LocalDiskWrapper {
    /// The underlying disk store
    disk: Arc<LocalDisk>,
    /// Health tracker
    health: Arc<DiskHealthTracker>,
    /// Internal metrics epoch preserved across local disk reconnects.
    metrics: Arc<DiskHealthMetricEpoch>,
    /// Whether health checking is enabled
    health_check: bool,
    /// Cancellation token for monitoring tasks
    cancel_token: CancellationToken,
    /// Disk ID for stale checking
    disk_id: Arc<RwLock<Option<Uuid>>>,
    /// Timeout policy for scanner-sensitive operations, loaded once on wrapper initialization.
    timeout_health_policy: TimeoutHealthPolicy,
}

impl LocalDiskWrapper {
    /// Create a new LocalDiskWrapper
    pub fn new(disk: Arc<LocalDisk>, health_check: bool) -> Self {
        Self::new_with_health_and_metrics(
            disk,
            health_check,
            Arc::new(DiskHealthTracker::new()),
            Arc::new(DiskHealthMetricEpoch::default()),
        )
    }

    pub(crate) fn new_with_reconnect_state(
        disk: Arc<LocalDisk>,
        health_check: bool,
        reconnect: Option<ReconnectDiskHealthState>,
    ) -> Self {
        let reconnect = reconnect.unwrap_or_else(|| ReconnectDiskHealthState {
            health: Arc::new(DiskHealthTracker::new()),
            metrics: Arc::new(DiskHealthMetricEpoch::default()),
        });
        Self::new_with_health_and_metrics(disk, health_check, reconnect.health, reconnect.metrics)
    }

    fn new_with_health_and_metrics(
        disk: Arc<LocalDisk>,
        health_check: bool,
        health: Arc<DiskHealthTracker>,
        metrics: Arc<DiskHealthMetricEpoch>,
    ) -> Self {
        // Check environment variable for health check override.
        // Only enable if both param and env are true.
        let env_health_check =
            rustfs_utils::get_env_bool(ENV_RUSTFS_DRIVE_ACTIVE_MONITORING, DEFAULT_RUSTFS_DRIVE_ACTIVE_MONITORING);

        let wrapper = Self {
            disk,
            health,
            metrics,
            health_check: health_check && env_health_check,
            cancel_token: CancellationToken::new(),
            disk_id: Arc::new(RwLock::new(None)),
            timeout_health_policy: get_drive_timeout_health_policy(),
        };
        record_drive_runtime_state(&wrapper.disk.endpoint(), RuntimeDriveHealthState::Online);
        wrapper
    }

    pub(crate) fn health_tracker_epoch_for_reconnect(&self) -> ReconnectDiskHealthState {
        ReconnectDiskHealthState {
            health: Arc::new(self.health.metric_epoch_for_reconnect()),
            metrics: self.metrics.clone(),
        }
    }

    pub fn get_disk(&self) -> Arc<LocalDisk> {
        self.disk.clone()
    }

    pub fn get_object_path_if_local(&self, volume: &str, path: &str) -> crate::disk::error::Result<std::path::PathBuf> {
        self.disk.get_object_path(volume, path)
    }

    pub(crate) fn get_object_path_for_io(&self, volume: &str, path: &str) -> crate::disk::error::Result<std::path::PathBuf> {
        self.disk.get_object_path_for_io(volume, path)
    }

    pub(crate) fn get_bucket_path_for_io(&self, volume: &str) -> crate::disk::error::Result<std::path::PathBuf> {
        self.disk.get_bucket_path_for_io(volume)
    }

    pub fn replacement_mount_lease_root(&self) -> Option<std::path::PathBuf> {
        self.disk.replacement_mount_lease_root()
    }

    pub fn runtime_state(&self) -> RuntimeDriveHealthState {
        self.health.runtime_state()
    }

    pub fn offline_duration_secs(&self) -> Option<u64> {
        self.health.offline_duration().map(|duration| duration.as_secs())
    }

    pub fn last_capacity_snapshot(&self) -> Option<(u64, u64, u64, u64)> {
        self.health.last_capacity_snapshot()
    }

    pub fn record_capacity_probe(&self, total: u64, used: u64, free: u64) {
        self.health.record_capacity_probe(total, used, free);
    }

    fn scanner_timeout_health_action(&self) -> TimeoutHealthAction {
        self.timeout_health_policy.scanner_timeout_health_action()
    }

    #[cfg(test)]
    pub fn force_runtime_state_for_test(&self, state: RuntimeDriveHealthState) {
        self.health.force_runtime_state_for_test(state);
    }

    /// Same as [`DiskHealthTracker::reset_for_store_init_retry`]: undo a transient faulty mark before another format load attempt.
    pub fn reset_health_for_store_init_retry(&self) {
        self.health.reset_for_store_init_retry(&self.disk.endpoint());
    }

    #[cfg(test)]
    pub fn health_check_enabled_for_test(&self) -> bool {
        self.health_check
    }

    /// Enable health monitoring after disk creation.
    /// Used to defer health checks until after startup format loading completes.
    pub fn enable_health_check(&self) {
        if !self.health_check {
            return;
        }
        let health = Arc::clone(&self.health);
        let cancel_token = self.cancel_token.clone();
        let disk = Arc::clone(&self.disk);

        tokio::spawn(async move {
            Self::monitor_disk_writable(disk, health, cancel_token).await;
        });
    }

    /// Stop the disk monitoring
    pub async fn stop_monitoring(&self) {
        self.cancel_token.cancel();
    }

    fn spawn_recovery_monitor_if_needed(&self) {
        if !self.health_check {
            return;
        }

        self.health.increment_waiting();
        let health = Arc::clone(&self.health);
        let disk = Arc::clone(&self.disk);
        let cancel_token = self.cancel_token.clone();
        tokio::spawn(async move {
            Self::monitor_disk_status(disk, health, cancel_token).await;
        });
    }

    /// Monitor disk writability periodically
    async fn monitor_disk_writable(disk: Arc<LocalDisk>, health: Arc<DiskHealthTracker>, cancel_token: CancellationToken) {
        let mut interval = time::interval(get_drive_active_check_interval());
        let active_check_timeout = get_drive_active_check_timeout();

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    return;
                }
                _ = interval.tick() => {
                    if cancel_token.is_cancelled() {
                        return;
                    }

                    if health.is_faulty() {
                        continue;
                    }

                    let last_success_nanos = health.last_success.load(Ordering::Relaxed);
                    let elapsed = elapsed_since(last_success_nanos, current_unix_nanos());

                    if elapsed < SKIP_IF_SUCCESS_BEFORE {
                        continue;
                    }

                    tokio::time::sleep(Duration::from_secs(1)).await;



                    let test_obj = format!("health-check-{}", Uuid::new_v4());
                    if Self::perform_health_check(
                        disk.clone(),
                        &TEST_BUCKET,
                        &test_obj,
                        &TEST_DATA,
                        true,
                        active_check_timeout,
                    )
                    .await
                    .is_err()
                        && health.mark_failure(&disk.endpoint(), "active_health_check_failed")
                    {
                        // Health check failed, disk is considered faulty
                        warn!(
                            event = EVENT_DISK_HEALTH_CHECK_FAILED,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_DISK,
                            endpoint = %disk.endpoint(),
                            reason = "faulty_disk",
                            "Disk health check marked disk faulty"
                        );

                        health.increment_waiting(); // Balance the increment from failed operation

                        let health_clone = Arc::clone(&health);
                        let disk_clone = disk.clone();
                        let cancel_clone = cancel_token.clone();

                        tokio::spawn(async move {
                            Self::monitor_disk_status(disk_clone, health_clone, cancel_clone).await;
                        });
                    }
                }
            }
        }
    }

    /// Perform a health check by writing and reading a test file
    async fn perform_health_check(
        disk: Arc<LocalDisk>,
        test_bucket: &str,
        test_filename: &str,
        test_data: &Bytes,
        check_faulty_only: bool,
        timeout_duration: Duration,
    ) -> Result<()> {
        // Perform health check with timeout
        let health_check_result = tokio::time::timeout(timeout_duration, async {
            // Try to write test data
            disk.write_all(test_bucket, test_filename, test_data.clone()).await?;

            // Try to read back the data
            let read_data = disk.read_all(test_bucket, test_filename).await?;

            // Verify data integrity
            if read_data.len() != test_data.len() {
                warn!(
                    event = EVENT_DISK_HEALTH_CHECK_FAILED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK,
                    endpoint = %disk.endpoint(),
                    reason = "data_length_mismatch",
                    expected_bytes = test_data.len(),
                    actual_bytes = read_data.len(),
                    "Disk health check detected data length mismatch"
                );
                if check_faulty_only {
                    return Ok(());
                }
                return Err(DiskError::FaultyDisk);
            }

            // Clean up
            disk.delete(
                test_bucket,
                test_filename,
                DeleteOptions {
                    recursive: false,
                    immediate: false,
                    undo_write: false,
                    undo_delete: false,
                    old_data_dir: None,
                },
            )
            .await?;

            Ok(())
        })
        .await;

        match health_check_result {
            Ok(result) => match result {
                Ok(()) => Ok(()),
                Err(e) => {
                    warn!(
                        event = EVENT_DISK_HEALTH_CHECK_FAILED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK,
                        endpoint = %disk.endpoint(),
                        reason = "operation_failed",
                        error = ?e,
                        "Disk health check failed"
                    );

                    if e == DiskError::FaultyDisk {
                        return Err(e);
                    }

                    if check_faulty_only { Ok(()) } else { Err(e) }
                }
            },
            Err(_) => {
                // Timeout occurred
                warn!(
                    event = EVENT_DISK_HEALTH_CHECK_FAILED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK,
                    endpoint = %disk.endpoint(),
                    reason = "timeout",
                    timeout_secs = timeout_duration.as_secs(),
                    "Disk health check timed out"
                );
                Err(DiskError::FaultyDisk)
            }
        }
    }

    /// Monitor disk status and try to bring it back online
    async fn monitor_disk_status(disk: Arc<LocalDisk>, health: Arc<DiskHealthTracker>, cancel_token: CancellationToken) {
        let check_every = get_drive_returning_probe_interval();
        let active_check_timeout = get_drive_active_check_timeout();

        let mut interval = time::interval(check_every);

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    return;
                }
                _ = interval.tick() => {
                    if cancel_token.is_cancelled() {
                        return;
                    }

                    let test_obj = format!("health-check-{}", Uuid::new_v4());
                    match Self::perform_health_check(
                        disk.clone(),
                        &TEST_BUCKET,
                        &test_obj,
                        &TEST_DATA,
                        false,
                        active_check_timeout,
                    )
                    .await
                    {
                        Ok(_) => {
                            let state_before = health.runtime_state();
                            let is_online = health.mark_recovery_success(&disk.endpoint(), "recovery_probe_success");
                            info!(
                                event = EVENT_DISK_RECOVERY_PROBE_STATE,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_DISK,
                                endpoint = %disk.endpoint(),
                                state = "probe_succeeded",
                                previous_state = ?state_before,
                                "Disk recovery probe state changed"
                            );
                            if !is_online {
                                continue;
                            }
                            info!(
                                event = EVENT_DISK_RECOVERY_PROBE_STATE,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_DISK,
                                endpoint = %disk.endpoint(),
                                state = "online",
                                "Disk recovery probe restored disk online"
                            );
                            health.decrement_waiting();
                            return;
                        }
                        Err(e) => {
                            health.mark_failure(&disk.endpoint(), "recovery_probe_failed");
                            warn!(
                                event = EVENT_DISK_RECOVERY_PROBE_STATE,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_DISK,
                                endpoint = %disk.endpoint(),
                                state = "still_faulty",
                                error = ?e,
                                "Disk recovery probe detected disk still faulty"
                            );
                        }
                    }
                }
            }
        }
    }

    /// Check if disk ID is stale
    async fn check_disk_stale(&self) -> Result<()> {
        let Some(current_disk_id) = *self.disk_id.read().await else {
            return Ok(());
        };

        let stored_disk_id = match self.disk.get_disk_id().await? {
            Some(id) => id,
            None => return Ok(()), // Empty disk ID is allowed during initialization
        };

        if current_disk_id != stored_disk_id {
            return Err(DiskError::DiskNotFound);
        }

        Ok(())
    }

    /// Set the disk ID
    pub async fn set_disk_id_internal(&self, id: Option<Uuid>) -> Result<()> {
        let mut disk_id = self.disk_id.write().await;
        let previous = *disk_id;
        *disk_id = id;
        drop(disk_id);

        if self.disk.is_local() {
            runtime_sources::replace_local_disk_id(previous, id, self.disk.endpoint().to_string()).await;
        }
        Ok(())
    }

    pub(crate) async fn set_disk_id_state(&self, id: Option<Uuid>) {
        *self.disk_id.write().await = id;
    }

    pub(crate) fn metrics_snapshot(&self) -> DiskMetrics {
        self.metrics.metrics_snapshot()
    }

    fn record_result_error_metrics<T>(&self, result: &Result<T>) {
        match result {
            Err(DiskError::Timeout) => self.metrics.record_timeout_error(),
            Err(DiskError::FaultyDisk | DiskError::FaultyRemoteDisk | DiskError::DiskNotFound) => {
                self.metrics.record_availability_error();
            }
            _ => {}
        }
    }

    fn record_batch_delete_error_metrics(&self, result: &[Option<Error>]) {
        let mut saw_timeout = false;
        let mut saw_availability = false;
        for error in result.iter().flatten() {
            match error {
                DiskError::Timeout => saw_timeout = true,
                DiskError::FaultyDisk | DiskError::FaultyRemoteDisk | DiskError::DiskNotFound => saw_availability = true,
                _ => {}
            }
        }
        if saw_timeout {
            self.metrics.record_timeout_error();
        }
        if saw_availability {
            self.metrics.record_availability_error();
        }
    }

    /// Get the current disk ID
    pub async fn get_current_disk_id(&self) -> Option<Uuid> {
        *self.disk_id.read().await
    }

    /// Track disk health for an operation.
    /// This method should wrap disk operations to ensure health checking.
    pub async fn track_disk_health<T, F, Fut>(&self, operation: F, timeout_duration: Duration) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.track_disk_health_with_op("unknown", operation, timeout_duration).await
    }

    async fn track_disk_health_mutation<T, F, Fut>(
        &self,
        op: &'static str,
        mutation: DiskMetricMutation,
        operation: F,
        timeout_duration: Duration,
    ) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.track_disk_health_with_op_timeout_action_and_mutation(
            op,
            operation,
            timeout_duration,
            TimeoutHealthAction::MarkFailure,
            mutation,
        )
        .await
    }

    pub async fn track_disk_health_with_op<T, F, Fut>(
        &self,
        op: &'static str,
        operation: F,
        timeout_duration: Duration,
    ) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.track_disk_health_with_op_and_timeout_action(op, operation, timeout_duration, TimeoutHealthAction::MarkFailure)
            .await
    }

    async fn track_disk_health_with_op_and_timeout_action<T, F, Fut>(
        &self,
        op: &'static str,
        operation: F,
        timeout_duration: Duration,
        timeout_health_action: TimeoutHealthAction,
    ) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.track_disk_health_with_op_timeout_action_and_mutation(
            op,
            operation,
            timeout_duration,
            timeout_health_action,
            DiskMetricMutation::None,
        )
        .await
    }

    async fn track_disk_health_with_op_timeout_action_and_mutation<T, F, Fut>(
        &self,
        op: &'static str,
        operation: F,
        timeout_duration: Duration,
        timeout_health_action: TimeoutHealthAction,
        mutation: DiskMetricMutation,
    ) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.metrics.record_operation_call(op);
        // Check if disk is faulty
        if self.health.is_faulty() {
            self.metrics.record_availability_error();
            warn!(
                event = EVENT_DISK_HEALTH_CHECK_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK,
                endpoint = %self.endpoint(),
                reason = "disk_marked_faulty",
                "Disk health check rejected operation because disk is marked faulty"
            );
            return Err(DiskError::FaultyDisk);
        }

        // Check if disk is stale
        if let Err(err) = self.check_disk_stale().await {
            self.metrics.record_availability_error();
            return Err(err);
        }

        // Record operation start
        self.health.last_started.store(current_unix_nanos(), Ordering::Relaxed);
        let _waiting_guard = self.health.waiting_guard();
        let _metric_waiting_guard = self.metrics.waiting_guard();
        let started = Instant::now();

        if timeout_duration == Duration::ZERO {
            let result = operation().await;
            self.metrics.record_operation_latency(op, started.elapsed());
            self.record_result_error_metrics(&result);
            if result.is_ok() {
                self.health.record_operation_success(&self.endpoint(), "operation_success");
                self.metrics.record_mutation_success(mutation);
            }
            return result;
        }
        // Execute the operation with timeout
        let result = tokio::time::timeout(timeout_duration, operation()).await;

        match result {
            Ok(operation_result) => {
                self.metrics.record_operation_latency(op, started.elapsed());
                self.record_result_error_metrics(&operation_result);
                // Log success; the waiting guard balances every exit path.
                if operation_result.is_ok() {
                    self.health.record_operation_success(&self.endpoint(), "operation_success");
                    self.metrics.record_mutation_success(mutation);
                }
                operation_result
            }
            Err(_) => {
                self.metrics.record_operation_latency(op, started.elapsed());
                self.metrics.record_timeout_error();
                // Timeout occurred, mark disk as potentially faulty.
                if timeout_health_action == TimeoutHealthAction::MarkFailure
                    && self.health.mark_failure(&self.endpoint(), "operation_timeout")
                {
                    self.spawn_recovery_monitor_if_needed();
                }
                counter!(
                    "rustfs_drive_op_timeout_total",
                    "endpoint" => self.endpoint().to_string(),
                    "op" => op.to_string()
                )
                .increment(1);
                warn!(
                    event = EVENT_DISK_HEALTH_CHECK_FAILED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK,
                    endpoint = %self.endpoint(),
                    op,
                    timeout_ms = timeout_duration.as_millis(),
                    reason = "operation_timeout",
                    "Disk operation timed out"
                );
                Err(DiskError::Timeout)
            }
        }
    }
}

#[async_trait::async_trait]
impl DiskAPI for LocalDiskWrapper {
    fn has_replacement_mount_lease(&self) -> bool {
        self.disk.has_replacement_mount_lease()
    }

    async fn read_metadata(&self, volume: &str, path: &str) -> Result<Bytes> {
        self.track_disk_health_with_op_and_timeout_action(
            "read_metadata",
            || async { self.disk.read_metadata(volume, path).await },
            get_drive_metadata_timeout(),
            self.scanner_timeout_health_action(),
        )
        .await
    }

    fn start_scan(&self) -> ScanGuard {
        self.disk.start_scan()
    }

    fn to_string(&self) -> String {
        self.disk.to_string()
    }

    async fn is_online(&self) -> bool {
        let Ok(Some(disk_id)) = self.disk.get_disk_id().await else {
            return false;
        };

        // if disk_id is not set use the current disk_id
        if let Some(current_disk_id) = *self.disk_id.read().await {
            return current_disk_id == disk_id;
        } else {
            // if disk_id is not set, update the disk_id
            let _ = self.set_disk_id_internal(Some(disk_id)).await;
        }

        return true;
    }

    fn is_local(&self) -> bool {
        self.disk.is_local()
    }

    fn host_name(&self) -> String {
        self.disk.host_name()
    }

    fn endpoint(&self) -> Endpoint {
        self.disk.endpoint()
    }

    async fn close(&self) -> Result<()> {
        self.stop_monitoring().await;
        self.disk.close().await
    }

    async fn get_disk_id(&self) -> Result<Option<Uuid>> {
        self.disk.get_disk_id().await
    }

    async fn set_disk_id(&self, id: Option<Uuid>) -> Result<()> {
        self.set_disk_id_internal(id).await
    }

    fn path(&self) -> PathBuf {
        self.disk.path()
    }

    fn get_disk_location(&self) -> DiskLocation {
        self.disk.get_disk_location()
    }

    async fn disk_info(&self, opts: &DiskInfoOptions) -> Result<DiskInfo> {
        if opts.noop && opts.metrics {
            self.metrics.record_operation_call("disk_info");
            let info = DiskInfo {
                metrics: self.metrics_snapshot(),
                ..Default::default()
            };
            if self.health.is_faulty() {
                self.metrics.record_availability_error();
                return Err(DiskError::FaultyDisk);
            }
            return Ok(info);
        }

        if self.health.is_faulty() {
            self.metrics.record_operation_call("disk_info");
            self.metrics.record_availability_error();
            return Err(DiskError::FaultyDisk);
        }

        let result = self
            .track_disk_health_with_op_and_timeout_action(
                "disk_info",
                || async {
                    let result = self.disk.disk_info(opts).await?;

                    if let Some(current_disk_id) = *self.disk_id.read().await
                        && Some(current_disk_id) != result.id
                    {
                        return Err(DiskError::DiskNotFound);
                    };

                    Ok(result)
                },
                get_drive_disk_info_timeout(),
                self.scanner_timeout_health_action(),
            )
            .await;

        result.map(|mut info| {
            if opts.metrics {
                info.metrics = self.metrics_snapshot();
            }
            info
        })
    }

    async fn make_volume(&self, volume: &str) -> Result<()> {
        self.track_disk_health_mutation(
            "make_volume",
            DiskMetricMutation::Write,
            || async { self.disk.make_volume(volume).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn make_volumes(&self, volumes: Vec<&str>) -> Result<()> {
        self.track_disk_health_mutation(
            "make_volumes",
            DiskMetricMutation::Write,
            || async { self.disk.make_volumes(volumes).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        self.track_disk_health_with_op("list_volumes", || async { self.disk.list_volumes().await }, Duration::ZERO)
            .await
    }

    async fn stat_volume(&self, volume: &str) -> Result<VolumeInfo> {
        self.track_disk_health_with_op(
            "stat_volume",
            || async { self.disk.stat_volume(volume).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn delete_volume(&self, volume: &str, force_delete: bool) -> Result<()> {
        self.track_disk_health_mutation(
            "delete_volume",
            DiskMetricMutation::Delete,
            || async { self.disk.delete_volume(volume, force_delete).await },
            Duration::ZERO,
        )
        .await
    }

    async fn walk_dir<W: tokio::io::AsyncWrite + Unpin + Send>(&self, opts: WalkDirOptions, wr: &mut W) -> Result<()> {
        let timeout_duration = if opts.skip_total_timeout {
            Duration::ZERO
        } else {
            opts.timeout_duration().unwrap_or_else(get_drive_walkdir_timeout)
        };

        self.track_disk_health_with_op_and_timeout_action(
            "walk_dir",
            || async { self.disk.walk_dir(opts, wr).await },
            timeout_duration,
            // Listing/scanner backpressure should fail only the current walk, not poison drive health.
            TimeoutHealthAction::IgnoreFailure,
        )
        .await
    }

    async fn delete_version(
        &self,
        volume: &str,
        path: &str,
        fi: FileInfo,
        force_del_marker: bool,
        opts: DeleteOptions,
    ) -> Result<()> {
        self.track_disk_health_mutation(
            "delete_version",
            DiskMetricMutation::Delete,
            || async { self.disk.delete_version(volume, path, fi, force_del_marker, opts).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn delete_versions(&self, volume: &str, versions: Vec<FileInfoVersions>, opts: DeleteOptions) -> Vec<Option<Error>> {
        self.metrics.record_operation_call("delete_versions");
        // Check if disk is faulty before proceeding
        if self.health.is_faulty() {
            self.metrics.record_availability_error();
            return vec![Some(DiskError::FaultyDisk); versions.len()];
        }

        // Check if disk is stale
        if let Err(e) = self.check_disk_stale().await {
            self.metrics.record_availability_error();
            return vec![Some(e); versions.len()];
        }

        // Record operation start
        self.health.last_started.store(current_unix_nanos(), Ordering::Relaxed);
        self.health.increment_waiting();
        let metric_waiting_guard = self.metrics.waiting_guard();
        let started = Instant::now();

        // Execute the operation
        let result = self.disk.delete_versions(volume, versions, opts).await;
        self.metrics.record_operation_latency("delete_versions", started.elapsed());
        self.record_batch_delete_error_metrics(&result);

        self.health.decrement_waiting();
        drop(metric_waiting_guard);
        let has_err = result.iter().any(|e| e.is_some());
        if !has_err {
            // Log success and decrement waiting counter
            self.health.record_operation_success(&self.endpoint(), "operation_success");
            self.metrics.record_mutation_success(DiskMetricMutation::Delete);
        }

        result
    }

    async fn delete_paths(&self, volume: &str, paths: &[String]) -> Result<()> {
        self.track_disk_health_mutation(
            "delete_paths",
            DiskMetricMutation::Delete,
            || async { self.disk.delete_paths(volume, paths).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn acquire_snapshot_lease(&self, volume: &str, path: &str) -> Result<SnapshotLeaseToken> {
        self.track_disk_health_with_op(
            "acquire_snapshot_lease",
            || async { self.disk.acquire_snapshot_lease(volume, path).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn release_snapshot_lease(&self, volume: &str, path: &str, token: SnapshotLeaseToken) -> Result<()> {
        self.track_disk_health_with_op(
            "release_snapshot_lease",
            || async { self.disk.release_snapshot_lease(volume, path, token).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn renew_snapshot_lease(&self, volume: &str, path: &str, token: SnapshotLeaseToken) -> Result<SnapshotLeaseToken> {
        self.track_disk_health_with_op(
            "renew_snapshot_lease",
            || async { self.disk.renew_snapshot_lease(volume, path, token).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn delete_data_dir(&self, volume: &str, path: &str, opts: DeleteOptions) -> Result<DataDirDeleteStatus> {
        self.track_disk_health_mutation(
            "delete_data_dir",
            DiskMetricMutation::Delete,
            || async { self.disk.delete_data_dir(volume, path, opts).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn write_metadata(&self, org_volume: &str, volume: &str, path: &str, fi: FileInfo) -> Result<()> {
        self.track_disk_health_mutation(
            "write_metadata",
            DiskMetricMutation::Write,
            || async { self.disk.write_metadata(org_volume, volume, path, fi).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn update_metadata(&self, volume: &str, path: &str, fi: FileInfo, opts: &UpdateMetadataOpts) -> Result<()> {
        self.track_disk_health_mutation(
            "update_metadata",
            DiskMetricMutation::Write,
            || async { self.disk.update_metadata(volume, path, fi, opts).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn read_version(
        &self,
        org_volume: &str,
        volume: &str,
        path: &str,
        version_id: &str,
        opts: &ReadOptions,
    ) -> Result<FileInfo> {
        self.track_disk_health_with_op(
            "read_version",
            || async { self.disk.read_version(org_volume, volume, path, version_id, opts).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> Result<RawFileInfo> {
        self.track_disk_health_with_op(
            "read_xl",
            || async { self.disk.read_xl(volume, path, read_data).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn rename_data(
        &self,
        src_volume: &str,
        src_path: &str,
        fi: FileInfo,
        dst_volume: &str,
        dst_path: &str,
    ) -> Result<RenameDataResp> {
        self.rename_data_borrowed(src_volume, src_path, &fi, dst_volume, dst_path)
            .await
    }

    async fn list_dir(&self, origvolume: &str, volume: &str, dir_path: &str, count: i32) -> Result<Vec<String>> {
        self.track_disk_health_with_op_and_timeout_action(
            "list_dir",
            || async { self.disk.list_dir(origvolume, volume, dir_path, count).await },
            get_drive_list_dir_timeout(),
            self.scanner_timeout_health_action(),
        )
        .await
    }

    async fn read_file(&self, volume: &str, path: &str) -> Result<crate::disk::FileReader> {
        self.track_disk_health_with_op(
            "read_file",
            || async { self.disk.read_file(volume, path).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn read_file_stream(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<crate::disk::FileReader> {
        self.track_disk_health_with_op(
            "read_file_stream",
            || async { self.disk.read_file_stream(volume, path, offset, length).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn read_file_stream_chunks(
        &self,
        volume: &str,
        path: &str,
        offset: usize,
        length: usize,
    ) -> Result<Option<rustfs_rio::ChunkReaderBox>> {
        self.track_disk_health_with_op(
            "read_file_stream_chunks",
            || async { self.disk.read_file_stream_chunks(volume, path, offset, length).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn read_file_mmap_copy(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<bytes::Bytes> {
        self.track_disk_health_with_op(
            "read_file_mmap_copy",
            || async { self.disk.read_file_mmap_copy(volume, path, offset, length).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn read_file_mmap_copy_with_metrics(
        &self,
        volume: &str,
        path: &str,
        offset: usize,
        length: usize,
        metrics: Option<MmapCopyStageMetrics>,
    ) -> Result<bytes::Bytes> {
        self.track_disk_health_with_op(
            "read_file_mmap_copy_with_metrics",
            || async {
                self.disk
                    .read_file_mmap_copy_with_metrics(volume, path, offset, length, metrics)
                    .await
            },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn append_file(&self, volume: &str, path: &str) -> Result<crate::disk::FileWriter> {
        self.track_disk_health_with_op("append_file", || async { self.disk.append_file(volume, path).await }, Duration::ZERO)
            .await
    }

    async fn create_file(&self, origvolume: &str, volume: &str, path: &str, file_size: i64) -> Result<crate::disk::FileWriter> {
        self.track_disk_health_mutation(
            "create_file",
            DiskMetricMutation::Write,
            || async { self.disk.create_file(origvolume, volume, path, file_size).await },
            Duration::ZERO,
        )
        .await
    }

    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()> {
        self.track_disk_health_mutation(
            "rename_file",
            DiskMetricMutation::Write,
            || async { self.disk.rename_file(src_volume, src_path, dst_volume, dst_path).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn rename_part(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str, meta: Bytes) -> Result<()> {
        self.track_disk_health_mutation(
            "rename_part",
            DiskMetricMutation::Write,
            || async { self.disk.rename_part(src_volume, src_path, dst_volume, dst_path, meta).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn prepare_part_transaction(
        &self,
        src_volume: &str,
        src_path: &str,
        dst_volume: &str,
        dst_path: &str,
        meta: Bytes,
    ) -> Result<()> {
        self.track_disk_health_mutation(
            "prepare_part_transaction",
            DiskMetricMutation::Write,
            || async {
                self.disk
                    .prepare_part_transaction(src_volume, src_path, dst_volume, dst_path, meta)
                    .await
            },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn settle_part_transaction(&self, volume: &str, path: &str, action: crate::disk::PartTransactionAction) -> Result<()> {
        self.track_disk_health_mutation(
            "settle_part_transaction",
            DiskMetricMutation::Write,
            || async { self.disk.settle_part_transaction(volume, path, action).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn delete(&self, volume: &str, path: &str, opt: DeleteOptions) -> Result<()> {
        self.track_disk_health_mutation(
            "delete",
            DiskMetricMutation::Delete,
            || async { self.disk.delete(volume, path, opt).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn verify_file(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        self.track_disk_health_with_op("verify_file", || async { self.disk.verify_file(volume, path, fi).await }, Duration::ZERO)
            .await
    }

    async fn check_parts(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        self.track_disk_health_with_op("check_parts", || async { self.disk.check_parts(volume, path, fi).await }, Duration::ZERO)
            .await
    }

    async fn read_parts(&self, bucket: &str, paths: &[String]) -> Result<Vec<ObjectPartInfo>> {
        self.track_disk_health_with_op("read_parts", || async { self.disk.read_parts(bucket, paths).await }, Duration::ZERO)
            .await
    }

    async fn read_multiple(&self, req: ReadMultipleReq) -> Result<Vec<ReadMultipleResp>> {
        self.track_disk_health_with_op("read_multiple", || async { self.disk.read_multiple(req).await }, Duration::ZERO)
            .await
    }

    async fn write_all(&self, volume: &str, path: &str, data: Bytes) -> Result<()> {
        self.track_disk_health_mutation(
            "write_all",
            DiskMetricMutation::Write,
            || async { self.disk.write_all(volume, path, data).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn compare_and_update_file(
        &self,
        volume: &str,
        path: &str,
        expected: Option<Bytes>,
        replacement: Option<Bytes>,
    ) -> Result<crate::disk::ConditionalFileUpdate> {
        self.track_disk_health_mutation(
            "compare_and_update_file",
            DiskMetricMutation::Write,
            || async { self.disk.compare_and_update_file(volume, path, expected, replacement).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn read_all(&self, volume: &str, path: &str) -> Result<Bytes> {
        self.track_disk_health_with_op(
            "read_all",
            || async { self.disk.read_all(volume, path).await },
            get_max_timeout_duration(),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::endpoint::Endpoint;
    use crate::disk::health_state::RuntimeDriveHealthState;
    use std::{
        io,
        panic::{AssertUnwindSafe, catch_unwind},
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::io::AsyncWrite;

    struct PendingWriter;

    #[test]
    fn disk_health_waiting_guard_balances_cancellation() {
        let health = DiskHealthTracker::new();
        {
            let _guard = health.waiting_guard();
            assert_eq!(health.waiting_count(), 1);
        }
        assert_eq!(health.waiting_count(), 0);
    }

    #[test]
    fn disk_operation_metrics_keep_lifetime_calls_separate_from_last_minute_latency() {
        let mut metrics = DiskOperationMetrics::default();
        metrics.record(10, Duration::from_micros(5));
        metrics.record(69, Duration::from_micros(7));
        metrics.record(70, Duration::from_micros(11));

        assert_eq!(metrics.lifetime_calls.load(Ordering::Relaxed), 3);

        let window = metrics.last_minute_snapshot(70);
        assert_eq!(window.count, 2);
        assert_eq!(window.acc_time, 18_000);
    }

    #[test]
    fn disk_health_metrics_snapshot_exports_waiting_errors_and_operation_windows() {
        let metrics = DiskHealthMetricEpoch::default();
        metrics.record_operation_call("read_all");
        metrics.record_operation_latency("read_all", Duration::from_micros(13));
        metrics.record_availability_error();
        metrics.record_timeout_error();
        {
            let _guard = metrics.waiting_guard();
            let snapshot = metrics.metrics_snapshot();
            assert_eq!(snapshot.total_waiting, 1);
            assert_eq!(snapshot.total_errors_availability, 1);
            assert_eq!(snapshot.total_errors_timeout, 1);
            assert_eq!(snapshot.api_calls.get("read_all"), Some(&1));
            assert_eq!(snapshot.last_minute.get("read_all").map(|action| action.count), Some(1));
        }
    }

    #[test]
    fn disk_health_metrics_snapshot_excludes_recovery_monitor_waiting() {
        let health = DiskHealthTracker::new();
        let metrics = DiskHealthMetricEpoch::default();

        health.increment_waiting();
        let snapshot = metrics.metrics_snapshot();

        assert_eq!(health.waiting_count(), 1);
        assert_eq!(snapshot.total_waiting, 0);
    }

    #[test]
    fn disk_health_metrics_snapshot_keeps_expired_operation_windows() {
        let epoch = DiskHealthMetricEpoch::default();
        let metrics = epoch
            .predefined_operation_metric("read_all")
            .expect("read_all should have a preallocated metrics slot");
        metrics.record_call_atomic();
        metrics.record_latency_atomic(current_unix_secs().saturating_sub(60), Duration::from_micros(13));

        let snapshot = epoch.metrics_snapshot();

        assert_eq!(snapshot.api_calls.get("read_all"), Some(&1));
        assert_eq!(snapshot.last_minute.get("read_all").map(|action| action.count), Some(0));
    }

    #[test]
    fn disk_metric_epoch_recovers_poisoned_map_lock() {
        let epoch = DiskHealthMetricEpoch::default();
        let panic_result = catch_unwind(AssertUnwindSafe(|| {
            let _guard = epoch
                .fallback_operation_metrics
                .write()
                .expect("test should lock fallback operation metrics");
            panic!("poison disk operation metrics lock");
        }));

        assert!(panic_result.is_err());
        epoch.record_operation_call("custom_test_op");
        epoch.record_operation_latency("custom_test_op", Duration::from_micros(13));
        let snapshot = epoch.metrics_snapshot();

        assert_eq!(snapshot.api_calls.get("custom_test_op"), Some(&1));
    }

    #[test]
    fn reconnect_health_tracker_shares_metric_epoch_without_health_state() {
        let health = DiskHealthTracker::new();
        let metrics = Arc::new(DiskHealthMetricEpoch::default());
        metrics.record_operation_call("read_all");
        metrics.record_availability_error();
        health.set_faulty();

        let reconnect = ReconnectDiskHealthState {
            health: Arc::new(health.metric_epoch_for_reconnect()),
            metrics: metrics.clone(),
        };
        metrics.record_operation_call("read_all");
        reconnect.metrics.record_timeout_error();

        assert!(!reconnect.health.is_faulty());
        let snapshot = reconnect.metrics.metrics_snapshot();
        assert_eq!(snapshot.api_calls.get("read_all"), Some(&2));
        assert_eq!(snapshot.total_errors_availability, 1);
        assert_eq!(snapshot.total_errors_timeout, 1);
    }

    #[tokio::test]
    async fn local_disk_health_wrapper_balances_task_cancellation() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("temp dir should be valid UTF-8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let wrapper = Arc::new(LocalDiskWrapper::new(disk, false));
        let task_wrapper = Arc::clone(&wrapper);
        let task = tokio::spawn(async move {
            task_wrapper
                .track_disk_health_with_op(
                    "test_pending",
                    || async { std::future::pending::<Result<()>>().await },
                    Duration::ZERO,
                )
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while wrapper.health.waiting_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("operation should enter disk health tracking");
        task.abort();
        let _ = task.await;

        assert_eq!(wrapper.health.waiting_count(), 0);
        assert_eq!(wrapper.metrics_snapshot().api_calls.get("test_pending"), Some(&1));
    }

    #[tokio::test]
    async fn local_disk_health_wrapper_preserves_legacy_call_shape() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("temp dir should be valid UTF-8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let wrapper = LocalDiskWrapper::new(disk, false);

        wrapper
            .track_disk_health(|| async { Ok(()) }, Duration::ZERO)
            .await
            .expect("legacy health wrapper call should succeed");

        assert_eq!(wrapper.metrics_snapshot().api_calls.get("unknown"), Some(&1));
    }

    #[tokio::test]
    async fn local_disk_health_wrapper_counts_returned_availability_errors() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("temp dir should be valid UTF-8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let wrapper = LocalDiskWrapper::new(disk, false);

        let err = wrapper
            .track_disk_health_with_op("read_all", || async { Err::<(), DiskError>(DiskError::DiskNotFound) }, Duration::ZERO)
            .await
            .expect_err("returned availability error should propagate");

        assert_eq!(err, DiskError::DiskNotFound);
        let snapshot = wrapper.metrics_snapshot();
        assert_eq!(snapshot.api_calls.get("read_all"), Some(&1));
        assert_eq!(snapshot.total_errors_availability, 1);
    }

    #[tokio::test]
    async fn local_disk_health_wrapper_counts_faulty_precheck_rejections() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("temp dir should be valid UTF-8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let wrapper = LocalDiskWrapper::new(disk, false);
        wrapper.health.force_runtime_state_for_test(RuntimeDriveHealthState::Offline);
        let operation_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let operation_ran_in_call = Arc::clone(&operation_ran);

        let err = wrapper
            .track_disk_health_with_op(
                "read_all",
                || async move {
                    operation_ran_in_call.store(true, Ordering::Relaxed);
                    Ok(())
                },
                Duration::ZERO,
            )
            .await
            .expect_err("faulty generic wrapper call should be rejected before operation runs");

        assert_eq!(err, DiskError::FaultyDisk);
        assert!(!operation_ran.load(Ordering::Relaxed));
        let snapshot = wrapper.metrics_snapshot();
        assert_eq!(snapshot.api_calls.get("read_all"), Some(&1));
        assert_eq!(snapshot.total_errors_availability, 1);
    }

    #[tokio::test]
    async fn local_disk_health_wrapper_counts_stale_precheck_rejections() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("temp dir should be valid UTF-8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        {
            let mut format_info = disk.format_info.write().await;
            format_info.id = Some(Uuid::new_v4());
            format_info.file_info = Some(
                tokio::fs::metadata(dir.path())
                    .await
                    .expect("temp dir metadata should be readable"),
            );
            format_info.last_check = Some(::time::OffsetDateTime::now_utc());
        }
        let wrapper = LocalDiskWrapper::new(disk, false);
        wrapper.set_disk_id_state(Some(Uuid::new_v4())).await;
        let operation_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let operation_ran_in_call = Arc::clone(&operation_ran);

        let err = wrapper
            .track_disk_health_with_op(
                "write_all",
                || async move {
                    operation_ran_in_call.store(true, Ordering::Relaxed);
                    Ok(())
                },
                Duration::ZERO,
            )
            .await
            .expect_err("stale generic wrapper call should be rejected before operation runs");

        assert_eq!(err, DiskError::DiskNotFound);
        assert!(!operation_ran.load(Ordering::Relaxed));
        let snapshot = wrapper.metrics_snapshot();
        assert_eq!(snapshot.api_calls.get("write_all"), Some(&1));
        assert_eq!(snapshot.total_errors_availability, 1);
    }

    #[tokio::test]
    async fn delete_versions_counts_returned_batch_error_classes() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("temp dir should be valid UTF-8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let wrapper = LocalDiskWrapper::new(disk, false);

        wrapper.record_batch_delete_error_metrics(&[
            Some(DiskError::DiskNotFound),
            Some(DiskError::FaultyDisk),
            Some(DiskError::Timeout),
            None,
        ]);
        let snapshot = wrapper.metrics_snapshot();

        assert_eq!(snapshot.total_errors_availability, 1);
        assert_eq!(snapshot.total_errors_timeout, 1);
    }

    #[tokio::test]
    async fn local_disk_metrics_count_successful_write_and_delete_mutations() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("temp dir should be valid UTF-8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let wrapper = LocalDiskWrapper::new(disk, false);

        wrapper.make_volume("bucket").await.expect("volume should be created");
        wrapper
            .write_all("bucket", "object", Bytes::from_static(b"data"))
            .await
            .expect("object should be written");
        wrapper
            .delete("bucket", "object", DeleteOptions::default())
            .await
            .expect("object should be deleted");

        let snapshot = wrapper.metrics_snapshot();
        assert_eq!(snapshot.total_writes, 2);
        assert_eq!(snapshot.total_deletes, 1);
    }

    #[tokio::test]
    async fn delete_versions_counts_faulty_drive_availability_rejection() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("temp dir should be valid UTF-8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let wrapper = LocalDiskWrapper::new(disk, false);
        wrapper.health.force_runtime_state_for_test(RuntimeDriveHealthState::Offline);

        let result = wrapper
            .delete_versions("bucket", vec![FileInfoVersions::default()], DeleteOptions::default())
            .await;

        assert_eq!(result.len(), 1);
        assert!(matches!(result.first(), Some(Some(DiskError::FaultyDisk))));
        let snapshot = wrapper.metrics_snapshot();
        assert_eq!(snapshot.api_calls.get("delete_versions"), Some(&1));
        assert_eq!(snapshot.total_errors_availability, 1);
    }

    #[tokio::test]
    async fn disk_info_counts_faulty_noop_metrics_rejection() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("temp dir should be valid UTF-8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let wrapper = LocalDiskWrapper::new(disk, false);
        wrapper.health.force_runtime_state_for_test(RuntimeDriveHealthState::Offline);

        let err = wrapper
            .disk_info(&DiskInfoOptions {
                noop: true,
                metrics: true,
                ..Default::default()
            })
            .await
            .expect_err("faulty disk_info should be rejected");

        assert_eq!(err, DiskError::FaultyDisk);
        let snapshot = wrapper.metrics_snapshot();
        assert_eq!(snapshot.api_calls.get("disk_info"), Some(&1));
        assert_eq!(snapshot.total_errors_availability, 1);
    }

    #[tokio::test]
    async fn delete_versions_counts_stale_drive_availability_rejection() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("temp dir should be valid UTF-8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        {
            let mut format_info = disk.format_info.write().await;
            format_info.id = Some(Uuid::new_v4());
            format_info.file_info = Some(
                tokio::fs::metadata(dir.path())
                    .await
                    .expect("temp dir metadata should be readable"),
            );
            format_info.last_check = Some(::time::OffsetDateTime::now_utc());
        }
        let wrapper = LocalDiskWrapper::new(disk, false);
        wrapper.set_disk_id_state(Some(Uuid::new_v4())).await;

        let result = wrapper
            .delete_versions("bucket", vec![FileInfoVersions::default()], DeleteOptions::default())
            .await;

        assert_eq!(result.len(), 1);
        assert!(matches!(result.first(), Some(Some(DiskError::DiskNotFound))));
        let snapshot = wrapper.metrics_snapshot();
        assert_eq!(snapshot.api_calls.get("delete_versions"), Some(&1));
        assert_eq!(snapshot.total_errors_availability, 1);
    }

    impl AsyncWrite for PendingWriter {
        fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &[u8]) -> Poll<io::Result<usize>> {
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn drive_metadata_timeout_uses_default_when_unset() {
        temp_env::with_var_unset(rustfs_config::ENV_DRIVE_METADATA_TIMEOUT_SECS, || {
            temp_env::with_var_unset(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, || {
                temp_env::with_var_unset(rustfs_config::ENV_DRIVE_TIMEOUT_PROFILE, || {
                    assert_eq!(
                        get_drive_metadata_timeout(),
                        Duration::from_secs(rustfs_config::DEFAULT_DRIVE_METADATA_TIMEOUT_SECS)
                    );
                });
            });
        });
    }

    #[test]
    fn drive_metadata_timeout_uses_high_latency_profile_when_unset() {
        temp_env::with_var_unset(rustfs_config::ENV_DRIVE_METADATA_TIMEOUT_SECS, || {
            temp_env::with_var_unset(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, || {
                temp_env::with_var(
                    rustfs_config::ENV_DRIVE_TIMEOUT_PROFILE,
                    Some(rustfs_config::DRIVE_TIMEOUT_PROFILE_HIGH_LATENCY),
                    || {
                        assert_eq!(
                            get_drive_metadata_timeout(),
                            Duration::from_secs(rustfs_config::DRIVE_TIMEOUT_PROFILE_HIGH_LATENCY_SECS)
                        );
                    },
                );
            });
        });
    }

    #[test]
    fn drive_metadata_timeout_invalid_profile_falls_back_to_default() {
        temp_env::with_var_unset(rustfs_config::ENV_DRIVE_METADATA_TIMEOUT_SECS, || {
            temp_env::with_var_unset(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, || {
                temp_env::with_var(rustfs_config::ENV_DRIVE_TIMEOUT_PROFILE, Some("invalid"), || {
                    assert_eq!(
                        get_drive_metadata_timeout(),
                        Duration::from_secs(rustfs_config::DEFAULT_DRIVE_METADATA_TIMEOUT_SECS)
                    );
                });
            });
        });
    }

    #[test]
    fn drive_metadata_timeout_uses_legacy_fallback_when_canonical_unset() {
        temp_env::with_var_unset(rustfs_config::ENV_DRIVE_METADATA_TIMEOUT_SECS, || {
            temp_env::with_var(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, Some("17"), || {
                assert_eq!(get_drive_metadata_timeout(), Duration::from_secs(17));
            });
        });
    }

    #[test]
    fn drive_metadata_timeout_prefers_canonical_over_legacy() {
        temp_env::with_var(rustfs_config::ENV_DRIVE_METADATA_TIMEOUT_SECS, Some("7"), || {
            temp_env::with_var(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, Some("17"), || {
                assert_eq!(get_drive_metadata_timeout(), Duration::from_secs(7));
            });
        });
    }

    #[test]
    fn drive_walkdir_timeout_uses_default_when_unset() {
        temp_env::with_var_unset(rustfs_config::ENV_DRIVE_WALKDIR_TIMEOUT_SECS, || {
            temp_env::with_var_unset(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, || {
                temp_env::with_var_unset(rustfs_config::ENV_DRIVE_TIMEOUT_PROFILE, || {
                    assert_eq!(
                        get_drive_walkdir_timeout(),
                        Duration::from_secs(rustfs_config::DEFAULT_DRIVE_WALKDIR_TIMEOUT_SECS)
                    );
                });
            });
        });
    }

    #[test]
    fn drive_walkdir_stall_timeout_uses_default_when_unset() {
        temp_env::with_var_unset(rustfs_config::ENV_DRIVE_WALKDIR_STALL_TIMEOUT_SECS, || {
            temp_env::with_var_unset(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, || {
                temp_env::with_var_unset(rustfs_config::ENV_DRIVE_TIMEOUT_PROFILE, || {
                    assert_eq!(
                        get_drive_walkdir_stall_timeout(),
                        Duration::from_secs(rustfs_config::DEFAULT_DRIVE_WALKDIR_STALL_TIMEOUT_SECS)
                    );
                });
            });
        });
    }

    #[test]
    fn drive_walkdir_peek_timeout_uses_wider_default_when_unset() {
        temp_env::with_var_unset(rustfs_config::ENV_DRIVE_WALKDIR_PEEK_TIMEOUT_SECS, || {
            temp_env::with_var_unset(rustfs_config::ENV_DRIVE_WALKDIR_STALL_TIMEOUT_SECS, || {
                temp_env::with_var_unset(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, || {
                    temp_env::with_var_unset(rustfs_config::ENV_DRIVE_TIMEOUT_PROFILE, || {
                        assert_eq!(
                            get_drive_walkdir_peek_timeout(),
                            Duration::from_secs(rustfs_config::DEFAULT_DRIVE_WALKDIR_PEEK_TIMEOUT_SECS)
                        );
                    });
                });
            });
        });
    }

    #[test]
    fn drive_walkdir_peek_timeout_is_never_stricter_than_stall_timeout() {
        temp_env::with_var(rustfs_config::ENV_DRIVE_WALKDIR_PEEK_TIMEOUT_SECS, Some("3"), || {
            temp_env::with_var(rustfs_config::ENV_DRIVE_WALKDIR_STALL_TIMEOUT_SECS, Some("13"), || {
                temp_env::with_var_unset(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, || {
                    assert_eq!(get_drive_walkdir_peek_timeout(), Duration::from_secs(13));
                });
            });
        });
    }

    #[test]
    fn drive_walkdir_peek_timeout_prefers_canonical_over_legacy() {
        temp_env::with_var(rustfs_config::ENV_DRIVE_WALKDIR_PEEK_TIMEOUT_SECS, Some("23"), || {
            temp_env::with_var(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, Some("17"), || {
                temp_env::with_var_unset(rustfs_config::ENV_DRIVE_WALKDIR_STALL_TIMEOUT_SECS, || {
                    assert_eq!(get_drive_walkdir_peek_timeout(), Duration::from_secs(23));
                });
            });
        });
    }

    #[test]
    fn drive_walkdir_peek_timeout_uses_high_latency_profile_default() {
        temp_env::with_var_unset(rustfs_config::ENV_DRIVE_WALKDIR_PEEK_TIMEOUT_SECS, || {
            temp_env::with_var_unset(rustfs_config::ENV_DRIVE_WALKDIR_STALL_TIMEOUT_SECS, || {
                temp_env::with_var_unset(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, || {
                    temp_env::with_var(
                        rustfs_config::ENV_DRIVE_TIMEOUT_PROFILE,
                        Some(rustfs_config::DRIVE_TIMEOUT_PROFILE_HIGH_LATENCY),
                        || {
                            assert_eq!(
                                get_drive_walkdir_peek_timeout(),
                                Duration::from_secs(rustfs_config::DRIVE_TIMEOUT_PROFILE_HIGH_LATENCY_SECS * 2)
                            );
                        },
                    );
                });
            });
        });
    }

    #[test]
    fn drive_walkdir_timeout_prefers_canonical_over_legacy() {
        temp_env::with_var(rustfs_config::ENV_DRIVE_WALKDIR_TIMEOUT_SECS, Some("11"), || {
            temp_env::with_var(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, Some("17"), || {
                assert_eq!(get_drive_walkdir_timeout(), Duration::from_secs(11));
            });
        });
    }

    #[test]
    fn drive_walkdir_stall_timeout_prefers_canonical_over_legacy() {
        temp_env::with_var(rustfs_config::ENV_DRIVE_WALKDIR_STALL_TIMEOUT_SECS, Some("13"), || {
            temp_env::with_var(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, Some("17"), || {
                assert_eq!(get_drive_walkdir_stall_timeout(), Duration::from_secs(13));
            });
        });
    }

    #[test]
    fn object_disk_write_stall_timeout_default_and_override() {
        temp_env::with_var_unset(rustfs_config::ENV_OBJECT_DISK_WRITE_STALL_TIMEOUT, || {
            assert_eq!(
                get_object_disk_write_stall_timeout(),
                Duration::from_secs(rustfs_config::DEFAULT_OBJECT_DISK_WRITE_STALL_TIMEOUT)
            );
        });
        temp_env::with_var(rustfs_config::ENV_OBJECT_DISK_WRITE_STALL_TIMEOUT, Some("9"), || {
            assert_eq!(get_object_disk_write_stall_timeout(), Duration::from_secs(9));
        });
        temp_env::with_var(rustfs_config::ENV_OBJECT_DISK_WRITE_STALL_TIMEOUT, Some("0"), || {
            assert!(get_object_disk_write_stall_timeout().is_zero(), "0 disables the stall deadline");
        });
    }

    #[test]
    fn object_disk_write_absolute_cap_defaults_disabled() {
        temp_env::with_var_unset(rustfs_config::ENV_OBJECT_DISK_WRITE_ABSOLUTE_CAP, || {
            assert!(get_object_disk_write_absolute_cap().is_zero(), "absolute cap is disabled by default");
        });
        temp_env::with_var(rustfs_config::ENV_OBJECT_DISK_WRITE_ABSOLUTE_CAP, Some("120"), || {
            assert_eq!(get_object_disk_write_absolute_cap(), Duration::from_secs(120));
        });
    }

    #[test]
    fn object_disk_read_timeout_uses_default_when_unset() {
        temp_env::with_var_unset(rustfs_config::ENV_OBJECT_DISK_READ_TIMEOUT, || {
            temp_env::with_var_unset(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, || {
                temp_env::with_var_unset(rustfs_config::ENV_DRIVE_TIMEOUT_PROFILE, || {
                    assert_eq!(
                        get_object_disk_read_timeout(),
                        Duration::from_secs(rustfs_config::DEFAULT_OBJECT_DISK_READ_TIMEOUT)
                    );
                });
            });
        });
    }

    #[test]
    fn object_disk_read_timeout_uses_high_latency_profile_when_unset() {
        temp_env::with_var_unset(rustfs_config::ENV_OBJECT_DISK_READ_TIMEOUT, || {
            temp_env::with_var_unset(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, || {
                temp_env::with_var(
                    rustfs_config::ENV_DRIVE_TIMEOUT_PROFILE,
                    Some(rustfs_config::DRIVE_TIMEOUT_PROFILE_HIGH_LATENCY),
                    || {
                        assert_eq!(
                            get_object_disk_read_timeout(),
                            Duration::from_secs(rustfs_config::DRIVE_TIMEOUT_PROFILE_HIGH_LATENCY_SECS)
                        );
                    },
                );
            });
        });
    }

    #[test]
    fn object_disk_read_timeout_prefers_canonical_over_legacy() {
        temp_env::with_var(rustfs_config::ENV_OBJECT_DISK_READ_TIMEOUT, Some("7"), || {
            temp_env::with_var(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, Some("17"), || {
                assert_eq!(get_object_disk_read_timeout(), Duration::from_secs(7));
            });
        });
    }

    #[test]
    fn drive_active_check_interval_uses_default_when_unset() {
        temp_env::with_var_unset(rustfs_config::ENV_DRIVE_ACTIVE_CHECK_INTERVAL_SECS, || {
            assert_eq!(
                get_drive_active_check_interval(),
                Duration::from_secs(rustfs_config::DEFAULT_DRIVE_ACTIVE_CHECK_INTERVAL_SECS)
            );
        });
    }

    #[test]
    fn drive_active_check_interval_reads_env_override() {
        temp_env::with_var(rustfs_config::ENV_DRIVE_ACTIVE_CHECK_INTERVAL_SECS, Some("3"), || {
            assert_eq!(get_drive_active_check_interval(), Duration::from_secs(3));
        });
    }

    #[test]
    fn drive_active_check_timeout_uses_default_when_unset() {
        temp_env::with_var_unset(rustfs_config::ENV_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS, || {
            assert_eq!(
                get_drive_active_check_timeout(),
                Duration::from_secs(rustfs_config::DEFAULT_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS)
            );
        });
    }

    #[test]
    fn drive_active_check_timeout_reads_env_override() {
        temp_env::with_var(rustfs_config::ENV_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS, Some("1"), || {
            assert_eq!(get_drive_active_check_timeout(), Duration::from_secs(1));
        });
    }

    #[test]
    fn runtime_state_transitions_from_online_to_suspect_then_offline() {
        temp_env::with_var(rustfs_config::ENV_DRIVE_SUSPECT_FAILURE_THRESHOLD, Some("2"), || {
            let endpoint = Endpoint::try_from("/tmp/runtime-state-disk").expect("endpoint should parse");
            let health = DiskHealthTracker::new();

            assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Online);
            assert!(!health.mark_failure(&endpoint, "timeout"));
            assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Suspect);
            assert!(!health.is_faulty());

            assert!(health.mark_failure(&endpoint, "timeout"));
            assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Offline);
            assert!(health.is_faulty());
            assert!(health.offline_duration().is_some());
        });
    }

    #[test]
    fn runtime_state_transitions_back_online_after_recovery_threshold() {
        temp_env::with_var(rustfs_config::ENV_DRIVE_SUSPECT_FAILURE_THRESHOLD, Some("2"), || {
            let endpoint = Endpoint::try_from("/tmp/runtime-state-recovery").expect("endpoint should parse");
            let health = DiskHealthTracker::new();

            health.mark_failure(&endpoint, "timeout");
            health.mark_failure(&endpoint, "timeout");
            assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Offline);

            assert!(!health.mark_recovery_success(&endpoint, "probe"));
            assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Returning);

            assert!(!health.mark_recovery_success(&endpoint, "probe"));
            assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Returning);

            assert!(health.mark_recovery_success(&endpoint, "probe"));
            assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Online);
            assert!(health.offline_duration().is_none());
        });
    }

    #[test]
    #[serial_test::serial]
    fn concurrent_failure_and_recovery_publish_one_health_snapshot() {
        temp_env::with_var(rustfs_config::ENV_DRIVE_SUSPECT_FAILURE_THRESHOLD, Some("2"), || {
            let endpoint = Endpoint::try_from("/tmp/concurrent-health-snapshot").expect("endpoint should parse");
            let health = Arc::new(DiskHealthTracker::new());
            let transition_guard = health
                .transition_lock
                .lock()
                .expect("health transition lock should not be poisoned");
            let start = Arc::new(std::sync::Barrier::new(3));
            let (completed_tx, completed_rx) = std::sync::mpsc::channel();
            let workers = (0..2)
                .map(|_| {
                    let health = Arc::clone(&health);
                    let endpoint = endpoint.clone();
                    let start = Arc::clone(&start);
                    let completed_tx = completed_tx.clone();
                    std::thread::spawn(move || {
                        start.wait();
                        health.mark_failure(&endpoint, "concurrent_test");
                        completed_tx.send(()).expect("completion receiver should remain available");
                    })
                })
                .collect::<Vec<_>>();

            start.wait();
            assert!(
                matches!(
                    completed_rx.recv_timeout(Duration::from_millis(250)),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout)
                ),
                "concurrent transitions must wait for the serialization lock"
            );
            drop(transition_guard);
            completed_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("first failure transition should complete after lock release");
            completed_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("second failure transition should complete after lock release");
            for worker in workers {
                worker.join().expect("health transition worker should not panic");
            }

            assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Offline);
            assert!(health.is_faulty());
            assert_eq!(health.consecutive_failures.load(Ordering::Acquire), 2);
        });
    }

    #[test]
    fn operation_success_recovers_suspect_drive_without_faulting() {
        let endpoint = Endpoint::try_from("/tmp/runtime-state-suspect-success").expect("endpoint should parse");
        let health = DiskHealthTracker::new();

        assert!(!health.mark_failure(&endpoint, "timeout"));
        assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Suspect);
        assert!(!health.is_faulty());

        health.record_operation_success(&endpoint, "operation_success");
        assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Online);
        assert!(!health.is_faulty());
        assert!(health.offline_duration().is_none());
    }

    #[tokio::test]
    async fn ignored_timeout_does_not_mark_drive_failure() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("temp dir should be valid UTF-8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let wrapper = LocalDiskWrapper::new(disk, false);

        let result = wrapper
            .track_disk_health_with_op_and_timeout_action(
                "walk_dir",
                || async {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    Ok(())
                },
                Duration::from_millis(1),
                TimeoutHealthAction::IgnoreFailure,
            )
            .await;

        assert_eq!(result.expect_err("operation should time out"), DiskError::Timeout);
        assert_eq!(wrapper.runtime_state(), RuntimeDriveHealthState::Online);
        assert!(!wrapper.health.is_faulty());
    }

    #[tokio::test]
    async fn walk_dir_writer_backpressure_timeout_does_not_mark_drive_failure() {
        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_DRIVE_WALKDIR_TIMEOUT_SECS, Some("1")),
                (
                    rustfs_config::ENV_DRIVE_TIMEOUT_HEALTH_ACTION,
                    Some(rustfs_config::DRIVE_TIMEOUT_HEALTH_ACTION_IGNORE_SCANNER),
                ),
            ],
            async {
                let dir = tempfile::tempdir().expect("temp dir should be created");
                let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be valid UTF-8"))
                    .expect("endpoint should parse");
                let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
                let wrapper = LocalDiskWrapper::new(disk, false);
                let bucket = "test-bucket";
                let object = "test-object";

                wrapper.make_volume(bucket).await.expect("bucket should be created");

                let mut file_info = FileInfo::new(&format!("{bucket}/{object}"), 1, 0);
                file_info.volume = bucket.to_string();
                file_info.name = object.to_string();
                file_info.mod_time = Some(::time::OffsetDateTime::now_utc());
                file_info.erasure.index = 1;

                wrapper
                    .write_metadata("", bucket, object, file_info)
                    .await
                    .expect("object metadata should be written");

                let mut writer = PendingWriter;
                let result = wrapper
                    .walk_dir(
                        WalkDirOptions {
                            bucket: bucket.to_string(),
                            recursive: true,
                            ..Default::default()
                        },
                        &mut writer,
                    )
                    .await;

                assert_eq!(result.expect_err("walk_dir should time out"), DiskError::Timeout);
                assert_eq!(wrapper.runtime_state(), RuntimeDriveHealthState::Online);
                assert!(!wrapper.health.is_faulty());
            },
        )
        .await;
    }

    #[tokio::test]
    async fn walk_dir_writer_backpressure_timeout_does_not_mark_drive_failure_by_default() {
        temp_env::async_with_vars([(rustfs_config::ENV_DRIVE_WALKDIR_TIMEOUT_SECS, Some("1"))], async {
            let dir = tempfile::tempdir().expect("temp dir should be created");
            let endpoint =
                Endpoint::try_from(dir.path().to_str().expect("temp dir should be valid UTF-8")).expect("endpoint should parse");
            let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
            let wrapper = LocalDiskWrapper::new(disk, false);
            let bucket = "test-bucket";
            let object = "test-object";

            wrapper.make_volume(bucket).await.expect("bucket should be created");

            let mut file_info = FileInfo::new(&format!("{bucket}/{object}"), 1, 0);
            file_info.volume = bucket.to_string();
            file_info.name = object.to_string();
            file_info.mod_time = Some(::time::OffsetDateTime::now_utc());
            file_info.erasure.index = 1;

            wrapper
                .write_metadata("", bucket, object, file_info)
                .await
                .expect("object metadata should be written");

            let mut writer = PendingWriter;
            let result = wrapper
                .walk_dir(
                    WalkDirOptions {
                        bucket: bucket.to_string(),
                        recursive: true,
                        ..Default::default()
                    },
                    &mut writer,
                )
                .await;

            assert_eq!(result.expect_err("walk_dir should time out"), DiskError::Timeout);
            assert_eq!(wrapper.runtime_state(), RuntimeDriveHealthState::Online);
            assert!(!wrapper.health.is_faulty());
        })
        .await;
    }

    #[tokio::test]
    async fn walk_dir_uses_per_request_timeout_before_env_default() {
        temp_env::async_with_vars([(rustfs_config::ENV_DRIVE_WALKDIR_TIMEOUT_SECS, Some("60"))], async {
            let dir = tempfile::tempdir().expect("temp dir should be created");
            let endpoint =
                Endpoint::try_from(dir.path().to_str().expect("temp dir should be valid UTF-8")).expect("endpoint should parse");
            let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
            let wrapper = LocalDiskWrapper::new(disk, false);
            let bucket = "test-bucket";
            let object = "test-object";

            wrapper.make_volume(bucket).await.expect("bucket should be created");

            let mut file_info = FileInfo::new(&format!("{bucket}/{object}"), 1, 0);
            file_info.volume = bucket.to_string();
            file_info.name = object.to_string();
            file_info.mod_time = Some(::time::OffsetDateTime::now_utc());
            file_info.erasure.index = 1;

            wrapper
                .write_metadata("", bucket, object, file_info)
                .await
                .expect("object metadata should be written");

            let mut writer = PendingWriter;
            let result = wrapper
                .walk_dir(
                    WalkDirOptions {
                        bucket: bucket.to_string(),
                        recursive: true,
                        timeout_ms: Some(10),
                        ..Default::default()
                    },
                    &mut writer,
                )
                .await;

            assert_eq!(result.expect_err("walk_dir should use per-request timeout"), DiskError::Timeout);
            assert_eq!(wrapper.runtime_state(), RuntimeDriveHealthState::Online);
            assert!(!wrapper.health.is_faulty());
        })
        .await;
    }

    #[tokio::test]
    async fn walk_dir_total_timeout_disable_modes_keep_stream_pending() {
        temp_env::async_with_vars([(rustfs_config::ENV_DRIVE_WALKDIR_TIMEOUT_SECS, Some("1"))], async {
            let dir = tempfile::tempdir().expect("temp dir should be created");
            let endpoint =
                Endpoint::try_from(dir.path().to_str().expect("temp dir should be valid UTF-8")).expect("endpoint should parse");
            let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
            let wrapper = LocalDiskWrapper::new(disk, false);
            let bucket = "test-bucket";
            let object = "test-object";

            wrapper.make_volume(bucket).await.expect("bucket should be created");

            let mut file_info = FileInfo::new(&format!("{bucket}/{object}"), 1, 0);
            file_info.volume = bucket.to_string();
            file_info.name = object.to_string();
            file_info.mod_time = Some(::time::OffsetDateTime::now_utc());
            file_info.erasure.index = 1;

            wrapper
                .write_metadata("", bucket, object, file_info)
                .await
                .expect("object metadata should be written");

            for (reason, options) in [
                (
                    "skip_total_timeout",
                    WalkDirOptions {
                        bucket: bucket.to_string(),
                        recursive: true,
                        skip_total_timeout: true,
                        ..Default::default()
                    },
                ),
                (
                    "zero per-request timeout",
                    WalkDirOptions {
                        bucket: bucket.to_string(),
                        recursive: true,
                        timeout_ms: Some(0),
                        stall_timeout_ms: None,
                        ..Default::default()
                    },
                ),
            ] {
                let mut writer = PendingWriter;
                let result = tokio::time::timeout(Duration::from_millis(1_100), wrapper.walk_dir(options, &mut writer)).await;

                assert!(result.is_err(), "{reason} should leave backpressured walk pending");
                assert_eq!(wrapper.runtime_state(), RuntimeDriveHealthState::Online);
                assert!(!wrapper.health.is_faulty());
            }
        })
        .await;
    }

    #[tokio::test]
    async fn walk_dir_timeout_does_not_break_followup_stat_volume() {
        temp_env::async_with_vars([(rustfs_config::ENV_DRIVE_WALKDIR_TIMEOUT_SECS, Some("1"))], async {
            let dir = tempfile::tempdir().expect("temp dir should be created");
            let endpoint =
                Endpoint::try_from(dir.path().to_str().expect("temp dir should be valid UTF-8")).expect("endpoint should parse");
            let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
            let wrapper = LocalDiskWrapper::new(disk, false);
            let bucket = "test-bucket";
            let object = "test-object";

            wrapper.make_volume(bucket).await.expect("bucket should be created");

            let mut file_info = FileInfo::new(&format!("{bucket}/{object}"), 1, 0);
            file_info.volume = bucket.to_string();
            file_info.name = object.to_string();
            file_info.mod_time = Some(::time::OffsetDateTime::now_utc());
            file_info.erasure.index = 1;

            wrapper
                .write_metadata("", bucket, object, file_info)
                .await
                .expect("object metadata should be written");

            let mut writer = PendingWriter;
            let walk_err = wrapper
                .walk_dir(
                    WalkDirOptions {
                        bucket: bucket.to_string(),
                        recursive: true,
                        ..Default::default()
                    },
                    &mut writer,
                )
                .await
                .expect_err("walk_dir should time out");

            assert_eq!(walk_err, DiskError::Timeout);
            assert_eq!(wrapper.runtime_state(), RuntimeDriveHealthState::Online);
            assert!(!wrapper.health.is_faulty());
            assert_eq!(wrapper.metrics_snapshot().total_errors_timeout, 1);

            let info = wrapper
                .stat_volume(bucket)
                .await
                .expect("follow-up bucket stat should still succeed after walk timeout");
            assert_eq!(info.name, bucket);
            assert_eq!(wrapper.runtime_state(), RuntimeDriveHealthState::Online);
            assert!(!wrapper.health.is_faulty());
        })
        .await;
    }

    #[tokio::test]
    async fn default_timeout_marks_drive_failure() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("temp dir should be valid UTF-8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let wrapper = LocalDiskWrapper::new(disk, false);

        let result = wrapper
            .track_disk_health_with_op(
                "read_metadata",
                || async {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    Ok(())
                },
                Duration::from_millis(1),
            )
            .await;

        assert_eq!(result.expect_err("operation should time out"), DiskError::Timeout);
        assert_eq!(wrapper.runtime_state(), RuntimeDriveHealthState::Suspect);
    }

    #[test]
    #[serial_test::serial]
    fn drive_timeout_health_policy_defaults_to_mark_failure() {
        temp_env::with_var_unset(rustfs_config::ENV_DRIVE_TIMEOUT_HEALTH_ACTION, || {
            let policy = get_drive_timeout_health_policy();
            assert_eq!(policy, TimeoutHealthPolicy::MarkFailure);
            assert_eq!(policy.scanner_timeout_health_action(), TimeoutHealthAction::MarkFailure);
        });
    }

    #[test]
    #[serial_test::serial]
    fn drive_timeout_health_policy_respects_ignore_scanner() {
        temp_env::with_var(
            rustfs_config::ENV_DRIVE_TIMEOUT_HEALTH_ACTION,
            Some(rustfs_config::DRIVE_TIMEOUT_HEALTH_ACTION_IGNORE_SCANNER),
            || {
                let policy = get_drive_timeout_health_policy();
                assert_eq!(policy, TimeoutHealthPolicy::IgnoreScanner);
                assert_eq!(policy.scanner_timeout_health_action(), TimeoutHealthAction::IgnoreFailure);
            },
        );
    }

    #[test]
    #[serial_test::serial]
    fn drive_timeout_health_policy_invalid_value_falls_back_to_default() {
        temp_env::with_var(rustfs_config::ENV_DRIVE_TIMEOUT_HEALTH_ACTION, Some("invalid"), || {
            let policy = get_drive_timeout_health_policy();
            assert_eq!(policy, TimeoutHealthPolicy::MarkFailure);
            assert_eq!(policy.scanner_timeout_health_action(), TimeoutHealthAction::MarkFailure);
        });
    }

    #[test]
    fn reset_for_store_init_retry_clears_faulty_and_back_online() {
        let endpoint = Endpoint::try_from("/tmp/reset-store-init-retry").expect("endpoint should parse");
        let health = DiskHealthTracker::new();

        assert!(health.mark_offline(&endpoint, "simulated_fault"));
        assert!(health.is_faulty());
        assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Offline);

        health.reset_for_store_init_retry(&endpoint);
        assert!(!health.is_faulty());
        assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Online);

        assert!(health.mark_offline(&endpoint, "again"));
        assert!(health.is_faulty());
    }

    #[test]
    fn unix_time_clamps_epoch_and_pre_epoch_to_zero() {
        let before_epoch = UNIX_EPOCH
            .checked_sub(Duration::from_nanos(1))
            .expect("one nanosecond before the Unix epoch should be representable");

        assert_eq!(unix_time_since_epoch(UNIX_EPOCH), Duration::ZERO);
        assert_eq!(unix_time_since_epoch(before_epoch), Duration::ZERO);
    }

    #[test]
    fn elapsed_and_offline_duration_saturate_on_clock_rollback() {
        let health = DiskHealthTracker::new();
        health.offline_since_unix_secs.store(10, Ordering::Release);

        assert_eq!(elapsed_since(10, 12), Duration::from_nanos(2));
        assert_eq!(elapsed_since(10, 9), Duration::ZERO);
        assert_eq!(health.offline_duration_at(9), Some(Duration::ZERO));
    }

    #[test]
    fn pre_epoch_retry_reset_updates_the_complete_health_state() {
        let endpoint = Endpoint::try_from("/tmp/reset-store-init-retry-pre-epoch").expect("endpoint should parse");
        let health = DiskHealthTracker::new();
        health.status.store(DISK_HEALTH_FAULTY, Ordering::Release);
        health
            .runtime_state
            .store(RuntimeDriveHealthState::Offline as u32, Ordering::Release);
        health.consecutive_failures.store(3, Ordering::Release);
        health.consecutive_successes.store(2, Ordering::Release);
        health.offline_since_unix_secs.store(11, Ordering::Release);
        health.waiting.store(4, Ordering::Release);
        health.last_success.store(12, Ordering::Release);
        health.last_started.store(13, Ordering::Release);
        health.last_transition_unix_secs.store(14, Ordering::Release);

        health.reset_for_store_init_retry_at(&endpoint, unix_time_since_epoch(UNIX_EPOCH - Duration::from_nanos(1)));

        assert_eq!(health.status.load(Ordering::Acquire), DISK_HEALTH_OK);
        assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Online);
        assert_eq!(health.consecutive_failures.load(Ordering::Acquire), 0);
        assert_eq!(health.consecutive_successes.load(Ordering::Acquire), 0);
        assert_eq!(health.offline_since_unix_secs.load(Ordering::Acquire), 0);
        assert_eq!(health.waiting.load(Ordering::Acquire), 0);
        assert_eq!(health.last_success.load(Ordering::Acquire), 0);
        assert_eq!(health.last_started.load(Ordering::Acquire), 0);
        assert_eq!(health.last_transition_unix_secs.load(Ordering::Acquire), 0);
    }
}
