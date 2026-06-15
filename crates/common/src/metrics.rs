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

use crate::heal_channel::HealScanMode;
use crate::last_minute::{AccElem, LastMinuteLatency};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Display,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IlmAction {
    NoneAction = 0,
    DeleteAction,
    DeleteVersionAction,
    TransitionAction,
    TransitionVersionAction,
    DeleteRestoredAction,
    DeleteRestoredVersionAction,
    DeleteAllVersionsAction,
    DelMarkerDeleteAllVersionsAction,
    ActionCount,
}

impl IlmAction {
    pub fn delete_restored(&self) -> bool {
        *self == Self::DeleteRestoredAction || *self == Self::DeleteRestoredVersionAction
    }

    pub fn delete_versioned(&self) -> bool {
        *self == Self::DeleteVersionAction || *self == Self::DeleteRestoredVersionAction
    }

    pub fn delete_all(&self) -> bool {
        *self == Self::DeleteAllVersionsAction || *self == Self::DelMarkerDeleteAllVersionsAction
    }

    pub fn delete(&self) -> bool {
        if self.delete_restored() {
            return true;
        }
        *self == Self::DeleteVersionAction
            || *self == Self::DeleteAction
            || *self == Self::DeleteAllVersionsAction
            || *self == Self::DelMarkerDeleteAllVersionsAction
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NoneAction => "none",
            Self::DeleteAction => "delete",
            Self::DeleteVersionAction => "delete_version",
            Self::TransitionAction => "transition",
            Self::TransitionVersionAction => "transition_version",
            Self::DeleteRestoredAction => "delete_restored",
            Self::DeleteRestoredVersionAction => "delete_restored_version",
            Self::DeleteAllVersionsAction => "delete_all_versions",
            Self::DelMarkerDeleteAllVersionsAction => "del_marker_delete_all_versions",
            Self::ActionCount => "action_count",
        }
    }

    pub fn from_index(i: usize) -> Option<Self> {
        match i {
            0 => Some(Self::NoneAction),
            1 => Some(Self::DeleteAction),
            2 => Some(Self::DeleteVersionAction),
            3 => Some(Self::TransitionAction),
            4 => Some(Self::TransitionVersionAction),
            5 => Some(Self::DeleteRestoredAction),
            6 => Some(Self::DeleteRestoredVersionAction),
            7 => Some(Self::DeleteAllVersionsAction),
            8 => Some(Self::DelMarkerDeleteAllVersionsAction),
            9 => Some(Self::ActionCount),
            _ => None,
        }
    }
}

impl Display for IlmAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

pub static GLOBAL_METRICS: OnceLock<Arc<Metrics>> = OnceLock::new();

pub fn global_metrics() -> &'static Arc<Metrics> {
    GLOBAL_METRICS.get_or_init(|| Arc::new(Metrics::new()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    // START Realtime metrics, that only records
    // last minute latencies and total operation count.
    ReadMetadata = 0,
    CheckMissing,
    SaveUsage,
    ApplyAll,
    ApplyVersion,
    TierObjSweep,
    HealCheck,
    Ilm,
    CheckReplication,
    Yield,
    ThrottleSleep,
    CleanAbandoned,
    ApplyNonCurrent,
    HealAbandonedVersion,

    // Quota metrics:
    QuotaCheck,
    QuotaViolation,
    QuotaSync,

    // START Trace metrics:
    StartTrace,
    ScanObject, // Scan object. All operations included.
    HealAbandonedObject,

    // END realtime metrics:
    LastRealtime,

    // Trace only metrics:
    ScanFolder,      // Scan a folder on disk, recursively.
    ScanCycle,       // Full cycle, cluster global.
    ScanBucketDrive, // Single bucket on one drive.
    CompactFolder,   // Folder compacted.
    ScanBucketDriveStart,
    ScanBucketDriveFailure,

    // Must be last:
    Last,
}

impl Metric {
    /// Convert to string representation for metrics
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReadMetadata => "read_metadata",
            Self::CheckMissing => "check_missing",
            Self::SaveUsage => "save_usage",
            Self::ApplyAll => "apply_all",
            Self::ApplyVersion => "apply_version",
            Self::TierObjSweep => "tier_obj_sweep",
            Self::HealCheck => "heal_check",
            Self::Ilm => "ilm",
            Self::CheckReplication => "check_replication",
            Self::Yield => "yield",
            Self::ThrottleSleep => "throttle_sleep",
            Self::CleanAbandoned => "clean_abandoned",
            Self::ApplyNonCurrent => "apply_non_current",
            Self::HealAbandonedVersion => "heal_abandoned_version",
            Self::QuotaCheck => "quota_check",
            Self::QuotaViolation => "quota_violation",
            Self::QuotaSync => "quota_sync",
            Self::StartTrace => "start_trace",
            Self::ScanObject => "scan_object",
            Self::HealAbandonedObject => "heal_abandoned_object",
            Self::LastRealtime => "last_realtime",
            Self::ScanFolder => "scan_folder",
            Self::ScanCycle => "scan_cycle",
            Self::ScanBucketDrive => "scan_bucket_drive",
            Self::CompactFolder => "compact_folder",
            Self::ScanBucketDriveStart => "scan_bucket_drive_start",
            Self::ScanBucketDriveFailure => "scan_bucket_drive_failure",
            Self::Last => "last",
        }
    }

    /// Convert from index back to enum (safe version)
    pub fn from_index(index: usize) -> Option<Self> {
        if index >= Self::Last as usize {
            return None;
        }
        match index {
            0 => Some(Self::ReadMetadata),
            1 => Some(Self::CheckMissing),
            2 => Some(Self::SaveUsage),
            3 => Some(Self::ApplyAll),
            4 => Some(Self::ApplyVersion),
            5 => Some(Self::TierObjSweep),
            6 => Some(Self::HealCheck),
            7 => Some(Self::Ilm),
            8 => Some(Self::CheckReplication),
            9 => Some(Self::Yield),
            10 => Some(Self::ThrottleSleep),
            11 => Some(Self::CleanAbandoned),
            12 => Some(Self::ApplyNonCurrent),
            13 => Some(Self::HealAbandonedVersion),
            14 => Some(Self::QuotaCheck),
            15 => Some(Self::QuotaViolation),
            16 => Some(Self::QuotaSync),
            17 => Some(Self::StartTrace),
            18 => Some(Self::ScanObject),
            19 => Some(Self::HealAbandonedObject),
            20 => Some(Self::LastRealtime),
            21 => Some(Self::ScanFolder),
            22 => Some(Self::ScanCycle),
            23 => Some(Self::ScanBucketDrive),
            24 => Some(Self::CompactFolder),
            25 => Some(Self::ScanBucketDriveStart),
            26 => Some(Self::ScanBucketDriveFailure),
            27 => Some(Self::Last),
            _ => None,
        }
    }
}

const SCANNER_CHECKPOINT_EVENT_SET: &str = "set";
const SCANNER_CHECKPOINT_EVENT_USED: &str = "used";
const SCANNER_CHECKPOINT_EVENT_IGNORED: &str = "ignored";
const SCANNER_CHECKPOINT_EVENT_STALE: &str = "stale";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScannerWorkSource {
    Usage,
    Lifecycle,
    BucketReplication,
    SiteReplication,
    Heal,
    Bitrot,
    Alerts,
}

impl ScannerWorkSource {
    const ALL: [Self; 7] = [
        Self::Usage,
        Self::Lifecycle,
        Self::BucketReplication,
        Self::SiteReplication,
        Self::Heal,
        Self::Bitrot,
        Self::Alerts,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Usage => "usage",
            Self::Lifecycle => "lifecycle",
            Self::BucketReplication => "bucket_replication",
            Self::SiteReplication => "site_replication",
            Self::Heal => "heal",
            Self::Bitrot => "bitrot",
            Self::Alerts => "alerts",
        }
    }

    fn code(self) -> u8 {
        match self {
            Self::Usage => 1,
            Self::Lifecycle => 2,
            Self::BucketReplication => 3,
            Self::SiteReplication => 4,
            Self::Heal => 5,
            Self::Bitrot => 6,
            Self::Alerts => 7,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Usage),
            2 => Some(Self::Lifecycle),
            3 => Some(Self::BucketReplication),
            4 => Some(Self::SiteReplication),
            5 => Some(Self::Heal),
            6 => Some(Self::Bitrot),
            7 => Some(Self::Alerts),
            _ => None,
        }
    }

    fn all() -> &'static [Self] {
        &Self::ALL
    }

    fn index(self) -> usize {
        match self {
            Self::Usage => 0,
            Self::Lifecycle => 1,
            Self::BucketReplication => 2,
            Self::SiteReplication => 3,
            Self::Heal => 4,
            Self::Bitrot => 5,
            Self::Alerts => 6,
        }
    }
}

#[derive(Debug, Default)]
struct ScannerSourceWorkCounters {
    checked: AtomicU64,
    queued: AtomicU64,
    executed: AtomicU64,
    failed: AtomicU64,
    skipped: AtomicU64,
    missed: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScannerSourceWorkUpdate {
    pub checked: u64,
    pub queued: u64,
    pub executed: u64,
    pub failed: u64,
    pub skipped: u64,
    pub missed: u64,
}

impl ScannerSourceWorkUpdate {
    pub const fn checked(count: u64) -> Self {
        Self {
            checked: count,
            queued: 0,
            executed: 0,
            failed: 0,
            skipped: 0,
            missed: 0,
        }
    }

    pub const fn queued(count: u64) -> Self {
        Self {
            checked: 0,
            queued: count,
            executed: 0,
            failed: 0,
            skipped: 0,
            missed: 0,
        }
    }

    pub const fn executed(count: u64) -> Self {
        Self {
            checked: 0,
            queued: 0,
            executed: count,
            failed: 0,
            skipped: 0,
            missed: 0,
        }
    }

    pub const fn missed(count: u64) -> Self {
        Self {
            checked: 0,
            queued: 0,
            executed: 0,
            failed: 0,
            skipped: 0,
            missed: count,
        }
    }
}

impl ScannerSourceWorkCounters {
    fn add(&self, work: ScannerSourceWorkUpdate) {
        self.checked.fetch_add(work.checked, Ordering::Relaxed);
        self.queued.fetch_add(work.queued, Ordering::Relaxed);
        self.executed.fetch_add(work.executed, Ordering::Relaxed);
        self.failed.fetch_add(work.failed, Ordering::Relaxed);
        self.skipped.fetch_add(work.skipped, Ordering::Relaxed);
        self.missed.fetch_add(work.missed, Ordering::Relaxed);
    }

    fn store(&self, values: ScannerSourceWorkValues) {
        self.checked.store(values.checked, Ordering::Relaxed);
        self.queued.store(values.queued, Ordering::Relaxed);
        self.executed.store(values.executed, Ordering::Relaxed);
        self.failed.store(values.failed, Ordering::Relaxed);
        self.skipped.store(values.skipped, Ordering::Relaxed);
        self.missed.store(values.missed, Ordering::Relaxed);
    }

    fn values(&self) -> ScannerSourceWorkValues {
        ScannerSourceWorkValues {
            checked: self.checked.load(Ordering::Relaxed),
            queued: self.queued.load(Ordering::Relaxed),
            executed: self.executed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            skipped: self.skipped.load(Ordering::Relaxed),
            missed: self.missed.load(Ordering::Relaxed),
        }
    }

    fn snapshot(&self, source: ScannerWorkSource) -> ScannerSourceWorkSnapshot {
        self.values().snapshot(source)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ScannerSourceWorkValues {
    checked: u64,
    queued: u64,
    executed: u64,
    failed: u64,
    skipped: u64,
    missed: u64,
}

impl ScannerSourceWorkValues {
    fn saturating_sub(self, start: Self) -> Self {
        Self {
            checked: self.checked.saturating_sub(start.checked),
            queued: self.queued.saturating_sub(start.queued),
            executed: self.executed.saturating_sub(start.executed),
            failed: self.failed.saturating_sub(start.failed),
            skipped: self.skipped.saturating_sub(start.skipped),
            missed: self.missed.saturating_sub(start.missed),
        }
    }

    fn snapshot(self, source: ScannerWorkSource) -> ScannerSourceWorkSnapshot {
        ScannerSourceWorkSnapshot {
            source: source.as_str().to_string(),
            checked: self.checked,
            queued: self.queued,
            executed: self.executed,
            failed: self.failed,
            skipped: self.skipped,
            missed: self.missed,
        }
    }
}

// ---------------------------------------------------------------------------
// LockedLastMinuteLatency
// ---------------------------------------------------------------------------
//
// Uses std::sync::Mutex instead of tokio::sync::Mutex.
//
// Rationale: the critical section is a handful of integer additions inside
// LastMinuteLatency::add_all / get_total — no I/O, no blocking syscalls, no
// awaiting.  A std blocking mutex is cheaper (no task-wakeup overhead) and,
// crucially, lets every caller be *synchronous*.  That eliminates the need to
// spawn a background task just to record a duration, which was the only reason
// tokio::spawn appeared in log/time/time_size/time_n/time_ilm.
//
// Note: vec![LockedLastMinuteLatency::default(); N] with the old Arc-based
// Clone made every element share the *same* inner mutex — a latent bug where
// all metrics slots wrote to one counter.  The new Clone creates a fresh
// independent Mutex per element, matching the intent.

/// Thread-safe wrapper for LastMinuteLatency backed by a std blocking mutex.
pub struct LockedLastMinuteLatency {
    // Arc so Clone is cheap *and* each cloned value stays independent (its own
    // allocation).  We never hand out the Arc to two Metrics at once; the Arc
    // is purely a convenience for the Clone impl below.
    latency: Arc<Mutex<LastMinuteLatency>>,
}

impl Default for LockedLastMinuteLatency {
    fn default() -> Self {
        Self::new()
    }
}

// Produce a fresh, independent slot — *not* a shared alias.  This is what
// vec![val; N] and #[derive(Clone)] on the parent struct both need.
impl Clone for LockedLastMinuteLatency {
    fn clone(&self) -> Self {
        let inner = match self.latency.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        Self {
            latency: Arc::new(Mutex::new(inner)),
        }
    }
}

impl LockedLastMinuteLatency {
    pub fn new() -> Self {
        Self {
            latency: Arc::new(Mutex::new(LastMinuteLatency::default())),
        }
    }

    /// Record a duration sample (no size).
    pub fn add(&self, duration: Duration) {
        self.add_size(duration, 0);
    }

    /// Record a duration sample with an associated byte count.
    pub fn add_size(&self, duration: Duration, size: u64) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let elem = AccElem {
            n: 1,
            total: duration.as_secs(),
            size,
        };

        match self.latency.lock() {
            Ok(mut guard) => guard.add_all(now, &elem),
            Err(poisoned) => poisoned.into_inner().add_all(now, &elem),
        }
    }

    /// Return accumulated totals for the last minute window.
    pub fn total(&self) -> AccElem {
        match self.latency.lock() {
            Ok(mut latency) => latency.get_total(),
            Err(poisoned) => poisoned.into_inner().get_total(),
        }
    }
}

// ---------------------------------------------------------------------------
// CurrentPathTracker — unchanged, still uses tokio::sync::RwLock because it
// lives inside async path-update callbacks.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct CurrentPathState {
    path: String,
    updated_at: DateTime<Utc>,
}

struct CurrentPathTracker {
    state: Arc<RwLock<CurrentPathState>>,
}

impl CurrentPathTracker {
    fn new(initial_path: String) -> Self {
        Self::new_at(initial_path, Utc::now())
    }

    fn new_at(initial_path: String, updated_at: DateTime<Utc>) -> Self {
        Self {
            state: Arc::new(RwLock::new(CurrentPathState {
                path: initial_path,
                updated_at,
            })),
        }
    }

    async fn update_path(&self, path: String) {
        let mut state = self.state.write().await;
        state.path = path;
        state.updated_at = Utc::now();
    }

    async fn get_state(&self) -> CurrentPathState {
        self.state.read().await.clone()
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ScannerDiskBucketScanState {
    concurrency_limit: u64,
    queued: u64,
    active: u64,
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

pub struct Metrics {
    operations: Vec<AtomicU64>,
    latency: Vec<LockedLastMinuteLatency>,
    actions: Vec<AtomicU64>,
    actions_latency: Vec<LockedLastMinuteLatency>,
    current_paths: Arc<RwLock<HashMap<String, Arc<CurrentPathTracker>>>>,
    cycle_info: Arc<RwLock<Option<CurrentCycle>>>,
    current_scan_mode: AtomicU8,
    current_scan_cycle_work_active: AtomicBool,
    current_scan_cycle_objects_start: AtomicU64,
    current_scan_cycle_directories_start: AtomicU64,
    current_scan_cycle_bucket_drive_scans_start: AtomicU64,
    current_scan_cycle_bucket_drive_failures_start: AtomicU64,
    current_scan_cycle_yield_events_start: AtomicU64,
    current_scan_cycle_yield_duration_millis_start: AtomicU64,
    current_scan_cycle_throttle_sleep_events_start: AtomicU64,
    current_scan_cycle_throttle_sleep_duration_millis_start: AtomicU64,
    current_scan_cycle_ilm_actions_start: AtomicU64,
    current_scan_cycle_lifecycle_expiry_actions_start: AtomicU64,
    current_scan_cycle_lifecycle_transition_actions_start: AtomicU64,
    current_scan_cycle_heal_objects_start: AtomicU64,
    current_scan_cycle_replication_checks_start: AtomicU64,
    current_scan_cycle_usage_saves_start: AtomicU64,
    scanner_set_scan_concurrency_limit: AtomicU64,
    scanner_set_scans_queued: AtomicU64,
    scanner_set_scans_active: AtomicU64,
    scanner_disk_bucket_scan_states: Mutex<HashMap<String, ScannerDiskBucketScanState>>,
    last_scan_cycle_result: AtomicU8,
    last_scan_cycle_partial_reason: AtomicU8,
    last_scan_cycle_partial_source: AtomicU8,
    last_scan_cycle_duration_millis: AtomicU64,
    last_scan_cycle_objects_scanned: AtomicU64,
    last_scan_cycle_directories_scanned: AtomicU64,
    last_scan_cycle_bucket_drive_scans: AtomicU64,
    last_scan_cycle_bucket_drive_failures: AtomicU64,
    last_scan_cycle_yield_events: AtomicU64,
    last_scan_cycle_yield_duration_millis: AtomicU64,
    last_scan_cycle_throttle_sleep_events: AtomicU64,
    last_scan_cycle_throttle_sleep_duration_millis: AtomicU64,
    last_scan_cycle_ilm_actions: AtomicU64,
    last_scan_cycle_lifecycle_expiry_actions: AtomicU64,
    last_scan_cycle_lifecycle_transition_actions: AtomicU64,
    last_scan_cycle_heal_objects: AtomicU64,
    last_scan_cycle_replication_checks: AtomicU64,
    last_scan_cycle_usage_saves: AtomicU64,
    failed_scan_cycles: AtomicU64,
    partial_scan_cycles_unknown: AtomicU64,
    partial_scan_cycles_runtime: AtomicU64,
    partial_scan_cycles_objects: AtomicU64,
    partial_scan_cycles_directories: AtomicU64,
    partial_scan_cycles_by_source: Vec<AtomicU64>,
    scanner_yield_duration_millis: AtomicU64,
    scanner_throttle_sleep_duration_millis: AtomicU64,
    scanner_ilm_actions: AtomicU64,
    scanner_lifecycle_expiry_actions: AtomicU64,
    scanner_lifecycle_transition_actions: AtomicU64,
    scanner_transition_queue_capacity: AtomicU64,
    scanner_transition_queued: AtomicU64,
    scanner_transition_active: AtomicU64,
    scanner_transition_workers: AtomicU64,
    scanner_transition_queue_full: AtomicU64,
    scanner_transition_queue_send_timeout: AtomicU64,
    scanner_transition_compensation_scheduled: AtomicU64,
    scanner_transition_compensation_running: AtomicU64,
    scanner_transition_queued_total: AtomicU64,
    scanner_transition_missed_total: AtomicU64,
    scanner_transition_completed: AtomicU64,
    scanner_transition_failed: AtomicU64,
    scanner_throttle_idle_mode_enabled: AtomicBool,
    scanner_throttle_sleep_factor_micros: AtomicU64,
    scanner_throttle_max_sleep_millis: AtomicU64,
    scanner_yield_every_n_objects: AtomicU64,
    scanner_cycle_interval_millis: AtomicU64,
    scanner_cycle_max_duration_millis: AtomicU64,
    scanner_cycle_max_objects: AtomicU64,
    scanner_cycle_max_directories: AtomicU64,
    scanner_bitrot_cycle_enabled: AtomicBool,
    scanner_bitrot_cycle_millis: AtomicU64,
    scanner_checkpoint: Mutex<Option<ScannerCheckpointReport>>,
    scanner_checkpoint_used: AtomicU64,
    scanner_checkpoint_cleared: AtomicU64,
    scanner_checkpoint_ignored: AtomicU64,
    scanner_checkpoint_stale: AtomicU64,
    scanner_source_work: Vec<ScannerSourceWorkCounters>,
    current_scan_cycle_source_work_start: Vec<ScannerSourceWorkCounters>,
    last_scan_cycle_source_work: Vec<ScannerSourceWorkCounters>,
    partial_scan_cycles: AtomicU64,
}

const SCAN_CYCLE_RESULT_UNKNOWN: u8 = 0;
const SCAN_CYCLE_RESULT_SUCCESS: u8 = 1;
const SCAN_CYCLE_RESULT_ERROR: u8 = 2;
const SCAN_CYCLE_RESULT_PARTIAL: u8 = 3;
const SCAN_CYCLE_RESULT_UNKNOWN_LABEL: &str = "unknown";
const SCAN_CYCLE_RESULT_SUCCESS_LABEL: &str = "success";
const SCAN_CYCLE_RESULT_ERROR_LABEL: &str = "error";
const SCAN_CYCLE_RESULT_PARTIAL_LABEL: &str = "partial";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ScanCyclePartialReason {
    #[default]
    Unknown = 0,
    Runtime = 1,
    Objects = 2,
    Directories = 3,
}

impl ScanCyclePartialReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => SCAN_CYCLE_RESULT_UNKNOWN_LABEL,
            Self::Runtime => "runtime",
            Self::Objects => "objects",
            Self::Directories => "directories",
        }
    }

    fn from_code(code: u8) -> Self {
        match code {
            1 => Self::Runtime,
            2 => Self::Objects,
            3 => Self::Directories,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CurrentCycle {
    pub current: u64,
    pub next: u64,
    pub cycle_completed: Vec<DateTime<Utc>>,
    pub started: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScanCycleWorkSnapshot {
    objects_scanned: u64,
    directories_scanned: u64,
    bucket_drive_scans: u64,
    bucket_drive_failures: u64,
    yield_events: u64,
    yield_duration_millis: u64,
    throttle_sleep_events: u64,
    throttle_sleep_duration_millis: u64,
    ilm_actions: u64,
    lifecycle_expiry_actions: u64,
    lifecycle_transition_actions: u64,
    heal_objects: u64,
    replication_checks: u64,
    usage_saves: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScannerTimedAction {
    pub count: u64,
    pub acc_time: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScannerCheckpointReport {
    pub version: u16,
    pub resume_after: String,
    pub reason: String,
    pub last_event: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScannerSourceWorkSnapshot {
    pub source: String,
    pub checked: u64,
    pub queued: u64,
    pub executed: u64,
    pub failed: u64,
    pub skipped: u64,
    #[serde(default)]
    pub missed: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScannerMaintenanceSourceSnapshot {
    pub source: String,
    pub state: String,
    pub reason: String,
    pub backlog: u64,
    pub current_checked: u64,
    pub current_queued: u64,
    pub current_missed: u64,
    pub lifetime_missed: u64,
    pub partial_cycles: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScannerMaintenanceControlSnapshot {
    pub primary_control: String,
    pub sources: Vec<ScannerMaintenanceSourceSnapshot>,
}

impl Default for ScannerMaintenanceControlSnapshot {
    fn default() -> Self {
        Self {
            primary_control: SCANNER_MAINTENANCE_CONTROL_NONE.to_string(),
            sources: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScannerSourceCycleSnapshot {
    pub source: String,
    pub cycles: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ScannerPacingPressureSnapshot {
    pub primary_pressure: String,
    pub current_queued_scans: u64,
    pub current_active_scans: u64,
    pub last_cycle_budget_limited: bool,
    pub last_cycle_pause_observed: bool,
    pub last_cycle_throttle_sleep_ratio: f64,
    pub last_cycle_yield_ratio: f64,
    pub last_cycle_total_pause_ratio: f64,
}

impl Default for ScannerPacingPressureSnapshot {
    fn default() -> Self {
        Self {
            primary_pressure: "none".to_string(),
            current_queued_scans: 0,
            current_active_scans: 0,
            last_cycle_budget_limited: false,
            last_cycle_pause_observed: false,
            last_cycle_throttle_sleep_ratio: 0.0,
            last_cycle_yield_ratio: 0.0,
            last_cycle_total_pause_ratio: 0.0,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScannerLifecycleTransitionStateUpdate {
    pub queue_capacity: u64,
    pub queued: u64,
    pub active: u64,
    pub workers: u64,
    pub queue_full: u64,
    pub queue_send_timeout: u64,
    pub compensation_scheduled: u64,
    pub compensation_running: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScannerLifecycleTransitionSnapshot {
    pub current_queue_capacity: u64,
    pub current_queued: u64,
    pub current_active: u64,
    pub current_workers: u64,
    pub queue_full: u64,
    pub queue_send_timeout: u64,
    pub compensation_scheduled: u64,
    pub compensation_running: u64,
    pub scanner_queued: u64,
    pub scanner_missed: u64,
    pub completed: u64,
    pub failed: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScannerLastMinute {
    pub actions: HashMap<String, ScannerTimedAction>,
    pub ilm: HashMap<String, ScannerTimedAction>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScannerMetricsReport {
    pub collected_at: DateTime<Utc>,
    pub current_cycle: u64,
    pub current_started: DateTime<Utc>,
    pub cycles_completed_at: Vec<DateTime<Utc>>,
    pub ongoing_buckets: usize,
    #[serde(default)]
    pub active_scan_paths: usize,
    #[serde(default)]
    pub oldest_active_path_age_seconds: u64,
    pub life_time_ops: HashMap<String, u64>,
    pub life_time_ilm: HashMap<String, u64>,
    pub last_minute: ScannerLastMinute,
    pub active_paths: Vec<String>,
    pub current_scan_mode: String,
    #[serde(default)]
    pub current_set_scan_concurrency_limit: u64,
    #[serde(default)]
    pub current_set_scans_queued: u64,
    #[serde(default)]
    pub current_set_scans_active: u64,
    #[serde(default)]
    pub current_disk_scan_concurrency_limit: u64,
    #[serde(default)]
    pub current_disk_bucket_scans_queued: u64,
    #[serde(default)]
    pub current_disk_bucket_scans_active: u64,
    #[serde(default)]
    pub current_cycle_objects_scanned: u64,
    #[serde(default)]
    pub current_cycle_directories_scanned: u64,
    #[serde(default)]
    pub current_cycle_bucket_drive_scans: u64,
    #[serde(default)]
    pub current_cycle_bucket_drive_failures: u64,
    #[serde(default)]
    pub current_cycle_yield_events: u64,
    #[serde(default)]
    pub current_cycle_yield_duration_seconds: f64,
    #[serde(default)]
    pub current_cycle_throttle_sleep_events: u64,
    #[serde(default)]
    pub current_cycle_throttle_sleep_duration_seconds: f64,
    #[serde(default)]
    pub current_cycle_ilm_actions: u64,
    #[serde(default)]
    pub current_cycle_lifecycle_expiry_actions: u64,
    #[serde(default)]
    pub current_cycle_lifecycle_transition_actions: u64,
    #[serde(default)]
    pub current_cycle_heal_objects: u64,
    #[serde(default)]
    pub current_cycle_replication_checks: u64,
    #[serde(default)]
    pub current_cycle_usage_saves: u64,
    pub last_cycle_result: String,
    pub last_cycle_result_code: u64,
    #[serde(default)]
    pub last_cycle_partial_reason: String,
    #[serde(default)]
    pub last_cycle_partial_reason_code: u64,
    #[serde(default)]
    pub last_cycle_partial_source: String,
    #[serde(default)]
    pub last_cycle_partial_source_code: u64,
    pub last_cycle_duration_seconds: f64,
    #[serde(default)]
    pub last_cycle_objects_scanned: u64,
    #[serde(default)]
    pub last_cycle_directories_scanned: u64,
    #[serde(default)]
    pub last_cycle_bucket_drive_scans: u64,
    #[serde(default)]
    pub last_cycle_bucket_drive_failures: u64,
    #[serde(default)]
    pub last_cycle_yield_events: u64,
    #[serde(default)]
    pub last_cycle_yield_duration_seconds: f64,
    #[serde(default)]
    pub last_cycle_throttle_sleep_events: u64,
    #[serde(default)]
    pub last_cycle_throttle_sleep_duration_seconds: f64,
    #[serde(default)]
    pub last_cycle_ilm_actions: u64,
    #[serde(default)]
    pub last_cycle_lifecycle_expiry_actions: u64,
    #[serde(default)]
    pub last_cycle_lifecycle_transition_actions: u64,
    #[serde(default)]
    pub last_cycle_heal_objects: u64,
    #[serde(default)]
    pub last_cycle_replication_checks: u64,
    #[serde(default)]
    pub last_cycle_usage_saves: u64,
    pub failed_cycles: u64,
    #[serde(default)]
    pub partial_cycles_unknown: u64,
    #[serde(default)]
    pub partial_cycles_runtime: u64,
    #[serde(default)]
    pub partial_cycles_objects: u64,
    #[serde(default)]
    pub partial_cycles_directories: u64,
    #[serde(default)]
    pub partial_cycles_by_source: Vec<ScannerSourceCycleSnapshot>,
    #[serde(default)]
    pub pacing_pressure: ScannerPacingPressureSnapshot,
    #[serde(default)]
    pub lifecycle_transition: ScannerLifecycleTransitionSnapshot,
    #[serde(default)]
    pub maintenance_control: ScannerMaintenanceControlSnapshot,
    #[serde(default)]
    pub throttle_idle_mode_enabled: bool,
    #[serde(default)]
    pub throttle_sleep_factor: f64,
    #[serde(default)]
    pub throttle_max_sleep_seconds: f64,
    #[serde(default)]
    pub yield_every_n_objects: u64,
    #[serde(default)]
    pub cycle_interval_seconds: f64,
    #[serde(default)]
    pub cycle_max_duration_seconds: f64,
    #[serde(default)]
    pub cycle_max_objects: u64,
    #[serde(default)]
    pub cycle_max_directories: u64,
    #[serde(default)]
    pub bitrot_cycle_enabled: bool,
    #[serde(default)]
    pub bitrot_cycle_seconds: f64,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scan_checkpoint: Option<ScannerCheckpointReport>,
    #[serde(default)]
    pub scan_checkpoint_used: u64,
    #[serde(default)]
    pub scan_checkpoint_cleared: u64,
    #[serde(default)]
    pub scan_checkpoint_ignored: u64,
    #[serde(default)]
    pub scan_checkpoint_stale: u64,
    #[serde(default)]
    pub source_work: Vec<ScannerSourceWorkSnapshot>,
    #[serde(default)]
    pub current_cycle_source_work: Vec<ScannerSourceWorkSnapshot>,
    #[serde(default)]
    pub last_cycle_source_work: Vec<ScannerSourceWorkSnapshot>,
    #[serde(default)]
    pub partial_cycles: u64,
}

impl CurrentCycle {
    pub fn unmarshal(&mut self, buf: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        *self = rmp_serde::from_slice(buf)?;
        Ok(())
    }

    pub fn marshal(&self) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(rmp_serde::to_vec(self)?)
    }
}

/// OTEL metric name constants for scanner metrics
const OTEL_SCANNER_OBJECTS_SCANNED: &str = "rustfs_scanner_objects_scanned_total";
const OTEL_SCANNER_DIRECTORIES_SCANNED: &str = "rustfs_scanner_directories_scanned_total";
const OTEL_SCANNER_BUCKETS_SCANNED: &str = "rustfs_scanner_buckets_scanned_total";
const OTEL_SCANNER_CYCLES: &str = "rustfs_scanner_cycles_total";
const OTEL_SCANNER_CYCLE_DURATION_SECONDS: &str = "rustfs_scanner_cycle_duration_seconds";
const OTEL_SCANNER_BUCKET_DRIVE_DURATION_SECONDS: &str = "rustfs_scanner_bucket_drive_duration_seconds";

fn emit_otel_counter(metric: usize, count: u64) {
    match Metric::from_index(metric) {
        Some(Metric::ScanObject) => {
            metrics::counter!(OTEL_SCANNER_OBJECTS_SCANNED).increment(count);
        }
        Some(Metric::ScanFolder) => {
            metrics::counter!(OTEL_SCANNER_DIRECTORIES_SCANNED).increment(count);
        }
        _ => {}
    }
}

fn scan_cycle_result_label(result: u8) -> &'static str {
    match result {
        SCAN_CYCLE_RESULT_SUCCESS => SCAN_CYCLE_RESULT_SUCCESS_LABEL,
        SCAN_CYCLE_RESULT_ERROR => SCAN_CYCLE_RESULT_ERROR_LABEL,
        SCAN_CYCLE_RESULT_PARTIAL => SCAN_CYCLE_RESULT_PARTIAL_LABEL,
        _ => SCAN_CYCLE_RESULT_UNKNOWN_LABEL,
    }
}

fn scanner_ratio(numerator: f64, denominator: f64) -> f64 {
    if denominator <= 0.0 {
        0.0
    } else {
        (numerator / denominator).clamp(0.0, 1.0)
    }
}

fn scanner_last_cycle_budget_limited(result_code: u64, partial_reason: &str) -> bool {
    result_code == u64::from(SCAN_CYCLE_RESULT_PARTIAL) && matches!(partial_reason, "runtime" | "objects" | "directories")
}

fn scanner_primary_pressure(
    current_queued_scans: u64,
    current_active_scans: u64,
    last_cycle_budget_limited: bool,
    last_cycle_throttle_sleep_events: u64,
    last_cycle_yield_events: u64,
) -> &'static str {
    if current_queued_scans > 0 {
        "queued_scans"
    } else if last_cycle_budget_limited {
        "cycle_budget"
    } else if last_cycle_throttle_sleep_events > 0 || last_cycle_yield_events > 0 {
        "throttle_pause"
    } else if current_active_scans > 0 {
        "active_scans"
    } else {
        "none"
    }
}

fn scanner_pacing_pressure(metrics: &ScannerMetricsReport) -> ScannerPacingPressureSnapshot {
    let current_queued_scans = metrics
        .current_set_scans_queued
        .saturating_add(metrics.current_disk_bucket_scans_queued);
    let current_active_scans = metrics
        .current_set_scans_active
        .saturating_add(metrics.current_disk_bucket_scans_active)
        .max(usize_to_u64_saturated(metrics.active_scan_paths));
    let last_cycle_budget_limited =
        scanner_last_cycle_budget_limited(metrics.last_cycle_result_code, metrics.last_cycle_partial_reason.as_str());
    let last_cycle_pause_observed = metrics.last_cycle_throttle_sleep_events > 0 || metrics.last_cycle_yield_events > 0;
    let last_cycle_throttle_sleep_ratio =
        scanner_ratio(metrics.last_cycle_throttle_sleep_duration_seconds, metrics.last_cycle_duration_seconds);
    let last_cycle_yield_ratio = scanner_ratio(metrics.last_cycle_yield_duration_seconds, metrics.last_cycle_duration_seconds);
    let last_cycle_total_pause_ratio = scanner_ratio(
        metrics.last_cycle_throttle_sleep_duration_seconds.max(0.0) + metrics.last_cycle_yield_duration_seconds.max(0.0),
        metrics.last_cycle_duration_seconds,
    );

    ScannerPacingPressureSnapshot {
        primary_pressure: scanner_primary_pressure(
            current_queued_scans,
            current_active_scans,
            last_cycle_budget_limited,
            metrics.last_cycle_throttle_sleep_events,
            metrics.last_cycle_yield_events,
        )
        .to_string(),
        current_queued_scans,
        current_active_scans,
        last_cycle_budget_limited,
        last_cycle_pause_observed,
        last_cycle_throttle_sleep_ratio,
        last_cycle_yield_ratio,
        last_cycle_total_pause_ratio,
    }
}

const SCANNER_MAINTENANCE_CONTROL_NONE: &str = "none";
const SCANNER_MAINTENANCE_CONTROL_BLOCKED_SOURCE: &str = "blocked_source";
const SCANNER_MAINTENANCE_CONTROL_DEFERRED_SOURCE: &str = "deferred_source";
const SCANNER_MAINTENANCE_CONTROL_ACTIVE_SOURCE: &str = "active_source";
const SCANNER_MAINTENANCE_CONTROL_PACING_PRESSURE: &str = "pacing_pressure";

const SCANNER_MAINTENANCE_STATE_IDLE: &str = "idle";
const SCANNER_MAINTENANCE_STATE_ACTIVE: &str = "active";
const SCANNER_MAINTENANCE_STATE_DEFERRED: &str = "deferred";
const SCANNER_MAINTENANCE_STATE_BLOCKED: &str = "blocked";

const SCANNER_MAINTENANCE_REASON_IDLE: &str = "idle";
const SCANNER_MAINTENANCE_REASON_ACTIVE_WORK: &str = "active_work";
const SCANNER_MAINTENANCE_REASON_QUEUED_WORK: &str = "queued_work";
const SCANNER_MAINTENANCE_REASON_PARTIAL_CYCLE: &str = "partial_cycle";
const SCANNER_MAINTENANCE_REASON_MISSED_WORK: &str = "missed_work";
const SCANNER_MAINTENANCE_REASON_TRANSITION_QUEUE_BACKLOG: &str = "transition_queue_backlog";
const SCANNER_MAINTENANCE_REASON_TRANSITION_QUEUE_FULL: &str = "transition_queue_full";

fn scanner_source_work_snapshot(work: &[ScannerSourceWorkSnapshot], source: ScannerWorkSource) -> ScannerSourceWorkSnapshot {
    work.iter()
        .find(|snapshot| snapshot.source == source.as_str())
        .cloned()
        .unwrap_or_else(|| ScannerSourceWorkSnapshot {
            source: source.as_str().to_string(),
            ..Default::default()
        })
}

fn scanner_source_partial_cycles(metrics: &ScannerMetricsReport, source: ScannerWorkSource) -> u64 {
    metrics
        .partial_cycles_by_source
        .iter()
        .find(|snapshot| snapshot.source == source.as_str())
        .map(|snapshot| snapshot.cycles)
        .unwrap_or_default()
}

fn scanner_source_has_current_work(work: &ScannerSourceWorkSnapshot) -> bool {
    work.checked > 0 || work.queued > 0 || work.executed > 0 || work.failed > 0 || work.skipped > 0 || work.missed > 0
}

fn scanner_lifecycle_transition_backlog(metrics: &ScannerMetricsReport) -> u64 {
    metrics
        .lifecycle_transition
        .current_queued
        .saturating_add(metrics.lifecycle_transition.current_active)
}

fn scanner_source_maintenance_state(
    source: ScannerWorkSource,
    current: &ScannerSourceWorkSnapshot,
    metrics: &ScannerMetricsReport,
) -> (&'static str, &'static str, u64) {
    if current.missed > 0 {
        return (SCANNER_MAINTENANCE_STATE_BLOCKED, SCANNER_MAINTENANCE_REASON_MISSED_WORK, current.missed);
    }

    if source == ScannerWorkSource::Lifecycle {
        let transition_backlog = scanner_lifecycle_transition_backlog(metrics);
        if metrics.lifecycle_transition.current_queue_capacity > 0
            && metrics.lifecycle_transition.current_queued >= metrics.lifecycle_transition.current_queue_capacity
        {
            return (
                SCANNER_MAINTENANCE_STATE_BLOCKED,
                SCANNER_MAINTENANCE_REASON_TRANSITION_QUEUE_FULL,
                transition_backlog,
            );
        }
    }

    if metrics.last_cycle_partial_source == source.as_str() && metrics.pacing_pressure.last_cycle_budget_limited {
        return (SCANNER_MAINTENANCE_STATE_DEFERRED, SCANNER_MAINTENANCE_REASON_PARTIAL_CYCLE, 0);
    }

    if current.queued > 0 {
        return (SCANNER_MAINTENANCE_STATE_ACTIVE, SCANNER_MAINTENANCE_REASON_QUEUED_WORK, current.queued);
    }

    if source == ScannerWorkSource::Lifecycle {
        let transition_backlog = scanner_lifecycle_transition_backlog(metrics);
        if transition_backlog > 0 {
            return (
                SCANNER_MAINTENANCE_STATE_ACTIVE,
                SCANNER_MAINTENANCE_REASON_TRANSITION_QUEUE_BACKLOG,
                transition_backlog,
            );
        }
    }

    if scanner_source_has_current_work(current) {
        return (SCANNER_MAINTENANCE_STATE_ACTIVE, SCANNER_MAINTENANCE_REASON_ACTIVE_WORK, 0);
    }

    (SCANNER_MAINTENANCE_STATE_IDLE, SCANNER_MAINTENANCE_REASON_IDLE, 0)
}

fn scanner_maintenance_source_work(metrics: &ScannerMetricsReport) -> &[ScannerSourceWorkSnapshot] {
    if metrics.current_cycle_source_work.is_empty() {
        &metrics.last_cycle_source_work
    } else {
        &metrics.current_cycle_source_work
    }
}

fn scanner_maintenance_control(metrics: &ScannerMetricsReport) -> ScannerMaintenanceControlSnapshot {
    let mut has_blocked = false;
    let mut has_deferred = false;
    let mut has_active = false;
    let current_source_work = scanner_maintenance_source_work(metrics);

    let sources = ScannerWorkSource::all()
        .iter()
        .map(|source| {
            let current = scanner_source_work_snapshot(current_source_work, *source);
            let lifetime = scanner_source_work_snapshot(&metrics.source_work, *source);
            let partial_cycles = scanner_source_partial_cycles(metrics, *source);
            let (state, reason, backlog) = scanner_source_maintenance_state(*source, &current, metrics);

            match state {
                SCANNER_MAINTENANCE_STATE_BLOCKED => has_blocked = true,
                SCANNER_MAINTENANCE_STATE_DEFERRED => has_deferred = true,
                SCANNER_MAINTENANCE_STATE_ACTIVE => has_active = true,
                _ => {}
            }

            ScannerMaintenanceSourceSnapshot {
                source: source.as_str().to_string(),
                state: state.to_string(),
                reason: reason.to_string(),
                backlog,
                current_checked: current.checked,
                current_queued: current.queued,
                current_missed: current.missed,
                lifetime_missed: lifetime.missed,
                partial_cycles,
            }
        })
        .collect();

    let primary_control = if has_blocked {
        SCANNER_MAINTENANCE_CONTROL_BLOCKED_SOURCE
    } else if has_deferred {
        SCANNER_MAINTENANCE_CONTROL_DEFERRED_SOURCE
    } else if has_active {
        SCANNER_MAINTENANCE_CONTROL_ACTIVE_SOURCE
    } else if metrics.pacing_pressure.primary_pressure != SCANNER_MAINTENANCE_CONTROL_NONE {
        SCANNER_MAINTENANCE_CONTROL_PACING_PRESSURE
    } else {
        SCANNER_MAINTENANCE_CONTROL_NONE
    };

    ScannerMaintenanceControlSnapshot {
        primary_control: primary_control.to_string(),
        sources,
    }
}

fn duration_millis_saturated(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

fn usize_to_u64_saturated(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn scaled_f64_to_u64_saturated(value: f64, scale: f64) -> u64 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }

    (value * scale).round().clamp(0.0, u64::MAX as f64) as u64
}

pub fn emit_scan_cycle_complete(success: bool, duration: Duration) {
    let result = if success {
        SCAN_CYCLE_RESULT_SUCCESS_LABEL
    } else {
        SCAN_CYCLE_RESULT_ERROR_LABEL
    };
    global_metrics().record_scan_cycle_complete(success, duration);
    metrics::counter!(OTEL_SCANNER_CYCLES, "result" => result).increment(1);
    if success {
        metrics::gauge!(OTEL_SCANNER_CYCLE_DURATION_SECONDS).set(duration.as_secs_f64());
    }
}

pub fn emit_scan_cycle_partial(duration: Duration, reason: ScanCyclePartialReason) {
    global_metrics().record_scan_cycle_partial(duration, reason);
    metrics::counter!(OTEL_SCANNER_CYCLES, "result" => SCAN_CYCLE_RESULT_PARTIAL_LABEL).increment(1);
}

pub fn emit_scan_cycle_partial_with_source(
    duration: Duration,
    reason: ScanCyclePartialReason,
    source: Option<ScannerWorkSource>,
) {
    global_metrics().record_scan_cycle_partial_with_source(duration, reason, source);
    metrics::counter!(OTEL_SCANNER_CYCLES, "result" => SCAN_CYCLE_RESULT_PARTIAL_LABEL).increment(1);
}

pub fn emit_scan_bucket_drive_complete(success: bool, bucket: &str, disk: &str, duration: Duration) {
    let result = if success { "success" } else { "error" };
    metrics::counter!(
        OTEL_SCANNER_BUCKETS_SCANNED,
        "result" => result,
        "bucket" => bucket.to_owned(),
        "disk" => disk.to_owned()
    )
    .increment(1);
    metrics::histogram!(
        OTEL_SCANNER_BUCKET_DRIVE_DURATION_SECONDS,
        "bucket" => bucket.to_owned(),
        "disk" => disk.to_owned()
    )
    .record(duration.as_secs_f64());
}

pub fn emit_scan_bucket_drive_partial(bucket: &str, disk: &str, duration: Duration) {
    metrics::counter!(
        OTEL_SCANNER_BUCKETS_SCANNED,
        "result" => SCAN_CYCLE_RESULT_PARTIAL_LABEL,
        "bucket" => bucket.to_owned(),
        "disk" => disk.to_owned()
    )
    .increment(1);
    metrics::histogram!(
        OTEL_SCANNER_BUCKET_DRIVE_DURATION_SECONDS,
        "bucket" => bucket.to_owned(),
        "disk" => disk.to_owned()
    )
    .record(duration.as_secs_f64());
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            operations: (0..Metric::Last as usize).map(|_| AtomicU64::new(0)).collect(),
            // Each slot gets its own fresh LockedLastMinuteLatency so that
            // different metrics never accidentally share state.
            latency: (0..Metric::LastRealtime as usize)
                .map(|_| LockedLastMinuteLatency::new())
                .collect(),
            actions: (0..IlmAction::ActionCount as usize).map(|_| AtomicU64::new(0)).collect(),
            actions_latency: (0..IlmAction::ActionCount as usize)
                .map(|_| LockedLastMinuteLatency::new())
                .collect(),
            current_paths: Arc::new(RwLock::new(HashMap::new())),
            cycle_info: Arc::new(RwLock::new(None)),
            current_scan_mode: AtomicU8::new(HealScanMode::Unknown as u8),
            current_scan_cycle_work_active: AtomicBool::new(false),
            current_scan_cycle_objects_start: AtomicU64::new(0),
            current_scan_cycle_directories_start: AtomicU64::new(0),
            current_scan_cycle_bucket_drive_scans_start: AtomicU64::new(0),
            current_scan_cycle_bucket_drive_failures_start: AtomicU64::new(0),
            current_scan_cycle_yield_events_start: AtomicU64::new(0),
            current_scan_cycle_yield_duration_millis_start: AtomicU64::new(0),
            current_scan_cycle_throttle_sleep_events_start: AtomicU64::new(0),
            current_scan_cycle_throttle_sleep_duration_millis_start: AtomicU64::new(0),
            current_scan_cycle_ilm_actions_start: AtomicU64::new(0),
            current_scan_cycle_lifecycle_expiry_actions_start: AtomicU64::new(0),
            current_scan_cycle_lifecycle_transition_actions_start: AtomicU64::new(0),
            current_scan_cycle_heal_objects_start: AtomicU64::new(0),
            current_scan_cycle_replication_checks_start: AtomicU64::new(0),
            current_scan_cycle_usage_saves_start: AtomicU64::new(0),
            scanner_set_scan_concurrency_limit: AtomicU64::new(0),
            scanner_set_scans_queued: AtomicU64::new(0),
            scanner_set_scans_active: AtomicU64::new(0),
            scanner_disk_bucket_scan_states: Mutex::new(HashMap::new()),
            last_scan_cycle_result: AtomicU8::new(SCAN_CYCLE_RESULT_UNKNOWN),
            last_scan_cycle_partial_reason: AtomicU8::new(ScanCyclePartialReason::Unknown as u8),
            last_scan_cycle_partial_source: AtomicU8::new(0),
            last_scan_cycle_duration_millis: AtomicU64::new(0),
            last_scan_cycle_objects_scanned: AtomicU64::new(0),
            last_scan_cycle_directories_scanned: AtomicU64::new(0),
            last_scan_cycle_bucket_drive_scans: AtomicU64::new(0),
            last_scan_cycle_bucket_drive_failures: AtomicU64::new(0),
            last_scan_cycle_yield_events: AtomicU64::new(0),
            last_scan_cycle_yield_duration_millis: AtomicU64::new(0),
            last_scan_cycle_throttle_sleep_events: AtomicU64::new(0),
            last_scan_cycle_throttle_sleep_duration_millis: AtomicU64::new(0),
            last_scan_cycle_ilm_actions: AtomicU64::new(0),
            last_scan_cycle_lifecycle_expiry_actions: AtomicU64::new(0),
            last_scan_cycle_lifecycle_transition_actions: AtomicU64::new(0),
            last_scan_cycle_heal_objects: AtomicU64::new(0),
            last_scan_cycle_replication_checks: AtomicU64::new(0),
            last_scan_cycle_usage_saves: AtomicU64::new(0),
            failed_scan_cycles: AtomicU64::new(0),
            partial_scan_cycles_unknown: AtomicU64::new(0),
            partial_scan_cycles_runtime: AtomicU64::new(0),
            partial_scan_cycles_objects: AtomicU64::new(0),
            partial_scan_cycles_directories: AtomicU64::new(0),
            partial_scan_cycles_by_source: ScannerWorkSource::all().iter().map(|_| AtomicU64::new(0)).collect(),
            scanner_yield_duration_millis: AtomicU64::new(0),
            scanner_throttle_sleep_duration_millis: AtomicU64::new(0),
            scanner_ilm_actions: AtomicU64::new(0),
            scanner_lifecycle_expiry_actions: AtomicU64::new(0),
            scanner_lifecycle_transition_actions: AtomicU64::new(0),
            scanner_transition_queue_capacity: AtomicU64::new(0),
            scanner_transition_queued: AtomicU64::new(0),
            scanner_transition_active: AtomicU64::new(0),
            scanner_transition_workers: AtomicU64::new(0),
            scanner_transition_queue_full: AtomicU64::new(0),
            scanner_transition_queue_send_timeout: AtomicU64::new(0),
            scanner_transition_compensation_scheduled: AtomicU64::new(0),
            scanner_transition_compensation_running: AtomicU64::new(0),
            scanner_transition_queued_total: AtomicU64::new(0),
            scanner_transition_missed_total: AtomicU64::new(0),
            scanner_transition_completed: AtomicU64::new(0),
            scanner_transition_failed: AtomicU64::new(0),
            scanner_throttle_idle_mode_enabled: AtomicBool::new(false),
            scanner_throttle_sleep_factor_micros: AtomicU64::new(0),
            scanner_throttle_max_sleep_millis: AtomicU64::new(0),
            scanner_yield_every_n_objects: AtomicU64::new(0),
            scanner_cycle_interval_millis: AtomicU64::new(0),
            scanner_cycle_max_duration_millis: AtomicU64::new(0),
            scanner_cycle_max_objects: AtomicU64::new(0),
            scanner_cycle_max_directories: AtomicU64::new(0),
            scanner_bitrot_cycle_enabled: AtomicBool::new(false),
            scanner_bitrot_cycle_millis: AtomicU64::new(0),
            scanner_checkpoint: Mutex::new(None),
            scanner_checkpoint_used: AtomicU64::new(0),
            scanner_checkpoint_cleared: AtomicU64::new(0),
            scanner_checkpoint_ignored: AtomicU64::new(0),
            scanner_checkpoint_stale: AtomicU64::new(0),
            scanner_source_work: ScannerWorkSource::all()
                .iter()
                .map(|_| ScannerSourceWorkCounters::default())
                .collect(),
            current_scan_cycle_source_work_start: ScannerWorkSource::all()
                .iter()
                .map(|_| ScannerSourceWorkCounters::default())
                .collect(),
            last_scan_cycle_source_work: ScannerWorkSource::all()
                .iter()
                .map(|_| ScannerSourceWorkCounters::default())
                .collect(),
            partial_scan_cycles: AtomicU64::new(0),
        }
    }

    // -----------------------------------------------------------------------
    // Metric recording helpers
    //
    // All of these are now pure sync closures.  No tokio::spawn, no heap-
    // allocated future, no scheduler overhead — just an atomic increment and a
    // std-mutex lock for fewer than ~10 ns of work.
    // -----------------------------------------------------------------------

    /// Return a closure that records one observation of `metric` (with
    /// optional caller-supplied metadata).  Call it once the operation ends.
    pub fn log(metric: Metric) -> impl Fn(&HashMap<String, String>) {
        let metric_idx = metric as usize;
        let start = SystemTime::now();
        move |_custom: &HashMap<String, String>| {
            let duration = SystemTime::now().duration_since(start).unwrap_or_default();
            global_metrics().operations[metric_idx].fetch_add(1, Ordering::Relaxed);
            global_metrics().record_source_work_for_metric(metric, 1);
            emit_otel_counter(metric_idx, 1);
            if metric_idx < Metric::LastRealtime as usize {
                global_metrics().latency[metric_idx].add(duration);
            }
        }
    }

    /// Return a closure that records one observation of `metric` together with
    /// a byte count.  Call `done(size_bytes)` when the operation ends.
    pub fn time_size(metric: Metric) -> impl Fn(u64) {
        let metric_idx = metric as usize;
        let start = SystemTime::now();
        move |size: u64| {
            let duration = SystemTime::now().duration_since(start).unwrap_or_default();
            global_metrics().operations[metric_idx].fetch_add(1, Ordering::Relaxed);
            global_metrics().record_source_work_for_metric(metric, 1);
            emit_otel_counter(metric_idx, 1);
            if metric_idx < Metric::LastRealtime as usize {
                global_metrics().latency[metric_idx].add_size(duration, size);
            }
        }
    }

    /// Return a closure that records one observation of `metric`.
    /// Call `done()` when the operation ends.
    pub fn time(metric: Metric) -> impl Fn() {
        let metric_idx = metric as usize;
        let start = SystemTime::now();
        move || {
            let duration = SystemTime::now().duration_since(start).unwrap_or_default();
            global_metrics().operations[metric_idx].fetch_add(1, Ordering::Relaxed);
            global_metrics().record_source_work_for_metric(metric, 1);
            emit_otel_counter(metric_idx, 1);
            if metric_idx < Metric::LastRealtime as usize {
                global_metrics().latency[metric_idx].add(duration);
            }
        }
    }

    /// Return a two-stage closure: first call takes an item count, second call
    /// (returned closure) fires when the batch of `count` operations ends.
    pub fn time_n(metric: Metric) -> Box<dyn Fn(usize) -> Box<dyn Fn() + Send + Sync> + Send + Sync> {
        let metric_idx = metric as usize;
        let start = SystemTime::now();
        Box::new(move |count: usize| {
            Box::new(move || {
                let duration = SystemTime::now().duration_since(start).unwrap_or_default();
                let count = usize_to_u64_saturated(count);
                global_metrics().operations[metric_idx].fetch_add(count, Ordering::Relaxed);
                global_metrics().record_source_work_for_metric(metric, count);
                emit_otel_counter(metric_idx, count);
                if metric_idx < Metric::LastRealtime as usize {
                    global_metrics().latency[metric_idx].add(duration);
                }
            })
        })
    }

    /// Return a two-stage closure for ILM actions: first call takes a version
    /// count, second call fires when the action completes.
    pub fn time_ilm(a: IlmAction) -> Box<dyn Fn(u64) -> Box<dyn Fn() + Send + Sync> + Send + Sync> {
        let a_idx = a as usize;
        if a_idx == IlmAction::NoneAction as usize || a_idx >= IlmAction::ActionCount as usize {
            return Box::new(|_| Box::new(|| {}));
        }
        let start = SystemTime::now();
        Box::new(move |versions: u64| {
            Box::new(move || {
                let duration = SystemTime::now().duration_since(start).unwrap_or_default();
                let metric_idx = Metric::Ilm as usize;
                global_metrics().operations[metric_idx].fetch_add(versions, Ordering::Relaxed);
                emit_otel_counter(metric_idx, versions);
                global_metrics().actions[a_idx].fetch_add(versions, Ordering::Relaxed);
                global_metrics().actions_latency[a_idx].add(duration);
            })
        })
    }

    /// Record a single observation of `metric` with a caller-supplied duration.
    /// No longer async — nothing inside requires it.
    pub fn inc_time(metric: Metric, duration: Duration) {
        let metric_idx = metric as usize;
        global_metrics().operations[metric_idx].fetch_add(1, Ordering::Relaxed);
        global_metrics().record_source_work_for_metric(metric, 1);
        emit_otel_counter(metric_idx, 1);
        if metric_idx < Metric::LastRealtime as usize {
            global_metrics().latency[metric_idx].add(duration);
        }
    }

    pub fn record_scanner_yield(&self, duration: Duration) {
        let metric_idx = Metric::Yield as usize;
        self.operations[metric_idx].fetch_add(1, Ordering::Relaxed);
        if metric_idx < Metric::LastRealtime as usize {
            self.latency[metric_idx].add(duration);
        }
        let duration_millis = duration_millis_saturated(duration);
        let _ = self
            .scanner_yield_duration_millis
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(duration_millis))
            });
    }

    pub fn record_scanner_throttle_sleep(&self, duration: Duration) {
        let metric_idx = Metric::ThrottleSleep as usize;
        self.operations[metric_idx].fetch_add(1, Ordering::Relaxed);
        if metric_idx < Metric::LastRealtime as usize {
            self.latency[metric_idx].add(duration);
        }
        let duration_millis = duration_millis_saturated(duration);
        let _ = self
            .scanner_throttle_sleep_duration_millis
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(duration_millis))
            });
    }

    pub fn record_scanner_ilm_action(&self, count: u64) {
        self.scanner_ilm_actions.fetch_add(count, Ordering::Relaxed);
        self.record_scanner_source_executed(ScannerWorkSource::Lifecycle, count);
    }

    pub fn record_scanner_lifecycle_action(&self, action: IlmAction, count: u64) {
        self.record_scanner_ilm_action(count);
        match action {
            IlmAction::TransitionAction | IlmAction::TransitionVersionAction => {
                self.scanner_lifecycle_transition_actions.fetch_add(count, Ordering::Relaxed);
            }
            _ if action.delete() => {
                self.scanner_lifecycle_expiry_actions.fetch_add(count, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    pub fn record_scanner_ilm_enqueue_result(&self, count: u64, queued: bool) {
        if queued {
            self.record_scanner_source_queued(ScannerWorkSource::Lifecycle, count);
        } else {
            self.record_scanner_source_missed(ScannerWorkSource::Lifecycle, count);
        }
    }

    pub fn record_scanner_transition_enqueue_result(&self, count: u64, queued: bool) {
        self.record_scanner_ilm_enqueue_result(count, queued);
        if queued {
            self.scanner_transition_queued_total.fetch_add(count, Ordering::Relaxed);
        } else {
            self.scanner_transition_missed_total.fetch_add(count, Ordering::Relaxed);
        }
    }

    pub fn record_scanner_lifecycle_transition_state(&self, state: ScannerLifecycleTransitionStateUpdate) {
        self.scanner_transition_queue_capacity
            .store(state.queue_capacity, Ordering::Relaxed);
        self.scanner_transition_queued.store(state.queued, Ordering::Relaxed);
        self.scanner_transition_active.store(state.active, Ordering::Relaxed);
        self.scanner_transition_workers.store(state.workers, Ordering::Relaxed);
        self.scanner_transition_queue_full.store(state.queue_full, Ordering::Relaxed);
        self.scanner_transition_queue_send_timeout
            .store(state.queue_send_timeout, Ordering::Relaxed);
        self.scanner_transition_compensation_scheduled
            .store(state.compensation_scheduled, Ordering::Relaxed);
        self.scanner_transition_compensation_running
            .store(state.compensation_running, Ordering::Relaxed);
    }

    pub fn record_scanner_transition_completed(&self, count: u64) {
        self.scanner_transition_completed.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_scanner_transition_failed(&self, count: u64) {
        self.scanner_transition_failed.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_scanner_checkpoint_set(&self, version: u16, resume_after: impl Into<String>, reason: impl Into<String>) {
        let checkpoint = ScannerCheckpointReport {
            version,
            resume_after: resume_after.into(),
            reason: reason.into(),
            last_event: SCANNER_CHECKPOINT_EVENT_SET.to_string(),
        };
        match self.scanner_checkpoint.lock() {
            Ok(mut current) => *current = Some(checkpoint),
            Err(poisoned) => *poisoned.into_inner() = Some(checkpoint),
        }
    }

    pub fn record_scanner_checkpoint_used(&self) {
        self.scanner_checkpoint_used.fetch_add(1, Ordering::Relaxed);
        self.update_scanner_checkpoint_event(SCANNER_CHECKPOINT_EVENT_USED);
    }

    pub fn record_scanner_checkpoint_cleared(&self) {
        self.scanner_checkpoint_cleared.fetch_add(1, Ordering::Relaxed);
        match self.scanner_checkpoint.lock() {
            Ok(mut current) => *current = None,
            Err(poisoned) => *poisoned.into_inner() = None,
        }
    }

    pub fn record_scanner_checkpoint_ignored(&self) {
        self.scanner_checkpoint_ignored.fetch_add(1, Ordering::Relaxed);
        self.update_scanner_checkpoint_event(SCANNER_CHECKPOINT_EVENT_IGNORED);
    }

    pub fn record_scanner_checkpoint_stale(&self) {
        self.scanner_checkpoint_stale.fetch_add(1, Ordering::Relaxed);
        self.update_scanner_checkpoint_event(SCANNER_CHECKPOINT_EVENT_STALE);
    }

    pub fn record_scanner_source_work(&self, source: ScannerWorkSource, work: ScannerSourceWorkUpdate) {
        if let Some(counters) = self.scanner_source_work.get(source.index()) {
            counters.add(work);
        }
    }

    pub fn record_scanner_source_checked(&self, source: ScannerWorkSource, count: u64) {
        self.record_scanner_source_work(source, ScannerSourceWorkUpdate::checked(count));
    }

    pub fn record_scanner_source_queued(&self, source: ScannerWorkSource, count: u64) {
        self.record_scanner_source_work(source, ScannerSourceWorkUpdate::queued(count));
    }

    pub fn record_scanner_source_executed(&self, source: ScannerWorkSource, count: u64) {
        self.record_scanner_source_work(source, ScannerSourceWorkUpdate::executed(count));
    }

    pub fn record_scanner_source_missed(&self, source: ScannerWorkSource, count: u64) {
        self.record_scanner_source_work(source, ScannerSourceWorkUpdate::missed(count));
    }

    fn update_scanner_checkpoint_event(&self, event: &str) {
        match self.scanner_checkpoint.lock() {
            Ok(mut current) => {
                if let Some(checkpoint) = current.as_mut() {
                    checkpoint.last_event = event.to_string();
                }
            }
            Err(poisoned) => {
                let mut current = poisoned.into_inner();
                if let Some(checkpoint) = current.as_mut() {
                    checkpoint.last_event = event.to_string();
                }
            }
        }
    }

    fn record_source_work_for_metric(&self, metric: Metric, count: u64) {
        match metric {
            Metric::ScanObject | Metric::ScanFolder => {
                self.record_scanner_source_checked(ScannerWorkSource::Usage, count);
            }
            Metric::SaveUsage => {
                self.record_scanner_source_executed(ScannerWorkSource::Usage, count);
            }
            Metric::CheckReplication => {
                self.record_scanner_source_checked(ScannerWorkSource::BucketReplication, count);
            }
            Metric::HealCheck => {
                self.record_scanner_source_checked(ScannerWorkSource::Heal, count);
            }
            _ => {}
        }
    }

    pub fn record_scan_bucket_drive_start(&self) {
        self.operations[Metric::ScanBucketDriveStart as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_scan_bucket_drive_failure(&self) {
        self.operations[Metric::ScanBucketDriveFailure as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_scanner_throttle_config(
        &self,
        idle_mode_enabled: bool,
        sleep_factor: f64,
        max_sleep: Duration,
        yield_every_n_objects: u64,
    ) {
        self.scanner_throttle_idle_mode_enabled
            .store(idle_mode_enabled, Ordering::Relaxed);
        self.scanner_throttle_sleep_factor_micros
            .store(scaled_f64_to_u64_saturated(sleep_factor, 1_000_000.0), Ordering::Relaxed);
        self.scanner_throttle_max_sleep_millis
            .store(duration_millis_saturated(max_sleep), Ordering::Relaxed);
        self.scanner_yield_every_n_objects
            .store(yield_every_n_objects, Ordering::Relaxed);
    }

    pub fn record_scanner_cycle_config(
        &self,
        cycle_interval: Duration,
        bitrot_cycle: Option<Duration>,
        cycle_max_duration: Option<Duration>,
        cycle_max_objects: Option<u64>,
        cycle_max_directories: Option<u64>,
    ) {
        self.scanner_cycle_interval_millis
            .store(duration_millis_saturated(cycle_interval), Ordering::Relaxed);
        self.scanner_cycle_max_duration_millis
            .store(cycle_max_duration.map(duration_millis_saturated).unwrap_or_default(), Ordering::Relaxed);
        self.scanner_cycle_max_objects
            .store(cycle_max_objects.unwrap_or_default(), Ordering::Relaxed);
        self.scanner_cycle_max_directories
            .store(cycle_max_directories.unwrap_or_default(), Ordering::Relaxed);
        self.scanner_bitrot_cycle_enabled
            .store(bitrot_cycle.is_some(), Ordering::Relaxed);
        self.scanner_bitrot_cycle_millis
            .store(bitrot_cycle.map(duration_millis_saturated).unwrap_or_default(), Ordering::Relaxed);
    }

    pub fn record_scanner_set_scan_state(&self, concurrency_limit: Option<usize>, queued: Option<usize>, active: Option<usize>) {
        if let Some(concurrency_limit) = concurrency_limit {
            self.scanner_set_scan_concurrency_limit
                .store(concurrency_limit as u64, Ordering::Relaxed);
        }
        if let Some(queued) = queued {
            self.scanner_set_scans_queued.store(queued as u64, Ordering::Relaxed);
        }
        if let Some(active) = active {
            self.scanner_set_scans_active.store(active as u64, Ordering::Relaxed);
        }
    }

    pub fn reset_scanner_set_scan_state(&self) {
        self.record_scanner_set_scan_state(Some(0), Some(0), Some(0));
        match self.scanner_disk_bucket_scan_states.lock() {
            Ok(mut states) => states.clear(),
            Err(poisoned) => poisoned.into_inner().clear(),
        }
    }

    pub fn record_scanner_disk_bucket_scan_state(
        &self,
        pool: &str,
        set: &str,
        concurrency_limit: Option<usize>,
        queued: Option<usize>,
        active: Option<usize>,
    ) {
        let key = format!("{pool}/{set}");
        let mut states = match self.scanner_disk_bucket_scan_states.lock() {
            Ok(states) => states,
            Err(poisoned) => poisoned.into_inner(),
        };
        let state = states.entry(key).or_default();
        if let Some(concurrency_limit) = concurrency_limit {
            state.concurrency_limit = concurrency_limit as u64;
        }
        if let Some(queued) = queued {
            state.queued = queued as u64;
        }
        if let Some(active) = active {
            state.active = active as u64;
        }
    }

    // -----------------------------------------------------------------------
    // Read-side helpers
    // -----------------------------------------------------------------------

    /// Lifetime operation count for `metric`.
    pub fn lifetime(&self, metric: Metric) -> u64 {
        let idx = metric as usize;
        if idx >= Metric::Last as usize {
            return 0;
        }
        self.operations[idx].load(Ordering::Relaxed)
    }

    /// Last-minute accumulated stats for a realtime metric.
    /// No longer async — LockedLastMinuteLatency::total() is now synchronous.
    pub fn last_minute(&self, metric: Metric) -> AccElem {
        let idx = metric as usize;
        if idx >= Metric::LastRealtime as usize {
            return AccElem::default();
        }
        self.latency[idx].total()
    }

    /// Replace the current cycle record.
    pub async fn set_cycle(&self, cycle: Option<CurrentCycle>) {
        *self.cycle_info.write().await = cycle;
    }

    /// Read the current cycle record.
    pub async fn get_cycle(&self) -> Option<CurrentCycle> {
        self.cycle_info.read().await.clone()
    }

    pub fn set_current_scan_mode(&self, scan_mode: HealScanMode) {
        self.current_scan_mode.store(scan_mode as u8, Ordering::Relaxed);
    }

    pub fn clear_current_scan_mode(&self) {
        self.set_current_scan_mode(HealScanMode::Unknown);
    }

    pub fn current_scan_mode(&self) -> HealScanMode {
        HealScanMode::from_u8(self.current_scan_mode.load(Ordering::Relaxed)).unwrap_or(HealScanMode::Unknown)
    }

    pub fn record_scan_cycle_complete(&self, success: bool, duration: Duration) {
        let result = if success {
            SCAN_CYCLE_RESULT_SUCCESS
        } else {
            self.failed_scan_cycles.fetch_add(1, Ordering::Relaxed);
            SCAN_CYCLE_RESULT_ERROR
        };
        self.last_scan_cycle_result.store(result, Ordering::Relaxed);
        self.last_scan_cycle_partial_reason
            .store(ScanCyclePartialReason::Unknown as u8, Ordering::Relaxed);
        self.last_scan_cycle_partial_source.store(0, Ordering::Relaxed);
        self.last_scan_cycle_duration_millis
            .store(duration_millis_saturated(duration), Ordering::Relaxed);
    }

    pub fn record_scan_cycle_partial(&self, duration: Duration, reason: ScanCyclePartialReason) {
        self.record_scan_cycle_partial_with_source(duration, reason, None);
    }

    pub fn record_scan_cycle_partial_with_source(
        &self,
        duration: Duration,
        reason: ScanCyclePartialReason,
        source: Option<ScannerWorkSource>,
    ) {
        self.partial_scan_cycles.fetch_add(1, Ordering::Relaxed);
        match reason {
            ScanCyclePartialReason::Unknown => &self.partial_scan_cycles_unknown,
            ScanCyclePartialReason::Runtime => &self.partial_scan_cycles_runtime,
            ScanCyclePartialReason::Objects => &self.partial_scan_cycles_objects,
            ScanCyclePartialReason::Directories => &self.partial_scan_cycles_directories,
        }
        .fetch_add(1, Ordering::Relaxed);
        self.last_scan_cycle_result
            .store(SCAN_CYCLE_RESULT_PARTIAL, Ordering::Relaxed);
        self.last_scan_cycle_partial_reason.store(reason as u8, Ordering::Relaxed);
        self.last_scan_cycle_partial_source
            .store(source.map(ScannerWorkSource::code).unwrap_or_default(), Ordering::Relaxed);
        if let Some(source) = source
            && let Some(cycles) = self.partial_scan_cycles_by_source.get(source.index())
        {
            cycles.fetch_add(1, Ordering::Relaxed);
        }
        self.last_scan_cycle_duration_millis
            .store(duration_millis_saturated(duration), Ordering::Relaxed);
    }

    pub fn start_scan_cycle_work(&self) -> ScanCycleWorkSnapshot {
        let snapshot = self.scan_cycle_work_snapshot();
        let source_snapshot = self.scanner_source_work_values();
        self.current_scan_cycle_objects_start
            .store(snapshot.objects_scanned, Ordering::Relaxed);
        self.current_scan_cycle_directories_start
            .store(snapshot.directories_scanned, Ordering::Relaxed);
        self.current_scan_cycle_bucket_drive_scans_start
            .store(snapshot.bucket_drive_scans, Ordering::Relaxed);
        self.current_scan_cycle_bucket_drive_failures_start
            .store(snapshot.bucket_drive_failures, Ordering::Relaxed);
        self.current_scan_cycle_yield_events_start
            .store(snapshot.yield_events, Ordering::Relaxed);
        self.current_scan_cycle_yield_duration_millis_start
            .store(snapshot.yield_duration_millis, Ordering::Relaxed);
        self.current_scan_cycle_throttle_sleep_events_start
            .store(snapshot.throttle_sleep_events, Ordering::Relaxed);
        self.current_scan_cycle_throttle_sleep_duration_millis_start
            .store(snapshot.throttle_sleep_duration_millis, Ordering::Relaxed);
        self.current_scan_cycle_ilm_actions_start
            .store(snapshot.ilm_actions, Ordering::Relaxed);
        self.current_scan_cycle_lifecycle_expiry_actions_start
            .store(snapshot.lifecycle_expiry_actions, Ordering::Relaxed);
        self.current_scan_cycle_lifecycle_transition_actions_start
            .store(snapshot.lifecycle_transition_actions, Ordering::Relaxed);
        self.current_scan_cycle_heal_objects_start
            .store(snapshot.heal_objects, Ordering::Relaxed);
        self.current_scan_cycle_replication_checks_start
            .store(snapshot.replication_checks, Ordering::Relaxed);
        self.current_scan_cycle_usage_saves_start
            .store(snapshot.usage_saves, Ordering::Relaxed);
        self.store_scanner_source_work_values(&self.current_scan_cycle_source_work_start, &source_snapshot);
        self.current_scan_cycle_work_active.store(true, Ordering::Relaxed);
        snapshot
    }

    pub fn finish_scan_cycle_work(&self, start: ScanCycleWorkSnapshot) {
        let work = self.scan_cycle_work_since(start);
        let source_work = self.scanner_source_work_since(&self.current_scan_cycle_source_work_start_values());
        self.record_scan_cycle_work(work);
        self.record_scan_cycle_source_work(&source_work);
        self.current_scan_cycle_work_active.store(false, Ordering::Relaxed);
    }

    fn scan_cycle_work_snapshot(&self) -> ScanCycleWorkSnapshot {
        ScanCycleWorkSnapshot {
            objects_scanned: self.lifetime(Metric::ScanObject),
            directories_scanned: self.lifetime(Metric::ScanFolder),
            bucket_drive_scans: self.lifetime(Metric::ScanBucketDrive),
            bucket_drive_failures: self.lifetime(Metric::ScanBucketDriveFailure),
            yield_events: self.lifetime(Metric::Yield),
            yield_duration_millis: self.scanner_yield_duration_millis.load(Ordering::Relaxed),
            throttle_sleep_events: self.lifetime(Metric::ThrottleSleep),
            throttle_sleep_duration_millis: self.scanner_throttle_sleep_duration_millis.load(Ordering::Relaxed),
            ilm_actions: self.scanner_ilm_actions.load(Ordering::Relaxed),
            lifecycle_expiry_actions: self.scanner_lifecycle_expiry_actions.load(Ordering::Relaxed),
            lifecycle_transition_actions: self.scanner_lifecycle_transition_actions.load(Ordering::Relaxed),
            heal_objects: self.lifetime(Metric::HealAbandonedObject),
            replication_checks: self.lifetime(Metric::CheckReplication),
            usage_saves: self.lifetime(Metric::SaveUsage),
        }
    }

    fn current_scan_cycle_work_start(&self) -> ScanCycleWorkSnapshot {
        ScanCycleWorkSnapshot {
            objects_scanned: self.current_scan_cycle_objects_start.load(Ordering::Relaxed),
            directories_scanned: self.current_scan_cycle_directories_start.load(Ordering::Relaxed),
            bucket_drive_scans: self.current_scan_cycle_bucket_drive_scans_start.load(Ordering::Relaxed),
            bucket_drive_failures: self.current_scan_cycle_bucket_drive_failures_start.load(Ordering::Relaxed),
            yield_events: self.current_scan_cycle_yield_events_start.load(Ordering::Relaxed),
            yield_duration_millis: self.current_scan_cycle_yield_duration_millis_start.load(Ordering::Relaxed),
            throttle_sleep_events: self.current_scan_cycle_throttle_sleep_events_start.load(Ordering::Relaxed),
            throttle_sleep_duration_millis: self
                .current_scan_cycle_throttle_sleep_duration_millis_start
                .load(Ordering::Relaxed),
            ilm_actions: self.current_scan_cycle_ilm_actions_start.load(Ordering::Relaxed),
            lifecycle_expiry_actions: self.current_scan_cycle_lifecycle_expiry_actions_start.load(Ordering::Relaxed),
            lifecycle_transition_actions: self
                .current_scan_cycle_lifecycle_transition_actions_start
                .load(Ordering::Relaxed),
            heal_objects: self.current_scan_cycle_heal_objects_start.load(Ordering::Relaxed),
            replication_checks: self.current_scan_cycle_replication_checks_start.load(Ordering::Relaxed),
            usage_saves: self.current_scan_cycle_usage_saves_start.load(Ordering::Relaxed),
        }
    }

    fn scan_cycle_work_since(&self, start: ScanCycleWorkSnapshot) -> ScanCycleWorkSnapshot {
        let current = self.scan_cycle_work_snapshot();
        ScanCycleWorkSnapshot {
            objects_scanned: current.objects_scanned.saturating_sub(start.objects_scanned),
            directories_scanned: current.directories_scanned.saturating_sub(start.directories_scanned),
            bucket_drive_scans: current.bucket_drive_scans.saturating_sub(start.bucket_drive_scans),
            bucket_drive_failures: current.bucket_drive_failures.saturating_sub(start.bucket_drive_failures),
            yield_events: current.yield_events.saturating_sub(start.yield_events),
            yield_duration_millis: current.yield_duration_millis.saturating_sub(start.yield_duration_millis),
            throttle_sleep_events: current.throttle_sleep_events.saturating_sub(start.throttle_sleep_events),
            throttle_sleep_duration_millis: current
                .throttle_sleep_duration_millis
                .saturating_sub(start.throttle_sleep_duration_millis),
            ilm_actions: current.ilm_actions.saturating_sub(start.ilm_actions),
            lifecycle_expiry_actions: current
                .lifecycle_expiry_actions
                .saturating_sub(start.lifecycle_expiry_actions),
            lifecycle_transition_actions: current
                .lifecycle_transition_actions
                .saturating_sub(start.lifecycle_transition_actions),
            heal_objects: current.heal_objects.saturating_sub(start.heal_objects),
            replication_checks: current.replication_checks.saturating_sub(start.replication_checks),
            usage_saves: current.usage_saves.saturating_sub(start.usage_saves),
        }
    }

    fn scanner_source_work_values(&self) -> Vec<ScannerSourceWorkValues> {
        ScannerWorkSource::all()
            .iter()
            .filter_map(|source| {
                self.scanner_source_work
                    .get(source.index())
                    .map(ScannerSourceWorkCounters::values)
            })
            .collect()
    }

    fn current_scan_cycle_source_work_start_values(&self) -> Vec<ScannerSourceWorkValues> {
        ScannerWorkSource::all()
            .iter()
            .filter_map(|source| {
                self.current_scan_cycle_source_work_start
                    .get(source.index())
                    .map(ScannerSourceWorkCounters::values)
            })
            .collect()
    }

    fn scanner_source_work_since(&self, start: &[ScannerSourceWorkValues]) -> Vec<ScannerSourceWorkValues> {
        self.scanner_source_work_values()
            .into_iter()
            .enumerate()
            .map(|(index, current)| current.saturating_sub(start.get(index).copied().unwrap_or_default()))
            .collect()
    }

    fn scanner_source_work_snapshots(&self, values: &[ScannerSourceWorkValues]) -> Vec<ScannerSourceWorkSnapshot> {
        ScannerWorkSource::all()
            .iter()
            .filter_map(|source| values.get(source.index()).map(|values| values.snapshot(*source)))
            .collect()
    }

    fn scanner_source_work_counter_snapshots(&self, counters: &[ScannerSourceWorkCounters]) -> Vec<ScannerSourceWorkSnapshot> {
        ScannerWorkSource::all()
            .iter()
            .filter_map(|source| counters.get(source.index()).map(|counters| counters.snapshot(*source)))
            .collect()
    }

    fn store_scanner_source_work_values(&self, counters: &[ScannerSourceWorkCounters], values: &[ScannerSourceWorkValues]) {
        for source in ScannerWorkSource::all() {
            if let (Some(counter), Some(values)) = (counters.get(source.index()), values.get(source.index())) {
                counter.store(*values);
            }
        }
    }

    pub fn record_scan_cycle_work(&self, work: ScanCycleWorkSnapshot) {
        // Telemetry-only gauges: readers may observe a transient mixed snapshot
        // while these independent atomic fields are updated.
        self.last_scan_cycle_objects_scanned
            .store(work.objects_scanned, Ordering::Relaxed);
        self.last_scan_cycle_directories_scanned
            .store(work.directories_scanned, Ordering::Relaxed);
        self.last_scan_cycle_bucket_drive_scans
            .store(work.bucket_drive_scans, Ordering::Relaxed);
        self.last_scan_cycle_bucket_drive_failures
            .store(work.bucket_drive_failures, Ordering::Relaxed);
        self.last_scan_cycle_yield_events.store(work.yield_events, Ordering::Relaxed);
        self.last_scan_cycle_yield_duration_millis
            .store(work.yield_duration_millis, Ordering::Relaxed);
        self.last_scan_cycle_throttle_sleep_events
            .store(work.throttle_sleep_events, Ordering::Relaxed);
        self.last_scan_cycle_throttle_sleep_duration_millis
            .store(work.throttle_sleep_duration_millis, Ordering::Relaxed);
        self.last_scan_cycle_ilm_actions.store(work.ilm_actions, Ordering::Relaxed);
        self.last_scan_cycle_lifecycle_expiry_actions
            .store(work.lifecycle_expiry_actions, Ordering::Relaxed);
        self.last_scan_cycle_lifecycle_transition_actions
            .store(work.lifecycle_transition_actions, Ordering::Relaxed);
        self.last_scan_cycle_heal_objects.store(work.heal_objects, Ordering::Relaxed);
        self.last_scan_cycle_replication_checks
            .store(work.replication_checks, Ordering::Relaxed);
        self.last_scan_cycle_usage_saves.store(work.usage_saves, Ordering::Relaxed);
    }

    fn record_scan_cycle_source_work(&self, work: &[ScannerSourceWorkValues]) {
        self.store_scanner_source_work_values(&self.last_scan_cycle_source_work, work);
    }

    /// Snapshot of every path currently being scanned.
    pub async fn get_current_paths(&self) -> Vec<String> {
        self.current_path_snapshots()
            .await
            .into_iter()
            .map(|(disk, state)| format!("{disk}/{}", state.path))
            .collect()
    }

    async fn current_path_snapshots(&self) -> Vec<(String, CurrentPathState)> {
        let paths = self.current_paths.read().await;
        let mut result = Vec::with_capacity(paths.len());
        for (disk, tracker) in paths.iter() {
            result.push((disk.clone(), tracker.get_state().await));
        }
        result.sort_by(|(left, _), (right, _)| left.cmp(right));
        result
    }

    /// Number of drives with an active scan in progress.
    pub async fn active_drives(&self) -> usize {
        self.current_paths.read().await.len()
    }

    /// Build a full metrics report snapshot.
    pub async fn report(&self) -> ScannerMetricsReport {
        let mut m = ScannerMetricsReport::default();

        let has_cycle = if let Some(cycle) = self.get_cycle().await {
            m.current_cycle = cycle.current;
            m.cycles_completed_at = cycle.cycle_completed;
            m.current_started = cycle.started;
            true
        } else {
            false
        };

        if !has_cycle && let Some(init_time) = crate::get_global_init_time().await {
            m.current_started = init_time;
        }

        m.collected_at = Utc::now();
        let current_path_snapshots = self.current_path_snapshots().await;
        m.active_scan_paths = current_path_snapshots.len();
        m.oldest_active_path_age_seconds = current_path_snapshots
            .iter()
            .map(|(_, state)| m.collected_at.signed_duration_since(state.updated_at).num_seconds().max(0) as u64)
            .max()
            .unwrap_or_default();
        m.active_paths = current_path_snapshots
            .into_iter()
            .map(|(disk, state)| format!("{disk}/{}", state.path))
            .collect();
        m.current_scan_mode = self.current_scan_mode().as_str().to_string();
        m.current_set_scan_concurrency_limit = self.scanner_set_scan_concurrency_limit.load(Ordering::Relaxed);
        m.current_set_scans_queued = self.scanner_set_scans_queued.load(Ordering::Relaxed);
        m.current_set_scans_active = self.scanner_set_scans_active.load(Ordering::Relaxed);
        let (disk_scan_concurrency_limit, disk_bucket_scans_queued, disk_bucket_scans_active) =
            match self.scanner_disk_bucket_scan_states.lock() {
                Ok(states) => states.values().fold((0, 0, 0), |acc, state| {
                    (acc.0 + state.concurrency_limit, acc.1 + state.queued, acc.2 + state.active)
                }),
                Err(poisoned) => poisoned.into_inner().values().fold((0, 0, 0), |acc, state| {
                    (acc.0 + state.concurrency_limit, acc.1 + state.queued, acc.2 + state.active)
                }),
            };
        m.current_disk_scan_concurrency_limit = disk_scan_concurrency_limit;
        m.current_disk_bucket_scans_queued = disk_bucket_scans_queued;
        m.current_disk_bucket_scans_active = disk_bucket_scans_active;
        if self.current_scan_cycle_work_active.load(Ordering::Relaxed) {
            let current_work = self.scan_cycle_work_since(self.current_scan_cycle_work_start());
            let current_source_work = self.scanner_source_work_since(&self.current_scan_cycle_source_work_start_values());
            m.current_cycle_objects_scanned = current_work.objects_scanned;
            m.current_cycle_directories_scanned = current_work.directories_scanned;
            m.current_cycle_bucket_drive_scans = current_work.bucket_drive_scans;
            m.current_cycle_bucket_drive_failures = current_work.bucket_drive_failures;
            m.current_cycle_yield_events = current_work.yield_events;
            m.current_cycle_yield_duration_seconds = current_work.yield_duration_millis as f64 / 1000.0;
            m.current_cycle_throttle_sleep_events = current_work.throttle_sleep_events;
            m.current_cycle_throttle_sleep_duration_seconds = current_work.throttle_sleep_duration_millis as f64 / 1000.0;
            m.current_cycle_ilm_actions = current_work.ilm_actions;
            m.current_cycle_lifecycle_expiry_actions = current_work.lifecycle_expiry_actions;
            m.current_cycle_lifecycle_transition_actions = current_work.lifecycle_transition_actions;
            m.current_cycle_heal_objects = current_work.heal_objects;
            m.current_cycle_replication_checks = current_work.replication_checks;
            m.current_cycle_usage_saves = current_work.usage_saves;
            m.current_cycle_source_work = self.scanner_source_work_snapshots(&current_source_work);
        }
        let last_cycle_result = self.last_scan_cycle_result.load(Ordering::Relaxed);
        m.last_cycle_result = scan_cycle_result_label(last_cycle_result).to_string();
        m.last_cycle_result_code = last_cycle_result as u64;
        let partial_reason = ScanCyclePartialReason::from_code(self.last_scan_cycle_partial_reason.load(Ordering::Relaxed));
        m.last_cycle_partial_reason = partial_reason.as_str().to_string();
        m.last_cycle_partial_reason_code = partial_reason as u8 as u64;
        let partial_source = ScannerWorkSource::from_code(self.last_scan_cycle_partial_source.load(Ordering::Relaxed));
        m.last_cycle_partial_source = partial_source
            .map(ScannerWorkSource::as_str)
            .unwrap_or(SCAN_CYCLE_RESULT_UNKNOWN_LABEL)
            .to_string();
        m.last_cycle_partial_source_code = partial_source.map(|source| source.code().into()).unwrap_or_default();
        m.last_cycle_duration_seconds = self.last_scan_cycle_duration_millis.load(Ordering::Relaxed) as f64 / 1000.0;
        m.last_cycle_objects_scanned = self.last_scan_cycle_objects_scanned.load(Ordering::Relaxed);
        m.last_cycle_directories_scanned = self.last_scan_cycle_directories_scanned.load(Ordering::Relaxed);
        m.last_cycle_bucket_drive_scans = self.last_scan_cycle_bucket_drive_scans.load(Ordering::Relaxed);
        m.last_cycle_bucket_drive_failures = self.last_scan_cycle_bucket_drive_failures.load(Ordering::Relaxed);
        m.last_cycle_yield_events = self.last_scan_cycle_yield_events.load(Ordering::Relaxed);
        m.last_cycle_yield_duration_seconds = self.last_scan_cycle_yield_duration_millis.load(Ordering::Relaxed) as f64 / 1000.0;
        m.last_cycle_throttle_sleep_events = self.last_scan_cycle_throttle_sleep_events.load(Ordering::Relaxed);
        m.last_cycle_throttle_sleep_duration_seconds =
            self.last_scan_cycle_throttle_sleep_duration_millis.load(Ordering::Relaxed) as f64 / 1000.0;
        m.last_cycle_ilm_actions = self.last_scan_cycle_ilm_actions.load(Ordering::Relaxed);
        m.last_cycle_lifecycle_expiry_actions = self.last_scan_cycle_lifecycle_expiry_actions.load(Ordering::Relaxed);
        m.last_cycle_lifecycle_transition_actions = self.last_scan_cycle_lifecycle_transition_actions.load(Ordering::Relaxed);
        m.last_cycle_heal_objects = self.last_scan_cycle_heal_objects.load(Ordering::Relaxed);
        m.last_cycle_replication_checks = self.last_scan_cycle_replication_checks.load(Ordering::Relaxed);
        m.last_cycle_usage_saves = self.last_scan_cycle_usage_saves.load(Ordering::Relaxed);
        m.last_cycle_source_work = self.scanner_source_work_counter_snapshots(&self.last_scan_cycle_source_work);
        m.failed_cycles = self.failed_scan_cycles.load(Ordering::Relaxed);
        m.partial_cycles_unknown = self.partial_scan_cycles_unknown.load(Ordering::Relaxed);
        m.partial_cycles_runtime = self.partial_scan_cycles_runtime.load(Ordering::Relaxed);
        m.partial_cycles_objects = self.partial_scan_cycles_objects.load(Ordering::Relaxed);
        m.partial_cycles_directories = self.partial_scan_cycles_directories.load(Ordering::Relaxed);
        m.partial_cycles_by_source = ScannerWorkSource::all()
            .iter()
            .filter_map(|source| {
                self.partial_scan_cycles_by_source
                    .get(source.index())
                    .map(|cycles| ScannerSourceCycleSnapshot {
                        source: source.as_str().to_string(),
                        cycles: cycles.load(Ordering::Relaxed),
                    })
            })
            .collect();
        m.lifecycle_transition = ScannerLifecycleTransitionSnapshot {
            current_queue_capacity: self.scanner_transition_queue_capacity.load(Ordering::Relaxed),
            current_queued: self.scanner_transition_queued.load(Ordering::Relaxed),
            current_active: self.scanner_transition_active.load(Ordering::Relaxed),
            current_workers: self.scanner_transition_workers.load(Ordering::Relaxed),
            queue_full: self.scanner_transition_queue_full.load(Ordering::Relaxed),
            queue_send_timeout: self.scanner_transition_queue_send_timeout.load(Ordering::Relaxed),
            compensation_scheduled: self.scanner_transition_compensation_scheduled.load(Ordering::Relaxed),
            compensation_running: self.scanner_transition_compensation_running.load(Ordering::Relaxed),
            scanner_queued: self.scanner_transition_queued_total.load(Ordering::Relaxed),
            scanner_missed: self.scanner_transition_missed_total.load(Ordering::Relaxed),
            completed: self.scanner_transition_completed.load(Ordering::Relaxed),
            failed: self.scanner_transition_failed.load(Ordering::Relaxed),
        };
        m.throttle_idle_mode_enabled = self.scanner_throttle_idle_mode_enabled.load(Ordering::Relaxed);
        m.throttle_sleep_factor = self.scanner_throttle_sleep_factor_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        m.throttle_max_sleep_seconds = self.scanner_throttle_max_sleep_millis.load(Ordering::Relaxed) as f64 / 1000.0;
        m.yield_every_n_objects = self.scanner_yield_every_n_objects.load(Ordering::Relaxed);
        m.cycle_interval_seconds = self.scanner_cycle_interval_millis.load(Ordering::Relaxed) as f64 / 1000.0;
        m.cycle_max_duration_seconds = self.scanner_cycle_max_duration_millis.load(Ordering::Relaxed) as f64 / 1000.0;
        m.cycle_max_objects = self.scanner_cycle_max_objects.load(Ordering::Relaxed);
        m.cycle_max_directories = self.scanner_cycle_max_directories.load(Ordering::Relaxed);
        m.bitrot_cycle_enabled = self.scanner_bitrot_cycle_enabled.load(Ordering::Relaxed);
        m.bitrot_cycle_seconds = self.scanner_bitrot_cycle_millis.load(Ordering::Relaxed) as f64 / 1000.0;
        m.scan_checkpoint = match self.scanner_checkpoint.lock() {
            Ok(checkpoint) => checkpoint.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        m.scan_checkpoint_used = self.scanner_checkpoint_used.load(Ordering::Relaxed);
        m.scan_checkpoint_cleared = self.scanner_checkpoint_cleared.load(Ordering::Relaxed);
        m.scan_checkpoint_ignored = self.scanner_checkpoint_ignored.load(Ordering::Relaxed);
        m.scan_checkpoint_stale = self.scanner_checkpoint_stale.load(Ordering::Relaxed);
        m.source_work = self.scanner_source_work_counter_snapshots(&self.scanner_source_work);
        m.partial_cycles = self.partial_scan_cycles.load(Ordering::Relaxed);

        // Lifetime operation counts
        for i in 0..Metric::Last as usize {
            let count = self.operations[i].load(Ordering::Relaxed);
            if count > 0
                && let Some(metric) = Metric::from_index(i)
            {
                m.life_time_ops.insert(metric.as_str().to_string(), count);
            }
        }

        // Last-minute stats for realtime metrics — now plain sync calls
        for i in 0..Metric::LastRealtime as usize {
            let last_min = self.latency[i].total();
            if last_min.n > 0
                && let Some(metric) = Metric::from_index(i)
            {
                m.last_minute.actions.insert(
                    metric.as_str().to_string(),
                    ScannerTimedAction {
                        count: last_min.n,
                        acc_time: last_min.total,
                        bytes: last_min.size,
                    },
                );
            }
        }

        // Lifetime ILM counts
        for i in 0..IlmAction::ActionCount as usize {
            let count = self.actions[i].load(Ordering::Relaxed);
            if count > 0
                && let Some(action) = IlmAction::from_index(i)
            {
                m.life_time_ilm.insert(action.as_str().to_string(), count);
            }
        }

        // Last-minute ILM latency — plain sync calls
        for i in 0..IlmAction::ActionCount as usize {
            let last_min = self.actions_latency[i].total();
            if last_min.n > 0
                && let Some(action) = IlmAction::from_index(i)
            {
                m.last_minute.ilm.insert(
                    action.as_str().to_string(),
                    ScannerTimedAction {
                        count: last_min.n,
                        acc_time: last_min.total,
                        bytes: last_min.size,
                    },
                );
            }
        }

        m.pacing_pressure = scanner_pacing_pressure(&m);
        m.maintenance_control = scanner_maintenance_control(&m);

        m
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Path tracking helpers
// ---------------------------------------------------------------------------

pub type UpdateCurrentPathFn = Arc<dyn Fn(&str) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;
pub type CloseDiskFn = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// Register a new disk in the global path tracker and return two callbacks:
/// one to update the current path and one to deregister the disk when done.
pub fn current_path_updater(disk: &str, initial: &str) -> (UpdateCurrentPathFn, CloseDiskFn) {
    let tracker = Arc::new(CurrentPathTracker::new(initial.to_string()));
    let disk_name = disk.to_string();

    let tracker_clone = Arc::clone(&tracker);
    let disk_insert = disk_name.clone();
    tokio::spawn(async move {
        global_metrics()
            .current_paths
            .write()
            .await
            .insert(disk_insert, tracker_clone);
    });

    let update_fn: UpdateCurrentPathFn = {
        let tracker = Arc::clone(&tracker);
        Arc::new(move |path: &str| {
            let tracker = Arc::clone(&tracker);
            let path = path.to_string();
            Box::pin(async move { tracker.update_path(path).await })
        })
    };

    let done_fn: CloseDiskFn = {
        let disk = disk_name;
        Arc::new(move || {
            let disk = disk.clone();
            Box::pin(async move {
                global_metrics().current_paths.write().await.remove(&disk);
            })
        })
    };

    (update_fn, done_fn)
}

// ---------------------------------------------------------------------------
// CloseDiskGuard
// ---------------------------------------------------------------------------

pub struct CloseDiskGuard(CloseDiskFn);

impl CloseDiskGuard {
    pub fn new(close_disk: CloseDiskFn) -> Self {
        Self(close_disk)
    }

    pub async fn close(&self) {
        self.0().await;
    }
}

impl Drop for CloseDiskGuard {
    fn drop(&mut self) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let close_fn = self.0.clone();
            handle.spawn(async move { close_fn().await });
        }
        // If there is no runtime we are in a test or shutdown path; skip cleanup.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn report_counts_active_scan_paths() {
        let metrics = Metrics::new();
        let updated_at = Utc::now() - chrono::Duration::seconds(12);
        metrics.current_paths.write().await.insert(
            "disk-a".to_string(),
            Arc::new(CurrentPathTracker::new_at("bucket-a".to_string(), updated_at)),
        );

        let report = metrics.report().await;

        assert_eq!(report.active_scan_paths, 1);
        assert!(
            report.oldest_active_path_age_seconds >= 12,
            "expected active path age to reflect last path update"
        );
        assert_eq!(report.ongoing_buckets, 0);
        assert_eq!(report.active_paths, vec!["disk-a/bucket-a".to_string()]);
    }

    #[tokio::test]
    async fn report_active_path_age_uses_last_path_update() {
        let metrics = Metrics::new();
        let tracker = Arc::new(CurrentPathTracker::new_at(
            "bucket-a".to_string(),
            Utc::now() - chrono::Duration::hours(1),
        ));
        metrics
            .current_paths
            .write()
            .await
            .insert("disk-a".to_string(), Arc::clone(&tracker));

        tracker.update_path("bucket-a/prefix".to_string()).await;

        let report = metrics.report().await;

        assert_eq!(report.active_paths, vec!["disk-a/bucket-a/prefix".to_string()]);
        assert!(
            report.oldest_active_path_age_seconds < 5,
            "expected active path age to reset after path update"
        );
    }

    #[tokio::test]
    async fn report_includes_scanner_queue_state() {
        let metrics = Metrics::new();
        metrics.record_scanner_set_scan_state(Some(3), Some(5), Some(2));
        metrics.record_scanner_disk_bucket_scan_state("0", "0", Some(2), Some(7), Some(1));
        metrics.record_scanner_disk_bucket_scan_state("0", "1", Some(4), Some(11), Some(3));

        let report = metrics.report().await;

        assert_eq!(report.current_set_scan_concurrency_limit, 3);
        assert_eq!(report.current_set_scans_queued, 5);
        assert_eq!(report.current_set_scans_active, 2);
        assert_eq!(report.current_disk_scan_concurrency_limit, 6);
        assert_eq!(report.current_disk_bucket_scans_queued, 18);
        assert_eq!(report.current_disk_bucket_scans_active, 4);

        metrics.reset_scanner_set_scan_state();
        let report = metrics.report().await;

        assert_eq!(report.current_set_scan_concurrency_limit, 0);
        assert_eq!(report.current_set_scans_queued, 0);
        assert_eq!(report.current_set_scans_active, 0);
        assert_eq!(report.current_disk_scan_concurrency_limit, 0);
        assert_eq!(report.current_disk_bucket_scans_queued, 0);
        assert_eq!(report.current_disk_bucket_scans_active, 0);
    }

    #[tokio::test]
    async fn report_derives_scanner_pacing_pressure() {
        let metrics = Metrics::new();
        metrics.record_scanner_set_scan_state(Some(2), Some(3), Some(1));
        metrics.record_scanner_disk_bucket_scan_state("0", "0", Some(1), Some(4), Some(1));
        metrics.record_scan_cycle_partial_with_source(
            Duration::from_secs(10),
            ScanCyclePartialReason::Runtime,
            Some(ScannerWorkSource::Usage),
        );
        metrics.record_scan_cycle_work(ScanCycleWorkSnapshot {
            yield_events: 2,
            yield_duration_millis: 500,
            throttle_sleep_events: 4,
            throttle_sleep_duration_millis: 2500,
            ..Default::default()
        });

        let report = metrics.report().await;

        assert_eq!(report.pacing_pressure.primary_pressure, "queued_scans");
        assert_eq!(report.pacing_pressure.current_queued_scans, 7);
        assert_eq!(report.pacing_pressure.current_active_scans, 2);
        assert!(report.pacing_pressure.last_cycle_budget_limited);
        assert!(report.pacing_pressure.last_cycle_pause_observed);
        assert_eq!(report.pacing_pressure.last_cycle_throttle_sleep_ratio, 0.25);
        assert_eq!(report.pacing_pressure.last_cycle_yield_ratio, 0.05);
        assert_eq!(report.pacing_pressure.last_cycle_total_pause_ratio, 0.3);
    }

    #[tokio::test]
    async fn report_includes_scanner_checkpoint_status() {
        let metrics = Metrics::new();
        metrics.record_scanner_checkpoint_set(1, "bucket/child-a", "directories");
        metrics.record_scanner_checkpoint_used();

        let report = metrics.report().await;

        let checkpoint = report.scan_checkpoint.as_ref().expect("scanner checkpoint should be visible");
        assert_eq!(checkpoint.version, 1);
        assert_eq!(checkpoint.resume_after, "bucket/child-a");
        assert_eq!(checkpoint.reason, "directories");
        assert_eq!(checkpoint.last_event, "used");
        assert_eq!(report.scan_checkpoint_used, 1);
        assert_eq!(report.scan_checkpoint_cleared, 0);

        metrics.record_scanner_checkpoint_stale();
        metrics.record_scanner_checkpoint_cleared();
        let report = metrics.report().await;

        assert!(report.scan_checkpoint.is_none());
        assert_eq!(report.scan_checkpoint_used, 1);
        assert_eq!(report.scan_checkpoint_stale, 1);
        assert_eq!(report.scan_checkpoint_cleared, 1);
    }

    #[tokio::test]
    async fn report_includes_scanner_source_work() {
        let metrics = Metrics::new();
        metrics.record_scanner_source_work(
            ScannerWorkSource::Usage,
            ScannerSourceWorkUpdate {
                checked: 3,
                executed: 1,
                ..Default::default()
            },
        );
        metrics.record_scanner_source_work(
            ScannerWorkSource::Lifecycle,
            ScannerSourceWorkUpdate {
                queued: 2,
                executed: 1,
                failed: 1,
                missed: 3,
                ..Default::default()
            },
        );
        metrics.record_scanner_ilm_enqueue_result(4, true);
        metrics.record_scanner_ilm_enqueue_result(5, false);

        let report = metrics.report().await;

        let usage = report
            .source_work
            .iter()
            .find(|work| work.source == ScannerWorkSource::Usage.as_str())
            .expect("usage source work should be visible");
        assert_eq!(usage.checked, 3);
        assert_eq!(usage.executed, 1);

        let lifecycle = report
            .source_work
            .iter()
            .find(|work| work.source == ScannerWorkSource::Lifecycle.as_str())
            .expect("lifecycle source work should be visible");
        assert_eq!(lifecycle.queued, 6);
        assert_eq!(lifecycle.executed, 1);
        assert_eq!(lifecycle.failed, 1);
        assert_eq!(lifecycle.missed, 8);
    }

    #[tokio::test]
    async fn report_splits_scanner_lifecycle_actions_by_expiry_and_transition() {
        let metrics = Metrics::new();
        let start = metrics.start_scan_cycle_work();

        metrics.record_scanner_lifecycle_action(IlmAction::DeleteAction, 2);
        metrics.record_scanner_lifecycle_action(IlmAction::DeleteRestoredVersionAction, 3);
        metrics.record_scanner_lifecycle_action(IlmAction::TransitionAction, 5);
        metrics.record_scanner_lifecycle_action(IlmAction::TransitionVersionAction, 7);

        let report = metrics.report().await;

        assert_eq!(report.current_cycle_ilm_actions, 17);
        assert_eq!(report.current_cycle_lifecycle_expiry_actions, 5);
        assert_eq!(report.current_cycle_lifecycle_transition_actions, 12);

        metrics.finish_scan_cycle_work(start);
        let report = metrics.report().await;

        assert_eq!(report.last_cycle_ilm_actions, 17);
        assert_eq!(report.last_cycle_lifecycle_expiry_actions, 5);
        assert_eq!(report.last_cycle_lifecycle_transition_actions, 12);
    }

    #[tokio::test]
    async fn report_includes_scanner_lifecycle_transition_status() {
        let metrics = Metrics::new();
        metrics.record_scanner_lifecycle_transition_state(ScannerLifecycleTransitionStateUpdate {
            queue_capacity: 16,
            queued: 5,
            active: 2,
            workers: 4,
            queue_full: 3,
            queue_send_timeout: 1,
            compensation_scheduled: 2,
            compensation_running: 1,
        });
        metrics.record_scanner_transition_enqueue_result(6, true);
        metrics.record_scanner_transition_enqueue_result(2, false);
        metrics.record_scanner_transition_completed(4);
        metrics.record_scanner_transition_failed(1);

        let report = metrics.report().await;

        assert_eq!(report.lifecycle_transition.current_queue_capacity, 16);
        assert_eq!(report.lifecycle_transition.current_queued, 5);
        assert_eq!(report.lifecycle_transition.current_active, 2);
        assert_eq!(report.lifecycle_transition.current_workers, 4);
        assert_eq!(report.lifecycle_transition.queue_full, 3);
        assert_eq!(report.lifecycle_transition.queue_send_timeout, 1);
        assert_eq!(report.lifecycle_transition.compensation_scheduled, 2);
        assert_eq!(report.lifecycle_transition.compensation_running, 1);
        assert_eq!(report.lifecycle_transition.scanner_queued, 6);
        assert_eq!(report.lifecycle_transition.scanner_missed, 2);
        assert_eq!(report.lifecycle_transition.completed, 4);
        assert_eq!(report.lifecycle_transition.failed, 1);
    }

    #[tokio::test]
    async fn report_derives_scanner_maintenance_control_status() {
        let metrics = Metrics::new();
        let start = metrics.start_scan_cycle_work();
        metrics.record_scanner_source_checked(ScannerWorkSource::Usage, 3);
        metrics.record_scanner_source_missed(ScannerWorkSource::Lifecycle, 2);
        metrics.record_scanner_source_queued(ScannerWorkSource::BucketReplication, 1);
        metrics.record_scan_cycle_partial_with_source(
            Duration::from_secs(5),
            ScanCyclePartialReason::Objects,
            Some(ScannerWorkSource::Usage),
        );
        metrics.record_scanner_lifecycle_transition_state(ScannerLifecycleTransitionStateUpdate {
            queue_capacity: 4,
            queued: 2,
            queue_full: 1,
            ..Default::default()
        });

        let report = metrics.report().await;

        assert_eq!(report.maintenance_control.primary_control, "blocked_source");
        let usage = report
            .maintenance_control
            .sources
            .iter()
            .find(|source| source.source == ScannerWorkSource::Usage.as_str())
            .expect("usage source control should be visible");
        assert_eq!(usage.state, "deferred");
        assert_eq!(usage.reason, "partial_cycle");
        assert_eq!(usage.current_checked, 3);
        assert_eq!(usage.partial_cycles, 1);

        let lifecycle = report
            .maintenance_control
            .sources
            .iter()
            .find(|source| source.source == ScannerWorkSource::Lifecycle.as_str())
            .expect("lifecycle source control should be visible");
        assert_eq!(lifecycle.state, "blocked");
        assert_eq!(lifecycle.reason, "missed_work");
        assert_eq!(lifecycle.current_missed, 2);
        assert_eq!(lifecycle.backlog, 2);

        let bucket_replication = report
            .maintenance_control
            .sources
            .iter()
            .find(|source| source.source == ScannerWorkSource::BucketReplication.as_str())
            .expect("bucket replication source control should be visible");
        assert_eq!(bucket_replication.state, "active");
        assert_eq!(bucket_replication.reason, "queued_work");
        assert_eq!(bucket_replication.current_queued, 1);

        metrics.finish_scan_cycle_work(start);
    }

    #[tokio::test]
    async fn report_uses_last_cycle_source_work_for_maintenance_control_between_cycles() {
        let metrics = Metrics::new();
        let start = metrics.start_scan_cycle_work();
        metrics.record_scanner_source_missed(ScannerWorkSource::Lifecycle, 2);

        metrics.finish_scan_cycle_work(start);
        let report = metrics.report().await;

        assert!(report.current_cycle_source_work.is_empty());
        let lifecycle = report
            .maintenance_control
            .sources
            .iter()
            .find(|source| source.source == ScannerWorkSource::Lifecycle.as_str())
            .expect("lifecycle source control should be visible");
        assert_eq!(report.maintenance_control.primary_control, "blocked_source");
        assert_eq!(lifecycle.state, "blocked");
        assert_eq!(lifecycle.reason, "missed_work");
        assert_eq!(lifecycle.current_missed, 2);
        assert_eq!(lifecycle.backlog, 2);
    }

    #[tokio::test]
    async fn scanner_metrics_update_source_work() {
        let metrics = Metrics::new();
        metrics.record_source_work_for_metric(Metric::ScanObject, 3);
        metrics.record_source_work_for_metric(Metric::ScanFolder, 2);
        metrics.record_source_work_for_metric(Metric::SaveUsage, 1);
        metrics.record_source_work_for_metric(Metric::CheckReplication, 4);
        metrics.record_source_work_for_metric(Metric::HealCheck, 5);
        metrics.record_source_work_for_metric(Metric::HealAbandonedObject, 6);

        let report = metrics.report().await;

        let usage = report
            .source_work
            .iter()
            .find(|work| work.source == ScannerWorkSource::Usage.as_str())
            .expect("usage source work should be visible");
        assert_eq!(usage.checked, 5);
        assert_eq!(usage.executed, 1);

        let bucket_replication = report
            .source_work
            .iter()
            .find(|work| work.source == ScannerWorkSource::BucketReplication.as_str())
            .expect("bucket replication source work should be visible");
        assert_eq!(bucket_replication.checked, 4);

        let heal = report
            .source_work
            .iter()
            .find(|work| work.source == ScannerWorkSource::Heal.as_str())
            .expect("heal source work should be visible");
        assert_eq!(heal.checked, 5);
        assert_eq!(heal.executed, 0);
    }

    #[tokio::test]
    async fn report_includes_scan_cycle_source_work() {
        let metrics = Metrics::new();
        metrics.record_scanner_source_work(
            ScannerWorkSource::Usage,
            ScannerSourceWorkUpdate {
                checked: 10,
                executed: 1,
                ..Default::default()
            },
        );

        let start = metrics.start_scan_cycle_work();
        metrics.record_scanner_source_work(
            ScannerWorkSource::Usage,
            ScannerSourceWorkUpdate {
                checked: 3,
                executed: 2,
                ..Default::default()
            },
        );
        metrics.record_scanner_source_work(
            ScannerWorkSource::Lifecycle,
            ScannerSourceWorkUpdate {
                queued: 1,
                executed: 1,
                missed: 2,
                ..Default::default()
            },
        );

        let report = metrics.report().await;
        let usage = report
            .current_cycle_source_work
            .iter()
            .find(|work| work.source == ScannerWorkSource::Usage.as_str())
            .expect("current cycle usage source work should be visible");
        assert_eq!(usage.checked, 3);
        assert_eq!(usage.executed, 2);

        let lifecycle = report
            .current_cycle_source_work
            .iter()
            .find(|work| work.source == ScannerWorkSource::Lifecycle.as_str())
            .expect("current cycle lifecycle source work should be visible");
        assert_eq!(lifecycle.queued, 1);
        assert_eq!(lifecycle.executed, 1);
        assert_eq!(lifecycle.missed, 2);

        metrics.finish_scan_cycle_work(start);
        let report = metrics.report().await;
        assert!(report.current_cycle_source_work.is_empty());

        let usage = report
            .last_cycle_source_work
            .iter()
            .find(|work| work.source == ScannerWorkSource::Usage.as_str())
            .expect("last cycle usage source work should be visible");
        assert_eq!(usage.checked, 3);
        assert_eq!(usage.executed, 2);

        let lifecycle = report
            .last_cycle_source_work
            .iter()
            .find(|work| work.source == ScannerWorkSource::Lifecycle.as_str())
            .expect("last cycle lifecycle source work should be visible");
        assert_eq!(lifecycle.queued, 1);
        assert_eq!(lifecycle.executed, 1);
        assert_eq!(lifecycle.missed, 2);
    }

    #[tokio::test]
    async fn report_preserves_current_cycle_started_time() {
        let previous_init_time = *crate::globals::GLOBAL_INIT_TIME.read().await;
        let init_time = Utc::now() - chrono::Duration::hours(1);
        let cycle_started = Utc::now();
        *crate::globals::GLOBAL_INIT_TIME.write().await = Some(init_time);

        let metrics = Metrics::new();
        metrics
            .set_cycle(Some(CurrentCycle {
                current: 7,
                started: cycle_started,
                ..Default::default()
            }))
            .await;

        let report = metrics.report().await;
        *crate::globals::GLOBAL_INIT_TIME.write().await = previous_init_time;

        assert_eq!(report.current_started, cycle_started);
    }

    #[tokio::test]
    async fn report_includes_current_scan_mode() {
        let metrics = Metrics::new();
        metrics.set_current_scan_mode(HealScanMode::Deep);

        let report = metrics.report().await;

        assert_eq!(report.current_scan_mode, HealScanMode::Deep.as_str());
    }

    #[tokio::test]
    async fn report_includes_last_scan_cycle_result() {
        let metrics = Metrics::new();
        metrics.record_scan_cycle_complete(false, Duration::from_millis(1500));

        let report = metrics.report().await;

        assert_eq!(report.last_cycle_result, SCAN_CYCLE_RESULT_ERROR_LABEL);
        assert_eq!(report.last_cycle_result_code, SCAN_CYCLE_RESULT_ERROR as u64);
        assert_eq!(report.last_cycle_duration_seconds, 1.5);
        assert_eq!(report.failed_cycles, 1);
    }

    #[tokio::test]
    async fn report_tracks_successful_scan_cycle_without_failed_increment() {
        let metrics = Metrics::new();
        metrics.record_scan_cycle_partial_with_source(
            Duration::from_millis(500),
            ScanCyclePartialReason::Runtime,
            Some(ScannerWorkSource::Heal),
        );
        metrics.record_scan_cycle_complete(true, Duration::from_secs(2));

        let report = metrics.report().await;

        assert_eq!(report.last_cycle_result, SCAN_CYCLE_RESULT_SUCCESS_LABEL);
        assert_eq!(report.last_cycle_result_code, SCAN_CYCLE_RESULT_SUCCESS as u64);
        assert_eq!(report.last_cycle_partial_reason, SCAN_CYCLE_RESULT_UNKNOWN_LABEL);
        assert_eq!(report.last_cycle_partial_reason_code, ScanCyclePartialReason::Unknown as u8 as u64);
        assert_eq!(report.last_cycle_partial_source, SCAN_CYCLE_RESULT_UNKNOWN_LABEL);
        assert_eq!(report.last_cycle_partial_source_code, 0);
        assert_eq!(report.last_cycle_duration_seconds, 2.0);
        assert_eq!(report.failed_cycles, 0);
        assert_eq!(report.partial_cycles, 1);
    }

    #[tokio::test]
    async fn report_tracks_partial_scan_cycle_without_failed_increment() {
        let metrics = Metrics::new();
        metrics.record_scan_cycle_partial_with_source(
            Duration::from_millis(2500),
            ScanCyclePartialReason::Directories,
            Some(ScannerWorkSource::Usage),
        );

        let report = metrics.report().await;

        assert_eq!(report.last_cycle_result, SCAN_CYCLE_RESULT_PARTIAL_LABEL);
        assert_eq!(report.last_cycle_result_code, SCAN_CYCLE_RESULT_PARTIAL as u64);
        assert_eq!(report.last_cycle_partial_reason, "directories");
        assert_eq!(report.last_cycle_partial_reason_code, ScanCyclePartialReason::Directories as u8 as u64);
        assert_eq!(report.last_cycle_partial_source, ScannerWorkSource::Usage.as_str());
        assert_eq!(report.last_cycle_partial_source_code, 1);
        assert_eq!(report.last_cycle_duration_seconds, 2.5);
        assert_eq!(report.failed_cycles, 0);
        assert_eq!(report.partial_cycles, 1);
        assert_eq!(report.partial_cycles_directories, 1);
        assert_eq!(report.partial_cycles_runtime, 0);
        assert_eq!(report.partial_cycles_objects, 0);
        assert_eq!(report.partial_cycles_unknown, 0);
        let usage = report
            .partial_cycles_by_source
            .iter()
            .find(|source| source.source == ScannerWorkSource::Usage.as_str())
            .expect("usage partial source count should be visible");
        assert_eq!(usage.cycles, 1);
    }

    #[tokio::test]
    async fn report_includes_last_scan_cycle_work() {
        let metrics = Metrics::new();
        metrics.record_scan_cycle_work(ScanCycleWorkSnapshot {
            objects_scanned: 11,
            directories_scanned: 7,
            bucket_drive_scans: 3,
            bucket_drive_failures: 2,
            yield_events: 5,
            yield_duration_millis: 250,
            throttle_sleep_events: 7,
            throttle_sleep_duration_millis: 500,
            ilm_actions: 13,
            heal_objects: 2,
            replication_checks: 4,
            usage_saves: 6,
            ..Default::default()
        });

        let report = metrics.report().await;

        assert_eq!(report.last_cycle_objects_scanned, 11);
        assert_eq!(report.last_cycle_directories_scanned, 7);
        assert_eq!(report.last_cycle_bucket_drive_scans, 3);
        assert_eq!(report.last_cycle_bucket_drive_failures, 2);
        assert_eq!(report.last_cycle_yield_events, 5);
        assert_eq!(report.last_cycle_yield_duration_seconds, 0.25);
        assert_eq!(report.last_cycle_throttle_sleep_events, 7);
        assert_eq!(report.last_cycle_throttle_sleep_duration_seconds, 0.5);
        assert_eq!(report.last_cycle_ilm_actions, 13);
        assert_eq!(report.last_cycle_heal_objects, 2);
        assert_eq!(report.last_cycle_replication_checks, 4);
        assert_eq!(report.last_cycle_usage_saves, 6);
    }

    #[tokio::test]
    async fn report_includes_bucket_drive_scan_starts() {
        let metrics = Metrics::new();
        metrics.record_scan_bucket_drive_start();
        metrics.record_scan_bucket_drive_failure();

        let report = metrics.report().await;

        assert_eq!(report.life_time_ops.get("scan_bucket_drive_start"), Some(&1));
        assert_eq!(report.life_time_ops.get("scan_bucket_drive_failure"), Some(&1));
    }

    #[tokio::test]
    async fn report_includes_active_scan_cycle_work() {
        let metrics = Metrics::new();
        metrics.operations[Metric::ScanObject as usize].store(10, Ordering::Relaxed);
        metrics.operations[Metric::ScanFolder as usize].store(5, Ordering::Relaxed);
        metrics.operations[Metric::ScanBucketDrive as usize].store(1, Ordering::Relaxed);
        metrics.operations[Metric::ScanBucketDriveFailure as usize].store(1, Ordering::Relaxed);
        metrics.operations[Metric::Yield as usize].store(2, Ordering::Relaxed);
        metrics.operations[Metric::ThrottleSleep as usize].store(3, Ordering::Relaxed);
        metrics.operations[Metric::Ilm as usize].store(6, Ordering::Relaxed);
        metrics.record_scanner_ilm_action(6);
        metrics.operations[Metric::HealAbandonedObject as usize].store(4, Ordering::Relaxed);
        metrics.operations[Metric::CheckReplication as usize].store(5, Ordering::Relaxed);
        metrics.operations[Metric::SaveUsage as usize].store(3, Ordering::Relaxed);
        metrics.scanner_yield_duration_millis.store(100, Ordering::Relaxed);
        metrics.scanner_throttle_sleep_duration_millis.store(200, Ordering::Relaxed);

        let start = metrics.start_scan_cycle_work();
        metrics.operations[Metric::ScanObject as usize].store(17, Ordering::Relaxed);
        metrics.operations[Metric::ScanFolder as usize].store(8, Ordering::Relaxed);
        metrics.operations[Metric::ScanBucketDrive as usize].store(3, Ordering::Relaxed);
        metrics.operations[Metric::ScanBucketDriveFailure as usize].store(4, Ordering::Relaxed);
        metrics.operations[Metric::Ilm as usize].store(15, Ordering::Relaxed);
        metrics.record_scanner_ilm_action(9);
        metrics.operations[Metric::HealAbandonedObject as usize].store(7, Ordering::Relaxed);
        metrics.operations[Metric::CheckReplication as usize].store(11, Ordering::Relaxed);
        metrics.operations[Metric::SaveUsage as usize].store(5, Ordering::Relaxed);
        metrics.record_scanner_yield(Duration::from_millis(150));
        metrics.record_scanner_throttle_sleep(Duration::from_millis(250));

        let report = metrics.report().await;

        assert_eq!(report.current_cycle_objects_scanned, 7);
        assert_eq!(report.current_cycle_directories_scanned, 3);
        assert_eq!(report.current_cycle_bucket_drive_scans, 2);
        assert_eq!(report.current_cycle_bucket_drive_failures, 3);
        assert_eq!(report.current_cycle_yield_events, 1);
        assert_eq!(report.current_cycle_yield_duration_seconds, 0.15);
        assert_eq!(report.current_cycle_throttle_sleep_events, 1);
        assert_eq!(report.current_cycle_throttle_sleep_duration_seconds, 0.25);
        assert_eq!(report.current_cycle_ilm_actions, 9);
        assert_eq!(report.current_cycle_heal_objects, 3);
        assert_eq!(report.current_cycle_replication_checks, 6);
        assert_eq!(report.current_cycle_usage_saves, 2);

        metrics.finish_scan_cycle_work(start);
        let report = metrics.report().await;

        assert_eq!(report.current_cycle_objects_scanned, 0);
        assert_eq!(report.current_cycle_directories_scanned, 0);
        assert_eq!(report.current_cycle_bucket_drive_scans, 0);
        assert_eq!(report.current_cycle_bucket_drive_failures, 0);
        assert_eq!(report.current_cycle_yield_events, 0);
        assert_eq!(report.current_cycle_yield_duration_seconds, 0.0);
        assert_eq!(report.current_cycle_throttle_sleep_events, 0);
        assert_eq!(report.current_cycle_throttle_sleep_duration_seconds, 0.0);
        assert_eq!(report.current_cycle_ilm_actions, 0);
        assert_eq!(report.current_cycle_heal_objects, 0);
        assert_eq!(report.current_cycle_replication_checks, 0);
        assert_eq!(report.current_cycle_usage_saves, 0);
        assert_eq!(report.last_cycle_objects_scanned, 7);
        assert_eq!(report.last_cycle_directories_scanned, 3);
        assert_eq!(report.last_cycle_bucket_drive_scans, 2);
        assert_eq!(report.last_cycle_bucket_drive_failures, 3);
        assert_eq!(report.last_cycle_yield_events, 1);
        assert_eq!(report.last_cycle_yield_duration_seconds, 0.15);
        assert_eq!(report.last_cycle_throttle_sleep_events, 1);
        assert_eq!(report.last_cycle_throttle_sleep_duration_seconds, 0.25);
        assert_eq!(report.last_cycle_ilm_actions, 9);
        assert_eq!(report.last_cycle_heal_objects, 3);
        assert_eq!(report.last_cycle_replication_checks, 6);
        assert_eq!(report.last_cycle_usage_saves, 2);
    }

    #[tokio::test]
    async fn scanner_cycle_ilm_actions_ignore_global_ilm_work() {
        let metrics = Metrics::new();
        metrics.operations[Metric::Ilm as usize].store(4, Ordering::Relaxed);

        let start = metrics.start_scan_cycle_work();
        metrics.operations[Metric::Ilm as usize].store(11, Ordering::Relaxed);

        let report = metrics.report().await;

        assert_eq!(report.current_cycle_ilm_actions, 0);

        metrics.record_scanner_ilm_action(3);
        let report = metrics.report().await;

        assert_eq!(report.current_cycle_ilm_actions, 3);

        metrics.finish_scan_cycle_work(start);
        let report = metrics.report().await;

        assert_eq!(report.last_cycle_ilm_actions, 3);
    }

    #[test]
    fn record_scanner_yield_tracks_count_and_duration() {
        let metrics = Metrics::new();

        metrics.record_scanner_yield(Duration::from_millis(42));

        assert_eq!(metrics.lifetime(Metric::Yield), 1);
        assert_eq!(metrics.scanner_yield_duration_millis.load(Ordering::Relaxed), 42);
        assert_eq!(metrics.last_minute(Metric::Yield).n, 1);
        assert_eq!(metrics.lifetime(Metric::ThrottleSleep), 0);
    }

    #[test]
    fn record_scanner_throttle_sleep_tracks_count_and_duration() {
        let metrics = Metrics::new();

        metrics.record_scanner_throttle_sleep(Duration::from_millis(42));

        assert_eq!(metrics.lifetime(Metric::ThrottleSleep), 1);
        assert_eq!(metrics.scanner_throttle_sleep_duration_millis.load(Ordering::Relaxed), 42);
        assert_eq!(metrics.last_minute(Metric::ThrottleSleep).n, 1);
        assert_eq!(metrics.lifetime(Metric::Yield), 0);
    }

    #[tokio::test]
    async fn report_includes_scanner_throttle_config() {
        let metrics = Metrics::new();

        metrics.record_scanner_throttle_config(true, 2.5, Duration::from_millis(1500), 128);
        let report = metrics.report().await;

        assert!(report.throttle_idle_mode_enabled);
        assert_eq!(report.throttle_sleep_factor, 2.5);
        assert_eq!(report.throttle_max_sleep_seconds, 1.5);
        assert_eq!(report.yield_every_n_objects, 128);
    }

    #[tokio::test]
    async fn scanner_throttle_config_rejects_invalid_sleep_factor() {
        let metrics = Metrics::new();

        metrics.record_scanner_throttle_config(false, f64::NAN, Duration::ZERO, 0);
        let report = metrics.report().await;

        assert!(!report.throttle_idle_mode_enabled);
        assert_eq!(report.throttle_sleep_factor, 0.0);
        assert_eq!(report.throttle_max_sleep_seconds, 0.0);
        assert_eq!(report.yield_every_n_objects, 0);
    }

    #[tokio::test]
    async fn report_includes_scanner_cycle_config() {
        let metrics = Metrics::new();

        metrics.record_scanner_cycle_config(
            Duration::from_secs(3600),
            Some(Duration::from_secs(86400)),
            Some(Duration::from_secs(1800)),
            Some(1_000_000),
            Some(100_000),
        );
        let report = metrics.report().await;

        assert_eq!(report.cycle_interval_seconds, 3600.0);
        assert_eq!(report.cycle_max_duration_seconds, 1800.0);
        assert_eq!(report.cycle_max_objects, 1_000_000);
        assert_eq!(report.cycle_max_directories, 100_000);
        assert!(report.bitrot_cycle_enabled);
        assert_eq!(report.bitrot_cycle_seconds, 86400.0);

        metrics.record_scanner_cycle_config(Duration::from_secs(60), None, None, None, None);
        let report = metrics.report().await;

        assert_eq!(report.cycle_interval_seconds, 60.0);
        assert_eq!(report.cycle_max_duration_seconds, 0.0);
        assert_eq!(report.cycle_max_objects, 0);
        assert_eq!(report.cycle_max_directories, 0);
        assert!(!report.bitrot_cycle_enabled);
        assert_eq!(report.bitrot_cycle_seconds, 0.0);
    }
}
