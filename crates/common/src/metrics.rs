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
            10 => Some(Self::CleanAbandoned),
            11 => Some(Self::ApplyNonCurrent),
            12 => Some(Self::HealAbandonedVersion),
            13 => Some(Self::QuotaCheck),
            14 => Some(Self::QuotaViolation),
            15 => Some(Self::QuotaSync),
            16 => Some(Self::StartTrace),
            17 => Some(Self::ScanObject),
            18 => Some(Self::HealAbandonedObject),
            19 => Some(Self::LastRealtime),
            20 => Some(Self::ScanFolder),
            21 => Some(Self::ScanCycle),
            22 => Some(Self::ScanBucketDrive),
            23 => Some(Self::CompactFolder),
            24 => Some(Self::Last),
            _ => None,
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

struct CurrentPathTracker {
    current_path: Arc<RwLock<String>>,
}

impl CurrentPathTracker {
    fn new(initial_path: String) -> Self {
        Self {
            current_path: Arc::new(RwLock::new(initial_path)),
        }
    }

    async fn update_path(&self, path: String) {
        *self.current_path.write().await = path;
    }

    async fn get_path(&self) -> String {
        self.current_path.read().await.clone()
    }
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
    last_scan_cycle_result: AtomicU8,
    last_scan_cycle_duration_millis: AtomicU64,
    last_scan_cycle_objects_scanned: AtomicU64,
    last_scan_cycle_directories_scanned: AtomicU64,
    last_scan_cycle_bucket_drive_scans: AtomicU64,
    failed_scan_cycles: AtomicU64,
}

const SCAN_CYCLE_RESULT_UNKNOWN: u8 = 0;
const SCAN_CYCLE_RESULT_SUCCESS: u8 = 1;
const SCAN_CYCLE_RESULT_ERROR: u8 = 2;
const SCAN_CYCLE_RESULT_UNKNOWN_LABEL: &str = "unknown";
const SCAN_CYCLE_RESULT_SUCCESS_LABEL: &str = "success";
const SCAN_CYCLE_RESULT_ERROR_LABEL: &str = "error";

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
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScannerTimedAction {
    pub count: u64,
    pub acc_time: u64,
    pub bytes: u64,
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
    pub life_time_ops: HashMap<String, u64>,
    pub life_time_ilm: HashMap<String, u64>,
    pub last_minute: ScannerLastMinute,
    pub active_paths: Vec<String>,
    pub current_scan_mode: String,
    #[serde(default)]
    pub current_cycle_objects_scanned: u64,
    #[serde(default)]
    pub current_cycle_directories_scanned: u64,
    #[serde(default)]
    pub current_cycle_bucket_drive_scans: u64,
    pub last_cycle_result: String,
    pub last_cycle_result_code: u64,
    pub last_cycle_duration_seconds: f64,
    #[serde(default)]
    pub last_cycle_objects_scanned: u64,
    #[serde(default)]
    pub last_cycle_directories_scanned: u64,
    #[serde(default)]
    pub last_cycle_bucket_drive_scans: u64,
    pub failed_cycles: u64,
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
        _ => SCAN_CYCLE_RESULT_UNKNOWN_LABEL,
    }
}

fn duration_millis_saturated(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
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
            last_scan_cycle_result: AtomicU8::new(SCAN_CYCLE_RESULT_UNKNOWN),
            last_scan_cycle_duration_millis: AtomicU64::new(0),
            last_scan_cycle_objects_scanned: AtomicU64::new(0),
            last_scan_cycle_directories_scanned: AtomicU64::new(0),
            last_scan_cycle_bucket_drive_scans: AtomicU64::new(0),
            failed_scan_cycles: AtomicU64::new(0),
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
                global_metrics().operations[metric_idx].fetch_add(count as u64, Ordering::Relaxed);
                emit_otel_counter(metric_idx, count as u64);
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
        emit_otel_counter(metric_idx, 1);
        if metric_idx < Metric::LastRealtime as usize {
            global_metrics().latency[metric_idx].add(duration);
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
        self.last_scan_cycle_duration_millis
            .store(duration_millis_saturated(duration), Ordering::Relaxed);
    }

    pub fn start_scan_cycle_work(&self) -> ScanCycleWorkSnapshot {
        let snapshot = self.scan_cycle_work_snapshot();
        self.current_scan_cycle_objects_start
            .store(snapshot.objects_scanned, Ordering::Relaxed);
        self.current_scan_cycle_directories_start
            .store(snapshot.directories_scanned, Ordering::Relaxed);
        self.current_scan_cycle_bucket_drive_scans_start
            .store(snapshot.bucket_drive_scans, Ordering::Relaxed);
        self.current_scan_cycle_work_active.store(true, Ordering::Relaxed);
        snapshot
    }

    pub fn finish_scan_cycle_work(&self, start: ScanCycleWorkSnapshot) {
        let work = self.scan_cycle_work_since(start);
        self.record_scan_cycle_work(work.objects_scanned, work.directories_scanned, work.bucket_drive_scans);
        self.current_scan_cycle_work_active.store(false, Ordering::Relaxed);
    }

    fn scan_cycle_work_snapshot(&self) -> ScanCycleWorkSnapshot {
        ScanCycleWorkSnapshot {
            objects_scanned: self.lifetime(Metric::ScanObject),
            directories_scanned: self.lifetime(Metric::ScanFolder),
            bucket_drive_scans: self.lifetime(Metric::ScanBucketDrive),
        }
    }

    fn current_scan_cycle_work_start(&self) -> ScanCycleWorkSnapshot {
        ScanCycleWorkSnapshot {
            objects_scanned: self.current_scan_cycle_objects_start.load(Ordering::Relaxed),
            directories_scanned: self.current_scan_cycle_directories_start.load(Ordering::Relaxed),
            bucket_drive_scans: self.current_scan_cycle_bucket_drive_scans_start.load(Ordering::Relaxed),
        }
    }

    fn scan_cycle_work_since(&self, start: ScanCycleWorkSnapshot) -> ScanCycleWorkSnapshot {
        let current = self.scan_cycle_work_snapshot();
        ScanCycleWorkSnapshot {
            objects_scanned: current.objects_scanned.saturating_sub(start.objects_scanned),
            directories_scanned: current.directories_scanned.saturating_sub(start.directories_scanned),
            bucket_drive_scans: current.bucket_drive_scans.saturating_sub(start.bucket_drive_scans),
        }
    }

    pub fn record_scan_cycle_work(&self, objects_scanned: u64, directories_scanned: u64, bucket_drive_scans: u64) {
        // Telemetry-only gauges: readers may observe a transient mixed snapshot
        // while these independent atomic fields are updated.
        self.last_scan_cycle_objects_scanned.store(objects_scanned, Ordering::Relaxed);
        self.last_scan_cycle_directories_scanned
            .store(directories_scanned, Ordering::Relaxed);
        self.last_scan_cycle_bucket_drive_scans
            .store(bucket_drive_scans, Ordering::Relaxed);
    }

    /// Snapshot of every path currently being scanned.
    pub async fn get_current_paths(&self) -> Vec<String> {
        let paths = self.current_paths.read().await;
        let mut result = Vec::with_capacity(paths.len());
        for (disk, tracker) in paths.iter() {
            result.push(format!("{disk}/{}", tracker.get_path().await));
        }
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
        m.active_paths = self.get_current_paths().await;
        m.active_scan_paths = m.active_paths.len();
        m.current_scan_mode = self.current_scan_mode().as_str().to_string();
        if self.current_scan_cycle_work_active.load(Ordering::Relaxed) {
            let current_work = self.scan_cycle_work_since(self.current_scan_cycle_work_start());
            m.current_cycle_objects_scanned = current_work.objects_scanned;
            m.current_cycle_directories_scanned = current_work.directories_scanned;
            m.current_cycle_bucket_drive_scans = current_work.bucket_drive_scans;
        }
        let last_cycle_result = self.last_scan_cycle_result.load(Ordering::Relaxed);
        m.last_cycle_result = scan_cycle_result_label(last_cycle_result).to_string();
        m.last_cycle_result_code = last_cycle_result as u64;
        m.last_cycle_duration_seconds = self.last_scan_cycle_duration_millis.load(Ordering::Relaxed) as f64 / 1000.0;
        m.last_cycle_objects_scanned = self.last_scan_cycle_objects_scanned.load(Ordering::Relaxed);
        m.last_cycle_directories_scanned = self.last_scan_cycle_directories_scanned.load(Ordering::Relaxed);
        m.last_cycle_bucket_drive_scans = self.last_scan_cycle_bucket_drive_scans.load(Ordering::Relaxed);
        m.failed_cycles = self.failed_scan_cycles.load(Ordering::Relaxed);

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
        metrics
            .current_paths
            .write()
            .await
            .insert("disk-a".to_string(), Arc::new(CurrentPathTracker::new("bucket-a".to_string())));

        let report = metrics.report().await;

        assert_eq!(report.active_scan_paths, 1);
        assert_eq!(report.ongoing_buckets, 0);
        assert_eq!(report.active_paths, vec!["disk-a/bucket-a".to_string()]);
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
        metrics.record_scan_cycle_complete(true, Duration::from_secs(2));

        let report = metrics.report().await;

        assert_eq!(report.last_cycle_result, SCAN_CYCLE_RESULT_SUCCESS_LABEL);
        assert_eq!(report.last_cycle_result_code, SCAN_CYCLE_RESULT_SUCCESS as u64);
        assert_eq!(report.last_cycle_duration_seconds, 2.0);
        assert_eq!(report.failed_cycles, 0);
    }

    #[tokio::test]
    async fn report_includes_last_scan_cycle_work() {
        let metrics = Metrics::new();
        metrics.record_scan_cycle_work(11, 7, 3);

        let report = metrics.report().await;

        assert_eq!(report.last_cycle_objects_scanned, 11);
        assert_eq!(report.last_cycle_directories_scanned, 7);
        assert_eq!(report.last_cycle_bucket_drive_scans, 3);
    }

    #[tokio::test]
    async fn report_includes_active_scan_cycle_work() {
        let metrics = Metrics::new();
        metrics.operations[Metric::ScanObject as usize].store(10, Ordering::Relaxed);
        metrics.operations[Metric::ScanFolder as usize].store(5, Ordering::Relaxed);
        metrics.operations[Metric::ScanBucketDrive as usize].store(1, Ordering::Relaxed);

        let start = metrics.start_scan_cycle_work();
        metrics.operations[Metric::ScanObject as usize].store(17, Ordering::Relaxed);
        metrics.operations[Metric::ScanFolder as usize].store(8, Ordering::Relaxed);
        metrics.operations[Metric::ScanBucketDrive as usize].store(3, Ordering::Relaxed);

        let report = metrics.report().await;

        assert_eq!(report.current_cycle_objects_scanned, 7);
        assert_eq!(report.current_cycle_directories_scanned, 3);
        assert_eq!(report.current_cycle_bucket_drive_scans, 2);

        metrics.finish_scan_cycle_work(start);
        let report = metrics.report().await;

        assert_eq!(report.current_cycle_objects_scanned, 0);
        assert_eq!(report.current_cycle_directories_scanned, 0);
        assert_eq!(report.current_cycle_bucket_drive_scans, 0);
        assert_eq!(report.last_cycle_objects_scanned, 7);
        assert_eq!(report.last_cycle_directories_scanned, 3);
        assert_eq!(report.last_cycle_bucket_drive_scans, 2);
    }
}
