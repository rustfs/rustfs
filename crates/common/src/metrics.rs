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

use crate::last_minute::{AccElem, LastMinuteLatency};
use chrono::{DateTime, Utc};
use rustfs_madmin::metrics::{ScannerMetrics as M_ScannerMetrics, TimedAction};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Display,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
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
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CurrentCycle {
    pub current: u64,
    pub next: u64,
    pub cycle_completed: Vec<DateTime<Utc>>,
    pub started: DateTime<Utc>,
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

pub fn emit_scan_cycle_complete(success: bool, duration: Duration) {
    let result = if success { "success" } else { "error" };
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
    pub async fn report(&self) -> M_ScannerMetrics {
        let mut m = M_ScannerMetrics::default();

        if let Some(cycle) = self.get_cycle().await {
            m.current_cycle = cycle.current;
            m.cycles_completed_at = cycle.cycle_completed;
            m.current_started = cycle.started;
        }

        if let Some(init_time) = crate::get_global_init_time().await {
            m.current_started = init_time;
        }

        m.collected_at = Utc::now();
        m.active_paths = self.get_current_paths().await;

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
                    TimedAction {
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
                    TimedAction {
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
