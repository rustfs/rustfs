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
use rustfs_madmin::metrics::ScannerMetrics as M_ScannerMetrics;
use std::{
    collections::HashMap,
    fmt::Display,
    pin::Pin,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::sync::{Mutex, RwLock};

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

#[derive(Clone, Debug, PartialEq, PartialOrd)]
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
    pub fn as_str(self) -> &'static str {
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
        // Safe conversion using match instead of unsafe transmute
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
            13 => Some(Self::StartTrace),
            14 => Some(Self::ScanObject),
            15 => Some(Self::HealAbandonedObject),
            16 => Some(Self::LastRealtime),
            17 => Some(Self::ScanFolder),
            18 => Some(Self::ScanCycle),
            19 => Some(Self::ScanBucketDrive),
            20 => Some(Self::CompactFolder),
            21 => Some(Self::Last),
            _ => None,
        }
    }
}

/// Thread-safe wrapper for LastMinuteLatency with atomic operations
#[derive(Default)]
pub struct LockedLastMinuteLatency {
    latency: Arc<Mutex<LastMinuteLatency>>,
}

impl Clone for LockedLastMinuteLatency {
    fn clone(&self) -> Self {
        Self {
            latency: Arc::clone(&self.latency),
        }
    }
}

impl LockedLastMinuteLatency {
    pub fn new() -> Self {
        Self {
            latency: Arc::new(Mutex::new(LastMinuteLatency::default())),
        }
    }

    /// Add a duration measurement
    pub async fn add(&self, duration: Duration) {
        self.add_size(duration, 0).await;
    }

    /// Add a duration measurement with size
    pub async fn add_size(&self, duration: Duration, size: u64) {
        let mut latency = self.latency.lock().await;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let elem = AccElem {
            n: 1,
            total: duration.as_secs(),
            size,
        };
        latency.add_all(now, &elem);
    }

    /// Get total accumulated metrics for the last minute
    pub async fn total(&self) -> AccElem {
        let mut latency = self.latency.lock().await;
        latency.get_total()
    }
}

/// Current path tracker for monitoring active scan paths
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

/// Main scanner metrics structure
pub struct Metrics {
    // All fields must be accessed atomically and aligned.
    operations: Vec<AtomicU64>,
    latency: Vec<LockedLastMinuteLatency>,
    actions: Vec<AtomicU64>,
    actions_latency: Vec<LockedLastMinuteLatency>,
    // Current paths contains disk -> tracker mappings
    current_paths: Arc<RwLock<HashMap<String, Arc<CurrentPathTracker>>>>,

    // Cycle information
    cycle_info: Arc<RwLock<Option<CurrentCycle>>>,
}

// This is a placeholder. We'll need to define this struct.
#[derive(Clone, Debug)]
pub struct CurrentCycle {
    pub current: u64,
    pub cycle_completed: Vec<DateTime<Utc>>,
    pub started: DateTime<Utc>,
}

impl Metrics {
    pub fn new() -> Self {
        let operations = (0..Metric::Last as usize).map(|_| AtomicU64::new(0)).collect();

        let latency = (0..Metric::LastRealtime as usize)
            .map(|_| LockedLastMinuteLatency::new())
            .collect();

        Self {
            operations,
            latency,
            actions: (0..IlmAction::ActionCount as usize).map(|_| AtomicU64::new(0)).collect(),
            actions_latency: vec![LockedLastMinuteLatency::default(); IlmAction::ActionCount as usize],
            current_paths: Arc::new(RwLock::new(HashMap::new())),
            cycle_info: Arc::new(RwLock::new(None)),
        }
    }

    /// Log scanner action with custom metadata - compatible with existing usage
    pub fn log(metric: Metric) -> impl Fn(&HashMap<String, String>) {
        let metric = metric as usize;
        let start_time = SystemTime::now();
        move |_custom: &HashMap<String, String>| {
            let duration = SystemTime::now().duration_since(start_time).unwrap_or_default();

            // Update operation count
            global_metrics().operations[metric].fetch_add(1, Ordering::Relaxed);

            // Update latency for realtime metrics (spawn async task for this)
            if (metric) < Metric::LastRealtime as usize {
                let metric_index = metric;
                tokio::spawn(async move {
                    global_metrics().latency[metric_index].add(duration).await;
                });
            }

            // Log trace metrics
            if metric as u8 > Metric::StartTrace as u8 {
                //debug!(metric = metric.as_str(), duration_ms = duration.as_millis(), "Scanner trace metric");
            }
        }
    }

    /// Time scanner action with size - returns function that takes size
    pub fn time_size(metric: Metric) -> impl Fn(u64) {
        let metric = metric as usize;
        let start_time = SystemTime::now();
        move |size: u64| {
            let duration = SystemTime::now().duration_since(start_time).unwrap_or_default();

            // Update operation count
            global_metrics().operations[metric].fetch_add(1, Ordering::Relaxed);

            // Update latency for realtime metrics with size (spawn async task)
            if (metric) < Metric::LastRealtime as usize {
                let metric_index = metric;
                tokio::spawn(async move {
                    global_metrics().latency[metric_index].add_size(duration, size).await;
                });
            }
        }
    }

    /// Time a scanner action - returns a closure to call when done
    pub fn time(metric: Metric) -> impl Fn() {
        let metric = metric as usize;
        let start_time = SystemTime::now();
        move || {
            let duration = SystemTime::now().duration_since(start_time).unwrap_or_default();

            // Update operation count
            global_metrics().operations[metric].fetch_add(1, Ordering::Relaxed);

            // Update latency for realtime metrics (spawn async task)
            if (metric) < Metric::LastRealtime as usize {
                let metric_index = metric;
                tokio::spawn(async move {
                    global_metrics().latency[metric_index].add(duration).await;
                });
            }
        }
    }

    /// Time N scanner actions - returns function that takes count, then returns completion function
    pub fn time_n(metric: Metric) -> Box<dyn Fn(usize) -> Box<dyn Fn() + Send + Sync> + Send + Sync> {
        let metric = metric as usize;
        let start_time = SystemTime::now();
        Box::new(move |count: usize| {
            Box::new(move || {
                let duration = SystemTime::now().duration_since(start_time).unwrap_or_default();

                // Update operation count
                global_metrics().operations[metric].fetch_add(count as u64, Ordering::Relaxed);

                // Update latency for realtime metrics (spawn async task)
                if (metric) < Metric::LastRealtime as usize {
                    let metric_index = metric;
                    tokio::spawn(async move {
                        global_metrics().latency[metric_index].add(duration).await;
                    });
                }
            })
        })
    }

    /// Time ILM action with versions - returns function that takes versions, then returns completion function
    pub fn time_ilm(a: IlmAction) -> Box<dyn Fn(u64) -> Box<dyn Fn() + Send + Sync> + Send + Sync> {
        let a_clone = a as usize;
        if a_clone == IlmAction::NoneAction as usize || a_clone >= IlmAction::ActionCount as usize {
            return Box::new(move |_: u64| Box::new(move || {}));
        }
        let start = SystemTime::now();
        Box::new(move |versions: u64| {
            Box::new(move || {
                let duration = SystemTime::now().duration_since(start).unwrap_or(Duration::from_secs(0));
                tokio::spawn(async move {
                    global_metrics().actions[a_clone].fetch_add(versions, Ordering::Relaxed);
                    global_metrics().actions_latency[a_clone].add(duration).await;
                });
            })
        })
    }

    /// Increment time with specific duration
    pub async fn inc_time(metric: Metric, duration: Duration) {
        let metric = metric as usize;
        // Update operation count
        global_metrics().operations[metric].fetch_add(1, Ordering::Relaxed);

        // Update latency for realtime metrics
        if (metric) < Metric::LastRealtime as usize {
            global_metrics().latency[metric].add(duration).await;
        }
    }

    /// Get lifetime operation count for a metric
    pub fn lifetime(&self, metric: Metric) -> u64 {
        let metric = metric as usize;
        if (metric) >= Metric::Last as usize {
            return 0;
        }
        self.operations[metric].load(Ordering::Relaxed)
    }

    /// Get last minute statistics for a metric
    pub async fn last_minute(&self, metric: Metric) -> AccElem {
        let metric = metric as usize;
        if (metric) >= Metric::LastRealtime as usize {
            return AccElem::default();
        }
        self.latency[metric].total().await
    }

    /// Set current cycle information
    pub async fn set_cycle(&self, cycle: Option<CurrentCycle>) {
        *self.cycle_info.write().await = cycle;
    }

    /// Get current cycle information
    pub async fn get_cycle(&self) -> Option<CurrentCycle> {
        self.cycle_info.read().await.clone()
    }

    /// Get current active paths
    pub async fn get_current_paths(&self) -> Vec<String> {
        let mut result = Vec::new();
        let paths = self.current_paths.read().await;

        for (disk, tracker) in paths.iter() {
            let path = tracker.get_path().await;
            result.push(format!("{disk}/{path}"));
        }

        result
    }

    /// Get number of active drives
    pub async fn active_drives(&self) -> usize {
        self.current_paths.read().await.len()
    }

    /// Generate metrics report
    pub async fn report(&self) -> M_ScannerMetrics {
        let mut metrics = M_ScannerMetrics::default();

        // Set cycle information
        if let Some(cycle) = self.get_cycle().await {
            metrics.current_cycle = cycle.current;
            metrics.cycles_completed_at = cycle.cycle_completed;
            metrics.current_started = cycle.started;
        }

        metrics.collected_at = Utc::now();
        metrics.active_paths = self.get_current_paths().await;

        // Lifetime operations
        for i in 0..Metric::Last as usize {
            let count = self.operations[i].load(Ordering::Relaxed);
            if count > 0 {
                if let Some(metric) = Metric::from_index(i) {
                    metrics.life_time_ops.insert(metric.as_str().to_string(), count);
                }
            }
        }

        // Last minute statistics for realtime metrics
        for i in 0..Metric::LastRealtime as usize {
            let last_min = self.latency[i].total().await;
            if last_min.n > 0 {
                if let Some(_metric) = Metric::from_index(i) {
                    // Convert to madmin TimedAction format if needed
                    // This would require implementing the conversion
                }
            }
        }

        metrics
    }
}

// Type aliases for compatibility with existing code
pub type UpdateCurrentPathFn = Arc<dyn Fn(&str) -> Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;
pub type CloseDiskFn = Arc<dyn Fn() -> Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

/// Create a current path updater for tracking scan progress
pub fn current_path_updater(disk: &str, initial: &str) -> (UpdateCurrentPathFn, CloseDiskFn) {
    let tracker = Arc::new(CurrentPathTracker::new(initial.to_string()));
    let disk_name = disk.to_string();

    // Store the tracker in global metrics
    let tracker_clone = Arc::clone(&tracker);
    let disk_clone = disk_name.clone();
    tokio::spawn(async move {
        global_metrics().current_paths.write().await.insert(disk_clone, tracker_clone);
    });

    let update_fn = {
        let tracker = Arc::clone(&tracker);
        Arc::new(move |path: &str| -> Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
            let tracker = Arc::clone(&tracker);
            let path = path.to_string();
            Box::pin(async move {
                tracker.update_path(path).await;
            })
        })
    };

    let done_fn = {
        let disk_name = disk_name.clone();
        Arc::new(move || -> Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
            let disk_name = disk_name.clone();
            Box::pin(async move {
                global_metrics().current_paths.write().await.remove(&disk_name);
            })
        })
    };

    (update_fn, done_fn)
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
