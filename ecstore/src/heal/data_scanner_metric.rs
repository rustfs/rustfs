use common::last_minute::{AccElem, LastMinuteLatency};
use lazy_static::lazy_static;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::Once;
use std::time::{Duration, UNIX_EPOCH};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::SystemTime,
};
use tokio::sync::RwLock;

use super::data_scanner::{CurrentScannerCycle, UpdateCurrentPathFn};

lazy_static! {
    pub static ref globalScannerMetrics: Arc<RwLock<ScannerMetrics>> = Arc::new(RwLock::new(ScannerMetrics::new()));
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum ScannerMetric {
    // START Realtime metrics, that only to records
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

static INIT: Once = Once::new();

#[derive(Default)]
pub struct LockedLastMinuteLatency {
    cached_sec: AtomicU64,
    cached: AccElem,
    mu: RwLock<bool>,
    latency: LastMinuteLatency,
}

impl Clone for LockedLastMinuteLatency {
    fn clone(&self) -> Self {
        Self {
            cached_sec: AtomicU64::new(0),
            cached: self.cached.clone(),
            mu: RwLock::new(true),
            latency: self.latency.clone(),
        }
    }
}

impl LockedLastMinuteLatency {
    pub async fn add(&mut self, value: &Duration) {
        self.add_size(value, 0).await;
    }

    pub async fn add_size(&mut self, value: &Duration, sz: u64) {
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        INIT.call_once(|| {
            self.cached = AccElem::default();
            self.cached_sec.store(t, Ordering::SeqCst);
        });
        let last_t = self.cached_sec.load(Ordering::SeqCst);
        if last_t != t
            && self
                .cached_sec
                .compare_exchange(last_t, t, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            let old = self.cached.clone();
            self.cached = AccElem::default();
            let mut a = AccElem::default();
            a.size = old.size;
            a.total = old.total;
            a.n = old.n;
            let _ = self.mu.write().await;
            self.latency.add_all(t - 1, &a);
        }
        self.cached.n += 1;
        self.cached.total += value.as_secs();
        self.cached.size += sz;
    }

    pub async fn total(&mut self) -> AccElem {
        let _ = self.mu.read().await;
        self.latency.get_total()
    }
}

pub type LogFn = Arc<dyn Fn(&HashMap<String, String>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;
pub type TimeSizeFn = Arc<dyn Fn(u64) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;
pub type TimeFn = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;

pub struct ScannerMetrics {
    operations: Vec<AtomicU32>,
    latency: Vec<LockedLastMinuteLatency>,
    cycle_info: RwLock<Option<CurrentScannerCycle>>,
    current_paths: HashMap<String, String>,
}

impl ScannerMetrics {
    pub fn new() -> Self {
        Self {
            operations: (0..ScannerMetric::Last as usize).map(|_| AtomicU32::new(0)).collect(),
            latency: vec![LockedLastMinuteLatency::default(); ScannerMetric::LastRealtime as usize],
            cycle_info: RwLock::new(None),
            current_paths: HashMap::new(),
        }
    }

    pub async fn set_cycle(&mut self, c: Option<CurrentScannerCycle>) {
        *self.cycle_info.write().await = c;
    }

    pub fn log(s: ScannerMetric) -> LogFn {
        let start = SystemTime::now();
        let s_clone = s as usize;
        Arc::new(move |_custom: &HashMap<String, String>| {
            Box::pin(async move {
                let duration = SystemTime::now().duration_since(start).unwrap_or(Duration::from_secs(0));
                let mut sm_w = globalScannerMetrics.write().await;
                sm_w.operations[s_clone].fetch_add(1, Ordering::SeqCst);
                if s_clone < ScannerMetric::LastRealtime as usize {
                    sm_w.latency[s_clone].add(&duration).await;
                }
            })
        })
    }

    pub async fn time_size(s: ScannerMetric) -> TimeSizeFn {
        let start = SystemTime::now();
        let s_clone = s as usize;
        Arc::new(move |sz: u64| {
            Box::pin(async move {
                let duration = SystemTime::now().duration_since(start).unwrap_or(Duration::from_secs(0));
                let mut sm_w = globalScannerMetrics.write().await;
                sm_w.operations[s_clone].fetch_add(1, Ordering::SeqCst);
                if s_clone < ScannerMetric::LastRealtime as usize {
                    sm_w.latency[s_clone].add_size(&duration, sz).await;
                }
            })
        })
    }

    pub fn time(s: ScannerMetric) -> TimeFn {
        let start = SystemTime::now();
        let s_clone = s as usize;
        Arc::new(move || {
            Box::pin(async move {
                let duration = SystemTime::now().duration_since(start).unwrap_or(Duration::from_secs(0));
                let mut sm_w = globalScannerMetrics.write().await;
                sm_w.operations[s_clone].fetch_add(1, Ordering::SeqCst);
                if s_clone < ScannerMetric::LastRealtime as usize {
                    sm_w.latency[s_clone].add(&duration).await;
                }
            })
        })
    }
}

pub type CloseDiskFn = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;
pub fn current_path_updater(disk: &str, _initial: &str) -> (UpdateCurrentPathFn, CloseDiskFn) {
    let disk_1 = disk.to_string();
    let disk_2 = disk.to_string();
    (
        Arc::new(move |path: &str| {
            let disk_inner = disk_1.clone();
            let path = path.to_string();
            Box::pin(async move {
                globalScannerMetrics
                    .write()
                    .await
                    .current_paths
                    .insert(disk_inner, path.to_string());
            })
        }),
        Arc::new(move || {
            let disk_inner = disk_2.clone();
            Box::pin(async move {
                globalScannerMetrics.write().await.current_paths.remove(&disk_inner);
            })
        }),
    )
}
