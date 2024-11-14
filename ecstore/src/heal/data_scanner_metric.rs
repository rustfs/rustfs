use lazy_static::lazy_static;
use std::future::Future;
use std::pin::Pin;
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

pub struct ScannerMetrics {
    operations: Vec<AtomicU32>,
    cycle_info: RwLock<Option<CurrentScannerCycle>>,
    current_paths: HashMap<String, String>,
}

impl ScannerMetrics {
    pub fn new() -> Self {
        Self {
            operations: (0..ScannerMetric::Last as usize).map(|_| AtomicU32::new(0)).collect(),
            cycle_info: RwLock::new(None),
            current_paths: HashMap::new(),
        }
    }

    pub fn log(&mut self, s: ScannerMetric, _paths: &[String], _custom: &HashMap<String, String>, _start_time: SystemTime) {
        // let duration = start_time.duration_since(start_time);
        self.operations[s.clone() as usize].fetch_add(1, Ordering::SeqCst);
        // Dodo
    }

    pub async fn set_cycle(&mut self, c: Option<CurrentScannerCycle>) {
        *self.cycle_info.write().await = c;
    }
}

pub type CloseDiskFn = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>;
pub fn current_path_updater(disk: &str, initial: &str) -> (UpdateCurrentPathFn, CloseDiskFn) {
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
