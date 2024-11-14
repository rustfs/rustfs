use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::SystemTime,
};

use lazy_static::lazy_static;
use tokio::sync::RwLock;

use super::data_scanner::CurrentScannerCycle;

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
}

impl ScannerMetrics {
    pub fn new() -> Self {
        Self {
            operations: (0..ScannerMetric::Last as usize).map(|_| AtomicU32::new(0)).collect(),
            cycle_info: RwLock::new(None),
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
