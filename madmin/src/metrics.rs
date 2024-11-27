use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TimedAction {
    count: u64,
    acc_time: u64,
    bytes: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DiskIOStats {
    read_ios: u64,
    read_merges: u64,
    read_sectors: u64,
    read_ticks: u64,
    write_ios: u64,
    write_merges: u64,
    write_sectors: u64,
    write_ticks: u64,
    current_ios: u64,
    total_ticks: u64,
    req_ticks: u64,
    discard_ios: u64,
    discard_merges: u64,
    discard_sectors: u64,
    discard_ticks: u64,
    flush_ios: u64,
    flush_ticks: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DiskMetric {
    collected_at: u64,
    n_disks: usize,
    offline: usize,
    healing: usize,
    life_time_ops: HashMap<String, u64>,
    last_minute: HashMap<String, TimedAction>,
    io_stats: DiskIOStats,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Metrics {}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct RealtimeMetrics {
    errors: Vec<String>,
    hosts: Vec<String>,
    aggregated: Metrics,
    by_host: HashMap<String, Metrics>,
    by_disk: HashMap<String, DiskMetric>,
    finally: bool,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CollectMetricsOpts {
    hosts: HashSet<String>,
    disks: HashSet<String>,
    job_id: String,
    dep_id: String,
}

pub type MetricType = u64;

pub fn collect_local_metrics(_types: MetricType, _opts: &CollectMetricsOpts) -> RealtimeMetrics {
    RealtimeMetrics::default()
}
