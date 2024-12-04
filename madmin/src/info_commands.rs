use std::{collections::HashMap, time::SystemTime};

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::metrics::TimedAction;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ItemState {
    Offline,
    Initializing,
    Online,
}

impl ItemState {
    pub fn to_string(&self) -> &str {
        match self {
            ItemState::Offline => "offline",
            ItemState::Initializing => "initializing",
            ItemState::Online => "online",
        }
    }

    pub fn from_string(s: &str) -> Option<ItemState> {
        match s {
            "offline" => Some(ItemState::Offline),
            "initializing" => Some(ItemState::Initializing),
            "online" => Some(ItemState::Online),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiskMetrics {
    pub last_minute: HashMap<String, TimedAction>,
    pub api_calls: HashMap<String, u64>,
    pub total_waiting: u32,
    pub total_errors_availability: u64,
    pub total_errors_timeout: u64,
    pub total_writes: u64,
    pub total_deletes: u64,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Disk {
    pub endpoint: String,
    pub root_disk: bool,
    pub drive_path: String,
    pub healing: bool,
    pub scanning: bool,
    pub state: String,
    pub uuid: String,
    pub major: u32,
    pub minor: u32,
    pub model: Option<String>,
    pub total_space: u64,
    pub used_space: u64,
    pub available_space: u64,
    pub read_throughput: f64,
    pub write_throughput: f64,
    pub read_latency: f64,
    pub write_latency: f64,
    pub utilization: f64,
    pub metrics: Option<DiskMetrics>,
    pub heal_info: Option<HealingDisk>,
    pub used_inodes: u64,
    pub free_inodes: u64,
    pub local: bool,
    pub pool_index: i32,
    pub set_index: i32,
    pub disk_index: i32,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HealingDisk {
    pub id: String,
    pub heal_id: String,
    pub pool_index: Option<usize>,
    pub set_index: Option<usize>,
    pub disk_index: Option<usize>,
    pub endpoint: String,
    pub path: String,
    pub started: Option<OffsetDateTime>,
    pub last_update: Option<SystemTime>,
    pub retry_attempts: u64,
    pub objects_total_count: u64,
    pub objects_total_size: u64,
    pub items_healed: u64,
    pub items_failed: u64,
    pub item_skipped: u64,
    pub bytes_done: u64,
    pub bytes_failed: u64,
    pub bytes_skipped: u64,
    pub objects_healed: u64,
    pub objects_failed: u64,
    pub bucket: String,
    pub object: String,
    pub queue_buckets: Vec<String>,
    pub healed_buckets: Vec<String>,
    pub finished: bool,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub enum BackendByte {
    #[default]
    Unknown,
    FS,
    Erasure,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct StorageInfo {
    pub disks: Vec<Disk>,
    pub backend: BackendInfo,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BackendDisks(HashMap<String, usize>);

impl BackendDisks {
    pub fn new() -> Self {
        Self(HashMap::new())
    }
    pub fn sum(&self) -> usize {
        self.0.values().sum()
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase", default)]
pub struct BackendInfo {
    pub backend_type: BackendByte,
    pub online_disks: BackendDisks,
    pub offline_disks: BackendDisks,
    #[serde(rename = "StandardSCData")]
    pub standard_sc_data: Vec<usize>,
    #[serde(rename = "StandardSCParities")]
    pub standard_sc_parities: Vec<usize>,
    #[serde(rename = "StandardSCParity")]
    pub standard_sc_parity: Option<usize>,
    #[serde(rename = "RRSCData")]
    pub rr_sc_data: Vec<usize>,
    #[serde(rename = "RRSCParities")]
    pub rr_sc_parities: Vec<usize>,
    #[serde(rename = "RRSCParity")]
    pub rr_sc_parity: Option<usize>,
    pub total_sets: Vec<usize>,
    pub drives_per_set: Vec<usize>,
}
