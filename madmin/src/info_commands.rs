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
pub struct BackendDisks(pub HashMap<String, usize>);

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

pub const ITEM_OFFLINE: &str = "offline";
pub const ITEM_INITIALIZING: &str = "initializing";
pub const ITEM_ONLINE: &str = "online";

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MemStats {
    pub alloc: u64,
    pub total_alloc: u64,
    pub mallocs: u64,
    pub frees: u64,
    pub heap_alloc: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ServerProperties {
    pub state: String,
    pub endpoint: String,
    pub scheme: String,
    pub uptime: u64,
    pub version: String,
    #[serde(rename = "commitID")]
    pub commit_id: String,
    pub network: HashMap<String, String>,
    #[serde(rename = "drives")]
    pub disks: Vec<Disk>,
    #[serde(rename = "poolNumber")]
    pub pool_number: i32,
    #[serde(rename = "poolNumbers")]
    pub pool_numbers: Vec<i32>,
    pub mem_stats: MemStats,
    pub max_procs: u64,
    pub num_cpu: u64,
    pub runtime_version: String,
    pub rustfs_env_vars: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Kms {
    pub status: Option<String>,
    pub encrypt: Option<String>,
    pub decrypt: Option<String>,
    pub endpoint: Option<String>,
    pub version: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Ldap {
    pub status: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Status {
    pub status: Option<String>,
}

pub type Audit = HashMap<String, Status>;

pub type Logger = HashMap<String, Status>;

pub type TargetIDStatus = HashMap<String, Status>;

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct Services {
    pub kms: Option<Kms>, // deprecated july 2023
    #[serde(rename = "kmsStatus")]
    pub kms_status: Option<Vec<Kms>>,
    pub ldap: Option<Ldap>,
    pub logger: Option<Vec<Logger>>,
    pub audit: Option<Vec<Audit>>,
    pub notifications: Option<Vec<HashMap<String, Vec<TargetIDStatus>>>>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Buckets {
    pub count: u64,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Objects {
    pub count: u64,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Versions {
    pub count: u64,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct DeleteMarkers {
    pub count: u64,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Usage {
    pub size: u64,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ErasureSetInfo {
    pub id: i32,
    #[serde(rename = "rawUsage")]
    pub raw_usage: u64,
    #[serde(rename = "rawCapacity")]
    pub raw_capacity: u64,
    pub usage: u64,
    #[serde(rename = "objectsCount")]
    pub objects_count: u64,
    #[serde(rename = "versionsCount")]
    pub versions_count: u64,
    #[serde(rename = "deleteMarkersCount")]
    pub delete_markers_count: u64,
    #[serde(rename = "healDisks")]
    pub heal_disks: i32,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub enum BackendType {
    #[default]
    #[serde(rename = "FS")]
    FsType,
    #[serde(rename = "Erasure")]
    ErasureType,
}

#[derive(Serialize, Deserialize)]
pub struct FSBackend {
    #[serde(rename = "backendType")]
    pub backend_type: BackendType,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ErasureBackend {
    #[serde(rename = "backendType")]
    pub backend_type: BackendType,
    #[serde(rename = "onlineDisks")]
    pub online_disks: usize,
    #[serde(rename = "offlineDisks")]
    pub offline_disks: usize,
    #[serde(rename = "standardSCParity")]
    pub standard_sc_parity: Option<usize>,
    #[serde(rename = "rrSCParity")]
    pub rr_sc_parity: Option<usize>,
    #[serde(rename = "totalSets")]
    pub total_sets: Vec<usize>,
    #[serde(rename = "totalDrivesPerSet")]
    pub drives_per_set: Vec<usize>,
}

#[derive(Serialize, Deserialize)]
pub struct InfoMessage {
    pub mode: Option<String>,
    pub domain: Option<Vec<String>>,
    pub region: Option<String>,
    #[serde(rename = "sqsARN")]
    pub sqs_arn: Option<Vec<String>>,
    #[serde(rename = "deploymentID")]
    pub deployment_id: Option<String>,
    pub buckets: Option<Buckets>,
    pub objects: Option<Objects>,
    pub versions: Option<Versions>,
    #[serde(rename = "deletemarkers")]
    pub delete_markers: Option<DeleteMarkers>,
    pub usage: Option<Usage>,
    pub services: Option<Services>,
    pub backend: Option<ErasureBackend>,
    pub servers: Option<Vec<ServerProperties>>,
    pub pools: Option<std::collections::HashMap<i32, std::collections::HashMap<i32, ErasureSetInfo>>>,
}
