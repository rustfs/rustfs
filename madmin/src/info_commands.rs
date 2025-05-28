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
    #[serde(rename = "rootDisk")]
    pub root_disk: bool,
    #[serde(rename = "path")]
    pub drive_path: String,
    pub healing: bool,
    pub scanning: bool,
    pub state: String,
    pub uuid: String,
    pub major: u32,
    pub minor: u32,
    pub model: Option<String>,
    #[serde(rename = "totalspace")]
    pub total_space: u64,
    #[serde(rename = "usedspace")]
    pub used_space: u64,
    #[serde(rename = "availspace")]
    pub available_space: u64,
    #[serde(rename = "readthroughput")]
    pub read_throughput: f64,
    #[serde(rename = "writethroughput")]
    pub write_throughput: f64,
    #[serde(rename = "readlatency")]
    pub read_latency: f64,
    #[serde(rename = "writelatency")]
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use std::collections::HashMap;
    use time::OffsetDateTime;

    #[test]
    fn test_item_state_to_string() {
        assert_eq!(ItemState::Offline.to_string(), ITEM_OFFLINE);
        assert_eq!(ItemState::Initializing.to_string(), ITEM_INITIALIZING);
        assert_eq!(ItemState::Online.to_string(), ITEM_ONLINE);
    }

    #[test]
    fn test_item_state_from_string_valid() {
        assert_eq!(ItemState::from_string(ITEM_OFFLINE), Some(ItemState::Offline));
        assert_eq!(ItemState::from_string(ITEM_INITIALIZING), Some(ItemState::Initializing));
        assert_eq!(ItemState::from_string(ITEM_ONLINE), Some(ItemState::Online));
    }

    #[test]
    fn test_item_state_from_string_invalid() {
        assert_eq!(ItemState::from_string("invalid"), None);
        assert_eq!(ItemState::from_string(""), None);
        assert_eq!(ItemState::from_string("OFFLINE"), None); // Case sensitive
    }

    #[test]
    fn test_disk_metrics_default() {
        let metrics = DiskMetrics::default();
        assert!(metrics.last_minute.is_empty());
        assert!(metrics.api_calls.is_empty());
        assert_eq!(metrics.total_waiting, 0);
        assert_eq!(metrics.total_errors_availability, 0);
        assert_eq!(metrics.total_errors_timeout, 0);
        assert_eq!(metrics.total_writes, 0);
        assert_eq!(metrics.total_deletes, 0);
    }

    #[test]
    fn test_disk_metrics_with_values() {
        let mut last_minute = HashMap::new();
        last_minute.insert("read".to_string(), TimedAction::default());

        let mut api_calls = HashMap::new();
        api_calls.insert("GET".to_string(), 100);
        api_calls.insert("PUT".to_string(), 50);

        let metrics = DiskMetrics {
            last_minute,
            api_calls,
            total_waiting: 5,
            total_errors_availability: 2,
            total_errors_timeout: 1,
            total_writes: 1000,
            total_deletes: 50,
        };

        assert_eq!(metrics.last_minute.len(), 1);
        assert_eq!(metrics.api_calls.len(), 2);
        assert_eq!(metrics.total_waiting, 5);
        assert_eq!(metrics.total_writes, 1000);
        assert_eq!(metrics.total_deletes, 50);
    }

    #[test]
    fn test_disk_default() {
        let disk = Disk::default();
        assert!(disk.endpoint.is_empty());
        assert!(!disk.root_disk);
        assert!(disk.drive_path.is_empty());
        assert!(!disk.healing);
        assert!(!disk.scanning);
        assert!(disk.state.is_empty());
        assert!(disk.uuid.is_empty());
        assert_eq!(disk.major, 0);
        assert_eq!(disk.minor, 0);
        assert!(disk.model.is_none());
        assert_eq!(disk.total_space, 0);
        assert_eq!(disk.used_space, 0);
        assert_eq!(disk.available_space, 0);
        assert_eq!(disk.read_throughput, 0.0);
        assert_eq!(disk.write_throughput, 0.0);
        assert_eq!(disk.read_latency, 0.0);
        assert_eq!(disk.write_latency, 0.0);
        assert_eq!(disk.utilization, 0.0);
        assert!(disk.metrics.is_none());
        assert!(disk.heal_info.is_none());
        assert_eq!(disk.used_inodes, 0);
        assert_eq!(disk.free_inodes, 0);
        assert!(!disk.local);
        assert_eq!(disk.pool_index, 0);
        assert_eq!(disk.set_index, 0);
        assert_eq!(disk.disk_index, 0);
    }

    #[test]
    fn test_disk_with_values() {
        let disk = Disk {
            endpoint: "http://localhost:9000".to_string(),
            root_disk: true,
            drive_path: "/data/disk1".to_string(),
            healing: false,
            scanning: true,
            state: "online".to_string(),
            uuid: "12345678-1234-1234-1234-123456789abc".to_string(),
            major: 8,
            minor: 1,
            model: Some("Samsung SSD 980".to_string()),
            total_space: 1000000000000,
            used_space: 500000000000,
            available_space: 500000000000,
            read_throughput: 100.5,
            write_throughput: 80.3,
            read_latency: 5.2,
            write_latency: 7.8,
            utilization: 50.0,
            metrics: Some(DiskMetrics::default()),
            heal_info: None,
            used_inodes: 1000000,
            free_inodes: 9000000,
            local: true,
            pool_index: 0,
            set_index: 1,
            disk_index: 2,
        };

        assert_eq!(disk.endpoint, "http://localhost:9000");
        assert!(disk.root_disk);
        assert_eq!(disk.drive_path, "/data/disk1");
        assert!(disk.scanning);
        assert_eq!(disk.state, "online");
        assert_eq!(disk.major, 8);
        assert_eq!(disk.minor, 1);
        assert_eq!(disk.model.unwrap(), "Samsung SSD 980");
        assert_eq!(disk.total_space, 1000000000000);
        assert_eq!(disk.utilization, 50.0);
        assert!(disk.metrics.is_some());
        assert!(disk.local);
    }

    #[test]
    fn test_healing_disk_default() {
        let healing_disk = HealingDisk::default();
        assert!(healing_disk.id.is_empty());
        assert!(healing_disk.heal_id.is_empty());
        assert!(healing_disk.pool_index.is_none());
        assert!(healing_disk.set_index.is_none());
        assert!(healing_disk.disk_index.is_none());
        assert!(healing_disk.endpoint.is_empty());
        assert!(healing_disk.path.is_empty());
        assert!(healing_disk.started.is_none());
        assert!(healing_disk.last_update.is_none());
        assert_eq!(healing_disk.retry_attempts, 0);
        assert_eq!(healing_disk.objects_total_count, 0);
        assert_eq!(healing_disk.objects_total_size, 0);
        assert_eq!(healing_disk.items_healed, 0);
        assert_eq!(healing_disk.items_failed, 0);
        assert_eq!(healing_disk.item_skipped, 0);
        assert_eq!(healing_disk.bytes_done, 0);
        assert_eq!(healing_disk.bytes_failed, 0);
        assert_eq!(healing_disk.bytes_skipped, 0);
        assert_eq!(healing_disk.objects_healed, 0);
        assert_eq!(healing_disk.objects_failed, 0);
        assert!(healing_disk.bucket.is_empty());
        assert!(healing_disk.object.is_empty());
        assert!(healing_disk.queue_buckets.is_empty());
        assert!(healing_disk.healed_buckets.is_empty());
        assert!(!healing_disk.finished);
    }

    #[test]
    fn test_healing_disk_with_values() {
        let now = OffsetDateTime::now_utc();
        let system_time = std::time::SystemTime::now();

        let healing_disk = HealingDisk {
            id: "heal-001".to_string(),
            heal_id: "heal-session-123".to_string(),
            pool_index: Some(0),
            set_index: Some(1),
            disk_index: Some(2),
            endpoint: "http://node1:9000".to_string(),
            path: "/data/disk1".to_string(),
            started: Some(now),
            last_update: Some(system_time),
            retry_attempts: 3,
            objects_total_count: 10000,
            objects_total_size: 1000000000,
            items_healed: 8000,
            items_failed: 100,
            item_skipped: 50,
            bytes_done: 800000000,
            bytes_failed: 10000000,
            bytes_skipped: 5000000,
            objects_healed: 7900,
            objects_failed: 100,
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            queue_buckets: vec!["bucket1".to_string(), "bucket2".to_string()],
            healed_buckets: vec!["bucket3".to_string()],
            finished: false,
        };

        assert_eq!(healing_disk.id, "heal-001");
        assert_eq!(healing_disk.heal_id, "heal-session-123");
        assert_eq!(healing_disk.pool_index.unwrap(), 0);
        assert_eq!(healing_disk.set_index.unwrap(), 1);
        assert_eq!(healing_disk.disk_index.unwrap(), 2);
        assert_eq!(healing_disk.retry_attempts, 3);
        assert_eq!(healing_disk.objects_total_count, 10000);
        assert_eq!(healing_disk.items_healed, 8000);
        assert_eq!(healing_disk.queue_buckets.len(), 2);
        assert_eq!(healing_disk.healed_buckets.len(), 1);
        assert!(!healing_disk.finished);
    }

    #[test]
    fn test_backend_byte_default() {
        let backend = BackendByte::default();
        assert!(matches!(backend, BackendByte::Unknown));
    }

    #[test]
    fn test_backend_byte_variants() {
        let unknown = BackendByte::Unknown;
        let fs = BackendByte::FS;
        let erasure = BackendByte::Erasure;

        // Test that all variants can be created
        assert!(matches!(unknown, BackendByte::Unknown));
        assert!(matches!(fs, BackendByte::FS));
        assert!(matches!(erasure, BackendByte::Erasure));
    }

    #[test]
    fn test_storage_info_creation() {
        let storage_info = StorageInfo {
            disks: vec![
                Disk {
                    endpoint: "node1:9000".to_string(),
                    state: "online".to_string(),
                    ..Default::default()
                },
                Disk {
                    endpoint: "node2:9000".to_string(),
                    state: "offline".to_string(),
                    ..Default::default()
                },
            ],
            backend: BackendInfo::default(),
        };

        assert_eq!(storage_info.disks.len(), 2);
        assert_eq!(storage_info.disks[0].endpoint, "node1:9000");
        assert_eq!(storage_info.disks[1].state, "offline");
    }

    #[test]
    fn test_backend_disks_new() {
        let backend_disks = BackendDisks::new();
        assert!(backend_disks.0.is_empty());
    }

    #[test]
    fn test_backend_disks_sum() {
        let mut backend_disks = BackendDisks::new();
        backend_disks.0.insert("pool1".to_string(), 4);
        backend_disks.0.insert("pool2".to_string(), 6);
        backend_disks.0.insert("pool3".to_string(), 2);

        assert_eq!(backend_disks.sum(), 12);
    }

    #[test]
    fn test_backend_disks_sum_empty() {
        let backend_disks = BackendDisks::new();
        assert_eq!(backend_disks.sum(), 0);
    }

    #[test]
    fn test_backend_info_default() {
        let backend_info = BackendInfo::default();
        assert!(matches!(backend_info.backend_type, BackendByte::Unknown));
        assert_eq!(backend_info.online_disks.sum(), 0);
        assert_eq!(backend_info.offline_disks.sum(), 0);
        assert!(backend_info.standard_sc_data.is_empty());
        assert!(backend_info.standard_sc_parities.is_empty());
        assert!(backend_info.standard_sc_parity.is_none());
        assert!(backend_info.rr_sc_data.is_empty());
        assert!(backend_info.rr_sc_parities.is_empty());
        assert!(backend_info.rr_sc_parity.is_none());
        assert!(backend_info.total_sets.is_empty());
        assert!(backend_info.drives_per_set.is_empty());
    }

    #[test]
    fn test_backend_info_with_values() {
        let mut online_disks = BackendDisks::new();
        online_disks.0.insert("set1".to_string(), 4);
        online_disks.0.insert("set2".to_string(), 4);

        let mut offline_disks = BackendDisks::new();
        offline_disks.0.insert("set1".to_string(), 0);
        offline_disks.0.insert("set2".to_string(), 1);

        let backend_info = BackendInfo {
            backend_type: BackendByte::Erasure,
            online_disks,
            offline_disks,
            standard_sc_data: vec![4, 4],
            standard_sc_parities: vec![2, 2],
            standard_sc_parity: Some(2),
            rr_sc_data: vec![2, 2],
            rr_sc_parities: vec![1, 1],
            rr_sc_parity: Some(1),
            total_sets: vec![2],
            drives_per_set: vec![6, 6],
        };

        assert!(matches!(backend_info.backend_type, BackendByte::Erasure));
        assert_eq!(backend_info.online_disks.sum(), 8);
        assert_eq!(backend_info.offline_disks.sum(), 1);
        assert_eq!(backend_info.standard_sc_data.len(), 2);
        assert_eq!(backend_info.standard_sc_parity.unwrap(), 2);
        assert_eq!(backend_info.total_sets.len(), 1);
        assert_eq!(backend_info.drives_per_set.len(), 2);
    }

    #[test]
    fn test_mem_stats_default() {
        let mem_stats = MemStats::default();
        assert_eq!(mem_stats.alloc, 0);
        assert_eq!(mem_stats.total_alloc, 0);
        assert_eq!(mem_stats.mallocs, 0);
        assert_eq!(mem_stats.frees, 0);
        assert_eq!(mem_stats.heap_alloc, 0);
    }

    #[test]
    fn test_mem_stats_with_values() {
        let mem_stats = MemStats {
            alloc: 1024000,
            total_alloc: 5120000,
            mallocs: 1000,
            frees: 800,
            heap_alloc: 2048000,
        };

        assert_eq!(mem_stats.alloc, 1024000);
        assert_eq!(mem_stats.total_alloc, 5120000);
        assert_eq!(mem_stats.mallocs, 1000);
        assert_eq!(mem_stats.frees, 800);
        assert_eq!(mem_stats.heap_alloc, 2048000);
    }

    #[test]
    fn test_server_properties_default() {
        let server_props = ServerProperties::default();
        assert!(server_props.state.is_empty());
        assert!(server_props.endpoint.is_empty());
        assert!(server_props.scheme.is_empty());
        assert_eq!(server_props.uptime, 0);
        assert!(server_props.version.is_empty());
        assert!(server_props.commit_id.is_empty());
        assert!(server_props.network.is_empty());
        assert!(server_props.disks.is_empty());
        assert_eq!(server_props.pool_number, 0);
        assert!(server_props.pool_numbers.is_empty());
        assert_eq!(server_props.mem_stats.alloc, 0);
        assert_eq!(server_props.max_procs, 0);
        assert_eq!(server_props.num_cpu, 0);
        assert!(server_props.runtime_version.is_empty());
        assert!(server_props.rustfs_env_vars.is_empty());
    }

    #[test]
    fn test_server_properties_with_values() {
        let mut network = HashMap::new();
        network.insert("interface".to_string(), "eth0".to_string());
        network.insert("ip".to_string(), "192.168.1.100".to_string());

        let mut env_vars = HashMap::new();
        env_vars.insert("RUSTFS_ROOT_USER".to_string(), "admin".to_string());
        env_vars.insert("RUSTFS_ROOT_PASSWORD".to_string(), "password".to_string());

        let server_props = ServerProperties {
            state: "online".to_string(),
            endpoint: "http://localhost:9000".to_string(),
            scheme: "http".to_string(),
            uptime: 3600,
            version: "1.0.0".to_string(),
            commit_id: "abc123def456".to_string(),
            network,
            disks: vec![Disk::default()],
            pool_number: 1,
            pool_numbers: vec![0, 1],
            mem_stats: MemStats {
                alloc: 1024000,
                total_alloc: 5120000,
                mallocs: 1000,
                frees: 800,
                heap_alloc: 2048000,
            },
            max_procs: 8,
            num_cpu: 4,
            runtime_version: "1.70.0".to_string(),
            rustfs_env_vars: env_vars,
        };

        assert_eq!(server_props.state, "online");
        assert_eq!(server_props.endpoint, "http://localhost:9000");
        assert_eq!(server_props.uptime, 3600);
        assert_eq!(server_props.version, "1.0.0");
        assert_eq!(server_props.network.len(), 2);
        assert_eq!(server_props.disks.len(), 1);
        assert_eq!(server_props.pool_number, 1);
        assert_eq!(server_props.pool_numbers.len(), 2);
        assert_eq!(server_props.mem_stats.alloc, 1024000);
        assert_eq!(server_props.max_procs, 8);
        assert_eq!(server_props.num_cpu, 4);
        assert_eq!(server_props.rustfs_env_vars.len(), 2);
    }

    #[test]
    fn test_kms_default() {
        let kms = Kms::default();
        assert!(kms.status.is_none());
        assert!(kms.encrypt.is_none());
        assert!(kms.decrypt.is_none());
        assert!(kms.endpoint.is_none());
        assert!(kms.version.is_none());
    }

    #[test]
    fn test_kms_with_values() {
        let kms = Kms {
            status: Some("enabled".to_string()),
            encrypt: Some("AES256".to_string()),
            decrypt: Some("AES256".to_string()),
            endpoint: Some("https://kms.example.com".to_string()),
            version: Some("1.0".to_string()),
        };

        assert_eq!(kms.status.unwrap(), "enabled");
        assert_eq!(kms.encrypt.unwrap(), "AES256");
        assert_eq!(kms.decrypt.unwrap(), "AES256");
        assert_eq!(kms.endpoint.unwrap(), "https://kms.example.com");
        assert_eq!(kms.version.unwrap(), "1.0");
    }

    #[test]
    fn test_ldap_default() {
        let ldap = Ldap::default();
        assert!(ldap.status.is_none());
    }

    #[test]
    fn test_ldap_with_values() {
        let ldap = Ldap {
            status: Some("enabled".to_string()),
        };

        assert_eq!(ldap.status.unwrap(), "enabled");
    }

    #[test]
    fn test_status_default() {
        let status = Status::default();
        assert!(status.status.is_none());
    }

    #[test]
    fn test_status_with_values() {
        let status = Status {
            status: Some("active".to_string()),
        };

        assert_eq!(status.status.unwrap(), "active");
    }

    #[test]
    fn test_services_default() {
        let services = Services::default();
        assert!(services.kms.is_none());
        assert!(services.kms_status.is_none());
        assert!(services.ldap.is_none());
        assert!(services.logger.is_none());
        assert!(services.audit.is_none());
        assert!(services.notifications.is_none());
    }

    #[test]
    fn test_services_with_values() {
        let services = Services {
            kms: Some(Kms::default()),
            kms_status: Some(vec![Kms::default()]),
            ldap: Some(Ldap::default()),
            logger: Some(vec![HashMap::new()]),
            audit: Some(vec![HashMap::new()]),
            notifications: Some(vec![HashMap::new()]),
        };

        assert!(services.kms.is_some());
        assert_eq!(services.kms_status.unwrap().len(), 1);
        assert!(services.ldap.is_some());
        assert_eq!(services.logger.unwrap().len(), 1);
        assert_eq!(services.audit.unwrap().len(), 1);
        assert_eq!(services.notifications.unwrap().len(), 1);
    }

    #[test]
    fn test_buckets_default() {
        let buckets = Buckets::default();
        assert_eq!(buckets.count, 0);
        assert!(buckets.error.is_none());
    }

    #[test]
    fn test_buckets_with_values() {
        let buckets = Buckets {
            count: 10,
            error: Some("Access denied".to_string()),
        };

        assert_eq!(buckets.count, 10);
        assert_eq!(buckets.error.unwrap(), "Access denied");
    }

    #[test]
    fn test_objects_default() {
        let objects = Objects::default();
        assert_eq!(objects.count, 0);
        assert!(objects.error.is_none());
    }

    #[test]
    fn test_versions_default() {
        let versions = Versions::default();
        assert_eq!(versions.count, 0);
        assert!(versions.error.is_none());
    }

    #[test]
    fn test_delete_markers_default() {
        let delete_markers = DeleteMarkers::default();
        assert_eq!(delete_markers.count, 0);
        assert!(delete_markers.error.is_none());
    }

    #[test]
    fn test_usage_default() {
        let usage = Usage::default();
        assert_eq!(usage.size, 0);
        assert!(usage.error.is_none());
    }

    #[test]
    fn test_erasure_set_info_default() {
        let erasure_set = ErasureSetInfo::default();
        assert_eq!(erasure_set.id, 0);
        assert_eq!(erasure_set.raw_usage, 0);
        assert_eq!(erasure_set.raw_capacity, 0);
        assert_eq!(erasure_set.usage, 0);
        assert_eq!(erasure_set.objects_count, 0);
        assert_eq!(erasure_set.versions_count, 0);
        assert_eq!(erasure_set.delete_markers_count, 0);
        assert_eq!(erasure_set.heal_disks, 0);
    }

    #[test]
    fn test_erasure_set_info_with_values() {
        let erasure_set = ErasureSetInfo {
            id: 1,
            raw_usage: 1000000000,
            raw_capacity: 2000000000,
            usage: 800000000,
            objects_count: 10000,
            versions_count: 15000,
            delete_markers_count: 500,
            heal_disks: 2,
        };

        assert_eq!(erasure_set.id, 1);
        assert_eq!(erasure_set.raw_usage, 1000000000);
        assert_eq!(erasure_set.raw_capacity, 2000000000);
        assert_eq!(erasure_set.usage, 800000000);
        assert_eq!(erasure_set.objects_count, 10000);
        assert_eq!(erasure_set.versions_count, 15000);
        assert_eq!(erasure_set.delete_markers_count, 500);
        assert_eq!(erasure_set.heal_disks, 2);
    }

    #[test]
    fn test_backend_type_default() {
        let backend_type = BackendType::default();
        assert!(matches!(backend_type, BackendType::FsType));
    }

    #[test]
    fn test_backend_type_variants() {
        let fs_type = BackendType::FsType;
        let erasure_type = BackendType::ErasureType;

        assert!(matches!(fs_type, BackendType::FsType));
        assert!(matches!(erasure_type, BackendType::ErasureType));
    }

    #[test]
    fn test_fs_backend_creation() {
        let fs_backend = FSBackend {
            backend_type: BackendType::FsType,
        };

        assert!(matches!(fs_backend.backend_type, BackendType::FsType));
    }

    #[test]
    fn test_erasure_backend_default() {
        let erasure_backend = ErasureBackend::default();
        assert!(matches!(erasure_backend.backend_type, BackendType::FsType));
        assert_eq!(erasure_backend.online_disks, 0);
        assert_eq!(erasure_backend.offline_disks, 0);
        assert!(erasure_backend.standard_sc_parity.is_none());
        assert!(erasure_backend.rr_sc_parity.is_none());
        assert!(erasure_backend.total_sets.is_empty());
        assert!(erasure_backend.drives_per_set.is_empty());
    }

    #[test]
    fn test_erasure_backend_with_values() {
        let erasure_backend = ErasureBackend {
            backend_type: BackendType::ErasureType,
            online_disks: 8,
            offline_disks: 0,
            standard_sc_parity: Some(2),
            rr_sc_parity: Some(1),
            total_sets: vec![2],
            drives_per_set: vec![4, 4],
        };

        assert!(matches!(erasure_backend.backend_type, BackendType::ErasureType));
        assert_eq!(erasure_backend.online_disks, 8);
        assert_eq!(erasure_backend.offline_disks, 0);
        assert_eq!(erasure_backend.standard_sc_parity.unwrap(), 2);
        assert_eq!(erasure_backend.rr_sc_parity.unwrap(), 1);
        assert_eq!(erasure_backend.total_sets.len(), 1);
        assert_eq!(erasure_backend.drives_per_set.len(), 2);
    }

    #[test]
    fn test_info_message_creation() {
        let mut pools = HashMap::new();
        let mut pool_sets = HashMap::new();
        pool_sets.insert(0, ErasureSetInfo::default());
        pools.insert(0, pool_sets);

        let info_message = InfoMessage {
            mode: Some("distributed".to_string()),
            domain: Some(vec!["example.com".to_string()]),
            region: Some("us-east-1".to_string()),
            sqs_arn: Some(vec!["arn:aws:sqs:us-east-1:123456789012:test-queue".to_string()]),
            deployment_id: Some("deployment-123".to_string()),
            buckets: Some(Buckets { count: 5, error: None }),
            objects: Some(Objects {
                count: 1000,
                error: None,
            }),
            versions: Some(Versions {
                count: 1200,
                error: None,
            }),
            delete_markers: Some(DeleteMarkers { count: 50, error: None }),
            usage: Some(Usage {
                size: 1000000000,
                error: None,
            }),
            services: Some(Services::default()),
            backend: Some(ErasureBackend::default()),
            servers: Some(vec![ServerProperties::default()]),
            pools: Some(pools),
        };

        assert_eq!(info_message.mode.unwrap(), "distributed");
        assert_eq!(info_message.domain.unwrap().len(), 1);
        assert_eq!(info_message.region.unwrap(), "us-east-1");
        assert_eq!(info_message.sqs_arn.unwrap().len(), 1);
        assert_eq!(info_message.deployment_id.unwrap(), "deployment-123");
        assert_eq!(info_message.buckets.unwrap().count, 5);
        assert_eq!(info_message.objects.unwrap().count, 1000);
        assert_eq!(info_message.versions.unwrap().count, 1200);
        assert_eq!(info_message.delete_markers.unwrap().count, 50);
        assert_eq!(info_message.usage.unwrap().size, 1000000000);
        assert!(info_message.services.is_some());
        assert_eq!(info_message.servers.unwrap().len(), 1);
        assert_eq!(info_message.pools.unwrap().len(), 1);
    }

    #[test]
    fn test_serialization_deserialization() {
        let disk = Disk {
            endpoint: "http://localhost:9000".to_string(),
            state: "online".to_string(),
            total_space: 1000000000,
            used_space: 500000000,
            ..Default::default()
        };

        let json = serde_json::to_string(&disk).unwrap();
        let deserialized: Disk = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.endpoint, "http://localhost:9000");
        assert_eq!(deserialized.state, "online");
        assert_eq!(deserialized.total_space, 1000000000);
        assert_eq!(deserialized.used_space, 500000000);
    }

    #[test]
    fn test_debug_format_all_structures() {
        let item_state = ItemState::Online;
        let disk_metrics = DiskMetrics::default();
        let disk = Disk::default();
        let healing_disk = HealingDisk::default();
        let backend_byte = BackendByte::default();
        let storage_info = StorageInfo {
            disks: vec![],
            backend: BackendInfo::default(),
        };
        let backend_info = BackendInfo::default();
        let mem_stats = MemStats::default();
        let server_props = ServerProperties::default();

        // Test that all structures can be formatted with Debug
        assert!(!format!("{:?}", item_state).is_empty());
        assert!(!format!("{:?}", disk_metrics).is_empty());
        assert!(!format!("{:?}", disk).is_empty());
        assert!(!format!("{:?}", healing_disk).is_empty());
        assert!(!format!("{:?}", backend_byte).is_empty());
        assert!(!format!("{:?}", storage_info).is_empty());
        assert!(!format!("{:?}", backend_info).is_empty());
        assert!(!format!("{:?}", mem_stats).is_empty());
        assert!(!format!("{:?}", server_props).is_empty());
    }

    #[test]
    fn test_memory_efficiency() {
        // Test that structures don't use excessive memory
        assert!(std::mem::size_of::<ItemState>() < 100);
        assert!(std::mem::size_of::<BackendByte>() < 100);
        assert!(std::mem::size_of::<BackendType>() < 100);
        assert!(std::mem::size_of::<MemStats>() < 1000);
        assert!(std::mem::size_of::<Buckets>() < 1000);
        assert!(std::mem::size_of::<Objects>() < 1000);
        assert!(std::mem::size_of::<Usage>() < 1000);
    }

    #[test]
    fn test_constants() {
        assert_eq!(ITEM_OFFLINE, "offline");
        assert_eq!(ITEM_INITIALIZING, "initializing");
        assert_eq!(ITEM_ONLINE, "online");
    }
}
