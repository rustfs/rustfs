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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NodeCommon {
    pub addr: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Cpu {
    pub vendor_id: String,
    pub family: String,
    pub model: String,
    pub stepping: i32,
    pub physical_id: String,
    pub model_name: String,
    pub mhz: f64,
    pub cache_size: i32,
    pub flags: Vec<String>,
    pub microcode: String,
    pub cores: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CpuFreqStats {
    name: String,
    cpuinfo_current_frequency: Option<u64>,
    cpuinfo_minimum_frequency: Option<u64>,
    cpuinfo_maximum_frequency: Option<u64>,
    cpuinfo_transition_latency: Option<u64>,
    scaling_current_frequency: Option<u64>,
    scaling_minimum_frequency: Option<u64>,
    scaling_maximum_frequency: Option<u64>,
    available_governors: String,
    driver: String,
    governor: String,
    related_cpus: String,
    set_speed: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Cpus {
    node_common: NodeCommon,
    cpus: Vec<Cpu>,
    cpu_freq_stats: Vec<CpuFreqStats>,
}

pub fn get_cpus() -> Cpus {
    // todo
    Cpus::default()
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Partition {
    pub error: String,
    device: String,
    model: String,
    revision: String,
    mountpoint: String,
    fs_type: String,
    mount_options: String,
    space_total: u64,
    space_free: u64,
    inode_total: u64,
    inode_free: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Partitions {
    node_common: NodeCommon,
    partitions: Vec<Partition>,
}

pub fn get_partitions() -> Partitions {
    Partitions::default()
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct OsInfo {
    node_common: NodeCommon,
}

pub fn get_os_info() -> OsInfo {
    OsInfo::default()
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ProcInfo {
    node_common: NodeCommon,
    pid: i32,
    is_background: bool,
    cpu_percent: f64,
    children_pids: Vec<i32>,
    cmd_line: String,
    num_connections: usize,
    create_time: u64,
    cwd: String,
    exec_path: String,
    gids: Vec<i32>,
    // io_counters:
    is_running: bool,
    // mem_info:
    // mem_maps:
    mem_percent: f32,
    name: String,
    nice: i32,
    //num_ctx_switches:
    num_fds: i32,
    num_threads: i32,
    // page_faults:
    ppid: i32,
    status: String,
    tgid: i32,
    uids: Vec<i32>,
    username: String,
}

pub fn get_proc_info(_addr: &str) -> ProcInfo {
    ProcInfo::default()
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SysService {
    name: String,
    status: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SysServices {
    node_common: NodeCommon,
    services: Vec<SysService>,
}

pub fn get_sys_services(_add: &str) -> SysServices {
    SysServices::default()
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SysConfig {
    node_common: NodeCommon,
    config: HashMap<String, String>,
}

pub fn get_sys_config(_addr: &str) -> SysConfig {
    SysConfig::default()
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SysErrors {
    node_common: NodeCommon,
    errors: Vec<String>,
}

pub fn get_sys_errors(_add: &str) -> SysErrors {
    SysErrors::default()
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MemInfo {
    node_common: NodeCommon,
    #[serde(skip_serializing_if = "Option::is_none")]
    total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    used: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    free: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    available: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    shared: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    buffers: Option<u64>,
    #[serde(rename = "swap_space_total", skip_serializing_if = "Option::is_none")]
    swap_space_total: Option<u64>,
    #[serde(rename = "swap_space_free", skip_serializing_if = "Option::is_none")]
    swap_space_free: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u64>,
}

pub fn get_mem_info(_addr: &str) -> MemInfo {
    MemInfo::default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_node_common_creation() {
        let node = NodeCommon::default();
        assert!(node.addr.is_empty(), "Default addr should be empty");
        assert!(node.error.is_none(), "Default error should be None");
    }

    #[test]
    fn test_node_common_with_values() {
        let node = NodeCommon {
            addr: "127.0.0.1:9000".to_string(),
            error: Some("Connection failed".to_string()),
        };
        assert_eq!(node.addr, "127.0.0.1:9000");
        assert_eq!(node.error.unwrap(), "Connection failed");
    }

    #[test]
    fn test_node_common_serialization() {
        let node = NodeCommon {
            addr: "localhost:8080".to_string(),
            error: None,
        };

        let json = serde_json::to_string(&node).unwrap();
        assert!(json.contains("localhost:8080"));
        assert!(!json.contains("error"), "None error should be skipped in serialization");
    }

    #[test]
    fn test_node_common_deserialization() {
        let json = r#"{"addr":"test.example.com:9000","error":"Test error"}"#;
        let node: NodeCommon = serde_json::from_str(json).unwrap();

        assert_eq!(node.addr, "test.example.com:9000");
        assert_eq!(node.error.unwrap(), "Test error");
    }

    #[test]
    fn test_cpu_default() {
        let cpu = Cpu::default();
        assert!(cpu.vendor_id.is_empty());
        assert!(cpu.family.is_empty());
        assert!(cpu.model.is_empty());
        assert_eq!(cpu.stepping, 0);
        assert_eq!(cpu.mhz, 0.0);
        assert_eq!(cpu.cache_size, 0);
        assert!(cpu.flags.is_empty());
        assert_eq!(cpu.cores, 0);
    }

    #[test]
    fn test_cpu_with_values() {
        let cpu = Cpu {
            vendor_id: "GenuineIntel".to_string(),
            family: "6".to_string(),
            model: "142".to_string(),
            stepping: 12,
            physical_id: "0".to_string(),
            model_name: "Intel(R) Core(TM) i7-8565U CPU @ 1.80GHz".to_string(),
            mhz: 1800.0,
            cache_size: 8192,
            flags: vec!["fpu".to_string(), "vme".to_string(), "de".to_string()],
            microcode: "0xf0".to_string(),
            cores: 4,
        };

        assert_eq!(cpu.vendor_id, "GenuineIntel");
        assert_eq!(cpu.cores, 4);
        assert_eq!(cpu.flags.len(), 3);
        assert!(cpu.flags.contains(&"fpu".to_string()));
    }

    #[test]
    fn test_cpu_serialization() {
        let cpu = Cpu {
            vendor_id: "AMD".to_string(),
            model_name: "AMD Ryzen 7".to_string(),
            cores: 8,
            ..Default::default()
        };

        let json = serde_json::to_string(&cpu).unwrap();
        assert!(json.contains("AMD"));
        assert!(json.contains("AMD Ryzen 7"));
        assert!(json.contains("8"));
    }

    #[test]
    fn test_cpu_freq_stats_default() {
        let stats = CpuFreqStats::default();
        assert!(stats.name.is_empty());
        assert!(stats.cpuinfo_current_frequency.is_none());
        assert!(stats.available_governors.is_empty());
        assert!(stats.driver.is_empty());
    }

    #[test]
    fn test_cpus_structure() {
        let cpus = Cpus {
            node_common: NodeCommon {
                addr: "node1".to_string(),
                error: None,
            },
            cpus: vec![Cpu {
                vendor_id: "Intel".to_string(),
                cores: 4,
                ..Default::default()
            }],
            cpu_freq_stats: vec![CpuFreqStats {
                name: "cpu0".to_string(),
                cpuinfo_current_frequency: Some(2400),
                ..Default::default()
            }],
        };

        assert_eq!(cpus.node_common.addr, "node1");
        assert_eq!(cpus.cpus.len(), 1);
        assert_eq!(cpus.cpu_freq_stats.len(), 1);
        assert_eq!(cpus.cpus[0].cores, 4);
    }

    #[test]
    fn test_get_cpus_function() {
        let cpus = get_cpus();
        assert!(cpus.node_common.addr.is_empty());
        assert!(cpus.cpus.is_empty());
        assert!(cpus.cpu_freq_stats.is_empty());
    }

    #[test]
    fn test_partition_default() {
        let partition = Partition::default();
        assert!(partition.error.is_empty());
        assert!(partition.device.is_empty());
        assert_eq!(partition.space_total, 0);
        assert_eq!(partition.space_free, 0);
        assert_eq!(partition.inode_total, 0);
        assert_eq!(partition.inode_free, 0);
    }

    #[test]
    fn test_partition_with_values() {
        let partition = Partition {
            error: "".to_string(),
            device: "/dev/sda1".to_string(),
            model: "Samsung SSD".to_string(),
            revision: "1.0".to_string(),
            mountpoint: "/".to_string(),
            fs_type: "ext4".to_string(),
            mount_options: "rw,relatime".to_string(),
            space_total: 1000000000,
            space_free: 500000000,
            inode_total: 1000000,
            inode_free: 800000,
        };

        assert_eq!(partition.device, "/dev/sda1");
        assert_eq!(partition.fs_type, "ext4");
        assert_eq!(partition.space_total, 1000000000);
        assert_eq!(partition.space_free, 500000000);
    }

    #[test]
    fn test_partitions_structure() {
        let partitions = Partitions {
            node_common: NodeCommon {
                addr: "storage-node".to_string(),
                error: None,
            },
            partitions: vec![
                Partition {
                    device: "/dev/sda1".to_string(),
                    mountpoint: "/".to_string(),
                    space_total: 1000000,
                    space_free: 500000,
                    ..Default::default()
                },
                Partition {
                    device: "/dev/sdb1".to_string(),
                    mountpoint: "/data".to_string(),
                    space_total: 2000000,
                    space_free: 1500000,
                    ..Default::default()
                },
            ],
        };

        assert_eq!(partitions.partitions.len(), 2);
        assert_eq!(partitions.partitions[0].device, "/dev/sda1");
        assert_eq!(partitions.partitions[1].mountpoint, "/data");
    }

    #[test]
    fn test_get_partitions_function() {
        let partitions = get_partitions();
        assert!(partitions.node_common.addr.is_empty());
        assert!(partitions.partitions.is_empty());
    }

    #[test]
    fn test_os_info_default() {
        let os_info = OsInfo::default();
        assert!(os_info.node_common.addr.is_empty());
        assert!(os_info.node_common.error.is_none());
    }

    #[test]
    fn test_get_os_info_function() {
        let os_info = get_os_info();
        assert!(os_info.node_common.addr.is_empty());
    }

    #[test]
    fn test_proc_info_default() {
        let proc_info = ProcInfo::default();
        assert_eq!(proc_info.pid, 0);
        assert!(!proc_info.is_background);
        assert_eq!(proc_info.cpu_percent, 0.0);
        assert!(proc_info.children_pids.is_empty());
        assert!(proc_info.cmd_line.is_empty());
        assert_eq!(proc_info.num_connections, 0);
        assert!(!proc_info.is_running);
        assert_eq!(proc_info.mem_percent, 0.0);
        assert!(proc_info.name.is_empty());
        assert_eq!(proc_info.nice, 0);
        assert_eq!(proc_info.num_fds, 0);
        assert_eq!(proc_info.num_threads, 0);
        assert_eq!(proc_info.ppid, 0);
        assert!(proc_info.status.is_empty());
        assert_eq!(proc_info.tgid, 0);
        assert!(proc_info.uids.is_empty());
        assert!(proc_info.username.is_empty());
    }

    #[test]
    fn test_proc_info_with_values() {
        let proc_info = ProcInfo {
            node_common: NodeCommon {
                addr: "worker-node".to_string(),
                error: None,
            },
            pid: 1234,
            is_background: true,
            cpu_percent: 15.5,
            children_pids: vec![1235, 1236],
            cmd_line: "rustfs --config /etc/rustfs.conf".to_string(),
            num_connections: 10,
            create_time: 1640995200,
            cwd: "/opt/rustfs".to_string(),
            exec_path: "/usr/bin/rustfs".to_string(),
            gids: vec![1000, 1001],
            is_running: true,
            mem_percent: 8.2,
            name: "rustfs".to_string(),
            nice: 0,
            num_fds: 25,
            num_threads: 4,
            ppid: 1,
            status: "running".to_string(),
            tgid: 1234,
            uids: vec![1000],
            username: "rustfs".to_string(),
        };

        assert_eq!(proc_info.pid, 1234);
        assert!(proc_info.is_background);
        assert_eq!(proc_info.cpu_percent, 15.5);
        assert_eq!(proc_info.children_pids.len(), 2);
        assert_eq!(proc_info.name, "rustfs");
        assert!(proc_info.is_running);
    }

    #[test]
    fn test_get_proc_info_function() {
        let proc_info = get_proc_info("127.0.0.1:9000");
        assert_eq!(proc_info.pid, 0);
        assert!(!proc_info.is_running);
    }

    #[test]
    fn test_sys_service_default() {
        let service = SysService::default();
        assert!(service.name.is_empty());
        assert!(service.status.is_empty());
    }

    #[test]
    fn test_sys_service_with_values() {
        let service = SysService {
            name: "rustfs".to_string(),
            status: "active".to_string(),
        };

        assert_eq!(service.name, "rustfs");
        assert_eq!(service.status, "active");
    }

    #[test]
    fn test_sys_services_structure() {
        let services = SysServices {
            node_common: NodeCommon {
                addr: "service-node".to_string(),
                error: None,
            },
            services: vec![
                SysService {
                    name: "rustfs".to_string(),
                    status: "active".to_string(),
                },
                SysService {
                    name: "nginx".to_string(),
                    status: "inactive".to_string(),
                },
            ],
        };

        assert_eq!(services.services.len(), 2);
        assert_eq!(services.services[0].name, "rustfs");
        assert_eq!(services.services[1].status, "inactive");
    }

    #[test]
    fn test_get_sys_services_function() {
        let services = get_sys_services("localhost");
        assert!(services.node_common.addr.is_empty());
        assert!(services.services.is_empty());
    }

    #[test]
    fn test_sys_config_default() {
        let config = SysConfig::default();
        assert!(config.node_common.addr.is_empty());
        assert!(config.config.is_empty());
    }

    #[test]
    fn test_sys_config_with_values() {
        let mut config_map = HashMap::new();
        config_map.insert("max_connections".to_string(), "1000".to_string());
        config_map.insert("timeout".to_string(), "30".to_string());

        let config = SysConfig {
            node_common: NodeCommon {
                addr: "config-node".to_string(),
                error: None,
            },
            config: config_map,
        };

        assert_eq!(config.config.len(), 2);
        assert_eq!(config.config.get("max_connections").unwrap(), "1000");
        assert_eq!(config.config.get("timeout").unwrap(), "30");
    }

    #[test]
    fn test_get_sys_config_function() {
        let config = get_sys_config("192.168.1.100");
        assert!(config.node_common.addr.is_empty());
        assert!(config.config.is_empty());
    }

    #[test]
    fn test_sys_errors_default() {
        let errors = SysErrors::default();
        assert!(errors.node_common.addr.is_empty());
        assert!(errors.errors.is_empty());
    }

    #[test]
    fn test_sys_errors_with_values() {
        let errors = SysErrors {
            node_common: NodeCommon {
                addr: "error-node".to_string(),
                error: None,
            },
            errors: vec![
                "Connection timeout".to_string(),
                "Memory allocation failed".to_string(),
                "Disk full".to_string(),
            ],
        };

        assert_eq!(errors.errors.len(), 3);
        assert!(errors.errors.contains(&"Connection timeout".to_string()));
        assert!(errors.errors.contains(&"Disk full".to_string()));
    }

    #[test]
    fn test_get_sys_errors_function() {
        let errors = get_sys_errors("test-node");
        assert!(errors.node_common.addr.is_empty());
        assert!(errors.errors.is_empty());
    }

    #[test]
    fn test_mem_info_default() {
        let mem_info = MemInfo::default();
        assert!(mem_info.node_common.addr.is_empty());
        assert!(mem_info.total.is_none());
        assert!(mem_info.used.is_none());
        assert!(mem_info.free.is_none());
        assert!(mem_info.available.is_none());
        assert!(mem_info.shared.is_none());
        assert!(mem_info.cache.is_none());
        assert!(mem_info.buffers.is_none());
        assert!(mem_info.swap_space_total.is_none());
        assert!(mem_info.swap_space_free.is_none());
        assert!(mem_info.limit.is_none());
    }

    #[test]
    fn test_mem_info_with_values() {
        let mem_info = MemInfo {
            node_common: NodeCommon {
                addr: "memory-node".to_string(),
                error: None,
            },
            total: Some(16777216000),
            used: Some(8388608000),
            free: Some(4194304000),
            available: Some(12582912000),
            shared: Some(1048576000),
            cache: Some(2097152000),
            buffers: Some(524288000),
            swap_space_total: Some(4294967296),
            swap_space_free: Some(2147483648),
            limit: Some(16777216000),
        };

        assert_eq!(mem_info.total.unwrap(), 16777216000);
        assert_eq!(mem_info.used.unwrap(), 8388608000);
        assert_eq!(mem_info.free.unwrap(), 4194304000);
        assert_eq!(mem_info.swap_space_total.unwrap(), 4294967296);
    }

    #[test]
    fn test_mem_info_serialization() {
        let mem_info = MemInfo {
            node_common: NodeCommon {
                addr: "test-node".to_string(),
                error: None,
            },
            total: Some(8000000000),
            used: Some(4000000000),
            free: None,
            available: Some(6000000000),
            ..Default::default()
        };

        let json = serde_json::to_string(&mem_info).unwrap();
        assert!(json.contains("8000000000"));
        assert!(json.contains("4000000000"));
        assert!(json.contains("6000000000"));
        assert!(!json.contains("free"), "None values should be skipped");
    }

    #[test]
    fn test_get_mem_info_function() {
        let mem_info = get_mem_info("memory-server");
        assert!(mem_info.node_common.addr.is_empty());
        assert!(mem_info.total.is_none());
        assert!(mem_info.used.is_none());
    }

    #[test]
    fn test_all_structures_debug_format() {
        let node = NodeCommon::default();
        let cpu = Cpu::default();
        let partition = Partition::default();
        let proc_info = ProcInfo::default();
        let service = SysService::default();
        let mem_info = MemInfo::default();

        // Test that all structures can be formatted with Debug
        assert!(!format!("{node:?}").is_empty());
        assert!(!format!("{cpu:?}").is_empty());
        assert!(!format!("{partition:?}").is_empty());
        assert!(!format!("{proc_info:?}").is_empty());
        assert!(!format!("{service:?}").is_empty());
        assert!(!format!("{mem_info:?}").is_empty());
    }

    #[test]
    fn test_memory_efficiency() {
        // Test that structures don't use excessive memory
        assert!(std::mem::size_of::<NodeCommon>() < 1000);
        assert!(std::mem::size_of::<Cpu>() < 2000);
        assert!(std::mem::size_of::<Partition>() < 2000);
        assert!(std::mem::size_of::<MemInfo>() < 1000);
    }
}
