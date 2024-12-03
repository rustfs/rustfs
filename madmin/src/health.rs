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
