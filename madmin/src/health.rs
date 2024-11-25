use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct NodeCommon {
    pub addr: String,
    pub error: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Cpu {
    vendor_id: String,
    family: String,
    model: String,
    stepping: i32,
    physical_id: String,
    model_name: String,
    mhz: f64,
    cache_size: i32,
    flags: Vec<String>,
    microcode: String,
    cores: u64,
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
