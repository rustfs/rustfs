use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::health::MemInfo;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimedAction {
    #[serde(rename = "count")]
    pub count: u64,
    #[serde(rename = "acc_time_ns")]
    pub acc_time: u64,
    #[serde(rename = "bytes")]
    pub bytes: u64,
}

impl TimedAction {
    pub fn merge(&mut self, other: &TimedAction) {
        self.count += other.count;
        self.acc_time += other.acc_time;
        self.bytes += other.bytes;
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DiskIOStats {
    #[serde(rename = "read_ios")]
    pub read_ios: u64,
    #[serde(rename = "read_merges")]
    pub read_merges: u64,
    #[serde(rename = "read_sectors")]
    pub read_sectors: u64,
    #[serde(rename = "read_ticks")]
    pub read_ticks: u64,
    #[serde(rename = "write_ios")]
    pub write_ios: u64,
    #[serde(rename = "write_merges")]
    pub write_merges: u64,
    #[serde(rename = "write_sectors")]
    pub write_sectors: u64,
    #[serde(rename = "write_ticks")]
    pub write_ticks: u64,
    #[serde(rename = "current_ios")]
    pub current_ios: u64,
    #[serde(rename = "total_ticks")]
    pub total_ticks: u64,
    #[serde(rename = "req_ticks")]
    pub req_ticks: u64,
    #[serde(rename = "discard_ios")]
    pub discard_ios: u64,
    #[serde(rename = "discard_merges")]
    pub discard_merges: u64,
    #[serde(rename = "discard_secotrs")]
    pub discard_sectors: u64,
    #[serde(rename = "discard_ticks")]
    pub discard_ticks: u64,
    #[serde(rename = "flush_ios")]
    pub flush_ios: u64,
    #[serde(rename = "flush_ticks")]
    pub flush_ticks: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DiskMetric {
    #[serde(rename = "collected")]
    pub collected_at: DateTime<Utc>,
    #[serde(rename = "n_disks")]
    pub n_disks: usize,
    #[serde(rename = "offline")]
    pub offline: usize,
    #[serde(rename = "healing")]
    pub healing: usize,
    #[serde(rename = "life_time_ops")]
    pub life_time_ops: HashMap<String, u64>,
    #[serde(rename = "last_minute")]
    pub last_minute: Operations,
    #[serde(rename = "iostats")]
    pub io_stats: DiskIOStats,
}

impl DiskMetric {
    pub fn merge(&mut self, other: &DiskMetric) {
        if self.collected_at < other.collected_at {
            self.collected_at = other.collected_at;
        }
        self.n_disks += other.n_disks;
        self.offline += other.offline;
        self.healing += other.healing;

        for (k, v) in other.life_time_ops.iter() {
            *self.life_time_ops.entry(k.clone()).or_insert(0) += v;
        }

        for (k, v) in other.last_minute.operations.iter() {
            self.last_minute.operations.entry(k.clone()).or_default().merge(v);
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LastMinute {
    #[serde(rename = "actions")]
    pub actions: HashMap<String, TimedAction>,
    #[serde(rename = "ilm")]
    pub ilm: HashMap<String, TimedAction>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScannerMetrics {
    #[serde(rename = "collected")]
    pub collected_at: DateTime<Utc>,
    #[serde(rename = "current_cycle")]
    pub current_cycle: u64,
    #[serde(rename = "current_started")]
    pub current_started: DateTime<Utc>,
    #[serde(rename = "cycle_complete_times")]
    pub cycles_completed_at: Vec<DateTime<Utc>>,
    #[serde(rename = "ongoing_buckets")]
    pub ongoing_buckets: usize,
    #[serde(rename = "life_time_ops")]
    pub life_time_ops: HashMap<String, u64>,
    #[serde(rename = "ilm_ops")]
    pub life_time_ilm: HashMap<String, u64>,
    #[serde(rename = "last_minute")]
    pub last_minute: LastMinute,
    #[serde(rename = "active")]
    pub active_paths: Vec<String>,
}

impl ScannerMetrics {
    pub fn merge(&mut self, other: &Self) {
        if self.collected_at < other.collected_at {
            self.collected_at = other.collected_at;
        }

        if self.ongoing_buckets < other.ongoing_buckets {
            self.ongoing_buckets = other.ongoing_buckets;
        }

        if self.current_cycle < other.current_cycle {
            self.current_cycle = other.current_cycle;
            self.cycles_completed_at = other.cycles_completed_at.clone();
            self.current_started = other.current_started;
        }

        if other.cycles_completed_at.len() > self.cycles_completed_at.len() {
            self.cycles_completed_at = other.cycles_completed_at.clone();
        }

        if !other.life_time_ops.is_empty() && self.life_time_ops.is_empty() {
            self.life_time_ops = other.life_time_ops.clone();
        }

        for (k, v) in other.life_time_ops.iter() {
            *self.life_time_ops.entry(k.clone()).or_default() += v;
        }

        for (k, v) in other.last_minute.actions.iter() {
            self.last_minute.actions.entry(k.clone()).or_default().merge(v);
        }

        for (k, v) in other.life_time_ilm.iter() {
            *self.life_time_ilm.entry(k.clone()).or_default() += v;
        }

        for (k, v) in other.last_minute.ilm.iter() {
            self.last_minute.ilm.entry(k.clone()).or_default().merge(v);
        }

        self.active_paths.extend(other.active_paths.clone());

        self.active_paths.sort();
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Metrics {
    #[serde(rename = "scanner", skip_serializing_if = "Option::is_none")]
    pub scanner: Option<ScannerMetrics>,
    #[serde(rename = "disk", skip_serializing_if = "Option::is_none")]
    pub disk: Option<DiskMetric>,
    #[serde(rename = "os", skip_serializing_if = "Option::is_none")]
    pub os: Option<OsMetrics>,
    #[serde(rename = "batchJobs", skip_serializing_if = "Option::is_none")]
    pub batch_jobs: Option<BatchJobMetrics>,
    #[serde(rename = "siteResync", skip_serializing_if = "Option::is_none")]
    pub site_resync: Option<SiteResyncMetrics>,
    #[serde(rename = "net", skip_serializing_if = "Option::is_none")]
    pub net: Option<NetMetrics>,
    #[serde(rename = "mem", skip_serializing_if = "Option::is_none")]
    pub mem: Option<MemMetrics>,
    #[serde(rename = "cpu", skip_serializing_if = "Option::is_none")]
    pub cpu: Option<CPUMetrics>,
    #[serde(rename = "rpc", skip_serializing_if = "Option::is_none")]
    pub rpc: Option<RPCMetrics>,
}

impl Metrics {
    pub fn merge(&mut self, other: &Self) {
        if let Some(scanner) = other.scanner.as_ref() {
            match self.scanner {
                Some(ref mut s_scanner) => s_scanner.merge(scanner),
                None => self.scanner = Some(scanner.clone()),
            }
        }

        if let Some(disk) = other.disk.as_ref() {
            match self.disk {
                Some(ref mut s_disk) => s_disk.merge(disk),
                None => self.disk = Some(disk.clone()),
            }
        }

        if let Some(os) = other.os.as_ref() {
            match self.os {
                Some(ref mut s_os) => s_os.merge(os),
                None => self.os = Some(os.clone()),
            }
        }

        if let Some(batch_jobs) = other.batch_jobs.as_ref() {
            match self.batch_jobs {
                Some(ref mut s_batch_jobs) => s_batch_jobs.merge(batch_jobs),
                None => self.batch_jobs = Some(batch_jobs.clone()),
            }
        }

        if let Some(site_resync) = other.site_resync.as_ref() {
            match self.site_resync {
                Some(ref mut s_site_resync) => s_site_resync.merge(site_resync),
                None => self.site_resync = Some(site_resync.clone()),
            }
        }

        if let Some(net) = other.net.as_ref() {
            match self.net {
                Some(ref mut s_net) => s_net.merge(net),
                None => self.net = Some(net.clone()),
            }
        }

        if let Some(rpc) = other.rpc.as_ref() {
            match self.rpc {
                Some(ref mut s_rpc) => s_rpc.merge(rpc),
                None => self.rpc = Some(rpc.clone()),
            }
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RPCMetrics {
    #[serde(rename = "collectedAt")]
    pub collected_at: DateTime<Utc>,

    pub connected: i32,

    #[serde(rename = "reconnectCount")]
    pub reconnect_count: i32,

    pub disconnected: i32,

    #[serde(rename = "outgoingStreams")]
    pub outgoing_streams: i32,

    #[serde(rename = "incomingStreams")]
    pub incoming_streams: i32,

    #[serde(rename = "outgoingBytes")]
    pub outgoing_bytes: i64,

    #[serde(rename = "incomingBytes")]
    pub incoming_bytes: i64,

    #[serde(rename = "outgoingMessages")]
    pub outgoing_messages: i64,

    #[serde(rename = "incomingMessages")]
    pub incoming_messages: i64,

    pub out_queue: i32,

    #[serde(rename = "lastPongTime")]
    pub last_pong_time: DateTime<Utc>,

    #[serde(rename = "lastPingMS")]
    pub last_ping_ms: f64,

    #[serde(rename = "maxPingDurMS")]
    pub max_ping_dur_ms: f64, // Maximum across all merged entries.

    #[serde(rename = "lastConnectTime")]
    pub last_connect_time: DateTime<Utc>,

    #[serde(rename = "byDestination", skip_serializing_if = "Option::is_none")]
    pub by_destination: Option<HashMap<String, RPCMetrics>>,

    #[serde(rename = "byCaller", skip_serializing_if = "Option::is_none")]
    pub by_caller: Option<HashMap<String, RPCMetrics>>,
}

impl RPCMetrics {
    pub fn merge(&mut self, other: &Self) {
        if self.collected_at < other.collected_at {
            self.collected_at = other.collected_at;
        }

        if self.last_connect_time < other.last_connect_time {
            self.last_connect_time = other.last_connect_time;
        }

        self.connected += other.connected;
        self.disconnected += other.disconnected;
        self.reconnect_count += other.reconnect_count;
        self.outgoing_streams += other.outgoing_streams;
        self.incoming_streams += other.incoming_streams;
        self.outgoing_bytes += other.outgoing_bytes;
        self.incoming_bytes += other.incoming_bytes;
        self.outgoing_messages += other.outgoing_messages;
        self.incoming_messages += other.incoming_messages;
        self.out_queue += other.out_queue;

        if self.last_pong_time < other.last_pong_time {
            self.last_pong_time = other.last_pong_time;
            self.last_ping_ms = other.last_ping_ms;
        }

        if self.max_ping_dur_ms < other.max_ping_dur_ms {
            self.max_ping_dur_ms = other.max_ping_dur_ms;
        }

        if let Some(by_destination) = other.by_destination.as_ref() {
            match self.by_destination.as_mut() {
                Some(s_by_de) => {
                    for (key, value) in by_destination {
                        s_by_de
                            .entry(key.to_string())
                            .and_modify(|v| v.merge(value))
                            .or_insert(value.clone());
                    }
                }
                None => self.by_destination = Some(by_destination.clone()),
            }
        }

        if let Some(by_caller) = other.by_caller.as_ref() {
            match self.by_caller.as_mut() {
                Some(s_by_caller) => {
                    for (key, value) in by_caller {
                        s_by_caller
                            .entry(key.to_string())
                            .and_modify(|v| v.merge(value))
                            .or_insert(value.clone());
                    }
                }
                None => self.by_caller = Some(by_caller.clone()),
            }
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CPUMetrics {}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NetMetrics {
    #[serde(rename = "collected")]
    pub collected_at: DateTime<Utc>,
    #[serde(rename = "interfaceName")]
    pub interface_name: String,
    #[serde(rename = "netstats")]
    pub net_stats: NetDevLine,
}

impl NetMetrics {
    pub fn merge(&mut self, other: &Self) {
        if self.collected_at < other.collected_at {
            self.collected_at = other.collected_at;
        }

        self.net_stats.rx_bytes += other.net_stats.rx_bytes;
        self.net_stats.rx_packets += other.net_stats.rx_packets;
        self.net_stats.rx_errors += other.net_stats.rx_errors;
        self.net_stats.rx_dropped += other.net_stats.rx_dropped;
        self.net_stats.rx_fifo += other.net_stats.rx_fifo;
        self.net_stats.rx_frame += other.net_stats.rx_frame;
        self.net_stats.rx_compressed += other.net_stats.rx_compressed;
        self.net_stats.rx_multicast += other.net_stats.rx_multicast;
        self.net_stats.tx_bytes += other.net_stats.tx_bytes;
        self.net_stats.tx_packets += other.net_stats.tx_packets;
        self.net_stats.tx_errors += other.net_stats.tx_errors;
        self.net_stats.tx_dropped += other.net_stats.tx_dropped;
        self.net_stats.tx_fifo += other.net_stats.tx_fifo;
        self.net_stats.tx_collisions += other.net_stats.tx_collisions;
        self.net_stats.tx_carrier += other.net_stats.tx_carrier;
        self.net_stats.tx_compressed += other.net_stats.tx_compressed;
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NetDevLine {
    #[serde(rename = "name")]
    pub name: String, // The name of the interface.

    #[serde(rename = "rx_bytes")]
    pub rx_bytes: u64, // Cumulative count of bytes received.

    #[serde(rename = "rx_packets")]
    pub rx_packets: u64, // Cumulative count of packets received.

    #[serde(rename = "rx_errors")]
    pub rx_errors: u64, // Cumulative count of receive errors encountered.

    #[serde(rename = "rx_dropped")]
    pub rx_dropped: u64, // Cumulative count of packets dropped while receiving.

    #[serde(rename = "rx_fifo")]
    pub rx_fifo: u64, // Cumulative count of FIFO buffer errors.

    #[serde(rename = "rx_frame")]
    pub rx_frame: u64, // Cumulative count of packet framing errors.

    #[serde(rename = "rx_compressed")]
    pub rx_compressed: u64, // Cumulative count of compressed packets received by the device driver.

    #[serde(rename = "rx_multicast")]
    pub rx_multicast: u64, // Cumulative count of multicast frames received by the device driver.

    #[serde(rename = "tx_bytes")]
    pub tx_bytes: u64, // Cumulative count of bytes transmitted.

    #[serde(rename = "tx_packets")]
    pub tx_packets: u64, // Cumulative count of packets transmitted.

    #[serde(rename = "tx_errors")]
    pub tx_errors: u64, // Cumulative count of transmit errors encountered.

    #[serde(rename = "tx_dropped")]
    pub tx_dropped: u64, // Cumulative count of packets dropped while transmitting.

    #[serde(rename = "tx_fifo")]
    pub tx_fifo: u64, // Cumulative count of FIFO buffer errors.

    #[serde(rename = "tx_collisions")]
    pub tx_collisions: u64, // Cumulative count of collisions detected on the interface.

    #[serde(rename = "tx_carrier")]
    pub tx_carrier: u64, // Cumulative count of carrier losses detected by the device driver.

    #[serde(rename = "tx_compressed")]
    pub tx_compressed: u64, // Cumulative count of compressed packets transmitted by the device driver.
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MemMetrics {
    #[serde(rename = "collected")]
    pub collected_at: DateTime<Utc>,
    #[serde(rename = "memInfo")]
    pub info: MemInfo,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SiteResyncMetrics {
    #[serde(rename = "collected")]
    pub collected_at: DateTime<Utc>,
    #[serde(rename = "resyncStatus", skip_serializing_if = "Option::is_none")]
    pub resync_status: Option<String>,
    #[serde(rename = "startTime")]
    pub start_time: DateTime<Utc>,
    #[serde(rename = "lastUpdate")]
    pub last_update: DateTime<Utc>,
    #[serde(rename = "numBuckets")]
    pub num_buckets: i64,
    #[serde(rename = "resyncID")]
    pub resync_id: String,
    #[serde(rename = "deplID")]
    pub depl_id: String,
    #[serde(rename = "completedReplicationSize")]
    pub replicated_size: i64,
    #[serde(rename = "replicationCount")]
    pub replicated_count: i64,
    #[serde(rename = "failedReplicationSize")]
    pub failed_size: i64,
    #[serde(rename = "failedReplicationCount")]
    pub failed_count: i64,
    #[serde(rename = "failedBuckets")]
    pub failed_buckets: Vec<String>,
    #[serde(rename = "bucket", skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(rename = "object", skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
}

impl SiteResyncMetrics {
    pub fn merge(&mut self, other: &Self) {
        if self.collected_at < other.collected_at {
            *self = other.clone();
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BatchJobMetrics {
    #[serde(rename = "collected")]
    pub collected_at: DateTime<Utc>,
    #[serde(rename = "Jobs")]
    pub jobs: HashMap<String, JobMetric>,
}

impl BatchJobMetrics {
    pub fn merge(&mut self, other: &BatchJobMetrics) {
        if other.jobs.is_empty() {
            return;
        }

        if self.collected_at < other.collected_at {
            self.collected_at = other.collected_at;
        }

        for (k, v) in other.jobs.clone().into_iter() {
            self.jobs.insert(k, v);
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct JobMetric {
    #[serde(rename = "jobID")]
    pub job_id: String,
    #[serde(rename = "jobType")]
    pub job_type: String,
    #[serde(rename = "startTime")]
    pub start_time: DateTime<Utc>,
    #[serde(rename = "lastUpdate")]
    pub last_update: DateTime<Utc>,
    #[serde(rename = "retryAttempts")]
    pub retry_attempts: i32,
    pub complete: bool,
    pub failed: bool,
    // Specific job type data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replicate: Option<ReplicateInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_rotate: Option<KeyRotationInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expired: Option<ExpirationInfo>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReplicateInfo {
    #[serde(rename = "lastBucket")]
    pub bucket: String,
    #[serde(rename = "lastObject")]
    pub object: String,
    #[serde(rename = "objects")]
    pub objects: i64,
    #[serde(rename = "objectsFailed")]
    pub objects_failed: i64,
    #[serde(rename = "bytesTransferred")]
    pub bytes_transferred: i64,
    #[serde(rename = "bytesFailed")]
    pub bytes_failed: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExpirationInfo {
    #[serde(rename = "lastBucket")]
    pub bucket: String,
    #[serde(rename = "lastObject")]
    pub object: String,
    #[serde(rename = "objects")]
    pub objects: i64,
    #[serde(rename = "objectsFailed")]
    pub objects_failed: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct KeyRotationInfo {
    #[serde(rename = "lastBucket")]
    pub bucket: String,
    #[serde(rename = "lastObject")]
    pub object: String,
    #[serde(rename = "objects")]
    pub objects: i64,
    #[serde(rename = "objectsFailed")]
    pub objects_failed: i64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct RealtimeMetrics {
    #[serde(rename = "errors")]
    pub errors: Vec<String>,
    #[serde(rename = "hosts")]
    pub hosts: Vec<String>,
    #[serde(rename = "aggregated")]
    pub aggregated: Metrics,
    #[serde(rename = "by_host")]
    pub by_host: HashMap<String, Metrics>,
    #[serde(rename = "by_disk")]
    pub by_disk: HashMap<String, DiskMetric>,
    #[serde(rename = "final")]
    pub finally: bool,
}

impl RealtimeMetrics {
    pub fn merge(&mut self, other: Self) {
        if !other.errors.is_empty() {
            self.errors.extend(other.errors);
        }

        for (k, v) in other.by_host.into_iter() {
            *self.by_host.entry(k).or_default() = v;
        }

        self.hosts.extend(other.hosts);
        self.aggregated.merge(&other.aggregated);
        self.hosts.sort();

        for (k, v) in other.by_disk.into_iter() {
            self.by_disk.entry(k.to_string()).and_modify(|h| *h = v.clone()).or_insert(v);
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OsMetrics {
    #[serde(rename = "collected")]
    pub collected_at: DateTime<Utc>,
    #[serde(rename = "life_time_ops")]
    pub life_time_ops: HashMap<String, u64>,
    #[serde(rename = "last_minute")]
    pub last_minute: Operations,
}

impl OsMetrics {
    pub fn merge(&mut self, other: &Self) {
        if self.collected_at < other.collected_at {
            self.collected_at = other.collected_at;
        }

        for (k, v) in other.life_time_ops.iter() {
            *self.life_time_ops.entry(k.clone()).or_default() += v;
        }

        for (k, v) in other.last_minute.operations.iter() {
            self.last_minute.operations.entry(k.clone()).or_default().merge(v);
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Operations {
    #[serde(rename = "operations")]
    pub operations: HashMap<String, TimedAction>,
}
