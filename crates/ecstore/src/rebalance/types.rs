use serde::{Deserialize, Serialize};
use std::sync::Arc;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebalanceStats {
    #[serde(rename = "ifs")]
    pub init_free_space: u64, // Pool free space at the start of rebalance
    #[serde(rename = "ic")]
    pub init_capacity: u64, // Pool capacity at the start of rebalance
    #[serde(rename = "bus")]
    pub buckets: Vec<String>, // Buckets being rebalanced or to be rebalanced
    #[serde(rename = "rbs")]
    pub rebalanced_buckets: Vec<String>, // Buckets rebalanced
    #[serde(rename = "bu")]
    pub bucket: String, // Last rebalanced bucket
    #[serde(rename = "ob")]
    pub object: String, // Last rebalanced object
    #[serde(rename = "no")]
    pub num_objects: u64, // Number of objects rebalanced
    #[serde(rename = "nv")]
    pub num_versions: u64, // Number of versions rebalanced
    #[serde(rename = "bs")]
    pub bytes: u64, // Number of bytes rebalanced
    #[serde(rename = "par")]
    pub participating: bool, // Whether the pool is participating in rebalance
    #[serde(rename = "inf")]
    pub info: RebalanceInfo, // Rebalance operation info
    #[serde(rename = "cw", default)]
    pub cleanup_warnings: RebalanceCleanupWarnings,
}

pub type RStats = Vec<Arc<RebalanceStats>>;

#[derive(Debug, Default)]
pub(super) struct RebalanceBucketConfigs {
    pub(super) lifecycle_config: Option<s3s::dto::BucketLifecycleConfiguration>,
    pub(super) lock_retention: Option<s3s::dto::DefaultRetention>,
    pub(super) replication_config: Option<(s3s::dto::ReplicationConfiguration, OffsetDateTime)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RebalanceBucketOutcome {
    Completed,
    Deferred { last_error: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RebalanceEntryOutcome {
    Completed,
    Deferred { last_error: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum RebalStatus {
    #[default]
    None,
    Started,
    Completed,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum RebalSaveOpt {
    #[default]
    Stats,
    StoppedAt,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebalanceInfo {
    #[serde(rename = "startTs")]
    pub start_time: Option<OffsetDateTime>, // Time at which rebalance-start was issued
    #[serde(rename = "stopTs")]
    pub end_time: Option<OffsetDateTime>, // Time at which rebalance operation completed or rebalance-stop was called
    #[serde(rename = "err")]
    pub last_error: Option<String>, // Last rebalance error message
    #[serde(rename = "status")]
    pub status: RebalStatus, // Current state of rebalance operation
    #[serde(rename = "stopping", default)]
    pub stopping: bool, // True after stop is requested and before worker terminal acknowledgement
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RebalanceCleanupWarningEntry {
    #[serde(rename = "bucket", default)]
    pub bucket: String,
    #[serde(rename = "object", default)]
    pub object: String,
    #[serde(rename = "message", default)]
    pub message: String,
    #[serde(rename = "timestamp", default)]
    pub timestamp: Option<OffsetDateTime>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RebalanceCleanupWarnings {
    #[serde(rename = "count", default)]
    pub count: u64,
    #[serde(rename = "lastMsg", default)]
    pub last_message: Option<String>,
    #[serde(rename = "lastBucket", default)]
    pub last_bucket: Option<String>,
    #[serde(rename = "lastObject", default)]
    pub last_object: Option<String>,
    #[serde(rename = "lastAt", default)]
    pub last_at: Option<OffsetDateTime>,
    #[serde(rename = "entries", default)]
    pub entries: Vec<RebalanceCleanupWarningEntry>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RebalanceStopPropagationRecord {
    #[serde(rename = "stopAttemptAt", default)]
    pub stop_attempt_at: Option<OffsetDateTime>,
    #[serde(rename = "stopFailures", default)]
    pub stop_failures: Vec<String>,
    #[serde(rename = "terminalReloadAttemptAt", default)]
    pub terminal_reload_attempt_at: Option<OffsetDateTime>,
    #[serde(rename = "terminalReloadFailures", default)]
    pub terminal_reload_failures: Vec<String>,
}

impl RebalanceStopPropagationRecord {
    pub fn has_failures(&self) -> bool {
        !self.stop_failures.is_empty() || !self.terminal_reload_failures.is_empty()
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct DiskStat {
    pub total_space: u64,
    pub available_space: u64,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct RebalanceMeta {
    #[serde(skip)]
    pub cancel: Option<CancellationToken>, // To be invoked on rebalance-stop
    #[serde(skip)]
    pub last_refreshed_at: Option<OffsetDateTime>,
    #[serde(rename = "stopTs")]
    pub stopped_at: Option<OffsetDateTime>, // Time when rebalance-stop was issued
    #[serde(rename = "id")]
    pub id: String, // ID of the ongoing rebalance operation
    #[serde(rename = "pf")]
    pub percent_free_goal: f64, // Computed from total free space and capacity at the start of rebalance
    #[serde(rename = "rss")]
    pub pool_stats: Vec<RebalanceStats>, // Per-pool rebalance stats keyed by pool index
}
