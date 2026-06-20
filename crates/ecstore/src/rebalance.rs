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

use crate::error::{Error, Result};
use crate::object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_REBALANCE: &str = "rebalance";
const EVENT_REBALANCE_STATE: &str = "rebalance_state";
const EVENT_REBALANCE_BUCKET: &str = "rebalance_bucket";
const EVENT_REBALANCE_ENTRY: &str = "rebalance_entry";
const EVENT_REBALANCE_LISTING: &str = "rebalance_listing";

const REBAL_META_FMT: u16 = 1; // Replace with actual format value
const REBAL_META_VER: u16 = 1; // Replace with actual version value
const REBAL_META_NAME: &str = "rebalance.bin";
const DEFAULT_REBALANCE_MAX_ATTEMPTS: usize = 3;
const REBALANCE_MAX_ATTEMPTS_ENV: &str = "RUSTFS_REBALANCE_MAX_ATTEMPTS";
const REBALANCE_LISTING_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);
const REBALANCE_MIGRATION_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);
const REBALANCE_MIGRATION_LOCK_RETRY_CAP: Duration = Duration::from_secs(10);
const REBALANCE_DEFERRED_ENTRY_ERROR_PREFIX: &str = "deferred transient rebalance entry failure:";

mod control;
mod entry;
mod meta;
mod migration;
mod runtime;
mod worker;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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
struct RebalanceBucketConfigs {
    lifecycle_config: Option<s3s::dto::BucketLifecycleConfiguration>,
    lock_retention: Option<s3s::dto::DefaultRetention>,
    replication_config: Option<(s3s::dto::ReplicationConfiguration, OffsetDateTime)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RebalanceBucketOutcome {
    Completed,
    Deferred { last_error: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RebalanceEntryOutcome {
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
pub struct RebalanceInfo {
    #[serde(rename = "startTs")]
    pub start_time: Option<OffsetDateTime>, // Time at which rebalance-start was issued
    #[serde(rename = "stopTs")]
    pub end_time: Option<OffsetDateTime>, // Time at which rebalance operation completed or rebalance-stop was called
    #[serde(rename = "err")]
    pub last_error: Option<String>, // Last rebalance error message
    #[serde(rename = "status")]
    pub status: RebalStatus, // Current state of rebalance operation
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct DiskStat {
    pub total_space: u64,
    pub available_space: u64,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
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

#[cfg(test)]
mod rebalance_unit_tests;
