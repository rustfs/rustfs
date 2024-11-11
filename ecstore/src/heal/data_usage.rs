use std::{collections::HashMap, time::SystemTime};

use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Receiver;
use tracing::info;

use crate::{
    config::common::save_config,
    disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET},
    new_object_layer_fn,
    utils::path::SLASH_SEPARATOR,
};

pub const DATA_USAGE_ROOT: &str = SLASH_SEPARATOR;
const DATA_USAGE_OBJ_NAME: &str = ".usage.json";
const DATA_USAGE_BLOOM_NAME: &str = ".bloomcycle.bin";
pub const DATA_USAGE_CACHE_NAME: &str = ".usage-cache.bin";
lazy_static! {
    pub static ref DATA_USAGE_BUCKET: String = format!("{}{}{}", RUSTFS_META_BUCKET, SLASH_SEPARATOR, BUCKET_META_PREFIX);
    pub static ref DATA_USAGE_OBJ_NAME_PATH: String = format!("{}{}{}", BUCKET_META_PREFIX, SLASH_SEPARATOR, DATA_USAGE_OBJ_NAME);
    pub static ref DATA_USAGE_BLOOM_NAME_PATH: String =
        format!("{}{}{}", BUCKET_META_PREFIX, SLASH_SEPARATOR, DATA_USAGE_BLOOM_NAME);
    pub static ref BACKGROUND_HEAL_INFO_PATH: String =
        format!("{}{}{}", BUCKET_META_PREFIX, SLASH_SEPARATOR, ".background-heal.json");
}

// BucketTargetUsageInfo - bucket target usage info provides
// - replicated size for all objects sent to this target
// - replica size for all objects received from this target
// - replication pending size for all objects pending replication to this target
// - replication failed size for all objects failed replication to this target
// - replica pending count
// - replica failed count
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BucketTargetUsageInfo {
    replication_pending_size: u64,
    replication_failed_size: u64,
    replicated_size: u64,
    replica_size: u64,
    replication_pending_count: u64,
    replication_failed_count: u64,
    replicated_count: u64,
}

// BucketUsageInfo - bucket usage info provides
// - total size of the bucket
// - total objects in a bucket
// - object size histogram per bucket
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BucketUsageInfo {
    size: u64,
    // Following five fields suffixed with V1 are here for backward compatibility
    // Total Size for objects that have not yet been replicated
    replication_pending_size_v1: u64,
    // Total size for objects that have witness one or more failures and will be retried
    replication_failed_size_v1: u64,
    // Total size for objects that have been replicated to destination
    replicated_size_v1: u64,
    // Total number of objects pending replication
    replication_pending_count_v1: u64,
    // Total number of objects that failed replication
    replication_failed_count_v1: u64,

    objects_count: u64,
    object_size_histogram: HashMap<String, u64>,
    object_versions_histogram: HashMap<String, u64>,
    versions_count: u64,
    delete_markers_count: u64,
    replica_size: u64,
    replica_count: u64,
    replication_info: HashMap<String, BucketTargetUsageInfo>,
}

// DataUsageInfo represents data usage stats of the underlying Object API
#[derive(Debug, Serialize, Deserialize)]
pub struct DataUsageInfo {
    total_capacity: u64,
    total_used_capacity: u64,
    total_free_capacity: u64,

    // LastUpdate is the timestamp of when the data usage info was last updated.
    // This does not indicate a full scan.
    last_update: SystemTime,

    // Objects total count across all buckets
    objects_total_count: u64,
    // Versions total count across all buckets
    versions_total_count: u64,
    // Delete markers total count across all buckets
    delete_markers_total_count: u64,
    // Objects total size across all buckets
    objects_total_size: u64,
    replication_info: HashMap<String, BucketTargetUsageInfo>,

    // Total number of buckets in this cluster
    buckets_count: u64,
    // Buckets usage info provides following information across all buckets
    // - total size of the bucket
    // - total objects in a bucket
    // - object size histogram per bucket
    buckets_usage: HashMap<String, BucketUsageInfo>,
    // Deprecated kept here for backward compatibility reasons.
    bucket_sizes: HashMap<String, u64>,
    // Todo: TierStats
    // TierStats contains per-tier stats of all configured remote tiers
}

impl Default for DataUsageInfo {
    fn default() -> Self {
        Self {
            total_capacity: Default::default(),
            total_used_capacity: Default::default(),
            total_free_capacity: Default::default(),
            last_update: SystemTime::now(),
            objects_total_count: Default::default(),
            versions_total_count: Default::default(),
            delete_markers_total_count: Default::default(),
            objects_total_size: Default::default(),
            replication_info: Default::default(),
            buckets_count: Default::default(),
            buckets_usage: Default::default(),
            bucket_sizes: Default::default(),
        }
    }
}

pub async fn store_data_usage_in_backend(mut rx: Receiver<DataUsageInfo>) {
    let layer = new_object_layer_fn();
    let lock = layer.read().await;
    let store = match lock.as_ref() {
        Some(s) => s,
        None => {
            info!("errServerNotInitialized");
            return;
        }
    };
    let mut attempts = 1;
    loop {
        match rx.recv().await {
            Some(data_usage_info) => {
                if let Ok(data) = serde_json::to_vec(&data_usage_info) {
                    if attempts > 10 {
                        let _ = save_config(store, &format!("{}{}", DATA_USAGE_OBJ_NAME_PATH.to_string(), ".bkp"), &data).await;
                        attempts += 1;
                    }
                    let _ = save_config(store, &DATA_USAGE_OBJ_NAME_PATH, &data).await;
                    attempts += 1;
                } else {
                    continue;
                }
            }
            None => {
                return;
            }
        }
    }
}
