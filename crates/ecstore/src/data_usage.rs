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

use std::{collections::HashMap, sync::Arc};

use crate::{bucket::metadata_sys::get_replication_config, config::com::read_config, store::ECStore, store_api::StorageAPI};
use rustfs_common::data_usage::{BucketTargetUsageInfo, DataUsageCache, DataUsageEntry, DataUsageInfo, SizeSummary};
use rustfs_utils::path::SLASH_SEPARATOR;
use tracing::{error, info, warn};

use crate::error::Error;

// Data usage storage constants
pub const DATA_USAGE_ROOT: &str = SLASH_SEPARATOR;
const DATA_USAGE_OBJ_NAME: &str = ".usage.json";
const DATA_USAGE_BLOOM_NAME: &str = ".bloomcycle.bin";
pub const DATA_USAGE_CACHE_NAME: &str = ".usage-cache.bin";

// Data usage storage paths
lazy_static::lazy_static! {
    pub static ref DATA_USAGE_BUCKET: String = format!("{}{}{}",
        crate::disk::RUSTFS_META_BUCKET,
        SLASH_SEPARATOR,
        crate::disk::BUCKET_META_PREFIX
    );
    pub static ref DATA_USAGE_OBJ_NAME_PATH: String = format!("{}{}{}",
        crate::disk::BUCKET_META_PREFIX,
        SLASH_SEPARATOR,
        DATA_USAGE_OBJ_NAME
    );
    pub static ref DATA_USAGE_BLOOM_NAME_PATH: String = format!("{}{}{}",
        crate::disk::BUCKET_META_PREFIX,
        SLASH_SEPARATOR,
        DATA_USAGE_BLOOM_NAME
    );
}

/// Store data usage info to backend storage
pub async fn store_data_usage_in_backend(data_usage_info: DataUsageInfo, store: Arc<ECStore>) -> Result<(), Error> {
    let data =
        serde_json::to_vec(&data_usage_info).map_err(|e| Error::other(format!("Failed to serialize data usage info: {e}")))?;

    // Save to backend using the same mechanism as original code
    crate::config::com::save_config(store, &DATA_USAGE_OBJ_NAME_PATH, data)
        .await
        .map_err(Error::other)?;

    Ok(())
}

/// Load data usage info from backend storage
pub async fn load_data_usage_from_backend(store: Arc<ECStore>) -> Result<DataUsageInfo, Error> {
    let buf: Vec<u8> = match read_config(store.clone(), &DATA_USAGE_OBJ_NAME_PATH).await {
        Ok(data) => data,
        Err(e) => {
            error!("Failed to read data usage info from backend: {}", e);
            if e == crate::error::Error::ConfigNotFound {
                warn!("Data usage config not found, building basic statistics");
                return build_basic_data_usage_info(store).await;
            }
            return Err(Error::other(e));
        }
    };

    let mut data_usage_info: DataUsageInfo =
        serde_json::from_slice(&buf).map_err(|e| Error::other(format!("Failed to deserialize data usage info: {e}")))?;

    info!("Loaded data usage info from backend with {} buckets", data_usage_info.buckets_count);

    // Validate data and supplement if empty
    if data_usage_info.buckets_count == 0 || data_usage_info.buckets_usage.is_empty() {
        warn!("Loaded data is empty, supplementing with basic statistics");
        if let Ok(basic_info) = build_basic_data_usage_info(store.clone()).await {
            data_usage_info.buckets_count = basic_info.buckets_count;
            data_usage_info.buckets_usage = basic_info.buckets_usage;
            data_usage_info.bucket_sizes = basic_info.bucket_sizes;
            data_usage_info.objects_total_count = basic_info.objects_total_count;
            data_usage_info.objects_total_size = basic_info.objects_total_size;
            data_usage_info.last_update = basic_info.last_update;
        }
    }

    // Handle backward compatibility
    if data_usage_info.buckets_usage.is_empty() {
        data_usage_info.buckets_usage = data_usage_info
            .bucket_sizes
            .iter()
            .map(|(bucket, &size)| {
                (
                    bucket.clone(),
                    rustfs_common::data_usage::BucketUsageInfo {
                        size,
                        ..Default::default()
                    },
                )
            })
            .collect();
    }

    if data_usage_info.bucket_sizes.is_empty() {
        data_usage_info.bucket_sizes = data_usage_info
            .buckets_usage
            .iter()
            .map(|(bucket, bui)| (bucket.clone(), bui.size))
            .collect();
    }

    // Handle replication info
    for (bucket, bui) in &data_usage_info.buckets_usage {
        if bui.replicated_size_v1 > 0
            || bui.replication_failed_count_v1 > 0
            || bui.replication_failed_size_v1 > 0
            || bui.replication_pending_count_v1 > 0
        {
            if let Ok((cfg, _)) = get_replication_config(bucket).await {
                if !cfg.role.is_empty() {
                    data_usage_info.replication_info.insert(
                        cfg.role.clone(),
                        BucketTargetUsageInfo {
                            replication_failed_size: bui.replication_failed_size_v1,
                            replication_failed_count: bui.replication_failed_count_v1,
                            replicated_size: bui.replicated_size_v1,
                            replication_pending_count: bui.replication_pending_count_v1,
                            replication_pending_size: bui.replication_pending_size_v1,
                            ..Default::default()
                        },
                    );
                }
            }
        }
    }

    Ok(data_usage_info)
}

/// Build basic data usage info with real object counts
async fn build_basic_data_usage_info(store: Arc<ECStore>) -> Result<DataUsageInfo, Error> {
    let mut data_usage_info = DataUsageInfo::default();

    // Get bucket list
    match store.list_bucket(&crate::store_api::BucketOptions::default()).await {
        Ok(buckets) => {
            data_usage_info.buckets_count = buckets.len() as u64;
            data_usage_info.last_update = Some(std::time::SystemTime::now());

            let mut total_objects = 0u64;
            let mut total_size = 0u64;

            for bucket_info in buckets {
                if bucket_info.name.starts_with('.') {
                    continue; // Skip system buckets
                }

                // Try to get actual object count for this bucket
                let (object_count, bucket_size) = match store
                    .clone()
                    .list_objects_v2(
                        &bucket_info.name,
                        "",    // prefix
                        None,  // continuation_token
                        None,  // delimiter
                        100,   // max_keys - small limit for performance
                        false, // fetch_owner
                        None,  // start_after
                    )
                    .await
                {
                    Ok(result) => {
                        let count = result.objects.len() as u64;
                        let size = result.objects.iter().map(|obj| obj.size as u64).sum();
                        (count, size)
                    }
                    Err(_) => (0, 0),
                };

                total_objects += object_count;
                total_size += bucket_size;

                let bucket_usage = rustfs_common::data_usage::BucketUsageInfo {
                    size: bucket_size,
                    objects_count: object_count,
                    versions_count: object_count, // Simplified
                    delete_markers_count: 0,
                    ..Default::default()
                };

                data_usage_info.buckets_usage.insert(bucket_info.name.clone(), bucket_usage);
                data_usage_info.bucket_sizes.insert(bucket_info.name, bucket_size);
            }

            data_usage_info.objects_total_count = total_objects;
            data_usage_info.objects_total_size = total_size;
            data_usage_info.versions_total_count = total_objects;
        }
        Err(e) => {
            warn!("Failed to list buckets for basic data usage info: {}", e);
        }
    }

    Ok(data_usage_info)
}

/// Create a data usage cache entry from size summary
pub fn create_cache_entry_from_summary(summary: &SizeSummary) -> DataUsageEntry {
    let mut entry = DataUsageEntry::default();
    entry.add_sizes(summary);
    entry
}

/// Convert data usage cache to DataUsageInfo
pub fn cache_to_data_usage_info(cache: &DataUsageCache, path: &str, buckets: &[crate::store_api::BucketInfo]) -> DataUsageInfo {
    let e = match cache.find(path) {
        Some(e) => e,
        None => return DataUsageInfo::default(),
    };
    let flat = cache.flatten(&e);

    let mut buckets_usage = HashMap::new();
    for bucket in buckets.iter() {
        let e = match cache.find(&bucket.name) {
            Some(e) => e,
            None => continue,
        };
        let flat = cache.flatten(&e);
        let mut bui = rustfs_common::data_usage::BucketUsageInfo {
            size: flat.size as u64,
            versions_count: flat.versions as u64,
            objects_count: flat.objects as u64,
            delete_markers_count: flat.delete_markers as u64,
            object_size_histogram: flat.obj_sizes.to_map(),
            object_versions_histogram: flat.obj_versions.to_map(),
            ..Default::default()
        };

        if let Some(rs) = &flat.replication_stats {
            bui.replica_size = rs.replica_size;
            bui.replica_count = rs.replica_count;

            for (arn, stat) in rs.targets.iter() {
                bui.replication_info.insert(
                    arn.clone(),
                    BucketTargetUsageInfo {
                        replication_pending_size: stat.pending_size,
                        replicated_size: stat.replicated_size,
                        replication_failed_size: stat.failed_size,
                        replication_pending_count: stat.pending_count,
                        replication_failed_count: stat.failed_count,
                        replicated_count: stat.replicated_count,
                        ..Default::default()
                    },
                );
            }
        }
        buckets_usage.insert(bucket.name.clone(), bui);
    }

    DataUsageInfo {
        last_update: cache.info.last_update,
        objects_total_count: flat.objects as u64,
        versions_total_count: flat.versions as u64,
        delete_markers_total_count: flat.delete_markers as u64,
        objects_total_size: flat.size as u64,
        buckets_count: e.children.len() as u64,
        buckets_usage,
        ..Default::default()
    }
}

// Helper functions for DataUsageCache operations
pub async fn load_data_usage_cache(store: &crate::set_disk::SetDisks, name: &str) -> crate::error::Result<DataUsageCache> {
    use crate::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
    use crate::store_api::{ObjectIO, ObjectOptions};
    use http::HeaderMap;
    use rand::Rng;
    use std::path::Path;
    use std::time::Duration;
    use tokio::time::sleep;

    let mut d = DataUsageCache::default();
    let mut retries = 0;
    while retries < 5 {
        let path = Path::new(BUCKET_META_PREFIX).join(name);
        match store
            .get_object_reader(
                RUSTFS_META_BUCKET,
                path.to_str().unwrap(),
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(mut reader) => {
                if let Ok(info) = DataUsageCache::unmarshal(&reader.read_all().await?) {
                    d = info
                }
                break;
            }
            Err(err) => match err {
                crate::error::Error::FileNotFound | crate::error::Error::VolumeNotFound => {
                    match store
                        .get_object_reader(
                            RUSTFS_META_BUCKET,
                            name,
                            None,
                            HeaderMap::new(),
                            &ObjectOptions {
                                no_lock: true,
                                ..Default::default()
                            },
                        )
                        .await
                    {
                        Ok(mut reader) => {
                            if let Ok(info) = DataUsageCache::unmarshal(&reader.read_all().await?) {
                                d = info
                            }
                            break;
                        }
                        Err(_) => match err {
                            crate::error::Error::FileNotFound | crate::error::Error::VolumeNotFound => {
                                break;
                            }
                            _ => {}
                        },
                    }
                }
                _ => {
                    break;
                }
            },
        }
        retries += 1;
        let dur = {
            let mut rng = rand::rng();
            rng.random_range(0..1_000)
        };
        sleep(Duration::from_millis(dur)).await;
    }
    Ok(d)
}

pub async fn save_data_usage_cache(cache: &DataUsageCache, name: &str) -> crate::error::Result<()> {
    use crate::config::com::save_config;
    use crate::disk::BUCKET_META_PREFIX;
    use crate::new_object_layer_fn;
    use std::path::Path;

    let Some(store) = new_object_layer_fn() else {
        return Err(crate::error::Error::other("errServerNotInitialized"));
    };
    let buf = cache.marshal_msg().map_err(crate::error::Error::other)?;
    let buf_clone = buf.clone();

    let store_clone = store.clone();

    let name = Path::new(BUCKET_META_PREFIX).join(name).to_string_lossy().to_string();

    let name_clone = name.clone();
    tokio::spawn(async move {
        let _ = save_config(store_clone, &format!("{}{}", &name_clone, ".bkp"), buf_clone).await;
    });
    save_config(store, &name, buf).await?;
    Ok(())
}
