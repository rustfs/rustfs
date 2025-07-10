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

use std::{collections::HashMap, sync::Arc, time::SystemTime};

use rustfs_ecstore::{bucket::metadata_sys::get_replication_config, config::com::read_config, store::ECStore};
use rustfs_utils::path::SLASH_SEPARATOR;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::error::{Error, Result};

// Data usage storage constants
pub const DATA_USAGE_ROOT: &str = SLASH_SEPARATOR;
const DATA_USAGE_OBJ_NAME: &str = ".usage.json";
const DATA_USAGE_BLOOM_NAME: &str = ".bloomcycle.bin";
pub const DATA_USAGE_CACHE_NAME: &str = ".usage-cache.bin";

// Data usage storage paths
lazy_static::lazy_static! {
    pub static ref DATA_USAGE_BUCKET: String = format!("{}{}{}",
        rustfs_ecstore::disk::RUSTFS_META_BUCKET,
        SLASH_SEPARATOR,
        rustfs_ecstore::disk::BUCKET_META_PREFIX
    );
    pub static ref DATA_USAGE_OBJ_NAME_PATH: String = format!("{}{}{}",
        rustfs_ecstore::disk::BUCKET_META_PREFIX,
        SLASH_SEPARATOR,
        DATA_USAGE_OBJ_NAME
    );
    pub static ref DATA_USAGE_BLOOM_NAME_PATH: String = format!("{}{}{}",
        rustfs_ecstore::disk::BUCKET_META_PREFIX,
        SLASH_SEPARATOR,
        DATA_USAGE_BLOOM_NAME
    );
}

/// Bucket target usage info provides replication statistics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BucketTargetUsageInfo {
    pub replication_pending_size: u64,
    pub replication_failed_size: u64,
    pub replicated_size: u64,
    pub replica_size: u64,
    pub replication_pending_count: u64,
    pub replication_failed_count: u64,
    pub replicated_count: u64,
}

/// Bucket usage info provides bucket-level statistics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BucketUsageInfo {
    pub size: u64,
    // Following five fields suffixed with V1 are here for backward compatibility
    // Total Size for objects that have not yet been replicated
    pub replication_pending_size_v1: u64,
    // Total size for objects that have witness one or more failures and will be retried
    pub replication_failed_size_v1: u64,
    // Total size for objects that have been replicated to destination
    pub replicated_size_v1: u64,
    // Total number of objects pending replication
    pub replication_pending_count_v1: u64,
    // Total number of objects that failed replication
    pub replication_failed_count_v1: u64,

    pub objects_count: u64,
    pub object_size_histogram: HashMap<String, u64>,
    pub object_versions_histogram: HashMap<String, u64>,
    pub versions_count: u64,
    pub delete_markers_count: u64,
    pub replica_size: u64,
    pub replica_count: u64,
    pub replication_info: HashMap<String, BucketTargetUsageInfo>,
}

/// DataUsageInfo represents data usage stats of the underlying storage
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DataUsageInfo {
    /// Total capacity
    pub total_capacity: u64,
    /// Total used capacity
    pub total_used_capacity: u64,
    /// Total free capacity
    pub total_free_capacity: u64,

    /// LastUpdate is the timestamp of when the data usage info was last updated
    pub last_update: Option<SystemTime>,

    /// Objects total count across all buckets
    pub objects_total_count: u64,
    /// Versions total count across all buckets
    pub versions_total_count: u64,
    /// Delete markers total count across all buckets
    pub delete_markers_total_count: u64,
    /// Objects total size across all buckets
    pub objects_total_size: u64,
    /// Replication info across all buckets
    pub replication_info: HashMap<String, BucketTargetUsageInfo>,

    /// Total number of buckets in this cluster
    pub buckets_count: u64,
    /// Buckets usage info provides following information across all buckets
    pub buckets_usage: HashMap<String, BucketUsageInfo>,
    /// Deprecated kept here for backward compatibility reasons
    pub bucket_sizes: HashMap<String, u64>,
}

/// Size summary for a single object or group of objects
#[derive(Debug, Default, Clone)]
pub struct SizeSummary {
    /// Total size
    pub total_size: usize,
    /// Number of versions
    pub versions: usize,
    /// Number of delete markers
    pub delete_markers: usize,
    /// Replicated size
    pub replicated_size: usize,
    /// Replicated count
    pub replicated_count: usize,
    /// Pending size
    pub pending_size: usize,
    /// Failed size
    pub failed_size: usize,
    /// Replica size
    pub replica_size: usize,
    /// Replica count
    pub replica_count: usize,
    /// Pending count
    pub pending_count: usize,
    /// Failed count
    pub failed_count: usize,
    /// Replication target stats
    pub repl_target_stats: HashMap<String, ReplTargetSizeSummary>,
}

/// Replication target size summary
#[derive(Debug, Default, Clone)]
pub struct ReplTargetSizeSummary {
    /// Replicated size
    pub replicated_size: usize,
    /// Replicated count
    pub replicated_count: usize,
    /// Pending size
    pub pending_size: usize,
    /// Failed size
    pub failed_size: usize,
    /// Pending count
    pub pending_count: usize,
    /// Failed count
    pub failed_count: usize,
}

impl DataUsageInfo {
    /// Create a new DataUsageInfo
    pub fn new() -> Self {
        Self::default()
    }

    /// Add object metadata to data usage statistics
    pub fn add_object(&mut self, object_path: &str, meta_object: &rustfs_filemeta::MetaObject) {
        // This method is kept for backward compatibility
        // For accurate version counting, use add_object_from_file_meta instead
        let bucket_name = match self.extract_bucket_from_path(object_path) {
            Ok(name) => name,
            Err(_) => return,
        };

        // Update bucket statistics
        if let Some(bucket_usage) = self.buckets_usage.get_mut(&bucket_name) {
            bucket_usage.size += meta_object.size as u64;
            bucket_usage.objects_count += 1;
            bucket_usage.versions_count += 1; // Simplified: assume 1 version per object

            // Update size histogram
            let total_size = meta_object.size as u64;
            let size_ranges = [
                ("0-1KB", 0, 1024),
                ("1KB-1MB", 1024, 1024 * 1024),
                ("1MB-10MB", 1024 * 1024, 10 * 1024 * 1024),
                ("10MB-100MB", 10 * 1024 * 1024, 100 * 1024 * 1024),
                ("100MB-1GB", 100 * 1024 * 1024, 1024 * 1024 * 1024),
                ("1GB+", 1024 * 1024 * 1024, u64::MAX),
            ];

            for (range_name, min_size, max_size) in size_ranges {
                if total_size >= min_size && total_size < max_size {
                    *bucket_usage.object_size_histogram.entry(range_name.to_string()).or_insert(0) += 1;
                    break;
                }
            }

            // Update version histogram (simplified - count as single version)
            *bucket_usage
                .object_versions_histogram
                .entry("SINGLE_VERSION".to_string())
                .or_insert(0) += 1;
        } else {
            // Create new bucket usage
            let mut bucket_usage = BucketUsageInfo {
                size: meta_object.size as u64,
                objects_count: 1,
                versions_count: 1,
                ..Default::default()
            };
            bucket_usage.object_size_histogram.insert("0-1KB".to_string(), 1);
            bucket_usage.object_versions_histogram.insert("SINGLE_VERSION".to_string(), 1);
            self.buckets_usage.insert(bucket_name, bucket_usage);
        }

        // Update global statistics
        self.objects_total_size += meta_object.size as u64;
        self.objects_total_count += 1;
        self.versions_total_count += 1;
    }

    /// Add object from FileMeta for accurate version counting
    pub fn add_object_from_file_meta(&mut self, object_path: &str, file_meta: &rustfs_filemeta::FileMeta) {
        let bucket_name = match self.extract_bucket_from_path(object_path) {
            Ok(name) => name,
            Err(_) => return,
        };

        // Calculate accurate statistics from all versions
        let mut total_size = 0u64;
        let mut versions_count = 0u64;
        let mut delete_markers_count = 0u64;
        let mut latest_object_size = 0u64;

        // Process all versions to get accurate counts
        for version in &file_meta.versions {
            match rustfs_filemeta::FileMetaVersion::try_from(version.clone()) {
                Ok(ver) => {
                    if let Some(obj) = ver.object {
                        total_size += obj.size as u64;
                        versions_count += 1;
                        latest_object_size = obj.size as u64; // Keep track of latest object size
                    } else if ver.delete_marker.is_some() {
                        delete_markers_count += 1;
                    }
                }
                Err(_) => {
                    // Skip invalid versions
                    continue;
                }
            }
        }

        // Update bucket statistics
        if let Some(bucket_usage) = self.buckets_usage.get_mut(&bucket_name) {
            bucket_usage.size += total_size;
            bucket_usage.objects_count += 1;
            bucket_usage.versions_count += versions_count;
            bucket_usage.delete_markers_count += delete_markers_count;

            // Update size histogram based on latest object size
            let size_ranges = [
                ("0-1KB", 0, 1024),
                ("1KB-1MB", 1024, 1024 * 1024),
                ("1MB-10MB", 1024 * 1024, 10 * 1024 * 1024),
                ("10MB-100MB", 10 * 1024 * 1024, 100 * 1024 * 1024),
                ("100MB-1GB", 100 * 1024 * 1024, 1024 * 1024 * 1024),
                ("1GB+", 1024 * 1024 * 1024, u64::MAX),
            ];

            for (range_name, min_size, max_size) in size_ranges {
                if latest_object_size >= min_size && latest_object_size < max_size {
                    *bucket_usage.object_size_histogram.entry(range_name.to_string()).or_insert(0) += 1;
                    break;
                }
            }

            // Update version histogram based on actual version count
            let version_ranges = [
                ("1", 1, 1),
                ("2-5", 2, 5),
                ("6-10", 6, 10),
                ("11-50", 11, 50),
                ("51-100", 51, 100),
                ("100+", 101, usize::MAX),
            ];

            for (range_name, min_versions, max_versions) in version_ranges {
                if versions_count as usize >= min_versions && versions_count as usize <= max_versions {
                    *bucket_usage
                        .object_versions_histogram
                        .entry(range_name.to_string())
                        .or_insert(0) += 1;
                    break;
                }
            }
        } else {
            // Create new bucket usage
            let mut bucket_usage = BucketUsageInfo {
                size: total_size,
                objects_count: 1,
                versions_count,
                delete_markers_count,
                ..Default::default()
            };

            // Set size histogram
            let size_ranges = [
                ("0-1KB", 0, 1024),
                ("1KB-1MB", 1024, 1024 * 1024),
                ("1MB-10MB", 1024 * 1024, 10 * 1024 * 1024),
                ("10MB-100MB", 10 * 1024 * 1024, 100 * 1024 * 1024),
                ("100MB-1GB", 100 * 1024 * 1024, 1024 * 1024 * 1024),
                ("1GB+", 1024 * 1024 * 1024, u64::MAX),
            ];

            for (range_name, min_size, max_size) in size_ranges {
                if latest_object_size >= min_size && latest_object_size < max_size {
                    bucket_usage.object_size_histogram.insert(range_name.to_string(), 1);
                    break;
                }
            }

            // Set version histogram
            let version_ranges = [
                ("1", 1, 1),
                ("2-5", 2, 5),
                ("6-10", 6, 10),
                ("11-50", 11, 50),
                ("51-100", 51, 100),
                ("100+", 101, usize::MAX),
            ];

            for (range_name, min_versions, max_versions) in version_ranges {
                if versions_count as usize >= min_versions && versions_count as usize <= max_versions {
                    bucket_usage.object_versions_histogram.insert(range_name.to_string(), 1);
                    break;
                }
            }

            self.buckets_usage.insert(bucket_name, bucket_usage);
            // Update buckets count when adding new bucket
            self.buckets_count = self.buckets_usage.len() as u64;
        }

        // Update global statistics
        self.objects_total_size += total_size;
        self.objects_total_count += 1;
        self.versions_total_count += versions_count;
        self.delete_markers_total_count += delete_markers_count;
    }

    /// Extract bucket name from object path
    fn extract_bucket_from_path(&self, object_path: &str) -> Result<String> {
        let parts: Vec<&str> = object_path.split('/').collect();
        if parts.is_empty() {
            return Err(Error::Scanner("Invalid object path: empty".to_string()));
        }
        Ok(parts[0].to_string())
    }

    /// Update capacity information
    pub fn update_capacity(&mut self, total: u64, used: u64, free: u64) {
        self.total_capacity = total;
        self.total_used_capacity = used;
        self.total_free_capacity = free;
        self.last_update = Some(SystemTime::now());
    }

    /// Add bucket usage info
    pub fn add_bucket_usage(&mut self, bucket: String, usage: BucketUsageInfo) {
        self.buckets_usage.insert(bucket.clone(), usage);
        self.buckets_count = self.buckets_usage.len() as u64;
        self.last_update = Some(SystemTime::now());
    }

    /// Get bucket usage info
    pub fn get_bucket_usage(&self, bucket: &str) -> Option<&BucketUsageInfo> {
        self.buckets_usage.get(bucket)
    }

    /// Calculate total statistics from all buckets
    pub fn calculate_totals(&mut self) {
        self.objects_total_count = 0;
        self.versions_total_count = 0;
        self.delete_markers_total_count = 0;
        self.objects_total_size = 0;

        for usage in self.buckets_usage.values() {
            self.objects_total_count += usage.objects_count;
            self.versions_total_count += usage.versions_count;
            self.delete_markers_total_count += usage.delete_markers_count;
            self.objects_total_size += usage.size;
        }
    }

    /// Merge another DataUsageInfo into this one
    pub fn merge(&mut self, other: &DataUsageInfo) {
        // Merge bucket usage
        for (bucket, usage) in &other.buckets_usage {
            if let Some(existing) = self.buckets_usage.get_mut(bucket) {
                existing.merge(usage);
            } else {
                self.buckets_usage.insert(bucket.clone(), usage.clone());
            }
        }

        // Recalculate totals
        self.calculate_totals();

        // Ensure buckets_count stays consistent with buckets_usage
        self.buckets_count = self.buckets_usage.len() as u64;

        // Update last update time
        if let Some(other_update) = other.last_update {
            if self.last_update.is_none() || other_update > self.last_update.unwrap() {
                self.last_update = Some(other_update);
            }
        }
    }
}

impl BucketUsageInfo {
    /// Create a new BucketUsageInfo
    pub fn new() -> Self {
        Self::default()
    }

    /// Add size summary to this bucket usage
    pub fn add_size_summary(&mut self, summary: &SizeSummary) {
        self.size += summary.total_size as u64;
        self.versions_count += summary.versions as u64;
        self.delete_markers_count += summary.delete_markers as u64;
        self.replica_size += summary.replica_size as u64;
        self.replica_count += summary.replica_count as u64;
    }

    /// Merge another BucketUsageInfo into this one
    pub fn merge(&mut self, other: &BucketUsageInfo) {
        self.size += other.size;
        self.objects_count += other.objects_count;
        self.versions_count += other.versions_count;
        self.delete_markers_count += other.delete_markers_count;
        self.replica_size += other.replica_size;
        self.replica_count += other.replica_count;

        // Merge histograms
        for (key, value) in &other.object_size_histogram {
            *self.object_size_histogram.entry(key.clone()).or_insert(0) += value;
        }

        for (key, value) in &other.object_versions_histogram {
            *self.object_versions_histogram.entry(key.clone()).or_insert(0) += value;
        }

        // Merge replication info
        for (target, info) in &other.replication_info {
            let entry = self.replication_info.entry(target.clone()).or_default();
            entry.replicated_size += info.replicated_size;
            entry.replica_size += info.replica_size;
            entry.replication_pending_size += info.replication_pending_size;
            entry.replication_failed_size += info.replication_failed_size;
            entry.replication_pending_count += info.replication_pending_count;
            entry.replication_failed_count += info.replication_failed_count;
            entry.replicated_count += info.replicated_count;
        }

        // Merge backward compatibility fields
        self.replication_pending_size_v1 += other.replication_pending_size_v1;
        self.replication_failed_size_v1 += other.replication_failed_size_v1;
        self.replicated_size_v1 += other.replicated_size_v1;
        self.replication_pending_count_v1 += other.replication_pending_count_v1;
        self.replication_failed_count_v1 += other.replication_failed_count_v1;
    }
}

impl SizeSummary {
    /// Create a new SizeSummary
    pub fn new() -> Self {
        Self::default()
    }

    /// Add another SizeSummary to this one
    pub fn add(&mut self, other: &SizeSummary) {
        self.total_size += other.total_size;
        self.versions += other.versions;
        self.delete_markers += other.delete_markers;
        self.replicated_size += other.replicated_size;
        self.replicated_count += other.replicated_count;
        self.pending_size += other.pending_size;
        self.failed_size += other.failed_size;
        self.replica_size += other.replica_size;
        self.replica_count += other.replica_count;
        self.pending_count += other.pending_count;
        self.failed_count += other.failed_count;

        // Merge replication target stats
        for (target, stats) in &other.repl_target_stats {
            let entry = self.repl_target_stats.entry(target.clone()).or_default();
            entry.replicated_size += stats.replicated_size;
            entry.replicated_count += stats.replicated_count;
            entry.pending_size += stats.pending_size;
            entry.failed_size += stats.failed_size;
            entry.pending_count += stats.pending_count;
            entry.failed_count += stats.failed_count;
        }
    }
}

/// Store data usage info to backend storage
pub async fn store_data_usage_in_backend(data_usage_info: DataUsageInfo, store: Arc<ECStore>) -> Result<()> {
    let data =
        serde_json::to_vec(&data_usage_info).map_err(|e| Error::Config(format!("Failed to serialize data usage info: {}", e)))?;

    // Save to backend using the same mechanism as original code
    rustfs_ecstore::config::com::save_config(store, &DATA_USAGE_OBJ_NAME_PATH, data)
        .await
        .map_err(Error::Storage)?;

    Ok(())
}

/// Load data usage info from backend storage
pub async fn load_data_usage_from_backend(store: Arc<ECStore>) -> Result<DataUsageInfo> {
    let buf = match read_config(store, &DATA_USAGE_OBJ_NAME_PATH).await {
        Ok(data) => data,
        Err(e) => {
            error!("Failed to read data usage info from backend: {}", e);
            if e == rustfs_ecstore::error::Error::ConfigNotFound {
                return Ok(DataUsageInfo::default());
            }
            return Err(Error::Storage(e));
        }
    };

    let mut data_usage_info: DataUsageInfo =
        serde_json::from_slice(&buf).map_err(|e| Error::Config(format!("Failed to deserialize data usage info: {}", e)))?;

    warn!("Loaded data usage info from backend {:?}", &data_usage_info);

    // Handle backward compatibility like original code
    if data_usage_info.buckets_usage.is_empty() {
        data_usage_info.buckets_usage = data_usage_info
            .bucket_sizes
            .iter()
            .map(|(bucket, &size)| {
                (
                    bucket.clone(),
                    BucketUsageInfo {
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

/// Example function showing how to use AHM data usage functionality
/// This demonstrates the integration pattern for DataUsageInfoHandler
pub async fn example_data_usage_integration() -> Result<()> {
    // Get the global storage instance
    let Some(store) = rustfs_ecstore::new_object_layer_fn() else {
        return Err(Error::Config("Storage not initialized".to_string()));
    };

    // Load data usage from backend (this replaces the original load_data_usage_from_backend)
    let data_usage = load_data_usage_from_backend(store).await?;

    info!(
        "Loaded data usage info: {} buckets, {} total objects",
        data_usage.buckets_count, data_usage.objects_total_count
    );

    // Example: Store updated data usage back to backend
    // This would typically be called by the scanner after collecting new statistics
    // store_data_usage_in_backend(data_usage, store).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_usage_info_creation() {
        let mut info = DataUsageInfo::new();
        info.update_capacity(1000, 500, 500);

        assert_eq!(info.total_capacity, 1000);
        assert_eq!(info.total_used_capacity, 500);
        assert_eq!(info.total_free_capacity, 500);
        assert!(info.last_update.is_some());
    }

    #[test]
    fn test_bucket_usage_info_merge() {
        let mut usage1 = BucketUsageInfo::new();
        usage1.size = 100;
        usage1.objects_count = 10;
        usage1.versions_count = 5;

        let mut usage2 = BucketUsageInfo::new();
        usage2.size = 200;
        usage2.objects_count = 20;
        usage2.versions_count = 10;

        usage1.merge(&usage2);

        assert_eq!(usage1.size, 300);
        assert_eq!(usage1.objects_count, 30);
        assert_eq!(usage1.versions_count, 15);
    }

    #[test]
    fn test_size_summary_add() {
        let mut summary1 = SizeSummary::new();
        summary1.total_size = 100;
        summary1.versions = 5;

        let mut summary2 = SizeSummary::new();
        summary2.total_size = 200;
        summary2.versions = 10;

        summary1.add(&summary2);

        assert_eq!(summary1.total_size, 300);
        assert_eq!(summary1.versions, 15);
    }
}
