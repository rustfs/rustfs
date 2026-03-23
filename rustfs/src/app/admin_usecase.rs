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

//! Admin application use-case contracts.

use crate::app::context::{AppContext, get_global_app_context};
use crate::error::ApiError;
use rustfs_common::data_usage::DataUsageInfo;
use rustfs_ecstore::admin_server_info::get_server_info;
use rustfs_ecstore::data_usage::load_data_usage_from_backend;
use rustfs_ecstore::endpoints::EndpointServerPools;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::pools::{PoolStatus, get_total_usable_capacity, get_total_usable_capacity_free};
use rustfs_ecstore::store_api::StorageAPI;
use rustfs_madmin::{InfoMessage, StorageInfo};
use s3s::S3ErrorCode;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};
use walkdir::WalkDir;

pub type AdminUsecaseResult<T> = Result<T, ApiError>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryServerInfoRequest {
    pub include_pools: bool,
}

pub struct QueryServerInfoResponse {
    pub info: InfoMessage,
}

impl std::fmt::Debug for QueryServerInfoResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryServerInfoResponse").finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DependencyReadiness {
    pub storage_ready: bool,
    pub iam_ready: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryPoolStatusRequest {
    pub pool: String,
    pub by_id: bool,
}

// Performance protection constants
const MAX_FILES_THRESHOLD: usize = 1_000_000; // 1M files threshold
const STAT_TIMEOUT_SECS: u64 = 5; // Statistics timeout
const SAMPLE_RATE: usize = 100; // Sample rate (1 in every 100 files)

/// Calculate actual used capacity of all data directories
async fn calculate_data_dir_used_capacity(
    disks: &[rustfs_madmin::Disk],
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let mut total_used = 0u64;
    let mut has_failure = false;

    for disk in disks {
        let path = Path::new(&disk.drive_path);

        // Check if path exists
        if !path.exists() {
            warn!("Data directory does not exist: {}", disk.drive_path);
            has_failure = true;
            continue;
        }

        // Asynchronously calculate directory size
        match get_dir_size_async(path).await {
            Ok(size) => {
                debug!("Data directory {} size: {} bytes", disk.drive_path, size);
                total_used += size;
            }
            Err(e) => {
                warn!("Failed to get size for directory {}: {:?}", disk.drive_path, e);
                has_failure = true;
                // Continue with other directories
            }
        }
    }

    // Log warning if there were failures
    if has_failure {
        warn!("Some directories failed to calculate size, result may be incomplete");
    }

    Ok(total_used)
}

/// Asynchronously get directory size (using walkdir for efficient traversal, with performance protection)
async fn get_dir_size_async(path: &Path) -> Result<u64, std::io::Error> {
    let path = path.to_path_buf();

    // Use tokio::task::spawn_blocking to avoid blocking the async runtime
    tokio::task::spawn_blocking(move || {
        let start_time = Instant::now();
        let mut total_size = 0u64;
        let mut file_count = 0usize;
        let mut sampled_size = 0u64;
        let mut sampled_count = 0usize;

        // Use walkdir to traverse directory tree
        let walker = WalkDir::new(&path)
            .follow_links(false) // Don't follow symbolic links
            .into_iter()
            .filter_map(|e| e.ok());

        for entry in walker {
            // Check timeout
            if start_time.elapsed() > Duration::from_secs(STAT_TIMEOUT_SECS) {
                warn!("Directory size calculation timeout after {} files, using sampled estimate", file_count);
                // Use sampling estimate: sampled_size * total_files / sampled_files
                if sampled_count > 0 {
                    return Ok(sampled_size * file_count as u64 / sampled_count as u64);
                }
                return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "Directory size calculation timeout"));
            }

            // Only count file sizes, ignore directories
            if let Ok(metadata) = entry.metadata()
                && metadata.is_file() {
                    file_count += 1;

                    // When file count exceeds threshold, enable sampling
                    if file_count > MAX_FILES_THRESHOLD {
                        // Sampling: count 1 in every SAMPLE_RATE files
                        if file_count.is_multiple_of(SAMPLE_RATE) {
                            sampled_size += metadata.len();
                            sampled_count += 1;
                        }

                        // Log progress every 100k files
                        if file_count.is_multiple_of(100_000) {
                            debug!(
                                "Processed {} files, sampled {} files, size: {} bytes",
                                file_count, sampled_count, sampled_size
                            );
                        }
                    } else {
                        // Below threshold, full statistics
                        total_size += metadata.len();
                    }
                }
        }

        // If sampling was enabled, return estimated value
        if file_count > MAX_FILES_THRESHOLD && sampled_count > 0 {
            let estimated_size = sampled_size * file_count as u64 / sampled_count as u64;
            info!(
                "Large directory detected: {} files, estimated size: {} bytes (sampled {}/{} files)",
                file_count, estimated_size, sampled_count, file_count
            );
            Ok(estimated_size)
        } else {
            debug!(
                "Directory size calculation completed: {} files, {} bytes, took {:?}",
                file_count,
                total_size,
                start_time.elapsed()
            );
            Ok(total_size)
        }
    })
    .await
    .map_err(std::io::Error::other)?
}

#[derive(Clone, Default)]
pub struct DefaultAdminUsecase {
    context: Option<Arc<AppContext>>,
}

impl DefaultAdminUsecase {
    #[cfg(test)]
    pub fn without_context() -> Self {
        Self { context: None }
    }

    pub fn from_global() -> Self {
        Self {
            context: get_global_app_context(),
        }
    }

    fn endpoints(&self) -> Option<EndpointServerPools> {
        self.context.as_ref().and_then(|context| context.endpoints().handle())
    }

    fn app_error(code: S3ErrorCode, message: impl Into<String>) -> ApiError {
        ApiError {
            code,
            message: message.into(),
            source: None,
        }
    }

    fn app_error_default(code: S3ErrorCode) -> ApiError {
        let message = ApiError::error_code_to_message(&code);
        Self::app_error(code, message)
    }

    pub async fn execute_query_server_info(&self, req: QueryServerInfoRequest) -> AdminUsecaseResult<QueryServerInfoResponse> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let info = get_server_info(req.include_pools).await;
        Ok(QueryServerInfoResponse { info })
    }

    pub async fn execute_query_storage_info(&self) -> AdminUsecaseResult<StorageInfo> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(Self::app_error(S3ErrorCode::InternalError, "Not init"));
        };

        Ok(store.storage_info().await)
    }

    pub async fn execute_query_data_usage_info(&self) -> AdminUsecaseResult<DataUsageInfo> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(Self::app_error(S3ErrorCode::InternalError, "Not init"));
        };

        let mut info = load_data_usage_from_backend(store.clone()).await.map_err(|e| {
            error!("load_data_usage_from_backend failed {:?}", e);
            Self::app_error(S3ErrorCode::InternalError, "load_data_usage_from_backend failed")
        })?;

        let storage_info = store.storage_info().await;

        // Keep the same capacity correction behavior as the previous admin handler implementation.
        const MAX_REASONABLE_CAPACITY: u64 = 100_000 * 1024 * 1024 * 1024 * 1024; // 100 PiB
        const MIN_REASONABLE_CAPACITY: u64 = 1024 * 1024 * 1024; // 1 GiB

        let total_u64 = get_total_usable_capacity(&storage_info.disks, &storage_info) as u64;
        let free_u64 = get_total_usable_capacity_free(&storage_info.disks, &storage_info) as u64;

        if total_u64 > MAX_REASONABLE_CAPACITY {
            error!(
                "Abnormal total capacity detected: {} bytes ({:.2} TiB), capping to physical capacity",
                total_u64,
                total_u64 as f64 / (1024.0_f64.powi(4))
            );

            let disk_count = storage_info.disks.len();
            if disk_count > 0 {
                use std::collections::HashSet;
                let unique_disks: HashSet<String> = storage_info
                    .disks
                    .iter()
                    .map(|disk| format!("{}|{}", disk.endpoint, disk.drive_path))
                    .collect();

                let actual_disk_count = unique_disks.len();

                if let Some(first_disk) = storage_info.disks.first() {
                    info.total_capacity = first_disk.total_space * actual_disk_count as u64;
                    info.total_free_capacity = first_disk.available_space * actual_disk_count as u64;

                    info!(
                        "Applied capacity correction: {} unique disks, capacity per disk: {} bytes",
                        actual_disk_count, first_disk.total_space
                    );
                } else {
                    info.total_capacity = 0;
                    info.total_free_capacity = 0;
                }
            } else {
                info.total_capacity = 0;
                info.total_free_capacity = 0;
            }
        } else if total_u64 < MIN_REASONABLE_CAPACITY && total_u64 > 0 {
            warn!(
                "Unusually small total capacity: {} bytes ({:.2} GiB)",
                total_u64,
                total_u64 as f64 / (1024.0_f64.powi(3))
            );
            info.total_capacity = total_u64;
            info.total_free_capacity = free_u64;
        } else {
            info.total_capacity = total_u64;
            info.total_free_capacity = free_u64;
        }

        // Calculate actual data directory used capacity with performance protection
        let start = Instant::now();
        match calculate_data_dir_used_capacity(&storage_info.disks).await {
            Ok(used_capacity) => {
                info.total_used_capacity = used_capacity;

                // Log calculation time
                let elapsed = start.elapsed();
                if elapsed > Duration::from_secs(1) {
                    warn!("Capacity calculation took {:?}, which is slower than expected", elapsed);
                } else {
                    debug!("Capacity calculation took {:?}", elapsed);
                }

                // Log capacity comparison
                let disk_used = info.total_capacity.saturating_sub(info.total_free_capacity);
                info!(
                    "Capacity statistics: data_dir_used={}, disk_used={}, difference={}",
                    used_capacity,
                    disk_used,
                    i64::try_from(used_capacity).unwrap_or(i64::MAX) - i64::try_from(disk_used).unwrap_or(i64::MAX)
                );
            }
            Err(e) => {
                warn!(
                    "Failed to calculate data directory used capacity: {:?}, falling back to disk used capacity",
                    e
                );
                // Fallback: use disk used capacity
                info.total_used_capacity = info.total_capacity.saturating_sub(info.total_free_capacity);
            }
        }

        debug!(
            "Capacity statistics: total={:.2} TiB, free={:.2} TiB, used={:.2} TiB",
            info.total_capacity as f64 / (1024.0_f64.powi(4)),
            info.total_free_capacity as f64 / (1024.0_f64.powi(4)),
            info.total_used_capacity as f64 / (1024.0_f64.powi(4))
        );

        Ok(info)
    }

    pub async fn execute_list_pool_statuses(&self) -> AdminUsecaseResult<Vec<PoolStatus>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(Self::app_error(S3ErrorCode::InternalError, "Not init"));
        };

        let Some(endpoints) = self.endpoints() else {
            return Err(Self::app_error_default(S3ErrorCode::NotImplemented));
        };

        if endpoints.legacy() {
            return Err(Self::app_error_default(S3ErrorCode::NotImplemented));
        }

        let mut pool_statuses = Vec::new();
        for (idx, _) in endpoints.as_ref().iter().enumerate() {
            let state = store.status(idx).await.map_err(ApiError::from)?;
            pool_statuses.push(state);
        }

        Ok(pool_statuses)
    }

    pub async fn execute_query_pool_status(&self, req: QueryPoolStatusRequest) -> AdminUsecaseResult<PoolStatus> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let Some(endpoints) = self.endpoints() else {
            return Err(Self::app_error_default(S3ErrorCode::NotImplemented));
        };

        if endpoints.legacy() {
            return Err(Self::app_error_default(S3ErrorCode::NotImplemented));
        }

        let has_idx = if req.by_id {
            let idx = req.pool.parse::<usize>().unwrap_or_default();
            if idx < endpoints.as_ref().len() { Some(idx) } else { None }
        } else {
            endpoints.get_pool_idx(&req.pool)
        };

        let Some(idx) = has_idx else {
            warn!("specified pool {} not found, please specify a valid pool", req.pool);
            return Err(Self::app_error_default(S3ErrorCode::InvalidArgument));
        };

        let Some(store) = new_object_layer_fn() else {
            return Err(Self::app_error(S3ErrorCode::InternalError, "Not init"));
        };

        store.status(idx).await.map_err(ApiError::from)
    }

    pub fn execute_collect_dependency_readiness(&self) -> DependencyReadiness {
        let iam_ready = self
            .context
            .as_ref()
            .map(|context| {
                let _ = context.object_store();
                context.iam().is_ready()
            })
            .unwrap_or(false);

        DependencyReadiness {
            storage_ready: new_object_layer_fn().is_some(),
            iam_ready,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn execute_query_storage_info_returns_internal_error_when_store_uninitialized() {
        let usecase = DefaultAdminUsecase::without_context();

        let err = usecase.execute_query_storage_info().await.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_query_data_usage_info_returns_internal_error_when_store_uninitialized() {
        let usecase = DefaultAdminUsecase::without_context();

        let err = usecase.execute_query_data_usage_info().await.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InternalError);
    }

    #[test]
    fn execute_collect_dependency_readiness_returns_state_flags() {
        let usecase = DefaultAdminUsecase::without_context();

        let readiness = usecase.execute_collect_dependency_readiness();
        let _ = readiness.storage_ready;
        let _ = readiness.iam_ready;
    }

    // Tests for directory size calculation functions
    #[tokio::test]
    async fn test_get_dir_size_async_empty_directory() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let size = get_dir_size_async(temp_dir.path()).await.unwrap();
        assert_eq!(size, 0);
    }

    #[tokio::test]
    async fn test_get_dir_size_async_single_file() {
        use std::fs::File;
        use std::io::Write;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        let mut file = File::create(&file_path).unwrap();
        file.write_all(b"Hello, World!").unwrap();

        let size = get_dir_size_async(temp_dir.path()).await.unwrap();
        assert_eq!(size, 13);
    }

    #[tokio::test]
    async fn test_get_dir_size_async_multiple_files() {
        use std::fs::File;
        use std::io::Write;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        // Create multiple files
        for i in 0..10 {
            let file_path = temp_dir.path().join(format!("file_{}.txt", i));
            let mut file = File::create(&file_path).unwrap();
            file.write_all(b"test").unwrap();
        }

        let size = get_dir_size_async(temp_dir.path()).await.unwrap();
        assert_eq!(size, 40); // 10 files * 4 bytes
    }

    #[tokio::test]
    async fn test_get_dir_size_async_nested_directories() {
        use std::fs::File;
        use std::io::Write;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        // Create nested directories and files
        let subdir = temp_dir.path().join("subdir");
        std::fs::create_dir(&subdir).unwrap();

        let file1 = temp_dir.path().join("file1.txt");
        let mut f1 = File::create(&file1).unwrap();
        f1.write_all(b"content1").unwrap();

        let file2 = subdir.join("file2.txt");
        let mut f2 = File::create(&file2).unwrap();
        f2.write_all(b"content2").unwrap();

        let size = get_dir_size_async(temp_dir.path()).await.unwrap();
        assert_eq!(size, 16); // "content1" (8) + "content2" (8)
    }

    #[tokio::test]
    async fn test_get_dir_size_async_nonexistent_directory() {
        let result = get_dir_size_async(Path::new("/nonexistent/path")).await;
        assert!(result.is_err());
    }
}
