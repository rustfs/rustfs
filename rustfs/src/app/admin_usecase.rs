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
use crate::capacity::CapacityDiskRef;
use crate::capacity::capacity_manager::{CapacityUpdate, DataSource, get_capacity_manager};
use crate::capacity::scan::{refresh_capacity_with_scope, select_capacity_refresh_disks};
use crate::error::ApiError;
use rustfs_common::data_usage::DataUsageInfo;
use rustfs_ecstore::admin_server_info::get_server_info;
use rustfs_ecstore::data_usage::load_data_usage_from_backend;
use rustfs_ecstore::endpoints::EndpointServerPools;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::pools::{PoolStatus, get_total_usable_capacity, get_total_usable_capacity_free};
use rustfs_ecstore::store_api::StorageAPI;
use rustfs_io_metrics::capacity_metrics::{
    record_capacity_cache_hit, record_capacity_cache_miss, record_capacity_cache_served, record_capacity_refresh_request,
    record_capacity_scan_mode,
};
use rustfs_madmin::{InfoMessage, StorageInfo};
use s3s::S3ErrorCode;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

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

fn capacity_disk_refs(disks: &[rustfs_madmin::Disk]) -> Vec<CapacityDiskRef> {
    disks
        .iter()
        .map(|disk| CapacityDiskRef {
            endpoint: disk.endpoint.clone(),
            drive_path: disk.drive_path.clone(),
        })
        .collect()
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

        // Use hybrid strategy for capacity calculation
        let capacity_manager = get_capacity_manager();

        // Check if we have a valid cache
        if let Some(cached) = capacity_manager.get_capacity().await {
            record_capacity_cache_hit();
            let cache_age = cached.last_update.elapsed();
            let fast_update_threshold = capacity_manager.get_config().fast_update_threshold;

            // If cache is fresh (< fast_update_threshold), use it directly
            if cache_age < fast_update_threshold {
                record_capacity_cache_served("fresh");
                info.total_used_capacity = cached.total_used;
                debug!(
                    "Using cached capacity: {} bytes (age: {:?}, source: {:?}, files={}, estimated={})",
                    cached.total_used, cache_age, cached.source, cached.file_count, cached.is_estimated
                );
            } else {
                // Cache is stale, check if we need fast update
                let needs_update = capacity_manager.needs_fast_update().await;
                let should_block = capacity_manager.should_block_on_refresh(cache_age);

                if needs_update && should_block {
                    let start = Instant::now();
                    record_capacity_refresh_request("blocking", DataSource::WriteTriggered.as_metric_label());
                    let capacity_disks = capacity_disk_refs(&storage_info.disks);
                    let (refresh_disks, dirty_subset) =
                        select_capacity_refresh_disks(capacity_manager.as_ref(), &capacity_disks).await;
                    match capacity_manager
                        .refresh_or_join(DataSource::WriteTriggered, move || {
                            let refresh_disks = refresh_disks.clone();
                            async move { refresh_capacity_with_scope(refresh_disks, dirty_subset).await }
                        })
                        .await
                    {
                        Ok(update) => {
                            info.total_used_capacity = update.total_used;

                            let elapsed = start.elapsed();
                            debug!(
                                "Foreground capacity refresh completed in {:?} (files={}, estimated={})",
                                elapsed, update.file_count, update.is_estimated
                            );
                        }
                        Err(e) => {
                            warn!("Foreground capacity refresh failed: {}, using cached value", e);
                            record_capacity_cache_served("stale");
                            info.total_used_capacity = cached.total_used;
                        }
                    }
                } else {
                    record_capacity_cache_served("stale");
                    info.total_used_capacity = cached.total_used;
                    debug!(
                        "Using stale cached capacity: {} bytes (age: {:?}, source: {:?}, files={}, estimated={}, needs_update={}, blocking={})",
                        cached.total_used,
                        cache_age,
                        cached.source,
                        cached.file_count,
                        cached.is_estimated,
                        needs_update,
                        should_block
                    );

                    let disks = capacity_disk_refs(&storage_info.disks);
                    let manager = capacity_manager.clone();
                    record_capacity_refresh_request("background", DataSource::Scheduled.as_metric_label());
                    let (refresh_disks, dirty_subset) = select_capacity_refresh_disks(manager.as_ref(), &disks).await;
                    if manager
                        .clone()
                        .spawn_refresh_if_needed(DataSource::Scheduled, move || async move {
                            refresh_capacity_with_scope(refresh_disks, dirty_subset).await
                        })
                        .await
                    {
                        debug!("Background capacity update started");
                    } else {
                        debug!("Background update already in progress, skipping spawn");
                    }
                }
            }
        } else {
            // No cache, perform initial calculation
            let start = Instant::now();
            record_capacity_cache_miss();
            record_capacity_refresh_request("initial", DataSource::RealTime.as_metric_label());
            match capacity_manager
                .refresh_or_join(DataSource::RealTime, || {
                    let disks = capacity_disk_refs(&storage_info.disks);
                    async move { refresh_capacity_with_scope(disks, false).await }
                })
                .await
            {
                Ok(update) => {
                    info.total_used_capacity = update.total_used;

                    let elapsed = start.elapsed();
                    info!(
                        "Initial capacity calculation completed: {} bytes in {:?} (files={}, estimated={})",
                        update.total_used, elapsed, update.file_count, update.is_estimated
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to calculate data directory used capacity: {}, falling back to disk used capacity",
                        e
                    );
                    record_capacity_cache_served("fallback");
                    record_capacity_scan_mode("fallback");
                    info.total_used_capacity = info.total_capacity.saturating_sub(info.total_free_capacity);
                    capacity_manager
                        .update_capacity(CapacityUpdate::fallback(info.total_used_capacity), DataSource::Fallback)
                        .await;
                }
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
}
