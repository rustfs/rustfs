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

use super::storage_api::StorageAdminApi;
use super::storage_api::admin::get_server_info;
use super::storage_api::capacity::{
    PoolDecommissionInfo, PoolStatus, RebalStatus, get_total_usable_capacity, get_total_usable_capacity_free,
};
use super::storage_api::data_usage::{apply_bucket_usage_memory_overlay, load_data_usage_from_backend};
use super::storage_api::{ECStore, EndpointServerPools};
use crate::app::runtime_sources::{
    AppContext, current_app_context, resolve_endpoints_handle, resolve_object_store_handle_for_context,
};
use crate::capacity::resolve_admin_used_capacity;
use crate::cluster_snapshot::{ClusterReadOnlySnapshot, collect_cluster_read_only_snapshot};
use crate::error::ApiError;
use crate::server::{DependencyReadiness, collect_dependency_readiness as collect_runtime_dependency_readiness};
use rustfs_data_usage::DataUsageInfo;
use rustfs_madmin::{InfoMessage, StorageInfo};
use s3s::S3ErrorCode;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

pub type AdminUsecaseResult<T> = Result<T, ApiError>;
pub const ADMIN_CLUSTER_SNAPSHOT_ROUTE: &str = "/rustfs/admin/v4/cluster/snapshot";
pub const ADMIN_EXTENSIONS_CATALOG_ROUTE: &str = "/rustfs/admin/v4/extensions/catalog";
pub const ADMIN_RUNTIME_CAPABILITIES_ROUTE: &str = "/rustfs/admin/v4/runtime/capabilities";

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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryPoolStatusRequest {
    pub pool: String,
    pub by_id: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct AdminPoolDecommissionInfo {
    #[serde(rename = "startTime", with = "time::serde::rfc3339::option")]
    pub start_time: Option<time::OffsetDateTime>,
    #[serde(rename = "startSize")]
    pub start_size: usize,
    #[serde(rename = "totalSize")]
    pub total_size: usize,
    #[serde(rename = "currentSize")]
    pub current_size: usize,
    #[serde(rename = "complete")]
    pub complete: bool,
    #[serde(rename = "failed")]
    pub failed: bool,
    #[serde(rename = "canceled")]
    pub canceled: bool,
    #[serde(rename = "queued")]
    pub queued: bool,
    #[serde(rename = "queuedBuckets")]
    pub queued_buckets: Vec<String>,
    #[serde(rename = "decommissionedBuckets")]
    pub decommissioned_buckets: Vec<String>,
    #[serde(rename = "bucket")]
    pub bucket: String,
    #[serde(rename = "prefix")]
    pub prefix: String,
    #[serde(rename = "object")]
    pub object: String,
    #[serde(rename = "stage")]
    pub stage: String,
    #[serde(rename = "objectsDecommissioned")]
    pub items_decommissioned: usize,
    #[serde(rename = "objectsDecommissionedFailed")]
    pub items_decommission_failed: usize,
    #[serde(rename = "bytesDecommissioned")]
    pub bytes_done: usize,
    #[serde(rename = "bytesDecommissionedFailed")]
    pub bytes_failed: usize,
    #[serde(rename = "waitingReason")]
    pub waiting_reason: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct AdminPoolStatus {
    #[serde(rename = "id")]
    pub id: usize,
    #[serde(rename = "cmdline")]
    pub cmd_line: String,
    #[serde(rename = "lastUpdate", with = "time::serde::rfc3339")]
    pub last_update: time::OffsetDateTime,
    #[serde(rename = "totalSize")]
    pub total_size: usize,
    #[serde(rename = "currentSize")]
    pub current_size: usize,
    #[serde(rename = "usedSize")]
    pub used_size: usize,
    #[serde(rename = "used")]
    pub used: f64,
    #[serde(rename = "status")]
    pub status: String,
    #[serde(rename = "decommissionStatus")]
    pub decommission_status: String,
    #[serde(rename = "rebalanceStatus")]
    pub rebalance_status: String,
    #[serde(rename = "decommissionInfo")]
    pub decommission: Option<AdminPoolDecommissionInfo>,
}

pub type AdminPoolListItem = AdminPoolStatus;

#[derive(Debug, Clone, serde::Serialize)]
pub struct AdminDecommissionPoolStatus {
    #[serde(rename = "id")]
    pub id: usize,
    #[serde(rename = "cmdline")]
    pub cmd_line: String,
    #[serde(rename = "status")]
    pub status: String,
    #[serde(rename = "poolStatus")]
    pub pool_status: String,
    #[serde(rename = "decommissionInfo")]
    pub decommission: Option<AdminPoolDecommissionInfo>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct AdminDecommissionStatus {
    #[serde(rename = "pools")]
    pub pools: Vec<AdminDecommissionPoolStatus>,
}

#[derive(Clone, Default)]
pub struct DefaultAdminUsecase {
    context: Option<Arc<AppContext>>,
}

impl DefaultAdminUsecase {
    const POOL_STATUS_CANCELED: &'static str = "canceled";
    const POOL_STATUS_COMPLETE: &'static str = "complete";
    const POOL_STATUS_FAILED: &'static str = "failed";
    const POOL_STATUS_QUEUED: &'static str = "queued";
    const POOL_STATUS_RUNNING: &'static str = "running";
    const POOL_STATUS_UNKNOWN: &'static str = "unknown";
    const POOL_STATE_ACTIVE: &'static str = "active";
    const POOL_STATE_BLOCKED: &'static str = "blocked";
    const POOL_STATE_DECOMMISSIONED: &'static str = "decommissioned";
    const POOL_STATE_DECOMMISSIONING: &'static str = "decommissioning";
    const POOL_STATE_REBALANCING: &'static str = "rebalancing";
    const REBALANCE_STATUS_COMPLETED: &'static str = "completed";
    const REBALANCE_STATUS_FAILED: &'static str = "failed";
    const REBALANCE_STATUS_NONE: &'static str = "none";
    const REBALANCE_STATUS_STARTED: &'static str = "started";
    const REBALANCE_STATUS_STOPPING: &'static str = "stopping";
    const REBALANCE_STATUS_STOPPED: &'static str = "stopped";

    #[cfg(test)]
    pub fn without_context() -> Self {
        Self { context: None }
    }

    pub fn from_global() -> Self {
        Self {
            context: current_app_context(),
        }
    }

    fn endpoints(&self) -> Option<EndpointServerPools> {
        self.context.as_ref().and_then(|context| context.endpoints().handle())
    }

    fn object_store(&self) -> Option<Arc<ECStore>> {
        resolve_object_store_handle_for_context(self.context.as_deref())
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

    async fn refresh_rebalance_status_snapshot(store: &ECStore) -> AdminUsecaseResult<()> {
        store.refresh_rebalance_status_meta().await.map_err(|err| {
            error!("refresh rebalance metadata for pool status failed: {:?}", err);
            ApiError::from(err)
        })
    }

    async fn refresh_pool_status_snapshot(store: &ECStore) -> AdminUsecaseResult<()> {
        store.refresh_pool_status_meta().await.map_err(|err| {
            error!("refresh pool metadata for pool status failed: {:?}", err);
            ApiError::from(err)
        })
    }

    pub async fn execute_query_server_info(&self, req: QueryServerInfoRequest) -> AdminUsecaseResult<QueryServerInfoResponse> {
        let info = get_server_info(req.include_pools).await;
        Ok(QueryServerInfoResponse { info })
    }

    pub async fn execute_query_storage_info(&self) -> AdminUsecaseResult<StorageInfo> {
        let Some(store) = self.object_store() else {
            return Err(Self::app_error(S3ErrorCode::InternalError, "Not init"));
        };

        Ok(StorageAdminApi::storage_info(store.as_ref()).await)
    }

    pub async fn execute_query_data_usage_info(&self) -> AdminUsecaseResult<DataUsageInfo> {
        let Some(store) = self.object_store() else {
            return Err(Self::app_error(S3ErrorCode::InternalError, "Not init"));
        };

        let mut info = load_data_usage_from_backend(store.clone()).await.map_err(|e| {
            error!("load_data_usage_from_backend failed {:?}", e);
            Self::app_error(S3ErrorCode::InternalError, "load_data_usage_from_backend failed")
        })?;
        apply_bucket_usage_memory_overlay(&mut info).await;

        let storage_info = StorageAdminApi::storage_info(store.as_ref()).await;

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

        info.total_used_capacity =
            resolve_admin_used_capacity(&storage_info.disks, info.total_capacity.saturating_sub(info.total_free_capacity)).await;
        debug!(
            "Capacity statistics: total={:.2} TiB, free={:.2} TiB, used={:.2} TiB",
            info.total_capacity as f64 / (1024.0_f64.powi(4)),
            info.total_free_capacity as f64 / (1024.0_f64.powi(4)),
            info.total_used_capacity as f64 / (1024.0_f64.powi(4))
        );

        Ok(info)
    }

    pub async fn execute_list_pool_statuses(&self) -> AdminUsecaseResult<Vec<PoolStatus>> {
        let Some(store) = self.object_store() else {
            return Err(Self::app_error(S3ErrorCode::InternalError, "Not init"));
        };

        let Some(endpoints) = self.endpoints() else {
            return Err(Self::app_error_default(S3ErrorCode::NotImplemented));
        };

        if endpoints.legacy() {
            return Err(Self::app_error_default(S3ErrorCode::NotImplemented));
        }

        Self::refresh_pool_status_snapshot(store.as_ref()).await?;

        let mut pool_statuses = Vec::new();
        for (idx, _) in endpoints.as_ref().iter().enumerate() {
            let state = store.status(idx).await.map_err(ApiError::from)?;
            pool_statuses.push(state);
        }

        Ok(pool_statuses)
    }

    pub async fn execute_list_pools(&self) -> AdminUsecaseResult<Vec<AdminPoolListItem>> {
        let Some(store) = self.object_store() else {
            return Err(Self::app_error(S3ErrorCode::InternalError, "Not init"));
        };
        let pool_statuses = self.execute_list_pool_statuses().await?;
        Self::refresh_rebalance_status_snapshot(store.as_ref()).await?;
        let mut items = Vec::with_capacity(pool_statuses.len());
        for status in pool_statuses {
            let rebalance_status = store.pool_rebalance_status(status.id).await;
            items.push(Self::pool_list_item_from_status(status, rebalance_status));
        }
        Ok(items)
    }

    fn resolve_pool_index(&self, req: &QueryPoolStatusRequest, endpoints: &EndpointServerPools) -> AdminUsecaseResult<usize> {
        let has_idx = if req.by_id {
            Self::parse_pool_idx_by_id(&req.pool, endpoints.as_ref().len())
        } else {
            endpoints.get_pool_idx(&req.pool)
        };

        let Some(idx) = has_idx else {
            warn!("specified pool {} not found, please specify a valid pool", req.pool);
            return Err(Self::app_error_default(S3ErrorCode::InvalidArgument));
        };

        Ok(idx)
    }

    pub async fn execute_query_pool_status(&self, req: QueryPoolStatusRequest) -> AdminUsecaseResult<AdminPoolStatus> {
        let Some(endpoints) = self.endpoints() else {
            return Err(Self::app_error_default(S3ErrorCode::NotImplemented));
        };

        if endpoints.legacy() {
            return Err(Self::app_error_default(S3ErrorCode::NotImplemented));
        }

        let idx = self.resolve_pool_index(&req, &endpoints)?;

        let Some(store) = self.object_store() else {
            return Err(Self::app_error(S3ErrorCode::InternalError, "Not init"));
        };

        Self::refresh_pool_status_snapshot(store.as_ref()).await?;
        let status = store.status(idx).await.map_err(ApiError::from)?;
        Self::refresh_rebalance_status_snapshot(store.as_ref()).await?;
        let rebalance_status = store.pool_rebalance_status(idx).await;
        Ok(Self::pool_list_item_from_status(status, rebalance_status))
    }

    pub async fn execute_list_decommission_status(&self) -> AdminUsecaseResult<AdminDecommissionStatus> {
        let Some(store) = self.object_store() else {
            return Err(Self::app_error(S3ErrorCode::InternalError, "Not init"));
        };
        let pool_statuses = self.execute_list_pool_statuses().await?;
        Self::refresh_rebalance_status_snapshot(store.as_ref()).await?;
        let mut pools = Vec::with_capacity(pool_statuses.len());
        for status in pool_statuses {
            let rebalance_status = store.pool_rebalance_status(status.id).await;
            pools.push(Self::decommission_pool_status_from_status(status, rebalance_status));
        }
        Ok(AdminDecommissionStatus { pools })
    }

    pub async fn execute_query_decommission_status(
        &self,
        req: QueryPoolStatusRequest,
    ) -> AdminUsecaseResult<AdminDecommissionPoolStatus> {
        let Some(endpoints) = self.endpoints() else {
            return Err(Self::app_error_default(S3ErrorCode::NotImplemented));
        };

        if endpoints.legacy() {
            return Err(Self::app_error_default(S3ErrorCode::NotImplemented));
        }

        let idx = self.resolve_pool_index(&req, &endpoints)?;

        let Some(store) = self.object_store() else {
            return Err(Self::app_error(S3ErrorCode::InternalError, "Not init"));
        };

        Self::refresh_pool_status_snapshot(store.as_ref()).await?;
        let status = store.status(idx).await.map_err(ApiError::from)?;
        Self::refresh_rebalance_status_snapshot(store.as_ref()).await?;
        let rebalance_status = store.pool_rebalance_status(idx).await;
        Ok(Self::decommission_pool_status_from_status(status, rebalance_status))
    }

    fn pool_list_item_from_status(status: PoolStatus, rebalance_status: (RebalStatus, bool)) -> AdminPoolListItem {
        let PoolStatus {
            id,
            cmd_line,
            last_update,
            decommission,
        } = status;
        let total_size = decommission.as_ref().map(|info| info.total_size).unwrap_or_default();
        let current_size = decommission.as_ref().map(|info| info.current_size).unwrap_or_default();
        let used_size = total_size.saturating_sub(current_size);
        let decommission = decommission.filter(PoolDecommissionInfo::has_decommission_state);
        let decommission_status = Self::pool_decommission_status(decommission.as_ref());
        let rebalance_status = Self::pool_rebalance_status(rebalance_status);
        let pool_state = Self::pool_lifecycle_state(decommission.as_ref(), rebalance_status);

        AdminPoolStatus {
            id,
            cmd_line,
            last_update,
            total_size,
            current_size,
            used_size,
            used: Self::used_ratio(total_size, used_size),
            status: pool_state.to_string(),
            decommission_status: decommission_status.to_string(),
            rebalance_status: rebalance_status.to_string(),
            decommission: decommission.map(Self::admin_decommission_info_from_pool),
        }
    }

    fn pool_lifecycle_state(decommission: Option<&PoolDecommissionInfo>, rebalance_status: &'static str) -> &'static str {
        match decommission {
            Some(info) if info.complete => Self::POOL_STATE_DECOMMISSIONED,
            Some(info) if info.failed || info.canceled => Self::POOL_STATE_BLOCKED,
            Some(info) if !info.has_decommission_state() && Self::rebalance_status_is_active(rebalance_status) => {
                Self::POOL_STATE_REBALANCING
            }
            Some(info) if !info.has_decommission_state() => Self::POOL_STATE_ACTIVE,
            Some(_) => Self::POOL_STATE_DECOMMISSIONING,
            None if Self::rebalance_status_is_active(rebalance_status) => Self::POOL_STATE_REBALANCING,
            None => Self::POOL_STATE_ACTIVE,
        }
    }

    fn pool_decommission_status(decommission: Option<&PoolDecommissionInfo>) -> &'static str {
        match decommission {
            Some(info) if info.complete => Self::POOL_STATUS_COMPLETE,
            Some(info) if info.failed => Self::POOL_STATUS_FAILED,
            Some(info) if info.canceled => Self::POOL_STATUS_CANCELED,
            Some(info) if info.queued => Self::POOL_STATUS_QUEUED,
            Some(info) if info.start_time.is_some() => Self::POOL_STATUS_RUNNING,
            Some(info) if !info.has_decommission_state() => Self::REBALANCE_STATUS_NONE,
            Some(_) => Self::POOL_STATUS_UNKNOWN,
            None => Self::REBALANCE_STATUS_NONE,
        }
    }

    fn rebalance_status_is_active(status: &'static str) -> bool {
        matches!(status, Self::REBALANCE_STATUS_STARTED | Self::REBALANCE_STATUS_STOPPING)
    }

    fn pool_rebalance_status((status, stopping): (RebalStatus, bool)) -> &'static str {
        if stopping {
            return Self::REBALANCE_STATUS_STOPPING;
        }

        match status {
            RebalStatus::None => Self::REBALANCE_STATUS_NONE,
            RebalStatus::Started => Self::REBALANCE_STATUS_STARTED,
            RebalStatus::Completed => Self::REBALANCE_STATUS_COMPLETED,
            RebalStatus::Stopped => Self::REBALANCE_STATUS_STOPPED,
            RebalStatus::Failed => Self::REBALANCE_STATUS_FAILED,
        }
    }

    fn decommission_pool_status_from_status(
        status: PoolStatus,
        rebalance_status: (RebalStatus, bool),
    ) -> AdminDecommissionPoolStatus {
        let PoolStatus {
            id,
            cmd_line,
            decommission,
            ..
        } = status;
        let decommission = decommission.filter(PoolDecommissionInfo::has_decommission_state);
        let status = Self::pool_decommission_status(decommission.as_ref()).to_string();
        let rebalance_status = Self::pool_rebalance_status(rebalance_status);
        let pool_status = Self::pool_lifecycle_state(decommission.as_ref(), rebalance_status).to_string();

        AdminDecommissionPoolStatus {
            id,
            cmd_line,
            status,
            pool_status,
            decommission: decommission.map(Self::admin_decommission_info_from_pool),
        }
    }

    fn admin_decommission_info_from_pool(info: PoolDecommissionInfo) -> AdminPoolDecommissionInfo {
        let waiting_reason = Self::decommission_waiting_reason(&info).map(str::to_string);
        AdminPoolDecommissionInfo {
            start_time: info.start_time,
            start_size: info.start_size,
            total_size: info.total_size,
            current_size: info.current_size,
            complete: info.complete,
            failed: info.failed,
            canceled: info.canceled,
            queued: info.queued,
            queued_buckets: info.queued_buckets,
            decommissioned_buckets: info.decommissioned_buckets,
            bucket: info.bucket,
            prefix: info.prefix,
            object: info.object,
            stage: info.stage,
            items_decommissioned: info.items_decommissioned,
            items_decommission_failed: info.items_decommission_failed,
            bytes_done: info.bytes_done,
            bytes_failed: info.bytes_failed,
            waiting_reason,
        }
    }

    fn decommission_waiting_reason(info: &PoolDecommissionInfo) -> Option<&'static str> {
        if !info.has_decommission_state() || info.complete || info.failed || info.canceled || info.start_time.is_some() {
            return None;
        }
        if info.queued {
            return Some("queued");
        }
        Some("waiting_for_worker")
    }

    fn used_ratio(total_size: usize, used_size: usize) -> f64 {
        if total_size == 0 {
            return 0.0;
        }

        used_size as f64 / total_size as f64
    }

    fn parse_pool_idx_by_id(pool: &str, endpoint_count: usize) -> Option<usize> {
        let idx = pool.parse::<usize>().ok()?;
        (idx < endpoint_count).then_some(idx)
    }

    pub async fn execute_collect_dependency_readiness(&self) -> DependencyReadiness {
        collect_runtime_dependency_readiness().await
    }

    pub async fn execute_collect_cluster_read_only_snapshot(&self) -> Option<ClusterReadOnlySnapshot> {
        let endpoint_pools = resolve_endpoints_handle()?;
        collect_cluster_read_only_snapshot(&endpoint_pools).await
    }

    pub fn cluster_snapshot_route(&self) -> &'static str {
        ADMIN_CLUSTER_SNAPSHOT_ROUTE
    }

    pub fn runtime_capabilities_route(&self) -> &'static str {
        ADMIN_RUNTIME_CAPABILITIES_ROUTE
    }

    pub fn extensions_catalog_route(&self) -> &'static str {
        ADMIN_EXTENSIONS_CATALOG_ROUTE
    }
}

#[cfg(test)]
mod tests {
    use super::super::storage_api::capacity::{PoolDecommissionInfo, PoolStatus};
    use super::*;
    use time::OffsetDateTime;

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

    #[tokio::test]
    async fn execute_collect_dependency_readiness_returns_state_flags() {
        let usecase = DefaultAdminUsecase::without_context();

        let readiness = usecase.execute_collect_dependency_readiness().await;
        let _ = readiness.storage_ready;
        let _ = readiness.iam_ready;
    }

    #[tokio::test]
    async fn execute_collect_cluster_read_only_snapshot_returns_none_without_context() {
        let usecase = DefaultAdminUsecase::without_context();

        let snapshot = usecase.execute_collect_cluster_read_only_snapshot().await;

        assert!(snapshot.is_none());
    }

    #[test]
    fn admin_usecase_exposes_stable_discovery_routes() {
        let usecase = DefaultAdminUsecase::without_context();

        assert_eq!(usecase.cluster_snapshot_route(), "/rustfs/admin/v4/cluster/snapshot");
        assert_eq!(usecase.runtime_capabilities_route(), "/rustfs/admin/v4/runtime/capabilities");
        assert_eq!(usecase.extensions_catalog_route(), "/rustfs/admin/v4/extensions/catalog");
    }

    #[test]
    fn admin_query_pool_status_by_id_rejects_non_numeric_index() {
        assert_eq!(DefaultAdminUsecase::parse_pool_idx_by_id("pool-a", 4), None);
    }

    #[test]
    fn admin_query_pool_status_by_id_rejects_out_of_range_index() {
        assert_eq!(DefaultAdminUsecase::parse_pool_idx_by_id("4", 4), None);
    }

    #[test]
    fn admin_query_pool_status_by_id_accepts_valid_index() {
        assert_eq!(DefaultAdminUsecase::parse_pool_idx_by_id("0", 4), Some(0));
    }

    #[test]
    fn admin_pool_list_item_maps_capacity_without_decommission_state_as_active() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let pool = PoolStatus {
            id: 2,
            cmd_line: "http://node{1...4}/disk{1...4}".to_string(),
            last_update: now,
            decommission: Some(PoolDecommissionInfo {
                total_size: 1_000,
                current_size: 250,
                ..Default::default()
            }),
        };

        let item = DefaultAdminUsecase::pool_list_item_from_status(pool, (RebalStatus::None, false));

        assert_eq!(item.id, 2);
        assert_eq!(item.total_size, 1_000);
        assert_eq!(item.current_size, 250);
        assert_eq!(item.used_size, 750);
        assert!((item.used - 0.75).abs() < f64::EPSILON);
        assert_eq!(item.status, "active");
        assert_eq!(item.decommission_status, "none");
        assert_eq!(item.rebalance_status, "none");
        assert!(item.decommission.is_none());
    }

    #[test]
    fn admin_pool_list_item_maps_inconsistent_decommission_progress_as_unknown() {
        let item = DefaultAdminUsecase::pool_list_item_from_status(
            PoolStatus {
                id: 2,
                cmd_line: "http://node{1...4}/disk{1...4}".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo {
                    total_size: 1_000,
                    current_size: 250,
                    items_decommissioned: 1,
                    ..Default::default()
                }),
            },
            (RebalStatus::None, false),
        );

        assert_eq!(item.status, "decommissioning");
        assert_eq!(item.decommission_status, "unknown");
        assert!(item.decommission.is_some());
    }

    #[test]
    fn admin_pool_list_item_serializes_admin_api_fields() {
        let item = DefaultAdminUsecase::pool_list_item_from_status(
            PoolStatus {
                id: 1,
                cmd_line: "pool-1".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: None,
            },
            (RebalStatus::Completed, false),
        );

        let value = serde_json::to_value(item).unwrap();

        assert_eq!(
            value,
            serde_json::json!({
                "id": 1,
                "cmdline": "pool-1",
                "lastUpdate": "1970-01-01T00:00:00Z",
                "totalSize": 0,
                "currentSize": 0,
                "usedSize": 0,
                "used": 0.0,
                "status": "active",
                "decommissionStatus": "none",
                "rebalanceStatus": "completed",
                "decommissionInfo": null
            })
        );
    }

    #[test]
    fn admin_pool_list_item_saturates_used_size_when_current_exceeds_total() {
        let pool = PoolStatus {
            id: 0,
            cmd_line: "pool-0".to_string(),
            last_update: OffsetDateTime::UNIX_EPOCH,
            decommission: Some(PoolDecommissionInfo {
                total_size: 100,
                current_size: 150,
                ..Default::default()
            }),
        };

        let item = DefaultAdminUsecase::pool_list_item_from_status(pool, (RebalStatus::None, false));

        assert_eq!(item.total_size, 100);
        assert_eq!(item.current_size, 150);
        assert_eq!(item.used_size, 0);
        assert_eq!(item.used, 0.0);
    }

    #[test]
    fn admin_pool_list_item_maps_running_decommission_status() {
        let pool = PoolStatus {
            id: 0,
            cmd_line: "pool-0".to_string(),
            last_update: OffsetDateTime::UNIX_EPOCH,
            decommission: Some(PoolDecommissionInfo {
                total_size: 1_000,
                current_size: 500,
                start_time: Some(OffsetDateTime::UNIX_EPOCH),
                ..Default::default()
            }),
        };

        let item = DefaultAdminUsecase::pool_list_item_from_status(pool, (RebalStatus::Started, false));

        assert_eq!(item.status, "decommissioning");
        assert_eq!(item.decommission_status, "running");
        assert_eq!(item.rebalance_status, "started");
    }

    #[test]
    fn admin_pool_list_item_keeps_decommission_state_ahead_of_rebalance_state() {
        let item = DefaultAdminUsecase::pool_list_item_from_status(
            PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo {
                    failed: true,
                    ..Default::default()
                }),
            },
            (RebalStatus::Started, false),
        );

        assert_eq!(item.status, "blocked");
        assert_eq!(item.decommission_status, "failed");
        assert_eq!(item.rebalance_status, "started");
    }

    #[test]
    fn admin_pool_list_item_exposes_queued_decommission_state() {
        let item = DefaultAdminUsecase::pool_list_item_from_status(
            PoolStatus {
                id: 3,
                cmd_line: "pool-3".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo {
                    queued: true,
                    queued_buckets: vec!["bucket-a".to_string(), ".rustfs.sys/config".to_string()],
                    decommissioned_buckets: vec!["bucket-done".to_string()],
                    bucket: "bucket-a".to_string(),
                    prefix: "prefix/".to_string(),
                    object: "object.txt".to_string(),
                    stage: "migrate_object".to_string(),
                    items_decommissioned: 7,
                    items_decommission_failed: 1,
                    bytes_done: 1024,
                    bytes_failed: 64,
                    ..Default::default()
                }),
            },
            (RebalStatus::None, false),
        );

        assert_eq!(item.status, "decommissioning");
        assert_eq!(item.decommission_status, "queued");
        let value = serde_json::to_value(item).expect("admin pool status should serialize");
        assert_eq!(value["decommissionInfo"]["queued"], true);
        assert_eq!(
            value["decommissionInfo"]["queuedBuckets"],
            serde_json::json!(["bucket-a", ".rustfs.sys/config"])
        );
        assert_eq!(value["decommissionInfo"]["decommissionedBuckets"], serde_json::json!(["bucket-done"]));
        assert_eq!(value["decommissionInfo"]["bucket"], "bucket-a");
        assert_eq!(value["decommissionInfo"]["prefix"], "prefix/");
        assert_eq!(value["decommissionInfo"]["object"], "object.txt");
        assert_eq!(value["decommissionInfo"]["stage"], "migrate_object");
        assert_eq!(value["decommissionInfo"]["objectsDecommissioned"], 7);
        assert_eq!(value["decommissionInfo"]["objectsDecommissionedFailed"], 1);
        assert_eq!(value["decommissionInfo"]["bytesDecommissioned"], 1024);
        assert_eq!(value["decommissionInfo"]["bytesDecommissionedFailed"], 64);
        assert_eq!(value["decommissionInfo"]["waitingReason"], "queued");
    }

    #[test]
    fn admin_pool_list_item_maps_terminal_decommission_statuses() {
        let complete = DefaultAdminUsecase::pool_decommission_status(Some(&PoolDecommissionInfo {
            complete: true,
            ..Default::default()
        }));
        let failed = DefaultAdminUsecase::pool_decommission_status(Some(&PoolDecommissionInfo {
            failed: true,
            ..Default::default()
        }));
        let canceled = DefaultAdminUsecase::pool_decommission_status(Some(&PoolDecommissionInfo {
            canceled: true,
            ..Default::default()
        }));
        let queued = DefaultAdminUsecase::pool_decommission_status(Some(&PoolDecommissionInfo {
            queued: true,
            ..Default::default()
        }));
        let idle = DefaultAdminUsecase::pool_decommission_status(None);

        assert_eq!(complete, "complete");
        assert_eq!(failed, "failed");
        assert_eq!(canceled, "canceled");
        assert_eq!(queued, "queued");
        assert_eq!(idle, "none");
    }

    #[test]
    fn admin_pool_list_item_keeps_rebalance_failure_separate_from_pool_state() {
        let item = DefaultAdminUsecase::pool_list_item_from_status(
            PoolStatus {
                id: 1,
                cmd_line: "pool-1".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: None,
            },
            (RebalStatus::Failed, false),
        );

        assert_eq!(item.status, "active");
        assert_eq!(item.decommission_status, "none");
        assert_eq!(item.rebalance_status, "failed");
    }

    #[test]
    fn admin_pool_list_item_maps_started_rebalance_to_pool_rebalancing() {
        let item = DefaultAdminUsecase::pool_list_item_from_status(
            PoolStatus {
                id: 1,
                cmd_line: "pool-1".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: None,
            },
            (RebalStatus::Started, false),
        );

        assert_eq!(item.status, "rebalancing");
        assert_eq!(item.decommission_status, "none");
        assert_eq!(item.rebalance_status, "started");
    }

    #[test]
    fn admin_pool_list_item_maps_stopping_rebalance_status() {
        let item = DefaultAdminUsecase::pool_list_item_from_status(
            PoolStatus {
                id: 1,
                cmd_line: "pool-1".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: None,
            },
            (RebalStatus::Started, true),
        );

        assert_eq!(item.status, "rebalancing");
        assert_eq!(item.decommission_status, "none");
        assert_eq!(item.rebalance_status, "stopping");
    }

    #[test]
    fn admin_pool_lifecycle_state_distinguishes_decommission_terminal_states() {
        let complete = DefaultAdminUsecase::pool_lifecycle_state(
            Some(&PoolDecommissionInfo {
                complete: true,
                ..Default::default()
            }),
            DefaultAdminUsecase::REBALANCE_STATUS_NONE,
        );
        let failed = DefaultAdminUsecase::pool_lifecycle_state(
            Some(&PoolDecommissionInfo {
                failed: true,
                ..Default::default()
            }),
            DefaultAdminUsecase::REBALANCE_STATUS_NONE,
        );
        let canceled = DefaultAdminUsecase::pool_lifecycle_state(
            Some(&PoolDecommissionInfo {
                canceled: true,
                ..Default::default()
            }),
            DefaultAdminUsecase::REBALANCE_STATUS_NONE,
        );
        let rebalancing = DefaultAdminUsecase::pool_lifecycle_state(None, DefaultAdminUsecase::REBALANCE_STATUS_STARTED);

        assert_eq!(complete, "decommissioned");
        assert_eq!(failed, "blocked");
        assert_eq!(canceled, "blocked");
        assert_eq!(rebalancing, "rebalancing");
    }

    #[test]
    fn admin_decommission_status_serializes_task_status_and_pool_status() {
        let item = DefaultAdminUsecase::decommission_pool_status_from_status(
            PoolStatus {
                id: 3,
                cmd_line: "pool-3".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo {
                    failed: true,
                    ..Default::default()
                }),
            },
            (RebalStatus::Started, false),
        );

        let value = serde_json::to_value(item).expect("decommission status should serialize");

        assert_eq!(
            value,
            serde_json::json!({
                "id": 3,
                "cmdline": "pool-3",
                "status": "failed",
                "poolStatus": "blocked",
                "decommissionInfo": {
                    "startTime": null,
                    "startSize": 0,
                    "totalSize": 0,
                    "currentSize": 0,
                    "complete": false,
                    "failed": true,
                    "canceled": false,
                    "queued": false,
                    "queuedBuckets": [],
                    "decommissionedBuckets": [],
                    "bucket": "",
                    "prefix": "",
                    "object": "",
                    "stage": "",
                    "objectsDecommissioned": 0,
                    "objectsDecommissionedFailed": 0,
                    "bytesDecommissioned": 0,
                    "bytesDecommissionedFailed": 0,
                    "waitingReason": null
                }
            })
        );
    }
}
