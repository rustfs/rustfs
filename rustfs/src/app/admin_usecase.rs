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
use crate::capacity::resolve_admin_used_capacity;
use crate::error::ApiError;
use rustfs_common::data_usage::DataUsageInfo;
use rustfs_ecstore::admin_server_info::get_server_info;
use rustfs_ecstore::data_usage::load_data_usage_from_backend;
use rustfs_ecstore::endpoints::EndpointServerPools;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::pools::{PoolDecommissionInfo, PoolStatus, get_total_usable_capacity, get_total_usable_capacity_free};
use rustfs_ecstore::store_api::StorageAPI;
use rustfs_madmin::{Disk, InfoMessage, StorageInfo};
use s3s::S3ErrorCode;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
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

#[derive(Debug, Clone, serde::Serialize)]
pub struct AdminPoolListItem {
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
    #[serde(rename = "decommissionInfo")]
    pub decommission: Option<PoolDecommissionInfo>,
}

#[derive(Clone, Default)]
pub struct DefaultAdminUsecase {
    context: Option<Arc<AppContext>>,
}

#[derive(Debug, Clone, Copy)]
struct StorageReadinessCacheEntry {
    captured_at: Instant,
    storage_ready: bool,
}

impl DefaultAdminUsecase {
    const DISK_STATE_OK: &'static str = "ok";
    const DISK_STATE_UNFORMATTED: &'static str = "unformatted";
    const RUNTIME_STATE_RETURNING: &'static str = "returning";
    const POOL_STATUS_ACTIVE: &'static str = "active";
    const POOL_STATUS_CANCELED: &'static str = "canceled";
    const POOL_STATUS_COMPLETE: &'static str = "complete";
    const POOL_STATUS_FAILED: &'static str = "failed";
    const POOL_STATUS_RUNNING: &'static str = "running";

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
        let info = get_server_info(req.include_pools).await;
        Ok(QueryServerInfoResponse { info })
    }

    pub async fn execute_query_storage_info(&self) -> AdminUsecaseResult<StorageInfo> {
        let Some(store) = new_object_layer_fn() else {
            return Err(Self::app_error(S3ErrorCode::InternalError, "Not init"));
        };

        Ok(store.storage_info().await)
    }

    pub async fn execute_query_data_usage_info(&self) -> AdminUsecaseResult<DataUsageInfo> {
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

    pub async fn execute_list_pools(&self) -> AdminUsecaseResult<Vec<AdminPoolListItem>> {
        let pool_statuses = self.execute_list_pool_statuses().await?;
        Ok(pool_statuses.into_iter().map(Self::pool_list_item_from_status).collect())
    }

    pub async fn execute_query_pool_status(&self, req: QueryPoolStatusRequest) -> AdminUsecaseResult<PoolStatus> {
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

    fn pool_list_item_from_status(status: PoolStatus) -> AdminPoolListItem {
        let PoolStatus {
            id,
            cmd_line,
            last_update,
            decommission,
        } = status;
        let total_size = decommission.as_ref().map(|info| info.total_size).unwrap_or_default();
        let current_size = decommission.as_ref().map(|info| info.current_size).unwrap_or_default();
        let used_size = total_size.saturating_sub(current_size);

        AdminPoolListItem {
            id,
            cmd_line,
            last_update,
            total_size,
            current_size,
            used_size,
            used: Self::used_ratio(total_size, used_size),
            status: Self::pool_list_status(decommission.as_ref()).to_string(),
            decommission,
        }
    }

    fn pool_list_status(decommission: Option<&PoolDecommissionInfo>) -> &'static str {
        match decommission {
            Some(info) if info.complete => Self::POOL_STATUS_COMPLETE,
            Some(info) if info.failed => Self::POOL_STATUS_FAILED,
            Some(info) if info.canceled => Self::POOL_STATUS_CANCELED,
            Some(info) if info.start_time.is_some() => Self::POOL_STATUS_RUNNING,
            _ => Self::POOL_STATUS_ACTIVE,
        }
    }

    fn used_ratio(total_size: usize, used_size: usize) -> f64 {
        if total_size == 0 {
            return 0.0;
        }

        used_size as f64 / total_size as f64
    }

    fn disk_is_online_for_readiness(disk: &Disk) -> bool {
        let state_is_acceptable = disk.state.eq_ignore_ascii_case(Self::DISK_STATE_OK)
            || disk.state.eq_ignore_ascii_case(rustfs_madmin::ITEM_ONLINE)
            || disk.state.eq_ignore_ascii_case(Self::DISK_STATE_UNFORMATTED);

        if let Some(runtime_state) = disk.runtime_state.as_deref() {
            let runtime_state_is_acceptable = runtime_state.eq_ignore_ascii_case(rustfs_madmin::ITEM_ONLINE)
                || runtime_state.eq_ignore_ascii_case(Self::RUNTIME_STATE_RETURNING);
            return runtime_state_is_acceptable && state_is_acceptable;
        }

        state_is_acceptable
    }

    fn health_readiness_cache_ttl() -> Duration {
        Duration::from_millis(rustfs_utils::get_env_u64(
            rustfs_config::ENV_HEALTH_READINESS_CACHE_TTL_MS,
            rustfs_config::DEFAULT_HEALTH_READINESS_CACHE_TTL_MS,
        ))
    }

    fn storage_readiness_cache() -> &'static Mutex<Option<StorageReadinessCacheEntry>> {
        static CACHE: OnceLock<Mutex<Option<StorageReadinessCacheEntry>>> = OnceLock::new();
        CACHE.get_or_init(|| Mutex::new(None))
    }

    async fn load_cached_storage_readiness() -> Option<bool> {
        let ttl = Self::health_readiness_cache_ttl();
        if ttl.is_zero() {
            return None;
        }

        let cache = Self::storage_readiness_cache().lock().await;
        let entry = cache.as_ref()?;
        if entry.captured_at.elapsed() <= ttl {
            return Some(entry.storage_ready);
        }

        None
    }

    async fn update_storage_readiness_cache(storage_ready: bool) {
        if Self::health_readiness_cache_ttl().is_zero() {
            return;
        }

        let mut cache = Self::storage_readiness_cache().lock().await;
        *cache = Some(StorageReadinessCacheEntry {
            captured_at: Instant::now(),
            storage_ready,
        });
    }

    fn pool_write_quorum(info: &StorageInfo, pool_idx: usize, set_drive_count: usize) -> usize {
        if set_drive_count == 0 {
            return 1;
        }

        let data_drives = info
            .backend
            .standard_sc_data
            .get(pool_idx)
            .copied()
            .filter(|count| *count > 0)
            .unwrap_or_else(|| (set_drive_count / 2).max(1));

        let parity_drives = if let Some(drives_per_set) = info.backend.drives_per_set.get(pool_idx).copied() {
            drives_per_set.saturating_sub(data_drives)
        } else if let Some(parity) = info.backend.standard_sc_parities.get(pool_idx).copied() {
            parity
        } else if let Some(parity) = info.backend.standard_sc_parity {
            parity
        } else {
            set_drive_count.saturating_sub(data_drives)
        };

        let mut write_quorum = data_drives;
        if data_drives == parity_drives {
            write_quorum += 1;
        }
        write_quorum.max(1)
    }

    fn storage_ready_from_runtime_state(info: &StorageInfo) -> bool {
        if info.disks.is_empty() {
            return false;
        }

        let mut total_online = 0usize;
        let mut set_online_counts: HashMap<(usize, usize), usize> = HashMap::new();
        let mut set_drive_counts: HashMap<(usize, usize), usize> = HashMap::new();
        let mut seen_disks: HashSet<(String, String, i32, i32, i32)> = HashSet::new();

        for disk in &info.disks {
            if disk.pool_index < 0 || disk.set_index < 0 {
                continue;
            }

            let dedup_key = (
                disk.endpoint.clone(),
                disk.drive_path.clone(),
                disk.pool_index,
                disk.set_index,
                disk.disk_index,
            );
            if !seen_disks.insert(dedup_key) {
                continue;
            }

            let pool_idx = disk.pool_index as usize;
            let set_idx = disk.set_index as usize;
            let key = (pool_idx, set_idx);
            *set_drive_counts.entry(key).or_default() += 1;

            if Self::disk_is_online_for_readiness(disk) {
                total_online += 1;
                *set_online_counts.entry(key).or_default() += 1;
            }
        }

        if total_online == 0 {
            return false;
        }

        if set_drive_counts.is_empty() {
            return false;
        }

        set_drive_counts.into_iter().all(|((pool_idx, set_idx), set_drive_count)| {
            let online = set_online_counts.get(&(pool_idx, set_idx)).copied().unwrap_or_default();
            let write_quorum = Self::pool_write_quorum(info, pool_idx, set_drive_count);
            online >= write_quorum
        })
    }

    pub async fn execute_collect_dependency_readiness(&self) -> DependencyReadiness {
        let iam_ready = self.context.as_ref().map(|context| context.iam().is_ready()).unwrap_or(false);
        let storage_ready = if let Some(cached) = Self::load_cached_storage_readiness().await {
            cached
        } else {
            let computed = if let Some(store) = new_object_layer_fn() {
                let storage_info = store.storage_info().await;
                Self::storage_ready_from_runtime_state(&storage_info)
            } else {
                false
            };
            Self::update_storage_readiness_cache(computed).await;
            computed
        };

        DependencyReadiness {
            storage_ready,
            iam_ready: iam_ready && storage_ready,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_ecstore::pools::{PoolDecommissionInfo, PoolStatus};
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

    #[test]
    fn storage_ready_from_runtime_state_returns_false_when_all_disks_faulty() {
        let info = StorageInfo {
            backend: rustfs_madmin::BackendInfo {
                standard_sc_data: vec![1],
                drives_per_set: vec![1],
                ..Default::default()
            },
            disks: vec![Disk {
                pool_index: 0,
                set_index: 0,
                state: "offline".to_string(),
                runtime_state: Some("offline".to_string()),
                ..Default::default()
            }],
        };

        assert!(!DefaultAdminUsecase::storage_ready_from_runtime_state(&info));
    }

    #[test]
    fn storage_ready_from_runtime_state_returns_true_when_set_meets_write_quorum() {
        let info = StorageInfo {
            backend: rustfs_madmin::BackendInfo {
                standard_sc_data: vec![1],
                drives_per_set: vec![1],
                ..Default::default()
            },
            disks: vec![Disk {
                pool_index: 0,
                set_index: 0,
                state: "ok".to_string(),
                runtime_state: Some("online".to_string()),
                ..Default::default()
            }],
        };

        assert!(DefaultAdminUsecase::storage_ready_from_runtime_state(&info));
    }

    #[test]
    fn storage_ready_from_runtime_state_deduplicates_duplicate_disk_rows() {
        let duplicate_disk = Disk {
            endpoint: "127.0.0.1:9000".to_string(),
            drive_path: "/data0".to_string(),
            pool_index: 0,
            set_index: 0,
            disk_index: 0,
            state: "ok".to_string(),
            runtime_state: Some("online".to_string()),
            ..Default::default()
        };
        let info = StorageInfo {
            backend: rustfs_madmin::BackendInfo {
                standard_sc_data: vec![2],
                drives_per_set: vec![4],
                ..Default::default()
            },
            disks: vec![duplicate_disk.clone(), duplicate_disk],
        };

        assert!(
            !DefaultAdminUsecase::storage_ready_from_runtime_state(&info),
            "duplicate rows must not satisfy write quorum"
        );
    }

    #[test]
    fn disk_online_for_readiness_requires_runtime_and_state_both_acceptable() {
        let disk = Disk {
            state: "disk io error".to_string(),
            runtime_state: Some("online".to_string()),
            ..Default::default()
        };
        assert!(!DefaultAdminUsecase::disk_is_online_for_readiness(&disk));
    }

    #[test]
    fn storage_ready_from_runtime_state_requires_all_sets_meet_quorum() {
        let info = StorageInfo {
            backend: rustfs_madmin::BackendInfo {
                standard_sc_data: vec![1],
                drives_per_set: vec![2],
                ..Default::default()
            },
            disks: vec![
                Disk {
                    endpoint: "127.0.0.1:9000".to_string(),
                    drive_path: "/set0d0".to_string(),
                    pool_index: 0,
                    set_index: 0,
                    disk_index: 0,
                    state: "ok".to_string(),
                    runtime_state: Some("online".to_string()),
                    ..Default::default()
                },
                Disk {
                    endpoint: "127.0.0.1:9000".to_string(),
                    drive_path: "/set1d0".to_string(),
                    pool_index: 0,
                    set_index: 1,
                    disk_index: 0,
                    state: "offline".to_string(),
                    runtime_state: Some("offline".to_string()),
                    ..Default::default()
                },
            ],
        };

        assert!(
            !DefaultAdminUsecase::storage_ready_from_runtime_state(&info),
            "if any set fails write quorum, readiness must be false"
        );
    }

    #[test]
    fn admin_pool_list_item_maps_capacity_and_active_status() {
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

        let item = DefaultAdminUsecase::pool_list_item_from_status(pool);

        assert_eq!(item.id, 2);
        assert_eq!(item.total_size, 1_000);
        assert_eq!(item.current_size, 250);
        assert_eq!(item.used_size, 750);
        assert!((item.used - 0.75).abs() < f64::EPSILON);
        assert_eq!(item.status, "active");
    }

    #[test]
    fn admin_pool_list_item_serializes_admin_api_fields() {
        let item = DefaultAdminUsecase::pool_list_item_from_status(PoolStatus {
            id: 1,
            cmd_line: "pool-1".to_string(),
            last_update: OffsetDateTime::UNIX_EPOCH,
            decommission: None,
        });

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

        let item = DefaultAdminUsecase::pool_list_item_from_status(pool);

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

        let item = DefaultAdminUsecase::pool_list_item_from_status(pool);

        assert_eq!(item.status, "running");
    }

    #[test]
    fn admin_pool_list_item_maps_terminal_decommission_statuses() {
        let complete = DefaultAdminUsecase::pool_list_status(Some(&PoolDecommissionInfo {
            complete: true,
            ..Default::default()
        }));
        let failed = DefaultAdminUsecase::pool_list_status(Some(&PoolDecommissionInfo {
            failed: true,
            ..Default::default()
        }));
        let canceled = DefaultAdminUsecase::pool_list_status(Some(&PoolDecommissionInfo {
            canceled: true,
            ..Default::default()
        }));
        let idle = DefaultAdminUsecase::pool_list_status(None);

        assert_eq!(complete, "complete");
        assert_eq!(failed, "failed");
        assert_eq!(canceled, "canceled");
        assert_eq!(idle, "active");
    }
}
