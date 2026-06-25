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

//! Application layer module entry.
//! Concrete use-case modules will be introduced incrementally in Phase 3.

pub mod admin_usecase;
pub mod bucket_usecase;
pub mod context;
pub mod multipart_usecase;
pub mod object_usecase;
pub(crate) mod runtime_sources;
pub(crate) mod s3_api;
mod select_object;
pub(crate) mod storage_api;

#[cfg(test)]
mod capacity_dirty_scope_test;
#[cfg(test)]
mod lifecycle_transition_api_test;

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

mod ecstore_admin {
    pub(crate) use crate::storage::ecstore_admin::get_server_info;
}

mod ecstore_capacity {
    pub(crate) use crate::storage::ecstore_capacity::{
        PoolDecommissionInfo, PoolStatus, get_total_usable_capacity, get_total_usable_capacity_free,
    };
}

mod ecstore_data_usage {
    pub(crate) use crate::storage::ecstore_data_usage::{
        apply_bucket_usage_memory_overlay, load_data_usage_from_backend, record_bucket_object_delete_memory,
        record_bucket_object_write_memory, remove_bucket_usage_from_backend,
    };
}

#[allow(unused_imports)]
mod ecstore_tier {
    pub(crate) use crate::storage::ecstore_tier::{tier, tier_config, warm_backend};
}

pub(crate) type DynReplicationPool = crate::storage::DynReplicationPool;
pub(crate) type ECStore = crate::storage::ECStore;
pub(crate) type EndpointServerPools = crate::storage::EndpointServerPools;
pub(crate) type ExpiryState = crate::storage::ExpiryState;
pub(crate) type NotificationSys = crate::storage::NotificationSys;
pub(crate) type BucketBandwidthMonitor = crate::storage::BucketBandwidthMonitor;
pub(crate) type ObjectStoreResolver = crate::storage::ObjectStoreResolver;
pub(crate) type PoolDecommissionInfo = ecstore_capacity::PoolDecommissionInfo;
pub(crate) type PoolStatus = ecstore_capacity::PoolStatus;
pub(crate) type RebalStatus = crate::storage::ecstore_rebalance::RebalStatus;
pub(crate) type Error = crate::storage::StorageError;
pub(crate) type TierConfigMgr = crate::storage::TierConfigMgr;

#[cfg(test)]
pub(crate) type Endpoint = crate::storage::Endpoint;
#[cfg(test)]
pub(crate) type Endpoints = crate::storage::Endpoints;
#[cfg(test)]
pub(crate) type PoolEndpoints = crate::storage::PoolEndpoints;
#[cfg(test)]
pub(crate) type TierConfig = ecstore_tier::tier_config::TierConfig;
#[cfg(test)]
pub(crate) type TierType = ecstore_tier::tier_config::TierType;
#[cfg(test)]
pub(crate) use ecstore_tier::warm_backend::WarmBackend as AppWarmBackend;
#[cfg(test)]
pub(crate) type WarmBackendGetOpts = ecstore_tier::warm_backend::WarmBackendGetOpts;

#[cfg(test)]
#[allow(non_snake_case)]
pub(crate) fn EndpointServerPools(pools: Vec<PoolEndpoints>) -> EndpointServerPools {
    crate::storage::EndpointServerPools::from(pools)
}

pub(crate) async fn get_server_info(get_pools: bool) -> rustfs_madmin::InfoMessage {
    ecstore_admin::get_server_info(get_pools).await
}

pub(crate) type StorageClassConfig = crate::storage::ecstore_config::storageclass::Config;

pub(crate) fn set_global_storage_class(cfg: StorageClassConfig) {
    crate::storage::ecstore_config::set_global_storage_class(cfg);
}

pub(crate) fn get_total_usable_capacity(disks: &[rustfs_madmin::Disk], info: &rustfs_madmin::StorageInfo) -> usize {
    ecstore_capacity::get_total_usable_capacity(disks, info)
}

pub(crate) fn get_total_usable_capacity_free(disks: &[rustfs_madmin::Disk], info: &rustfs_madmin::StorageInfo) -> usize {
    ecstore_capacity::get_total_usable_capacity_free(disks, info)
}

pub(crate) async fn apply_bucket_usage_memory_overlay(data_usage_info: &mut rustfs_data_usage::DataUsageInfo) {
    ecstore_data_usage::apply_bucket_usage_memory_overlay(data_usage_info).await;
}

pub(crate) async fn load_data_usage_from_backend(
    store: Arc<ECStore>,
) -> std::result::Result<rustfs_data_usage::DataUsageInfo, Error> {
    ecstore_data_usage::load_data_usage_from_backend(store).await
}

pub(crate) async fn record_bucket_object_delete_memory(bucket: &str, deleted_size: u64, removed_current_object: bool) {
    ecstore_data_usage::record_bucket_object_delete_memory(bucket, deleted_size, removed_current_object).await;
}

pub(crate) async fn record_bucket_object_write_memory(bucket: &str, previous_current_size: Option<u64>, new_size: u64) {
    ecstore_data_usage::record_bucket_object_write_memory(bucket, previous_current_size, new_size).await;
}

pub(crate) async fn remove_bucket_usage_from_backend(store: Arc<ECStore>, bucket: &str) -> std::result::Result<(), Error> {
    ecstore_data_usage::remove_bucket_usage_from_backend(store, bucket).await
}

pub(crate) fn get_global_endpoints_opt() -> Option<EndpointServerPools> {
    crate::storage::get_global_endpoints_opt()
}

pub(crate) fn get_global_deployment_id() -> Option<String> {
    crate::storage::get_global_deployment_id()
}

pub(crate) fn get_global_lock_client() -> Option<Arc<dyn rustfs_lock::client::LockClient>> {
    crate::storage::get_global_lock_client()
}

pub(crate) fn get_global_lock_clients() -> Option<&'static HashMap<String, Arc<dyn rustfs_lock::client::LockClient>>> {
    crate::storage::get_global_lock_clients()
}

pub(crate) fn get_global_region() -> Option<s3s::region::Region> {
    crate::storage::get_global_region()
}

pub(crate) fn global_rustfs_port() -> u16 {
    crate::storage::global_rustfs_port()
}

pub(crate) fn get_global_tier_config_mgr() -> Arc<tokio::sync::RwLock<TierConfigMgr>> {
    crate::storage::get_global_tier_config_mgr()
}

pub(crate) fn get_global_expiry_state() -> Arc<tokio::sync::RwLock<ExpiryState>> {
    crate::storage::get_global_expiry_state()
}

pub(crate) fn new_object_layer_fn() -> Option<Arc<ECStore>> {
    crate::storage::new_object_layer_fn()
}

pub(crate) fn set_object_store_resolver(resolver: Arc<ObjectStoreResolver>) -> bool {
    crate::storage::set_object_store_resolver(resolver)
}

pub(crate) fn get_global_notification_sys() -> Option<&'static NotificationSys> {
    crate::storage::get_global_notification_sys()
}

pub(crate) fn get_global_bucket_monitor() -> Option<Arc<BucketBandwidthMonitor>> {
    crate::storage::get_global_bucket_monitor()
}

pub(crate) fn get_global_replication_pool() -> Option<Arc<DynReplicationPool>> {
    crate::storage::get_global_replication_pool()
}

pub(crate) type ReplicationStats = crate::storage::ReplicationStats;

pub(crate) fn get_global_replication_stats() -> Option<Arc<ReplicationStats>> {
    crate::storage::get_global_replication_stats()
}

pub(crate) type DailyAllTierStats = crate::storage::DailyAllTierStats;

pub(crate) fn get_global_boot_time() -> Option<std::time::SystemTime> {
    crate::storage::get_global_boot_time()
}

pub(crate) fn get_daily_all_tier_stats() -> DailyAllTierStats {
    crate::storage::get_daily_all_tier_stats()
}

pub(crate) type ScannerMetricsReport = rustfs_common::metrics::ScannerMetricsReport;

pub(crate) async fn collect_scanner_metrics_report() -> ScannerMetricsReport {
    rustfs_common::metrics::global_metrics().report().await
}

#[cfg(test)]
pub(crate) async fn init_local_disks(endpoint_pools: EndpointServerPools) -> Result<(), Error> {
    crate::storage::init_local_disks(endpoint_pools).await
}
