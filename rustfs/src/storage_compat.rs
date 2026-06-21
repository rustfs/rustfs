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

use std::{sync::Arc, time::Duration};

use rustfs_ecstore::api::{
    bucket as ecstore_bucket, cluster as ecstore_cluster, config as ecstore_config, disk as ecstore_disk, error as ecstore_error,
    global as ecstore_global, layout as ecstore_layout, notification as ecstore_notification, set_disk as ecstore_set_disk,
    storage as ecstore_storage,
};

pub(crate) const BUCKET_TABLE_CATALOG_META_PREFIX: &str = ecstore_bucket::metadata::BUCKET_TABLE_CATALOG_META_PREFIX;
pub(crate) const BUCKET_TABLE_CATALOG_TABLE_BUCKETS_PREFIX: &str =
    ecstore_bucket::metadata::BUCKET_TABLE_CATALOG_TABLE_BUCKETS_PREFIX;
pub(crate) const BUCKET_TABLE_CONFIG: &str = ecstore_bucket::metadata::BUCKET_TABLE_CONFIG;
pub(crate) const BUCKET_TABLE_RESERVED_PREFIX: &str = ecstore_bucket::metadata::BUCKET_TABLE_RESERVED_PREFIX;
pub(crate) const RUSTFS_META_BUCKET: &str = ecstore_disk::RUSTFS_META_BUCKET;
pub(crate) type ECStore = ecstore_storage::ECStore;
pub(crate) type EcstoreError = ecstore_error::Error;
pub(crate) type EcstoreResult<T> = ecstore_error::Result<T>;
#[cfg(test)]
pub(crate) type Endpoint = ecstore_disk::endpoint::Endpoint;
pub(crate) type EndpointServerPools = ecstore_layout::EndpointServerPools;
pub(crate) type QuotaError = ecstore_bucket::quota::QuotaError;
pub(crate) type StorageError = ecstore_error::StorageError;

#[cfg(test)]
pub(crate) type DisksLayout = ecstore_layout::DisksLayout;
#[cfg(test)]
pub(crate) type Endpoints = ecstore_layout::Endpoints;
#[cfg(test)]
pub(crate) type PoolEndpoints = ecstore_layout::PoolEndpoints;

pub(crate) fn table_catalog_path_hash(value: &str) -> String {
    ecstore_bucket::metadata::table_catalog_path_hash(value)
}

pub(crate) async fn get_bucket_metadata(bucket: &str) -> ecstore_error::Result<Arc<ecstore_bucket::metadata::BucketMetadata>> {
    ecstore_bucket::metadata_sys::get(bucket).await
}

pub(crate) fn get_global_bucket_metadata_sys() -> Option<Arc<tokio::sync::RwLock<ecstore_bucket::metadata_sys::BucketMetadataSys>>>
{
    ecstore_bucket::metadata_sys::get_global_bucket_metadata_sys()
}

pub(crate) async fn get_notification_config(bucket: &str) -> ecstore_error::Result<Option<s3s::dto::NotificationConfiguration>> {
    ecstore_bucket::metadata_sys::get_notification_config(bucket).await
}

pub(crate) async fn init_bucket_metadata_sys(api: Arc<ECStore>, buckets: Vec<String>) {
    ecstore_bucket::metadata_sys::init_bucket_metadata_sys(api, buckets).await;
}

pub(crate) async fn try_migrate_bucket_metadata(store: Arc<ECStore>) {
    ecstore_bucket::migration::try_migrate_bucket_metadata(store).await;
}

pub(crate) async fn try_migrate_iam_config(store: Arc<ECStore>) {
    ecstore_bucket::migration::try_migrate_iam_config(store).await;
}

pub(crate) fn get_global_replication_pool() -> Option<Arc<ecstore_bucket::replication::DynReplicationPool>> {
    ecstore_bucket::replication::get_global_replication_pool()
}

pub(crate) fn replication_queue_current_count() -> Option<i64> {
    ecstore_bucket::replication::GLOBAL_REPLICATION_STATS.get().and_then(|stats| {
        stats
            .q_cache
            .try_lock()
            .ok()
            .map(|cache| cache.sr_queue_stats.curr.get_current_count())
    })
}

pub(crate) async fn init_background_replication(storage: Arc<ECStore>) {
    ecstore_bucket::replication::init_background_replication(storage).await;
}

pub(crate) fn topology_snapshot_from_endpoint_pools_with_capabilities(
    endpoint_pools: &EndpointServerPools,
    capabilities: rustfs_storage_api::TopologyCapabilities,
    disk_capabilities: rustfs_storage_api::DiskCapabilities,
) -> rustfs_storage_api::TopologySnapshot {
    ecstore_cluster::topology_snapshot_from_endpoint_pools_with_capabilities(endpoint_pools, capabilities, disk_capabilities)
}

pub(crate) fn init_ecstore_config() {
    ecstore_config::init();
}

pub(crate) async fn init_global_config_sys(api: Arc<ECStore>) -> EcstoreResult<()> {
    ecstore_config::init_global_config_sys(api).await
}

pub(crate) async fn try_migrate_server_config(api: Arc<ECStore>) {
    ecstore_config::try_migrate_server_config(api).await;
}

pub(crate) fn get_global_region() -> Option<s3s::region::Region> {
    ecstore_global::get_global_region()
}

pub(crate) fn set_global_endpoints(eps: Vec<ecstore_layout::PoolEndpoints>) {
    ecstore_global::set_global_endpoints(eps);
}

pub(crate) fn set_global_region(region: s3s::region::Region) {
    ecstore_global::set_global_region(region);
}

pub(crate) fn set_global_rustfs_port(value: u16) {
    ecstore_global::set_global_rustfs_port(value);
}

pub(crate) fn shutdown_background_services() {
    ecstore_global::shutdown_background_services();
}

pub(crate) async fn update_erasure_type(setup_type: ecstore_layout::SetupType) {
    ecstore_global::update_erasure_type(setup_type).await;
}

pub(crate) async fn new_global_notification_sys(eps: EndpointServerPools) -> EcstoreResult<()> {
    ecstore_notification::new_global_notification_sys(eps).await
}

pub(crate) fn get_lock_acquire_timeout() -> Duration {
    ecstore_set_disk::get_lock_acquire_timeout()
}

pub(crate) async fn init_local_disks(endpoint_pools: EndpointServerPools) -> EcstoreResult<()> {
    ecstore_storage::init_local_disks(endpoint_pools).await
}

pub(crate) fn init_lock_clients(endpoint_pools: EndpointServerPools) {
    ecstore_storage::init_lock_clients(endpoint_pools);
}

pub(crate) async fn prewarm_local_disk_id_map() {
    ecstore_storage::prewarm_local_disk_id_map().await;
}
