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

use std::{collections::HashMap, sync::Arc, time::Duration};

pub(crate) const BUCKET_TABLE_CATALOG_META_PREFIX: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_TABLE_CATALOG_META_PREFIX;
pub(crate) const BUCKET_TABLE_CATALOG_TABLE_BUCKETS_PREFIX: &str =
    rustfs_ecstore::api::bucket::metadata::BUCKET_TABLE_CATALOG_TABLE_BUCKETS_PREFIX;
pub(crate) const BUCKET_TABLE_CONFIG: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_TABLE_CONFIG;
pub(crate) const BUCKET_TABLE_RESERVED_PREFIX: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_TABLE_RESERVED_PREFIX;
pub(crate) const RUSTFS_META_BUCKET: &str = rustfs_ecstore::api::disk::RUSTFS_META_BUCKET;
pub(crate) const TONIC_RPC_PREFIX: &str = rustfs_ecstore::api::rpc::TONIC_RPC_PREFIX;

pub(crate) type ECStore = rustfs_ecstore::api::storage::ECStore;
pub(crate) type EcstoreError = rustfs_ecstore::api::error::Error;
pub(crate) type EcstoreEventArgs = rustfs_ecstore::api::event::EventArgs;
pub(crate) type EcstoreResult<T> = rustfs_ecstore::api::error::Result<T>;
pub(crate) type Endpoint = rustfs_ecstore::api::disk::endpoint::Endpoint;
pub(crate) type EndpointServerPools = rustfs_ecstore::api::layout::EndpointServerPools;
pub(crate) type QuotaError = rustfs_ecstore::api::bucket::quota::QuotaError;
pub(crate) type StorageError = rustfs_ecstore::api::error::StorageError;

#[cfg(test)]
pub(crate) type DisksLayout = rustfs_ecstore::api::layout::DisksLayout;
#[cfg(test)]
pub(crate) type Endpoints = rustfs_ecstore::api::layout::Endpoints;
#[cfg(test)]
pub(crate) type PoolEndpoints = rustfs_ecstore::api::layout::PoolEndpoints;

pub(crate) trait DiskAPI {
    fn endpoint(&self) -> Endpoint;
    fn to_string(&self) -> String;
}

impl<T> DiskAPI for T
where
    T: rustfs_ecstore::api::disk::DiskAPI,
{
    fn endpoint(&self) -> Endpoint {
        rustfs_ecstore::api::disk::DiskAPI::endpoint(self)
    }

    fn to_string(&self) -> String {
        rustfs_ecstore::api::disk::DiskAPI::to_string(self)
    }
}

pub(crate) fn table_catalog_path_hash(value: &str) -> String {
    rustfs_ecstore::api::bucket::metadata::table_catalog_path_hash(value)
}

pub(crate) async fn get_bucket_metadata(
    bucket: &str,
) -> rustfs_ecstore::api::error::Result<Arc<rustfs_ecstore::api::bucket::metadata::BucketMetadata>> {
    rustfs_ecstore::api::bucket::metadata_sys::get(bucket).await
}

pub(crate) fn get_global_bucket_metadata_sys()
-> Option<Arc<tokio::sync::RwLock<rustfs_ecstore::api::bucket::metadata_sys::BucketMetadataSys>>> {
    rustfs_ecstore::api::bucket::metadata_sys::get_global_bucket_metadata_sys()
}

pub(crate) async fn get_notification_config(
    bucket: &str,
) -> rustfs_ecstore::api::error::Result<Option<s3s::dto::NotificationConfiguration>> {
    rustfs_ecstore::api::bucket::metadata_sys::get_notification_config(bucket).await
}

pub(crate) async fn init_bucket_metadata_sys(api: Arc<ECStore>, buckets: Vec<String>) {
    rustfs_ecstore::api::bucket::metadata_sys::init_bucket_metadata_sys(api, buckets).await;
}

pub(crate) async fn try_migrate_bucket_metadata(store: Arc<ECStore>) {
    rustfs_ecstore::api::bucket::migration::try_migrate_bucket_metadata(store).await;
}

pub(crate) async fn try_migrate_iam_config(store: Arc<ECStore>) {
    rustfs_ecstore::api::bucket::migration::try_migrate_iam_config(store).await;
}

pub(crate) fn get_global_replication_pool() -> Option<Arc<rustfs_ecstore::api::bucket::replication::DynReplicationPool>> {
    rustfs_ecstore::api::bucket::replication::get_global_replication_pool()
}

pub(crate) fn replication_queue_current_count() -> Option<i64> {
    rustfs_ecstore::api::bucket::replication::GLOBAL_REPLICATION_STATS
        .get()
        .and_then(|stats| {
            stats
                .q_cache
                .try_lock()
                .ok()
                .map(|cache| cache.sr_queue_stats.curr.get_current_count())
        })
}

pub(crate) async fn init_background_replication(storage: Arc<ECStore>) {
    rustfs_ecstore::api::bucket::replication::init_background_replication(storage).await;
}

pub(crate) fn topology_snapshot_from_endpoint_pools_with_capabilities(
    endpoint_pools: &EndpointServerPools,
    capabilities: rustfs_storage_api::TopologyCapabilities,
    disk_capabilities: rustfs_storage_api::DiskCapabilities,
) -> rustfs_storage_api::TopologySnapshot {
    rustfs_ecstore::api::cluster::topology_snapshot_from_endpoint_pools_with_capabilities(
        endpoint_pools,
        capabilities,
        disk_capabilities,
    )
}

pub(crate) async fn read_ecstore_config(api: Arc<ECStore>, file: &str) -> EcstoreResult<Vec<u8>> {
    rustfs_ecstore::api::config::com::read_config(api, file).await
}

pub(crate) async fn save_ecstore_config(api: Arc<ECStore>, file: &str, data: Vec<u8>) -> EcstoreResult<()> {
    rustfs_ecstore::api::config::com::save_config(api, file, data).await
}

pub(crate) fn init_ecstore_config() {
    rustfs_ecstore::api::config::init();
}

pub(crate) async fn init_global_config_sys(api: Arc<ECStore>) -> EcstoreResult<()> {
    rustfs_ecstore::api::config::init_global_config_sys(api).await
}

pub(crate) async fn try_migrate_server_config(api: Arc<ECStore>) {
    rustfs_ecstore::api::config::try_migrate_server_config(api).await;
}

pub(crate) fn register_event_dispatch_hook<F>(hook: F) -> bool
where
    F: Fn(EcstoreEventArgs) + Send + Sync + 'static,
{
    rustfs_ecstore::api::event::register_event_dispatch_hook(hook)
}

pub(crate) fn get_global_endpoints_opt() -> Option<EndpointServerPools> {
    rustfs_ecstore::api::global::get_global_endpoints_opt()
}

pub(crate) fn get_global_lock_clients() -> Option<&'static HashMap<String, Arc<dyn rustfs_lock::client::LockClient>>> {
    rustfs_ecstore::api::global::get_global_lock_clients()
}

pub(crate) fn get_global_region() -> Option<s3s::region::Region> {
    rustfs_ecstore::api::global::get_global_region()
}

pub(crate) async fn is_dist_erasure() -> bool {
    rustfs_ecstore::api::global::is_dist_erasure().await
}

pub(crate) fn resolve_object_store_handle() -> Option<Arc<ECStore>> {
    rustfs_ecstore::api::global::resolve_object_store_handle()
}

pub(crate) fn set_global_endpoints(eps: Vec<rustfs_ecstore::api::layout::PoolEndpoints>) {
    rustfs_ecstore::api::global::set_global_endpoints(eps);
}

pub(crate) fn set_global_region(region: s3s::region::Region) {
    rustfs_ecstore::api::global::set_global_region(region);
}

pub(crate) fn set_global_rustfs_port(value: u16) {
    rustfs_ecstore::api::global::set_global_rustfs_port(value);
}

pub(crate) fn shutdown_background_services() {
    rustfs_ecstore::api::global::shutdown_background_services();
}

pub(crate) async fn update_erasure_type(setup_type: rustfs_ecstore::api::layout::SetupType) {
    rustfs_ecstore::api::global::update_erasure_type(setup_type).await;
}

pub(crate) async fn new_global_notification_sys(eps: EndpointServerPools) -> EcstoreResult<()> {
    rustfs_ecstore::api::notification::new_global_notification_sys(eps).await
}

pub(crate) fn verify_rpc_signature(url: &str, method: &http::Method, headers: &http::HeaderMap) -> std::io::Result<()> {
    rustfs_ecstore::api::rpc::verify_rpc_signature(url, method, headers)
}

pub(crate) fn get_lock_acquire_timeout() -> Duration {
    rustfs_ecstore::api::set_disk::get_lock_acquire_timeout()
}

pub(crate) async fn all_local_disk() -> Vec<rustfs_ecstore::api::disk::DiskStore> {
    rustfs_ecstore::api::storage::all_local_disk().await
}

pub(crate) async fn init_local_disks(endpoint_pools: EndpointServerPools) -> EcstoreResult<()> {
    rustfs_ecstore::api::storage::init_local_disks(endpoint_pools).await
}

pub(crate) fn init_lock_clients(endpoint_pools: EndpointServerPools) {
    rustfs_ecstore::api::storage::init_lock_clients(endpoint_pools);
}

pub(crate) async fn prewarm_local_disk_id_map() {
    rustfs_ecstore::api::storage::prewarm_local_disk_id_map().await;
}
