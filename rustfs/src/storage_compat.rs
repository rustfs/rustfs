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
    bucket as ecstore_bucket, cluster as ecstore_cluster, disk as ecstore_disk, error as ecstore_error, layout as ecstore_layout,
    set_disk as ecstore_set_disk,
};

pub(crate) const BUCKET_TABLE_CATALOG_META_PREFIX: &str = ecstore_bucket::metadata::BUCKET_TABLE_CATALOG_META_PREFIX;
pub(crate) const BUCKET_TABLE_CATALOG_TABLE_BUCKETS_PREFIX: &str =
    ecstore_bucket::metadata::BUCKET_TABLE_CATALOG_TABLE_BUCKETS_PREFIX;
pub(crate) const BUCKET_TABLE_CONFIG: &str = ecstore_bucket::metadata::BUCKET_TABLE_CONFIG;
pub(crate) const BUCKET_TABLE_RESERVED_PREFIX: &str = ecstore_bucket::metadata::BUCKET_TABLE_RESERVED_PREFIX;
pub(crate) const RUSTFS_META_BUCKET: &str = ecstore_disk::RUSTFS_META_BUCKET;
pub(crate) type EcstoreError = ecstore_error::Error;
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

pub(crate) fn topology_snapshot_from_endpoint_pools_with_capabilities(
    endpoint_pools: &EndpointServerPools,
    capabilities: rustfs_storage_api::TopologyCapabilities,
    disk_capabilities: rustfs_storage_api::DiskCapabilities,
) -> rustfs_storage_api::TopologySnapshot {
    ecstore_cluster::topology_snapshot_from_endpoint_pools_with_capabilities(endpoint_pools, capabilities, disk_capabilities)
}

pub(crate) fn get_lock_acquire_timeout() -> Duration {
    ecstore_set_disk::get_lock_acquire_timeout()
}
