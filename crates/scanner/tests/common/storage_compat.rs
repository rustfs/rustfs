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

#![allow(dead_code, unused_imports)]

use std::collections::HashMap;
use std::sync::Arc;

use rustfs_ecstore::api::bucket as ecstore_bucket;
use rustfs_ecstore::api::capacity as ecstore_capacity;
use rustfs_ecstore::api::client as ecstore_client;
use rustfs_ecstore::api::disk as ecstore_disk;
use rustfs_ecstore::api::error as ecstore_error;
use rustfs_ecstore::api::global as ecstore_global;
use rustfs_ecstore::api::layout as ecstore_layout;
use rustfs_ecstore::api::storage as ecstore_storage;
use rustfs_ecstore::api::tier as ecstore_tier;
use time::OffsetDateTime;

pub(crate) const BUCKET_LIFECYCLE_CONFIG: &str = ecstore_bucket::metadata::BUCKET_LIFECYCLE_CONFIG;
pub(crate) const STORAGE_FORMAT_FILE: &str = ecstore_disk::STORAGE_FORMAT_FILE;

pub(crate) type BucketMetadata = ecstore_bucket::metadata::BucketMetadata;
pub(crate) type BucketVersioningSys = ecstore_bucket::versioning_sys::BucketVersioningSys;
pub(crate) type DiskOption = ecstore_disk::DiskOption;
pub(crate) type ECStore = ecstore_storage::ECStore;
pub(crate) type Endpoint = ecstore_disk::endpoint::Endpoint;
pub(crate) type EndpointServerPools = ecstore_layout::EndpointServerPools;
pub(crate) type Endpoints = ecstore_layout::Endpoints;
pub(crate) type PoolEndpoints = ecstore_layout::PoolEndpoints;
pub(crate) type ReadCloser = ecstore_client::transition_api::ReadCloser;
pub(crate) type ReaderImpl = ecstore_client::transition_api::ReaderImpl;
pub(crate) type TierConfig = ecstore_tier::tier_config::TierConfig;
pub(crate) type TierConfigMgr = ecstore_tier::tier::TierConfigMgr;
pub(crate) type TierMinIO = ecstore_tier::tier_config::TierMinIO;
pub(crate) type TierType = ecstore_tier::tier_config::TierType;
pub(crate) type TransitionOptions = ecstore_bucket::lifecycle::lifecycle::TransitionOptions;
pub(crate) use ecstore_tier::warm_backend::WarmBackend as ScannerWarmBackend;
pub(crate) type WarmBackendGetOpts = ecstore_tier::warm_backend::WarmBackendGetOpts;

pub(crate) trait ScannerTestDiskExt {
    async fn read_metadata(&self, volume: &str, path: &str) -> ecstore_disk::error::Result<ecstore_disk::Bytes>;
}

impl<T> ScannerTestDiskExt for T
where
    T: ecstore_disk::DiskAPI,
{
    async fn read_metadata(&self, volume: &str, path: &str) -> ecstore_disk::error::Result<ecstore_disk::Bytes> {
        ecstore_disk::DiskAPI::read_metadata(self, volume, path).await
    }
}

#[allow(non_snake_case)]
pub(crate) fn EndpointServerPools(pools: Vec<PoolEndpoints>) -> EndpointServerPools {
    ecstore_layout::EndpointServerPools::from(pools)
}

pub(crate) struct GlobalTierConfigMgrCompat;

#[allow(non_upper_case_globals)]
pub(crate) static GLOBAL_TierConfigMgr: GlobalTierConfigMgrCompat = GlobalTierConfigMgrCompat;

impl std::ops::Deref for GlobalTierConfigMgrCompat {
    type Target = Arc<tokio::sync::RwLock<TierConfigMgr>>;

    fn deref(&self) -> &Self::Target {
        &ecstore_global::GLOBAL_TierConfigMgr
    }
}

pub(crate) fn build_transition_put_options(
    storage_class: String,
    metadata: HashMap<String, String>,
) -> ecstore_client::api_put_object::PutObjectOptions {
    ecstore_tier::warm_backend::build_transition_put_options(storage_class, metadata)
}

pub(crate) async fn enqueue_transition_for_existing_objects(api: Arc<ECStore>, bucket: &str) -> ecstore_error::Result<()> {
    ecstore_bucket::lifecycle::bucket_lifecycle_ops::enqueue_transition_for_existing_objects(api, bucket).await
}

pub(crate) async fn init_background_expiry(api: Arc<ECStore>) {
    ecstore_bucket::lifecycle::bucket_lifecycle_ops::init_background_expiry(api).await;
}

pub(crate) async fn get_bucket_metadata(bucket: &str) -> ecstore_error::Result<Arc<BucketMetadata>> {
    ecstore_bucket::metadata_sys::get(bucket).await
}

pub(crate) async fn init_bucket_metadata_sys(api: Arc<ECStore>, buckets: Vec<String>) {
    ecstore_bucket::metadata_sys::init_bucket_metadata_sys(api, buckets).await;
}

pub(crate) async fn init_local_disks(endpoint_pools: EndpointServerPools) -> ecstore_error::Result<()> {
    ecstore_storage::init_local_disks(endpoint_pools).await
}

pub(crate) async fn new_disk(ep: &Endpoint, opt: &DiskOption) -> ecstore_disk::error::Result<ecstore_disk::DiskStore> {
    ecstore_disk::new_disk(ep, opt).await
}

pub(crate) fn path2_bucket_object_with_base_path(base_path: &str, path: &str) -> (String, String) {
    ecstore_capacity::path2_bucket_object_with_base_path(base_path, path)
}

pub(crate) async fn update_bucket_metadata(
    bucket: &str,
    config_file: &str,
    data: Vec<u8>,
) -> ecstore_error::Result<OffsetDateTime> {
    ecstore_bucket::metadata_sys::update(bucket, config_file, data).await
}
