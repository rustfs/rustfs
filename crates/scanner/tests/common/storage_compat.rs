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

use std::sync::Arc;
use time::OffsetDateTime;

pub(crate) const BUCKET_LIFECYCLE_CONFIG: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_LIFECYCLE_CONFIG;
pub(crate) const STORAGE_FORMAT_FILE: &str = rustfs_ecstore::api::disk::STORAGE_FORMAT_FILE;

pub(crate) type BucketMetadata = rustfs_ecstore::api::bucket::metadata::BucketMetadata;
pub(crate) type BucketVersioningSys = rustfs_ecstore::api::bucket::versioning_sys::BucketVersioningSys;
pub(crate) type DiskOption = rustfs_ecstore::api::disk::DiskOption;
pub(crate) type ECStore = rustfs_ecstore::api::storage::ECStore;
pub(crate) type Endpoint = rustfs_ecstore::api::disk::endpoint::Endpoint;
pub(crate) type Endpoints = rustfs_ecstore::api::layout::Endpoints;
pub(crate) type PoolEndpoints = rustfs_ecstore::api::layout::PoolEndpoints;
pub(crate) type ReadCloser = rustfs_ecstore::api::client::transition_api::ReadCloser;
pub(crate) type ReaderImpl = rustfs_ecstore::api::client::transition_api::ReaderImpl;
pub(crate) type TierConfig = rustfs_ecstore::api::tier::tier_config::TierConfig;
pub(crate) type TierMinIO = rustfs_ecstore::api::tier::tier_config::TierMinIO;
pub(crate) type TierType = rustfs_ecstore::api::tier::tier_config::TierType;
pub(crate) type TransitionOptions = rustfs_ecstore::api::bucket::lifecycle::lifecycle::TransitionOptions;
pub(crate) type WarmBackendGetOpts = rustfs_ecstore::api::tier::warm_backend::WarmBackendGetOpts;

pub(crate) use rustfs_ecstore::api::disk::DiskAPI;
pub(crate) use rustfs_ecstore::api::global::GLOBAL_TierConfigMgr;
pub(crate) use rustfs_ecstore::api::layout::EndpointServerPools;
pub(crate) use rustfs_ecstore::api::tier::warm_backend::WarmBackend;
pub(crate) use rustfs_ecstore::api::tier::warm_backend::build_transition_put_options;

pub(crate) async fn enqueue_transition_for_existing_objects(
    api: Arc<ECStore>,
    bucket: &str,
) -> rustfs_ecstore::api::error::Result<()> {
    rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::enqueue_transition_for_existing_objects(api, bucket).await
}

pub(crate) async fn init_background_expiry(api: Arc<ECStore>) {
    rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::init_background_expiry(api).await;
}

pub(crate) async fn get_bucket_metadata(bucket: &str) -> rustfs_ecstore::api::error::Result<Arc<BucketMetadata>> {
    rustfs_ecstore::api::bucket::metadata_sys::get(bucket).await
}

pub(crate) async fn init_bucket_metadata_sys(api: Arc<ECStore>, buckets: Vec<String>) {
    rustfs_ecstore::api::bucket::metadata_sys::init_bucket_metadata_sys(api, buckets).await;
}

pub(crate) async fn init_local_disks(endpoint_pools: EndpointServerPools) -> rustfs_ecstore::api::error::Result<()> {
    rustfs_ecstore::api::storage::init_local_disks(endpoint_pools).await
}

pub(crate) async fn new_disk(
    ep: &Endpoint,
    opt: &DiskOption,
) -> rustfs_ecstore::api::disk::error::Result<rustfs_ecstore::api::disk::DiskStore> {
    rustfs_ecstore::api::disk::new_disk(ep, opt).await
}

pub(crate) fn path2_bucket_object_with_base_path(base_path: &str, path: &str) -> (String, String) {
    rustfs_ecstore::api::capacity::path2_bucket_object_with_base_path(base_path, path)
}

pub(crate) async fn update_bucket_metadata(
    bucket: &str,
    config_file: &str,
    data: Vec<u8>,
) -> rustfs_ecstore::api::error::Result<OffsetDateTime> {
    rustfs_ecstore::api::bucket::metadata_sys::update(bucket, config_file, data).await
}
