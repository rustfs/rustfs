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

use rustfs_ecstore::api::bucket as ecstore_bucket;
use rustfs_ecstore::api::disk as ecstore_disk;
use rustfs_ecstore::api::error as ecstore_error;
use rustfs_ecstore::api::layout as ecstore_layout;
use rustfs_ecstore::api::storage as ecstore_storage;

pub(crate) type DiskStore = ecstore_disk::DiskStore;
pub(crate) type ECStore = ecstore_storage::ECStore;
pub(crate) type Endpoint = ecstore_disk::endpoint::Endpoint;
pub(crate) type EndpointServerPools = ecstore_layout::EndpointServerPools;
pub(crate) type Endpoints = ecstore_layout::Endpoints;
pub(crate) type PoolEndpoints = ecstore_layout::PoolEndpoints;

#[allow(non_snake_case)]
pub(crate) fn EndpointServerPools(pools: Vec<PoolEndpoints>) -> EndpointServerPools {
    ecstore_layout::EndpointServerPools::from(pools)
}

pub(crate) async fn init_bucket_metadata_sys(api: Arc<ECStore>, buckets: Vec<String>) {
    ecstore_bucket::metadata_sys::init_bucket_metadata_sys(api, buckets).await;
}

pub(crate) async fn init_local_disks(endpoint_pools: EndpointServerPools) -> ecstore_error::Result<()> {
    ecstore_storage::init_local_disks(endpoint_pools).await
}
