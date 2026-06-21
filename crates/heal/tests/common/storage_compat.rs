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

pub(crate) type DiskStore = rustfs_ecstore::api::disk::DiskStore;
pub(crate) type ECStore = rustfs_ecstore::api::storage::ECStore;
pub(crate) type Endpoint = rustfs_ecstore::api::disk::endpoint::Endpoint;
pub(crate) type EndpointServerPools = rustfs_ecstore::api::layout::EndpointServerPools;
pub(crate) type Endpoints = rustfs_ecstore::api::layout::Endpoints;
pub(crate) type PoolEndpoints = rustfs_ecstore::api::layout::PoolEndpoints;

#[allow(non_snake_case)]
pub(crate) fn EndpointServerPools(pools: Vec<PoolEndpoints>) -> EndpointServerPools {
    rustfs_ecstore::api::layout::EndpointServerPools::from(pools)
}

pub(crate) async fn init_bucket_metadata_sys(api: Arc<ECStore>, buckets: Vec<String>) {
    rustfs_ecstore::api::bucket::metadata_sys::init_bucket_metadata_sys(api, buckets).await;
}

pub(crate) async fn init_local_disks(endpoint_pools: EndpointServerPools) -> rustfs_ecstore::api::error::Result<()> {
    rustfs_ecstore::api::storage::init_local_disks(endpoint_pools).await
}
