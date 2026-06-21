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

use rustfs_ecstore::api::{
    config as ecstore_config, disk as ecstore_disk, error as ecstore_error, event as ecstore_event, global as ecstore_global,
    layout as ecstore_layout, rpc as ecstore_rpc, storage as ecstore_storage,
};

pub(crate) const TONIC_RPC_PREFIX: &str = ecstore_rpc::TONIC_RPC_PREFIX;

pub(crate) type ECStore = ecstore_storage::ECStore;
pub(crate) type EcstoreError = ecstore_error::Error;
pub(crate) type EcstoreEventArgs = ecstore_event::EventArgs;
pub(crate) type EcstoreResult<T> = ecstore_error::Result<T>;
pub(crate) type Endpoint = ecstore_disk::endpoint::Endpoint;
pub(crate) type EndpointServerPools = ecstore_layout::EndpointServerPools;

#[cfg(test)]
pub(crate) type Endpoints = ecstore_layout::Endpoints;
#[cfg(test)]
pub(crate) type PoolEndpoints = ecstore_layout::PoolEndpoints;

pub(crate) async fn read_ecstore_config(api: Arc<ECStore>, file: &str) -> EcstoreResult<Vec<u8>> {
    ecstore_config::com::read_config(api, file).await
}

pub(crate) async fn save_ecstore_config(api: Arc<ECStore>, file: &str, data: Vec<u8>) -> EcstoreResult<()> {
    ecstore_config::com::save_config(api, file, data).await
}

pub(crate) fn register_event_dispatch_hook<F>(hook: F) -> bool
where
    F: Fn(EcstoreEventArgs) + Send + Sync + 'static,
{
    ecstore_event::register_event_dispatch_hook(hook)
}

pub(crate) fn get_global_endpoints_opt() -> Option<EndpointServerPools> {
    ecstore_global::get_global_endpoints_opt()
}

pub(crate) fn get_global_lock_clients() -> Option<&'static HashMap<String, Arc<dyn rustfs_lock::client::LockClient>>> {
    ecstore_global::get_global_lock_clients()
}

pub(crate) async fn is_dist_erasure() -> bool {
    ecstore_global::is_dist_erasure().await
}

pub(crate) fn resolve_object_store_handle() -> Option<Arc<ECStore>> {
    ecstore_global::resolve_object_store_handle()
}

pub(crate) fn verify_rpc_signature(url: &str, method: &http::Method, headers: &http::HeaderMap) -> std::io::Result<()> {
    ecstore_rpc::verify_rpc_signature(url, method, headers)
}
