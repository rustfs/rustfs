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

use std::sync::Arc;

use rustfs_ecstore::api::config::com::{
    read_existing_server_config_no_lock as read_existing_notify_config_from_backend_no_lock,
    read_server_config_snapshot as read_notify_server_config_snapshot_from_backend,
    save_server_config_snapshot as save_notify_server_config_snapshot_to_backend,
    with_server_config_read_lock as with_notify_server_config_read_lock_from_backend,
};
use rustfs_ecstore::api::runtime::object_store_handle as resolve_notify_object_store_handle_from_backend;
pub use rustfs_ecstore::api::storage::ECStore as NotifyStore;

pub(crate) fn resolve_notify_object_store_handle() -> Option<Arc<NotifyStore>> {
    resolve_notify_object_store_handle_from_backend()
}

pub(crate) type NotifyServerConfigSnapshot = rustfs_ecstore::api::config::com::ServerConfigSnapshot;

pub(crate) async fn read_notify_server_config_snapshot(store: Arc<NotifyStore>) -> Result<NotifyServerConfigSnapshot, String> {
    read_notify_server_config_snapshot_from_backend(store)
        .await
        .map_err(|err| err.to_string())
}

pub(crate) async fn read_existing_notify_server_config_no_lock(
    store: Arc<NotifyStore>,
) -> Result<rustfs_config::server_config::Config, String> {
    read_existing_notify_config_from_backend_no_lock(store)
        .await
        .map_err(|err| err.to_string())
}

pub(crate) async fn save_notify_server_config_snapshot(
    store: Arc<NotifyStore>,
    config: &rustfs_config::server_config::Config,
    snapshot: &NotifyServerConfigSnapshot,
) -> Result<bool, String> {
    save_notify_server_config_snapshot_to_backend(store, config, snapshot)
        .await
        .map_err(|err| err.to_string())
}

pub(crate) async fn with_notify_server_config_read_lock<F, Fut, T>(store: Arc<NotifyStore>, operation: F) -> Result<T, String>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    with_notify_server_config_read_lock_from_backend(store, operation)
        .await
        .map_err(|err| err.to_string())
}

pub(crate) mod crate_boundary {
    pub(crate) use super::{
        read_existing_notify_server_config_no_lock, read_notify_server_config_snapshot, resolve_notify_object_store_handle,
        save_notify_server_config_snapshot, with_notify_server_config_read_lock,
    };
}
