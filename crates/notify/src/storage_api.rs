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
    read_config_without_migrate_no_lock as read_notify_config_without_migrate_from_backend_no_lock,
    read_existing_server_config_no_lock as read_existing_notify_config_from_backend_no_lock,
    save_server_config_no_lock as save_notify_server_config_to_backend_no_lock,
    with_server_config_read_lock as with_notify_server_config_read_lock_from_backend,
    with_server_config_write_lock as with_notify_server_config_write_lock_from_backend,
};
use rustfs_ecstore::api::runtime::object_store_handle as resolve_notify_object_store_handle_from_backend;
pub use rustfs_ecstore::api::storage::ECStore as NotifyStore;

pub(crate) fn resolve_notify_object_store_handle() -> Option<Arc<NotifyStore>> {
    resolve_notify_object_store_handle_from_backend()
}

pub(crate) async fn read_notify_server_config_without_migrate_no_lock(
    store: Arc<NotifyStore>,
) -> Result<rustfs_config::server_config::Config, String> {
    read_notify_config_without_migrate_from_backend_no_lock(store)
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

pub(crate) async fn save_notify_server_config_no_lock(
    store: Arc<NotifyStore>,
    config: &rustfs_config::server_config::Config,
) -> Result<(), String> {
    save_notify_server_config_to_backend_no_lock(store, config)
        .await
        .map_err(|err| err.to_string())
}

pub(crate) async fn with_notify_server_config_write_lock<F, Fut, T>(store: Arc<NotifyStore>, operation: F) -> Result<T, String>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    with_notify_server_config_write_lock_from_backend(store, operation)
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
        read_existing_notify_server_config_no_lock, read_notify_server_config_without_migrate_no_lock,
        resolve_notify_object_store_handle, save_notify_server_config_no_lock, with_notify_server_config_read_lock,
        with_notify_server_config_write_lock,
    };
}
