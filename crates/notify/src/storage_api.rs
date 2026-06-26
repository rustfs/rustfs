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
    read_config_without_migrate as read_notify_config_without_migrate_from_backend,
    save_server_config as save_notify_server_config_to_backend,
};
use rustfs_ecstore::api::global::resolve_object_store_handle as resolve_notify_object_store_handle_from_backend;
pub(crate) use rustfs_ecstore::api::storage::ECStore as NotifyStore;

pub(crate) fn resolve_notify_object_store_handle() -> Option<Arc<NotifyStore>> {
    resolve_notify_object_store_handle_from_backend()
}

pub(crate) async fn read_notify_server_config_without_migrate(
    store: Arc<NotifyStore>,
) -> Result<rustfs_config::server_config::Config, String> {
    read_notify_config_without_migrate_from_backend(store)
        .await
        .map_err(|err| err.to_string())
}

pub(crate) async fn save_notify_server_config(
    store: Arc<NotifyStore>,
    config: &rustfs_config::server_config::Config,
) -> Result<(), String> {
    save_notify_server_config_to_backend(store, config)
        .await
        .map_err(|err| err.to_string())
}

pub(crate) mod crate_boundary {
    pub(crate) use super::{
        read_notify_server_config_without_migrate, resolve_notify_object_store_handle, save_notify_server_config,
    };
}
