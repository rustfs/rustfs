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

use rustfs_config::server_config::Config;
use rustfs_ecstore::api::config as ecstore_config;
use rustfs_ecstore::api::global as ecstore_global;
use rustfs_ecstore::api::storage as ecstore_storage;
use std::sync::Arc;

type NotifyStore = ecstore_storage::ECStore;

#[derive(Debug)]
pub(crate) enum NotifyConfigStoreError {
    StorageNotAvailable,
    Read(String),
    Save(String),
}

pub(crate) async fn update_server_config<F>(mut modifier: F) -> Result<Option<Config>, NotifyConfigStoreError>
where
    F: FnMut(&mut Config) -> bool,
{
    let Some(store) = resolve_notify_object_store_handle() else {
        return Err(NotifyConfigStoreError::StorageNotAvailable);
    };

    let mut new_config = read_notify_server_config_without_migrate(store.clone()).await?;

    if !modifier(&mut new_config) {
        return Ok(None);
    }

    save_notify_server_config(store, &new_config).await?;

    Ok(Some(new_config))
}

fn resolve_notify_object_store_handle() -> Option<Arc<NotifyStore>> {
    ecstore_global::resolve_object_store_handle()
}

async fn read_notify_server_config_without_migrate(store: Arc<NotifyStore>) -> Result<Config, NotifyConfigStoreError> {
    ecstore_config::com::read_config_without_migrate(store)
        .await
        .map_err(|err| NotifyConfigStoreError::Read(err.to_string()))
}

async fn save_notify_server_config(store: Arc<NotifyStore>, config: &Config) -> Result<(), NotifyConfigStoreError> {
    ecstore_config::com::save_server_config(store, config)
        .await
        .map_err(|err| NotifyConfigStoreError::Save(err.to_string()))
}
