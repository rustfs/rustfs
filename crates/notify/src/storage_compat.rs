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
use rustfs_ecstore::{config, global};

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
    let Some(store) = global::resolve_object_store_handle() else {
        return Err(NotifyConfigStoreError::StorageNotAvailable);
    };

    let mut new_config = config::com::read_config_without_migrate(store.clone())
        .await
        .map_err(|err| NotifyConfigStoreError::Read(err.to_string()))?;

    if !modifier(&mut new_config) {
        return Ok(None);
    }

    config::com::save_server_config(store, &new_config)
        .await
        .map_err(|err| NotifyConfigStoreError::Save(err.to_string()))?;

    Ok(Some(new_config))
}
