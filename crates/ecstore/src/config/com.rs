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

use crate::config::{Config, GLOBAL_STORAGE_CLASS, storageclass};
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result};
use crate::store_api::{ObjectInfo, ObjectOptions, PutObjReader, StorageAPI};
use http::HeaderMap;
use rustfs_config::DEFAULT_DELIMITER;
use rustfs_utils::path::SLASH_SEPARATOR;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::LazyLock;
use tracing::{error, warn};

pub const CONFIG_PREFIX: &str = "config";
const CONFIG_FILE: &str = "config.json";

pub const STORAGE_CLASS_SUB_SYS: &str = "storage_class";

static CONFIG_BUCKET: LazyLock<String> = LazyLock::new(|| format!("{RUSTFS_META_BUCKET}{SLASH_SEPARATOR}{CONFIG_PREFIX}"));

static SUB_SYSTEMS_DYNAMIC: LazyLock<HashSet<String>> = LazyLock::new(|| {
    let mut h = HashSet::new();
    h.insert(STORAGE_CLASS_SUB_SYS.to_owned());
    h
});
pub async fn read_config<S: StorageAPI>(api: Arc<S>, file: &str) -> Result<Vec<u8>> {
    let (data, _obj) = read_config_with_metadata(api, file, &ObjectOptions::default()).await?;
    Ok(data)
}

pub async fn read_config_with_metadata<S: StorageAPI>(
    api: Arc<S>,
    file: &str,
    opts: &ObjectOptions,
) -> Result<(Vec<u8>, ObjectInfo)> {
    let h = HeaderMap::new();
    let mut rd = api
        .get_object_reader(RUSTFS_META_BUCKET, file, None, h, opts)
        .await
        .map_err(|err| {
            if err == Error::FileNotFound || matches!(err, Error::ObjectNotFound(_, _)) {
                Error::ConfigNotFound
            } else {
                warn!("read_config_with_metadata: err: {:?}, file: {}", err, file);
                err
            }
        })?;

    let data = rd.read_all().await?;

    if data.is_empty() {
        return Err(Error::ConfigNotFound);
    }

    Ok((data, rd.object_info))
}

pub async fn save_config<S: StorageAPI>(api: Arc<S>, file: &str, data: Vec<u8>) -> Result<()> {
    save_config_with_opts(
        api,
        file,
        data,
        &ObjectOptions {
            max_parity: true,
            ..Default::default()
        },
    )
    .await
}

pub async fn delete_config<S: StorageAPI>(api: Arc<S>, file: &str) -> Result<()> {
    match api
        .delete_object(
            RUSTFS_META_BUCKET,
            file,
            ObjectOptions {
                delete_prefix: true,
                delete_prefix_object: true,
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(err) => {
            if err == Error::FileNotFound || matches!(err, Error::ObjectNotFound(_, _)) {
                Err(Error::ConfigNotFound)
            } else {
                Err(err)
            }
        }
    }
}

pub async fn save_config_with_opts<S: StorageAPI>(api: Arc<S>, file: &str, data: Vec<u8>, opts: &ObjectOptions) -> Result<()> {
    if let Err(err) = api
        .put_object(RUSTFS_META_BUCKET, file, &mut PutObjReader::from_vec(data), opts)
        .await
    {
        error!("save_config_with_opts: err: {:?}, file: {}", err, file);
        return Err(err);
    }
    Ok(())
}

fn new_server_config() -> Config {
    Config::new()
}

async fn new_and_save_server_config<S: StorageAPI>(api: Arc<S>) -> Result<Config> {
    let mut cfg = new_server_config();
    lookup_configs(&mut cfg, api.clone()).await;
    save_server_config(api, &cfg).await?;

    Ok(cfg)
}

fn get_config_file() -> String {
    format!("{CONFIG_PREFIX}{SLASH_SEPARATOR}{CONFIG_FILE}")
}

/// Handle the situation where the configuration file does not exist, create and save a new configuration
async fn handle_missing_config<S: StorageAPI>(api: Arc<S>, context: &str) -> Result<Config> {
    warn!("Configuration not found ({}): Start initializing new configuration", context);
    let cfg = new_and_save_server_config(api).await?;
    warn!("Configuration initialization complete ({})", context);
    Ok(cfg)
}

/// Handle configuration file read errors
fn handle_config_read_error(err: Error, file_path: &str) -> Result<Config> {
    error!("Read configuration failed (path: '{}'): {:?}", file_path, err);
    Err(err)
}

pub async fn read_config_without_migrate<S: StorageAPI>(api: Arc<S>) -> Result<Config> {
    let config_file = get_config_file();

    // Try to read the configuration file
    match read_config(api.clone(), &config_file).await {
        Ok(data) => read_server_config(api, &data).await,
        Err(Error::ConfigNotFound) => handle_missing_config(api, "Read the main configuration").await,
        Err(err) => handle_config_read_error(err, &config_file),
    }
}

async fn read_server_config<S: StorageAPI>(api: Arc<S>, data: &[u8]) -> Result<Config> {
    // If the provided data is empty, try to read from the file again
    if data.is_empty() {
        let config_file = get_config_file();
        warn!("Received empty configuration data, try to reread from '{}'", config_file);

        // Try to read the configuration again
        match read_config(api.clone(), &config_file).await {
            Ok(cfg_data) => {
                // TODO: decrypt
                let cfg = Config::unmarshal(&cfg_data)?;
                return Ok(cfg.merge());
            }
            Err(Error::ConfigNotFound) => return handle_missing_config(api, "Read alternate configuration").await,
            Err(err) => return handle_config_read_error(err, &config_file),
        }
    }

    // Process non-empty configuration data
    let cfg = Config::unmarshal(data)?;
    Ok(cfg.merge())
}

pub async fn save_server_config<S: StorageAPI>(api: Arc<S>, cfg: &Config) -> Result<()> {
    let data = cfg.marshal()?;

    let config_file = get_config_file();

    save_config(api, &config_file, data).await
}

pub async fn lookup_configs<S: StorageAPI>(cfg: &mut Config, api: Arc<S>) {
    // TODO: from etcd
    if let Err(err) = apply_dynamic_config(cfg, api).await {
        error!("apply_dynamic_config err {:?}", &err);
    }
}

async fn apply_dynamic_config<S: StorageAPI>(cfg: &mut Config, api: Arc<S>) -> Result<()> {
    for key in SUB_SYSTEMS_DYNAMIC.iter() {
        apply_dynamic_config_for_sub_sys(cfg, api.clone(), key).await?;
    }

    Ok(())
}

async fn apply_dynamic_config_for_sub_sys<S: StorageAPI>(cfg: &mut Config, api: Arc<S>, subsys: &str) -> Result<()> {
    let set_drive_counts = api.set_drive_counts();
    if subsys == STORAGE_CLASS_SUB_SYS {
        let kvs = cfg.get_value(STORAGE_CLASS_SUB_SYS, DEFAULT_DELIMITER).unwrap_or_default();

        for (i, count) in set_drive_counts.iter().enumerate() {
            match storageclass::lookup_config(&kvs, *count) {
                Ok(res) => {
                    if i == 0 && GLOBAL_STORAGE_CLASS.get().is_none() {
                        if let Err(r) = GLOBAL_STORAGE_CLASS.set(res) {
                            error!("GLOBAL_STORAGE_CLASS.set failed {:?}", r);
                        }
                    }
                }
                Err(err) => {
                    error!("init storage class err:{:?}", &err);
                    break;
                }
            }
        }
    }

    Ok(())
}
