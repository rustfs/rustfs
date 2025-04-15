use super::error::{is_err_config_not_found, ConfigError};
use super::{storageclass, Config, GLOBAL_StorageClass, KVS};
use crate::disk::RUSTFS_META_BUCKET;
use crate::store_api::{ObjectInfo, ObjectOptions, PutObjReader, StorageAPI};
use crate::store_err::is_err_object_not_found;
use crate::utils::path::SLASH_SEPARATOR;
use common::error::{Error, Result};
use http::HeaderMap;
use lazy_static::lazy_static;
use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;
use tracing::{error, warn};

pub const CONFIG_PREFIX: &str = "config";
const CONFIG_FILE: &str = "config.json";

pub const STORAGE_CLASS_SUB_SYS: &str = "storage_class";
pub const DEFAULT_KV_KEY: &str = "_";

lazy_static! {
    static ref CONFIG_BUCKET: String = format!("{}{}{}", RUSTFS_META_BUCKET, SLASH_SEPARATOR, CONFIG_PREFIX);
    static ref SubSystemsDynamic: HashSet<String> = {
        let mut h = HashSet::new();
        h.insert(STORAGE_CLASS_SUB_SYS.to_owned());
        h
    };
}
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
            if is_err_object_not_found(&err) {
                Error::new(ConfigError::NotFound)
            } else {
                err
            }
        })?;

    let data = rd.read_all().await?;

    if data.is_empty() {
        return Err(Error::new(ConfigError::NotFound));
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
            if is_err_object_not_found(&err) {
                Err(Error::new(ConfigError::NotFound))
            } else {
                Err(err)
            }
        }
    }
}

pub async fn save_config_with_opts<S: StorageAPI>(api: Arc<S>, file: &str, data: Vec<u8>, opts: &ObjectOptions) -> Result<()> {
    let size = data.len();
    let _ = api
        .put_object(RUSTFS_META_BUCKET, file, &mut PutObjReader::new(Box::new(Cursor::new(data)), size), opts)
        .await?;
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

pub async fn read_config_without_migrate<S: StorageAPI>(api: Arc<S>) -> Result<Config> {
    let config_file = format!("{}{}{}", CONFIG_PREFIX, SLASH_SEPARATOR, CONFIG_FILE);
    let data = match read_config(api.clone(), config_file.as_str()).await {
        Ok(res) => res,
        Err(err) => {
            if is_err_config_not_found(&err) {
                warn!("config not found, start to init");
                let cfg = new_and_save_server_config(api).await?;
                warn!("config init done");
                return Ok(cfg);
            } else {
                error!("read config err {:?}", &err);
                return Err(err);
            }
        }
    };

    read_server_config(api, data.as_slice()).await
}

async fn read_server_config<S: StorageAPI>(api: Arc<S>, data: &[u8]) -> Result<Config> {
    let cfg = {
        if data.is_empty() {
            let config_file = format!("{}{}{}", CONFIG_PREFIX, SLASH_SEPARATOR, CONFIG_FILE);
            let cfg_data = match read_config(api.clone(), config_file.as_str()).await {
                Ok(res) => res,
                Err(err) => {
                    if is_err_config_not_found(&err) {
                        warn!("config not found init start");
                        let cfg = new_and_save_server_config(api).await?;
                        warn!("config not found init done");
                        return Ok(cfg);
                    } else {
                        error!("read config err {:?}", &err);
                        return Err(err);
                    }
                }
            };
            // TODO: decrypt

            Config::unmarshal(cfg_data.as_slice())?
        } else {
            Config::unmarshal(data)?
        }
    };

    Ok(cfg.merge())
}

async fn save_server_config<S: StorageAPI>(api: Arc<S>, cfg: &Config) -> Result<()> {
    let data = cfg.marshal()?;

    let config_file = format!("{}{}{}", CONFIG_PREFIX, SLASH_SEPARATOR, CONFIG_FILE);

    save_config(api, &config_file, data).await
}

pub async fn lookup_configs<S: StorageAPI>(cfg: &mut Config, api: Arc<S>) {
    // TODO: from etcd
    if let Err(err) = apply_dynamic_config(cfg, api).await {
        error!("apply_dynamic_config err {:?}", &err);
    }
}

async fn apply_dynamic_config<S: StorageAPI>(cfg: &mut Config, api: Arc<S>) -> Result<()> {
    for key in SubSystemsDynamic.iter() {
        apply_dynamic_config_for_sub_sys(cfg, api.clone(), key).await?;
    }

    Ok(())
}

async fn apply_dynamic_config_for_sub_sys<S: StorageAPI>(cfg: &mut Config, api: Arc<S>, subsys: &str) -> Result<()> {
    let set_drive_counts = api.set_drive_counts();
    if subsys == STORAGE_CLASS_SUB_SYS {
        let kvs = match cfg.get_value(STORAGE_CLASS_SUB_SYS, DEFAULT_KV_KEY) {
            Some(res) => res,
            None => KVS::new(),
        };

        for (i, count) in set_drive_counts.iter().enumerate() {
            match storageclass::lookup_config(&kvs, *count) {
                Ok(res) => {
                    if i == 0 && GLOBAL_StorageClass.get().is_none() {
                        if let Err(r) = GLOBAL_StorageClass.set(res) {
                            error!("GLOBAL_StorageClass.set failed {:?}", r);
                        }
                    }
                }
                Err(err) => {
                    error!("init storageclass err:{:?}", &err);
                    break;
                }
            }
        }
    }

    Ok(())
}
