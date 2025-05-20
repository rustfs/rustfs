use crate::NotifierConfig;
use common::error::{Error, Result};
use ecstore::config::com::CONFIG_PREFIX;
use ecstore::config::error::{is_err_config_not_found, ConfigError};
use ecstore::disk::RUSTFS_META_BUCKET;
use ecstore::store::ECStore;
use ecstore::store_api::{ObjectInfo, ObjectOptions, PutObjReader};
use ecstore::store_err::is_err_object_not_found;
use ecstore::utils::path::SLASH_SEPARATOR;
use ecstore::StorageAPI;
use http::HeaderMap;
use lazy_static::lazy_static;
use std::io::Cursor;
use std::sync::{Arc, OnceLock};
use tracing::{error, instrument, warn};

lazy_static! {
    pub static ref GLOBAL_EventSys: EventSys = EventSys::new();
    pub static ref GLOBAL_EventSysConfig: OnceLock<NotifierConfig> = OnceLock::new();
}
/// * config file
const CONFIG_FILE: &str = "event.json";

/// event sys config
const EVENT: &str = "event";

#[derive(Debug)]
pub struct EventSys {}

impl Default for EventSys {
    fn default() -> Self {
        Self::new()
    }
}

impl EventSys {
    pub fn new() -> Self {
        Self {}
    }
    #[instrument(skip_all)]
    pub async fn init(&self, api: Arc<ECStore>) -> Result<()> {
        tracing::info!("event sys config init start");
        let cfg = read_config_without_migrate(api.clone().clone()).await?;
        let _ = GLOBAL_EventSysConfig.set(cfg);
        tracing::info!("event sys config init done");
        Ok(())
    }
}

/// get event sys config file
///
/// # Returns
///  NotifierConfig
pub fn get_event_notifier_config() -> &'static NotifierConfig {
    GLOBAL_EventSysConfig.get_or_init(NotifierConfig::default)
}

fn get_event_sys_file() -> String {
    format!("{}{}{}{}{}", CONFIG_PREFIX, SLASH_SEPARATOR, EVENT, SLASH_SEPARATOR, CONFIG_FILE)
}

/// read config without migrate
///
/// # Parameters
/// - `api`: StorageAPI
///
/// # Returns
/// Configuration information
pub async fn read_config_without_migrate<S: StorageAPI>(api: Arc<S>) -> Result<NotifierConfig> {
    let config_file = get_event_sys_file();
    let data = match read_config(api.clone(), config_file.as_str()).await {
        Ok(res) => res,
        Err(err) => {
            return if is_err_config_not_found(&err) {
                warn!("config not found, start to init");
                let cfg = new_and_save_server_config(api).await?;
                warn!("config init done");
                Ok(cfg)
            } else {
                error!("read config err {:?}", &err);
                Err(err)
            }
        }
    };

    read_server_config(api, data.as_slice()).await
}

/// save config with options
///
/// # Parameters
/// - `api`: StorageAPI
/// - `file`: file name
/// - `data`: data to save
/// - `opts`: object options
///
/// # Returns
/// Result
pub async fn save_config_with_opts<S: StorageAPI>(api: Arc<S>, file: &str, data: Vec<u8>, opts: &ObjectOptions) -> Result<()> {
    let size = data.len();
    let _ = api
        .put_object(RUSTFS_META_BUCKET, file, &mut PutObjReader::new(Box::new(Cursor::new(data)), size), opts)
        .await?;
    Ok(())
}

/// new server config
///
/// # Returns
/// NotifierConfig
fn new_server_config() -> NotifierConfig {
    NotifierConfig::new()
}

async fn new_and_save_server_config<S: StorageAPI>(api: Arc<S>) -> Result<NotifierConfig> {
    let cfg = new_server_config();
    save_server_config(api, &cfg).await?;

    Ok(cfg)
}

async fn read_server_config<S: StorageAPI>(api: Arc<S>, data: &[u8]) -> Result<NotifierConfig> {
    let cfg = {
        if data.is_empty() {
            let config_file = get_event_sys_file();
            let cfg_data = match read_config(api.clone(), config_file.as_str()).await {
                Ok(res) => res,
                Err(err) => {
                    return if is_err_config_not_found(&err) {
                        warn!("config not found init start");
                        let cfg = new_and_save_server_config(api).await?;
                        warn!("config not found init done");
                        Ok(cfg)
                    } else {
                        error!("read config err {:?}", &err);
                        Err(err)
                    }
                }
            };
            // TODO: decrypt

            NotifierConfig::unmarshal(cfg_data.as_slice())?
        } else {
            NotifierConfig::unmarshal(data)?
        }
    };

    Ok(cfg.merge())
}

/// save server config
///
/// # Parameters
/// - `api`: StorageAPI
/// - `cfg`: configuration to save
///
/// # Returns
/// Result
async fn save_server_config<S: StorageAPI>(api: Arc<S>, cfg: &NotifierConfig) -> Result<()> {
    let data = cfg.marshal()?;

    let config_file = get_event_sys_file();

    save_config(api, &config_file, data).await
}

/// save config
///
/// # Parameters
/// - `api`: StorageAPI
/// - `file`: file name
/// - `data`: data to save
///
/// # Returns
/// Result
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

/// delete config
///
/// # Parameters
/// - `api`: StorageAPI
/// - `file`: file name
///
/// # Returns
/// Result
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

/// read config
///
/// # Parameters
/// - `api`: StorageAPI
/// - `file`: file name
///
/// # Returns
/// Configuration data
pub async fn read_config<S: StorageAPI>(api: Arc<S>, file: &str) -> Result<Vec<u8>> {
    let (data, _obj) = read_config_with_metadata(api, file, &ObjectOptions::default()).await?;
    Ok(data)
}

/// read config with metadata
///
/// # Parameters
/// - `api`: StorageAPI
/// - `file`: file name
/// - `opts`: object options
///
/// # Returns
/// Configuration data and object info
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
