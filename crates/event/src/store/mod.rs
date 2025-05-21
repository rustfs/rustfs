pub(crate) mod event;

use crate::NotifierConfig;
use common::error::Result;
use ecstore::config::com::{read_config, save_config, CONFIG_PREFIX};
use ecstore::config::error::is_err_config_not_found;
use ecstore::store::ECStore;
use ecstore::utils::path::SLASH_SEPARATOR;
use ecstore::StorageAPI;
use lazy_static::lazy_static;
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
        Ok(data) => {
            if data.is_empty() {
                return new_and_save_server_config(api).await;
            }
            data
        }
        Err(err) if is_err_config_not_found(&err) => {
            warn!("config not found, start to init");
            return new_and_save_server_config(api).await;
        }
        Err(err) => {
            error!("read config error: {:?}", err);
            return Err(err);
        }
    };

    // TODO: decrypt if needed
    let cfg = NotifierConfig::unmarshal(data.as_slice())?;
    Ok(cfg.merge())
}
