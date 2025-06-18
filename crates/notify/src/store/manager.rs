use crate::{adapter, ChannelAdapter, EventNotifierConfig};
use common::error::{Error, Result};
use ecstore::config::com::{read_config, save_config, CONFIG_PREFIX};
use ecstore::disk::RUSTFS_META_BUCKET;
use ecstore::store::ECStore;
use ecstore::store_api::ObjectOptions;
use ecstore::utils::path::SLASH_SEPARATOR;
use ecstore::StorageAPI;
use once_cell::sync::Lazy;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::instrument;

/// * config file
const CONFIG_FILE: &str = "event.json";

/// event sys config
const EVENT: &str = "event";

/// Global storage API access point
pub static GLOBAL_STORE_API: Lazy<Mutex<Option<Arc<ECStore>>>> = Lazy::new(|| Mutex::new(None));

/// Global event system configuration
pub static GLOBAL_EVENT_CONFIG: Lazy<Mutex<Option<EventNotifierConfig>>> = Lazy::new(|| Mutex::new(None));

/// EventManager Responsible for managing all operations of the event system
#[derive(Debug)]
pub struct EventManager {
    api: Arc<ECStore>,
}

impl EventManager {
    /// Create a new Event Manager
    pub async fn new(api: Arc<ECStore>) -> Self {
        // Set the global storage API
        {
            let mut global_api = GLOBAL_STORE_API.lock().await;
            *global_api = Some(api.clone());
        }

        Self { api }
    }

    /// Initialize the Event Manager
    ///
    /// # Returns
    /// If it succeeds, it returns configuration information, and if it fails, it returns an error
    #[instrument(skip_all)]
    pub async fn init(&self) -> Result<EventNotifierConfig> {
        tracing::info!("Event system configuration initialization begins");

        let cfg = match read_config_without_migrate(self.api.clone()).await {
            Ok(cfg) => {
                tracing::info!("The event system configuration was successfully read");
                cfg
            }
            Err(err) => {
                tracing::error!("Failed to initialize the event system configuration:{:?}", err);
                return Err(err);
            }
        };

        *GLOBAL_EVENT_CONFIG.lock().await = Some(cfg.clone());

        tracing::info!("The initialization of the event system configuration is complete");

        Ok(cfg)
    }

    /// Create a new configuration
    ///
    /// # Parameters
    /// - `cfg`: The configuration to be created
    ///
    /// # Returns
    /// The result of the operation
    pub async fn create_config(&self, cfg: &EventNotifierConfig) -> Result<()> {
        // Check whether the configuration already exists
        if read_event_config(self.api.clone()).await.is_ok() {
            return Err(Error::msg("The configuration already exists, use the update action"));
        }

        save_event_config(self.api.clone(), cfg).await?;
        *GLOBAL_EVENT_CONFIG.lock().await = Some(cfg.clone());

        Ok(())
    }

    /// Update the configuration
    ///
    /// # Parameters
    /// - `cfg`: The configuration to be updated
    ///
    /// # Returns
    /// The result of the operation
    pub async fn update_config(&self, cfg: &EventNotifierConfig) -> Result<()> {
        // Read the existing configuration first to merge
        let current_cfg = read_event_config(self.api.clone()).await.unwrap_or_default();

        // This is where the merge logic can be implemented
        let merged_cfg = self.merge_configs(current_cfg, cfg.clone());

        save_event_config(self.api.clone(), &merged_cfg).await?;
        *GLOBAL_EVENT_CONFIG.lock().await = Some(merged_cfg);

        Ok(())
    }

    /// Merge the two configurations
    fn merge_configs(&self, current: EventNotifierConfig, new: EventNotifierConfig) -> EventNotifierConfig {
        let mut merged = current;

        // Merge webhook configurations
        for (id, config) in new.webhook {
            merged.webhook.insert(id, config);
        }

        // Merge MQTT configurations
        for (id, config) in new.mqtt {
            merged.mqtt.insert(id, config);
        }

        merged
    }

    /// Delete the configuration
    pub async fn delete_config(&self) -> Result<()> {
        let config_file = get_event_config_file();
        self.api
            .delete_object(
                RUSTFS_META_BUCKET,
                &config_file,
                ObjectOptions {
                    delete_prefix: true,
                    delete_prefix_object: true,
                    ..Default::default()
                },
            )
            .await?;

        // Reset the global configuration to default
        // let _ = GLOBAL_EventSysConfig.set(self.read_config().await?);

        Ok(())
    }

    /// Read the configuration
    pub async fn read_config(&self) -> Result<EventNotifierConfig> {
        read_event_config(self.api.clone()).await
    }

    /// Create all enabled adapters
    pub async fn create_adapters(&self) -> Result<Vec<Arc<dyn ChannelAdapter>>> {
        let config = match GLOBAL_EVENT_CONFIG.lock().await.clone() {
            Some(cfg) => cfg,
            None => return Err(Error::msg("The global configuration is not initialized")),
        };

        let adapter_configs = config.to_adapter_configs();
        match adapter::create_adapters(adapter_configs).await {
            Ok(adapters) => Ok(adapters),
            Err(err) => {
                tracing::error!("Failed to create adapters: {:?}", err);
                Err(Error::from(err))
            }
        }
    }
}

/// Get the Global Storage API
pub async fn get_global_store_api() -> Option<Arc<ECStore>> {
    GLOBAL_STORE_API.lock().await.clone()
}

/// Get the Global Storage API
pub async fn get_global_event_config() -> Option<EventNotifierConfig> {
    GLOBAL_EVENT_CONFIG.lock().await.clone()
}

/// Read event configuration
async fn read_event_config<S: StorageAPI>(api: Arc<S>) -> Result<EventNotifierConfig> {
    let config_file = get_event_config_file();
    let data = read_config(api, &config_file).await?;

    EventNotifierConfig::unmarshal(&data)
}

/// Save the event configuration
async fn save_event_config<S: StorageAPI>(api: Arc<S>, config: &EventNotifierConfig) -> Result<()> {
    let config_file = get_event_config_file();
    let data = config.marshal()?;

    save_config(api, &config_file, data).await
}

/// Get the event profile path
fn get_event_config_file() -> String {
    format!("{}{}{}{}{}", CONFIG_PREFIX, SLASH_SEPARATOR, EVENT, SLASH_SEPARATOR, CONFIG_FILE)
}

/// Read the configuration file and create a default configuration if it doesn't exist
pub async fn read_config_without_migrate<S: StorageAPI>(api: Arc<S>) -> Result<EventNotifierConfig> {
    let config_file = get_event_config_file();
    let data = match read_config(api.clone(), &config_file).await {
        Ok(data) => {
            if data.is_empty() {
                return new_and_save_event_config(api).await;
            }
            data
        }
        Err(err) if ecstore::config::error::is_err_config_not_found(&err) => {
            tracing::warn!("If the configuration file does not exist, start initializing the default configuration");
            return new_and_save_event_config(api).await;
        }
        Err(err) => {
            tracing::error!("Read configuration file error: {:?}", err);
            return Err(err);
        }
    };

    // Parse configuration
    let cfg = EventNotifierConfig::unmarshal(&data)?;
    Ok(cfg)
}

/// Create and save a new configuration
async fn new_and_save_event_config<S: StorageAPI>(api: Arc<S>) -> Result<EventNotifierConfig> {
    let cfg = EventNotifierConfig::default();
    save_event_config(api, &cfg).await?;

    Ok(cfg)
}
