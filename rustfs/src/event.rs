use ecstore::config::GLOBAL_ServerConfig;
use rustfs_config::DEFAULT_DELIMITER;
use tracing::{error, info, instrument};

#[instrument]
pub(crate) async fn init_event_notifier() {
    info!("Initializing event notifier...");

    // 1. Get the global configuration loaded by ecstore
    let server_config = match GLOBAL_ServerConfig.get() {
        Some(config) => config.clone(), // Clone the config to pass ownership
        None => {
            error!("Event notifier initialization failed: Global server config not loaded.");
            return;
        }
    };

    info!("Global server configuration loaded successfully. config: {:?}", server_config);
    // 2. Check if the notify subsystem exists in the configuration, and skip initialization if it doesn't
    if server_config
        .get_value(rustfs_config::notify::NOTIFY_MQTT_SUB_SYS, DEFAULT_DELIMITER)
        .is_none()
        || server_config
            .get_value(rustfs_config::notify::NOTIFY_WEBHOOK_SUB_SYS, DEFAULT_DELIMITER)
            .is_none()
    {
        info!("'notify' subsystem not configured, skipping event notifier initialization.");
        return;
    }

    info!("Event notifier configuration found, proceeding with initialization.");

    // 3. Initialize the notification system asynchronously with a global configuration
    // Put it into a separate task to avoid blocking the main initialization process
    tokio::spawn(async move {
        if let Err(e) = rustfs_notify::initialize(server_config).await {
            error!("Failed to initialize event notifier system: {}", e);
        } else {
            info!("Event notifier system initialized successfully.");
        }
    });
}
