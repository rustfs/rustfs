//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use rustfs_config::DEFAULT_DELIMITER;
use rustfs_ecstore::config::GLOBAL_SERVER_CONFIG;
use tracing::{error, info, instrument, warn};

/// Shuts down the event notifier system gracefully
pub(crate) async fn shutdown_event_notifier() {
    info!("Shutting down event notifier system...");

    if !rustfs_notify::is_notification_system_initialized() {
        info!("Event notifier system is not initialized, nothing to shut down.");
        return;
    }

    let system = match rustfs_notify::notification_system() {
        Some(sys) => sys,
        None => {
            info!("Event notifier system is not initialized.");
            return;
        }
    };

    // Call the shutdown function from the rustfs_notify module
    system.shutdown().await;
    info!("Event notifier system shut down successfully.");
}

#[instrument]
pub(crate) async fn init_event_notifier() {
    info!(
        target: "rustfs::main::init_event_notifier",
        "Initializing event notifier..."
    );

    // 1. Get the global configuration loaded by ecstore
    let server_config = match GLOBAL_SERVER_CONFIG.get() {
        Some(config) => config.clone(), // Clone the config to pass ownership
        None => {
            warn!("Event notifier initialization failed: Global server config not loaded.");
            return;
        }
    };

    info!(
        target: "rustfs::main::init_event_notifier",
        "Global server configuration loaded successfully"
    );
    // 2. Check if the notify subsystem exists in the configuration, and skip initialization if it doesn't
    if server_config
        .get_value(rustfs_config::notify::NOTIFY_MQTT_SUB_SYS, DEFAULT_DELIMITER)
        .is_none()
        || server_config
            .get_value(rustfs_config::notify::NOTIFY_WEBHOOK_SUB_SYS, DEFAULT_DELIMITER)
            .is_none()
    {
        info!(
            target: "rustfs::main::init_event_notifier",
            "'notify' subsystem not configured, skipping event notifier initialization."
        );
        return;
    }

    info!(
        target: "rustfs::main::init_event_notifier",
        "Event notifier configuration found, proceeding with initialization."
    );

    // 3. Initialize the notification system asynchronously with a global configuration
    // Use direct await for better error handling and faster initialization
    if let Err(e) = rustfs_notify::initialize(server_config).await {
        error!("Failed to initialize event notifier system: {}", e);
    } else {
        info!(
            target: "rustfs::main::init_event_notifier",
            "Event notifier system initialized successfully."
        );
    }
}
