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

use rustfs_audit::system::AuditSystemState;
use rustfs_audit::{AuditError, AuditResult, audit_system, init_audit_system};
use rustfs_config::DEFAULT_DELIMITER;
use rustfs_ecstore::config::GLOBAL_SERVER_CONFIG;
use tracing::{error, info, warn};

pub(crate) async fn start_audit_system() -> AuditResult<()> {
    info!(
        target: "rustfs::main::start_audit_system",
        "Step 1: Initializing the audit system..."
    );

    // 1. Get the global configuration loaded by ecstore
    let server_config = match GLOBAL_SERVER_CONFIG.get() {
        Some(config) => {
            info!(
                target: "rustfs::main::start_audit_system",
                "Global server configuration loads successfully: {:?}", config
            );
            config.clone()
        }
        None => {
            error!(
                target: "rustfs::main::start_audit_system",
                "Audit system initialization failed: Global server configuration not loaded."
            );
            return Err(AuditError::ConfigNotLoaded);
        }
    };

    info!(
        target: "rustfs::main::start_audit_system",
        "The global server configuration is loaded"
    );
    // 2. Check if the notify subsystem exists in the configuration, and skip initialization if it doesn't
    let mqtt_config = server_config.get_value(rustfs_config::audit::AUDIT_MQTT_SUB_SYS, DEFAULT_DELIMITER);
    let webhook_config = server_config.get_value(rustfs_config::audit::AUDIT_WEBHOOK_SUB_SYS, DEFAULT_DELIMITER);

    if mqtt_config.is_none() && webhook_config.is_none() {
        info!(
            target: "rustfs::main::start_audit_system",
            "Audit subsystem (MQTT/Webhook) is not configured, and audit system initialization is skipped."
        );
        return Ok(());
    }

    info!(
        target: "rustfs::main::start_audit_system",
        "Audit subsystem configuration detected (MQTT: {}, Webhook: {}) and started initializing the audit system.",
        mqtt_config.is_some(),
        webhook_config.is_some()
    );
    let system = init_audit_system();
    let state = system.get_state().await;
    if state == AuditSystemState::Running {
        warn!(
            target: "rustfs::main::start_audit_system",
            "The audit system is running, skip repeated initialization."
        );
        return Err(AuditError::AlreadyInitialized);
    }
    // Preparation before starting
    match system.start(server_config).await {
        Ok(_) => {
            info!(
                target: "rustfs::main::start_audit_system",
                "Audit system started successfully with time: {}.",
                chrono::Utc::now()
            );
            Ok(())
        }
        Err(e) => {
            error!(
                target: "rustfs::main::start_audit_system",
                "Audit system startup failed: {:?}",
                e
            );
            Err(e)
        }
    }
}

pub(crate) async fn stop_audit_system() -> AuditResult<()> {
    if let Some(system) = audit_system() {
        let state = system.get_state().await;
        if state == AuditSystemState::Stopped {
            warn!("Audit system already stopped");
            return Ok(());
        }
        // Prepare before stopping
        system.close().await?;
        // Record after stopping
        info!("Audit system stopped at {}", chrono::Utc::now());
        Ok(())
    } else {
        warn!("Audit system not initialized, cannot stop");
        Ok(())
    }
}
