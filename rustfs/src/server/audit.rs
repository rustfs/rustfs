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

use crate::app::context::resolve_server_config;
use rustfs_audit::{AuditError, AuditResult, audit_system, init_audit_system, system::AuditSystemState};
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{info, warn};

static AUDIT_MODULE_ENABLED: AtomicBool = AtomicBool::new(rustfs_config::DEFAULT_AUDIT_ENABLE);

fn server_config_from_context() -> Option<rustfs_ecstore::config::Config> {
    resolve_server_config()
}

pub fn refresh_audit_module_enabled() -> bool {
    let enabled = rustfs_utils::get_env_bool(rustfs_config::ENV_AUDIT_ENABLE, rustfs_config::DEFAULT_AUDIT_ENABLE);
    AUDIT_MODULE_ENABLED.store(enabled, Ordering::Relaxed);
    enabled
}

pub fn is_audit_module_enabled() -> bool {
    AUDIT_MODULE_ENABLED.load(Ordering::Relaxed)
}

fn has_any_audit_targets(config: &rustfs_ecstore::config::Config) -> bool {
    for subsystem in [
        rustfs_config::audit::AUDIT_MQTT_SUB_SYS,
        rustfs_config::audit::AUDIT_NATS_SUB_SYS,
        rustfs_config::audit::AUDIT_PULSAR_SUB_SYS,
        rustfs_config::audit::AUDIT_WEBHOOK_SUB_SYS,
    ] {
        let Some(targets) = config.0.get(subsystem) else {
            continue;
        };
        if targets.keys().any(|key| key != rustfs_config::DEFAULT_DELIMITER) {
            return true;
        }
    }
    false
}

/// Start the audit system.
/// This function checks if the audit subsystem is configured in the global server configuration.
/// If configured, it initializes and starts the audit system.
/// If not configured, it skips the initialization.
/// It also handles cases where the audit system is already running or if the global configuration is not loaded.
pub async fn start_audit_system() -> AuditResult<()> {
    let enabled = refresh_audit_module_enabled();
    if !enabled {
        info!(
            target: "rustfs::main::start_audit_system",
            "Audit module is disabled by RUSTFS_AUDIT_ENABLE=false, audit system initialization is skipped."
        );
        return Ok(());
    }

    info!(
        target: "rustfs::main::start_audit_system",
        "Initializing the audit system..."
    );

    // 1. Get the global configuration loaded by ecstore
    let server_config = match server_config_from_context() {
        Some(config) => {
            info!(
                target: "rustfs::main::start_audit_system",
                "Global server configuration loads successfully: {:?}", config
            );
            config
        }
        None => {
            warn!(
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
    let has_targets = has_any_audit_targets(&server_config);
    if !has_targets {
        info!(
            target: "rustfs::main::start_audit_system",
            "Audit subsystem (Webhook/MQTT/NATS/Pulsar) is not configured, and audit system initialization is skipped."
        );
        return Ok(());
    }

    info!(
        target: "rustfs::main::start_audit_system",
        "Audit subsystem configuration detected and started initializing the audit system."
    );
    // 3. Initialize and start the audit system
    let system = init_audit_system();
    // Check if the audit system is already running
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
                jiff::Zoned::now()
            );
            Ok(())
        }
        Err(e) => {
            warn!(
                target: "rustfs::main::start_audit_system",
                "Audit system startup failed: {:?}",
                e
            );
            Err(e)
        }
    }
}

/// Stop the audit system.
/// This function checks if the audit system is initialized and running.
/// If it is running, it prepares to stop the system, stops it, and records the stop time.
/// If the system is already stopped or not initialized, it logs a warning and returns.
pub async fn stop_audit_system() -> AuditResult<()> {
    if let Some(system) = audit_system() {
        let state = system.get_state().await;
        if state == AuditSystemState::Stopped {
            warn!("Audit system already stopped");
            return Ok(());
        }
        // Prepare before stopping
        system.close().await?;
        // Record after stopping
        info!("Audit system stopped at {}", jiff::Zoned::now());
        Ok(())
    } else {
        warn!("Audit system not initialized, cannot stop");
        Ok(())
    }
}
