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

use super::{module_switch::resolve_notify_module_state, refresh_persisted_module_switches_from_store};
use crate::app::context::resolve_server_config;
use rustfs_ecstore::event_notification::{EventArgs as EcstoreEventArgs, register_event_dispatch_hook};
use rustfs_notify::EventArgs as NotifyEventArgs;
use rustfs_s3_common::EventName;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::spawn;
use tracing::{error, info, instrument, warn};

static NOTIFY_MODULE_ENABLED: AtomicBool = AtomicBool::new(rustfs_config::DEFAULT_NOTIFY_ENABLE);

fn server_config_from_context() -> Option<rustfs_ecstore::config::Config> {
    resolve_server_config()
}

pub fn refresh_notify_module_enabled() -> bool {
    let enabled = resolve_notify_module_state().enabled;
    NOTIFY_MODULE_ENABLED.store(enabled, Ordering::Relaxed);
    enabled
}

pub fn is_notify_module_enabled() -> bool {
    NOTIFY_MODULE_ENABLED.load(Ordering::Relaxed)
}

fn convert_ecstore_event_args(args: EcstoreEventArgs) -> NotifyEventArgs {
    let version_id = args.object.version_id.map(|v| v.to_string()).unwrap_or_default();
    let (host, port) = match args.host.rsplit_once(':') {
        Some((host, port)) => match port.parse::<u16>() {
            Ok(port) => (host.to_string(), port),
            Err(_) => (args.host, 0),
        },
        None => (args.host, 0),
    };
    let req_params = args.req_params.into_iter().collect();
    let resp_elements = args.resp_elements.into_iter().collect();

    NotifyEventArgs {
        event_name: EventName::from(args.event_name.as_str()),
        bucket_name: args.bucket_name,
        object: args.object,
        req_params,
        resp_elements,
        version_id,
        host,
        port,
        user_agent: args.user_agent,
    }
}

fn install_ecstore_event_dispatch_hook() {
    let installed = register_event_dispatch_hook(|args| {
        let notify_args = convert_ecstore_event_args(args);
        spawn(async move {
            rustfs_notify::notifier_global::notify(notify_args).await;
        });
    });

    if !installed {
        warn!("ECStore event dispatch hook was already registered");
    }
}

/// Shuts down the event notifier system gracefully
pub async fn shutdown_event_notifier() {
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
pub async fn init_event_notifier() {
    if let Err(err) = refresh_persisted_module_switches_from_store().await {
        warn!("Failed to refresh persisted notify module switch from store: {}", err);
    }

    let enabled = refresh_notify_module_enabled();
    if !enabled {
        info!(
            target: "rustfs::main::init_event_notifier",
            "Notify module is disabled, event notifier initialization is skipped. Enable the notify module first."
        );
        return;
    }

    info!(
        target: "rustfs::main::init_event_notifier",
        "Initializing event notifier..."
    );

    // 1. Get the global configuration loaded by ecstore
    let server_config = match server_config_from_context() {
        Some(config) => config,
        None => {
            warn!("Event notifier initialization failed: Global server config not loaded.");
            return;
        }
    };

    info!(
        target: "rustfs::main::init_event_notifier",
        "Event notifier configuration found, proceeding with initialization."
    );

    if let Some(system) = rustfs_notify::notification_system() {
        // Reuse the existing global system on re-enable so bucket rules, metrics,
        // and stream lifecycle stay aligned with the current process singleton.
        if let Err(e) = system.reload_config(server_config).await {
            error!("Failed to reload event notifier system: {}", e);
        } else {
            info!(
                target: "rustfs::main::init_event_notifier",
                "Event notifier system reloaded successfully."
            );
        }
    } else if let Err(e) = rustfs_notify::initialize(server_config).await {
        error!("Failed to initialize event notifier system: {}", e);
    } else {
        install_ecstore_event_dispatch_hook();
        info!(
            target: "rustfs::main::init_event_notifier",
            "Event notifier system initialized successfully."
        );
    }
}
