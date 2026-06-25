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

use super::{module_switch::resolve_notify_module_state, refresh_persisted_module_switches_from_store, runtime_sources};
use crate::storage::{EventArgs as EcstoreEventArgs, StorageObjectInfo, register_event_dispatch_hook};
use chrono::{DateTime, Utc};
use rustfs_notify::{EventArgs as NotifyEventArgs, NotifyObjectInfo};
use rustfs_s3_types::EventName;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::spawn;
use tracing::{error, info, instrument, warn};

static NOTIFY_MODULE_ENABLED: AtomicBool = AtomicBool::new(rustfs_config::DEFAULT_NOTIFY_ENABLE);

fn server_config_from_context() -> Option<rustfs_config::server_config::Config> {
    runtime_sources::server_config()
}

pub fn refresh_notify_module_enabled() -> bool {
    let enabled = resolve_notify_module_state().enabled;
    NOTIFY_MODULE_ENABLED.store(enabled, Ordering::Relaxed);
    enabled
}

pub fn is_notify_module_enabled() -> bool {
    NOTIFY_MODULE_ENABLED.load(Ordering::Relaxed)
}

pub(crate) fn convert_ecstore_object_info(object: StorageObjectInfo) -> NotifyObjectInfo {
    NotifyObjectInfo {
        bucket: object.bucket,
        name: object.name,
        size: object.size,
        etag: object.etag,
        content_type: object.content_type,
        user_defined: object
            .user_defined
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        version_id: object.version_id.map(|version_id| version_id.to_string()),
        mod_time: object
            .mod_time
            .and_then(|value| DateTime::<Utc>::from_timestamp(value.unix_timestamp(), value.nanosecond())),
        restore_expires: object
            .restore_expires
            .and_then(|value| DateTime::<Utc>::from_timestamp(value.unix_timestamp(), value.nanosecond())),
        storage_class: object.storage_class,
        transitioned_tier: (!object.transitioned_object.tier.is_empty()).then_some(object.transitioned_object.tier),
    }
}

fn convert_ecstore_event_args(args: EcstoreEventArgs) -> Option<NotifyEventArgs> {
    let version_id = args.object.version_id.map(|v| v.to_string()).unwrap_or_default();
    let (host, port) = parse_host_and_port(args.host);
    let req_params = args.req_params.into_iter().collect();
    let resp_elements = args.resp_elements.into_iter().collect();
    let event_name = match EventName::try_from_event_str(args.event_name.as_str()) {
        Ok(event_name) => event_name,
        Err(err) => {
            warn!(
                event_name = args.event_name,
                bucket = args.bucket_name,
                error = %err,
                "dropping ecstore event with invalid event name"
            );
            return None;
        }
    };

    Some(NotifyEventArgs {
        event_name,
        bucket_name: args.bucket_name,
        object: convert_ecstore_object_info(args.object),
        req_params,
        resp_elements,
        version_id,
        host,
        port,
        user_agent: args.user_agent,
    })
}

fn parse_host_and_port(host: String) -> (String, u16) {
    if let Ok(addr) = host.parse::<SocketAddr>() {
        return (addr.ip().to_string(), addr.port());
    }

    if host.chars().filter(|&c| c == ':').count() != 1 {
        return (host, 0);
    }

    match host.split_once(':') {
        Some((base, port)) if !base.is_empty() => match port.parse::<u16>() {
            Ok(port) => (base.to_string(), port),
            Err(_) => (host, 0),
        },
        _ => (host, 0),
    }
}

fn install_ecstore_event_dispatch_hook() {
    let installed = register_event_dispatch_hook(|args| {
        let Some(notify_args) = convert_ecstore_event_args(args) else {
            return;
        };
        spawn(async move {
            runtime_sources::notify_interface().notify(notify_args).await;
        });
    });

    if !installed {
        warn!("ECStore event dispatch hook was already registered");
    }
}

fn ensure_live_events_initialized() -> bool {
    if rustfs_notify::notification_system().is_some() {
        return true;
    }

    match rustfs_notify::initialize_live_events() {
        Ok(()) => {
            install_ecstore_event_dispatch_hook();
            true
        }
        Err(e) => {
            error!("Failed to initialize live event stream support: {}", e);
            false
        }
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
            "Notify module is disabled, initializing live event stream support only. Set {}=true to enable notification targets.",
            rustfs_config::ENV_NOTIFY_ENABLE
        );
        if ensure_live_events_initialized() {
            info!(
                target: "rustfs::main::init_event_notifier",
                "Live event stream support initialized successfully."
            );
        }
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
    } else {
        match rustfs_notify::initialize(server_config).await {
            Ok(()) => {
                install_ecstore_event_dispatch_hook();
                info!(
                    target: "rustfs::main::init_event_notifier",
                    "Event notifier system initialized successfully."
                );
            }
            Err(e) => error!("Failed to initialize event notifier system: {}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{convert_ecstore_object_info, parse_host_and_port};
    use crate::storage::StorageObjectInfo;
    use chrono::{DateTime, Utc};
    use rustfs_storage_api::TransitionedObject;
    use std::{collections::HashMap, sync::Arc};
    use time::{Duration, OffsetDateTime};

    #[test]
    fn parse_host_and_port_with_ipv4_and_port() {
        let (host, port) = parse_host_and_port("127.0.0.1:9000".to_string());
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 9000);
    }

    #[test]
    fn parse_host_and_port_with_bracketed_ipv6_and_port() {
        let (host, port) = parse_host_and_port("[::1]:9000".to_string());
        assert_eq!(host, "::1");
        assert_eq!(port, 9000);
    }

    #[test]
    fn parse_host_and_port_with_ipv6_without_port() {
        let (host, port) = parse_host_and_port("::1".to_string());
        assert_eq!(host, "::1");
        assert_eq!(port, 0);
    }

    #[test]
    fn parse_host_and_port_with_hostname_and_port() {
        let (host, port) = parse_host_and_port("localhost:9001".to_string());
        assert_eq!(host, "localhost");
        assert_eq!(port, 9001);
    }

    #[test]
    fn convert_ecstore_object_info_preserves_notify_event_fields() {
        let mod_time = OffsetDateTime::UNIX_EPOCH + Duration::seconds(42);
        let restore_expires = OffsetDateTime::UNIX_EPOCH + Duration::seconds(1_700_000_000);
        let mut metadata = HashMap::new();
        metadata.insert("x-amz-meta-key".to_string(), "value".to_string());

        let converted = convert_ecstore_object_info(StorageObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            size: 123,
            etag: Some("etag".to_string()),
            content_type: Some("text/plain".to_string()),
            user_defined: Arc::new(metadata),
            mod_time: Some(mod_time),
            restore_expires: Some(restore_expires),
            storage_class: Some("GLACIER".to_string()),
            transitioned_object: TransitionedObject {
                tier: "DEEP_ARCHIVE".to_string(),
                ..Default::default()
            },
            ..Default::default()
        });

        assert_eq!(converted.bucket, "bucket");
        assert_eq!(converted.name, "object");
        assert_eq!(converted.size, 123);
        assert_eq!(converted.etag.as_deref(), Some("etag"));
        assert_eq!(converted.content_type.as_deref(), Some("text/plain"));
        assert_eq!(converted.user_defined.get("x-amz-meta-key").map(String::as_str), Some("value"));
        assert_eq!(converted.mod_time, DateTime::<Utc>::from_timestamp(42, 0));
        assert_eq!(converted.restore_expires, DateTime::<Utc>::from_timestamp(1_700_000_000, 0));
        assert_eq!(converted.storage_class.as_deref(), Some("GLACIER"));
        assert_eq!(converted.transitioned_tier.as_deref(), Some("DEEP_ARCHIVE"));
    }
}
