// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::storage_compat::set_global_rustfs_port;
use crate::{
    capacity::capacity_integration::init_capacity_management,
    config::Config,
    server::{ServiceState, ServiceStateManager, ShutdownHandle, start_http_server},
};
use rustfs_common::{GlobalReadiness, set_global_addr};
use rustfs_credentials::init_global_action_credentials;
use rustfs_utils::net::parse_and_resolve_address;
use std::{
    io::{Error, Result},
    net::SocketAddr,
    sync::Arc,
};
use tracing::{debug, error, info, warn};

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const LOG_SUBSYSTEM_AUTH: &str = "auth";
const EVENT_DEFAULT_CREDENTIALS_DETECTED: &str = "default_credentials_detected";
const EVENT_SERVER_CONFIG_SANITIZED: &str = "server_config_sanitized";
const EVENT_SERVER_STARTING: &str = "server_starting";
const EVENT_ACTION_CREDENTIALS_INITIALIZED: &str = "action_credentials_initialized";
const EVENT_ACTION_CREDENTIALS_INITIALIZATION_FAILED: &str = "action_credentials_initialization_failed";
const DEFAULT_CREDENTIALS_WARNING_MESSAGE: &str = "Detected default root credentials; set RUSTFS_ACCESS_KEY and RUSTFS_SECRET_KEY to non-default values for production deployments";

pub struct StartupListenContext {
    pub readiness: Arc<GlobalReadiness>,
    pub server_addr: SocketAddr,
    pub server_address: String,
}

pub struct StartupHttpServers {
    pub state_manager: Arc<ServiceStateManager>,
    pub s3_shutdown_tx: Option<ShutdownHandle>,
    pub console_shutdown_tx: Option<ShutdownHandle>,
}

pub async fn init_startup_listen_context(config: &Config) -> Result<StartupListenContext> {
    log_sanitized_server_config(config);
    let readiness = Arc::new(GlobalReadiness::new());

    if let Some(region_str) = &config.region {
        region_str
            .parse::<s3s::region::Region>()
            .map(crate::storage_compat::set_global_region)
            .map_err(|err| Error::other(format!("invalid region '{}': {}", region_str, err)))?;
    }

    let server_addr = parse_and_resolve_address(config.address.as_str()).map_err(Error::other)?;
    let server_port = server_addr.port();
    let server_address = server_addr.to_string();

    if config.is_using_default_credentials() {
        warn!(
            target: "rustfs::main::run",
            event = EVENT_DEFAULT_CREDENTIALS_DETECTED,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_AUTH,
            warning = DEFAULT_CREDENTIALS_WARNING_MESSAGE,
            "{DEFAULT_CREDENTIALS_WARNING_MESSAGE}"
        );
    }

    info!(
        target: "rustfs::main::run",
        event = EVENT_SERVER_STARTING,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        server_address = %server_address,
        ip = %server_addr.ip(),
        port = %server_port,
        version = %crate::version::get_version(),
        "Starting RustFS server"
    );

    init_startup_action_credentials(config)?;
    set_global_rustfs_port(server_port);
    set_global_addr(&config.address).await;

    Ok(StartupListenContext {
        readiness,
        server_addr,
        server_address,
    })
}

pub async fn init_startup_http_servers(config: &Config, readiness: Arc<GlobalReadiness>) -> Result<StartupHttpServers> {
    init_capacity_management().await;
    let state_manager = Arc::new(ServiceStateManager::new());
    state_manager.update(ServiceState::Starting);

    let s3_config = s3_http_server_config(config);
    let (s3_shutdown_tx, _) = start_http_server(&s3_config, readiness.clone()).await?;

    let console_shutdown_tx = match console_http_server_config(config) {
        Some(console_config) => Some(start_http_server(&console_config, readiness).await?.0),
        None => None,
    };

    Ok(StartupHttpServers {
        state_manager,
        s3_shutdown_tx: Some(s3_shutdown_tx),
        console_shutdown_tx,
    })
}

fn log_sanitized_server_config(config: &Config) {
    debug!(
        target: "rustfs::main::run",
        event = EVENT_SERVER_CONFIG_SANITIZED,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        address = %config.address,
        volume_count = config.volumes.len(),
        server_domain_count = config.server_domains.len(),
        console_enable = config.console_enable,
        console_address = %config.console_address,
        tls_enabled = config.tls_path.as_deref().is_some_and(|value| !value.trim().is_empty()),
        kms_enable = config.kms_enable,
        kms_backend = %config.kms_backend,
        region = config.region.as_deref().unwrap_or_default(),
        buffer_profile = %config.buffer_profile,
        "Loaded sanitized server configuration"
    );
}

fn init_startup_action_credentials(config: &Config) -> Result<()> {
    match init_global_action_credentials(Some(config.access_key.clone()), Some(config.secret_key.clone())) {
        Ok(_) => {
            debug!(
                target: "rustfs::main::run",
                event = EVENT_ACTION_CREDENTIALS_INITIALIZED,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_AUTH,
                result = "ok",
                "Initialized global action credentials"
            );
            Ok(())
        }
        Err(err) => {
            let msg = format!("init global action credentials failed: {err:?}");
            error!(
                target: "rustfs::main::run",
                event = EVENT_ACTION_CREDENTIALS_INITIALIZATION_FAILED,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_AUTH,
                error = %err,
                "Failed to initialize global action credentials"
            );
            Err(Error::other(msg))
        }
    }
}

fn s3_http_server_config(config: &Config) -> Config {
    let mut s3_config = config.clone();
    s3_config.console_enable = false;
    s3_config
}

fn console_http_server_config(config: &Config) -> Option<Config> {
    if config.console_enable && !config.console_address.is_empty() {
        let mut console_config = config.clone();
        console_config.address = console_config.console_address.clone();
        Some(console_config)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_CREDENTIALS_WARNING_MESSAGE, console_http_server_config, s3_http_server_config};
    use crate::config::Config;

    #[test]
    fn s3_http_server_config_disables_console_without_changing_address() {
        let mut config = Config::new("127.0.0.1:9000", vec!["/tmp/rustfs-data".to_string()]);
        config.console_enable = true;
        config.console_address = "127.0.0.1:9001".to_string();

        let s3_config = s3_http_server_config(&config);

        assert!(!s3_config.console_enable);
        assert_eq!(s3_config.address, "127.0.0.1:9000");
        assert_eq!(s3_config.console_address, "127.0.0.1:9001");
    }

    #[test]
    fn console_http_server_config_uses_console_address_when_enabled() {
        let mut config = Config::new("127.0.0.1:9000", vec!["/tmp/rustfs-data".to_string()]);
        config.console_enable = true;
        config.console_address = "127.0.0.1:9001".to_string();

        let Some(console_config) = console_http_server_config(&config) else {
            panic!("enabled console should build config");
        };

        assert_eq!(console_config.address, "127.0.0.1:9001");
        assert_eq!(console_config.console_address, "127.0.0.1:9001");
        assert!(console_config.console_enable);
    }

    #[test]
    fn console_http_server_config_skips_disabled_or_empty_console() {
        let mut config = Config::new("127.0.0.1:9000", vec!["/tmp/rustfs-data".to_string()]);
        config.console_enable = false;
        config.console_address = "127.0.0.1:9001".to_string();
        assert!(console_http_server_config(&config).is_none());

        config.console_enable = true;
        config.console_address.clear();
        assert!(console_http_server_config(&config).is_none());
    }

    #[test]
    fn is_using_default_credentials_returns_true_for_default_keys() {
        let mut config = Config::new("127.0.0.1:9000", Vec::new());
        config.console_enable = true;
        config.console_address = "127.0.0.1:9001".to_string();

        assert!(config.is_using_default_credentials());
    }

    #[test]
    fn is_using_default_credentials_returns_false_for_custom_keys() {
        let mut config = Config::new("127.0.0.1:9000", Vec::new());
        config.access_key = "custom-access-key".to_string();
        config.secret_key = "custom-secret-key".to_string();

        assert!(!config.is_using_default_credentials());
    }

    #[test]
    fn default_credentials_messages_are_actionable_without_exposing_values() {
        assert!(DEFAULT_CREDENTIALS_WARNING_MESSAGE.contains(rustfs_config::ENV_RUSTFS_ACCESS_KEY));
        assert!(DEFAULT_CREDENTIALS_WARNING_MESSAGE.contains(rustfs_config::ENV_RUSTFS_SECRET_KEY));
        assert!(!DEFAULT_CREDENTIALS_WARNING_MESSAGE.contains(rustfs_credentials::DEFAULT_ACCESS_KEY));
        assert!(!DEFAULT_CREDENTIALS_WARNING_MESSAGE.contains(rustfs_credentials::DEFAULT_SECRET_KEY));
    }
}
