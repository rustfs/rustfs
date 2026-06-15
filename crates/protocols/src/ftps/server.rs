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

use super::config::{FtpsConfig, FtpsInitError};
use super::driver::FtpsDriver;
use crate::common::client::s3::StorageBackend;
use crate::common::session::{Protocol, ProtocolPrincipal, SessionContext};
use crate::constants::{network::DEFAULT_SOURCE_IP, paths::ROOT_PATH};
use libunftp::options::FtpsRequired;
use rustfs_config::{DEFAULT_TLS_RELOAD_ENABLE, DEFAULT_TLS_RELOAD_INTERVAL, ENV_TLS_RELOAD_ENABLE, ENV_TLS_RELOAD_INTERVAL};
use rustfs_tls_runtime::{ReloadableServerCertResolver, TlsReloadOptions, spawn_server_cert_reload_loop};
use rustfs_utils::MaskedAccessKey;
use std::fmt::{Debug, Display, Formatter};
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};
use unftp_core::auth::{
    AuthenticationError, Authenticator, Credentials, Principal, UserDetail, UserDetailError, UserDetailProvider,
};

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_FTPS_SERVER: &str = "ftps_server";
const LOG_SUBSYSTEM_FTPS_AUTH: &str = "ftps_auth";
const EVENT_FTPS_SERVER_STATE: &str = "ftps_server_state";
const EVENT_FTPS_TLS_STATE: &str = "ftps_tls_state";
const EVENT_FTPS_CONFIG_STATE: &str = "ftps_config_state";
const EVENT_FTPS_RUNTIME_STATE: &str = "ftps_runtime_state";
const EVENT_FTPS_AUTH_STATE: &str = "ftps_auth_state";

/// FTPS user implementation
#[derive(Debug, Clone)]
pub struct FtpsUser {
    /// Username for the FTP session
    pub username: String,
    /// User's display name
    pub name: Option<String>,
    /// Session context for this user
    pub session_context: SessionContext,
}

impl UserDetail for FtpsUser {
    fn home(&self) -> Option<&Path> {
        Some(Path::new(ROOT_PATH))
    }
}

impl Display for FtpsUser {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.name {
            Some(display_name) => write!(f, "FtpsUser({} - {})", self.username, display_name),
            None => write!(f, "FtpsUser({})", self.username),
        }
    }
}

/// FTPS server implementation
pub struct FtpsServer<S> {
    /// Server configuration
    config: FtpsConfig,
    /// S3 storage backend
    storage: S,
}

impl<S> FtpsServer<S>
where
    S: StorageBackend + Clone + Send + Sync + 'static + Debug,
{
    fn tls_reload_options() -> TlsReloadOptions {
        TlsReloadOptions {
            enabled: rustfs_utils::get_env_bool(ENV_TLS_RELOAD_ENABLE, DEFAULT_TLS_RELOAD_ENABLE),
            interval: Duration::from_secs(rustfs_utils::get_env_u64(ENV_TLS_RELOAD_INTERVAL, DEFAULT_TLS_RELOAD_INTERVAL).max(5)),
            ..TlsReloadOptions::default()
        }
    }

    /// Create a new FTPS server
    pub async fn new(config: FtpsConfig, storage: S) -> Result<Self, FtpsInitError> {
        config.validate().await?;
        Ok(Self { config, storage })
    }

    /// Start the FTPS server
    ///
    /// This method binds the listener first to ensure the port is available,
    /// then spawns the server loop in a background task.
    pub async fn start(&self, mut shutdown_rx: broadcast::Receiver<()>) -> Result<(), FtpsInitError> {
        info!(
            event = EVENT_FTPS_SERVER_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_FTPS_SERVER,
            state = "starting",
            bind_addr = %self.config.bind_addr,
            tls_enabled = self.config.tls_enabled,
            ftps_required = self.config.ftps_required,
            "FTPS server starting"
        );
        let (reload_shutdown_tx, reload_shutdown_rx) = watch::channel(false);

        let storage_clone = self.storage.clone();
        let mut server_builder = libunftp::ServerBuilder::with_user_detail_provider(
            Box::new(move || FtpsDriver::new(storage_clone.clone())),
            Arc::new(FtpsUserDetailProvider),
        )
        .authenticator(Arc::new(FtpsAuthenticator::new()));

        // Configure passive ports for data connections
        if let Some(passive_ports) = &self.config.passive_ports {
            let range = self.config.parse_passive_ports()?;
            debug!(
                event = EVENT_FTPS_CONFIG_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_FTPS_SERVER,
                state = "passive_ports_configured",
                passive_ports = %passive_ports,
                passive_port_range = ?range,
                "FTPS passive ports configured"
            );
            server_builder = server_builder.passive_ports(range);
        } else {
            warn!(
                event = EVENT_FTPS_CONFIG_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_FTPS_SERVER,
                result = "system_assigned_passive_ports",
                "FTPS passive ports defaulted"
            );
        }

        // Configure external IP address for passive mode
        if let Some(ref external_ip) = self.config.external_ip {
            debug!(
                event = EVENT_FTPS_CONFIG_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_FTPS_SERVER,
                state = "external_ip_configured",
                external_ip = %external_ip,
                "FTPS external IP configured"
            );
            server_builder = server_builder.passive_host(external_ip.as_str());
        }

        // Configure both active and passive mode support
        use libunftp::options::ActivePassiveMode;
        server_builder = server_builder.active_passive_mode(ActivePassiveMode::ActiveAndPassive);
        debug!(
            event = EVENT_FTPS_CONFIG_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_FTPS_SERVER,
            state = "active_passive_mode_enabled",
            mode = "active_and_passive",
            "FTPS active/passive mode configured"
        );

        // Configure FTPS / TLS
        if self.config.tls_enabled {
            if let Some(cert_dir) = &self.config.cert_dir {
                debug!(
                    event = EVENT_FTPS_TLS_STATE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_FTPS_SERVER,
                    state = "enabled",
                    cert_dir = %cert_dir,
                    "FTPS TLS enabled"
                );

                let resolver = ReloadableServerCertResolver::load_from_directory(cert_dir)
                    .map_err(|e| FtpsInitError::InvalidConfig(format!("Failed to create certificate resolver: {}", e)))?;
                let _reload_task = spawn_server_cert_reload_loop(
                    "ftps",
                    resolver.clone(),
                    Self::tls_reload_options(),
                    reload_shutdown_rx.clone(),
                );

                // Build ServerConfig with SNI support
                let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

                let server_config = rustls::ServerConfig::builder()
                    .with_no_client_auth()
                    .with_cert_resolver(resolver);

                server_builder = server_builder.ftps_manual::<std::path::PathBuf>(Arc::new(server_config));

                if self.config.ftps_required {
                    debug!(
                        event = EVENT_FTPS_TLS_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_FTPS_SERVER,
                        state = "required",
                        "FTPS TLS required"
                    );
                    server_builder = server_builder.ftps_required(FtpsRequired::All, FtpsRequired::All);
                }
            } else if self.config.ftps_required {
                return Err(FtpsInitError::InvalidConfig(
                    "FTPS required but certificate directory not provided".into(),
                ));
            }
        } else {
            info!(
                event = EVENT_FTPS_TLS_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_FTPS_SERVER,
                state = "disabled",
                mode = "plain_ftp",
                "FTPS TLS disabled"
            );
        }

        // Build the server instance
        let server = server_builder.build().map_err(FtpsInitError::Server)?;

        // libunftp's listen() binds to the address and runs the loop
        let bind_addr = self.config.bind_addr.to_string();
        let server_handle = tokio::spawn(async move {
            if let Err(e) = server.listen(bind_addr).await {
                error!(
                    event = EVENT_FTPS_RUNTIME_STATE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_FTPS_SERVER,
                    result = "runtime_error",
                    error = %e,
                    "ftps runtime state changed"
                );
                return Err(FtpsInitError::Server(e));
            }
            Ok(())
        });

        // Wait for shutdown signal or server failure
        tokio::select! {
            result = server_handle => {
                let _ = reload_shutdown_tx.send(true);
                match result {
                    Ok(Ok(())) => {
                        debug!(
                            event = EVENT_FTPS_SERVER_STATE,
                            component = LOG_COMPONENT_PROTOCOLS,
                            subsystem = LOG_SUBSYSTEM_FTPS_SERVER,
                            state = "stopped",
                            result = "ok",
                            "FTPS server stopped"
                        );
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        error!(
                            event = EVENT_FTPS_RUNTIME_STATE,
                            component = LOG_COMPONENT_PROTOCOLS,
                            subsystem = LOG_SUBSYSTEM_FTPS_SERVER,
                            result = "internal_error",
                            error = %e,
                            "ftps runtime state changed"
                        );
                        Err(e)
                    }
                    Err(e) => {
                        error!(
                            event = EVENT_FTPS_RUNTIME_STATE,
                            component = LOG_COMPONENT_PROTOCOLS,
                            subsystem = LOG_SUBSYSTEM_FTPS_SERVER,
                            result = "task_failed",
                            error = %e,
                            "ftps runtime state changed"
                        );
                        Err(FtpsInitError::Bind(std::io::Error::other(e.to_string())))
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                info!(
                    event = EVENT_FTPS_SERVER_STATE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_FTPS_SERVER,
                    state = "shutdown_requested",
                    "FTPS shutdown requested"
                );
                let _ = reload_shutdown_tx.send(true);
                // libunftp listen() is not easily cancellable gracefully without dropping the future.
                // The select! dropping server_handle will close the listener.
                Ok(())
            }
        }
    }

    /// Get server configuration
    pub fn config(&self) -> &FtpsConfig {
        &self.config
    }

    /// Get storage backend
    pub fn storage(&self) -> &S {
        &self.storage
    }
}

/// FTPS user detail provider implementation
#[derive(Debug)]
pub struct FtpsUserDetailProvider;

#[async_trait::async_trait]
impl UserDetailProvider for FtpsUserDetailProvider {
    type User = FtpsUser;

    async fn provide_user_detail(&self, principal: &Principal) -> Result<Self::User, UserDetailError> {
        use rustfs_iam::get;
        let masked_username = MaskedAccessKey(&principal.username);

        // Access IAM system
        let iam_sys = get().map_err(|e| {
            error!(
                event = EVENT_FTPS_AUTH_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_FTPS_AUTH,
                result = "iam_unavailable",
                phase = "user_detail",
                error = %e,
                "FTPS user-detail IAM unavailable"
            );
            UserDetailError::ImplPropagated("Internal authentication service unavailable".to_string(), Some(Box::new(e)))
        })?;

        let (user_identity, _is_valid) = iam_sys.check_key(&principal.username).await.map_err(|e| {
            error!(
                event = EVENT_FTPS_AUTH_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_FTPS_AUTH,
                result = "check_key_failed",
                phase = "user_detail",
                username = %masked_username,
                error = %e,
                "FTPS user-detail key check failed"
            );
            UserDetailError::ImplPropagated("Authentication verification failed".to_string(), Some(Box::new(e)))
        })?;

        let identity = user_identity.ok_or_else(|| {
            error!(
                event = EVENT_FTPS_AUTH_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_FTPS_AUTH,
                result = "identity_missing",
                phase = "user_detail",
                username = %masked_username,
                "FTPS user-detail identity missing"
            );
            UserDetailError::UserNotFound {
                username: principal.username.clone(),
            }
        })?;

        let source_ip: IpAddr = DEFAULT_SOURCE_IP.parse().unwrap();

        let session_context = SessionContext::new(ProtocolPrincipal::new(Arc::new(identity.clone())), Protocol::Ftps, source_ip);

        let ftps_user = FtpsUser {
            username: principal.username.clone(),
            name: identity.credentials.name.clone(),
            session_context,
        };

        Ok(ftps_user)
    }
}

/// FTPS authenticator implementation
#[derive(Debug, Default)]
pub struct FtpsAuthenticator;

impl FtpsAuthenticator {
    /// Create a new FTPS authenticator
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl Authenticator for FtpsAuthenticator {
    /// Authenticate FTP user against RustFS IAM system
    async fn authenticate(&self, username: &str, creds: &Credentials) -> Result<Principal, AuthenticationError> {
        use rustfs_credentials::Credentials as S3Credentials;
        use rustfs_iam::get;
        let masked_username = MaskedAccessKey(username);

        // Access IAM system
        let iam_sys = get().map_err(|e| {
            error!(
                event = EVENT_FTPS_AUTH_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_FTPS_AUTH,
                result = "iam_unavailable",
                phase = "authenticate",
                error = %e,
                "FTPS auth IAM unavailable"
            );
            AuthenticationError::ImplPropagated("Internal authentication service unavailable".to_string(), Some(Box::new(e)))
        })?;

        let s3_creds = S3Credentials {
            access_key: username.to_string(),
            secret_key: creds.password.clone().unwrap_or_default(),
            session_token: String::new(),
            expiration: None,
            status: String::new(),
            parent_user: String::new(),
            groups: None,
            claims: None,
            name: None,
            description: None,
        };

        let (user_identity, is_valid) = iam_sys.check_key(&s3_creds.access_key).await.map_err(|e| {
            error!(
                event = EVENT_FTPS_AUTH_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_FTPS_AUTH,
                result = "check_key_failed",
                phase = "authenticate",
                username = %masked_username,
                error = %e,
                "FTPS auth key check failed"
            );
            AuthenticationError::ImplPropagated("Authentication verification failed".to_string(), Some(Box::new(e)))
        })?;

        if !is_valid {
            warn!(
                event = EVENT_FTPS_AUTH_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_FTPS_AUTH,
                result = "invalid_access_key",
                phase = "authenticate",
                username = %masked_username,
                "FTPS auth rejected access key"
            );
            return Err(AuthenticationError::BadUser);
        }

        let identity = user_identity.ok_or_else(|| {
            error!(
                event = EVENT_FTPS_AUTH_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_FTPS_AUTH,
                result = "identity_missing",
                phase = "authenticate",
                username = %masked_username,
                "FTPS auth identity missing"
            );
            AuthenticationError::BadUser
        })?;

        if !identity.credentials.secret_key.eq(&s3_creds.secret_key) {
            warn!(
                event = EVENT_FTPS_AUTH_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_FTPS_AUTH,
                result = "invalid_secret_key",
                phase = "authenticate",
                username = %masked_username,
                "FTPS auth rejected secret key"
            );
            return Err(AuthenticationError::BadPassword);
        }

        debug!(
            event = EVENT_FTPS_AUTH_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_FTPS_AUTH,
            result = "authenticated",
            phase = "authenticate",
            username = %masked_username,
            "FTPS auth accepted"
        );
        Ok(Principal {
            username: username.to_string(),
        })
    }
}
