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
use libunftp::auth::{AuthenticationError, UserDetail};
use libunftp::options::FtpsRequired;
use std::fmt::{Debug, Display, Formatter};
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

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
    S: StorageBackend + Clone + Send + Sync + 'static + std::fmt::Debug,
{
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
        info!("Initializing FTPS server on {}", self.config.bind_addr);

        let storage_clone = self.storage.clone();
        let mut server_builder = libunftp::ServerBuilder::with_authenticator(
            Box::new(move || FtpsDriver::new(storage_clone.clone())),
            Arc::new(FtpsAuthenticator::new()),
        );

        // Configure passive ports for data connections
        if let Some(passive_ports) = &self.config.passive_ports {
            let range = self.config.parse_passive_ports()?;
            info!("Configuring FTPS passive ports range: {:?} ({})", range, passive_ports);
            server_builder = server_builder.passive_ports(range);
        } else {
            warn!("No passive ports configured, using system-assigned ports");
        }

        // Configure external IP address for passive mode
        if let Some(ref external_ip) = self.config.external_ip {
            info!("Configuring FTPS external IP for passive mode: {}", external_ip);
            server_builder = server_builder.passive_host(external_ip.as_str());
        }

        // Configure both active and passive mode support
        use libunftp::options::ActivePassiveMode;
        server_builder = server_builder.active_passive_mode(ActivePassiveMode::ActiveAndPassive);
        info!("FTPS server configured for both active and passive mode support");

        // Configure FTPS / TLS
        if self.config.tls_enabled {
            if let Some(cert_dir) = &self.config.cert_dir {
                debug!("Enabling FTPS with multi-certificate support from directory: {}", cert_dir);

                // Load all certificates from directory
                let cert_key_pairs = rustfs_utils::load_all_certs_from_directory(cert_dir)
                    .map_err(|e| FtpsInitError::InvalidConfig(format!("Failed to load certificates: {}", e)))?;

                if cert_key_pairs.is_empty() {
                    return Err(FtpsInitError::InvalidConfig("No valid certificates found in directory".into()));
                }

                debug!("Loaded {} certificates for FTPS", cert_key_pairs.len());

                // Create multi-certificate resolver with SNI support
                let resolver = rustfs_utils::create_multi_cert_resolver(cert_key_pairs)
                    .map_err(|e| FtpsInitError::InvalidConfig(format!("Failed to create certificate resolver: {}", e)))?;

                // Build ServerConfig with SNI support
                let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

                let server_config = rustls::ServerConfig::builder()
                    .with_no_client_auth()
                    .with_cert_resolver(std::sync::Arc::new(resolver));

                server_builder = server_builder.ftps_manual::<std::path::PathBuf>(std::sync::Arc::new(server_config));

                if self.config.ftps_required {
                    info!("FTPS is explicitly required for all connections");
                    server_builder = server_builder.ftps_required(FtpsRequired::All, FtpsRequired::All);
                }
            } else if self.config.ftps_required {
                return Err(FtpsInitError::InvalidConfig(
                    "FTPS required but certificate directory not provided".into(),
                ));
            }
        } else {
            info!("TLS disabled, running in plain FTP mode");
        }

        // Build the server instance
        let server = server_builder.build().map_err(FtpsInitError::Server)?;

        // libunftp's listen() binds to the address and runs the loop
        let bind_addr = self.config.bind_addr.to_string();
        let server_handle = tokio::spawn(async move {
            if let Err(e) = server.listen(bind_addr).await {
                error!("FTPS server runtime error: {}", e);
                return Err(FtpsInitError::Server(e));
            }
            Ok(())
        });

        // Wait for shutdown signal or server failure
        tokio::select! {
            result = server_handle => {
                match result {
                    Ok(Ok(())) => {
                        info!("FTPS server stopped normally");
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        error!("FTPS server internal error: {}", e);
                        Err(e)
                    }
                    Err(e) => {
                        error!("FTPS server panic or task cancellation: {}", e);
                        Err(FtpsInitError::Bind(std::io::Error::other(e.to_string())))
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                info!("FTPS server received shutdown signal");
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
impl libunftp::auth::Authenticator<FtpsUser> for FtpsAuthenticator {
    /// Authenticate FTP user against RustFS IAM system
    async fn authenticate(&self, username: &str, creds: &libunftp::auth::Credentials) -> Result<FtpsUser, AuthenticationError> {
        use rustfs_credentials::Credentials as S3Credentials;
        use rustfs_iam::get;

        // Access IAM system
        let iam_sys = get().map_err(|e| {
            error!("IAM system unavailable during FTPS auth: {}", e);
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
            error!("IAM check_key failed for {}: {}", username, e);
            AuthenticationError::ImplPropagated("Authentication verification failed".to_string(), Some(Box::new(e)))
        })?;

        if !is_valid {
            warn!("FTPS login failed: Invalid access key '{}'", username);
            return Err(AuthenticationError::BadUser);
        }

        let identity = user_identity.ok_or_else(|| {
            error!("User identity missing despite valid key for {}", username);
            AuthenticationError::BadUser
        })?;

        if !identity.credentials.secret_key.eq(&s3_creds.secret_key) {
            warn!("FTPS login failed: Invalid secret key for '{}'", username);
            return Err(AuthenticationError::BadPassword);
        }

        let source_ip: IpAddr = DEFAULT_SOURCE_IP.parse().unwrap();

        let session_context = SessionContext::new(ProtocolPrincipal::new(Arc::new(identity.clone())), Protocol::Ftps, source_ip);

        let ftps_user = FtpsUser {
            username: username.to_string(),
            name: identity.credentials.name.clone(),
            session_context,
        };

        info!("FTPS user '{}' authenticated successfully", username);
        Ok(ftps_user)
    }
}
