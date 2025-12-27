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

use crate::protocols::ftps::driver::FtpsDriver;
use crate::protocols::session::context::{Protocol as SessionProtocol, SessionContext};
use crate::protocols::session::principal::ProtocolPrincipal;
use libunftp::{
    auth::{AuthenticationError, UserDetail},
    options::FtpsRequired,
    ServerError,
};
use rustfs_policy::auth::UserIdentity;
use std::fmt::{Debug, Display, Formatter};
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

/// FTPS user implementation
#[derive(Debug, Clone)]
pub struct FtpsUser {
    /// Username for the FTP session
    pub username: String,
    /// User's display name
    pub name: Option<String>,
    /// Protocol principal containing user identity and credentials
    pub principal: Arc<UserIdentity>,
    /// Session context for this user
    pub session_context: SessionContext,
}

impl UserDetail for FtpsUser {
    fn account_enabled(&self) -> bool {
        self.principal.credentials.status == "enabled"
    }

    fn home(&self) -> Option<&Path> {
        // RustFS uses a flat bucket structure or virtual paths,
        // so we don't restrict to a specific OS-level home directory here.
        // The Gateway layer handles S3 bucket access control.
        None
    }
}

impl Display for FtpsUser {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FtpsUser({})", self.username)
    }
}

/// FTPS server initialization error
#[derive(Debug, Error)]
pub enum FtpsInitError {
    #[error("failed to bind address {0}")]
    Bind(#[from] std::io::Error),

    #[error("server error: {0}")]
    Server(#[from] ServerError),

    #[error("invalid FTPS configuration: {0}")]
    InvalidConfig(String),

    #[error("TLS initialization failed: {0}")]
    TlsInit(String),
}

/// FTPS server configuration
#[derive(Debug, Clone)]
pub struct FtpsConfig {
    /// Server bind address
    pub bind_addr: SocketAddr,
    /// Passive port range (e.g., "40000-50000")
    pub passive_ports: Option<String>,
    /// Whether FTPS is required
    pub ftps_required: bool,
    /// Certificate file path
    pub cert_file: Option<String>,
    /// Private key file path
    pub key_file: Option<String>,
}

impl FtpsConfig {
    /// Validates the configuration
    pub async fn validate(&self) -> Result<(), FtpsInitError> {
        if self.ftps_required {
            if self.cert_file.is_none() || self.key_file.is_none() {
                return Err(FtpsInitError::InvalidConfig(
                    "FTPS is required but certificate or key file is missing".to_string(),
                ));
            }
        }

        if let Some(path) = &self.cert_file {
            if !tokio::fs::try_exists(path).await.unwrap_or(false) {
                return Err(FtpsInitError::InvalidConfig(format!(
                    "Certificate file not found: {}",
                    path
                )));
            }
        }

        if let Some(path) = &self.key_file {
            if !tokio::fs::try_exists(path).await.unwrap_or(false) {
                return Err(FtpsInitError::InvalidConfig(format!(
                    "Key file not found: {}",
                    path
                )));
            }
        }

        // Validate passive ports format
        if self.passive_ports.is_some() {
            self.parse_passive_ports()?;
        }

        Ok(())
    }

    /// Parse passive ports range from string format "start-end"
    fn parse_passive_ports(&self) -> Result<std::ops::RangeInclusive<u16>, FtpsInitError> {
        match &self.passive_ports {
            Some(ports) => {
                let parts: Vec<&str> = ports.split('-').collect();
                if parts.len() != 2 {
                    return Err(FtpsInitError::InvalidConfig(format!(
                        "Invalid passive ports format: {}, expected 'start-end'",
                        ports
                    )));
                }

                let start = parts[0].parse::<u16>().map_err(|e| {
                    FtpsInitError::InvalidConfig(format!("Invalid start port: {}", e))
                })?;
                let end = parts[1].parse::<u16>().map_err(|e| {
                    FtpsInitError::InvalidConfig(format!("Invalid end port: {}", e))
                })?;

                if start > end {
                    return Err(FtpsInitError::InvalidConfig(
                        "Start port cannot be greater than end port".to_string(),
                    ));
                }

                Ok(start..=end)
            }
            None => Err(FtpsInitError::InvalidConfig(
                "No passive ports configured".to_string(),
            )),
        }
    }
}

/// FTPS server implementation
pub struct FtpsServer {
    /// Server configuration
    config: FtpsConfig,
}

impl FtpsServer {
    /// Create a new FTPS server
    pub async fn new(config: FtpsConfig) -> Result<Self, FtpsInitError> {
        config.validate().await?;
        Ok(Self { config })
    }

    /// Start the FTPS server
    ///
    /// This method binds the listener first to ensure the port is available,
    /// then spawns the server loop in a background task.
    pub async fn start(
        &self,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<(), FtpsInitError> {
        info!("Initializing FTPS server on {}", self.config.bind_addr);

        // 1. Configure the server builder
        let mut server_builder = libunftp::ServerBuilder::with_authenticator(
            Box::new(|| FtpsDriver::new()),
            Arc::new(FtpsAuthenticator::new()),
        );

        // Configure passive ports
        if self.config.passive_ports.is_some() {
            let range = self.config.parse_passive_ports()?;
            debug!("Configuring passive ports range: {:?}", range);
            server_builder = server_builder.passive_ports(range);
        }

        // Configure FTPS / TLS
        if let Some(cert) = &self.config.cert_file {
            if let Some(key) = &self.config.key_file {
                debug!("Enabling FTPS with cert: {} and key: {}", cert, key);
                server_builder = server_builder.ftps(cert, key);

                if self.config.ftps_required {
                    info!("FTPS is explicitly required for all connections");
                    server_builder =
                        server_builder.ftps_required(FtpsRequired::All, FtpsRequired::All);
                }
            }
        } else if self.config.ftps_required {
            return Err(FtpsInitError::InvalidConfig(
                "FTPS required but certificates not provided".into(),
            ));
        }

        // Build the server instance
        let server = server_builder
            .build()
            .map_err(|e| FtpsInitError::Server(e))?;

        // 2. Start server in background task
        // libunftp's listen() binds to the address and runs the loop
        let bind_addr = self.config.bind_addr.to_string();
        let server_handle = tokio::spawn(async move {
            if let Err(e) = server.listen(bind_addr).await {
                error!("FTPS server runtime error: {}", e);
                return Err(FtpsInitError::Server(e));
            }
            Ok(())
        });

        // 3. Wait for shutdown signal or server failure
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
                        Err(FtpsInitError::Bind(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))
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
    async fn authenticate(
        &self,
        username: &str,
        creds: &libunftp::auth::Credentials,
    ) -> Result<FtpsUser, AuthenticationError> {
        use rustfs_iam::get;
        use rustfs_policy::auth::Credentials as S3Credentials;

        debug!("FTPS authentication attempt for user: {}", username);

        // 1. Access IAM system
        let iam_sys = get().map_err(|e| {
            error!("IAM system unavailable during FTPS auth: {}", e);
            AuthenticationError::ImplPropagated(
                "Internal authentication service unavailable".to_string(),
                Some(Box::new(e)),
            )
        })?;

        // 2. Map FTP credentials to S3 Credentials structure
        // Note: FTP PASSWORD is treated as S3 SECRET KEY
        let s3_creds = S3Credentials {
            access_key: username.to_string(),
            secret_key: creds.password.clone().unwrap_or_default(),
            // Fields below are not used for authentication verification, but for struct compliance
            session_token: String::new(),
            expiration: None,
            status: String::new(),
            parent_user: String::new(),
            groups: None,
            claims: None,
            name: None,
            description: None,
        };

        // 3. Validate Access Key (User existence)
        let (user_identity, is_valid) = iam_sys.check_key(&s3_creds.access_key).await.map_err(|e| {
            error!("IAM check_key failed for {}: {}", username, e);
            AuthenticationError::ImplPropagated(
                "Authentication verification failed".to_string(),
                Some(Box::new(e)),
            )
        })?;

        if !is_valid {
            warn!("FTPS login failed: Invalid access key '{}'", username);
            return Err(AuthenticationError::BadUser);
        }

        // 4. Validate Secret Key
        let identity = user_identity.ok_or_else(|| {
            error!("User identity missing despite valid key for {}", username);
            AuthenticationError::BadUser
        })?;

        // Constant time comparison is preferred if available, but for now simple eq
        if !identity.credentials.secret_key.eq(&s3_creds.secret_key) {
            warn!("FTPS login failed: Invalid secret key for '{}'", username);
            return Err(AuthenticationError::BadPassword);
        }

        // 5. Construct Session Context
        // LIMITATION: libunftp's Authenticator trait does not currently provide the client's source IP address.
        // We set it to a safe default (0.0.0.0) or loopback.
        // This means Policy conditions relying on `aws:SourceIp` will currently not work correctly for FTP.
        // TODO: Investigate wrapping the authenticator or using Proxy Protocol metadata if available in future libunftp versions.
        let source_ip: IpAddr = "0.0.0.0".parse().unwrap();

        let session_context = SessionContext::new(
            ProtocolPrincipal::new(Arc::new(identity.clone())),
            SessionProtocol::Ftps,
            source_ip,
        );

        let ftps_user = FtpsUser {
            username: username.to_string(),
            name: identity.credentials.name.clone(),
            principal: Arc::new(identity),
            session_context,
        };

        info!("FTPS user '{}' authenticated successfully", username);
        Ok(ftps_user)
    }
}