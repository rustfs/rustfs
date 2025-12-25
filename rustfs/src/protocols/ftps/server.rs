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
    auth::UserDetail,
    options::FtpsRequired,
    ServerError,
};
use rustfs_policy::auth::UserIdentity;
use std::fmt::{Debug, Display, Formatter};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::broadcast;
use tracing::{error, info};

/// FTPS user implementation
///
/// This user type carries the principal information needed for
/// authentication and authorization in the FTPS protocol.
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
        None // No home directory restriction for S3
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

    #[error("TLS initialization failed")]
    TlsInit(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// FTPS server configuration
#[derive(Debug, Clone)]
pub struct FtpsConfig {
    /// Server bind address
    pub bind_addr: SocketAddr,
    /// Passive port range
    pub passive_ports: Option<String>,
    /// Whether FTPS is required
    pub ftps_required: bool,
    /// Certificate file path
    pub cert_file: Option<String>,
    /// Private key file path
    pub key_file: Option<String>,
}

impl FtpsConfig {
    /// Parse passive ports range from string format "start-end"
    fn parse_passive_ports(&self) -> Option<std::ops::RangeInclusive<u16>> {
        let ports = self.passive_ports.as_ref()?;
        let parts: Vec<&str> = ports.split('-').collect();
        if parts.len() != 2 {
            return None;
        }

        let start = parts[0].parse::<u16>().ok()?;
        let end = parts[1].parse::<u16>().ok()?;
        Some(start..=end)
    }
}

/// FTPS server implementation
///
/// This server MUST only provide capabilities
/// that match what an external S3 client can do.
pub struct FtpsServer {
    /// Server configuration
    config: FtpsConfig,
}

impl FtpsServer {
    /// Create a new FTPS server
    pub async fn new(config: FtpsConfig) -> Result<Self, FtpsInitError> {
        Ok(Self { config })
    }

    /// Start the FTPS server
    ///
    /// MINIO CONSTRAINT: Must follow the same lifecycle management as other services
    pub async fn start(&self, mut shutdown_rx: broadcast::Receiver<()>) -> Result<(), FtpsInitError> {
        info!("Starting FTPS server on {}", self.config.bind_addr);

        // Create a new server instance for the background task
        let mut server_builder = libunftp::ServerBuilder::with_authenticator(
            Box::new(|| FtpsDriver::new()),
            Arc::new(FtpsAuthenticator::new())
        );

        // Configure passive ports if specified
        if let Some(ports_range) = self.config.parse_passive_ports() {
            server_builder = server_builder.passive_ports(ports_range);
        }

        // Configure FTPS if required
        if self.config.ftps_required {
            let cert_file = self.config.cert_file.as_ref()
                .ok_or_else(|| FtpsInitError::InvalidConfig("FTPS required but certificate file not specified".to_string()))?;
            let key_file = self.config.key_file.as_ref()
                .ok_or_else(|| FtpsInitError::InvalidConfig("FTPS required but key file not specified".to_string()))?;

            server_builder = server_builder.ftps(cert_file, key_file);
            server_builder = server_builder.ftps_required(FtpsRequired::All, FtpsRequired::All);
        }

        let server = server_builder.build()
            .map_err(|e| FtpsInitError::Server(e))?;

        // Start server in background task
        let server_handle = {
            let bind_addr = self.config.bind_addr;
            tokio::spawn(async move {
                server.listen(bind_addr.to_string()).await.map_err(FtpsInitError::Server)
            })
        };

        // Wait for either server completion or shutdown signal
        tokio::select! {
            result = server_handle => {
                match result {
                    Ok(Ok(())) => {
                        info!("FTPS server stopped normally");
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        error!("FTPS server error: {}", e);
                        Err(FtpsInitError::from(e))
                    }
                    Err(e) => {
                        error!("FTPS server task error: {}", e);
                        Err(FtpsInitError::Bind(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                info!("FTPS server shutdown requested");
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
///
/// MINIO CONSTRAINT: This authenticator MUST use the same authentication path
/// as external S3 clients and MUST NOT bypass IAM system.
#[derive(Debug, Default)]
pub struct FtpsAuthenticator;

impl FtpsAuthenticator {
    /// Create a new FTPS authenticator
    ///
    /// MINIO CONSTRAINT: Must use the same authentication path as external clients
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl libunftp::auth::Authenticator<FtpsUser> for FtpsAuthenticator {
    /// Authenticate FTP user
    ///
    /// MINIO CONSTRAINT: Must validate credentials through the same path as external clients
    async fn authenticate(&self, username: &str, creds: &libunftp::auth::Credentials) -> Result<FtpsUser, libunftp::auth::AuthenticationError> {
        use libunftp::auth::AuthenticationError;
        use rustfs_iam::get;
        use rustfs_policy::auth::Credentials as S3Credentials;

        tracing::trace!("FTPS authentication request for user: {}", username);

        // Get IAM system
        let iam_sys = get().map_err(|e| {
            error!("Failed to get IAM system: {}", e);
            AuthenticationError::ImplPropagated("IAM system not available".to_string(), Some(Box::new(e)))
        })?;

        // Create S3 credentials from FTP credentials
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

        // Validate credentials through IAM system
        let (user_identity, is_valid) = iam_sys.check_key(&s3_creds.access_key).await.map_err(|e| {
            error!("Failed to check key in IAM system: {}", e);
            AuthenticationError::ImplPropagated("Authentication failed".to_string(), Some(Box::new(e)))
        })?;

        if !is_valid {
            error!("Invalid credentials for user: {}", username);
            return Err(AuthenticationError::BadUser);
        }

        // Verify the secret key matches
        let identity = user_identity.ok_or_else(|| {
            error!("User identity not found for user: {}", username);
            AuthenticationError::BadUser
        })?;

        if !identity.credentials.secret_key.eq(&s3_creds.secret_key) {
            error!("Invalid secret key for user: {}", username);
            return Err(AuthenticationError::BadPassword);
        }

        // Create session context (for now using dummy IP, in real implementation this would come from the connection)
        let session_context = SessionContext::new(
            ProtocolPrincipal::new(Arc::new(identity.clone())),
            SessionProtocol::Ftps,
            "127.0.0.1".parse().unwrap(), // This should be the actual client IP
        );

        let ftps_user = FtpsUser {
            username: username.to_string(),
            name: identity.credentials.name.clone(),
            principal: Arc::new(identity),
            session_context,
        };

        tracing::debug!("Successfully authenticated user: {}", username);
        Ok(ftps_user)
    }
}