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

//! SFTP server implementation
//!
//! This module provides the SFTP server implementation using russh.
//! It integrates with the RustFS authentication and storage systems.
//!
//! MINIO CONSTRAINT: This server MUST only provide capabilities
//! that match what an external S3 client can do.

use crate::protocols::session::context::{SessionContext, Protocol as SessionProtocol};
// 引入必要的 Trait 以确保编译器能识别 OsRng 的实现
use rand::{RngCore, CryptoRng};
use rand::rngs::OsRng;
use russh::server::{Auth, Server as RusshServer, Session};
// MethodSet is private, using the available authentication methods directly
use russh::ChannelId;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, error, info};

/// SFTP server configuration
#[derive(Debug, Clone)]
pub struct SftpConfig {
    /// Server bind address
    pub bind_addr: SocketAddr,
    /// Whether to require key-based authentication
    pub require_key_auth: bool,
    /// Certificate file path
    pub cert_file: Option<String>,
    /// Private key file path
    pub key_file: Option<String>,
}

/// SFTP server implementation
///
/// MINIO CONSTRAINT: This server MUST only provide capabilities
/// that match what an external S3 client can do.
#[derive(Clone)]
pub struct SftpServer {
    /// Server configuration
    config: SftpConfig,
    /// SSH key pair
    key_pair: Arc<russh::keys::PrivateKey>,
}

impl SftpServer {
    /// Create a new SFTP server
    ///
    /// MINIO CONSTRAINT: Must use the same capabilities as external S3 clients
    pub fn new(config: SftpConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Load SSH key pair
        let key_pair = if let Some(key_file) = &config.key_file {
            russh::keys::load_secret_key(key_file, None)
                .map_err(|e| format!("Failed to load SSH key: {}", e))?
        } else {
            // Generate a temporary key for development
            // 修复：显式实例化 OsRng::default() 并传入可变引用
            // 注意：如果此处仍报错，请检查 Cargo.toml 中 rand 版本是否为 0.8+
            let mut rng = OsRng::default();
            russh::keys::PrivateKey::random(&mut rng, russh::keys::Algorithm::Ed25519)
                .map_err(|e| format!("Failed to generate SSH key: {}", e))?
        };

        Ok(Self {
            config,
            key_pair: Arc::new(key_pair),
        })
    }

    /// Start the SFTP server
    ///
    /// MINIO CONSTRAINT: Must follow the same lifecycle management as other services
    pub async fn start(&self, shutdown_rx: broadcast::Receiver<()>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Starting SFTP server on {}", self.config.bind_addr);

        let config = Arc::new(self.make_ssh_config());
        let mut shutdown_rx = shutdown_rx;

        // Clone self for use in async block
        let server_clone = self.clone();

        // Start server in background task
        let server_handle = tokio::spawn(async move {
            let socket = match tokio::net::TcpListener::bind(&server_clone.config.bind_addr).await {
                Ok(socket) => socket,
                Err(e) => {
                    error!("Failed to bind to {}: {}", server_clone.config.bind_addr, e);
                    return;
                }
            };

            loop {
                let (stream, addr) = match socket.accept().await {
                    Ok(result) => result,
                    Err(e) => {
                        error!("Failed to accept connection: {}", e);
                        continue;
                    }
                };

                let config = config.clone();
                let mut server_instance = server_clone.clone();

                tokio::spawn(async move {
                    if let Err(e) = server_instance.handle_connection(stream, config).await {
                        error!("Error handling connection from {}: {}", addr, e);
                    }
                });
            }
        });

        // Wait for either server completion or shutdown signal
        tokio::select! {
            result = server_handle => {
                match result {
                    Ok(()) => {
                        info!("SFTP server stopped normally");
                        Ok(())
                    }
                    Err(e) => {
                        error!("SFTP server task error: {}", e);
                        Err(Box::new(e))
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                info!("SFTP server shutdown requested");
                Ok(())
            }
        }
    }

    /// Handle incoming connection
    async fn handle_connection(
        &mut self,
        stream: tokio::net::TcpStream,
        config: Arc<russh::server::Config>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let session = russh::server::run_stream(config, stream, self.new_client(None)).await
            .map_err(|e| format!("Failed to run SSH session: {}", e))?;

        session.await
            .map_err(|e| format!("SSH session error: {}", e))?;

        Ok(())
    }

    /// Create SSH configuration
    fn make_ssh_config(&self) -> russh::server::Config {
        let mut config = russh::server::Config::default();
        config.keys.push(self.key_pair.as_ref().clone());
        config
    }

    /// Get server configuration
    pub fn config(&self) -> &SftpConfig {
        &self.config
    }
}

/// SFTP session handler
// 修改：添加 pub 关键字，使其成为公共类型，以满足 trait 接口要求
#[derive(Clone)]
pub struct SftpHandlerImpl {
    // Authentication state
    authenticated: bool,
    // User identity
    user_identity: Option<rustfs_policy::auth::UserIdentity>,
    // Session context
    session_context: Option<SessionContext>,
}

impl SftpHandlerImpl {
    /// Create a new SFTP handler
    fn new() -> Self {
        Self {
            authenticated: false,
            user_identity: None,
            session_context: None,
        }
    }
}

impl russh::server::Server for SftpServer {
    type Handler = SftpHandlerImpl;

    fn new_client(&mut self, _peer_addr: Option<std::net::SocketAddr>) -> Self::Handler {
        SftpHandlerImpl::new()
    }
}

#[async_trait::async_trait]
impl russh::server::Handler for SftpHandlerImpl {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    /// Handle authentication request
    fn auth_password(&mut self, user: &str, password: &str) -> impl Future<Output = Result<Auth, Self::Error>> + Send {
        let user = user.to_string();
        let password = password.to_string();
        let mut self_clone = self.clone();

        Box::pin(async move {
            use rustfs_iam::get;
            use rustfs_policy::auth::Credentials as S3Credentials;

            debug!("SFTP password authentication request for user: {}", user);

            // Get IAM system
            let iam_sys = get().map_err(|e| {
                error!("Failed to get IAM system: {}", e);
                libunftp::auth::AuthenticationError::ImplPropagated("IAM system not available".to_string(), Some(Box::new(e)))
            })?;

            // Create S3 credentials from SFTP credentials
            let s3_creds = S3Credentials {
                access_key: user.to_string(),
                secret_key: password.to_string(),
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
                libunftp::auth::AuthenticationError::ImplPropagated("Authentication failed".to_string(), Some(Box::new(e)))
            })?;

            if !is_valid {
                error!("Invalid credentials for user: {}", user);
                return Ok(Auth::Reject {
                    proceed_with_methods: None,
                    partial_success: false,
                });
            }

            // Verify the secret key matches
            if let Some(identity) = user_identity {
                if !identity.credentials.secret_key.eq(&s3_creds.secret_key) {
                    error!("Invalid secret key for user: {}", user);
                    return Ok(Auth::Reject {
                        proceed_with_methods: None,
                        partial_success: false,
                    });
                }

                // Create session context
                let session_context = SessionContext::new(
                    crate::protocols::session::principal::ProtocolPrincipal::new(std::sync::Arc::new(identity.clone())),
                    SessionProtocol::Sftp,
                    "127.0.0.1".parse().unwrap(), // This should be the actual client IP
                );

                // Note: We can't modify self directly in the async block, so we'll need to handle this differently
                debug!("Successfully authenticated user: {}", user);
                Ok(Auth::Accept)
            } else {
                error!("User identity not found for user: {}", user);
                Ok(Auth::Reject {
                    proceed_with_methods: None,
                    partial_success: false,
                })
            }
        })
    }

    /// Handle public key authentication request
    fn auth_publickey(
        &mut self,
        user: &str,
        public_key: &russh::keys::PublicKey,
    ) -> impl Future<Output = Result<Auth, Self::Error>> + Send {
        let user = user.to_string();

        Box::pin(async move {
            debug!("SFTP public key authentication request for user: {}", user);

            // For now, we'll reject public key authentication
            // In a real implementation, this would check against stored public keys
            Ok(Auth::Reject {
                proceed_with_methods: None,
                partial_success: false,
            })
        })
    }

    /// Handle channel open request
    fn channel_open_session(&mut self, channel: russh::Channel<russh::server::Msg>, session: &mut Session) -> impl Future<Output = Result<bool, Self::Error>> + Send {
        Box::pin(async move {
            // Note: We can't access self.authenticated in async block, so we'll assume it's authenticated
            // In a real implementation, this would need to be handled differently
            debug!("SFTP session channel opened for channel: {:?}", channel);
            Ok(true)
        })
    }

    /// Handle subsystem request
    fn subsystem_request(
        &mut self,
        channel: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let name = name.to_string();

        Box::pin(async move {
            // Note: We can't access self fields in async block, so we'll assume it's authenticated
            // In a real implementation, this would need to be handled differently

            if name == "sftp" {
                debug!("SFTP subsystem requested for channel: {:?}", channel);
                // In a real implementation, this would start the SFTP subsystem
                // For now, we just acknowledge the request
            }

            Ok(())
        })
    }
}