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

use crate::protocols::sftp::handler::SftpHandler;
use crate::protocols::session::context::{SessionContext, Protocol as SessionProtocol};
use crate::protocols::session::principal::ProtocolPrincipal;
use russh::{ChannelId};
use russh::server::{Auth, Handler, Server as RusshServer, Session};
use russh::keys::PrivateKey;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

// Define a common error type for the server handler
type ServerError = Box<dyn std::error::Error + Send + Sync>;

/// SFTP server configuration
#[derive(Debug, Clone)]
pub struct SftpConfig {
    pub bind_addr: SocketAddr,
    pub require_key_auth: bool,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
}

/// The main Server entry point
#[derive(Clone)]
pub struct SftpServer {
    config: SftpConfig,
    key_pair: Arc<PrivateKey>,
}

impl SftpServer {
    pub fn new(config: SftpConfig) -> Result<Self, ServerError> {
        // Load or generate key
        let key_pair = if let Some(key_file) = &config.key_file {
            let path = std::path::Path::new(key_file);
            russh::keys::load_secret_key(path, None)?
        } else {
            warn!("No host key provided for SFTP, generating random key.");
            let mut rng = rand::rngs::OsRng;
            PrivateKey::random(&mut rng, russh::keys::Algorithm::Ed25519)?
        };

        Ok(Self {
            config,
            key_pair: Arc::new(key_pair),
        })
    }

    pub async fn start(&self, mut shutdown_rx: tokio::sync::broadcast::Receiver<()>) -> Result<(), ServerError> {
        info!("Starting SFTP server on {}", self.config.bind_addr);

        let config = Arc::new(self.make_ssh_config());
        let socket = tokio::net::TcpListener::bind(&self.config.bind_addr).await?;
        let server_stub = self.clone();

        loop {
            tokio::select! {
                accept_res = socket.accept() => {
                    match accept_res {
                        Ok((stream, addr)) => {
                            let config = config.clone();
                            tokio::spawn(async move {
                                let handler = SftpConnectionHandler::new(addr);
                                if let Err(e) = russh::server::run_stream(config, stream, handler).await {
                                    debug!("SFTP session closed from {}: {}", addr, e);
                                }
                            });
                        }
                        Err(e) => error!("Failed to accept SFTP connection: {}", e),
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("SFTP server shutting down");
                    break;
                }
            }
        }
        Ok(())
    }

    fn make_ssh_config(&self) -> russh::server::Config {
        let mut config = russh::server::Config::default();
        config.keys.push(self.key_pair.as_ref().clone());
        // config.connection_timeout removed as it's not a direct field in recent russh Config
        // or requires specific Duration type handling not standard in Config struct init.
        // Default keep-alive settings are usually sufficient.
        config
    }

    pub fn config(&self) -> &SftpConfig {
        &self.config
    }
}

// Implement the factory trait for russh
impl RusshServer for SftpServer {
    type Handler = SftpConnectionHandler;

    fn new_client(&mut self, peer_addr: Option<SocketAddr>) -> Self::Handler {
        let addr = peer_addr.unwrap_or_else(|| "0.0.0.0:0".parse().unwrap());
        SftpConnectionHandler::new(addr)
    }
}

/// Shared state for a single SSH connection
struct ConnectionState {
    client_ip: SocketAddr,
    identity: Option<rustfs_policy::auth::UserIdentity>,
    /// Map ChannelId to a sender. When we receive SSH data, we send it into this channel,
    /// which pipes it to the SFTP subsystem.
    sftp_channels: HashMap<ChannelId, mpsc::UnboundedSender<Vec<u8>>>,
}

/// The Handler for a single SSH connection
#[derive(Clone)]
pub struct SftpConnectionHandler {
    state: Arc<Mutex<ConnectionState>>,
}

impl SftpConnectionHandler {
    fn new(client_ip: SocketAddr) -> Self {
        Self {
            state: Arc::new(Mutex::new(ConnectionState {
                client_ip,
                identity: None,
                sftp_channels: HashMap::new(),
            })),
        }
    }
}

impl Handler for SftpConnectionHandler {
    type Error = ServerError;

    fn auth_password(&mut self, user: &str, password: &str) -> impl Future<Output = Result<Auth, Self::Error>> + Send {
        let user = user.to_string();
        let password = password.to_string();
        let state = self.state.clone();

        async move {
            use rustfs_iam::get;
            use rustfs_policy::auth::Credentials as S3Credentials;

            debug!("SFTP password auth request: {}", user);

            let iam_sys = get().map_err(|e| format!("IAM system unavailable: {}", e))?;

            // Prepare S3 Credentials check
            let s3_creds = S3Credentials {
                access_key: user.clone(),
                secret_key: password.clone(),
                // Fill defaults
                session_token: String::new(), expiration: None, status: String::new(),
                parent_user: String::new(), groups: None, claims: None, name: None, description: None,
            };

            let (user_identity, is_valid) = iam_sys.check_key(&s3_creds.access_key).await
                .map_err(|e| format!("IAM check failed: {}", e))?;

            if !is_valid {
                warn!("Invalid AccessKey: {}", user);
                return Ok(Auth::Reject { proceed_with_methods: None, partial_success: false });
            }

            if let Some(identity) = user_identity {
                if identity.credentials.secret_key != s3_creds.secret_key {
                    warn!("Invalid SecretKey for user: {}", user);
                    return Ok(Auth::Reject { proceed_with_methods: None, partial_success: false });
                }

                // SUCCESS: Update shared state
                {
                    let mut guard = state.lock().unwrap();
                    guard.identity = Some(identity);
                }
                debug!("User {} authenticated successfully via Password", user);
                Ok(Auth::Accept)
            } else {
                Ok(Auth::Reject { proceed_with_methods: None, partial_success: false })
            }
        }
    }

    fn auth_publickey(&mut self, user: &str, _key: &russh::keys::PublicKey) -> impl Future<Output = Result<Auth, Self::Error>> + Send {
        let user = user.to_string();
        async move {
            debug!("SFTP public key auth not yet implemented for user: {}", user);
            Ok(Auth::Reject { proceed_with_methods: None, partial_success: false })
        }
    }

    fn channel_open_session(&mut self, _channel: russh::Channel<russh::server::Msg>, _session: &mut Session) -> impl Future<Output = Result<bool, Self::Error>> + Send {
        async move { Ok(true) }
    }

    /// VITAL: This maps the SSH subsystem request "sftp" to the actual russh-sftp handler
    fn subsystem_request(&mut self, channel_id: ChannelId, name: &str, session: &mut Session) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let name = name.to_string();
        let state = self.state.clone();
        let session_handle = session.handle();

        async move {
            if name == "sftp" {
                let (identity, client_ip) = {
                    let guard = state.lock().unwrap();
                    if let Some(id) = &guard.identity {
                        (id.clone(), guard.client_ip)
                    } else {
                        error!("SFTP subsystem requested but user not authenticated");
                        return Ok(());
                    }
                };

                debug!("Initializing SFTP subsystem for user: {} (Channel: {:?})", identity.credentials.access_key, channel_id);

                // Create SessionContext
                let context = SessionContext::new(
                    ProtocolPrincipal::new(Arc::new(identity)),
                    SessionProtocol::Sftp,
                    client_ip.ip(),
                );

                // Create a Duplex Stream to bridge SSH and SFTP
                // russh (client_pipe) <==> russh-sftp (server_pipe)
                let (client_pipe, server_pipe) = tokio::io::duplex(65536);
                let (mut client_read, mut client_write) = tokio::io::split(client_pipe);

                // 1. Setup channel to feed data FROM ssh TO sftp
                let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
                {
                    let mut guard = state.lock().unwrap();
                    guard.sftp_channels.insert(channel_id, tx);
                }

                // Task: Pump data from mpsc -> SFTP input
                tokio::spawn(async move {
                    use tokio::io::AsyncWriteExt;
                    while let Some(data) = rx.recv().await {
                        if let Err(e) = client_write.write_all(&data).await {
                            error!("Failed to write to SFTP pipe: {}", e);
                            break;
                        }
                    }
                });

                // 2. Run the SFTP Protocol Server
                let sftp_handler = SftpHandler::new(context);
                tokio::spawn(async move {
                    // 修复：russh_sftp::server::run 返回 ()，而不是 Result。
                    // 它会一直运行直到流结束（客户端断开连接）。
                    russh_sftp::server::run(server_pipe, sftp_handler).await;

                    // 当 run 完成时，意味着会话结束
                    debug!("SFTP protocol loop ended (connection closed)");
                });

                // 3. Task: Pump data FROM SFTP output TO ssh client
                let session_handle = session_handle.clone();
                tokio::spawn(async move {
                    use tokio::io::AsyncReadExt;
                    let mut buf = vec![0u8; 32 * 1024];
                    loop {
                        match client_read.read(&mut buf).await {
                            Ok(0) => break, // EOF
                            Ok(n) => {
                                // Send back to SSH client
                                // FIX: Convert Vec<u8> to CryptoVec via .into()
                                let data: Vec<u8> = buf[..n].to_vec();
                                if let Err(_) = session_handle.data(channel_id, data.into()).await {
                                    break;
                                }
                            }
                            Err(e) => {
                                error!("Error reading from SFTP output pipe: {}", e);
                                break;
                            }
                        }
                    }
                    let _ = session_handle.close(channel_id).await;
                });
            }
            Ok(())
        }
    }

    /// Handle incoming data from SSH client
    fn data(&mut self, channel_id: ChannelId, data: &[u8], _session: &mut Session) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let state = self.state.clone();
        let data = data.to_vec();

        async move {
            let sender = {
                let guard = state.lock().unwrap();
                guard.sftp_channels.get(&channel_id).cloned()
            };

            if let Some(tx) = sender {
                // Send data to the pumping task
                if let Err(_) = tx.send(data) {
                    // Channel closed, ignore
                }
            }
            Ok(())
        }
    }
}