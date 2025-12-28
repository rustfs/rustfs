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
use russh::ChannelId;
use russh::server::{Auth, Handler, Server as RusshServer, Session};
use russh::keys::{PrivateKey, PublicKey, Algorithm, HashAlg, PublicKeyBase64};
use ssh_key::Certificate;
use ssh_key::certificate::CertType;
use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
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
    trusted_certificates: Arc<Vec<ssh_key::PublicKey>>,
}

impl SftpServer {
    pub fn new(config: SftpConfig) -> Result<Self, ServerError> {
        // Load SSH host key
        let key_pair = if let Some(key_file) = &config.key_file {
            let path = Path::new(key_file);
            russh::keys::load_secret_key(path, None)?
        } else {
            warn!("No host key provided for SFTP, generating random key (NOT RECOMMENDED for production).");
            let mut rng = rand::rngs::OsRng;
            PrivateKey::random(&mut rng, Algorithm::Ed25519)?
        };

        // Load trusted CA certificates
        let trusted_certificates = if let Some(cert_file) = &config.cert_file {
            info!("Loading trusted CA certificates from: {}", cert_file);
            load_trusted_certificates(cert_file)?
        } else {
            if config.require_key_auth {
                warn!("Key auth required but no CA certs provided!");
            }
            Vec::new()
        };

        Ok(Self {
            config,
            key_pair: Arc::new(key_pair),
            trusted_certificates: Arc::new(trusted_certificates),
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
                            let server_instance = server_stub.clone();
                            tokio::spawn(async move {
                                let handler = SftpConnectionHandler::new(addr, server_instance.trusted_certificates.clone());
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

        // Explicitly set preferred algorithms
        config.preferred.key = Cow::Borrowed(&[
            Algorithm::Ed25519,
            Algorithm::Rsa { hash: None },
            Algorithm::Rsa { hash: Some(HashAlg::Sha256) },
            Algorithm::Rsa { hash: Some(HashAlg::Sha512) },
        ]);

        config
    }

    pub fn config(&self) -> &SftpConfig {
        &self.config
    }
}

impl RusshServer for SftpServer {
    type Handler = SftpConnectionHandler;

    fn new_client(&mut self, peer_addr: Option<SocketAddr>) -> Self::Handler {
        let addr = peer_addr.unwrap_or_else(|| "0.0.0.0:0".parse().unwrap());
        SftpConnectionHandler::new(addr, self.trusted_certificates.clone())
    }
}

// ============================================================================
// 2. Connection Handler & State
// ============================================================================

/// Shared state for a single SSH connection
struct ConnectionState {
    client_ip: SocketAddr,
    identity: Option<rustfs_policy::auth::UserIdentity>,
    trusted_certificates: Arc<Vec<ssh_key::PublicKey>>,
    sftp_channels: HashMap<ChannelId, mpsc::UnboundedSender<Vec<u8>>>,
}

/// The Handler for a single SSH connection
#[derive(Clone)]
pub struct SftpConnectionHandler {
    state: Arc<Mutex<ConnectionState>>,
}

impl SftpConnectionHandler {
    fn new(client_ip: SocketAddr, trusted_certificates: Arc<Vec<ssh_key::PublicKey>>) -> Self {
        Self {
            state: Arc::new(Mutex::new(ConnectionState {
                client_ip,
                identity: None,
                trusted_certificates,
                sftp_channels: HashMap::new(),
            })),
        }
    }
}

impl Handler for SftpConnectionHandler {
    type Error = ServerError;

    fn auth_password(&mut self, user: &str, password: &str) -> impl Future<Output = Result<Auth, Self::Error>> + Send {
        let raw_user = user.to_string();
        let password = password.to_string();
        let state = self.state.clone();

        async move {
            use rustfs_iam::get;
            use rustfs_policy::auth::Credentials as S3Credentials;

            debug!("SFTP password auth request: {}", raw_user);

            let (username, suffix) = parse_auth_username(&raw_user);
            if let Some(s) = suffix {
                debug!("Detected auth suffix '{}' for user '{}'", s, username);
            }

            let iam_sys = get().map_err(|e| format!("IAM system unavailable: {}", e))?;

            let s3_creds = S3Credentials {
                access_key: username.to_string(),
                secret_key: password.clone(),
                session_token: String::new(), expiration: None, status: String::new(),
                parent_user: String::new(), groups: None, claims: None, name: None, description: None,
            };

            let (user_identity, is_valid) = iam_sys.check_key(&s3_creds.access_key).await
                .map_err(|e| format!("IAM check failed: {}", e))?;

            if !is_valid {
                warn!("Invalid AccessKey: {}", username);
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                return Ok(Auth::Reject { proceed_with_methods: None, partial_success: false });
            }

            if let Some(identity) = user_identity {
                if identity.credentials.secret_key != s3_creds.secret_key {
                    warn!("Invalid SecretKey for user: {}", username);
                    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                    return Ok(Auth::Reject { proceed_with_methods: None, partial_success: false });
                }

                {
                    let mut guard = state.lock().unwrap();
                    guard.identity = Some(identity);
                }
                debug!("User {} authenticated successfully via Password", username);
                Ok(Auth::Accept)
            } else {
                Ok(Auth::Reject { proceed_with_methods: None, partial_success: false })
            }
        }
    }

    fn auth_publickey(&mut self, user: &str, key: &PublicKey) -> impl Future<Output = Result<Auth, Self::Error>> + Send {
        let raw_user = user.to_string();
        // Clone the key (which is russh::keys::PublicKey)
        let key = key.clone();
        let state = self.state.clone();

        async move {
            debug!("SFTP public key auth request for user: {}", raw_user);

            let trusted_cas = {
                let guard = state.lock().unwrap();
                guard.trusted_certificates.clone()
            };

            if !trusted_cas.is_empty() {
                match validate_ssh_certificate(&key, &trusted_cas, &raw_user) {
                    Ok(true) => {
                        let (username, _) = parse_auth_username(&raw_user);

                        use rustfs_iam::get;
                        let iam_sys = get().map_err(|e| format!("IAM system unavailable: {}", e))?;

                        // Check if user exists (skip pwd check since cert is trusted)
                        let (user_identity, is_valid) = iam_sys.check_key(username).await
                            .map_err(|e| format!("IAM lookup error: {}", e))?;

                        if is_valid && user_identity.is_some() {
                            {
                                let mut guard = state.lock().unwrap();
                                guard.identity = user_identity;
                            }
                            info!("User {} authenticated via SSH Certificate", username);
                            Ok(Auth::Accept)
                        } else {
                            warn!("Valid certificate presented, but user '{}' does not exist in IAM", username);
                            Ok(Auth::Reject { proceed_with_methods: None, partial_success: false })
                        }
                    }
                    Ok(false) => {
                        Ok(Auth::Reject { proceed_with_methods: None, partial_success: false })
                    }
                    Err(e) => {
                        error!("SSH certificate validation error: {}", e);
                        Ok(Auth::Reject { proceed_with_methods: None, partial_success: false })
                    }
                }
            } else {
                debug!("No trusted CA certificates configured, denying public key auth");
                Ok(Auth::Reject { proceed_with_methods: None, partial_success: false })
            }
        }
    }

    fn channel_open_session(&mut self, _channel: russh::Channel<russh::server::Msg>, _session: &mut Session) -> impl Future<Output = Result<bool, Self::Error>> + Send {
        async move { Ok(true) }
    }

    fn data(&mut self, channel_id: ChannelId, data: &[u8], _session: &mut Session) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let state = self.state.clone();
        let data = data.to_vec();

        async move {
            let sender = {
                let guard = state.lock().unwrap();
                guard.sftp_channels.get(&channel_id).cloned()
            };

            if let Some(tx) = sender {
                if let Err(_) = tx.send(data) {
                    // Receiver closed
                }
            }
            Ok(())
        }
    }

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

                debug!("Initializing SFTP subsystem for user: {}", identity.credentials.access_key);

                let context = SessionContext::new(
                    ProtocolPrincipal::new(Arc::new(identity)),
                    SessionProtocol::Sftp,
                    client_ip.ip(),
                );

                let (client_pipe, server_pipe) = tokio::io::duplex(65536);
                let (mut client_read, mut client_write) = tokio::io::split(client_pipe);

                let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
                {
                    let mut guard = state.lock().unwrap();
                    guard.sftp_channels.insert(channel_id, tx);
                }

                tokio::spawn(async move {
                    use tokio::io::AsyncWriteExt;
                    while let Some(data) = rx.recv().await {
                        if let Err(e) = client_write.write_all(&data).await {
                            debug!("SFTP input pipe closed: {}", e);
                            break;
                        }
                    }
                });

                let sftp_handler = SftpHandler::new(context);
                tokio::spawn(async move {
                    russh_sftp::server::run(server_pipe, sftp_handler).await;
                    debug!("SFTP handler finished");
                });

                let session_handle = session_handle.clone();
                tokio::spawn(async move {
                    use tokio::io::AsyncReadExt;
                    let mut buf = vec![0u8; 32 * 1024];
                    loop {
                        match client_read.read(&mut buf).await {
                            Ok(0) => break, // EOF
                            Ok(n) => {
                                let data: Vec<u8> = buf[..n].to_vec();
                                if let Err(_) = session_handle.data(channel_id, data.into()).await {
                                    break;
                                }
                            }
                            Err(e) => {
                                error!("Error reading from SFTP output: {}", e);
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
}

// ============================================================================
// 3. Helpers & Validation Logic
// ============================================================================

/// Helper to load trusted CA certificates
fn load_trusted_certificates(ca_cert_path: &str) -> Result<Vec<ssh_key::PublicKey>, ServerError> {
    let path = Path::new(ca_cert_path);
    if !path.exists() {
        return Err(format!("CA certificate file not found: {}", ca_cert_path).into());
    }

    let contents = std::fs::read_to_string(path)?;
    let mut keys = Vec::new();

    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        match ssh_key::PublicKey::from_openssh(line) {
            Ok(key) => keys.push(key),
            Err(e) => warn!("Skipping invalid CA key line in {}: {}", ca_cert_path, e),
        }
    }

    info!("Loaded {} trusted CA certificates from {}", keys.len(), ca_cert_path);
    Ok(keys)
}

fn parse_auth_username(username: &str) -> (&str, Option<&str>) {
    if let Some(idx) = username.rfind('=') {
        let suffix = &username[idx..];
        if suffix == "=svc" || suffix == "=ldap" {
            return (&username[..idx], Some(suffix));
        }
    }
    (username, None)
}

/// Validates an SSH certificate using ssh-key crate
fn validate_ssh_certificate(
    russh_key: &PublicKey,
    trusted_cas: &[ssh_key::PublicKey],
    raw_username: &str,
) -> Result<bool, ServerError> {
    let (username, _suffix) = parse_auth_username(raw_username);

    // 1. Get raw bytes from russh key (Fix: use public_key_bytes() instead of Encode trait)
    let key_bytes = russh_key.public_key_bytes();

    // 2. Parse using ssh-key crate
    let cert = match Certificate::from_bytes(&key_bytes) {
        Ok(c) => c,
        Err(_) => {
            debug!("Provided key is not a certificate. Skipping cert validation.");
            return Ok(false);
        }
    };

    debug!("Verifying SSH Certificate: KeyID='{}', Serial={}", cert.comment(), cert.serial());

    // 3. Find the signing CA
    let mut signature_valid = false;
    let signature_key = cert.signature_key(); // This gets the Signing CA's public key from the cert

    // Check if the certificate's signer is in our trusted CA list
    for ca in trusted_cas {
        if ca.key_data() == signature_key  {
            signature_valid = true;
            debug!("Certificate signed by trusted CA: {}", ca.fingerprint(Default::default()));
            break;
        }
    }

    if !signature_valid {
        warn!("Certificate signer not found in trusted CAs");
        return Ok(false);
    }

    // 4. Validate Time
    let now = SystemTime::now();
    let valid_after = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(cert.valid_after());
    let valid_before = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(cert.valid_before());

    if now < valid_after {
        warn!("Certificate is not yet valid (valid after {:?})", valid_after);
        return Ok(false);
    }
    if now > valid_before {
        warn!("Certificate has expired (valid until {:?})", valid_before);
        return Ok(false);
    }

    // 5. Validate Principal
    if !cert.valid_principals().contains(&username.to_string()) {
        warn!("Certificate does not authorize user '{}'. Principals: {:?}", username, cert.valid_principals());
        return Ok(false);
    }

    // 6. Validate Key Type
    match cert.cert_type() {
        CertType::User => {},
        _ => {
            warn!("Certificate is not a User certificate");
            return Ok(false);
        }
    }

    // 7. Validate Critical Options
    for (name, _value) in cert.critical_options().iter() {
        if name.as_str() == "source-address" {
            // TODO: Add source IP check here if needed
        } else {
            warn!("Rejecting certificate due to unsupported critical option: {}", name);
            return Ok(false);
        }
    }

    info!("SSH Certificate validation successful for user '{}'", username);
    Ok(true)
}