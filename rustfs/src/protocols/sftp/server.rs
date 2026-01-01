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

use crate::protocols::session::context::{Protocol as SessionProtocol, SessionContext};
use crate::protocols::session::principal::ProtocolPrincipal;
use crate::protocols::sftp::handler::SftpHandler;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use russh::ChannelId;
use russh::keys::{Algorithm, HashAlg, PrivateKey, PublicKey, PublicKeyBase64};
use russh::server::{Auth, Handler, Server as RusshServer, Session};
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

const DEFAULT_ADDR: &str = "0.0.0.0:0";
const AUTH_SUFFIX_SVC: &str = "=svc";
const AUTH_SUFFIX_LDAP: &str = "=ldap";
const SSH_KEY_TYPE_RSA: &str = "ssh-rsa";
const SSH_KEY_TYPE_ED25519: &str = "ssh-ed25519";
const SSH_KEY_TYPE_ECDSA: &str = "ecdsa-";
const SFTP_SUBSYSTEM: &str = "sftp";
const CRITICAL_OPTION_SOURCE_ADDRESS: &str = "source-address";
const AUTH_FAILURE_DELAY_MS: u64 = 300;
const SFTP_BUFFER_SIZE: usize = 65536;
const SFTP_READ_BUF_SIZE: usize = 32 * 1024;

type ServerError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Clone)]
pub struct SftpConfig {
    pub bind_addr: SocketAddr,
    pub require_key_auth: bool,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub authorized_keys_file: Option<String>,
}

#[derive(Clone)]
pub struct SftpServer {
    config: SftpConfig,
    key_pair: Arc<PrivateKey>,
    trusted_certificates: Arc<Vec<ssh_key::PublicKey>>,
    authorized_keys: Arc<Vec<String>>,
}

impl SftpServer {
    pub fn new(config: SftpConfig) -> Result<Self, ServerError> {
        let key_pair = if let Some(key_file) = &config.key_file {
            let path = Path::new(key_file);
            russh::keys::load_secret_key(path, None)?
        } else {
            warn!("No host key provided, generating random key (not recommended for production).");
            use russh::keys::signature::rand_core::OsRng;
            let mut rng = OsRng;
            PrivateKey::random(&mut rng, Algorithm::Ed25519)?
        };

        let trusted_certificates = if let Some(cert_file) = &config.cert_file {
            info!("Loading trusted CA certificates from: {}", cert_file);
            load_trusted_certificates(cert_file)?
        } else {
            if config.require_key_auth {
                warn!("Key auth required but no CA certs provided.");
            }
            Vec::new()
        };

        let authorized_keys = if let Some(auth_keys_file) = &config.authorized_keys_file {
            info!("Loading authorized SSH public keys from: {}", auth_keys_file);
            load_authorized_keys(auth_keys_file).unwrap_or_else(|e| {
                error!("Failed to load authorized keys from {}: {}", auth_keys_file, e);
                Vec::new()
            })
        } else {
            info!("No authorized keys file provided, will use IAM for key validation.");
            Vec::new()
        };

        info!("Loaded {} authorized SSH public key(s)", authorized_keys.len());

        Ok(Self {
            config,
            key_pair: Arc::new(key_pair),
            trusted_certificates: Arc::new(trusted_certificates),
            authorized_keys: Arc::new(authorized_keys),
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
                                let handler = SftpConnectionHandler::new(addr, server_instance.trusted_certificates.clone(), server_instance.authorized_keys.clone());
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

        config.preferred.key = Cow::Borrowed(&[
            Algorithm::Ed25519,
            Algorithm::Rsa { hash: None },
            Algorithm::Rsa {
                hash: Some(HashAlg::Sha256),
            },
            Algorithm::Rsa {
                hash: Some(HashAlg::Sha512),
            },
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
        let addr = peer_addr.unwrap_or_else(|| DEFAULT_ADDR.parse().unwrap());
        SftpConnectionHandler::new(addr, self.trusted_certificates.clone(), self.authorized_keys.clone())
    }
}

struct ConnectionState {
    client_ip: SocketAddr,
    identity: Option<rustfs_policy::auth::UserIdentity>,
    trusted_certificates: Arc<Vec<ssh_key::PublicKey>>,
    authorized_keys: Arc<Vec<String>>,
    sftp_channels: HashMap<ChannelId, mpsc::UnboundedSender<Vec<u8>>>,
}

#[derive(Clone)]
pub struct SftpConnectionHandler {
    state: Arc<Mutex<ConnectionState>>,
}

impl SftpConnectionHandler {
    fn new(client_ip: SocketAddr, trusted_certificates: Arc<Vec<ssh_key::PublicKey>>, authorized_keys: Arc<Vec<String>>) -> Self {
        Self {
            state: Arc::new(Mutex::new(ConnectionState {
                client_ip,
                identity: None,
                trusted_certificates,
                authorized_keys,
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
            use rustfs_credentials::Credentials as S3Credentials;
            use rustfs_iam::get;

            let (username, suffix) = parse_auth_username(&raw_user);
            if let Some(s) = suffix {
                debug!("Detected auth suffix '{}' for user '{}'", s, username);
            }

            let iam_sys = get().map_err(|e| format!("IAM system unavailable: {}", e))?;

            let s3_creds = S3Credentials {
                access_key: username.to_string(),
                secret_key: password.clone(),
                session_token: String::new(),
                expiration: None,
                status: String::new(),
                parent_user: String::new(),
                groups: None,
                claims: None,
                name: None,
                description: None,
            };

            let (user_identity, is_valid) = iam_sys
                .check_key(&s3_creds.access_key)
                .await
                .map_err(|e| format!("IAM check failed: {}", e))?;

            if !is_valid {
                warn!("Invalid AccessKey: {}", username);
                tokio::time::sleep(std::time::Duration::from_millis(AUTH_FAILURE_DELAY_MS)).await;
                return Ok(Auth::Reject {
                    proceed_with_methods: None,
                    partial_success: false,
                });
            }

            if let Some(identity) = user_identity {
                if identity.credentials.secret_key != s3_creds.secret_key {
                    warn!("Invalid SecretKey for user: {}", username);
                    tokio::time::sleep(std::time::Duration::from_millis(AUTH_FAILURE_DELAY_MS)).await;
                    return Ok(Auth::Reject {
                        proceed_with_methods: None,
                        partial_success: false,
                    });
                }

                {
                    let mut guard = state.lock().unwrap();
                    guard.identity = Some(identity);
                }
                debug!("User {} authenticated successfully via password", username);
                Ok(Auth::Accept)
            } else {
                Ok(Auth::Reject {
                    proceed_with_methods: None,
                    partial_success: false,
                })
            }
        }
    }

    fn auth_publickey(&mut self, user: &str, key: &PublicKey) -> impl Future<Output = Result<Auth, Self::Error>> + Send {
        let raw_user = user.to_string();
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

                        let (user_identity, is_valid) = iam_sys
                            .check_key(username)
                            .await
                            .map_err(|e| format!("IAM lookup error: {}", e))?;

                        if is_valid && user_identity.is_some() {
                            {
                                let mut guard = state.lock().unwrap();
                                guard.identity = user_identity;
                            }
                            info!("User {} authenticated via SSH certificate", username);
                            Ok(Auth::Accept)
                        } else {
                            warn!("Valid certificate presented, but user '{}' does not exist in IAM", username);
                            Ok(Auth::Reject {
                                proceed_with_methods: None,
                                partial_success: false,
                            })
                        }
                    }
                    Ok(false) => Ok(Auth::Reject {
                        proceed_with_methods: None,
                        partial_success: false,
                    }),
                    Err(e) => {
                        error!("SSH certificate validation error: {}", e);
                        Ok(Auth::Reject {
                            proceed_with_methods: None,
                            partial_success: false,
                        })
                    }
                }
            } else {
                let (username, _) = parse_auth_username(&raw_user);

                use russh::keys::PublicKeyBase64;

                let client_key_bytes = key.public_key_bytes();
                let client_key_openssh = BASE64.encode(&client_key_bytes);

                let authorized_keys_clone = {
                    let guard = state.lock().unwrap();
                    guard.authorized_keys.clone()
                };

                if !authorized_keys_clone.is_empty() {
                    debug!("Checking against {} pre-loaded authorized key(s)", authorized_keys_clone.len());

                    for authorized_key in authorized_keys_clone.iter() {
                        if authorized_key.contains(&client_key_openssh)
                            || authorized_key == &client_key_openssh
                            || compare_keys(authorized_key, &client_key_openssh)
                        {
                            use rustfs_iam::get;
                            if let Ok(iam_sys) = get() {
                                match iam_sys.check_key(username).await {
                                    Ok((user_identity, is_valid)) => {
                                        if is_valid && user_identity.is_some() {
                                            let mut guard = state.lock().unwrap();
                                            guard.identity = user_identity;
                                            info!("User {} authenticated via pre-loaded authorized key (IAM verified)", username);
                                            return Ok(Auth::Accept);
                                        }
                                    }
                                    Err(e) => {
                                        error!("IAM lookup error: {}", e);
                                    }
                                }
                            }
                            warn!(
                                "Key matched pre-loaded authorized keys, but IAM verification failed for user '{}'",
                                username
                            );
                        }
                    }
                }

                use rustfs_iam::get;
                match get() {
                    Ok(iam_sys) => match iam_sys.check_key(username).await {
                        Ok((user_identity, is_valid)) => {
                            if is_valid {
                                if let Some(identity) = user_identity {
                                    let authorized_keys = identity.get_ssh_public_keys();

                                    if authorized_keys.is_empty() {
                                        warn!("User '{}' found in IAM but has no SSH public keys registered", username);
                                        return Ok(Auth::Reject {
                                            proceed_with_methods: None,
                                            partial_success: false,
                                        });
                                    }

                                    let key_valid = authorized_keys.iter().any(|authorized_key| {
                                        authorized_key.contains(&client_key_openssh)
                                            || authorized_key == &client_key_openssh
                                            || compare_keys(authorized_key, &client_key_openssh)
                                    });

                                    if key_valid {
                                        {
                                            let mut guard = state.lock().unwrap();
                                            guard.identity = Some(identity);
                                        }
                                        info!("User {} authenticated via public key from IAM", username);
                                        Ok(Auth::Accept)
                                    } else {
                                        warn!("Public key auth failed: client key not in IAM for user '{}'", username);
                                        Ok(Auth::Reject {
                                            proceed_with_methods: None,
                                            partial_success: false,
                                        })
                                    }
                                } else {
                                    warn!("Public key auth failed: user '{}' not found in IAM", username);
                                    Ok(Auth::Reject {
                                        proceed_with_methods: None,
                                        partial_success: false,
                                    })
                                }
                            } else {
                                warn!("Public key auth failed: user '{}' not valid in IAM", username);
                                Ok(Auth::Reject {
                                    proceed_with_methods: None,
                                    partial_success: false,
                                })
                            }
                        }
                        Err(e) => {
                            error!("IAM lookup error: {}", e);
                            Ok(Auth::Reject {
                                proceed_with_methods: None,
                                partial_success: false,
                            })
                        }
                    },
                    Err(e) => {
                        error!("IAM system unavailable: {}", e);
                        Ok(Auth::Reject {
                            proceed_with_methods: None,
                            partial_success: false,
                        })
                    }
                }
            }
        }
    }

    async fn channel_open_session(
        &mut self,
        _channel: russh::Channel<russh::server::Msg>,
        _session: &mut Session,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }

    fn data(
        &mut self,
        channel_id: ChannelId,
        data: &[u8],
        _session: &mut Session,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let state = self.state.clone();
        let data = data.to_vec();

        async move {
            let sender = {
                let guard = state.lock().unwrap();
                guard.sftp_channels.get(&channel_id).cloned()
            };

            if let Some(tx) = sender {
                let _ = tx.send(data);
            }
            Ok(())
        }
    }

    fn subsystem_request(
        &mut self,
        channel_id: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let name = name.to_string();
        let state = self.state.clone();
        let session_handle = session.handle();

        async move {
            if name == SFTP_SUBSYSTEM {
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

                let context =
                    SessionContext::new(ProtocolPrincipal::new(Arc::new(identity)), SessionProtocol::Sftp, client_ip.ip());

                let (client_pipe, server_pipe) = tokio::io::duplex(SFTP_BUFFER_SIZE);
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
                    let mut buf = vec![0u8; SFTP_READ_BUF_SIZE];
                    loop {
                        match client_read.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(n) => {
                                let data: Vec<u8> = buf[..n].to_vec();
                                if session_handle.data(channel_id, data.into()).await.is_err() {
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

fn load_authorized_keys(auth_keys_path: &str) -> Result<Vec<String>, ServerError> {
    let path = Path::new(auth_keys_path);
    if !path.exists() {
        return Err(format!("Authorized keys file not found: {}", auth_keys_path).into());
    }

    let contents = std::fs::read_to_string(path)?;
    let mut keys = Vec::new();

    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with(SSH_KEY_TYPE_RSA) || line.starts_with(SSH_KEY_TYPE_ED25519) || line.starts_with(SSH_KEY_TYPE_ECDSA) {
            keys.push(line.to_string());
        } else {
            warn!(
                "Skipping invalid authorized key line in {}: doesn't start with valid key type",
                auth_keys_path
            );
        }
    }

    info!("Loaded {} authorized SSH public keys from {}", keys.len(), auth_keys_path);
    Ok(keys)
}

fn parse_auth_username(username: &str) -> (&str, Option<&str>) {
    if let Some(idx) = username.rfind('=') {
        let suffix = &username[idx..];
        if suffix == AUTH_SUFFIX_SVC || suffix == AUTH_SUFFIX_LDAP {
            return (&username[..idx], Some(suffix));
        }
    }
    (username, None)
}

fn validate_ssh_certificate(
    russh_key: &PublicKey,
    trusted_cas: &[ssh_key::PublicKey],
    raw_username: &str,
) -> Result<bool, ServerError> {
    let (username, _suffix) = parse_auth_username(raw_username);

    let key_bytes = russh_key.public_key_bytes();

    let cert = match Certificate::from_bytes(&key_bytes) {
        Ok(c) => c,
        Err(_) => {
            debug!("Provided key is not a certificate. Skipping cert validation.");
            return Ok(false);
        }
    };

    debug!("Verifying SSH Certificate: KeyID='{}', Serial={}", cert.comment(), cert.serial());

    let mut signature_valid = false;
    let signature_key = cert.signature_key();

    for ca in trusted_cas {
        if ca.key_data() == signature_key {
            signature_valid = true;
            debug!("Certificate signed by trusted CA: {}", ca.fingerprint(Default::default()));
            break;
        }
    }

    if !signature_valid {
        warn!("Certificate signer not found in trusted CAs");
        return Ok(false);
    }

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

    if !cert.valid_principals().contains(&username.to_string()) {
        warn!(
            "Certificate does not authorize user '{}'. Principals: {:?}",
            username,
            cert.valid_principals()
        );
        return Ok(false);
    }

    match cert.cert_type() {
        CertType::User => {}
        _ => {
            warn!("Certificate is not a User certificate");
            return Ok(false);
        }
    }

    for (name, _value) in cert.critical_options().iter() {
        if name.as_str() == CRITICAL_OPTION_SOURCE_ADDRESS {
        } else {
            warn!("Rejecting certificate due to unsupported critical option: {}", name);
            return Ok(false);
        }
    }

    info!("SSH Certificate validation successful for user '{}'", username);
    Ok(true)
}

fn compare_keys(stored_key: &str, client_key_base64: &str) -> bool {
    let stored_key_parts: Vec<&str> = stored_key.split_whitespace().collect();
    if stored_key_parts.is_empty() {
        return false;
    }

    let stored_key_data = stored_key_parts.get(1).unwrap_or(&stored_key);

    if *stored_key_data == client_key_base64 {
        return true;
    }

    if let Ok(stored_bytes) = BASE64.decode(stored_key_data) {
        if let Ok(client_bytes) = BASE64.decode(client_key_base64) {
            return stored_bytes == client_bytes;
        }
    }

    false
}
