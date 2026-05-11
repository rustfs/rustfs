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

use super::config::{WebDavConfig, WebDavInitError};
use super::driver::WebDavDriver;
use crate::common::client::s3::StorageBackend;
use crate::common::session::{Protocol, ProtocolPrincipal, SessionContext};
use crate::tls_hot_reload::{ReloadableCertResolver, spawn_cert_reload_loop};
use bytes::Bytes;
use dav_server::DavHandler;
use dav_server::fakels::FakeLs;
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use rustls::ServerConfig;
use std::convert::Infallible;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, warn};

/// WebDAV server implementation
pub struct WebDavServer<S>
where
    S: StorageBackend + Clone + Send + Sync + 'static + std::fmt::Debug,
{
    /// Server configuration
    config: WebDavConfig,
    /// S3 storage backend
    storage: S,
}

impl<S> WebDavServer<S>
where
    S: StorageBackend + Clone + Send + Sync + 'static + std::fmt::Debug,
{
    /// Create a new WebDAV server
    pub async fn new(config: WebDavConfig, storage: S) -> Result<Self, WebDavInitError> {
        config.validate().await?;
        Ok(Self { config, storage })
    }

    /// Start the WebDAV server
    pub async fn start(&self, mut shutdown_rx: broadcast::Receiver<()>) -> Result<(), WebDavInitError> {
        info!("Initializing WebDAV server on {}", self.config.bind_addr);

        let listener = TcpListener::bind(self.config.bind_addr).await?;
        info!("WebDAV server listening on {}", self.config.bind_addr);

        // Setup TLS if enabled
        let tls_acceptor = if self.config.tls_enabled {
            if let Some(cert_dir) = &self.config.cert_dir {
                debug!("Enabling WebDAV TLS with certificates from: {}", cert_dir);

                let resolver = ReloadableCertResolver::load_from_directory(cert_dir)
                    .map_err(|e| WebDavInitError::Tls(format!("Failed to create certificate resolver: {}", e)))?;
                spawn_cert_reload_loop("webdav", cert_dir.clone(), resolver.clone());

                let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

                let server_config = ServerConfig::builder().with_no_client_auth().with_cert_resolver(resolver);

                Some(TlsAcceptor::from(Arc::new(server_config)))
            } else {
                None
            }
        } else {
            None
        };

        let storage = self.storage.clone();

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, addr)) => {
                            let storage = storage.clone();
                            let tls_acceptor = tls_acceptor.clone();

                            let max_body_size = self.config.max_body_size;
                            tokio::spawn(async move {
                                let source_ip: IpAddr = addr.ip();

                                if let Some(acceptor) = tls_acceptor {
                                    match acceptor.accept(stream).await {
                                        Ok(tls_stream) => {
                                            let io = TokioIo::new(tls_stream);
                                            if let Err(e) = Self::handle_connection_impl(io, storage, source_ip, max_body_size).await {
                                                debug!("Connection error: {}", e);
                                            }
                                        }
                                        Err(e) => {
                                            debug!("TLS handshake failed: {}", e);
                                        }
                                    }
                                } else {
                                    let io = TokioIo::new(stream);
                                    if let Err(e) = Self::handle_connection_impl(io, storage, source_ip, max_body_size).await {
                                        debug!("Connection error: {}", e);
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            error!("Failed to accept connection: {}", e);
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("WebDAV server received shutdown signal");
                    break;
                }
            }
        }

        info!("WebDAV server stopped");
        Ok(())
    }

    /// Handle a single connection with hyper-util TokioIo wrapper
    async fn handle_connection_impl<I>(
        io: TokioIo<I>,
        storage: S,
        source_ip: IpAddr,
        max_body_size: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let service = service_fn(move |req: Request<hyper::body::Incoming>| {
            let storage = storage.clone();
            async move { Self::handle_request(req, storage, source_ip, max_body_size).await }
        });

        http1::Builder::new().serve_connection(io, service).await?;

        Ok(())
    }

    /// Handle a single WebDAV request
    async fn handle_request(
        req: Request<hyper::body::Incoming>,
        storage: S,
        source_ip: IpAddr,
        max_body_size: u64,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        // Check Content-Length against max_body_size before reading body
        if let Some(content_length) = req.headers().get("content-length")
            && let Ok(length_str) = content_length.to_str()
            && let Ok(length) = length_str.parse::<u64>()
            && length > max_body_size
        {
            warn!("Request body too large: {} > {}", length, max_body_size);
            return Ok(error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                &format!("Request body too large. Maximum size is {} bytes", max_body_size),
            ));
        }

        // Extract authorization header
        let auth_header = req.headers().get("authorization").and_then(|h| h.to_str().ok());

        // Parse Basic auth credentials
        let (access_key, secret_key) = match auth_header {
            Some(auth) if auth.starts_with("Basic ") => {
                let encoded = &auth[6..];
                match base64_decode(encoded) {
                    Ok(decoded) => {
                        let decoded_str = String::from_utf8_lossy(&decoded);
                        if let Some((user, pass)) = decoded_str.split_once(':') {
                            (user.to_string(), pass.to_string())
                        } else {
                            return Ok(unauthorized_response());
                        }
                    }
                    Err(_) => return Ok(unauthorized_response()),
                }
            }
            _ => return Ok(unauthorized_response()),
        };

        // Authenticate user
        let session_context = match Self::authenticate(&access_key, &secret_key, source_ip).await {
            Ok(ctx) => ctx,
            Err(_) => return Ok(unauthorized_response()),
        };

        // Create WebDAV driver with session context
        let driver = WebDavDriver::new(storage, Arc::new(session_context));

        // Build DAV handler with boxed filesystem
        let dav_handler = DavHandler::builder()
            .filesystem(Box::new(driver))
            .locksystem(FakeLs::new())
            .build_handler();

        // Convert request body
        let (parts, body) = req.into_parts();
        let body_bytes = match body.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => {
                error!("Failed to read request body: {}", e);
                return Ok(error_response(StatusCode::BAD_REQUEST, "Failed to read request body"));
            }
        };

        // Create request for dav-server using Bytes
        let dav_req = Request::from_parts(parts, dav_server::body::Body::from(body_bytes));

        // Handle the request
        let dav_resp = dav_handler.handle(dav_req).await;

        // Convert response
        let (parts, body) = dav_resp.into_parts();
        let body_bytes = match body.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => {
                error!("Failed to read response body: {}", e);
                return Ok(error_response(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error"));
            }
        };

        Ok(Response::from_parts(parts, Full::new(body_bytes)))
    }

    /// Authenticate user against IAM system
    async fn authenticate(access_key: &str, secret_key: &str, source_ip: IpAddr) -> Result<SessionContext, WebDavInitError> {
        use rustfs_credentials::Credentials as S3Credentials;
        use rustfs_iam::get;

        // Access IAM system
        let iam_sys = get().map_err(|e| {
            error!("IAM system unavailable during WebDAV auth: {}", e);
            WebDavInitError::Server("Internal authentication service unavailable".to_string())
        })?;

        let s3_creds = S3Credentials {
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
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
            error!("IAM check_key failed for {}: {}", access_key, e);
            WebDavInitError::Server("Authentication verification failed".to_string())
        })?;

        if !is_valid {
            warn!("WebDAV login failed: Invalid access key '{}'", access_key);
            return Err(WebDavInitError::Server("Invalid credentials".to_string()));
        }

        let identity = user_identity.ok_or_else(|| {
            error!("User identity missing despite valid key for {}", access_key);
            WebDavInitError::Server("User not found".to_string())
        })?;

        if !identity.credentials.secret_key.eq(&s3_creds.secret_key) {
            warn!("WebDAV login failed: Invalid secret key for '{}'", access_key);
            return Err(WebDavInitError::Server("Invalid credentials".to_string()));
        }

        info!("WebDAV user '{}' authenticated successfully", access_key);

        Ok(SessionContext::new(
            ProtocolPrincipal::new(Arc::new(identity)),
            Protocol::WebDav,
            source_ip,
        ))
    }

    /// Get server configuration
    pub fn config(&self) -> &WebDavConfig {
        &self.config
    }

    /// Get storage backend
    pub fn storage(&self) -> &S {
        &self.storage
    }
}

/// Create unauthorized response with WWW-Authenticate header
fn unauthorized_response() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header("WWW-Authenticate", "Basic realm=\"RustFS WebDAV\"")
        .body(Full::new(Bytes::from("Unauthorized")))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::from("Unauthorized"))))
}

/// Create error response
fn error_response(status: StatusCode, message: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::from(message.to_string())))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::from("Internal Server Error"))))
}

/// Decode base64 string
fn base64_decode(encoded: &str) -> Result<Vec<u8>, ()> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(encoded).map_err(|_| ())
}
