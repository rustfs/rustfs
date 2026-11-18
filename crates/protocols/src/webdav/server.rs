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
use crate::common::session::{Protocol, ProtocolPrincipal, SessionContext, is_temporary_credential};
use bytes::Bytes;
use dav_server::DavHandler;
use dav_server::fakels::FakeLs;
use http::header::{
    ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
    ACCESS_CONTROL_EXPOSE_HEADERS, ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD, AUTHORIZATION, ORIGIN, REFERER,
    USER_AGENT, VARY,
};
use http::{HeaderMap, HeaderValue, Method};
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::body::Body as HttpBody;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use rustfs_config::{DEFAULT_TLS_RELOAD_ENABLE, DEFAULT_TLS_RELOAD_INTERVAL, ENV_TLS_RELOAD_ENABLE, ENV_TLS_RELOAD_INTERVAL};
use rustfs_tls_runtime::{ReloadableServerCertResolver, TlsReloadOptions, spawn_server_cert_reload_loop};
use rustfs_utils::MaskedAccessKey;
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, broadcast, watch};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tracing::{Instrument, debug, error, info, info_span, warn};

const CONNECTION_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_WEBDAV_SERVER: &str = "webdav_server";
const LOG_SUBSYSTEM_WEBDAV_AUTH: &str = "webdav_auth";
const EVENT_WEBDAV_SERVER_STATE: &str = "webdav_server_state";
const EVENT_WEBDAV_TLS_STATE: &str = "webdav_tls_state";
const EVENT_WEBDAV_CONNECTION_STATE: &str = "webdav_connection_state";
const EVENT_WEBDAV_REQUEST_VALIDATION_FAILED: &str = "webdav_request_validation_failed";
const EVENT_WEBDAV_REQUEST_BODY_FAILED: &str = "webdav_request_body_failed";
const EVENT_WEBDAV_AUTH_STATE: &str = "webdav_auth_state";
const EVENT_WEBDAV_CONNECTION_CAP_STATE: &str = "webdav_connection_cap_state";

/// Response body handed back to Hyper.
///
/// Boxed instead of collected: buffering the dav-server body would
/// materialise a whole object in memory for every GET.
type WebDavBody = Pin<Box<dyn HttpBody<Data = Bytes, Error = io::Error> + Send>>;

/// Listener-wide browser policy, parsed once and shared across connections.
enum WebDavCors {
    Disabled,
    Wildcard,
    Origins(Vec<HeaderValue>),
}

impl WebDavCors {
    const METHODS: &str = "OPTIONS, GET, HEAD, PUT, DELETE, PATCH, PROPFIND, PROPPATCH, MKCOL, COPY, MOVE, LOCK, UNLOCK";

    fn new(origins: &str) -> Self {
        match origins.trim() {
            "" => Self::Disabled,
            "*" => Self::Wildcard,
            origins => Self::Origins(
                origins
                    .split(',')
                    .map(str::trim)
                    .filter(|origin| !origin.is_empty() && *origin != "*")
                    .filter_map(|origin| HeaderValue::from_str(origin).ok())
                    .collect(),
            ),
        }
    }

    fn allowed_origin(&self, headers: &HeaderMap) -> Option<HeaderValue> {
        if matches!(self, Self::Disabled) {
            return None;
        }
        let mut origins = headers.get_all(ORIGIN).iter();
        let origin = origins.next()?;
        let value = origin.to_str().ok()?;
        if origins.next().is_some() || value.is_empty() || value.bytes().any(|byte| byte.is_ascii_whitespace() || byte == b',') {
            return None;
        }
        match self {
            Self::Origins(allowed) => allowed.iter().find(|allowed| *allowed == origin).cloned(),
            Self::Wildcard => Some(HeaderValue::from_static("*")),
            Self::Disabled => None,
        }
    }

    /// Keep the CORS boundary outside authentication and DAV dispatch so every
    /// application response, including early errors, follows the same policy.
    async fn handle<B, F, Fut>(&self, req: Request<B>, next: F) -> Result<Response<WebDavBody>, Infallible>
    where
        F: FnOnce(Request<B>) -> Fut,
        Fut: Future<Output = Result<Response<WebDavBody>, Infallible>>,
    {
        let allow_origin = self.allowed_origin(req.headers());
        let is_options = req.method() == Method::OPTIONS;
        let is_preflight =
            is_options && req.headers().contains_key(ORIGIN) && req.headers().contains_key(ACCESS_CONTROL_REQUEST_METHOD);

        let mut response = if is_preflight {
            self.preflight(req.headers(), allow_origin.is_some())
        } else {
            next(req).await?
        };

        // Absent and rejected Origin values also select cache variants. Append
        // rather than overwrite the DAV handler's existing Vary fields.
        response.headers_mut().append(VARY, HeaderValue::from_static("Origin"));
        if is_options {
            response.headers_mut().append(
                VARY,
                HeaderValue::from_static("Access-Control-Request-Method, Access-Control-Request-Headers"),
            );
        }
        if let Some(origin) = allow_origin {
            response.headers_mut().insert(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
            if matches!(self, Self::Origins(_)) {
                response
                    .headers_mut()
                    .insert(ACCESS_CONTROL_ALLOW_CREDENTIALS, HeaderValue::from_static("true"));
            }
            if !is_preflight {
                response.headers_mut().insert(
                    ACCESS_CONTROL_EXPOSE_HEADERS,
                    HeaderValue::from_static("ETag, DAV, Allow, Lock-Token, Content-Range, Accept-Ranges, WWW-Authenticate"),
                );
            }
        }
        Ok(response)
    }

    fn preflight(&self, headers: &HeaderMap, origin_allowed: bool) -> Response<WebDavBody> {
        let mut methods = headers.get_all(ACCESS_CONTROL_REQUEST_METHOD).iter();
        let method = methods.next().and_then(|value| value.to_str().ok());
        let method_allowed = method.is_some_and(|method| Self::METHODS.split(", ").any(|allowed| allowed == method));
        if !origin_allowed || !method_allowed || methods.next().is_some() {
            return error_response(StatusCode::FORBIDDEN, "CORS preflight denied");
        }
        let Ok(allowed_headers) = cors_requested_headers(headers) else {
            return error_response(StatusCode::FORBIDDEN, "CORS preflight denied");
        };

        let mut response = Response::new(fixed_body(Bytes::new()));
        *response.status_mut() = StatusCode::NO_CONTENT;
        response
            .headers_mut()
            .insert(ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static(Self::METHODS));
        // A wildcard does not cover Authorization, and is not a wildcard for
        // credentialed requests. Echo the validated names explicitly instead.
        if let Some(value) = allowed_headers {
            response.headers_mut().insert(ACCESS_CONTROL_ALLOW_HEADERS, value);
        }
        response
    }
}

/// Validate borrowed field names, reusing an ordinary single header value.
/// RFC 9110 section 5.6.1.2 requires recipients to ignore empty list elements;
/// normalize those and repeated fields into one nonempty response list.
fn cors_requested_headers(headers: &HeaderMap) -> Result<Option<HeaderValue>, ()> {
    let values = headers.get_all(ACCESS_CONTROL_REQUEST_HEADERS);
    let mut fields = values.iter();
    let Some(first) = fields.next() else {
        return Ok(None);
    };
    let mut normalize = fields.next().is_some();
    let mut has_name = false;
    for value in values.iter() {
        for name in value.to_str().map_err(|_| ())?.split(',') {
            let name = name.trim_matches([' ', '\t']);
            if name.is_empty() {
                normalize = true;
                continue;
            }
            if !is_cors_header_name(name) {
                return Err(());
            }
            has_name = true;
        }
    }
    if !has_name {
        return Err(());
    }
    if !normalize {
        return Ok(Some(first.clone()));
    }

    let capacity = values.iter().map(|value| value.as_bytes().len() + 1).sum();
    let mut normalized = String::with_capacity(capacity);
    for value in values {
        for name in value.to_str().map_err(|_| ())?.split(',') {
            let name = name.trim_matches([' ', '\t']);
            if name.is_empty() {
                continue;
            }
            if !normalized.is_empty() {
                normalized.push(',');
            }
            normalized.push_str(name);
        }
    }
    HeaderValue::try_from(normalized).map(Some).map_err(|_| ())
}

/// RFC 9110 field-name is a nonempty sequence of ASCII tchar bytes.
fn is_cors_header_name(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#' | b'$' | b'%' | b'&' | b'\'' | b'*' | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|' | b'~'
                )
        })
}

fn policy_request_headers(headers: &HeaderMap) -> HeaderMap {
    let mut policy_headers = HeaderMap::new();
    for name in [USER_AGENT, REFERER] {
        if let Some(value) = headers.get(&name) {
            policy_headers.insert(name, value.clone());
        }
    }

    let mut authorization = HeaderValue::from_static("Basic");
    authorization.set_sensitive(true);
    policy_headers.insert(AUTHORIZATION, authorization);
    policy_headers
}

struct ConnectionLimits {
    max_body_size: u64,
    request_timeout: Duration,
}

/// WebDAV server implementation
pub struct WebDavServer<S>
where
    S: StorageBackend + Clone + Send + Sync + 'static + std::fmt::Debug,
{
    /// Server configuration
    config: WebDavConfig,
    /// S3 storage backend
    storage: S,
    cors: Arc<WebDavCors>,
}

impl<S> WebDavServer<S>
where
    S: StorageBackend + Clone + Send + Sync + 'static + std::fmt::Debug,
{
    fn tls_reload_options() -> TlsReloadOptions {
        TlsReloadOptions {
            enabled: rustfs_utils::get_env_bool(ENV_TLS_RELOAD_ENABLE, DEFAULT_TLS_RELOAD_ENABLE),
            interval: Duration::from_secs(rustfs_utils::get_env_u64(ENV_TLS_RELOAD_INTERVAL, DEFAULT_TLS_RELOAD_INTERVAL).max(5)),
            ..TlsReloadOptions::default()
        }
    }

    /// Create a new WebDAV server
    pub async fn new(config: WebDavConfig, storage: S) -> Result<Self, WebDavInitError> {
        config.validate().await?;
        let origins =
            rustfs_utils::get_env_str(rustfs_config::ENV_CORS_ALLOWED_ORIGINS, rustfs_config::DEFAULT_CORS_ALLOWED_ORIGINS);
        Ok(Self {
            config,
            storage,
            cors: Arc::new(WebDavCors::new(&origins)),
        })
    }

    /// Start the WebDAV server
    pub async fn start(&self, shutdown_rx: broadcast::Receiver<()>) -> Result<(), WebDavInitError> {
        info!(
            event = EVENT_WEBDAV_SERVER_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_WEBDAV_SERVER,
            state = "starting",
            bind_addr = %self.config.bind_addr,
            tls_enabled = self.config.tls_enabled,
            max_body_size = self.config.max_body_size,
            max_connections = self.config.max_connections,
            "WebDAV server starting"
        );

        if self.config.tls_enabled && self.config.cert_dir.is_some() {
            rustfs_tls_runtime::ensure_crypto_provider().map_err(|err| WebDavInitError::Tls(err.to_string()))?;
        }

        let listener = TcpListener::bind(self.config.bind_addr).await?;
        self.serve(listener, shutdown_rx).await
    }

    /// Serve connections from an already bound listener
    async fn serve(&self, listener: TcpListener, mut shutdown_rx: broadcast::Receiver<()>) -> Result<(), WebDavInitError> {
        info!(
            event = EVENT_WEBDAV_SERVER_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_WEBDAV_SERVER,
            state = "listening",
            bind_addr = %self.config.bind_addr,
            "WebDAV server listening"
        );
        let (shutdown_tx, shutdown) = watch::channel(false);
        let mut reload_task = None;

        // Setup TLS if enabled
        let tls_acceptor = if self.config.tls_enabled {
            if let Some(cert_dir) = &self.config.cert_dir {
                debug!(
                    event = EVENT_WEBDAV_TLS_STATE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_WEBDAV_SERVER,
                    state = "enabled",
                    cert_dir = %cert_dir,
                    "WebDAV TLS enabled"
                );

                let resolver = ReloadableServerCertResolver::load_from_directory(cert_dir)
                    .map_err(|e| WebDavInitError::Tls(format!("Failed to create certificate resolver: {}", e)))?;
                let server_config = crate::common::tls::server_config(resolver.clone(), self.config.ca_file.as_deref())
                    .map_err(|error| WebDavInitError::Tls(format!("Invalid client CA configuration: {error}")))?;
                reload_task = spawn_server_cert_reload_loop("webdav", resolver, Self::tls_reload_options(), shutdown.clone());

                Some(TlsAcceptor::from(Arc::new(server_config)))
            } else {
                None
            }
        } else {
            None
        };

        let storage = self.storage.clone();
        let request_timeout = Duration::from_secs(self.config.request_timeout_secs);
        let connection_limiter = Arc::new(Semaphore::new(self.config.max_connections.min(Semaphore::MAX_PERMITS)));

        let mut connections = JoinSet::new();
        let mut failure = None;
        'accept: loop {
            while let Some(result) = connections.try_join_next() {
                if let Err(error) = result {
                    failure = Some(WebDavInitError::TaskJoin(error));
                    break 'accept;
                }
            }
            // Admission control: hold a permit before accepting, so at
            // saturation the kernel backlog absorbs the burst instead of the
            // process spawning an unbounded number of connection tasks. The
            // permit travels into the task and is released when it ends.
            let permit = match connection_limiter.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    debug!(
                        event = EVENT_WEBDAV_CONNECTION_CAP_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_WEBDAV_SERVER,
                        state = "saturated",
                        max_connections = self.config.max_connections,
                        "WebDAV connection cap saturated"
                    );
                    tokio::select! {
                        permit = connection_limiter.clone().acquire_owned() => match permit {
                            Ok(permit) => permit,
                            // The semaphore is never closed; fail safe by
                            // stopping the accept loop if it ever is.
                            Err(_) => break,
                        },
                        _ = shutdown_rx.recv() => {
                            info!(
                                event = EVENT_WEBDAV_SERVER_STATE,
                                component = LOG_COMPONENT_PROTOCOLS,
                                subsystem = LOG_SUBSYSTEM_WEBDAV_SERVER,
                                state = "shutdown_requested",
                                "WebDAV shutdown requested"
                            );
                            break;
                        }
                    }
                }
            };

            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, addr)) => {
                            let storage = storage.clone();
                            let cors = self.cors.clone();
                            let tls_acceptor = tls_acceptor.clone();

                            let limits = ConnectionLimits {
                                max_body_size: self.config.max_body_size,
                                request_timeout,
                            };
                            let source_ip: IpAddr = addr.ip();
                            let span = info_span!(
                                "webdav-connection",
                                peer = %source_ip,
                                transport = if tls_acceptor.is_some() { "tls" } else { "tcp" },
                            );
                            let mut shutdown = shutdown.clone();
                            connections.spawn(
                                async move {
                                    let _permit = permit;
                                    if let Some(acceptor) = tls_acceptor {
                                        // A handshake that never completes would otherwise
                                        // hold its connection permit forever.
                                        let handshake = tokio::select! {
                                            biased;
                                            _ = shutdown.changed() => return,
                                            result = timeout(request_timeout, acceptor.accept(stream)) => result,
                                        };
                                        match handshake {
                                            Ok(Ok(tls_stream)) => {
                                                let io = TokioIo::new(tls_stream);
                                                if let Err(e) = Self::handle_connection_impl(io, storage, source_ip, true, limits, cors, shutdown).await {
                                                    debug!(
                                                        event = EVENT_WEBDAV_CONNECTION_STATE,
                                                        component = LOG_COMPONENT_PROTOCOLS,
                                                        subsystem = LOG_SUBSYSTEM_WEBDAV_SERVER,
                                                        result = "error",
                                                        peer = %source_ip,
                                                        transport = "tls",
                                                        error = %e,
                                                        "webdav connection ended with error"
                                                    );
                                                }
                                            }
                                            Ok(Err(e)) => {
                                                debug!(
                                                    event = EVENT_WEBDAV_CONNECTION_STATE,
                                                    component = LOG_COMPONENT_PROTOCOLS,
                                                    subsystem = LOG_SUBSYSTEM_WEBDAV_SERVER,
                                                    result = "tls_handshake_failed",
                                                    peer = %source_ip,
                                                    error = %e,
                                                    "webdav connection ended with error"
                                                );
                                            }
                                            Err(_) => {
                                                debug!(
                                                    event = EVENT_WEBDAV_CONNECTION_STATE,
                                                    component = LOG_COMPONENT_PROTOCOLS,
                                                    subsystem = LOG_SUBSYSTEM_WEBDAV_SERVER,
                                                    result = "tls_handshake_timeout",
                                                    peer = %source_ip,
                                                    timeout_secs = request_timeout.as_secs(),
                                                    "webdav connection ended with error"
                                                );
                                            }
                                        }
                                    } else {
                                        let io = TokioIo::new(stream);
                                        if let Err(e) = Self::handle_connection_impl(io, storage, source_ip, false, limits, cors, shutdown).await {
                                            debug!(
                                                event = EVENT_WEBDAV_CONNECTION_STATE,
                                                component = LOG_COMPONENT_PROTOCOLS,
                                                subsystem = LOG_SUBSYSTEM_WEBDAV_SERVER,
                                                result = "error",
                                                peer = %source_ip,
                                                transport = "tcp",
                                                error = %e,
                                                "webdav connection ended with error"
                                            );
                                        }
                                    }
                                }
                                .instrument(span),
                            );
                        }
                        Err(e) => {
                            error!(
                                event = EVENT_WEBDAV_CONNECTION_STATE,
                                component = LOG_COMPONENT_PROTOCOLS,
                                subsystem = LOG_SUBSYSTEM_WEBDAV_SERVER,
                                result = "accept_failed",
                                error = %e,
                                "webdav connection accept failed"
                            );
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!(
                        event = EVENT_WEBDAV_SERVER_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_WEBDAV_SERVER,
                        state = "shutdown_requested",
                        "WebDAV shutdown requested"
                    );
                    break;
                }
            }
        }

        drop(listener);
        shutdown_tx.send_replace(true);
        let drain = async {
            while let Some(result) = connections.join_next().await {
                if let Err(error) = result {
                    failure.get_or_insert(WebDavInitError::TaskJoin(error));
                }
            }
        };
        if timeout(CONNECTION_DRAIN_TIMEOUT, drain).await.is_err() {
            connections.shutdown().await;
            failure.get_or_insert(WebDavInitError::ShutdownDeadline);
        }
        if let Some(task) = reload_task
            && let Err(error) = task.await
        {
            failure.get_or_insert(WebDavInitError::TaskJoin(error));
        }
        info!(
            event = EVENT_WEBDAV_SERVER_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_WEBDAV_SERVER,
            state = "stopped",
            "WebDAV server stopped"
        );
        failure.map_or(Ok(()), Err)
    }

    /// Handle a single connection with hyper-util TokioIo wrapper
    async fn handle_connection_impl<I>(
        io: TokioIo<I>,
        storage: S,
        source_ip: IpAddr,
        secure_transport: bool,
        limits: ConnectionLimits,
        cors: Arc<WebDavCors>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let ConnectionLimits {
            max_body_size,
            request_timeout,
        } = limits;
        // These borrows live for the connection. Preflights complete before
        // invoking the closure that clones the backend for an actual request.
        let cors = cors.as_ref();
        let storage = &storage;
        let received_request = AtomicBool::new(false);
        let service = service_fn(|req: Request<hyper::body::Incoming>| {
            received_request.store(true, Ordering::Relaxed);
            cors.handle(req, move |req| {
                Self::handle_request(req, storage.clone(), source_ip, secure_transport, max_body_size, request_timeout)
            })
        });

        // A peer that opens a connection and dribbles (or never finishes)
        // request headers is disconnected once the configured request
        // timeout elapses. The timer is required for the deadline to apply.
        let connection = http1::Builder::new()
            .timer(TokioTimer::new())
            .header_read_timeout(request_timeout)
            .serve_connection(io, service);
        tokio::pin!(connection);
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                // Hyper starts in a busy state even before parsing the first
                // request. An incomplete header has no application work to drain.
                if received_request.load(Ordering::Relaxed) {
                    connection.as_mut().graceful_shutdown();
                    connection.await?;
                }
            }
            result = &mut connection => result?,
        }

        Ok(())
    }

    /// Handle a single WebDAV request
    async fn handle_request(
        req: Request<hyper::body::Incoming>,
        storage: S,
        source_ip: IpAddr,
        secure_transport: bool,
        max_body_size: u64,
        request_timeout: Duration,
    ) -> Result<Response<WebDavBody>, Infallible> {
        // Advisory fast path only: a declared Content-Length lets an
        // oversized request be rejected before any body is read. The
        // authoritative limit is enforced in `dispatch_dav` against the
        // bytes actually received, which a chunked request cannot dodge.
        if let Some(content_length) = req.headers().get("content-length")
            && let Ok(length_str) = content_length.to_str()
            && let Ok(length) = length_str.parse::<u64>()
            && length > max_body_size
        {
            warn!(
                event = EVENT_WEBDAV_REQUEST_VALIDATION_FAILED,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_WEBDAV_SERVER,
                result = "payload_too_large",
                content_length = length,
                max_body_size,
                source_ip = %source_ip,
                "webdav request validation failed"
            );
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
        let driver = WebDavDriver::new(storage, Arc::new(session_context))
            .with_request_context(policy_request_headers(req.headers()), secure_transport);

        // Build DAV handler with boxed filesystem
        let dav_handler = DavHandler::builder()
            .filesystem(Box::new(driver))
            .locksystem(FakeLs::new())
            .build_handler();

        Ok(dispatch_dav(req, dav_handler, source_ip, max_body_size, request_timeout).await)
    }

    /// Authenticate user against IAM system
    async fn authenticate(access_key: &str, secret_key: &str, source_ip: IpAddr) -> Result<SessionContext, WebDavInitError> {
        use rustfs_credentials::Credentials as S3Credentials;
        use rustfs_iam::get;
        let masked_access_key = MaskedAccessKey(access_key);

        // Access IAM system
        let iam_sys = get().map_err(|e| {
            error!(
                event = EVENT_WEBDAV_AUTH_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_WEBDAV_AUTH,
                result = "iam_unavailable",
                source_ip = %source_ip,
                error = %e,
                "WebDAV auth IAM unavailable"
            );
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
            error!(
                event = EVENT_WEBDAV_AUTH_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_WEBDAV_AUTH,
                result = "check_key_failed",
                source_ip = %source_ip,
                access_key = %masked_access_key,
                error = %e,
                "WebDAV auth key check failed"
            );
            WebDavInitError::Server("Authentication verification failed".to_string())
        })?;

        if !is_valid {
            warn!(
                event = EVENT_WEBDAV_AUTH_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_WEBDAV_AUTH,
                result = "invalid_access_key",
                source_ip = %source_ip,
                access_key = %masked_access_key,
                "WebDAV auth rejected access key"
            );
            return Err(WebDavInitError::Server("Invalid credentials".to_string()));
        }

        let identity = user_identity.ok_or_else(|| {
            error!(
                event = EVENT_WEBDAV_AUTH_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_WEBDAV_AUTH,
                result = "identity_missing",
                source_ip = %source_ip,
                access_key = %masked_access_key,
                "WebDAV auth identity missing"
            );
            WebDavInitError::Server("User not found".to_string())
        })?;

        if is_temporary_credential(&identity.credentials) {
            warn!(
                event = EVENT_WEBDAV_AUTH_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_WEBDAV_AUTH,
                result = "temporary_credential_rejected",
                source_ip = %source_ip,
                access_key = %masked_access_key,
                "WebDAV auth rejected temporary credential"
            );
            return Err(WebDavInitError::Server("Invalid credentials".to_string()));
        }

        // Constant-time secret comparison to prevent timing side-channel
        // attacks. Same primitive used by the SFTP handler and rustfs/src/auth.rs.
        let secret_matches: bool = identity
            .credentials
            .secret_key
            .as_bytes()
            .ct_eq(s3_creds.secret_key.as_bytes())
            .into();
        if !secret_matches {
            warn!(
                event = EVENT_WEBDAV_AUTH_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_WEBDAV_AUTH,
                result = "invalid_secret_key",
                source_ip = %source_ip,
                access_key = %masked_access_key,
                "WebDAV auth rejected secret key"
            );
            return Err(WebDavInitError::Server("Invalid credentials".to_string()));
        }

        debug!(
            event = EVENT_WEBDAV_AUTH_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_WEBDAV_AUTH,
            result = "authenticated",
            source_ip = %source_ip,
            access_key = %masked_access_key,
            "WebDAV auth accepted"
        );

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

/// Read the request body and run it through the dav handler.
///
/// Both limits configured for the listener are enforced here: the body is
/// truncated at `max_body_size` bytes actually received (a chunked upload
/// declares no length, so a header check cannot bound it), and neither the
/// body read nor the dav handler may run past `request_timeout`.
async fn dispatch_dav<B>(
    req: Request<B>,
    dav_handler: DavHandler,
    source_ip: IpAddr,
    max_body_size: u64,
    request_timeout: Duration,
) -> Response<WebDavBody>
where
    B: HttpBody<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let (parts, body) = req.into_parts();
    let limit = usize::try_from(max_body_size).unwrap_or(usize::MAX);

    let body_bytes = match timeout(request_timeout, Limited::new(body, limit).collect()).await {
        Ok(Ok(collected)) => collected.to_bytes(),
        Ok(Err(e)) if e.downcast_ref::<LengthLimitError>().is_some() => {
            warn!(
                event = EVENT_WEBDAV_REQUEST_VALIDATION_FAILED,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_WEBDAV_SERVER,
                result = "payload_too_large",
                max_body_size,
                source_ip = %source_ip,
                "webdav request validation failed"
            );
            return error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                &format!("Request body too large. Maximum size is {} bytes", max_body_size),
            );
        }
        Ok(Err(e)) => {
            error!(
                event = EVENT_WEBDAV_REQUEST_BODY_FAILED,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_WEBDAV_SERVER,
                result = "request_body_read_failed",
                source_ip = %source_ip,
                error = %e,
                "webdav request body failed"
            );
            return error_response(StatusCode::BAD_REQUEST, "Failed to read request body");
        }
        Err(_) => {
            warn!(
                event = EVENT_WEBDAV_REQUEST_VALIDATION_FAILED,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_WEBDAV_SERVER,
                result = "request_body_read_timeout",
                timeout_secs = request_timeout.as_secs(),
                source_ip = %source_ip,
                "webdav request validation failed"
            );
            return error_response(StatusCode::REQUEST_TIMEOUT, "Request timed out");
        }
    };

    // Create request for dav-server using Bytes
    let dav_req = Request::from_parts(parts, dav_server::body::Body::from(body_bytes));

    let dav_resp = match timeout(request_timeout, dav_handler.handle(dav_req)).await {
        Ok(resp) => resp,
        Err(_) => {
            warn!(
                event = EVENT_WEBDAV_REQUEST_VALIDATION_FAILED,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_WEBDAV_SERVER,
                result = "request_handling_timeout",
                timeout_secs = request_timeout.as_secs(),
                source_ip = %source_ip,
                "webdav request validation failed"
            );
            return error_response(StatusCode::REQUEST_TIMEOUT, "Request timed out");
        }
    };

    // Streamed straight to Hyper: collecting here would hold the whole
    // object in memory for the duration of a GET.
    let (parts, body) = dav_resp.into_parts();
    Response::from_parts(parts, Box::pin(body) as WebDavBody)
}

/// Create unauthorized response with WWW-Authenticate header
fn unauthorized_response() -> Response<WebDavBody> {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header("WWW-Authenticate", "Basic realm=\"RustFS WebDAV\"")
        .body(fixed_body("Unauthorized"))
        .unwrap_or_else(|_| Response::new(fixed_body("Unauthorized")))
}

/// Create error response
fn error_response(status: StatusCode, message: &str) -> Response<WebDavBody> {
    Response::builder()
        .status(status)
        .body(fixed_body(message.to_string()))
        .unwrap_or_else(|_| Response::new(fixed_body("Internal Server Error")))
}

/// Wrap a fixed byte payload in the streaming response body type
fn fixed_body(message: impl Into<Bytes>) -> WebDavBody {
    Box::pin(Full::new(message.into()).map_err(|never| match never {}))
}

/// Decode base64 string
fn base64_decode(encoded: &str) -> Result<Vec<u8>, ()> {
    base64_simd::STANDARD.decode_to_vec(encoded).map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::client::s3::StorageBackend;
    use async_trait::async_trait;
    use dav_server::memfs::MemFs;
    use futures_util::stream;
    use http_body_util::StreamBody;
    use hyper::body::Frame;
    use rustfs_credentials::Credentials;
    use rustfs_gateway_types::dto::*;
    use std::fmt::{Debug, Formatter};
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    const TEST_IP: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

    /// Storage double for the connection-level tests. Those requests are
    /// rejected before authentication succeeds, so storage is never reached.
    #[derive(Default)]
    struct StubStorage {
        clone_count: Arc<AtomicUsize>,
    }

    impl Clone for StubStorage {
        fn clone(&self) -> Self {
            self.clone_count.fetch_add(1, Ordering::Relaxed);
            Self {
                clone_count: self.clone_count.clone(),
            }
        }
    }

    impl Debug for StubStorage {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.write_str("StubStorage")
        }
    }

    #[async_trait]
    impl StorageBackend for StubStorage {
        type Error = std::io::Error;

        async fn get_object(
            &self,
            _bucket: &str,
            _key: &str,
            _credentials: &Credentials,
            _start_pos: Option<u64>,
        ) -> Result<GetObjectOutput, Self::Error> {
            unreachable!("connection tests should not hit storage")
        }

        async fn get_object_range(
            &self,
            _bucket: &str,
            _key: &str,
            _credentials: &Credentials,
            _start_pos: u64,
            _length: u64,
        ) -> Result<GetObjectOutput, Self::Error> {
            unreachable!("connection tests should not hit storage")
        }

        async fn put_object(&self, _input: PutObjectInput, _credentials: &Credentials) -> Result<PutObjectOutput, Self::Error> {
            unreachable!("connection tests should not hit storage")
        }

        async fn delete_object(
            &self,
            _bucket: &str,
            _key: &str,
            _credentials: &Credentials,
        ) -> Result<DeleteObjectOutput, Self::Error> {
            unreachable!("connection tests should not hit storage")
        }

        async fn head_object(
            &self,
            _bucket: &str,
            _key: &str,
            _credentials: &Credentials,
        ) -> Result<HeadObjectOutput, Self::Error> {
            unreachable!("connection tests should not hit storage")
        }

        async fn head_bucket(&self, _bucket: &str, _credentials: &Credentials) -> Result<HeadBucketOutput, Self::Error> {
            unreachable!("connection tests should not hit storage")
        }

        async fn list_objects_v2(
            &self,
            _input: ListObjectsV2Input,
            _credentials: &Credentials,
        ) -> Result<ListObjectsV2Output, Self::Error> {
            unreachable!("connection tests should not hit storage")
        }

        async fn list_buckets(&self, _credentials: &Credentials) -> Result<ListBucketsOutput, Self::Error> {
            unreachable!("connection tests should not hit storage")
        }

        async fn create_bucket(&self, _bucket: &str, _credentials: &Credentials) -> Result<CreateBucketOutput, Self::Error> {
            unreachable!("connection tests should not hit storage")
        }

        async fn delete_bucket(&self, _bucket: &str, _credentials: &Credentials) -> Result<DeleteBucketOutput, Self::Error> {
            unreachable!("connection tests should not hit storage")
        }

        async fn copy_object(
            &self,
            _input: CopyObjectInput,
            _credentials: &Credentials,
        ) -> Result<CopyObjectOutput, Self::Error> {
            unreachable!("connection tests should not hit storage")
        }

        async fn create_multipart_upload(
            &self,
            _input: CreateMultipartUploadInput,
            _credentials: &Credentials,
        ) -> Result<CreateMultipartUploadOutput, Self::Error> {
            unreachable!("connection tests should not hit storage")
        }

        async fn upload_part(
            &self,
            _input: UploadPartInput,
            _credentials: &Credentials,
        ) -> Result<UploadPartOutput, Self::Error> {
            unreachable!("connection tests should not hit storage")
        }

        async fn complete_multipart_upload(
            &self,
            _input: CompleteMultipartUploadInput,
            _credentials: &Credentials,
        ) -> Result<CompleteMultipartUploadOutput, Self::Error> {
            unreachable!("connection tests should not hit storage")
        }

        async fn abort_multipart_upload(
            &self,
            _input: AbortMultipartUploadInput,
            _credentials: &Credentials,
        ) -> Result<AbortMultipartUploadOutput, Self::Error> {
            unreachable!("connection tests should not hit storage")
        }

        async fn upload_part_copy(
            &self,
            _input: UploadPartCopyInput,
            _credentials: &Credentials,
        ) -> Result<UploadPartCopyOutput, Self::Error> {
            unreachable!("connection tests should not hit storage")
        }
    }

    /// Request body that never produces a frame, standing in for a peer that
    /// opens a chunked upload and then stalls.
    struct StalledBody;

    impl HttpBody for StalledBody {
        type Data = Bytes;
        type Error = io::Error;

        fn poll_frame(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
            Poll::Pending
        }
    }

    fn memfs_handler() -> DavHandler {
        DavHandler::builder()
            .filesystem(MemFs::new())
            .locksystem(FakeLs::new())
            .build_handler()
    }

    /// Chunked upload: no Content-Length header, `chunks` frames of `chunk_len` bytes.
    fn chunked_request(
        chunks: usize,
        chunk_len: usize,
    ) -> Request<StreamBody<impl stream::Stream<Item = io::Result<Frame<Bytes>>>>> {
        let frames: Vec<io::Result<Frame<Bytes>>> = (0..chunks)
            .map(|_| Ok(Frame::data(Bytes::from(vec![b'a'; chunk_len]))))
            .collect();
        Request::builder()
            .method("PUT")
            .uri("/upload.bin")
            .body(StreamBody::new(stream::iter(frames)))
            .expect("build chunked request")
    }

    fn get_request(uri: &str) -> Request<Full<Bytes>> {
        Request::builder()
            .method("GET")
            .uri(uri)
            .body(Full::new(Bytes::new()))
            .expect("build get request")
    }

    const BROWSER_ORIGIN: &str = "https://dav-console.example";

    async fn cors_http_request(origins: &str, method: &str, headers: &str) -> String {
        cors_http_exchange(origins, method, headers).await.0
    }

    async fn cors_http_exchange(origins: &str, method: &str, headers: &str) -> (String, usize) {
        let storage = StubStorage::default();
        let clone_count = storage.clone_count.clone();
        let (mut client, server) = tokio::io::duplex(8192);
        let (_shutdown_tx, shutdown) = tokio::sync::watch::channel(false);
        let connection = tokio::spawn(WebDavServer::<StubStorage>::handle_connection_impl(
            TokioIo::new(server),
            storage,
            TEST_IP,
            false,
            ConnectionLimits {
                max_body_size: 16,
                request_timeout: Duration::from_secs(30),
            },
            Arc::new(WebDavCors::new(origins)),
            shutdown,
        ));
        let request = format!("{method} / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n{headers}\r\n");
        client.write_all(request.as_bytes()).await.expect("write browser request");
        let mut response = Vec::new();
        timeout(Duration::from_secs(5), client.read_to_end(&mut response))
            .await
            .expect("browser response timeout")
            .expect("read browser response");
        timeout(Duration::from_secs(5), connection)
            .await
            .expect("connection shutdown timeout")
            .expect("connection task")
            .expect("serve browser request");
        (
            String::from_utf8(response).expect("ASCII HTTP response"),
            clone_count.load(Ordering::Relaxed),
        )
    }

    fn browser_request(method: &str, path: &str) -> Request<Full<Bytes>> {
        Request::builder()
            .method(method)
            .uri(path)
            .header(ORIGIN, BROWSER_ORIGIN)
            .body(Full::new(Bytes::new()))
            .expect("build browser request")
    }

    fn assert_browser_headers(response: &Response<WebDavBody>) {
        assert_eq!(response.headers()[ACCESS_CONTROL_ALLOW_ORIGIN], BROWSER_ORIGIN);
        assert_eq!(response.headers()[ACCESS_CONTROL_ALLOW_CREDENTIALS], "true");
        assert!(response.headers().get_all(VARY).iter().any(|value| value == "Origin"));
    }

    #[tokio::test]
    async fn webdav_cors_preflight_bypasses_authentication() {
        for method in [
            "OPTIONS",
            "GET",
            "HEAD",
            "PUT",
            "DELETE",
            "PATCH",
            "PROPFIND",
            "PROPPATCH",
            "MKCOL",
            "COPY",
            "MOVE",
            "LOCK",
            "UNLOCK",
        ] {
            let headers = format!(
                "Origin: {BROWSER_ORIGIN}\r\nAccess-Control-Request-Method: {method}\r\nAccess-Control-Request-Headers: authorization,depth,content-type,destination,if-match\r\n"
            );
            let response = cors_http_request(BROWSER_ORIGIN, "OPTIONS", &headers).await;
            assert!(response.starts_with("HTTP/1.1 204"), "{method}: {response}");
            assert!(response.contains(&format!("access-control-allow-origin: {BROWSER_ORIGIN}\r\n")));
            assert!(response.contains("access-control-allow-credentials: true\r\n"));
            let allowed_methods = response
                .lines()
                .find_map(|line| line.strip_prefix("access-control-allow-methods: "))
                .expect("preflight must advertise allowed methods");
            assert!(allowed_methods.split(',').map(str::trim).any(|allowed| allowed == method));
            assert!(response.contains("access-control-allow-headers: authorization,depth,content-type,destination,if-match\r\n"));
            assert!(response.contains("vary: Access-Control-Request-Method, Access-Control-Request-Headers\r\n"));
            assert!(!response.contains("www-authenticate:"));
        }
    }

    #[tokio::test]
    async fn webdav_cors_actual_requests_and_plain_options_require_authentication() {
        for method in ["GET", "PUT", "DELETE", "PROPFIND", "MOVE", "OPTIONS"] {
            let response = cors_http_request(BROWSER_ORIGIN, method, &format!("Origin: {BROWSER_ORIGIN}\r\n")).await;
            assert!(response.starts_with("HTTP/1.1 401"), "{method}: {response}");
            assert!(response.contains(&format!("access-control-allow-origin: {BROWSER_ORIGIN}\r\n")));
            assert!(response.contains("www-authenticate: Basic"));
        }
        for method in ["GET", "PROPFIND"] {
            for headers in ["", "Origin: https://other.example\r\n"] {
                let response = cors_http_request(BROWSER_ORIGIN, method, headers).await;
                assert!(response.starts_with("HTTP/1.1 401"), "{method}: {response}");
                assert!(!response.contains("access-control-allow-origin:"));
                assert!(response.contains("vary: Origin\r\n"));
            }
        }
        for headers in [
            format!("Origin: {BROWSER_ORIGIN}\r\nAuthorization: Basic !!!!\r\n"),
            format!("Origin: {BROWSER_ORIGIN}\r\nAccess-Control-Request-Method: PUT\r\n"),
        ] {
            let response = cors_http_request(BROWSER_ORIGIN, "PUT", &headers).await;
            assert!(response.starts_with("HTTP/1.1 401"));
            assert!(response.contains(&format!("access-control-allow-origin: {BROWSER_ORIGIN}\r\n")));
        }
        let response = cors_http_request(BROWSER_ORIGIN, "OPTIONS", "Access-Control-Request-Method: PROPFIND\r\n").await;
        assert!(response.starts_with("HTTP/1.1 401"));
        assert!(!response.contains("access-control-allow-origin:"));
        assert!(response.contains("vary: Origin\r\n"));
        assert!(response.contains("vary: Access-Control-Request-Method, Access-Control-Request-Headers\r\n"));
    }

    #[tokio::test]
    async fn webdav_cors_keeps_early_body_limit_errors_visible() {
        let response =
            cors_http_request(BROWSER_ORIGIN, "PUT", &format!("Origin: {BROWSER_ORIGIN}\r\nContent-Length: 17\r\n")).await;
        assert!(response.starts_with("HTTP/1.1 413"));
        assert!(response.contains(&format!("access-control-allow-origin: {BROWSER_ORIGIN}\r\n")));
    }

    #[tokio::test]
    async fn webdav_cors_rejects_unconfigured_and_unmatched_origins() {
        for (configuration, origin) in [
            ("", BROWSER_ORIGIN),
            ("  ", BROWSER_ORIGIN),
            (BROWSER_ORIGIN, "https://dav-console.example.attacker.test"),
            (BROWSER_ORIGIN, "null"),
            (BROWSER_ORIGIN, "https://other.example"),
            ("*, https://other.example", BROWSER_ORIGIN),
        ] {
            let headers = format!("Origin: {origin}\r\nAccess-Control-Request-Method: PROPFIND\r\n");
            let response = cors_http_request(configuration, "OPTIONS", &headers).await;
            assert!(response.starts_with("HTTP/1.1 403"), "{configuration:?}: {response}");
            assert!(!response.contains("access-control-allow-origin:"));
            assert!(!response.contains("access-control-allow-credentials:"));
            assert!(response.contains("vary: Origin\r\n"));
        }
    }

    #[tokio::test]
    async fn webdav_cors_rejects_malformed_preflights() {
        for headers in [
            format!("Origin: {BROWSER_ORIGIN}\r\nAccess-Control-Request-Method: TRACE\r\n"),
            format!("Origin: {BROWSER_ORIGIN}\r\nAccess-Control-Request-Method: propfind\r\n"),
            format!(
                "Origin: {BROWSER_ORIGIN}\r\nAccess-Control-Request-Method: PROPFIND\r\nAccess-Control-Request-Method: GET\r\n"
            ),
            format!("Origin: {BROWSER_ORIGIN}\r\nOrigin: https://other.example\r\nAccess-Control-Request-Method: PROPFIND\r\n"),
            format!("Origin: {BROWSER_ORIGIN},https://other.example\r\nAccess-Control-Request-Method: PROPFIND\r\n"),
            format!(
                "Origin: {BROWSER_ORIGIN}\r\nAccess-Control-Request-Method: PROPFIND\r\nAccess-Control-Request-Headers: invalid header\r\n"
            ),
            format!(
                "Origin: {BROWSER_ORIGIN}\r\nAccess-Control-Request-Method: PROPFIND\r\nAccess-Control-Request-Headers: , \t,\r\n"
            ),
        ] {
            let response = cors_http_request(BROWSER_ORIGIN, "OPTIONS", &headers).await;
            assert!(response.starts_with("HTTP/1.1 403"), "{headers}: {response}");
            assert!(!response.contains("access-control-allow-methods:"));
        }
    }

    #[tokio::test]
    async fn webdav_cors_normalizes_header_lists() {
        for requested in [
            "authorization, ,depth,",
            " , authorization,\tdepth , ",
            "authorization,\r\nAccess-Control-Request-Headers: , depth,",
        ] {
            let headers = format!(
                "Origin: {BROWSER_ORIGIN}\r\nAccess-Control-Request-Method: PROPFIND\r\nAccess-Control-Request-Headers: {requested}\r\n"
            );
            let response = cors_http_request(BROWSER_ORIGIN, "OPTIONS", &headers).await;
            assert!(response.starts_with("HTTP/1.1 204"), "{response}");
            assert!(response.contains("access-control-allow-headers: authorization,depth\r\n"));
        }
    }

    #[test]
    fn webdav_cors_header_name_validation_matches_http_tokens() {
        for byte in u8::MIN..=u8::MAX {
            let bytes = [b'x', byte, b'y'];
            let actual = std::str::from_utf8(&bytes).is_ok_and(is_cors_header_name);
            assert_eq!(actual, http::HeaderName::from_bytes(&bytes).is_ok(), "byte {byte}");
        }
        assert!(!is_cors_header_name(""));
        assert!(is_cors_header_name("Authorization"));
        assert!(is_cors_header_name("!#$%&'*+-.^_`|~"));
    }

    #[tokio::test]
    async fn webdav_cors_only_clones_backend_for_actual_requests() {
        for (method, requested_method, expected_clones, expected_status) in [
            ("OPTIONS", Some("PROPFIND"), 0, "204"),
            ("OPTIONS", Some("TRACE"), 0, "403"),
            ("OPTIONS", None, 1, "401"),
            ("PUT", Some("PUT"), 1, "401"),
        ] {
            let mut headers = format!("Origin: {BROWSER_ORIGIN}\r\n");
            if let Some(requested_method) = requested_method {
                headers.push_str(&format!("Access-Control-Request-Method: {requested_method}\r\n"));
            }
            let (response, clones) = cors_http_exchange(BROWSER_ORIGIN, method, &headers).await;
            assert!(response.starts_with(&format!("HTTP/1.1 {expected_status}")), "{response}");
            assert_eq!(clones, expected_clones, "{method} with {requested_method:?}");
        }
    }

    #[tokio::test]
    async fn webdav_cors_wildcard_never_allows_credentials() {
        let headers = format!(
            "Origin: {BROWSER_ORIGIN}\r\nAccess-Control-Request-Method: PROPFIND\r\nAccess-Control-Request-Headers: Authorization\r\n"
        );
        let preflight = cors_http_request(" * ", "OPTIONS", &headers).await;
        assert!(preflight.starts_with("HTTP/1.1 204"));
        assert!(preflight.contains("access-control-allow-origin: *\r\n"));
        assert!(preflight.contains("access-control-allow-headers: Authorization\r\n"));
        assert!(!preflight.contains("access-control-allow-credentials:"));
        let actual = cors_http_request("*", "PROPFIND", &format!("Origin: {BROWSER_ORIGIN}\r\n")).await;
        assert!(actual.starts_with("HTTP/1.1 401"));
        assert!(actual.contains("access-control-allow-origin: *\r\n"));
        assert!(!actual.contains("access-control-allow-credentials:"));
    }

    #[tokio::test]
    async fn webdav_cors_allowlist_trims_entries_and_matches_exactly() {
        let configuration = format!(" https://other.example , {BROWSER_ORIGIN} ");
        let headers = format!("Origin: {BROWSER_ORIGIN}\r\nAccess-Control-Request-Method: PROPFIND\r\n");
        let response = cors_http_request(&configuration, "OPTIONS", &headers).await;
        assert!(response.starts_with("HTTP/1.1 204"));
        assert!(response.contains(&format!("access-control-allow-origin: {BROWSER_ORIGIN}\r\n")));
    }

    #[tokio::test]
    async fn webdav_cors_wraps_dav_multistatus_etag_and_streamed_body() {
        let cors = WebDavCors::new(BROWSER_ORIGIN);
        let handler = memfs_handler();
        let created = dispatch_dav(chunked_request(2, 4), handler.clone(), TEST_IP, 16, Duration::from_secs(30)).await;
        assert_eq!(created.status(), StatusCode::CREATED);

        let propfind = cors
            .handle(browser_request("PROPFIND", "/"), |req| async {
                Ok(dispatch_dav(req, handler.clone(), TEST_IP, 16, Duration::from_secs(30)).await)
            })
            .await
            .expect("PROPFIND response");
        assert_eq!(propfind.status(), StatusCode::MULTI_STATUS);
        assert_browser_headers(&propfind);

        let get = cors
            .handle(browser_request("GET", "/upload.bin"), |req| async {
                Ok(dispatch_dav(req, handler.clone(), TEST_IP, 16, Duration::from_secs(30)).await)
            })
            .await
            .expect("GET response");
        assert_eq!(get.status(), StatusCode::OK);
        assert_browser_headers(&get);
        assert!(get.headers().contains_key("etag"));
        assert!(
            get.headers()[ACCESS_CONTROL_EXPOSE_HEADERS]
                .to_str()
                .expect("exposed headers")
                .contains("ETag")
        );
        assert!(get.body().size_hint().upper().is_none(), "CORS must preserve streaming");
        let data = get.into_body().collect().await.expect("stream GET body").to_bytes();
        assert_eq!(data.as_ref(), b"aaaaaaaa");

        let mut put = browser_request("PUT", "/upload.bin");
        put.headers_mut()
            .insert("if-match", HeaderValue::from_static("\"stale-etag\""));
        let conditional = cors
            .handle(put, |req| async {
                Ok(dispatch_dav(req, handler, TEST_IP, 16, Duration::from_secs(30)).await)
            })
            .await
            .expect("conditional PUT response");
        assert_eq!(conditional.status(), StatusCode::PRECONDITION_FAILED);
        assert_browser_headers(&conditional);
    }

    #[tokio::test]
    async fn webdav_cors_wraps_dispatch_body_limit_errors() {
        let cors = WebDavCors::new(BROWSER_ORIGIN);
        let mut request = chunked_request(8, 8);
        request.headers_mut().insert(ORIGIN, HeaderValue::from_static(BROWSER_ORIGIN));
        let oversized = cors
            .handle(request, |req| async {
                Ok(dispatch_dav(req, memfs_handler(), TEST_IP, 16, Duration::from_secs(30)).await)
            })
            .await
            .expect("oversized response");
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_browser_headers(&oversized);
    }

    #[tokio::test(start_paused = true)]
    async fn webdav_cors_wraps_dispatch_timeout_errors() {
        let cors = WebDavCors::new(BROWSER_ORIGIN);
        let request = Request::builder()
            .method("PUT")
            .uri("/stalled.bin")
            .header(ORIGIN, BROWSER_ORIGIN)
            .body(StalledBody)
            .expect("stalled browser request");
        let response = cors
            .handle(request, |req| async {
                Ok(dispatch_dav(req, memfs_handler(), TEST_IP, 16, Duration::from_secs(30)).await)
            })
            .await
            .expect("timeout response");
        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
        assert_browser_headers(&response);
    }

    #[tokio::test]
    async fn webdav_cors_preserves_existing_vary_on_success_and_errors() {
        let cors = WebDavCors::new(BROWSER_ORIGIN);
        for status in [
            StatusCode::OK,
            StatusCode::FORBIDDEN,
            StatusCode::NOT_FOUND,
            StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            let response = cors
                .handle(browser_request("GET", "/"), |_| async {
                    let mut response = error_response(status, "test response");
                    response
                        .headers_mut()
                        .append(VARY, HeaderValue::from_static("Accept-Encoding"));
                    Ok(response)
                })
                .await
                .expect("wrapped response");
            assert_eq!(response.status(), status);
            assert_browser_headers(&response);
            assert!(
                response
                    .headers()
                    .get_all(VARY)
                    .iter()
                    .any(|value| value == "Accept-Encoding")
            );
        }
    }

    #[test]
    fn policy_headers_drop_credentials_and_s3_auth_spoofing() {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Basic dXNlcjpwYXNzd29yZA=="));
        headers.insert(USER_AGENT, HeaderValue::from_static("webdav-client"));
        headers.insert(REFERER, HeaderValue::from_static("https://example.test/"));
        headers.insert("x-amz-content-sha256", HeaderValue::from_static("STREAMING-AWS4-HMAC-SHA256-PAYLOAD"));
        headers.insert("x-amz-signature-age", HeaderValue::from_static("0"));

        let policy_headers = policy_request_headers(&headers);

        assert_eq!(policy_headers.get(AUTHORIZATION).expect("authorization marker"), "Basic");
        assert!(
            policy_headers
                .get(AUTHORIZATION)
                .expect("authorization marker")
                .is_sensitive()
        );
        assert_eq!(policy_headers.get(USER_AGENT).expect("user agent"), "webdav-client");
        assert_eq!(policy_headers.get(REFERER).expect("referer"), "https://example.test/");
        assert!(!policy_headers.contains_key("x-amz-content-sha256"));
        assert!(!policy_headers.contains_key("x-amz-signature-age"));
    }

    /// R03-CAN-051 / R03-CAN-067 / R05-CAN-094: a chunked upload declares no
    /// Content-Length, so the limit has to hold on the bytes actually read.
    #[tokio::test]
    async fn chunked_upload_over_max_body_size_is_rejected() {
        let handler = memfs_handler();

        let resp = dispatch_dav(chunked_request(8, 8), handler.clone(), TEST_IP, 16, Duration::from_secs(30)).await;
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);

        // The oversized body must not have reached the filesystem.
        let stored = dispatch_dav(get_request("/upload.bin"), handler, TEST_IP, 16, Duration::from_secs(30)).await;
        assert_eq!(stored.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn chunked_upload_within_max_body_size_is_accepted() {
        let handler = memfs_handler();

        let resp = dispatch_dav(chunked_request(2, 4), handler.clone(), TEST_IP, 16, Duration::from_secs(30)).await;
        assert_eq!(resp.status(), StatusCode::CREATED);

        let stored = dispatch_dav(get_request("/upload.bin"), handler, TEST_IP, 16, Duration::from_secs(30)).await;
        assert_eq!(stored.status(), StatusCode::OK);
        let bytes = stored.into_body().collect().await.expect("collect body").to_bytes();
        assert_eq!(bytes.len(), 8);
    }

    /// R04-CAN-089 / R05-CAN-094: a request whose body never arrives must be
    /// cut off at the configured request timeout.
    #[tokio::test(start_paused = true)]
    async fn stalled_request_body_hits_request_timeout() {
        let req = Request::builder()
            .method("PUT")
            .uri("/stalled.bin")
            .body(StalledBody)
            .expect("build stalled request");

        let resp = timeout(
            Duration::from_secs(600),
            dispatch_dav(req, memfs_handler(), TEST_IP, 1024, Duration::from_secs(30)),
        )
        .await
        .expect("stalled body was never cut off by the configured request timeout");

        assert_eq!(resp.status(), StatusCode::REQUEST_TIMEOUT);
    }

    /// R03-CAN-052: the object body must reach Hyper as a stream. A collected
    /// body reports an exact size hint; a streamed one does not.
    #[tokio::test]
    async fn get_response_body_is_streamed_not_buffered() {
        let handler = memfs_handler();
        let put = dispatch_dav(chunked_request(4, 4), handler.clone(), TEST_IP, 1024, Duration::from_secs(30)).await;
        assert_eq!(put.status(), StatusCode::CREATED);

        let resp = dispatch_dav(get_request("/upload.bin"), handler, TEST_IP, 1024, Duration::from_secs(30)).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            resp.body().size_hint().upper().is_none(),
            "response body was collected into memory instead of streamed"
        );

        let bytes = resp.into_body().collect().await.expect("collect body").to_bytes();
        assert_eq!(bytes.len(), 16);
    }

    /// R04-CAN-089: a peer that opens a connection and never finishes its
    /// request headers must be disconnected at the configured timeout.
    #[tokio::test(start_paused = true)]
    async fn slow_request_headers_hit_request_timeout() {
        let (mut client, server) = tokio::io::duplex(1024);

        let (_shutdown_tx, shutdown) = tokio::sync::watch::channel(false);
        let conn = tokio::spawn(WebDavServer::<StubStorage>::handle_connection_impl(
            TokioIo::new(server),
            StubStorage::default(),
            TEST_IP,
            false,
            ConnectionLimits {
                max_body_size: 1024,
                request_timeout: Duration::from_secs(30),
            },
            Arc::new(WebDavCors::new("")),
            shutdown,
        ));

        client
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
            .await
            .expect("write partial headers");

        let outcome = timeout(Duration::from_secs(600), conn).await;
        assert!(outcome.is_ok(), "connection outlived the configured request timeout");
    }

    #[tokio::test]
    async fn shutdown_closes_connection_with_incomplete_first_headers() {
        use std::future::{Future, poll_fn};
        use std::task::Poll;

        let (mut client, server) = tokio::io::duplex(1024);
        let (shutdown_tx, shutdown) = watch::channel(false);
        let connection = WebDavServer::<StubStorage>::handle_connection_impl(
            TokioIo::new(server),
            StubStorage::default(),
            TEST_IP,
            false,
            ConnectionLimits {
                max_body_size: 1024,
                request_timeout: Duration::from_secs(30),
            },
            Arc::new(WebDavCors::new("")),
            shutdown,
        );
        tokio::pin!(connection);
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
            .await
            .expect("write partial headers");
        let pending = poll_fn(|cx| Poll::Ready(connection.as_mut().poll(cx).is_pending())).await;
        assert!(pending, "connection should be waiting for the rest of the first header");
        shutdown_tx.send_replace(true);

        timeout(Duration::from_secs(1), connection.as_mut())
            .await
            .expect("shutdown should not wait for an incomplete header")
            .expect("no request reached the service");
    }

    /// R05-CAN-097: the accept loop must not serve more connections at once
    /// than the configured cap.
    #[tokio::test]
    async fn accept_loop_is_bounded_by_max_connections() {
        let config = WebDavConfig {
            bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            tls_enabled: false,
            cert_dir: None,
            ca_file: None,
            max_body_size: 1024,
            request_timeout_secs: 30,
            max_connections: 1,
        };
        let server = WebDavServer::new(config, StubStorage::default()).await.expect("build server");

        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let serving = tokio::spawn(async move { server.serve(listener, shutdown_rx).await });

        // The first client occupies the single permit; keep-alive holds it
        // after the response.
        let mut first = TcpStream::connect(addr).await.expect("connect first");
        first
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("write first");
        let mut buf = [0u8; 64];
        let read = first.read(&mut buf).await.expect("read first");
        assert!(read > 0, "first connection was not served");

        // The second client lands in the backlog and must not be served.
        let mut second = TcpStream::connect(addr).await.expect("connect second");
        second
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("write second");
        let blocked = timeout(Duration::from_millis(500), second.read(&mut buf)).await;
        assert!(blocked.is_err(), "second connection was served while the cap was saturated");

        // Releasing the first permit admits the queued connection.
        drop(first);
        let served = timeout(Duration::from_secs(10), second.read(&mut buf))
            .await
            .expect("second connection was never served")
            .expect("read second");
        assert!(served > 0, "second connection returned no data");

        shutdown_tx.send(()).expect("signal server");
        timeout(Duration::from_secs(5), serving)
            .await
            .expect("server did not retire the keep-alive connection")
            .expect("server task panicked")
            .expect("server did not shut down cleanly");
        let remaining = timeout(Duration::from_secs(5), second.read_to_end(&mut Vec::new()))
            .await
            .expect("connection remained open after server exit");
        remaining.expect("read remaining response and EOF");
    }

    #[tokio::test]
    async fn config_rejects_zero_max_connections() {
        let config = WebDavConfig {
            tls_enabled: false,
            max_connections: 0,
            ..WebDavConfig::default()
        };

        let err = config.validate().await.expect_err("zero max_connections must be rejected");
        assert!(matches!(err, WebDavInitError::InvalidConfig(_)), "unexpected error: {err}");
    }
}
