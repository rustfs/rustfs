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

// Import HTTP server components and compression configuration
use crate::admin;
use crate::auth::IAMAuth;
use crate::auth_keystone;
use crate::config;
use crate::server::{
    ReadinessGateLayer, RemoteAddr, ShutdownHandle,
    compress::{HttpCompressionConfig, PathAwareHttpCompressionPredicate, PathCategoryInjectionLayer},
    hybrid::hybrid,
    layer::{
        BodylessStatusFixLayer, ConditionalCorsLayer, EmptyBodyContentLengthCompatLayer, HeadRequestBodyFixLayer,
        ObjectAttributesEtagFixLayer, PublicHealthEndpointLayer, RedirectLayer, RequestContextLayer, RequestLoggingLayer,
        S3ErrorMessageCompatLayer, VirtualHostStyleHintLayer, redact_sensitive_uri_query,
    },
    tls_material::{
        TlsAcceptorHolder, TlsHandshakeFailureKind, build_acceptor_from_loaded, load_tls_material, spawn_reload_loop,
    },
};
use crate::storage;
use crate::storage::ecstore_rpc::{TONIC_RPC_PREFIX, verify_rpc_signature};
use crate::storage::request_context::{RequestContext, extract_request_id_from_headers};
use crate::storage::rpc::InternodeRpcService;
use crate::storage::tonic_service::make_server;
use bytes::Bytes;
use http::{HeaderMap, Method, Request as HttpRequest, Response};
use hyper::body::Incoming;
use hyper_util::{
    rt::{TokioExecutor, TokioIo, TokioTimer},
    server::conn::auto::Builder as ConnBuilder,
    server::graceful::{GracefulShutdown, Watcher},
    service::TowerToHyperService,
};
use metrics::{counter, gauge, histogram};
use opentelemetry::global;
use opentelemetry::trace::TraceContextExt;
use rustfs_common::GlobalReadiness;
use rustfs_keystone::KeystoneAuthLayer;
#[cfg(feature = "swift")]
use rustfs_protocols::SwiftService;
use rustfs_protos::proto_gen::node_service::node_service_server::NodeServiceServer;
use rustfs_trusted_proxies::ClientInfo;
use rustfs_utils::net::parse_and_resolve_address;
use s3s::{host::MultiDomain, service::S3Service, service::S3ServiceBuilder};
use socket2::{SockRef, TcpKeepalive};
use std::io::{Error, Result};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tonic::{Request, Status};
use tower::{Service, ServiceBuilder};
use tower_http::add_extension::AddExtensionLayer;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::compression::CompressionLayer;
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::trace::TraceLayer;
use tracing::{Span, debug, error, info, instrument, trace, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

const LABEL_HTTP_METHOD: &str = "method";
const LABEL_HTTP_STATUS_CLASS: &str = "status_class";
const METRIC_HTTP_SERVER_REQUESTS_TOTAL: &str = "rustfs_http_server_requests_total";
const METRIC_HTTP_SERVER_FAILURES_TOTAL: &str = "rustfs_http_server_failures_total";
const METRIC_HTTP_SERVER_ACTIVE_REQUESTS: &str = "rustfs_http_server_active_requests";
const METRIC_HTTP_SERVER_REQUEST_DURATION_SECONDS: &str = "rustfs_http_server_request_duration_seconds";
const METRIC_HTTP_SERVER_REQUEST_BODY_BYTES_TOTAL: &str = "rustfs_http_server_request_body_bytes_total";
const METRIC_HTTP_SERVER_REQUEST_BODY_SIZE_BYTES: &str = "rustfs_http_server_request_body_size_bytes";
const METRIC_HTTP_SERVER_RESPONSE_BODY_BYTES_TOTAL: &str = "rustfs_http_server_response_body_bytes_total";
const METRIC_HTTP_SERVER_RESPONSE_BODY_SIZE_BYTES: &str = "rustfs_http_server_response_body_size_bytes";
const LOG_COMPONENT_SERVER: &str = "server";
const LOG_SUBSYSTEM_HTTP: &str = "http";
const LOG_SUBSYSTEM_TRANSPORT: &str = "transport";
const LOG_SUBSYSTEM_TLS: &str = "tls";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const EVENT_TLS_HANDSHAKE_FAILED: &str = "tls_handshake_failed";
const EVENT_HTTP_TRANSPORT_CLOSED: &str = "http_transport_closed";
const EVENT_HTTP_TRANSPORT_FAILED: &str = "http_transport_failed";
const EVENT_SOCKET_FALLBACK: &str = "socket_fallback";
const EVENT_HTTP_BIND_FAILED: &str = "http_bind_failed";
const EVENT_HTTP_STARTUP_ENDPOINTS: &str = "http_startup_endpoints";
const EVENT_HTTP_HOST_ROUTING: &str = "http_host_routing";
const EVENT_HTTP_COMPRESSION_STATE: &str = "http_compression_state";
const EVENT_HTTP_TRANSPORT_PARAMETERS: &str = "http_transport_parameters";
const EVENT_HTTP_ACCEPT_LOOP_STATE: &str = "http_accept_loop_state";
const EVENT_HTTP_CONNECTION_DRAIN: &str = "http_connection_drain";
const EVENT_PEER_ADDR_UNAVAILABLE: &str = "peer_addr_unavailable";
const EVENT_RPC_SIGNATURE_VERIFICATION_FAILED: &str = "rpc_signature_verification_failed";
const EVENT_GRPC_TRACE_CONTEXT_PROPAGATION_FAILED: &str = "grpc_trace_context_propagation_failed";

static ACTIVE_HTTP_REQUESTS: AtomicU64 = AtomicU64::new(0);

#[inline]
fn request_method_label(method: &Method) -> &'static str {
    match method.as_str() {
        "GET" => "GET",
        "PUT" => "PUT",
        "POST" => "POST",
        "DELETE" => "DELETE",
        "HEAD" => "HEAD",
        "OPTIONS" => "OPTIONS",
        "PATCH" => "PATCH",
        "CONNECT" => "CONNECT",
        "TRACE" => "TRACE",
        _ => "OTHER",
    }
}

#[inline]
fn status_class_label(status: http::StatusCode) -> &'static str {
    match status.as_u16() / 100 {
        1 => "1xx",
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        5 => "5xx",
        _ => "unknown",
    }
}

#[inline]
fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn log_tls_handshake_failure(peer_addr: &str, kind: TlsHandshakeFailureKind, err: &dyn std::fmt::Display) {
    match kind {
        TlsHandshakeFailureKind::UnexpectedEof => {
            debug!(
                event = EVENT_TLS_HANDSHAKE_FAILED,
                component = LOG_COMPONENT_SERVER,
                subsystem = LOG_SUBSYSTEM_TLS,
                peer_addr = %peer_addr,
                failure_type = kind.as_str(),
                error = %err,
                result = "client_disconnect",
                "TLS handshake failed"
            );
        }
        TlsHandshakeFailureKind::ProtocolVersion | TlsHandshakeFailureKind::Certificate | TlsHandshakeFailureKind::Alert => {
            warn!(
                event = EVENT_TLS_HANDSHAKE_FAILED,
                component = LOG_COMPONENT_SERVER,
                subsystem = LOG_SUBSYSTEM_TLS,
                peer_addr = %peer_addr,
                failure_type = kind.as_str(),
                error = %err,
                result = "client_error",
                "TLS handshake failed"
            );
        }
        TlsHandshakeFailureKind::Unknown => {
            error!(
                event = EVENT_TLS_HANDSHAKE_FAILED,
                component = LOG_COMPONENT_SERVER,
                subsystem = LOG_SUBSYSTEM_TLS,
                peer_addr = %peer_addr,
                failure_type = kind.as_str(),
                error = %err,
                result = "transport_error",
                "TLS handshake failed"
            );
        }
    }
}

fn log_transport_closed(peer_addr: &str, error_kind: &str, error_message: &str, result: &str) {
    debug!(
        event = EVENT_HTTP_TRANSPORT_CLOSED,
        component = LOG_COMPONENT_SERVER,
        subsystem = LOG_SUBSYSTEM_TRANSPORT,
        peer_addr = %peer_addr,
        error_kind,
        error = %error_message,
        result,
        "HTTP transport closed"
    );
}

fn log_transport_failed(peer_addr: &str, error_kind: &str, error_message: &str) {
    warn!(
        event = EVENT_HTTP_TRANSPORT_FAILED,
        component = LOG_COMPONENT_SERVER,
        subsystem = LOG_SUBSYSTEM_TRANSPORT,
        peer_addr = %peer_addr,
        error_kind,
        error = %error_message,
        result = "transport_error",
        "HTTP transport failed"
    );
}

#[inline]
fn record_active_http_requests(delta: i64) {
    let next = if delta >= 0 {
        ACTIVE_HTTP_REQUESTS.fetch_add(delta as u64, Ordering::Relaxed) + delta as u64
    } else {
        let decrement = (-delta) as u64;
        ACTIVE_HTTP_REQUESTS
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| Some(current.saturating_sub(decrement)))
            .unwrap_or_else(|current| current)
            .saturating_sub(decrement)
    };
    gauge!(METRIC_HTTP_SERVER_ACTIVE_REQUESTS).set(next as f64);
}

pub(crate) fn active_http_requests() -> u64 {
    ACTIVE_HTTP_REQUESTS.load(Ordering::Relaxed)
}

fn trace_on_response<ResBody>(response: &Response<ResBody>, latency: Duration, span: &Span) {
    span.record("status_code", tracing::field::display(response.status()));
    let _enter = span.enter();
    let status_class = status_class_label(response.status());
    record_active_http_requests(-1);
    histogram!(
        METRIC_HTTP_SERVER_REQUEST_DURATION_SECONDS,
        LABEL_HTTP_STATUS_CLASS => status_class
    )
    .record(latency.as_secs_f64());
    if response.status().is_client_error() || response.status().is_server_error() {
        counter!(
            METRIC_HTTP_SERVER_FAILURES_TOTAL,
            LABEL_HTTP_STATUS_CLASS => status_class
        )
        .increment(1);
    }
    if let Some(cl) = response.headers().get("content-length")
        && let Some(len) = cl.to_str().ok().and_then(|s| s.parse::<u64>().ok())
    {
        histogram!(
            METRIC_HTTP_SERVER_RESPONSE_BODY_SIZE_BYTES,
            LABEL_HTTP_STATUS_CLASS => status_class
        )
        .record(len as f64);
    }
}

pub async fn start_http_server(config: &config::Config, readiness: Arc<GlobalReadiness>) -> Result<(ShutdownHandle, SocketAddr)> {
    let server_addr = parse_and_resolve_address(config.address.as_str()).map_err(Error::other)?;

    // The listening address and port are obtained from the parameters
    let listener = {
        let mut server_addr = server_addr;

        // Try to create a socket for the address family; if that fails, fallback to IPv4.
        let mut socket = match socket2::Socket::new(
            socket2::Domain::for_address(server_addr),
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        ) {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    event = EVENT_SOCKET_FALLBACK,
                    component = LOG_COMPONENT_SERVER,
                    subsystem = LOG_SUBSYSTEM_STARTUP,
                    from_addr = %server_addr,
                    fallback = "ipv4",
                    error = %e,
                    "Socket creation fell back to IPv4"
                );
                let ipv4_addr = SocketAddr::new(std::net::Ipv4Addr::UNSPECIFIED.into(), server_addr.port());
                server_addr = ipv4_addr;
                socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?
            }
        };

        // If address is IPv6 try to enable dual-stack; on failure, switch to IPv4 socket.
        #[cfg(not(target_os = "openbsd"))]
        if server_addr.is_ipv6()
            && let Err(e) = socket.set_only_v6(false)
        {
            warn!(
                event = EVENT_SOCKET_FALLBACK,
                component = LOG_COMPONENT_SERVER,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                from_addr = %server_addr,
                fallback = "ipv4",
                error = %e,
                "Dual-stack socket setup fell back to IPv4"
            );
            let ipv4_addr = SocketAddr::new(std::net::Ipv4Addr::UNSPECIFIED.into(), server_addr.port());
            server_addr = ipv4_addr;
            socket = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
        }

        // Common setup for both IPv4 and successful dual-stack IPv6
        let backlog = get_listen_backlog();
        let keepalive = get_default_tcp_keepalive();

        // Helper to configure socket with optimized parameters
        let configure_socket = |socket: &socket2::Socket| -> Result<()> {
            if let Err(e) = socket.set_reuse_address(true) {
                debug!(
                    event = "socket_option_unavailable",
                    component = LOG_COMPONENT_SERVER,
                    subsystem = LOG_SUBSYSTEM_STARTUP,
                    option = "SO_REUSEADDR",
                    error = %e,
                    "Socket option is unavailable"
                );
            }

            // Set the socket to non-blocking before passing it to Tokio.
            socket.set_nonblocking(true)?;

            // 1. Disable Nagle algorithm: Critical for 4KB Payload, achieving ultra-low latency
            if let Err(e) = socket.set_tcp_nodelay(true) {
                debug!(
                    event = "socket_option_unavailable",
                    component = LOG_COMPONENT_SERVER,
                    subsystem = LOG_SUBSYSTEM_STARTUP,
                    option = "TCP_NODELAY",
                    error = %e,
                    "Socket option is unavailable"
                );
            }

            // 2. Enable SO_REUSEPORT for better multi-core scalability on supported platforms
            #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
            if let Err(e) = socket.set_reuse_port(true) {
                debug!(
                    event = "socket_option_unavailable",
                    component = LOG_COMPONENT_SERVER,
                    subsystem = LOG_SUBSYSTEM_STARTUP,
                    option = "SO_REUSEPORT",
                    error = %e,
                    "Socket option is unavailable"
                );
            }

            // 3. Set system-level TCP KeepAlive to protect long connections
            if let Err(e) = socket.set_tcp_keepalive(&keepalive) {
                debug!(
                    event = "socket_option_unavailable",
                    component = LOG_COMPONENT_SERVER,
                    subsystem = LOG_SUBSYSTEM_STARTUP,
                    option = "TCP_KEEPALIVE",
                    error = %e,
                    "Socket option is unavailable"
                );
            }

            // 4. Increase receive/send buffer to support BDP at GB-level throughput.
            // Some constrained local environments reject these socket options with
            // EPERM/ENOPROTOOPT-style failures; log and continue in that case.
            if let Err(e) = socket.set_recv_buffer_size(4 * rustfs_config::MI_B) {
                debug!(
                    event = "socket_option_unavailable",
                    component = LOG_COMPONENT_SERVER,
                    subsystem = LOG_SUBSYSTEM_STARTUP,
                    option = "SO_RCVBUF",
                    error = %e,
                    "Socket option is unavailable"
                );
            }
            if let Err(e) = socket.set_send_buffer_size(4 * rustfs_config::MI_B) {
                debug!(
                    event = "socket_option_unavailable",
                    component = LOG_COMPONENT_SERVER,
                    subsystem = LOG_SUBSYSTEM_STARTUP,
                    option = "SO_SNDBUF",
                    error = %e,
                    "Socket option is unavailable"
                );
            }

            Ok(())
        };

        configure_socket(&socket)?;

        // Attempt bind; if bind fails for IPv6, try IPv4 fallback once more.
        if let Err(bind_err) = socket.bind(&server_addr.into()) {
            warn!(
                event = EVENT_HTTP_BIND_FAILED,
                component = LOG_COMPONENT_SERVER,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                bind_addr = %server_addr,
                error = %bind_err,
                "HTTP listener bind failed"
            );
            if server_addr.is_ipv6() {
                // Try IPv4 fallback
                let ipv4_addr = SocketAddr::new(std::net::Ipv4Addr::UNSPECIFIED.into(), server_addr.port());
                server_addr = ipv4_addr;
                socket = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
                configure_socket(&socket)?;
                socket.bind(&server_addr.into())?;
                socket.listen(backlog)?;
            } else {
                return Err(bind_err);
            }
        } else {
            // Listen on the socket when initial bind succeeded
            socket.listen(backlog)?;
        }
        TcpListener::from_std(socket.into())?
    };

    let tls_path = config.tls_path.as_deref().map(str::trim).unwrap_or_default();
    let tls_path_configured = !tls_path.is_empty();
    // Load TLS materials and build server acceptor in a single pass.
    // Outbound material (root CAs, mTLS identity) was already published in main.rs;
    // this load is needed for the server-side TLS acceptor and reload loop.
    let tls_acceptor = if tls_path_configured {
        let snapshot = load_tls_material(tls_path).await.map_err(|e| {
            Error::other(format!(
                "TLS is explicitly configured via RUSTFS_TLS_PATH/tls_path='{}' but TLS acceptor initialization failed: {}",
                tls_path, e
            ))
        })?;
        let acceptor = build_acceptor_from_loaded(snapshot.server, std::path::Path::new(tls_path))
            .await
            .map_err(|e| Error::other(e.to_string()))?;

        // Fail closed: if TLS was explicitly configured but no server certificates
        // were found, refuse to start rather than silently falling back to plain HTTP.
        match acceptor {
            None => {
                return Err(Error::other(format!(
                    "TLS is explicitly configured via RUSTFS_TLS_PATH/tls_path='{}' but no server certificates were found",
                    tls_path
                )));
            }
            Some(a) => Some(a),
        }
    } else {
        None
    };
    let tls_enabled = tls_acceptor.is_some();
    let protocol = if tls_enabled { "https" } else { "http" };

    // Spawn background TLS certificate hot-reload loop (if enabled).
    if let Some(holder) = &tls_acceptor {
        spawn_reload_loop(tls_path.to_string(), holder.clone());
    }
    // Obtain the listener address
    let local_addr: SocketAddr = listener.local_addr()?;
    let local_ip = match rustfs_utils::get_local_ip() {
        Some(ip) => ip,
        None => {
            warn!(
                event = "local_ip_fallback",
                component = LOG_COMPONENT_SERVER,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                fallback_ip = %local_addr.ip(),
                "Falling back to listener IP for startup endpoint logging"
            );
            local_addr.ip()
        }
    };
    let local_port = local_addr.port();

    let local_ip_str = if local_ip.is_ipv6() {
        format!("[{local_ip}]")
    } else {
        local_ip.to_string()
    };

    // Detailed endpoint information (showing all API endpoints)
    let api_endpoints = format!("{protocol}://{local_ip_str}:{local_port}");
    let localhost_endpoint = format!("{protocol}://127.0.0.1:{local_port}");
    let now_time = jiff::Zoned::now().strftime("%Y-%m-%d %H:%M:%S").to_string();
    if config.console_enable {
        admin::console::init_console_cfg(local_ip, local_port);

        info!(
            target: "rustfs::console::startup",
            event = EVENT_HTTP_STARTUP_ENDPOINTS,
            component = LOG_COMPONENT_SERVER,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            service = "console",
            endpoint = %format!("{protocol}://{local_ip_str}:{local_port}/rustfs/console/index.html"),
            "Startup endpoint available"
        );
        info!(
            target: "rustfs::console::startup",
            event = EVENT_HTTP_STARTUP_ENDPOINTS,
            component = LOG_COMPONENT_SERVER,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            service = "console_localhost",
            endpoint = %format!("{protocol}://127.0.0.1:{local_port}/rustfs/console/index.html"),
            "Startup endpoint available"
        );
    } else {
        info!(
            target: "rustfs::main::startup",
            event = EVENT_HTTP_STARTUP_ENDPOINTS,
            component = LOG_COMPONENT_SERVER,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            service = "s3_api",
            api_endpoint = %api_endpoints,
            localhost_endpoint = %localhost_endpoint,
            started_at = %now_time,
            console_enabled = false,
            docs_url = "https://rustfs.com/docs/",
            "Startup endpoints ready"
        );
    }

    // Setup S3 service
    // This project uses the S3S library to implement S3 services
    let s3_service = {
        let store = storage::ecfs::FS::new();
        let mut b = S3ServiceBuilder::new(store.clone());

        let access_key = config.access_key.clone();
        let secret_key = config.secret_key.clone();

        b.set_auth(IAMAuth::new(access_key, secret_key));
        b.set_access(store);
        b.set_route(admin::make_admin_route(config.console_enable)?);

        // Virtual-hosted-style requests are only set up for S3 API when server domains are configured and console is disabled
        if !config.server_domains.is_empty() && !config.console_enable {
            MultiDomain::new(&config.server_domains).map_err(Error::other)?; // validate domains

            // add the default port number to the given server domains
            let mut domain_sets = std::collections::HashSet::new();
            for domain in &config.server_domains {
                domain_sets.insert(domain.to_string());
                if let Some((host, _)) = domain.split_once(':') {
                    domain_sets.insert(format!("{host}:{local_port}"));
                } else {
                    domain_sets.insert(format!("{domain}:{local_port}"));
                }
            }

            info!(
                event = EVENT_HTTP_HOST_ROUTING,
                component = LOG_COMPONENT_SERVER,
                subsystem = LOG_SUBSYSTEM_HTTP,
                state = "enabled",
                domain_count = domain_sets.len(),
                domains = ?domain_sets,
                "Virtual-hosted-style routing configured"
            );
            b.set_host(MultiDomain::new(domain_sets).map_err(Error::other)?);
        }

        b.build()
    };

    // Create shutdown channel
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1);
    // Create compression configuration from environment variables
    let compression_config = HttpCompressionConfig::from_env();
    if compression_config.enabled {
        info!(
            event = EVENT_HTTP_COMPRESSION_STATE,
            component = LOG_COMPONENT_SERVER,
            subsystem = LOG_SUBSYSTEM_HTTP,
            state = "enabled",
            extensions = ?compression_config.extensions,
            mime_patterns = ?compression_config.mime_patterns,
            min_size_bytes = compression_config.min_size,
            "HTTP response compression state changed"
        );
    } else {
        debug!(
            event = EVENT_HTTP_COMPRESSION_STATE,
            component = LOG_COMPONENT_SERVER,
            subsystem = LOG_SUBSYSTEM_HTTP,
            state = "disabled",
            "HTTP response compression state changed"
        );
    }

    let is_console = config.console_enable;
    let server_domains_configured = !config.server_domains.is_empty();
    let task_handle = tokio::spawn(async move {
        // Note: CORS layer is removed from global middleware stack
        // - S3 API CORS is handled by bucket-level CORS configuration in apply_cors_headers()
        // - Console CORS is handled by its own cors_layer in setup_console_middleware_stack()
        // This ensures S3 API CORS behavior matches AWS S3 specification

        // ── HTTP Transport Tuning (configurable via env vars) ──
        // Read all transport parameters from environment, falling back to defaults.
        // H2 frame size is clamped to RFC 7540 range: 2^14 (16KB) to 2^24 (16MB).

        let h2_stream_window = rustfs_utils::get_env_u32(
            rustfs_config::ENV_H2_INITIAL_STREAM_WINDOW_SIZE,
            rustfs_config::DEFAULT_H2_INITIAL_STREAM_WINDOW_SIZE,
        );
        let h2_conn_window = rustfs_utils::get_env_u32(
            rustfs_config::ENV_H2_INITIAL_CONN_WINDOW_SIZE,
            rustfs_config::DEFAULT_H2_INITIAL_CONN_WINDOW_SIZE,
        );
        let h2_max_frame_size =
            rustfs_utils::get_env_u32(rustfs_config::ENV_H2_MAX_FRAME_SIZE, rustfs_config::DEFAULT_H2_MAX_FRAME_SIZE)
                .clamp(16_384, 16_777_216); // RFC 7540
        let h2_max_header_list_size =
            rustfs_utils::get_env_u32(rustfs_config::ENV_H2_MAX_HEADER_LIST_SIZE, rustfs_config::DEFAULT_H2_MAX_HEADER_LIST_SIZE);
        let h2_max_concurrent_streams = rustfs_utils::get_env_u32(
            rustfs_config::ENV_H2_MAX_CONCURRENT_STREAMS,
            rustfs_config::DEFAULT_H2_MAX_CONCURRENT_STREAMS,
        )
        .max(1);
        let h2_keep_alive_interval =
            rustfs_utils::get_env_u64(rustfs_config::ENV_H2_KEEP_ALIVE_INTERVAL, rustfs_config::DEFAULT_H2_KEEP_ALIVE_INTERVAL);
        let h2_keep_alive_timeout =
            rustfs_utils::get_env_u64(rustfs_config::ENV_H2_KEEP_ALIVE_TIMEOUT, rustfs_config::DEFAULT_H2_KEEP_ALIVE_TIMEOUT);
        let http1_header_read_timeout = rustfs_utils::get_env_u64(
            rustfs_config::ENV_HTTP1_HEADER_READ_TIMEOUT,
            rustfs_config::DEFAULT_HTTP1_HEADER_READ_TIMEOUT,
        );
        let http1_max_buf_size =
            rustfs_utils::get_env_usize(rustfs_config::ENV_HTTP1_MAX_BUF_SIZE, rustfs_config::DEFAULT_HTTP1_MAX_BUF_SIZE);

        info!(
            event = EVENT_HTTP_TRANSPORT_PARAMETERS,
            component = LOG_COMPONENT_SERVER,
            subsystem = LOG_SUBSYSTEM_TRANSPORT,
            h2_stream_window,
            h2_conn_window,
            h2_max_frame_size,
            h2_max_header_list_size,
            h2_max_concurrent_streams,
            h2_keep_alive_interval_secs = h2_keep_alive_interval,
            h2_keep_alive_timeout_secs = h2_keep_alive_timeout,
            http1_header_read_timeout_secs = http1_header_read_timeout,
            http1_max_buf_size,
            "HTTP transport parameters configured"
        );

        let mut conn_builder = ConnBuilder::new(TokioExecutor::new());

        // Optimize for HTTP/1.1 (S3 small files/management plane)
        conn_builder
            .http1()
            .timer(TokioTimer::new())
            .keep_alive(true)
            .header_read_timeout(Duration::from_secs(http1_header_read_timeout))
            .max_buf_size(http1_max_buf_size)
            .writev(true);

        // Optimize for HTTP/2 (AI/Data Lake high concurrency synchronization)
        conn_builder
            .http2()
            .timer(TokioTimer::new())
            .adaptive_window(true)
            .initial_stream_window_size(h2_stream_window)
            .initial_connection_window_size(h2_conn_window)
            .max_frame_size(h2_max_frame_size)
            .max_concurrent_streams(Some(h2_max_concurrent_streams))
            .max_header_list_size(h2_max_header_list_size)
            .keep_alive_interval(Some(Duration::from_secs(h2_keep_alive_interval)))
            .keep_alive_timeout(Duration::from_secs(h2_keep_alive_timeout));

        let http_server = Arc::new(conn_builder);
        let graceful = GracefulShutdown::new();
        debug!(
            event = EVENT_HTTP_ACCEPT_LOOP_STATE,
            component = LOG_COMPONENT_SERVER,
            subsystem = LOG_SUBSYSTEM_TRANSPORT,
            state = "started",
            "HTTP accept loop started"
        );

        loop {
            trace!("Waiting for new connection");
            let (socket, _) = {
                #[cfg(unix)]
                {
                    tokio::select! {
                        res = listener.accept() => match res {
                            Ok(conn) => conn,
                            Err(err) => {
                                error!(
                                    event = EVENT_HTTP_ACCEPT_LOOP_STATE,
                                    component = LOG_COMPONENT_SERVER,
                                    subsystem = LOG_SUBSYSTEM_TRANSPORT,
                                    state = "accept_failed",
                                    error = %err,
                                    "HTTP accept loop state changed"
                                );
                                continue;
                            }
                        },
                        _ = shutdown_rx.recv() => {
                            info!(
                                event = EVENT_HTTP_ACCEPT_LOOP_STATE,
                                component = LOG_COMPONENT_SERVER,
                                subsystem = LOG_SUBSYSTEM_TRANSPORT,
                                state = "shutdown_signal_received",
                                "HTTP accept loop state changed"
                            );
                            break;
                        }
                    }
                }
                #[cfg(not(unix))]
                {
                    tokio::select! {
                        res = listener.accept() => match res {
                            Ok(conn) => conn,
                            Err(err) => {
                                error!(
                                    event = EVENT_HTTP_ACCEPT_LOOP_STATE,
                                    component = LOG_COMPONENT_SERVER,
                                    subsystem = LOG_SUBSYSTEM_TRANSPORT,
                                    state = "accept_failed",
                                    error = %err,
                                    "HTTP accept loop state changed"
                                );
                                continue;
                            }
                        },
                        _ = shutdown_rx.recv() => {
                            info!(
                                event = EVENT_HTTP_ACCEPT_LOOP_STATE,
                                component = LOG_COMPONENT_SERVER,
                                subsystem = LOG_SUBSYSTEM_TRANSPORT,
                                state = "shutdown_signal_received",
                                "HTTP accept loop state changed"
                            );
                            break;
                        }
                    }
                }
            };
            #[allow(unused)]
            let socket_ref = SockRef::from(&socket);

            // ── POST-ACCEPT SOCKET SYSCALLS ──
            // The listening socket already sets TCP_NODELAY, TCP_KEEPALIVE,
            // SO_RCVBUF, and SO_SNDBUF. On Linux/BSD, these are inherited by
            // accepted sockets, so we skip redundant re-application here.
            //
            // Only TCP_QUICKACK (Linux) is kept — it is inherently per-connection
            // and NOT inherited from the listening socket.
            //
            // T03 optimized: syscall count reduced from 5 → 1 (Linux) / 0 (other)

            // Enable TCP QuickAck to reduce latency for small requests (Linux only)
            #[cfg(target_os = "linux")]
            if let Err(err) = socket_ref.set_tcp_quickack(true) {
                debug!(?err, "Failed to set TCP_QUICKACK");
            }

            // Debug-only: verify listening socket options were inherited
            #[cfg(debug_assertions)]
            {
                debug!(
                    nodelay = socket_ref.tcp_nodelay().unwrap_or(false),
                    "TCP_NODELAY inherited from listening socket"
                );
            }

            let connection_ctx = ConnectionContext {
                http_server: http_server.clone(),
                s3_service: s3_service.clone(),
                compression_config: compression_config.clone(),
                is_console,
                server_domains_configured,
                readiness: readiness.clone(),
                keystone_auth: auth_keystone::get_keystone_auth(),
                trusted_proxy_layer: rustfs_trusted_proxies::is_enabled().then(|| rustfs_trusted_proxies::layer().clone()),
            };

            process_connection(socket, tls_acceptor.clone(), connection_ctx, graceful.watcher());
        }

        let active_connections = graceful.count();
        if active_connections > 0 {
            info!(
                event = EVENT_HTTP_CONNECTION_DRAIN,
                component = LOG_COMPONENT_SERVER,
                subsystem = LOG_SUBSYSTEM_TRANSPORT,
                state = "draining",
                active_connections,
                "HTTP connection drain started"
            );
        }
        tokio::select! {
            () = graceful.shutdown() => {
                debug!(
                    event = EVENT_HTTP_CONNECTION_DRAIN,
                    component = LOG_COMPONENT_SERVER,
                    subsystem = LOG_SUBSYSTEM_TRANSPORT,
                    state = "completed",
                    "HTTP connection drain completed"
                );
            },
            () = tokio::time::sleep(Duration::from_secs(10)) => {
                warn!(
                    event = EVENT_HTTP_CONNECTION_DRAIN,
                    component = LOG_COMPONENT_SERVER,
                    subsystem = LOG_SUBSYSTEM_TRANSPORT,
                    state = "timeout",
                    active_connections,
                    timeout_secs = 10,
                    "HTTP connection drain timed out"
                );
            }
        }
    });

    Ok((ShutdownHandle::new(shutdown_tx, task_handle), local_addr))
}

#[derive(Clone)]
struct ConnectionContext {
    http_server: Arc<ConnBuilder<TokioExecutor>>,
    s3_service: S3Service,
    compression_config: HttpCompressionConfig,
    is_console: bool,
    /// Whether `RUSTFS_SERVER_DOMAINS` is configured (i.e. s3s virtual-hosted-style routing is active).
    server_domains_configured: bool,
    readiness: Arc<GlobalReadiness>,
    /// Pre-computed Keystone auth provider (avoids per-connection OnceLock read).
    keystone_auth: Option<Arc<rustfs_keystone::KeystoneAuthProvider>>,
    /// Pre-computed trusted proxy layer (avoids per-connection is_enabled() check).
    trusted_proxy_layer: Option<rustfs_trusted_proxies::TrustedProxyLayer>,
}

#[derive(Clone)]
struct PathDispatchService<A, B> {
    external: A,
    internode: B,
}

#[derive(Clone, Default)]
struct InternodeRequestContextLiteLayer;

impl<S> tower::Layer<S> for InternodeRequestContextLiteLayer {
    type Service = InternodeRequestContextLiteService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        InternodeRequestContextLiteService { inner }
    }
}

#[derive(Clone)]
struct InternodeRequestContextLiteService<S> {
    inner: S,
}

impl<S, B> Service<HttpRequest<B>> for InternodeRequestContextLiteService<S>
where
    S: Service<HttpRequest<B>> + Clone,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: HttpRequest<B>) -> Self::Future {
        let request_id = extract_request_id_from_headers(req.headers());
        req.extensions_mut().insert(RequestContext {
            x_amz_request_id: request_id.clone(),
            request_id,
            trace_id: None,
            span_id: None,
            start_time: std::time::Instant::now(),
        });
        self.inner.call(req)
    }
}

impl<A, B> PathDispatchService<A, B> {
    fn new(external: A, internode: B) -> Self {
        Self { external, internode }
    }

    fn is_internode_path(path: &str) -> bool {
        crate::server::has_path_prefix(path, crate::server::RPC_PREFIX)
    }
}

impl<A, B> Service<HttpRequest<Incoming>> for PathDispatchService<A, B>
where
    A: Service<HttpRequest<Incoming>> + Clone + Send + 'static,
    A::Future: Send + 'static,
    B: Service<HttpRequest<Incoming>, Response = A::Response, Error = A::Error> + Clone + Send + 'static,
    B::Future: Send + 'static,
{
    type Response = A::Response;
    type Error = A::Error;
    type Future = Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        match self.external.poll_ready(cx)? {
            Poll::Ready(()) => self.internode.poll_ready(cx),
            Poll::Pending => Poll::Pending,
        }
    }

    fn call(&mut self, req: HttpRequest<Incoming>) -> Self::Future {
        if Self::is_internode_path(req.uri().path()) {
            Box::pin(self.internode.call(req))
        } else {
            Box::pin(self.external.call(req))
        }
    }
}

/// Adapter that implements the OpenTelemetry [`Extractor`] trait for Hyper's
/// [`HeaderMap`], enabling trace context propagation by extracting
/// OpenTelemetry headers from incoming HTTP requests.
pub struct HeaderMapCarrier<'a> {
    headers: &'a HeaderMap,
}

impl<'a> HeaderMapCarrier<'a> {
    pub fn new(headers: &'a HeaderMap) -> Self {
        Self { headers }
    }
}

impl<'a> opentelemetry::propagation::Extractor for HeaderMapCarrier<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.headers.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.headers.keys().map(|k| k.as_str()).collect()
    }

    fn get_all(&self, key: &str) -> Option<Vec<&str>> {
        let headers = self
            .headers
            .get_all(key)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .collect::<Vec<_>>();

        if headers.is_empty() { None } else { Some(headers) }
    }
}

/// Adapter that implements the OpenTelemetry [`Extractor`] trait for gRPC
/// metadata maps so internode gRPC requests can continue distributed traces.
struct MetadataMapCarrier<'a> {
    metadata: &'a tonic::metadata::MetadataMap,
}

impl<'a> MetadataMapCarrier<'a> {
    fn new(metadata: &'a tonic::metadata::MetadataMap) -> Self {
        Self { metadata }
    }
}

impl<'a> opentelemetry::propagation::Extractor for MetadataMapCarrier<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.metadata.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.metadata
            .keys()
            .filter_map(|key| match key {
                tonic::metadata::KeyRef::Ascii(v) => Some(v.as_str()),
                tonic::metadata::KeyRef::Binary(_) => None,
            })
            .collect()
    }

    fn get_all(&self, key: &str) -> Option<Vec<&str>> {
        let values = self
            .metadata
            .get_all(key)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .collect::<Vec<_>>();

        if values.is_empty() { None } else { Some(values) }
    }
}

/// Process a single incoming TCP connection.
///
/// This function is executed in a new Tokio task, and it will:
/// 1. If TLS is configured, perform TLS handshake.
/// 2. Build a complete service stack for this connection, including S3, RPC services, and all middleware.
/// 3. Use Hyper to handle HTTP requests on this connection.
/// 4. Incorporate connections into the management of elegant closures.
#[instrument(skip_all, fields(peer_addr = %socket.peer_addr().map(|a| a.to_string()).unwrap_or_else(|_| "unknown".to_string())
))]
fn process_connection(
    socket: TcpStream,
    tls_acceptor: Option<Arc<TlsAcceptorHolder>>,
    context: ConnectionContext,
    graceful: Watcher,
) {
    tokio::spawn(async move {
        let ConnectionContext {
            http_server,
            s3_service,
            compression_config,
            is_console,
            server_domains_configured,
            readiness,
            keystone_auth,
            trusted_proxy_layer,
        } = context;

        // Build the hybrid service per-connection.
        // Note: NodeService is not Clone (holds LocalPeerS3Client), and the SwiftService
        // type is feature-gated, so we cannot pre-build the full hybrid service.
        // The construction cost is negligible (struct wrapping only, no I/O).
        let rpc_service = NodeServiceServer::with_interceptor(make_server(), check_auth);

        #[cfg(feature = "swift")]
        let http_service = SwiftService::new(true, None, s3_service);
        #[cfg(not(feature = "swift"))]
        let http_service = s3_service;
        let http_service = InternodeRpcService::new(http_service);

        let external_service = hybrid(http_service.clone(), rpc_service.clone());
        let internode_service = hybrid(http_service, rpc_service);

        let remote_addr = match socket.peer_addr() {
            Ok(addr) => Some(RemoteAddr(addr)),
            Err(e) => {
                warn!(
                    event = EVENT_PEER_ADDR_UNAVAILABLE,
                    component = LOG_COMPONENT_SERVER,
                    subsystem = LOG_SUBSYSTEM_HTTP,
                    error = %e,
                    "Failed to obtain peer address; policy evaluation may fall back to a default source IP"
                );
                None
            }
        };
        // ── Canonical Middleware Stack Order (outermost → innermost) ──
        // This order MUST be preserved across refactorings.
        // Only AddExtensionLayer (layers 1-2) are per-connection; most remaining layers are stateless.
        //
        //  1. AddExtensionLayer<RemoteAddr>           — per-connection peer address
        //  2. AddExtensionLayer<SocketAddr>           — per-connection raw socket addr (TrustedProxy)
        //  3. TrustedProxyLayer                       — conditional, parses X-Forwarded-For
        //  4. SetRequestIdLayer                       — generates X-Request-ID
        //  5. RequestContextLayer                    — creates RequestContext in extensions
        //  6. EmptyBodyContentLengthCompatLayer       — adds Content-Length: 0 for known empty-body API routes
        //  7. CatchPanicLayer                        — panic → 500
        //  8. ReadinessGateLayer                     — blocks until ready
        //  9. KeystoneAuthLayer                      — X-Auth-Token validation
        // 10. TraceLayer                             — request span creation + metrics
        // 11. RequestLoggingLayer                    — single completion event per request
        // 12. PropagateRequestIdLayer                — X-Request-ID → response
        // 13. CompressionLayer                       — response compression (whitelist, path-aware)
        // 14. PathCategoryInjectionLayer             — injects path category for compression predicate
        // 15. S3ErrorMessageCompatLayer              — missing S3 error message compatibility
        // 16. ObjectAttributesEtagFixLayer           — ETag fix for GetObjectAttributes
        // 17. ConditionalCorsLayer                   — S3 API CORS
        // 18. RedirectLayer                          — console redirect (conditional)
        // 19. BodylessStatusFixLayer                 — clears body for 1xx/204/205/304 responses
        // 20. HeadRequestBodyFixLayer                — strips actual body bytes from HEAD responses
        // 21. PublicHealthEndpointLayer              — handles public health before s3s host parsing
        // 22. VirtualHostStyleHintLayer              — actionable error for unroutable virtual-hosted-style (conditional)
        // ─────────────────────────────────────────────────────────────
        // Batch 1 intentionally keeps the external and internode stacks behaviorally
        // identical while giving each path family a named construction boundary.
        // Later batches will trim internode-only middleware without risking drift in
        // the public HTTP stack.
        let build_external_stack = |service| {
            ServiceBuilder::new()
                // NOTE: Both extension types are intentionally inserted to maintain compatibility:
                // 1. `Option<RemoteAddr>` - Used by existing admin/storage handlers throughout the codebase
                // 2. `std::net::SocketAddr` - Required by TrustedProxyMiddleware for proxy validation
                // This dual insertion is necessary because the middleware expects the raw SocketAddr type
                // while our application code uses the RemoteAddr wrapper. Consolidating these would
                // require either modifying the third-party middleware or refactoring all existing handlers.
                .layer(AddExtensionLayer::new(remote_addr))
                .option_layer(remote_addr.map(|ra| AddExtensionLayer::new(ra.0)))
                // Add TrustedProxyLayer to handle X-Forwarded-For and other proxy headers
                // This should be placed before TraceLayer so that logs reflect the real client IP
                // Pre-computed in ConnectionContext to avoid per-connection is_enabled() check.
                .option_layer(trusted_proxy_layer.clone())
                .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
                .layer(InternodeRequestContextLiteLayer)
                .layer(EmptyBodyContentLengthCompatLayer)
                .layer(CatchPanicLayer::new())
                // CRITICAL: Insert ReadinessGateLayer before business logic
                // This stops requests from hitting IAMAuth or Storage if they are not ready.
                .layer(ReadinessGateLayer::new(readiness.clone()))
                // Add Keystone authentication middleware
                // This validates X-Auth-Token headers and stores credentials in task-local storage
                // Must be placed AFTER ReadinessGateLayer but BEFORE business logic
                // Pre-computed in ConnectionContext to avoid per-connection OnceLock read.
                .layer(KeystoneAuthLayer::new(keystone_auth.clone()))
                .layer(
                    TraceLayer::new_for_http()
                        .make_span_with(|request: &HttpRequest<_>| {
                            let request_context = request.extensions().get::<crate::storage::request_context::RequestContext>();
                            let request_id = request_context
                                .map(|ctx| ctx.request_id.as_str())
                                .unwrap_or("unknown");
                            let trace_id = request_context
                                .and_then(|ctx| ctx.trace_id.as_deref())
                                .unwrap_or("unknown");
                            let span_id = request_context
                                .and_then(|ctx| ctx.span_id.as_deref())
                                .unwrap_or("unknown");

                            let parent_context = global::get_text_map_propagator(|propagator| {
                                propagator.extract(&HeaderMapCarrier::new(request.headers()))
                            });

                            if parent_context.has_active_span() {
                                let span_ref = parent_context.span();
                                trace!(
                                    otel_trace_id = %span_ref.span_context().trace_id(),
                                    otel_parent_span_id = %span_ref.span_context().span_id(),
                                    sampled = span_ref.span_context().is_sampled(),
                                    "Extracted trace context from incoming request headers"
                                );
                            } else {
                                trace!("No trace context found in request headers, will create root span");
                            }
                            let client_info = request.extensions().get::<ClientInfo>();
                            let peer_addr = client_info
                                .map(|info| info.real_ip.to_string())
                                .or_else(|| request.extensions().get::<RemoteAddr>().map(|addr| addr.0.to_string()))
                                .unwrap_or_else(|| "unknown".to_string());

                            let span = tracing::info_span!("http-request",
                                request_id = %request_id,
                                trace_id = %trace_id,
                                span_id = %span_id,
                                status_code = tracing::field::Empty,
                                method = %request.method(),
                                peer_addr = %peer_addr,
                                uri = %redact_sensitive_uri_query(request.uri()),
                                version = ?request.version(),
                                user_agent = tracing::field::Empty,
                                content_type = tracing::field::Empty,
                                content_length = tracing::field::Empty,
                            );
                            if span.is_disabled() {
                                return span;
                            }
                            if let Err(e) = span.set_parent(parent_context) {
                                debug!(component = LOG_COMPONENT_SERVER, subsystem = LOG_SUBSYSTEM_HTTP, error = ?e, "Failed to propagate tracing context");
                            }
                            for (header_name, header_value) in request.headers() {
                                let value = header_value.to_str().unwrap_or("invalid");
                                if header_name == "user-agent" {
                                    span.record("user_agent", value);
                                } else if header_name == "content-type" {
                                    span.record("content_type", value);
                                } else if header_name == "content-length" {
                                    span.record("content_length", value);
                                }
                            }

                            span
                        })
                        .on_request(|request: &HttpRequest<_>, span: &Span| {
                            let _enter = span.enter();
                            trace!("HTTP request started");
                            let method = request_method_label(request.method());
                            record_active_http_requests(1);
                            counter!(
                                METRIC_HTTP_SERVER_REQUESTS_TOTAL,
                                LABEL_HTTP_METHOD => method
                            )
                            .increment(1);

                            if let Some(cl) = request.headers().get("content-length")
                                && let Some(len) = cl.to_str().ok().and_then(|s| s.parse::<u64>().ok())
                            {
                                counter!(METRIC_HTTP_SERVER_REQUEST_BODY_BYTES_TOTAL).increment(len);
                                histogram!(
                                    METRIC_HTTP_SERVER_REQUEST_BODY_SIZE_BYTES,
                                    LABEL_HTTP_METHOD => method
                                )
                                .record(len as f64);
                            }
                        })
                        .on_response(trace_on_response)
                        .on_body_chunk(|chunk: &Bytes, latency: Duration, span: &Span| {
                            counter!(METRIC_HTTP_SERVER_RESPONSE_BODY_BYTES_TOTAL).increment(chunk.len() as u64);
                            #[cfg(feature = "tracing-chunk-debug")]
                            {
                                let _enter = span.enter();
                                debug!(chunk_bytes = chunk.len(), duration_ms = duration_ms(latency), "HTTP response body chunk sent");
                            }
                            #[cfg(not(feature = "tracing-chunk-debug"))]
                            {
                                let _ = (latency, span);
                            }
                        })
                        .on_eos(|_trailers: Option<&HeaderMap>, stream_duration: Duration, span: &Span| {
                            #[cfg(feature = "tracing-chunk-debug")]
                            {
                                let _enter = span.enter();
                                debug!(duration_ms = duration_ms(stream_duration), "HTTP response stream closed");
                            }
                            #[cfg(not(feature = "tracing-chunk-debug"))]
                            {
                                let _ = (_trailers, stream_duration, span);
                            }
                        })
                        .on_failure(|error, latency: Duration, span: &Span| {
                            let _enter = span.enter();
                            record_active_http_requests(-1);
                            counter!(
                                METRIC_HTTP_SERVER_FAILURES_TOTAL,
                                LABEL_HTTP_STATUS_CLASS => "transport"
                            )
                            .increment(1);
                            trace!(error = ?error, duration_ms = duration_ms(latency), "HTTP request failure captured by trace layer");
                        }),
                )
                .layer(RequestLoggingLayer)
                .layer(PropagateRequestIdLayer::x_request_id())
                .layer(CompressionLayer::new().compress_when(PathAwareHttpCompressionPredicate::new(compression_config.clone())))
                .layer(PathCategoryInjectionLayer)
                .layer(S3ErrorMessageCompatLayer)
                .layer(ObjectAttributesEtagFixLayer)
                .layer(ConditionalCorsLayer::new())
                .option_layer(if is_console { Some(RedirectLayer) } else { None })
                .layer(BodylessStatusFixLayer)
                .layer(HeadRequestBodyFixLayer)
                .layer(PublicHealthEndpointLayer)
                .option_layer((!server_domains_configured && !is_console).then_some(VirtualHostStyleHintLayer))
                .service(service)
        };
        let build_internode_stack = |service| {
            ServiceBuilder::new()
                .layer(AddExtensionLayer::new(remote_addr))
                .option_layer(remote_addr.map(|ra| AddExtensionLayer::new(ra.0)))
                .option_layer(trusted_proxy_layer.clone())
                .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
                .layer(RequestContextLayer)
                .layer(EmptyBodyContentLengthCompatLayer)
                .layer(CatchPanicLayer::new())
                .layer(ReadinessGateLayer::new(readiness.clone()))
                .layer(KeystoneAuthLayer::new(keystone_auth.clone()))
                .layer(
                    TraceLayer::new_for_http()
                        .make_span_with(|request: &HttpRequest<_>| {
                            let request_context = request.extensions().get::<crate::storage::request_context::RequestContext>();
                            let request_id = request_context
                                .map(|ctx| ctx.request_id.as_str())
                                .unwrap_or("unknown");
                            let trace_id = request_context
                                .and_then(|ctx| ctx.trace_id.as_deref())
                                .unwrap_or("unknown");
                            let span_id = request_context
                                .and_then(|ctx| ctx.span_id.as_deref())
                                .unwrap_or("unknown");

                            let parent_context = global::get_text_map_propagator(|propagator| {
                                propagator.extract(&HeaderMapCarrier::new(request.headers()))
                            });

                            if parent_context.has_active_span() {
                                let span_ref = parent_context.span();
                                trace!(
                                    otel_trace_id = %span_ref.span_context().trace_id(),
                                    otel_parent_span_id = %span_ref.span_context().span_id(),
                                    sampled = span_ref.span_context().is_sampled(),
                                    "Extracted trace context from incoming request headers"
                                );
                            } else {
                                trace!("No trace context found in request headers, will create root span");
                            }
                            let client_info = request.extensions().get::<ClientInfo>();
                            let peer_addr = client_info
                                .map(|info| info.real_ip.to_string())
                                .or_else(|| request.extensions().get::<RemoteAddr>().map(|addr| addr.0.to_string()))
                                .unwrap_or_else(|| "unknown".to_string());

                            let span = tracing::info_span!("http-request",
                                request_id = %request_id,
                                trace_id = %trace_id,
                                span_id = %span_id,
                                status_code = tracing::field::Empty,
                                method = %request.method(),
                                peer_addr = %peer_addr,
                                uri = %redact_sensitive_uri_query(request.uri()),
                                version = ?request.version(),
                                user_agent = tracing::field::Empty,
                                content_type = tracing::field::Empty,
                                content_length = tracing::field::Empty,
                            );
                            if span.is_disabled() {
                                return span;
                            }
                            if let Err(e) = span.set_parent(parent_context) {
                                debug!(component = LOG_COMPONENT_SERVER, subsystem = LOG_SUBSYSTEM_HTTP, error = ?e, "Failed to propagate tracing context");
                            }
                            for (header_name, header_value) in request.headers() {
                                let value = header_value.to_str().unwrap_or("invalid");
                                if header_name == "user-agent" {
                                    span.record("user_agent", value);
                                } else if header_name == "content-type" {
                                    span.record("content_type", value);
                                } else if header_name == "content-length" {
                                    span.record("content_length", value);
                                }
                            }

                            span
                        })
                        .on_request(|request: &HttpRequest<_>, span: &Span| {
                            let _enter = span.enter();
                            trace!("HTTP request started");
                            let method = request_method_label(request.method());
                            record_active_http_requests(1);
                            counter!(
                                METRIC_HTTP_SERVER_REQUESTS_TOTAL,
                                LABEL_HTTP_METHOD => method
                            )
                            .increment(1);

                            if let Some(cl) = request.headers().get("content-length")
                                && let Some(len) = cl.to_str().ok().and_then(|s| s.parse::<u64>().ok())
                            {
                                counter!(METRIC_HTTP_SERVER_REQUEST_BODY_BYTES_TOTAL).increment(len);
                                histogram!(
                                    METRIC_HTTP_SERVER_REQUEST_BODY_SIZE_BYTES,
                                    LABEL_HTTP_METHOD => method
                                )
                                .record(len as f64);
                            }
                        })
                        .on_response(trace_on_response)
                        .on_body_chunk(|chunk: &Bytes, latency: Duration, span: &Span| {
                            counter!(METRIC_HTTP_SERVER_RESPONSE_BODY_BYTES_TOTAL).increment(chunk.len() as u64);
                            #[cfg(feature = "tracing-chunk-debug")]
                            {
                                let _enter = span.enter();
                                debug!(chunk_bytes = chunk.len(), duration_ms = duration_ms(latency), "HTTP response body chunk sent");
                            }
                            #[cfg(not(feature = "tracing-chunk-debug"))]
                            {
                                let _ = (latency, span);
                            }
                        })
                        .on_eos(|_trailers: Option<&HeaderMap>, stream_duration: Duration, span: &Span| {
                            #[cfg(feature = "tracing-chunk-debug")]
                            {
                                let _enter = span.enter();
                                debug!(duration_ms = duration_ms(stream_duration), "HTTP response stream closed");
                            }
                            #[cfg(not(feature = "tracing-chunk-debug"))]
                            {
                                let _ = (_trailers, stream_duration, span);
                            }
                        })
                        .on_failure(|error, latency: Duration, span: &Span| {
                            let _enter = span.enter();
                            record_active_http_requests(-1);
                            counter!(
                                METRIC_HTTP_SERVER_FAILURES_TOTAL,
                                LABEL_HTTP_STATUS_CLASS => "transport"
                            )
                            .increment(1);
                            trace!(error = ?error, duration_ms = duration_ms(latency), "HTTP request failure captured by trace layer");
                        }),
                )
                .layer(PropagateRequestIdLayer::x_request_id())
                .layer(CompressionLayer::new().compress_when(PathAwareHttpCompressionPredicate::new(compression_config.clone())))
                .layer(PathCategoryInjectionLayer)
                .layer(S3ErrorMessageCompatLayer)
                .layer(ObjectAttributesEtagFixLayer)
                .layer(ConditionalCorsLayer::new())
                .option_layer(if is_console { Some(RedirectLayer) } else { None })
                .layer(BodylessStatusFixLayer)
                .layer(HeadRequestBodyFixLayer)
                .layer(PublicHealthEndpointLayer)
                .option_layer((!server_domains_configured && !is_console).then_some(VirtualHostStyleHintLayer))
                .service(service)
        };
        let external_stack_service = build_external_stack(external_service);
        let internode_stack_service = build_internode_stack(internode_service);
        let hybrid_service = PathDispatchService::new(external_stack_service, internode_stack_service);

        let hybrid_service = TowerToHyperService::new(hybrid_service);

        // Decide whether to handle HTTPS or HTTP connections based on the existence of TLS Acceptor
        if let Some(holder) = tls_acceptor {
            trace!("TLS handshake start");
            let peer_addr = socket
                .peer_addr()
                .ok()
                .map_or_else(|| "unknown".to_string(), |addr| addr.to_string());
            let acceptor = holder.get();
            match acceptor.accept(socket).await {
                Ok(tls_socket) => {
                    trace!("TLS handshake successful");
                    let stream = TokioIo::new(tls_socket);
                    let conn = http_server.serve_connection(stream, hybrid_service);
                    if let Err(err) = graceful.watch(conn).await {
                        handle_connection_error(Some(peer_addr.as_str()), &*err);
                    }
                }
                Err(err) => {
                    let err_str = err.to_string();
                    let kind = TlsHandshakeFailureKind::classify(&err_str);
                    log_tls_handshake_failure(&peer_addr, kind, &err);
                    counter!("rustfs_tls_handshake_failures", &[("failure_type", kind.as_str())]).increment(1);
                    trace!(
                        peer_addr = %peer_addr,
                        error_type = %std::any::type_name_of_val(&err),
                        error_details = %err,
                        "TLS handshake failure details"
                    );

                    return;
                }
            }
            trace!("TLS handshake success");
        } else {
            trace!("HTTP connection handling start");
            let peer_addr = socket.peer_addr().ok().map(|addr| addr.to_string());
            let stream = TokioIo::new(socket);
            let conn = http_server.serve_connection(stream, hybrid_service);
            if let Err(err) = graceful.watch(conn).await {
                handle_connection_error(peer_addr.as_deref(), &*err);
            }
            trace!("HTTP connection handling finished");
        };
    });
}

/// Handles connection errors by logging them with appropriate severity
fn handle_connection_error(peer_addr: Option<&str>, err: &(dyn std::error::Error + 'static)) {
    let peer_addr = peer_addr.unwrap_or("unknown");
    let s = err.to_string();
    if s.contains("connection reset") || s.contains("broken pipe") {
        log_transport_closed(peer_addr, "connection_reset", &s, "client_disconnect");
        return;
    }

    if let Some(hyper_err) = err.downcast_ref::<hyper::Error>() {
        if hyper_err.is_incomplete_message() {
            log_transport_closed(peer_addr, "incomplete_message", &hyper_err.to_string(), "client_disconnect");
        } else if hyper_err.is_closed() {
            log_transport_closed(peer_addr, "connection_closed", &hyper_err.to_string(), "client_disconnect");
        } else if hyper_err.is_parse() {
            log_transport_failed(peer_addr, "parse_failure", &hyper_err.to_string());
        } else if hyper_err.is_user() {
            error!(
                event = EVENT_HTTP_TRANSPORT_FAILED,
                component = LOG_COMPONENT_SERVER,
                subsystem = LOG_SUBSYSTEM_TRANSPORT,
                peer_addr = %peer_addr,
                error_kind = "service_error",
                error = %hyper_err,
                result = "transport_error",
                "HTTP transport failed"
            );
        } else if hyper_err.is_canceled() {
            log_transport_closed(peer_addr, "canceled", &hyper_err.to_string(), "client_disconnect");
        } else if format!("{:?}", hyper_err).contains("HeaderTimeout") {
            log_transport_closed(peer_addr, "header_timeout", &hyper_err.to_string(), "client_timeout");
        } else {
            error!(
                event = EVENT_HTTP_TRANSPORT_FAILED,
                component = LOG_COMPONENT_SERVER,
                subsystem = LOG_SUBSYSTEM_TRANSPORT,
                peer_addr = %peer_addr,
                error_kind = "hyper_error",
                error = ?hyper_err,
                result = "transport_error",
                "HTTP transport failed"
            );
        }
    } else if let Some(io_err) = err.downcast_ref::<Error>() {
        error!(
            event = EVENT_HTTP_TRANSPORT_FAILED,
            component = LOG_COMPONENT_SERVER,
            subsystem = LOG_SUBSYSTEM_TRANSPORT,
            peer_addr = %peer_addr,
            error_kind = "io_error",
            error = %io_err,
            result = "transport_error",
            "HTTP transport failed"
        );
    } else {
        error!(
            event = EVENT_HTTP_TRANSPORT_FAILED,
            component = LOG_COMPONENT_SERVER,
            subsystem = LOG_SUBSYSTEM_TRANSPORT,
            peer_addr = %peer_addr,
            error_kind = "unknown_error",
            error = ?err,
            result = "transport_error",
            "HTTP transport failed"
        );
    }
}

#[allow(clippy::result_large_err)]
fn check_auth(req: Request<()>) -> std::result::Result<Request<()>, Status> {
    verify_rpc_signature(TONIC_RPC_PREFIX, &Method::GET, req.metadata().as_ref()).map_err(|e| {
        error!(
            event = EVENT_RPC_SIGNATURE_VERIFICATION_FAILED,
            component = LOG_COMPONENT_SERVER,
            subsystem = LOG_SUBSYSTEM_HTTP,
            error = %e,
            "RPC signature verification failed"
        );
        Status::unauthenticated("No valid auth token")
    })?;

    let parent_context =
        global::get_text_map_propagator(|propagator| propagator.extract(&MetadataMapCarrier::new(req.metadata())));
    if parent_context.has_active_span() {
        let span_ref = parent_context.span();
        debug!(
            otel_trace_id = %span_ref.span_context().trace_id(),
            otel_parent_span_id = %span_ref.span_context().span_id(),
            sampled = span_ref.span_context().is_sampled(),
            "Extracted trace context from incoming gRPC metadata"
        );
        if let Err(e) = tracing::Span::current().set_parent(parent_context) {
            warn!(
                event = EVENT_GRPC_TRACE_CONTEXT_PROPAGATION_FAILED,
                component = LOG_COMPONENT_SERVER,
                subsystem = LOG_SUBSYSTEM_HTTP,
                error = ?e,
                "Failed to propagate tracing context from gRPC metadata"
            );
        }
    }
    Ok(req)
}

/// Determines the listen backlog size.
///
/// It tries to read the system's maximum connection queue length (`somaxconn`).
/// If reading fails, it falls back to a default value (e.g., 1024).
/// This makes the backlog size adaptive to the system configuration.
#[cfg(target_os = "linux")]
fn get_listen_backlog() -> i32 {
    const DEFAULT_BACKLOG: i32 = 1024;

    // For Linux, read from /proc/sys/net/core/somaxconn
    match std::fs::read_to_string("/proc/sys/net/core/somaxconn") {
        Ok(s) => s.trim().parse().unwrap_or(DEFAULT_BACKLOG),
        Err(_) => DEFAULT_BACKLOG,
    }
}

// For macOS and BSD variants use the syscall way of getting the connection queue length.
// NetBSD has no somaxconn-like kernel state.
#[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "openbsd"))]
// SAFETY: The only unsafe operation in this function is `libc::sysctl`, called
// with kernel MIB arrays selected by target OS, a valid output buffer, and no
// input buffer.
#[allow(unsafe_code)]
fn get_listen_backlog() -> i32 {
    const DEFAULT_BACKLOG: i32 = 1024;

    #[cfg(target_os = "openbsd")]
    let mut name = [libc::CTL_KERN, libc::KERN_SOMAXCONN];
    #[cfg(any(target_os = "macos", target_os = "freebsd"))]
    let mut name = [libc::CTL_KERN, libc::KERN_IPC, libc::KIPC_SOMAXCONN];
    let mut buf = [0; 1];
    let mut buf_len = size_of_val(&buf);

    // SAFETY: `name` points to the target OS MIB, `buf` is a valid writable
    // output buffer, `buf_len` points to its size, and no input buffer is used.
    if unsafe {
        libc::sysctl(
            name.as_mut_ptr(),
            name.len() as u32,
            buf.as_mut_ptr() as *mut libc::c_void,
            &mut buf_len,
            std::ptr::null_mut(),
            0,
        )
    } != 0
    {
        return DEFAULT_BACKLOG;
    }

    buf[0]
}

// Fallback for Windows, NetBSD and other operating systems.
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "freebsd", target_os = "openbsd")))]
fn get_listen_backlog() -> i32 {
    const DEFAULT_BACKLOG: i32 = 1024;
    DEFAULT_BACKLOG
}

fn get_default_tcp_keepalive() -> TcpKeepalive {
    #[cfg(target_os = "openbsd")]
    {
        TcpKeepalive::new().with_time(Duration::from_secs(60))
    }

    #[cfg(not(target_os = "openbsd"))]
    {
        TcpKeepalive::new()
            .with_time(Duration::from_secs(60))
            .with_interval(Duration::from_secs(5))
            .with_retries(3)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::compress::RequestPathCategory;
    use bytes::Bytes;
    use http::Request as HttpRequest;
    use http::{HeaderMap, StatusCode};
    use http_body_util::{Empty, Full};
    use opentelemetry::propagation::Extractor;
    use std::convert::Infallible;
    use std::future::Ready;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use tower::{Layer, Service, ServiceBuilder};

    /// Baseline constants — reference the authoritative config defaults.
    /// If a config default changes, tests automatically follow.
    mod baseline {
        use rustfs_config::{
            DEFAULT_H2_INITIAL_CONN_WINDOW_SIZE, DEFAULT_H2_INITIAL_STREAM_WINDOW_SIZE, DEFAULT_H2_MAX_FRAME_SIZE,
            DEFAULT_H2_MAX_HEADER_LIST_SIZE, DEFAULT_HTTP1_HEADER_READ_TIMEOUT, DEFAULT_HTTP1_MAX_BUF_SIZE,
        };

        /// Number of middleware layers in the canonical stack order (see http.rs).
        /// Layers 1-2 are per-connection (AddExtension), 3-21 are stateless.
        pub const MIDDLEWARE_LAYER_COUNT: usize = 21;

        /// Current HTTP/2 defaults (from rustfs_config).
        pub const H2_INITIAL_STREAM_WINDOW_SIZE: u32 = DEFAULT_H2_INITIAL_STREAM_WINDOW_SIZE;
        pub const H2_INITIAL_CONN_WINDOW_SIZE: u32 = DEFAULT_H2_INITIAL_CONN_WINDOW_SIZE;
        pub const H2_MAX_FRAME_SIZE: u32 = DEFAULT_H2_MAX_FRAME_SIZE;
        pub const H2_MAX_HEADER_LIST_SIZE: u32 = DEFAULT_H2_MAX_HEADER_LIST_SIZE;

        /// Current HTTP/1.1 defaults (from rustfs_config).
        pub const HTTP1_HEADER_READ_TIMEOUT_SECS: u64 = DEFAULT_HTTP1_HEADER_READ_TIMEOUT;
        pub const HTTP1_MAX_BUF_SIZE: usize = DEFAULT_HTTP1_MAX_BUF_SIZE;

        /// Post-accept socket syscalls after T03 optimization.
        /// Linux: 1 (TCP_QUICKACK only). Other platforms: 0.
        #[cfg(target_os = "linux")]
        pub const POST_ACCEPT_SYSCALL_COUNT_LINUX: usize = 1;
        #[cfg(not(target_os = "linux"))]
        pub const POST_ACCEPT_SYSCALL_COUNT_OTHER: usize = 0;
    }

    #[test]
    fn test_baseline_h2_constants() {
        use rustfs_config::{
            DEFAULT_H2_INITIAL_CONN_WINDOW_SIZE, DEFAULT_H2_INITIAL_STREAM_WINDOW_SIZE, DEFAULT_H2_MAX_FRAME_SIZE,
            DEFAULT_H2_MAX_HEADER_LIST_SIZE,
        };
        assert_eq!(baseline::H2_INITIAL_STREAM_WINDOW_SIZE, DEFAULT_H2_INITIAL_STREAM_WINDOW_SIZE);
        assert_eq!(baseline::H2_INITIAL_CONN_WINDOW_SIZE, DEFAULT_H2_INITIAL_CONN_WINDOW_SIZE);
        assert_eq!(baseline::H2_MAX_FRAME_SIZE, DEFAULT_H2_MAX_FRAME_SIZE);
        assert_eq!(baseline::H2_MAX_HEADER_LIST_SIZE, DEFAULT_H2_MAX_HEADER_LIST_SIZE);
    }

    #[test]
    fn test_baseline_http1_constants() {
        use rustfs_config::{DEFAULT_HTTP1_HEADER_READ_TIMEOUT, DEFAULT_HTTP1_MAX_BUF_SIZE};
        assert_eq!(baseline::HTTP1_HEADER_READ_TIMEOUT_SECS, DEFAULT_HTTP1_HEADER_READ_TIMEOUT);
        assert_eq!(baseline::HTTP1_MAX_BUF_SIZE, DEFAULT_HTTP1_MAX_BUF_SIZE);
    }

    #[test]
    fn test_baseline_middleware_count() {
        assert_eq!(baseline::MIDDLEWARE_LAYER_COUNT, 21);
    }

    #[test]
    fn test_baseline_post_accept_syscall_count() {
        #[cfg(target_os = "linux")]
        assert_eq!(baseline::POST_ACCEPT_SYSCALL_COUNT_LINUX, 1);
        #[cfg(not(target_os = "linux"))]
        assert_eq!(baseline::POST_ACCEPT_SYSCALL_COUNT_OTHER, 0);
    }

    #[test]
    fn test_headermap_carrier_new() {
        let headers = HeaderMap::new();
        let carrier = HeaderMapCarrier::new(&headers);
        assert_eq!(carrier.keys().len(), 0);
    }

    #[test]
    fn test_headermap_carrier_get() {
        let mut headers = HeaderMap::new();
        headers.insert("user-agent", "test-agent".parse().unwrap());
        headers.insert("x-request-id", "12345".parse().unwrap());

        let carrier = HeaderMapCarrier::new(&headers);

        assert_eq!(carrier.get("user-agent"), Some("test-agent"));
        assert_eq!(carrier.get("x-request-id"), Some("12345"));
        assert_eq!(carrier.get("content-type"), None);
    }

    #[test]
    fn test_headermap_carrier_keys() {
        let mut headers = HeaderMap::new();
        headers.insert("user-agent", "test-agent".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        let carrier = HeaderMapCarrier::new(&headers);
        let keys = carrier.keys();

        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"user-agent"));
        assert!(keys.contains(&"content-type"));
    }

    #[test]
    fn test_http_metric_names_and_labels_use_snake_case() {
        let metric_names = [
            METRIC_HTTP_SERVER_REQUESTS_TOTAL,
            METRIC_HTTP_SERVER_FAILURES_TOTAL,
            METRIC_HTTP_SERVER_ACTIVE_REQUESTS,
            METRIC_HTTP_SERVER_REQUEST_DURATION_SECONDS,
            METRIC_HTTP_SERVER_REQUEST_BODY_BYTES_TOTAL,
            METRIC_HTTP_SERVER_REQUEST_BODY_SIZE_BYTES,
            METRIC_HTTP_SERVER_RESPONSE_BODY_BYTES_TOTAL,
            METRIC_HTTP_SERVER_RESPONSE_BODY_SIZE_BYTES,
        ];

        for metric_name in metric_names {
            assert!(metric_name.starts_with("rustfs_"));
            assert!(!metric_name.contains('.'));
        }

        assert_eq!(LABEL_HTTP_METHOD, "method");
        assert_eq!(LABEL_HTTP_STATUS_CLASS, "status_class");
    }

    #[test]
    fn test_headermap_carrier_get_all() {
        let mut headers = HeaderMap::new();
        headers.append("x-custom-header", "value1".parse().unwrap());
        headers.append("x-custom-header", "value2".parse().unwrap());
        headers.insert("user-agent", "test-agent".parse().unwrap());

        let carrier = HeaderMapCarrier::new(&headers);

        // Test multi-value header
        let values = carrier.get_all("x-custom-header");
        assert!(values.is_some());
        let v = values.unwrap();
        assert_eq!(v.len(), 2);
        assert!(v.contains(&"value1"));
        assert!(v.contains(&"value2"));

        // Test single value header
        let values = carrier.get_all("user-agent");
        assert!(values.is_some());
        let v = values.unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0], "test-agent");

        // Test missing header
        assert_eq!(carrier.get_all("missing-header"), None);
    }

    #[test]
    fn test_headermap_carrier_case_insensitivity() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());

        let carrier = HeaderMapCarrier::new(&headers);

        // HeaderMap::get is case insensitive
        assert_eq!(carrier.get("Content-Type"), Some("application/json"));
        assert_eq!(carrier.get("CONTENT-TYPE"), Some("application/json"));
    }

    #[derive(Clone, Copy)]
    struct ObserveCategoryLayer;

    #[derive(Clone)]
    struct ObserveCategoryService<S> {
        inner: S,
    }

    impl<S> Layer<S> for ObserveCategoryLayer {
        type Service = ObserveCategoryService<S>;

        fn layer(&self, inner: S) -> Self::Service {
            ObserveCategoryService { inner }
        }
    }

    impl<S, ReqBody, ResBody> Service<HttpRequest<ReqBody>> for ObserveCategoryService<S>
    where
        S: Service<HttpRequest<ReqBody>, Response = Response<ResBody>, Error = Infallible>,
    {
        type Response = Response<ResBody>;
        type Error = Infallible;
        type Future = Ready<std::result::Result<Response<ResBody>, Infallible>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: HttpRequest<ReqBody>) -> Self::Future {
            let response = futures::executor::block_on(self.inner.call(req)).expect("infallible");
            let mut response = response;
            let seen = response.extensions().get::<RequestPathCategory>().is_some();
            response
                .headers_mut()
                .insert("x-category-seen", if seen { "true" } else { "false" }.parse().expect("header"));
            std::future::ready(Ok(response))
        }
    }

    #[derive(Clone, Copy)]
    struct OkService;

    impl<ReqBody> Service<HttpRequest<ReqBody>> for OkService {
        type Response = Response<Empty<Bytes>>;
        type Error = Infallible;
        type Future = Ready<std::result::Result<Response<Empty<Bytes>>, Infallible>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: HttpRequest<ReqBody>) -> Self::Future {
            std::future::ready(Ok(Response::new(Empty::new())))
        }
    }

    #[derive(Clone)]
    struct MarkerService {
        name: &'static str,
        hits: Arc<Mutex<Vec<&'static str>>>,
    }

    impl MarkerService {
        fn new(name: &'static str, hits: Arc<Mutex<Vec<&'static str>>>) -> Self {
            Self { name, hits }
        }
    }

    impl<ReqBody> Service<HttpRequest<ReqBody>> for MarkerService {
        type Response = Response<Full<Bytes>>;
        type Error = Infallible;
        type Future = Ready<std::result::Result<Response<Full<Bytes>>, Infallible>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: HttpRequest<ReqBody>) -> Self::Future {
            self.hits.lock().expect("hits").push(self.name);
            std::future::ready(Ok(Response::builder()
                .status(StatusCode::OK)
                .body(Full::from(Bytes::from_static(self.name.as_bytes())))
                .expect("response")))
        }
    }

    #[test]
    fn test_service_builder_order_regression_for_response_extensions() {
        let request = HttpRequest::builder().uri("/bucket/archive.zip").body(()).expect("request");

        let mut broken_order = ServiceBuilder::new()
            .layer(PathCategoryInjectionLayer)
            .layer(ObserveCategoryLayer)
            .service(OkService);

        let broken_response = futures::executor::block_on(broken_order.call(request)).expect("response");
        assert_eq!(
            broken_response.headers().get("x-category-seen").and_then(|v| v.to_str().ok()),
            Some("false")
        );

        let request = HttpRequest::builder().uri("/bucket/archive.zip").body(()).expect("request");

        let mut fixed_order = ServiceBuilder::new()
            .layer(ObserveCategoryLayer)
            .layer(PathCategoryInjectionLayer)
            .service(OkService);

        let fixed_response = futures::executor::block_on(fixed_order.call(request)).expect("response");
        assert_eq!(
            fixed_response.headers().get("x-category-seen").and_then(|v| v.to_str().ok()),
            Some("true")
        );
    }

    #[test]
    fn path_dispatch_service_identifies_rpc_prefix() {
        let hits = Arc::new(Mutex::new(Vec::new()));
        let service =
            PathDispatchService::new(MarkerService::new("external", Arc::clone(&hits)), MarkerService::new("internode", hits));

        assert!(PathDispatchService::<MarkerService, MarkerService>::is_internode_path(&format!(
            "{}/put_file_stream",
            crate::server::RPC_PREFIX
        )));
        assert!(!PathDispatchService::<MarkerService, MarkerService>::is_internode_path(
            "/bucket/object.txt"
        ));
        assert!(!PathDispatchService::<MarkerService, MarkerService>::is_internode_path("/rustfs/rpcx"));
        let _ = service;
    }
}
