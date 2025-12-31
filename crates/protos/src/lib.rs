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

#[allow(unsafe_code)]
mod generated;

use proto_gen::node_service::node_service_client::NodeServiceClient;
use rustfs_common::{GLOBAL_CONN_MAP, GLOBAL_MTLS_IDENTITY, GLOBAL_ROOT_CERT, evict_connection};
use std::{error::Error, time::Duration};
use tonic::{
    Request, Status,
    metadata::MetadataValue,
    service::interceptor::InterceptedService,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};
use tracing::{debug, error, warn};

// Type alias for the complex client type
pub type NodeServiceClientType = NodeServiceClient<
    InterceptedService<Channel, Box<dyn Fn(Request<()>) -> Result<Request<()>, Status> + Send + Sync + 'static>>,
>;

pub use generated::*;

// Default 100 MB
pub const DEFAULT_GRPC_SERVER_MESSAGE_LEN: usize = 100 * 1024 * 1024;

/// Timeout for connection establishment - reduced for faster failure detection
const CONNECT_TIMEOUT_SECS: u64 = 3;

/// TCP keepalive interval - how often to probe the connection
const TCP_KEEPALIVE_SECS: u64 = 10;

/// HTTP/2 keepalive interval - application-layer heartbeat
const HTTP2_KEEPALIVE_INTERVAL_SECS: u64 = 5;

/// HTTP/2 keepalive timeout - how long to wait for PING ACK
const HTTP2_KEEPALIVE_TIMEOUT_SECS: u64 = 3;

/// Overall RPC timeout - maximum time for any single RPC operation
const RPC_TIMEOUT_SECS: u64 = 30;

/// Default HTTPS prefix for rustfs
/// This is the default HTTPS prefix for rustfs.
/// It is used to identify HTTPS URLs.
/// Default value: https://
const RUSTFS_HTTPS_PREFIX: &str = "https://";

/// Creates a new gRPC channel with optimized keepalive settings for cluster resilience.
///
/// This function is designed to detect dead peers quickly:
/// - Fast connection timeout (3s instead of default 30s+)
/// - Aggressive TCP keepalive (10s)
/// - HTTP/2 PING every 5s, timeout at 3s
/// - Overall RPC timeout of 30s (reduced from 60s)
async fn create_new_channel(addr: &str) -> Result<Channel, Box<dyn Error>> {
    debug!("Creating new gRPC channel to: {}", addr);

    let mut connector = Endpoint::from_shared(addr.to_string())?
        // Fast connection timeout for dead peer detection
        .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
        // TCP-level keepalive - OS will probe connection
        .tcp_keepalive(Some(Duration::from_secs(TCP_KEEPALIVE_SECS)))
        // HTTP/2 PING frames for application-layer health check
        .http2_keep_alive_interval(Duration::from_secs(HTTP2_KEEPALIVE_INTERVAL_SECS))
        // How long to wait for PING ACK before considering connection dead
        .keep_alive_timeout(Duration::from_secs(HTTP2_KEEPALIVE_TIMEOUT_SECS))
        // Send PINGs even when no active streams (critical for idle connections)
        .keep_alive_while_idle(true)
        // Overall timeout for any RPC - fail fast on unresponsive peers
        .timeout(Duration::from_secs(RPC_TIMEOUT_SECS));

    let root_cert = GLOBAL_ROOT_CERT.read().await;
    if addr.starts_with(RUSTFS_HTTPS_PREFIX) {
        if root_cert.is_none() {
            debug!("No custom root certificate configured; using system roots for TLS: {}", addr);
            // If no custom root cert is configured, try to use system roots.
            connector = connector.tls_config(ClientTlsConfig::new())?;
        }
        if let Some(cert_pem) = root_cert.as_ref() {
            let ca = Certificate::from_pem(cert_pem);
            // Derive the hostname from the HTTPS URL for TLS hostname verification.
            let domain = addr
                .trim_start_matches(RUSTFS_HTTPS_PREFIX)
                .split('/')
                .next()
                .unwrap_or("")
                .split(':')
                .next()
                .unwrap_or("");
            let tls = if !domain.is_empty() {
                let mut cfg = ClientTlsConfig::new().ca_certificate(ca).domain_name(domain);
                let mtls_identity = GLOBAL_MTLS_IDENTITY.read().await;
                if let Some(id) = mtls_identity.as_ref() {
                    let identity = tonic::transport::Identity::from_pem(id.cert_pem.clone(), id.key_pem.clone());
                    cfg = cfg.identity(identity);
                }
                cfg
            } else {
                // Fallback: configure TLS without explicit domain if parsing fails.
                ClientTlsConfig::new().ca_certificate(ca)
            };
            connector = connector.tls_config(tls)?;
            debug!("Configured TLS with custom root certificate for: {}", addr);
        } else {
            return Err(std::io::Error::other(
                "HTTPS requested but no trusted roots are configured. Provide tls/ca.crt (or enable system roots via RUSTFS_TRUST_SYSTEM_CA=true)."
            ).into());
        }
    }

    let channel = connector.connect().await?;

    // Cache the new connection
    {
        GLOBAL_CONN_MAP.write().await.insert(addr.to_string(), channel.clone());
    }

    debug!("Successfully created and cached gRPC channel to: {}", addr);
    Ok(channel)
}

/// Get a gRPC client for the NodeService with robust connection handling.
///
/// This function implements several resilience features:
/// 1. Connection caching for performance
/// 2. Automatic eviction of stale/dead connections on error
/// 3. Optimized keepalive settings for fast dead peer detection
/// 4. Reduced timeouts to fail fast when peers are unresponsive
///
/// # Connection Lifecycle
/// - Cached connections are reused for subsequent calls
/// - On any connection error, the cached connection is evicted
/// - Fresh connections are established with aggressive keepalive settings
///
/// # Cluster Power-Off Recovery
/// When a node experiences abrupt power-off:
/// 1. The cached connection will fail on next use
/// 2. The connection is automatically evicted from cache
/// 3. Subsequent calls will attempt fresh connections
/// 4. If node is still down, connection will fail fast (3s timeout)
pub async fn node_service_time_out_client(
    addr: &String,
) -> Result<
    NodeServiceClient<
        InterceptedService<Channel, Box<dyn Fn(Request<()>) -> Result<Request<()>, Status> + Send + Sync + 'static>>,
    >,
    Box<dyn Error>,
> {
    debug!("Obtaining gRPC client for NodeService at: {}", addr);
    let token_str = rustfs_credentials::get_grpc_token();
    let token: MetadataValue<_> = token_str.parse().map_err(|e| {
        error!(
            "Failed to parse gRPC auth token into MetadataValue: {:?}; env={} token_len={} token_prefix={}",
            e,
            rustfs_credentials::ENV_GRPC_AUTH_TOKEN,
            token_str.len(),
            token_str.chars().take(2).collect::<String>(),
        );
        e
    })?;

    // Try to get cached channel
    let cached_channel = { GLOBAL_CONN_MAP.read().await.get(addr).cloned() };

    let channel = match cached_channel {
        Some(channel) => {
            debug!("Using cached gRPC channel for: {}", addr);
            channel
        }
        None => {
            // No cached connection, create new one
            create_new_channel(addr).await?
        }
    };

    Ok(NodeServiceClient::with_interceptor(
        channel,
        Box::new(move |mut req: Request<()>| {
            req.metadata_mut().insert("authorization", token.clone());
            Ok(req)
        }),
    ))
}

/// Get a gRPC client with automatic connection eviction on failure.
///
/// This is the preferred method for cluster operations as it ensures
/// that failed connections are automatically cleaned up from the cache.
///
/// Returns the client and the address for later eviction if needed.
pub async fn node_service_client_with_eviction(
    addr: &String,
) -> Result<
    (
        NodeServiceClient<
            InterceptedService<Channel, Box<dyn Fn(Request<()>) -> Result<Request<()>, Status> + Send + Sync + 'static>>,
        >,
        String,
    ),
    Box<dyn Error>,
> {
    let client = node_service_time_out_client(addr).await?;
    Ok((client, addr.clone()))
}

/// Evict a connection from the cache after a failure.
/// This should be called when an RPC fails to ensure fresh connections are tried.
pub async fn evict_failed_connection(addr: &str) {
    warn!("Evicting failed gRPC connection: {}", addr);
    evict_connection(addr).await;
}
