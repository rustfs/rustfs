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

// SAFETY: `generated` is prost/tonic-generated protocol code. The allowance is
// scoped to that module so generated internals do not relax lints elsewhere.
#[allow(unsafe_code)]
mod generated;
mod runtime_sources;

use proto_gen::node_service::node_service_client::NodeServiceClient;
use rustfs_common::{cache_connection, evict_connection_with_log_level};
use std::{
    collections::HashMap,
    error::Error,
    sync::LazyLock,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tonic::{
    Request, Status,
    service::interceptor::InterceptedService,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};
use tracing::{debug, info, warn};

// Type alias for the complex client type
pub type NodeServiceClientType = NodeServiceClient<
    InterceptedService<Channel, Box<dyn Fn(Request<()>) -> Result<Request<()>, Status> + Send + Sync + 'static>>,
>;

pub use generated::*;
pub use rustfs_common::ConnectionEvictionLogLevel;

// Default 100 MB
pub const DEFAULT_GRPC_SERVER_MESSAGE_LEN: usize = 100 * 1024 * 1024;

/// Default HTTPS prefix for rustfs
/// This is the default HTTPS prefix for rustfs.
/// It is used to identify HTTPS URLs.
/// Default value: https://
const RUSTFS_HTTPS_PREFIX: &str = "https://";
const TLS_GENERATION_CACHE_MAX_SIZE: usize = 512;
static TLS_GENERATION_CACHE: LazyLock<Mutex<HashMap<String, u64>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn enforce_tls_generation_cache_bound(generation_cache: &mut HashMap<String, u64>, generation: u64, addr: &str) {
    if generation_cache.len() < TLS_GENERATION_CACHE_MAX_SIZE || generation_cache.contains_key(addr) {
        return;
    }

    generation_cache.retain(|_, g| *g == generation);
    if generation_cache.len() >= TLS_GENERATION_CACHE_MAX_SIZE
        && let Some(victim) = generation_cache.keys().next().cloned()
    {
        generation_cache.remove(&victim);
    }
}

fn internode_connect_timeout() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_INTERNODE_CONNECT_TIMEOUT_SECS,
        rustfs_config::DEFAULT_INTERNODE_CONNECT_TIMEOUT_SECS,
    ))
}

fn internode_tcp_keepalive() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_INTERNODE_TCP_KEEPALIVE_SECS,
        rustfs_config::DEFAULT_INTERNODE_TCP_KEEPALIVE_SECS,
    ))
}

fn internode_http2_keep_alive_interval() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_INTERNODE_HTTP2_KEEPALIVE_INTERVAL_SECS,
        rustfs_config::DEFAULT_INTERNODE_HTTP2_KEEPALIVE_INTERVAL_SECS,
    ))
}

fn internode_http2_keep_alive_timeout() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS,
        rustfs_config::DEFAULT_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS,
    ))
}

fn internode_rpc_timeout() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_INTERNODE_RPC_TIMEOUT_SECS,
        rustfs_config::DEFAULT_INTERNODE_RPC_TIMEOUT_SECS,
    ))
}

fn internode_rpc_tcp_nodelay() -> bool {
    rustfs_utils::get_env_bool(
        rustfs_config::ENV_INTERNODE_RPC_TCP_NODELAY,
        rustfs_config::DEFAULT_INTERNODE_RPC_TCP_NODELAY,
    )
}

/// HTTP/2 initial stream window size for the client channel, or `None` to use the
/// library default (a configured value of `0` opts out).
fn internode_rpc_http2_stream_window() -> Option<u32> {
    match rustfs_utils::get_env_u32(
        rustfs_config::ENV_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE,
        rustfs_config::DEFAULT_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE,
    ) {
        0 => None,
        v => Some(v),
    }
}

/// HTTP/2 initial connection window size for the client channel, or `None` to use the
/// library default (a configured value of `0` opts out).
fn internode_rpc_http2_conn_window() -> Option<u32> {
    match rustfs_utils::get_env_u32(
        rustfs_config::ENV_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE,
        rustfs_config::DEFAULT_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE,
    ) {
        0 => None,
        v => Some(v),
    }
}

/// Maximum encoded/decoded internode gRPC message size (bytes), shared by the client
/// `NodeServiceClient` and server `NodeServiceServer`. Defaults to
/// [`DEFAULT_GRPC_SERVER_MESSAGE_LEN`] (100 MiB) when the env var is unset.
pub fn internode_rpc_max_message_size() -> usize {
    rustfs_utils::get_env_usize(rustfs_config::ENV_INTERNODE_RPC_MAX_MESSAGE_SIZE, DEFAULT_GRPC_SERVER_MESSAGE_LEN)
}

/// Creates a new gRPC channel with optimized keepalive settings for cluster resilience.
///
/// This function is designed to detect dead peers quickly using env-configurable
/// internode transport settings. Defaults come from `rustfs_config` constants:
/// - Connect timeout: `DEFAULT_INTERNODE_CONNECT_TIMEOUT_SECS` (3s)
/// - TCP keepalive: `DEFAULT_INTERNODE_TCP_KEEPALIVE_SECS` (10s)
/// - HTTP/2 keepalive interval: `DEFAULT_INTERNODE_HTTP2_KEEPALIVE_INTERVAL_SECS` (5s)
/// - HTTP/2 keepalive timeout: `DEFAULT_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS` (3s)
/// - RPC timeout: `DEFAULT_INTERNODE_RPC_TIMEOUT_SECS` (10s)
pub async fn create_new_channel(addr: &str) -> Result<Channel, Box<dyn Error>> {
    debug!("Creating new gRPC channel to: {}", addr);
    let dial_started_at = Instant::now();
    let connect_timeout = internode_connect_timeout();
    let tcp_keepalive = internode_tcp_keepalive();
    let http2_keepalive_interval = internode_http2_keep_alive_interval();
    let http2_keepalive_timeout = internode_http2_keep_alive_timeout();
    let rpc_timeout = internode_rpc_timeout();

    let mut connector = Endpoint::from_shared(addr.to_string())?
        // Fast connection timeout for dead peer detection
        .connect_timeout(connect_timeout)
        // TCP-level keepalive - OS will probe connection
        .tcp_keepalive(Some(tcp_keepalive))
        // Disable Nagle so latency-sensitive control-plane RPCs (locks/health) are not batched
        .tcp_nodelay(internode_rpc_tcp_nodelay())
        // HTTP/2 PING frames for application-layer health check
        .http2_keep_alive_interval(http2_keepalive_interval)
        // How long to wait for PING ACK before considering connection dead
        .keep_alive_timeout(http2_keepalive_timeout)
        // Send PINGs even when no active streams (critical for idle connections)
        .keep_alive_while_idle(true)
        // Overall timeout for any RPC - fail fast on unresponsive peers
        .timeout(rpc_timeout);

    // Raise HTTP/2 flow-control windows above the 64KiB library default so larger unary
    // responses (ReadMultiple/BatchReadVersion) are not throttled by the BDP. Mirrors the
    // server-side window tuning in `rustfs/src/server/http.rs`.
    if let Some(stream_window) = internode_rpc_http2_stream_window() {
        connector = connector.initial_stream_window_size(stream_window);
    }
    if let Some(conn_window) = internode_rpc_http2_conn_window() {
        connector = connector.initial_connection_window_size(conn_window);
    }

    let outbound_tls = runtime_sources::outbound_tls_state().await;
    let generation = outbound_tls.generation.0;
    let mut stale_generation = false;
    {
        let generation_cache = TLS_GENERATION_CACHE.lock().await;
        if let Some(cached_generation) = generation_cache.get(addr)
            && *cached_generation != generation
        {
            stale_generation = true;
        }
    }
    if addr.starts_with(RUSTFS_HTTPS_PREFIX) {
        if let Some(cert_pem) = outbound_tls.root_ca_pem.as_ref() {
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
                if let Some(id) = outbound_tls.mtls_identity.as_ref() {
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
            // No custom root CA published — fall back to system roots.
            // This is the expected path when no TLS path is configured.
            debug!("No custom root certificate configured; using system roots for TLS: {}", addr);
            connector = connector.tls_config(ClientTlsConfig::new())?;
        }
    }

    let channel = match connector.connect().await {
        Ok(channel) => {
            runtime_sources::record_grpc_dial_result(dial_started_at.elapsed(), true);
            channel
        }
        Err(err) => {
            runtime_sources::record_grpc_dial_result(dial_started_at.elapsed(), false);
            return Err(err.into());
        }
    };

    cache_connection(addr.to_string(), channel.clone()).await;
    {
        let mut generation_cache = TLS_GENERATION_CACHE.lock().await;
        enforce_tls_generation_cache_bound(&mut generation_cache, generation, addr);
        generation_cache.insert(addr.to_string(), generation);
    }
    if stale_generation {
        runtime_sources::record_stale_grpc_channel_tls_generation();
    }

    debug!("Successfully created and cached gRPC channel to: {}", addr);
    Ok(channel)
}

/// Evict a connection from the cache after a failure.
/// This should be called when an RPC fails to ensure fresh connections are tried.
pub async fn evict_failed_connection(addr: &str) {
    evict_failed_connection_with_log_level(addr, ConnectionEvictionLogLevel::Warn).await;
}

/// Evict a connection from the cache after a failure with an explicit log level.
pub async fn evict_failed_connection_with_log_level(addr: &str, log_level: ConnectionEvictionLogLevel) {
    match log_level {
        ConnectionEvictionLogLevel::Warn => {
            warn!(
                addr = %addr,
                "Evicting cached gRPC connection after RPC failure; the next request will attempt to reconnect automatically"
            );
        }
        ConnectionEvictionLogLevel::Info => {
            info!(
                addr = %addr,
                "Evicting cached gRPC connection after RPC failure; the next request will attempt to reconnect automatically"
            );
        }
        ConnectionEvictionLogLevel::Debug => {
            debug!(
                addr = %addr,
                "Evicting cached gRPC connection after RPC failure; the next request will attempt to reconnect automatically"
            );
        }
    }

    let cache_log_level = match log_level {
        ConnectionEvictionLogLevel::Warn => ConnectionEvictionLogLevel::Info,
        level => level,
    };
    evict_connection_with_log_level(addr, cache_log_level).await;
    TLS_GENERATION_CACHE.lock().await.remove(addr);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enforce_tls_generation_cache_bound_evicts_when_retained_entries_still_full() {
        let mut cache = HashMap::new();
        for i in 0..TLS_GENERATION_CACHE_MAX_SIZE {
            cache.insert(format!("node-{i}"), 42);
        }

        enforce_tls_generation_cache_bound(&mut cache, 42, "new-node");
        assert_eq!(cache.len(), TLS_GENERATION_CACHE_MAX_SIZE - 1);
    }

    #[tokio::test]
    async fn evict_failed_connection_with_log_level_removes_cached_connection() {
        let addr = "http://evict-failed-connection-debug-test";
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        cache_connection(addr.to_string(), channel).await;

        evict_failed_connection_with_log_level(addr, ConnectionEvictionLogLevel::Debug).await;

        assert!(!rustfs_common::has_cached_connection(addr).await);
    }
}
