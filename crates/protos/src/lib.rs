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
use rustfs_common::{cache_connection, cached_connection, evict_connection_with_log_level};
use std::{
    collections::HashMap,
    error::Error,
    sync::LazyLock,
    sync::atomic::{AtomicUsize, Ordering},
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

/// Class of internode gRPC channel used to route an RPC (grpc-optimization P1).
///
/// - [`ChannelClass::Control`]: latency-sensitive control-plane RPCs (locks, health, small
///   metadata). Always uses the per-peer control connection keyed by the bare address.
/// - [`ChannelClass::Bulk`]: large `bytes`-carrying unary RPCs
///   (`ReadAll`/`WriteAll`/`ReadMultiple`/`BatchReadVersion`). When channel isolation is enabled
///   these are routed onto a separate per-peer bulk connection pool so a large transfer cannot
///   head-of-line block a lock RPC on the shared HTTP/2 connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelClass {
    Control,
    Bulk,
}

/// Whether control/bulk channel isolation is enabled (env-gated, default off for safe rollout).
fn channel_isolation_enabled() -> bool {
    rustfs_utils::get_env_bool(
        rustfs_config::ENV_INTERNODE_CHANNEL_ISOLATION,
        rustfs_config::DEFAULT_INTERNODE_CHANNEL_ISOLATION,
    )
}

/// Number of bulk channels maintained per peer (>= 1).
fn bulk_channel_pool_size() -> usize {
    rustfs_utils::get_env_usize(rustfs_config::ENV_INTERNODE_BULK_CHANNELS, rustfs_config::DEFAULT_INTERNODE_BULK_CHANNELS).max(1)
}

/// Round-robin cursor for selecting a bulk channel within a peer's pool. A single global cursor
/// is sufficient: it advances once per bulk acquisition and `% pool_size` spreads consecutive
/// acquisitions across the pool.
static BULK_CHANNEL_CURSOR: AtomicUsize = AtomicUsize::new(0);

/// Connection-cache key for the `idx`-th bulk channel to `addr`. The NUL separator cannot appear
/// in a URL, so a bulk key never collides with the control key (the bare `addr`).
fn bulk_cache_key(addr: &str, idx: usize) -> String {
    format!("{addr}\u{0}bulk\u{0}{idx}")
}

/// Acquire a cached-or-newly-dialed channel for the given peer and channel class.
///
/// For [`ChannelClass::Control`] (or whenever isolation is disabled) this is exactly the legacy
/// behavior: reuse the cached control channel keyed by `addr`, else dial a new one. For
/// [`ChannelClass::Bulk`] with isolation enabled, a channel is round-robin selected from the
/// per-peer bulk pool and dialed on demand, physically isolating large transfers from
/// control-plane RPCs.
pub async fn get_channel_for_class(addr: &str, class: ChannelClass) -> Result<Channel, Box<dyn Error>> {
    if class == ChannelClass::Control || !channel_isolation_enabled() {
        if let Some(channel) = cached_connection(addr).await {
            debug!("Using cached control gRPC channel for: {}", addr);
            return Ok(channel);
        }
        return create_new_channel(addr).await;
    }

    let pool_size = bulk_channel_pool_size();
    let idx = BULK_CHANNEL_CURSOR.fetch_add(1, Ordering::Relaxed) % pool_size;
    let cache_key = bulk_cache_key(addr, idx);
    if let Some(channel) = cached_connection(&cache_key).await {
        debug!("Using cached bulk gRPC channel {} for: {}", idx, addr);
        return Ok(channel);
    }
    build_channel(addr, &cache_key).await
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
    // The control channel is cached under the bare address, preserving the legacy key.
    build_channel(addr, addr).await
}

/// Dial a new gRPC channel to `dial_addr` and cache it under `cache_key`.
///
/// `dial_addr` is the real peer URL used for the TCP/TLS connection and hostname verification;
/// `cache_key` is the connection-cache key. They are identical for control channels and differ
/// for isolated bulk channels (see [`bulk_cache_key`]), letting several physically distinct
/// channels to the same peer be cached independently.
async fn build_channel(dial_addr: &str, cache_key: &str) -> Result<Channel, Box<dyn Error>> {
    debug!("Creating new gRPC channel to: {} (cache key: {})", dial_addr, cache_key);
    let dial_started_at = Instant::now();
    let connect_timeout = internode_connect_timeout();
    let tcp_keepalive = internode_tcp_keepalive();
    let http2_keepalive_interval = internode_http2_keep_alive_interval();
    let http2_keepalive_timeout = internode_http2_keep_alive_timeout();
    let rpc_timeout = internode_rpc_timeout();

    let mut connector = Endpoint::from_shared(dial_addr.to_string())?
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
        if let Some(cached_generation) = generation_cache.get(cache_key)
            && *cached_generation != generation
        {
            stale_generation = true;
        }
    }
    if dial_addr.starts_with(RUSTFS_HTTPS_PREFIX) {
        if let Some(cert_pem) = outbound_tls.root_ca_pem.as_ref() {
            let ca = Certificate::from_pem(cert_pem);
            // Derive the hostname from the HTTPS URL for TLS hostname verification.
            let domain = dial_addr
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
            debug!("Configured TLS with custom root certificate for: {}", dial_addr);
        } else {
            // No custom root CA published — fall back to system roots.
            // This is the expected path when no TLS path is configured.
            debug!("No custom root certificate configured; using system roots for TLS: {}", dial_addr);
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

    cache_connection(cache_key.to_string(), channel.clone()).await;
    {
        let mut generation_cache = TLS_GENERATION_CACHE.lock().await;
        enforce_tls_generation_cache_bound(&mut generation_cache, generation, cache_key);
        generation_cache.insert(cache_key.to_string(), generation);
    }
    if stale_generation {
        runtime_sources::record_stale_grpc_channel_tls_generation();
    }

    debug!(
        "Successfully created and cached gRPC channel to: {} (cache key: {})",
        dial_addr, cache_key
    );
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

    // A peer failure typically affects every channel to that peer, so also drop any isolated
    // bulk channels rather than leaving a half-dead connection cached. Round-robin selection
    // means the caller cannot know which bulk index it hit, so evict the whole pool.
    if channel_isolation_enabled() {
        for idx in 0..bulk_channel_pool_size() {
            let cache_key = bulk_cache_key(addr, idx);
            evict_connection_with_log_level(&cache_key, cache_log_level).await;
            TLS_GENERATION_CACHE.lock().await.remove(&cache_key);
        }
    }
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

    #[test]
    fn bulk_cache_key_is_distinct_and_cannot_collide_with_control_key() {
        let addr = "https://node-a:9000";
        let k0 = bulk_cache_key(addr, 0);
        let k1 = bulk_cache_key(addr, 1);
        assert_ne!(k0, k1);
        assert_ne!(k0, addr);
        assert!(k0.starts_with(addr));
        // The NUL separator cannot appear in a URL, so a bulk key never equals a real address.
        assert!(k0.contains('\u{0}'));
    }

    #[test]
    fn bulk_channel_pool_size_is_at_least_one() {
        // Even without env configuration the pool size is clamped to a usable minimum.
        assert!(bulk_channel_pool_size() >= 1);
    }

    #[tokio::test]
    async fn get_channel_for_class_bulk_reuses_control_cache_when_isolation_disabled() {
        // Isolation defaults off, so a Bulk request must reuse the control channel keyed by the
        // bare address and must NOT create a separate bulk-keyed entry (zero behavior change).
        let addr = "http://get-channel-isolation-off-test";
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        cache_connection(addr.to_string(), channel).await;

        let acquired = get_channel_for_class(addr, ChannelClass::Bulk).await;
        assert!(acquired.is_ok());
        assert!(!rustfs_common::has_cached_connection(&bulk_cache_key(addr, 0)).await);

        evict_failed_connection_with_log_level(addr, ConnectionEvictionLogLevel::Debug).await;
    }
}
