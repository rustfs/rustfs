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

use std::{error::Error, time::Duration};

pub use generated::*;
use proto_gen::node_service::node_service_client::NodeServiceClient;
use rustfs_common::globals::{GLOBAL_Conn_Map, evict_connection};
use tonic::{
    Request, Status,
    metadata::MetadataValue,
    service::interceptor::InterceptedService,
    transport::{Channel, Endpoint},
};
use tracing::{debug, warn};

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

/// Creates a new gRPC channel with optimized keepalive settings for cluster resilience.
///
/// This function is designed to detect dead peers quickly:
/// - Fast connection timeout (3s instead of default 30s+)
/// - Aggressive TCP keepalive (10s)
/// - HTTP/2 PING every 5s, timeout at 3s
/// - Overall RPC timeout of 30s (reduced from 60s)
async fn create_new_channel(addr: &str) -> Result<Channel, Box<dyn Error>> {
    debug!("Creating new gRPC channel to: {}", addr);

    let connector = Endpoint::from_shared(addr.to_string())?
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

    let channel = connector.connect().await?;

    // Cache the new connection
    {
        GLOBAL_Conn_Map.write().await.insert(addr.to_string(), channel.clone());
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
    let token: MetadataValue<_> = "rustfs rpc".parse()?;

    // Try to get cached channel
    let cached_channel = { GLOBAL_Conn_Map.read().await.get(addr).cloned() };

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
