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

/// Timeout for establishing a new internode gRPC connection.
pub const ENV_INTERNODE_CONNECT_TIMEOUT_SECS: &str = "RUSTFS_INTERNODE_CONNECT_TIMEOUT_SECS";
pub const DEFAULT_INTERNODE_CONNECT_TIMEOUT_SECS: u64 = 3;

/// TCP keepalive interval for internode gRPC channels.
pub const ENV_INTERNODE_TCP_KEEPALIVE_SECS: &str = "RUSTFS_INTERNODE_TCP_KEEPALIVE_SECS";
pub const DEFAULT_INTERNODE_TCP_KEEPALIVE_SECS: u64 = 10;

/// HTTP/2 keepalive interval for internode gRPC channels.
pub const ENV_INTERNODE_HTTP2_KEEPALIVE_INTERVAL_SECS: &str = "RUSTFS_INTERNODE_HTTP2_KEEPALIVE_INTERVAL_SECS";
pub const DEFAULT_INTERNODE_HTTP2_KEEPALIVE_INTERVAL_SECS: u64 = 5;

/// HTTP/2 keepalive timeout for internode gRPC channels.
///
/// This is the time a peer has to ACK a keepalive PING before the whole channel
/// (and every RPC/stream multiplexed on it) is torn down. A very aggressive value
/// misfires under load: on a saturated node the PING ACK is legitimately delayed,
/// the channel is wrongly declared dead, in-flight peer reads fail, and large-object
/// GETs truncate mid-stream (client "unexpected EOF"). See backlog#832. Keep this
/// generous enough to tolerate transient load while still detecting truly dead peers.
pub const ENV_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS: &str = "RUSTFS_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS";
pub const DEFAULT_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS: u64 = 20;

/// Overall timeout for a single internode gRPC request.
pub const ENV_INTERNODE_RPC_TIMEOUT_SECS: &str = "RUSTFS_INTERNODE_RPC_TIMEOUT_SECS";
pub const DEFAULT_INTERNODE_RPC_TIMEOUT_SECS: u64 = 30;

// ── Client-side internode gRPC channel tuning (P0) ──
// These mirror the server-side HTTP/2 transport tuning in `rustfs/src/server/http.rs`
// on the *client* `tonic` `Endpoint` used for internode control-plane RPCs. Prior to
// this, the client channel only set timeouts/keepalive, leaving Nagle enabled and the
// default 64KiB HTTP/2 window in place — hurting small lock-RPC latency and large
// metadata-response throughput respectively.

/// Disable Nagle's algorithm on internode gRPC client sockets.
///
/// Latency-sensitive control-plane RPCs (locks, health, small metadata) send tiny
/// frames; Nagle batching adds avoidable delay. Defaults to `true` (nodelay on),
/// matching the server socket configuration.
pub const ENV_INTERNODE_RPC_TCP_NODELAY: &str = "RUSTFS_INTERNODE_RPC_TCP_NODELAY";
pub const DEFAULT_INTERNODE_RPC_TCP_NODELAY: bool = true;

// Compile-time invariant: nodelay defaults on so latency-sensitive control-plane RPCs are not
// batched by Nagle, matching the server socket configuration.
const _: () = assert!(DEFAULT_INTERNODE_RPC_TCP_NODELAY);

/// HTTP/2 initial stream window size (bytes) for internode gRPC client channels.
///
/// The library default (64KiB) throttles larger unary responses (e.g. `ReadMultiple`,
/// `BatchReadVersion`) by the bandwidth-delay product. Set to 0 to fall back to the
/// `tonic`/`hyper` default. Defaults to 1MiB.
pub const ENV_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE: &str = "RUSTFS_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE";
pub const DEFAULT_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE: u32 = 1024 * 1024;

/// HTTP/2 initial connection window size (bytes) for internode gRPC client channels.
///
/// Should be >= the stream window so multiple concurrent streams are not starved by the
/// connection-level flow-control window. Set to 0 to fall back to the library default.
/// Defaults to 2MiB.
pub const ENV_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE: &str = "RUSTFS_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE";
pub const DEFAULT_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE: u32 = 2 * 1024 * 1024;

// Compile-time invariant: the connection-level window must be >= the per-stream window, or
// concurrent streams would be starved by the connection-level flow-control budget.
const _: () = assert!(DEFAULT_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE >= DEFAULT_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE);

/// Maximum encoded message size (bytes) for internode gRPC, applied to both the client
/// `NodeServiceClient` and the server `NodeServiceServer`.
///
/// Without this, `tonic`'s default 4MiB decode limit silently caps `bytes`-carrying
/// unary RPCs (`ReadAll`/`WriteAll`/`ReadMultiple`/`BatchReadVersion`); a large multi-version
/// `xl.meta` or an aggregated response then fails with `out_of_range`. The default comes
/// from `rustfs_protos::DEFAULT_GRPC_SERVER_MESSAGE_LEN` (100MiB) at the call sites.
pub const ENV_INTERNODE_RPC_MAX_MESSAGE_SIZE: &str = "RUSTFS_INTERNODE_RPC_MAX_MESSAGE_SIZE";

/// Payload-size threshold (bytes) above which a unary internode gRPC response counts as a
/// "large payload" for alerting.
///
/// Large `ReadAll`/`ReadMultiple` responses share the control-plane channel with
/// latency-sensitive lock/health RPCs and can head-of-line block them (see grpc-optimization
/// G2). This threshold drives the `rustfs_system_network_internode_operation_large_payloads_total`
/// counter so operators can size which paths need channel isolation in P1. Defaults to 8MiB.
pub const ENV_INTERNODE_RPC_LARGE_PAYLOAD_WARN_BYTES: &str = "RUSTFS_INTERNODE_RPC_LARGE_PAYLOAD_WARN_BYTES";
pub const DEFAULT_INTERNODE_RPC_LARGE_PAYLOAD_WARN_BYTES: usize = 8 * 1024 * 1024;

/// Profile selector for conservative internode HTTP data-plane client tuning.
pub const ENV_INTERNODE_HTTP_TUNING_PROFILE: &str = "RUSTFS_INTERNODE_HTTP_TUNING_PROFILE";
pub const DEFAULT_INTERNODE_HTTP_TUNING_PROFILE: &str = "legacy";

/// Internode HTTP connection pool maximum idle connections per host.
pub const ENV_INTERNODE_HTTP_POOL_MAX_IDLE_PER_HOST: &str = "RUSTFS_INTERNODE_HTTP_POOL_MAX_IDLE_PER_HOST";

/// Internode HTTP connection pool idle timeout in seconds.
pub const ENV_INTERNODE_HTTP_POOL_IDLE_TIMEOUT_SECS: &str = "RUSTFS_INTERNODE_HTTP_POOL_IDLE_TIMEOUT_SECS";

/// Internode HTTP/2 initial stream window size in bytes.
pub const ENV_INTERNODE_HTTP2_INITIAL_STREAM_WINDOW_SIZE: &str = "RUSTFS_INTERNODE_HTTP2_INITIAL_STREAM_WINDOW_SIZE";

/// Internode HTTP/2 initial connection window size in bytes.
pub const ENV_INTERNODE_HTTP2_INITIAL_CONNECTION_WINDOW_SIZE: &str = "RUSTFS_INTERNODE_HTTP2_INITIAL_CONNECTION_WINDOW_SIZE";

/// Whether internode HTTP/2 adaptive window sizing is enabled.
pub const ENV_INTERNODE_HTTP2_ADAPTIVE_WINDOW: &str = "RUSTFS_INTERNODE_HTTP2_ADAPTIVE_WINDOW";

/// Internode HTTP proxy mode: legacy, off, or system.
pub const ENV_INTERNODE_HTTP_PROXY: &str = "RUSTFS_INTERNODE_HTTP_PROXY";

/// Environment variable for selecting the internode data-plane transport backend.
pub const ENV_RUSTFS_INTERNODE_DATA_TRANSPORT: &str = "RUSTFS_INTERNODE_DATA_TRANSPORT";
pub const DEFAULT_INTERNODE_DATA_TRANSPORT: &str = "tcp-http";

/// Legacy alias for "tcp-http". Both values select the TCP/HTTP transport backend.
pub const INTERNODE_DATA_TRANSPORT_TCP: &str = "tcp";

/// Known internode transport backend names accepted by the config parser.
pub const KNOWN_INTERNODE_DATA_TRANSPORT_BACKENDS: &[&str] = &[DEFAULT_INTERNODE_DATA_TRANSPORT, INTERNODE_DATA_TRANSPORT_TCP];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internode_timeout_defaults_stay_in_expected_bounds() {
        assert_eq!(DEFAULT_INTERNODE_CONNECT_TIMEOUT_SECS, 3);
        assert_eq!(DEFAULT_INTERNODE_TCP_KEEPALIVE_SECS, 10);
        assert_eq!(DEFAULT_INTERNODE_HTTP2_KEEPALIVE_INTERVAL_SECS, 5);
        assert_eq!(DEFAULT_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS, 20);
        assert_eq!(DEFAULT_INTERNODE_RPC_TIMEOUT_SECS, 30);
        assert_eq!(DEFAULT_INTERNODE_HTTP_TUNING_PROFILE, "legacy");
    }

    #[test]
    fn internode_rpc_channel_tuning_defaults() {
        assert_eq!(DEFAULT_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE, 1024 * 1024);
        assert_eq!(DEFAULT_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE, 2 * 1024 * 1024);
        // The nodelay-on and connection-window >= stream-window invariants are enforced at
        // compile time next to the constant definitions.
    }

    #[test]
    fn internode_rpc_channel_tuning_env_names_are_stable() {
        assert_eq!(ENV_INTERNODE_RPC_TCP_NODELAY, "RUSTFS_INTERNODE_RPC_TCP_NODELAY");
        assert_eq!(
            ENV_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE,
            "RUSTFS_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE"
        );
        assert_eq!(ENV_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE, "RUSTFS_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE");
        assert_eq!(ENV_INTERNODE_RPC_MAX_MESSAGE_SIZE, "RUSTFS_INTERNODE_RPC_MAX_MESSAGE_SIZE");
        assert_eq!(
            ENV_INTERNODE_RPC_LARGE_PAYLOAD_WARN_BYTES,
            "RUSTFS_INTERNODE_RPC_LARGE_PAYLOAD_WARN_BYTES"
        );
        assert_eq!(DEFAULT_INTERNODE_RPC_LARGE_PAYLOAD_WARN_BYTES, 8 * 1024 * 1024);
    }

    #[test]
    fn internode_timeout_env_names_are_stable() {
        assert_eq!(ENV_INTERNODE_CONNECT_TIMEOUT_SECS, "RUSTFS_INTERNODE_CONNECT_TIMEOUT_SECS");
        assert_eq!(ENV_INTERNODE_TCP_KEEPALIVE_SECS, "RUSTFS_INTERNODE_TCP_KEEPALIVE_SECS");
        assert_eq!(
            ENV_INTERNODE_HTTP2_KEEPALIVE_INTERVAL_SECS,
            "RUSTFS_INTERNODE_HTTP2_KEEPALIVE_INTERVAL_SECS"
        );
        assert_eq!(
            ENV_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS,
            "RUSTFS_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS"
        );
        assert_eq!(ENV_INTERNODE_RPC_TIMEOUT_SECS, "RUSTFS_INTERNODE_RPC_TIMEOUT_SECS");
        assert_eq!(ENV_INTERNODE_HTTP_TUNING_PROFILE, "RUSTFS_INTERNODE_HTTP_TUNING_PROFILE");
        assert_eq!(ENV_INTERNODE_HTTP_POOL_MAX_IDLE_PER_HOST, "RUSTFS_INTERNODE_HTTP_POOL_MAX_IDLE_PER_HOST");
        assert_eq!(ENV_INTERNODE_HTTP_POOL_IDLE_TIMEOUT_SECS, "RUSTFS_INTERNODE_HTTP_POOL_IDLE_TIMEOUT_SECS");
        assert_eq!(
            ENV_INTERNODE_HTTP2_INITIAL_STREAM_WINDOW_SIZE,
            "RUSTFS_INTERNODE_HTTP2_INITIAL_STREAM_WINDOW_SIZE"
        );
        assert_eq!(
            ENV_INTERNODE_HTTP2_INITIAL_CONNECTION_WINDOW_SIZE,
            "RUSTFS_INTERNODE_HTTP2_INITIAL_CONNECTION_WINDOW_SIZE"
        );
        assert_eq!(ENV_INTERNODE_HTTP2_ADAPTIVE_WINDOW, "RUSTFS_INTERNODE_HTTP2_ADAPTIVE_WINDOW");
        assert_eq!(ENV_INTERNODE_HTTP_PROXY, "RUSTFS_INTERNODE_HTTP_PROXY");
        assert_eq!(ENV_RUSTFS_INTERNODE_DATA_TRANSPORT, "RUSTFS_INTERNODE_DATA_TRANSPORT");
        assert_eq!(DEFAULT_INTERNODE_DATA_TRANSPORT, "tcp-http");
        assert_eq!(INTERNODE_DATA_TRANSPORT_TCP, "tcp");
        assert_eq!(KNOWN_INTERNODE_DATA_TRANSPORT_BACKENDS, &["tcp-http", "tcp"]);
    }
}
