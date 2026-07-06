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

/// Stop dual-writing the JSON compatibility strings on internode metadata RPCs and send only the
/// msgpack `_bin` payloads (grpc-optimization P2-1).
///
/// Defaults to `false` (dual-write, byte-for-byte legacy behavior). This is a rollout lever, not a
/// wire-format change: it may only be enabled **after** the JSON-fallback counter
/// (`rustfs_system_network_internode_msgpack_json_fallback_total`) has read zero across a release
/// window fleet-wide, confirming every peer decodes `_bin` first. Single-env rollback. See
/// `docs/operations/internode-msgpack-json-convergence-runbook.md`.
pub const ENV_INTERNODE_RPC_MSGPACK_ONLY: &str = "RUSTFS_INTERNODE_RPC_MSGPACK_ONLY";
pub const DEFAULT_INTERNODE_RPC_MSGPACK_ONLY: bool = false;

// Compile-time invariant: dual-write by default so the base build is byte-for-byte legacy behavior.
const _: () = assert!(!DEFAULT_INTERNODE_RPC_MSGPACK_ONLY);

/// Consecutive-failure threshold after which an internode peer is marked offline (grpc-optimization
/// P3 observability).
///
/// A peer accrues a failure on each dial failure or RPC-triggered connection eviction, and flips
/// back online on the next successful dial. Drives the `rustfs_cluster_servers_offline_total` gauge
/// (parity with MinIO's `minio_cluster_servers_offline_total`). Clamped to at least 1. Defaults to 3.
pub const ENV_INTERNODE_OFFLINE_FAILURE_THRESHOLD: &str = "RUSTFS_INTERNODE_OFFLINE_FAILURE_THRESHOLD";
pub const DEFAULT_INTERNODE_OFFLINE_FAILURE_THRESHOLD: u32 = 3;

/// Prewarm internode control channels in the background at remote-disk construction, moving the
/// connect cost off the first RPC (grpc-optimization P3-1).
///
/// Best-effort and deduped per peer; failures fall through to the existing lazy connect + recovery
/// monitor. Defaults to `false` (opt-in): enabling it dials every peer at startup, and it is not yet
/// validated against a cold-start baseline.
pub const ENV_INTERNODE_PREWARM: &str = "RUSTFS_INTERNODE_PREWARM";
pub const DEFAULT_INTERNODE_PREWARM: bool = false;

/// Fast-fail (bypass) internode RPCs to a peer already marked offline, instead of paying the connect
/// timeout, letting quorum proceed sooner (grpc-optimization P3-2).
///
/// Defaults to `false` (opt-in): this touches peer routing (consistency-sensitive) and must be
/// validated with the failover bench before rollout. It does NOT change quorum. The bypass is
/// self-healing — one request per [`ENV_INTERNODE_OFFLINE_REPROBE_SECS`] is let through to recover a
/// peer even without a background monitor. Single-env rollback.
pub const ENV_INTERNODE_OFFLINE_BYPASS: &str = "RUSTFS_INTERNODE_OFFLINE_BYPASS";
pub const DEFAULT_INTERNODE_OFFLINE_BYPASS: bool = false;

/// Re-probe interval (seconds) for the offline bypass: while a peer is offline, one request is let
/// through this often to attempt recovery. Clamped to a small minimum by callers. Defaults to 5s.
pub const ENV_INTERNODE_OFFLINE_REPROBE_SECS: &str = "RUSTFS_INTERNODE_OFFLINE_REPROBE_SECS";
pub const DEFAULT_INTERNODE_OFFLINE_REPROBE_SECS: u64 = 5;

// Compile-time invariant: prewarm and offline-bypass are opt-in so the base build is unchanged.
const _: () = assert!(!DEFAULT_INTERNODE_PREWARM);
const _: () = assert!(!DEFAULT_INTERNODE_OFFLINE_BYPASS);

/// Extra attempts for idempotent, read-only/reentrant control-plane RPCs (e.g. `DiskInfo`) on
/// transient network failures, with exponential backoff (grpc-optimization P3-3).
///
/// Defaults to `0` (disabled) — retries change latency-on-failure behavior and are opt-in. **Only**
/// idempotent reads use this; write/lock RPCs (`WriteAll`/`RenameData`/`Delete*`/`Lock`/`UnLock`)
/// must never auto-retry, to preserve quorum and idempotency semantics (see `CLAUDE.md`).
pub const ENV_INTERNODE_IDEMPOTENT_READ_RETRIES: &str = "RUSTFS_INTERNODE_IDEMPOTENT_READ_RETRIES";
pub const DEFAULT_INTERNODE_IDEMPOTENT_READ_RETRIES: usize = 0;

// ── Control/bulk channel isolation (P1) ──
// Large `bytes`-carrying unary RPCs (ReadAll/WriteAll/ReadMultiple/BatchReadVersion) otherwise
// share the control-plane HTTP/2 connection with latency-sensitive lock/health RPCs; a large
// transfer can head-of-line block a lock RPC (G2/G5). When isolation is enabled these bulk RPCs
// are routed onto a separate per-peer channel pool. See grpc-optimization P1.

/// Enable control/bulk internode gRPC channel isolation.
///
/// Defaults to `false`: this touches lock-RPC transport routing (consistency-sensitive), so it
/// is opt-in and validated against a baseline before rollout. When `false`, bulk RPCs reuse the
/// control channel exactly as before, so the switch is a single-env rollback.
pub const ENV_INTERNODE_CHANNEL_ISOLATION: &str = "RUSTFS_INTERNODE_CHANNEL_ISOLATION";
pub const DEFAULT_INTERNODE_CHANNEL_ISOLATION: bool = false;

// Compile-time invariant: isolation is opt-in so the default build behaves exactly as before P1.
const _: () = assert!(!DEFAULT_INTERNODE_CHANNEL_ISOLATION);

/// Number of bulk channels maintained per peer when channel isolation is enabled.
///
/// A tonic `Channel` is a single TCP/HTTP2 connection; multiple bulk channels are round-robined
/// to relieve the single-connection throughput ceiling for large transfers. Kept small (default
/// 2) to avoid a connection storm; clamped to at least 1. Set to 1 to isolate bulk onto a single
/// dedicated connection (still separate from control).
pub const ENV_INTERNODE_BULK_CHANNELS: &str = "RUSTFS_INTERNODE_BULK_CHANNELS";
pub const DEFAULT_INTERNODE_BULK_CHANNELS: usize = 2;

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
    fn internode_channel_isolation_defaults_and_env_names() {
        // The isolation-off default is asserted at compile time next to the constant definition.
        assert_eq!(DEFAULT_INTERNODE_BULK_CHANNELS, 2);
        assert_eq!(ENV_INTERNODE_CHANNEL_ISOLATION, "RUSTFS_INTERNODE_CHANNEL_ISOLATION");
        assert_eq!(ENV_INTERNODE_BULK_CHANNELS, "RUSTFS_INTERNODE_BULK_CHANNELS");
    }

    #[test]
    fn internode_msgpack_only_env_name_is_stable() {
        // The dual-write-by-default invariant is asserted at compile time next to the definition.
        assert_eq!(ENV_INTERNODE_RPC_MSGPACK_ONLY, "RUSTFS_INTERNODE_RPC_MSGPACK_ONLY");
    }

    #[test]
    fn internode_offline_failure_threshold_defaults_and_env_name() {
        assert_eq!(DEFAULT_INTERNODE_OFFLINE_FAILURE_THRESHOLD, 3);
        assert_eq!(ENV_INTERNODE_OFFLINE_FAILURE_THRESHOLD, "RUSTFS_INTERNODE_OFFLINE_FAILURE_THRESHOLD");
    }

    #[test]
    fn internode_p3_lifecycle_defaults_are_opt_in() {
        // The opt-in (default-off) invariants are asserted at compile time next to the definitions.
        assert_eq!(DEFAULT_INTERNODE_OFFLINE_REPROBE_SECS, 5);
        assert_eq!(DEFAULT_INTERNODE_IDEMPOTENT_READ_RETRIES, 0);
        assert_eq!(ENV_INTERNODE_PREWARM, "RUSTFS_INTERNODE_PREWARM");
        assert_eq!(ENV_INTERNODE_OFFLINE_BYPASS, "RUSTFS_INTERNODE_OFFLINE_BYPASS");
        assert_eq!(ENV_INTERNODE_OFFLINE_REPROBE_SECS, "RUSTFS_INTERNODE_OFFLINE_REPROBE_SECS");
        assert_eq!(ENV_INTERNODE_IDEMPOTENT_READ_RETRIES, "RUSTFS_INTERNODE_IDEMPOTENT_READ_RETRIES");
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
