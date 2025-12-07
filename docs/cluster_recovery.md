# Resolution Report: Issue #1001 - Cluster Recovery from Abrupt Power-Off

## 1. Issue Description
**Problem**: The cluster failed to recover gracefully when a node experienced an abrupt power-off (hard failure).
**Symptoms**:
-   The application became unable to upload files.
-   The Console Web UI became unresponsive across the cluster.
-   The system "hung" indefinitely, unlike the immediate recovery observed during a graceful process termination (`kill`).

**Root Cause**:
The standard TCP protocol does not immediately detect a silent peer disappearance (power loss) because no `FIN` or `RST` packets are sent. Without active application-layer heartbeats, the surviving nodes kept connections implementation in an `ESTABLISHED` state, waiting indefinitely for responses that would never arrive.

---

## 2. Technical Approach
To resolve this, we needed to transform the passive failure detection (waiting for TCP timeout) into an active detection mechanism.

### Key Objectives:
1.  **Fail Fast**: Detect dead peers in seconds, not minutes.
2.  **Accuracy**: Distinguish between network congestion and actual node failure.
3.  **Safety**: Ensure no thread or task blocks forever on a remote procedure call (RPC).

---

## 3. Implemented Solution
We modified the internal gRPC client configuration in `crates/protos/src/lib.rs` to implement a multi-layered health check strategy.

### Solution Overview
The fix implements a multi-layered detection strategy covering both Control Plane (RPC) and Data Plane (Streaming):

1.  **Control Plane (gRPC)**:
    *   Enabled `http2_keep_alive_interval` (5s) and `keep_alive_timeout` (3s) in `tonic` clients.
    *   Enforced `tcp_keepalive` (10s) on underlying transport.
    *   Context: Ensures cluster metadata operations (raft, status checks) fail fast if a node dies.

2.  **Data Plane (File Uploads/Downloads)**:
    *   **Client (Rio)**: Updated `reqwest` client builder in `crates/rio` to enable TCP Keepalive (10s) and HTTP/2 Keepalive (5s). This prevents hangs during large file streaming (e.g., 1GB uploads).
    *   **Server**: Enabled `SO_KEEPALIVE` on all incoming TCP connections in `rustfs/src/server/http.rs` to forcefully close sockets from dead clients.

3.  **Cross-Platform Build Stability**:
    *   Guarded Linux-specific profiling code (`jemalloc_pprof`) with `#[cfg(target_os = "linux")]` to fix build failures on macOS/AArch64.

### Configuration Changes

```rust
let connector = Endpoint::from_shared(addr.to_string())?
    .connect_timeout(Duration::from_secs(5))
    // 1. App-Layer Heartbeats (Primary Detection)
    // Sends a hidden HTTP/2 PING frame every 5 seconds.
    .http2_keep_alive_interval(Duration::from_secs(5))
    // If PING is not acknowledged within 3 seconds, closes connection.
    .keep_alive_timeout(Duration::from_secs(3))
    // Ensures PINGs are sent even when no active requests are in flight.
    .keep_alive_while_idle(true)
    // 2. Transport-Layer Keepalive (OS Backup)
    .tcp_keepalive(Some(Duration::from_secs(10)))
    // 3. Global Safety Net
    // Hard deadline for any RPC operation.
    .timeout(Duration::from_secs(60));
```

### Outcome
-   **Detection Time**: Reduced from ~15+ minutes (OS default) to **~8 seconds** (5s interval + 3s timeout).
-   **Behavior**: When a node loses power, surviving peers now detect the lost connection almost immediately, throwing a protocol error that triggers standard cluster recovery/failover logic.
-   **Result**: The cluster now handles power-offs with the same resilience as graceful shutdowns.
