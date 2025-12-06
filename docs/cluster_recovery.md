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
