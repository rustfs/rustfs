# Resolution Report: Issue #1001 - Cluster Recovery from Abrupt Power-Off

## 1. Issue Description
**Problem**: The cluster failed to recover gracefully when a node experienced an abrupt power-off (hard failure).
**Symptoms**:
-   The application became unable to upload files.
-   The Console Web UI became unresponsive across the cluster.
-   The `rustfsadmin` user was unable to log in after a server power-off.
-   The performance page displayed 0 storage, 0 objects, and 0 servers online/offline.
-   The system "hung" indefinitely, unlike the immediate recovery observed during a graceful process termination (`kill`).

**Root Cause (Multi-Layered)**:
1. **TCP Connection Issue**: The standard TCP protocol does not immediately detect a silent peer disappearance (power loss) because no `FIN` or `RST` packets are sent.
2. **Stale Connection Cache**: Cached gRPC connections in `GLOBAL_Conn_Map` were reused even when the peer was dead, causing blocking on every RPC call.
3. **Blocking IAM Notifications**: Login operations blocked waiting for ALL peers to acknowledge user/policy changes.
4. **No Per-Peer Timeouts**: Console aggregation calls like `server_info()` and `storage_info()` could hang waiting for dead peers.

---

## 2. Technical Approach
To resolve this, we implemented a comprehensive multi-layered resilience strategy.

### Key Objectives:
1.  **Fail Fast**: Detect dead peers in seconds, not minutes.
2.  **Evict Stale Connections**: Automatically remove dead connections from cache to force reconnection.
3.  **Non-Blocking Operations**: Auth and IAM operations should not wait for dead peers.
4.  **Graceful Degradation**: Console should show partial data from healthy nodes, not hang.

---

## 3. Implemented Solution

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
pub async fn storage_info<S: StorageAPI>(&self, api: &S) -> rustfs_madmin::StorageInfo {
    let peer_timeout = Duration::from_secs(2);
    
    for client in self.peer_clients.iter() {
        futures.push(async move {
            if let Some(client) = client {
                match timeout(peer_timeout, client.local_storage_info()).await {
                    Ok(Ok(info)) => Some(info),
                    Ok(Err(_)) | Err(_) => {
                        // Return offline status for dead peer
                        Some(rustfs_madmin::StorageInfo {
                            disks: get_offline_disks(&host, &endpoints),
                            ..Default::default()
                        })
                    }
                }
            }
        });
    }
    // Rest continues even if some peers are down
}
```

### Fix 4: Enhanced gRPC Client Configuration

**File Modified**: `crates/protos/src/lib.rs`

**Configuration**:
```rust
const CONNECT_TIMEOUT_SECS: u64 = 3;      // Reduced from 5s
const TCP_KEEPALIVE_SECS: u64 = 10;        // OS-level keepalive
const HTTP2_KEEPALIVE_INTERVAL_SECS: u64 = 5; // HTTP/2 PING interval
const HTTP2_KEEPALIVE_TIMEOUT_SECS: u64 = 3;  // PING ACK timeout
const RPC_TIMEOUT_SECS: u64 = 30;          // Reduced from 60s

let connector = Endpoint::from_shared(addr.to_string())?
    .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
    .tcp_keepalive(Some(Duration::from_secs(TCP_KEEPALIVE_SECS)))
    .http2_keep_alive_interval(Duration::from_secs(HTTP2_KEEPALIVE_INTERVAL_SECS))
    .keep_alive_timeout(Duration::from_secs(HTTP2_KEEPALIVE_TIMEOUT_SECS))
    .keep_alive_while_idle(true)
    .timeout(Duration::from_secs(RPC_TIMEOUT_SECS));
```

---

## 4. Files Changed Summary

| File | Change |
|------|--------|
| `crates/common/src/globals.rs` | Added `evict_connection()`, `has_cached_connection()`, `clear_all_connections()` |
| `crates/common/Cargo.toml` | Added `tracing` dependency |
| `crates/protos/src/lib.rs` | Refactored to use constants, added `evict_failed_connection()`, improved documentation |
| `crates/protos/Cargo.toml` | Added `tracing` dependency |
| `crates/ecstore/src/rpc/peer_rest_client.rs` | Added auto-eviction on RPC failure for `server_info()` and `local_storage_info()` |
| `crates/ecstore/src/notification_sys.rs` | Added per-peer timeout to `storage_info()` |
| `crates/iam/src/sys.rs` | Made `notify_for_user()`, `notify_for_service_account()`, `notify_for_group()` non-blocking |

---

## 5. Test Results

All 299 tests pass:
```
test result: ok. 299 passed; 0 failed; 0 ignored
```

---

## 6. Expected Behavior After Fix

| Scenario | Before | After |
|----------|--------|-------|
| Node power-off | Cluster hangs indefinitely | Cluster recovers in ~8 seconds |
| Login during node failure | Login hangs | Login succeeds immediately |
| Console during node failure | Shows 0/0/0 | Shows partial data from healthy nodes |
| Upload during node failure | Upload stops | Upload fails fast, can be retried |
| Stale cached connection | Blocks forever | Auto-evicted, fresh connection attempted |

---

## 7. Verification Steps

1. **Start a 3+ node RustFS cluster**
2. **Test Console Recovery**:
   - Access console dashboard
   - Forcefully kill one node (e.g., `kill -9`)
   - Verify dashboard updates within 10 seconds showing offline status
3. **Test Login Recovery**:
   - Kill a node while logged out
   - Attempt login with `rustfsadmin`
   - Verify login succeeds within 5 seconds
4. **Test Upload Recovery**:
   - Start a large file upload
   - Kill the target node mid-upload
   - Verify upload fails fast (not hangs) and can be retried

---

## 8. Related Issues
- Issue #1001: Cluster Recovery from Abrupt Power-Off
- PR #1035: fix(net): resolve 1GB upload hang and macos build

## 9. Contributors
- Initial keepalive fix: Original PR #1035
- Deep-rooted reliability fix: This update
