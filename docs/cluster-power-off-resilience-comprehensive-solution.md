# Comprehensive Solution for Cluster Power-Off Resilience

## Executive Summary

This document provides a complete architectural analysis and solution for the critical reliability issue where a RustFS distributed storage cluster becomes unresponsive when a node experiences an abrupt power-off (hard failure), despite gracefully handling process-level terminations (kill signals).

**Problem Statement**: When one node in a 4-node RustFS cluster is abruptly powered off, the entire cluster becomes unresponsive - file uploads fail, the Console Web UI hangs, and administrative operations time out. This behavior differs significantly from graceful process termination, which the cluster handles correctly.

**Status**: Previous attempts (#1035, #1054) to address this issue through HTTP/2 keepalive configurations have been implemented but the issue persists, indicating deeper architectural challenges that require a comprehensive multi-layered solution.

---

## Table of Contents

1. [Root Cause Analysis](#root-cause-analysis)
2. [Architectural Overview](#architectural-overview)
3. [Detailed Problem Breakdown](#detailed-problem-breakdown)
4. [Comprehensive Solution Strategy](#comprehensive-solution-strategy)
5. [Implementation Details](#implementation-details)
6. [Testing & Validation](#testing--validation)
7. [Monitoring & Observability](#monitoring--observability)
8. [Operational Recommendations](#operational-recommendations)
9. [Troubleshooting Guide](#troubleshooting-guide)
10. [Future Enhancements](#future-enhancements)

---

## Root Cause Analysis

### Primary Causes

#### 1. TCP Connection State Management

**The Fundamental Problem:**
When a node experiences abrupt power-off, the TCP connections to that node remain in an established state on peer nodes. The TCP protocol does not immediately detect the peer's disappearance because:

- No `FIN` (finish) packet is sent during power-off
- No `RST` (reset) packet is sent during power-off
- The OS network stack on surviving nodes has no immediate indication the peer is gone
- TCP keepalive probes (if enabled) may take minutes to detect failure

**Why Graceful Kill Works Differently:**
When a process is killed gracefully:
- The OS sends `FIN` packets to close all open connections
- Peers receive immediate notification that connections are closed
- Connection pools can evict dead connections immediately
- New connections fail fast with connection refused errors

#### 2. Connection Caching Without Liveness Detection

The `GLOBAL_Conn_Map` in `crates/common/src/globals.rs` caches gRPC channels for performance. However:

- Cached connections are reused without proactive liveness checks
- When a cached connection is dead, RPC calls block waiting for TCP timeout
- Dead connections are evicted only after explicit errors occur (reactive, not proactive)
- During the blocking period, all operations waiting on that connection hang

#### 3. Blocking Cluster Coordination Operations

**IAM Notification Synchronization:**
Before recent fixes, IAM operations (login, user creation) attempted to synchronously notify all peers. If any peer is unresponsive, this blocks the entire login operation.

**Current Status**: This has been partially fixed with `tokio::spawn` for fire-and-forget notifications, but may not cover all cases.

#### 4. Console Aggregation Operations

The Console Web UI aggregates data from all nodes:

- `storage_info()` - Collects disk statistics from all nodes
- `server_info()` - Collects server properties from all nodes
- Performance metrics - Aggregates real-time metrics

**Problem**: If any peer times out, the entire aggregation can hang or return incomplete data, making the UI appear frozen.

**Current Status**: Timeout wrappers exist (2s per peer) but may be insufficient for worst-case scenarios.

#### 5. Data Path Streaming Operations

During large file uploads (1GB+):

- Multiple chunks are streamed across the cluster for erasure coding
- Each chunk may go to different nodes
- If a target node experiences power-off mid-upload:
  - The HTTP/2 stream hangs waiting for acknowledgment
  - Without aggressive keepalive, detection can take 10-20 seconds or more
  - The entire upload operation appears frozen

---

## Architectural Overview

### RustFS Cluster Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Client Applications                       │
│              (S3 API / Console Web UI)                      │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ├── HTTP/S3 API (Upload/Download)
                 ├── gRPC (Internal Cluster Communication)
                 └── WebSocket (Console Real-time Updates)
                 │
    ┌────────────┴────────────┐
    │                         │
┌───▼────────┐    ┌──────────▼───┐    ┌────────────┐    ┌────────────┐
│  Node 1    │◄───┤   Node 2     │◄───┤  Node 3    │◄───┤  Node 4    │
│  (Leader)  │───►│  (Follower)  │───►│ (Follower) │───►│ (Follower) │
└─────┬──────┘    └──────┬───────┘    └─────┬──────┘    └─────┬──────┘
      │                  │                   │                  │
   ┌──┴──┐            ┌──┴──┐            ┌──┴──┐           ┌──┴──┐
   │Disk1│            │Disk1│            │Disk1│           │Disk1│
   │Disk2│            │Disk2│            │Disk2│           │Disk2│
   │Disk3│            │Disk3│            │Disk3│           │Disk3│
   │Disk4│            │Disk4│            │Disk4│           │Disk4│
   └─────┘            └─────┘            └─────┘           └─────┘
```

### Communication Layers

#### Control Plane (gRPC)
- **Purpose**: Cluster coordination, metadata operations, IAM synchronization
- **Protocol**: HTTP/2 over TCP with tonic
- **Configuration Location**: `crates/protos/src/lib.rs`
- **Critical Operations**:
  - User/policy synchronization (`load_user`, `load_policy`)
  - Bucket metadata (`load_bucket_metadata`)
  - Server health checks (`server_info`, `local_storage_info`)

#### Data Plane (HTTP)
- **Purpose**: File upload/download, streaming operations
- **Protocol**: HTTP/1.1 or HTTP/2 over TCP
- **Configuration Location**: Various (needs comprehensive review)
- **Critical Operations**:
  - Multipart upload chunking
  - Erasure-coded data distribution
  - Cross-node data streaming

#### Management Plane (Console)
- **Purpose**: Web UI for monitoring and administration
- **Protocol**: HTTP + WebSocket
- **Configuration Location**: `rustfs/src/admin/` and `rustfs/src/server/`
- **Critical Operations**:
  - Dashboard data aggregation
  - Real-time performance metrics
  - Administrative commands

---

## Detailed Problem Breakdown

### Scenario: Node 3 Abrupt Power-Off During 1GB Upload

#### Timeline of Failure

**T=0s: Upload Begins**
- Client initiates 1GB file upload via S3 API
- RustFS receives upload, calculates erasure coding distribution
- File is split into chunks distributed across all 4 nodes

**T=5s: Node 3 Powers Off**
- Node 3 loses power without warning
- TCP connections remain in ESTABLISHED state on other nodes
- No FIN or RST packets sent
- Cached gRPC channels in `GLOBAL_Conn_Map` remain "valid"

**T=5s-10s: Initial Impact**
- Chunks destined for Node 3 begin to hang
- Upload progress stalls
- Client sees upload progress freeze at partial completion

**T=10s-20s: Cascading Effects**
- Upload handler waits for Node 3 acknowledgment
- No timeout triggers (or timeout is too long)
- Console UI attempts to refresh server info
- `server_info()` call to Node 3 hangs
- Console becomes unresponsive
- New login attempts may hang if IAM notification tries to reach Node 3

**T=20s+: Complete Cluster Hang**
- Multiple operations blocked waiting for Node 3
- Connection pool exhaustion possible
- User perceives entire cluster as down
- Performance page shows 0 servers online (aggregation incomplete)

### Why Previous Fixes Were Insufficient

#### Fix Attempt #1035
**Changes Made:**
- Added HTTP/2 keepalive to tonic channels
- Enabled TCP keepalive

**Why It Helped But Didn't Fully Solve:**
- Keepalive detection takes 5-10 seconds
- During that 5-10s window, operations still block
- Some code paths may bypass the keepalive-enabled channels:
  - Direct HTTP client usage in data plane operations (file uploads/downloads)
  - Admin/console handlers that create their own connections
  - Background workers that might cache old client instances
- Eviction logic may not be triggered fast enough
- **Action Required**: Audit all network client creation to ensure consistent keepalive configuration

#### Fix Attempt #1054
**Changes Made:**
- Further refinement of keepalive settings
- Connection eviction on error

**Why It Still Wasn't Enough:**
- Timeout values may still be too generous for user-facing operations
- Not all operation types have timeout protection
- Large file uploads may exceed all timeout values
- Race conditions in connection eviction logic

---

## Comprehensive Solution Strategy

### Design Principles

1. **Defense in Depth**: Multiple layers of failure detection and recovery
2. **Fail Fast**: Detect failures in seconds, not minutes
3. **Graceful Degradation**: Partial functionality is better than complete failure
4. **Non-Blocking Operations**: Critical operations should not wait for unresponsive peers
5. **Observable Failures**: Clear logging and metrics for failure scenarios
6. **Self-Healing**: Automatic recovery without operator intervention

### Solution Layers

#### Layer 1: Aggressive Connection Management
- **Goal**: Detect dead connections within 3-5 seconds
- **Mechanisms**: HTTP/2 PING, TCP keepalive, connection timeouts

#### Layer 2: Operation-Specific Timeouts
- **Goal**: No single operation hangs indefinitely
- **Mechanisms**: Per-operation timeout wrappers, circuit breakers

#### Layer 3: Quorum-Based Operations
- **Goal**: Continue with majority of healthy nodes
- **Mechanisms**: Partial response acceptance, degraded mode operation

#### Layer 4: Monitoring & Alerting
- **Goal**: Operators understand cluster health in real-time
- **Mechanisms**: Metrics, structured logging, health endpoints

---

## Implementation Details

### Layer 1: Enhanced Connection Management

#### A. gRPC Channel Configuration (CURRENT)

**File**: `crates/protos/src/lib.rs`

**Current Implementation:**
```rust
const CONNECT_TIMEOUT_SECS: u64 = 3;
const TCP_KEEPALIVE_SECS: u64 = 10;
const HTTP2_KEEPALIVE_INTERVAL_SECS: u64 = 5;
const HTTP2_KEEPALIVE_TIMEOUT_SECS: u64 = 3;
const RPC_TIMEOUT_SECS: u64 = 30;
```

**Analysis:**
These settings are reasonable but may need further tuning:

- **HTTP2_KEEPALIVE_INTERVAL_SECS: 5s** - Sends PING every 5 seconds
- **HTTP2_KEEPALIVE_TIMEOUT_SECS: 3s** - Waits 3 seconds for PING ACK
- **Total Detection Time**: ~8 seconds in worst case (5s + 3s)

**Recommendation:**
Consider more aggressive settings for latency-sensitive operations:

```rust
// For latency-sensitive operations (login, console queries)
const FAST_CONNECT_TIMEOUT_SECS: u64 = 2;
const FAST_HTTP2_KEEPALIVE_INTERVAL_SECS: u64 = 3;
const FAST_HTTP2_KEEPALIVE_TIMEOUT_SECS: u64 = 2;
const FAST_RPC_TIMEOUT_SECS: u64 = 10;

// For bulk data operations (large uploads)
const BULK_CONNECT_TIMEOUT_SECS: u64 = 3;
const BULK_HTTP2_KEEPALIVE_INTERVAL_SECS: u64 = 5;
const BULK_HTTP2_KEEPALIVE_TIMEOUT_SECS: u64 = 3;
const BULK_RPC_TIMEOUT_SECS: u64 = 30;
```

#### B. Connection Pool Management

**File**: `crates/common/src/globals.rs`

**Enhancement Needed:**
Implement proactive connection health checking:

```rust
// Pseudo-code for enhanced connection management
pub struct ConnectionHealth {
    last_successful_use: SystemTime,
    consecutive_failures: u32,
    last_health_check: SystemTime,
}

// Evict connections that:
// 1. Haven't been used successfully in 30 seconds
// 2. Have 3+ consecutive failures
// 3. Haven't passed health check in 10 seconds
pub async fn evict_unhealthy_connections() {
    // Implementation needed
}

// Background task to periodically health-check cached connections
pub async fn start_connection_health_checker() {
    tokio::spawn(async {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            evict_unhealthy_connections().await;
        }
    });
}
```

#### C. HTTP Client Configuration for Data Plane

**Problem**: The data plane (file uploads/downloads) uses separate HTTP clients that likely don't have the same keepalive configuration as gRPC.

**Status**: Requires verification through code audit.

**Files to Review and Verify:**
- `crates/rio/src/*.rs` - Check for reqwest::Client usage and configuration
- `crates/ecstore/src/put_object.rs` - Verify upload handling client configuration
- `crates/ecstore/src/get_object.rs` - Verify download handling client configuration
- `crates/ahm/src/scanner/stats_aggregator.rs:163` - Known reqwest::Client::builder() usage
- `crates/policy/src/policy/opa.rs:104` - Known reqwest::Client::builder() usage

**Verification Checklist:**
1. [ ] Identify all locations where HTTP clients are created
2. [ ] Verify each client has tcp_keepalive configured
3. [ ] Verify each client has http2_keep_alive_interval configured
4. [ ] Ensure timeout values are appropriate for operation type
5. [ ] Test that dead peer detection works for data plane operations

**Recommendation:**
Create a centralized HTTP client factory with consistent keepalive settings:

```rust
// File: crates/common/src/http_client.rs (NEW FILE)
use reqwest::ClientBuilder;
use std::time::Duration;

const HTTP_POOL_IDLE_TIMEOUT_SECS: u64 = 30;
const HTTP_POOL_MAX_IDLE_PER_HOST: usize = 10;
const HTTP_CONNECT_TIMEOUT_SECS: u64 = 3;
const HTTP_REQUEST_TIMEOUT_SECS: u64 = 30;
const HTTP_TCP_KEEPALIVE_SECS: u64 = 10;

pub fn create_resilient_http_client() -> Result<reqwest::Client, Box<dyn Error>> {
    ClientBuilder::new()
        .pool_idle_timeout(Duration::from_secs(HTTP_POOL_IDLE_TIMEOUT_SECS))
        .pool_max_idle_per_host(HTTP_POOL_MAX_IDLE_PER_HOST)
        .connect_timeout(Duration::from_secs(HTTP_CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(HTTP_REQUEST_TIMEOUT_SECS))
        .tcp_keepalive(Duration::from_secs(HTTP_TCP_KEEPALIVE_SECS))
        .http2_keep_alive_interval(Duration::from_secs(5))
        .http2_keep_alive_timeout(Duration::from_secs(3))
        .http2_keep_alive_while_idle(true)
        .build()
}
```

### Layer 2: Operation-Specific Timeouts

#### A. IAM Operations (CURRENT STATUS: PARTIALLY FIXED)

**File**: `crates/iam/src/sys.rs`

**Current Implementation:**
```rust
async fn notify_for_user(&self, name: &str, is_temp: bool) {
    // Fire-and-forget with tokio::spawn - GOOD
    let name = name.to_string();
    tokio::spawn(async move {
        if let Some(notification_sys) = get_global_notification_sys() {
            let resp = notification_sys.load_user(&name, is_temp).await;
            // ...
        }
    });
}
```

**Analysis**: This is correct for non-critical operations.

**Enhancement Recommendation:**
Add timeout to the spawned task:

```rust
async fn notify_for_user(&self, name: &str, is_temp: bool) {
    let name = name.to_string();
    tokio::spawn(async move {
        // Add overall timeout for notification
        let timeout_duration = Duration::from_secs(5);
        let result = timeout(timeout_duration, async {
            if let Some(notification_sys) = get_global_notification_sys() {
                notification_sys.load_user(&name, is_temp).await
            } else {
                vec![]
            }
        }).await;
        
        match result {
            Ok(resp) => {
                for r in resp {
                    if let Some(err) = r.err {
                        warn!("notify load_user failed: {}", err);
                    }
                }
            }
            Err(_) => {
                warn!("notify load_user timed out after {:?}", timeout_duration);
            }
        }
    });
}
```

#### B. Console Aggregation Operations (CURRENT STATUS: PARTIALLY FIXED)

**File**: `crates/ecstore/src/notification_sys.rs`

**Current Implementation:**
```rust
pub async fn storage_info<S: StorageAPI>(&self, api: &S) -> rustfs_madmin::StorageInfo {
    let peer_timeout = Duration::from_secs(2); // GOOD
    
    for client in self.peer_clients.iter() {
        futures.push(async move {
            if let Some(client) = client {
                match timeout(peer_timeout, client.local_storage_info()).await {
                    Ok(Ok(info)) => Some(info),
                    Ok(Err(err)) | Err(_) => {
                        // Return offline status - GOOD
                        Some(offline_storage_info(&host))
                    }
                }
            }
        });
    }
    // ...
}
```

**Analysis**: This is well-structured.

**Enhancement Recommendations:**

1. **Tunable Timeout Per Environment:**
```rust
// Allow operators to tune timeout via environment variable
fn get_peer_timeout() -> Duration {
    std::env::var("RUSTFS_PEER_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(2))
}
```

2. **Circuit Breaker Pattern:**
```rust
// Track peer health to avoid repeatedly trying dead peers
pub struct PeerCircuitBreaker {
    failure_count: AtomicU32,
    last_failure: RwLock<Option<Instant>>,
    state: AtomicU8, // 0=closed, 1=open, 2=half-open
}

impl PeerCircuitBreaker {
    pub fn should_attempt(&self) -> bool {
        let state = self.state.load(Ordering::Relaxed);
        if state == 0 { return true; } // Closed - healthy
        if state == 1 {
            // Open - check if we should try half-open
            if let Some(last) = *self.last_failure.read().unwrap() {
                if last.elapsed() > Duration::from_secs(30) {
                    self.state.store(2, Ordering::Relaxed); // Half-open
                    return true;
                }
            }
            return false; // Still in open state
        }
        true // Half-open - allow one attempt
    }
    
    pub fn record_success(&self) {
        self.failure_count.store(0, Ordering::Relaxed);
        self.state.store(0, Ordering::Relaxed); // Close circuit
    }
    
    pub fn record_failure(&self) {
        let failures = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
        *self.last_failure.write().unwrap() = Some(Instant::now());
        if failures >= 3 {
            self.state.store(1, Ordering::Relaxed); // Open circuit
        }
    }
}
```

#### C. Upload/Download Streaming Operations

**Problem Area**: Large file uploads that span multiple nodes.

**Files to Review and Enhance:**
- `crates/ecstore/src/put_object.rs`
- `crates/ecstore/src/erasure/encode.rs`

**Enhancement Strategy:**

1. **Chunk-Level Timeout:**
```rust
// Each chunk upload should have its own timeout
async fn upload_chunk_to_node(
    node: &str,
    chunk: &[u8],
) -> Result<(), Error> {
    let chunk_timeout = Duration::from_secs(10); // Adjust based on chunk size
    
    timeout(chunk_timeout, async {
        // Actual upload logic
    })
    .await
    .map_err(|_| Error::UploadTimeout(node.to_string()))?
}
```

2. **Retry with Different Node:**
```rust
// If one node fails, try another in the same erasure set
async fn upload_chunk_with_fallback(
    primary_node: &str,
    fallback_nodes: &[String],
    chunk: &[u8],
) -> Result<(), Error> {
    // Try primary
    match upload_chunk_to_node(primary_node, chunk).await {
        Ok(()) => return Ok(()),
        Err(e) => {
            warn!("Primary node {} failed: {}, trying fallback", primary_node, e);
            evict_connection(primary_node).await;
        }
    }
    
    // Try fallbacks
    for node in fallback_nodes {
        match upload_chunk_to_node(node, chunk).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                warn!("Fallback node {} failed: {}", node, e);
                evict_connection(node).await;
                continue;
            }
        }
    }
    
    Err(Error::AllNodesFailed)
}
```

### Layer 3: Quorum-Based Operations

#### Concept: Operate with Majority

In a 4-node cluster, as long as 3 nodes are healthy, the cluster should remain fully operational.

**Implementation Areas:**

#### A. Metadata Operations

```rust
// When synchronizing metadata, don't wait for all peers
pub async fn sync_metadata_with_quorum(
    &self,
    metadata: &Metadata,
    quorum_size: usize,
) -> Result<(), Error> {
    let total_peers = self.peer_clients.len();
    let required_acks = quorum_size.min(total_peers);
    
    let mut futures = Vec::new();
    for client in self.peer_clients.iter() {
        if let Some(client) = client {
            let metadata_clone = metadata.clone();
            futures.push(async move { client.sync_metadata(&metadata_clone).await });
        }
    }
    
    // Use select! to complete when we have enough acks
    let mut success_count = 0;
    let mut pending = futures;
    
    while !pending.is_empty() && success_count < required_acks {
        match futures::future::select_all(pending).await {
            (Ok(()), _, remaining) => {
                success_count += 1;
                pending = remaining;
            }
            (Err(e), _, remaining) => {
                warn!("Peer sync failed: {}", e);
                pending = remaining;
            }
        }
    }
    
    if success_count >= required_acks {
        Ok(())
    } else {
        Err(Error::QuorumNotReached {
            required: required_acks,
            achieved: success_count,
        })
    }
}
```

### Layer 4: Monitoring & Observability

#### A. Metrics to Add

**Connection Health Metrics:**
```rust
// File: crates/obs/src/metrics/cluster_health.rs (ENHANCE)

// Number of active connections per peer
gauge!("rustfs_peer_connections_active", peer_host => host);

// Connection failures per peer
counter!("rustfs_peer_connection_failures_total", peer_host => host);

// Connection evictions per peer
counter!("rustfs_peer_connection_evictions_total", peer_host => host);

// RPC latency per peer per operation
histogram!(
    "rustfs_peer_rpc_duration_seconds",
    peer_host => host,
    operation => op_name
);

// Timeout occurrences
counter!(
    "rustfs_peer_timeout_total",
    peer_host => host,
    operation => op_name
);
```

**Operation Metrics:**
```rust
// Upload operation tracking
histogram!("rustfs_upload_duration_seconds", size_bucket => "1mb_10mb");
counter!("rustfs_upload_failures_total", reason => "peer_timeout");
gauge!("rustfs_upload_in_progress");

// Console aggregation timing
histogram!("rustfs_console_aggregation_duration_seconds", operation => "storage_info");
counter!("rustfs_console_aggregation_partial_total");
```

#### B. Structured Logging

**Enhancement Recommendations:**
```rust
// Use structured logging with context
use tracing::{info, warn, error, instrument};

#[instrument(skip(self, data), fields(peer = %peer_host, size = data.len()))]
async fn send_data_to_peer(&self, peer_host: &str, data: &[u8]) -> Result<()> {
    let start = Instant::now();
    
    match timeout(Duration::from_secs(10), self.send_internal(peer_host, data)).await {
        Ok(Ok(())) => {
            info!(
                duration_ms = start.elapsed().as_millis(),
                "Successfully sent data to peer"
            );
            Ok(())
        }
        Ok(Err(e)) => {
            error!(
                duration_ms = start.elapsed().as_millis(),
                error = %e,
                "Failed to send data to peer"
            );
            Err(e)
        }
        Err(_) => {
            error!(
                duration_ms = start.elapsed().as_millis(),
                "Timeout sending data to peer"
            );
            Err(Error::Timeout)
        }
    }
}
```

---

## Testing & Validation

### Test Scenarios

#### Scenario 1: Single Node Abrupt Power-Off During Idle
**Setup:**
- 4-node cluster, all healthy
- No active operations

**Test Steps:**
1. Power off Node 3 abruptly
2. Wait 10 seconds
3. Attempt to login via Console
4. Check Console dashboard loads
5. Verify 3 nodes shown as online, 1 as offline

**Success Criteria:**
- Login completes within 5 seconds
- Dashboard loads within 5 seconds
- Correct node status displayed

#### Scenario 2: Single Node Abrupt Power-Off During 1GB Upload
**Setup:**
- 4-node cluster, all healthy
- Start uploading a 1GB file

**Test Steps:**
1. Start 1GB file upload
2. After 30% progress, power off Node 3 abruptly
3. Monitor upload progress

**Success Criteria:**
- Upload either completes successfully (using remaining nodes)
- OR upload fails with clear error within 15 seconds
- Other uploads can be initiated immediately
- Console remains responsive throughout

#### Scenario 3: Multiple Sequential Node Failures
**Setup:**
- 4-node cluster, all healthy

**Test Steps:**
1. Power off Node 4
2. Wait for detection (should be <10s)
3. Verify cluster still operational with 3 nodes
4. Power off Node 3
5. Verify cluster still operational with 2 nodes (if quorum allows)

**Success Criteria:**
- Each failure detected within 10 seconds
- Cluster continues serving requests after each failure (as long as quorum maintained)
- Console accurately reflects cluster state

#### Scenario 4: Node Recovery After Power-On
**Setup:**
- 4-node cluster with Node 3 powered off

**Test Steps:**
1. Power on Node 3
2. Monitor cluster behavior

**Success Criteria:**
- Node 3 rejoins cluster within 30 seconds
- Cached connections to Node 3 re-established
- Data rebalancing begins (if needed)
- Console shows 4 nodes online

### Load Testing

**Scenario: Sustained Load with Rolling Failures**

```bash
#!/bin/bash
# File: scripts/test_cluster_resilience.sh

# Start 4-node cluster
./start_test_cluster.sh

# Generate sustained upload load
for i in {1..100}; do
    s3cmd put large_file_${i}.dat s3://testbucket/ &
done

# While uploads are running, cause rolling failures
sleep 30
echo "Simulating abrupt power-off on node 2..."
# Use 'docker kill' to simulate abrupt power-off (no graceful shutdown)
docker kill rustfs_node2 &

sleep 60
echo "Simulating abrupt power-off on node 3..."
docker kill rustfs_node3 &

sleep 60
echo "Recovering node 2..."
docker start rustfs_node2 &

sleep 60
echo "Recovering node 3..."
docker start rustfs_node3 &

# Wait for all uploads
wait

# Validate results
echo "Upload success rate:"
s3cmd ls s3://testbucket/ | wc -l
```

---

## Monitoring & Observability

### Key Metrics Dashboard

**Grafana Panel Recommendations:**

#### Panel 1: Peer Connection Health
```promql
# Active connections per peer
rustfs_peer_connections_active{peer_host=~".*"}

# Connection failure rate
rate(rustfs_peer_connection_failures_total[5m])

# Connection eviction rate
rate(rustfs_peer_connection_evictions_total[5m])
```

#### Panel 2: RPC Performance
```promql
# P99 latency per peer
histogram_quantile(0.99, 
  rate(rustfs_peer_rpc_duration_seconds_bucket[5m])
)

# Timeout rate
rate(rustfs_peer_timeout_total[5m])
```

#### Panel 3: Upload Operations
```promql
# Upload success rate
rate(rustfs_upload_success_total[5m]) / 
(rate(rustfs_upload_success_total[5m]) + rate(rustfs_upload_failures_total[5m]))

# Upload duration P95
histogram_quantile(0.95,
  rate(rustfs_upload_duration_seconds_bucket[5m])
)
```

### Alert Rules

**Prometheus AlertManager Configuration:**

```yaml
# File: deploy/prometheus/alerts.yml

groups:
  - name: rustfs_cluster_health
    interval: 30s
    rules:
      - alert: PeerConnectionFailureRateHigh
        expr: rate(rustfs_peer_connection_failures_total[2m]) > 0.5
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "High connection failure rate to peer {{ $labels.peer_host }}"
          description: "Peer {{ $labels.peer_host }} has {{ $value }} connection failures per second"
      
      - alert: PeerUnreachable
        expr: rustfs_peer_connections_active == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Peer {{ $labels.peer_host }} is unreachable"
          description: "No active connections to peer {{ $labels.peer_host }} for 1 minute"
      
      - alert: UploadFailureRateHigh
        expr: |
          rate(rustfs_upload_failures_total[5m]) / 
          (rate(rustfs_upload_success_total[5m]) + rate(rustfs_upload_failures_total[5m])) 
          > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High upload failure rate"
          description: "{{ $value | humanizePercentage }} of uploads are failing"
```

---

## Operational Recommendations

### Deployment Best Practices

#### 1. Cluster Sizing for Resilience

**Minimum Recommendations:**
- **Production**: 5+ nodes to tolerate 2 simultaneous failures
- **Staging**: 3+ nodes to tolerate 1 failure
- **Development**: 3+ nodes to test failure scenarios

**Quorum Calculation:**
```
Quorum = floor(N/2) + 1
Tolerable Failures = N - Quorum

For N=4: Quorum=3, Tolerable=1
For N=5: Quorum=3, Tolerable=2
For N=6: Quorum=4, Tolerable=2
For N=7: Quorum=4, Tolerable=3
```

#### 2. Network Configuration

**TCP Tuning (Linux):**
```bash
# File: /etc/sysctl.d/99-rustfs.conf

# Reduce TIME_WAIT duration
net.ipv4.tcp_fin_timeout = 30

# Enable TCP keepalive
net.ipv4.tcp_keepalive_time = 10
net.ipv4.tcp_keepalive_intvl = 5
net.ipv4.tcp_keepalive_probes = 3

# Fast detection of broken connections
net.ipv4.tcp_retries2 = 5

# Larger connection tracking table
net.netfilter.nf_conntrack_max = 262144
```

Apply with:
```bash
sudo sysctl -p /etc/sysctl.d/99-rustfs.conf
```

#### 3. Resource Limits

**Ensure adequate file descriptors:**
```bash
# File: /etc/security/limits.d/rustfs.conf
rustfs soft nofile 65536
rustfs hard nofile 65536
```

**Systemd service configuration:**
```ini
# File: /etc/systemd/system/rustfs.service
[Service]
LimitNOFILE=65536
Restart=always
RestartSec=10
```

#### 4. Configuration Tuning

**Environment Variables:**
```bash
# Connection timeouts (seconds)
RUSTFS_CONNECT_TIMEOUT=3
RUSTFS_RPC_TIMEOUT=30
RUSTFS_PEER_TIMEOUT=2

# Keepalive settings (seconds)
RUSTFS_TCP_KEEPALIVE=10
RUSTFS_HTTP2_KEEPALIVE_INTERVAL=5
RUSTFS_HTTP2_KEEPALIVE_TIMEOUT=3

# Retry behavior
RUSTFS_MAX_RETRIES=3
RUSTFS_RETRY_BACKOFF_MS=100

# Connection pool
RUSTFS_POOL_IDLE_TIMEOUT=30
RUSTFS_POOL_MAX_IDLE_PER_HOST=10

# Circuit breaker
RUSTFS_CIRCUIT_BREAKER_THRESHOLD=3
RUSTFS_CIRCUIT_BREAKER_TIMEOUT=30
```

---

## Troubleshooting Guide

### Symptom: Console Web UI Hangs

**Diagnosis Steps:**
1. Check peer connectivity:
```bash
curl -v http://<peer_ip>:9000/minio/health/live
```

2. Check gRPC connectivity:
```bash
grpcurl -plaintext <peer_ip>:9001 grpc.health.v1.Health/Check
```

3. Review connection pool metrics:
```bash
curl http://localhost:9090/api/v1/query?query=rustfs_peer_connections_active
```

4. Check logs for timeout messages:
```bash
journalctl -u rustfs -f | grep -i "timeout\|unreachable"
```

**Resolution:**
- If peer is truly down: Wait for automatic detection (~10s)
- If peer is network-partitioned: Fix network routing
- If connection pool exhausted: Restart RustFS service (last resort)

### Symptom: Upload Hangs at Partial Progress

**Diagnosis Steps:**
1. Identify which node is receiving stuck chunks:
```bash
journalctl -u rustfs -f | grep -i "upload\|chunk"
```

2. Check if target node is reachable:
```bash
nc -zv <target_node_ip> 9000
```

3. Review RPC metrics for slow peers

**Resolution:**
- If node is down: Manual intervention to mark node offline
- If network is slow: Increase timeouts temporarily
- If upload is truly stuck: Cancel and retry

---

## Future Enhancements

### Short-Term (1-3 Months)

#### 1. Adaptive Timeout Mechanism
Dynamically adjust timeouts based on measured peer latency

#### 2. Health Score Per Peer
Replace binary healthy/unhealthy with gradient scoring

#### 3. Predictive Failure Detection
Use trends to predict impending failures

### Mid-Term (3-6 Months)

#### 1. Intelligent Request Routing
Route requests based on peer health scores

#### 2. Multi-Datacenter Support
Enhanced resilience across datacenters

#### 3. Automated Remediation
Self-healing cluster capabilities

### Long-Term (6+ Months)

#### 1. Chaos Engineering Integration
Built-in chaos testing framework

#### 2. Time-Series Anomaly Detection
Continuous analysis of metrics

#### 3. Zero-Downtime Rolling Updates
Coordinate updates to ensure quorum

---

## Appendices

### Appendix A: TCP State Machine and Keepalive

**TCP Connection States**
```
ESTABLISHED → (peer power-off, no packets sent)
   ↓ (keepalive timer expires)
PROBE_SENT → (keepalive probe sent)
   ↓ (timeout waiting for ACK)
PROBE_SENT → (retry keepalive probe)
   ↓ (after N probes timeout)
CLOSED → (connection declared dead)
```

**Key Parameters:**
- `tcp_keepalive_time`: Wait before first probe (we set 10s)
- `tcp_keepalive_intvl`: Wait between probes (we set 5s)
- `tcp_keepalive_probes`: Number of probes (we set 3)

**Total Detection Time:**
```
tcp_keepalive_time + (tcp_keepalive_probes * tcp_keepalive_intvl)
= 10s + (3 * 5s) = 25s
```

**HTTP/2 PING is faster:**
```
http2_keep_alive_interval + http2_keep_alive_timeout
= 5s + 3s = 8s
```

### Appendix B: Failure Detection Comparison

| Mechanism | Detection Time | Overhead | Reliability |
|-----------|----------------|----------|-------------|
| TCP FIN/RST | Immediate | None | 100% (graceful) |
| TCP Keepalive | 10-30s | Low | High |
| HTTP/2 PING | 5-10s | Medium | Very High |
| Application Heartbeat | 1-5s | High | Medium |

**Recommendation:** Use HTTP/2 PING for fast detection (8s), TCP keepalive as backup (25s).

---

## Conclusion

The cluster power-off resilience issue is a multi-faceted problem requiring defense-in-depth solutions across multiple architectural layers:

1. **Connection Management**: Aggressive keepalive and rapid eviction of dead connections
2. **Operation Timeouts**: Bounded execution time for all operations
3. **Graceful Degradation**: Continue with partial responses when some peers fail
4. **Observability**: Comprehensive metrics and logging

**Current Status Assessment:**

✅ **Already Implemented:**
- HTTP/2 keepalive in gRPC channels
- TCP keepalive configuration
- Per-peer timeouts in console aggregation
- Fire-and-forget IAM notifications

⚠️ **Partially Implemented:**
- Connection eviction (exists but may need enhancement)
- Timeout handling (exists but not comprehensive)

❌ **Not Yet Implemented:**
- Data plane (upload/download) keepalive configuration
- Circuit breaker pattern
- Adaptive timeouts
- Health scoring
- Comprehensive metrics

**Priority Action Items:**

**P0 (Critical - Immediate):**
1. Verify and enhance HTTP client keepalive for data plane operations
2. Add comprehensive timeout wrappers around all peer communication
3. Implement and test circuit breaker pattern

**P1 (High - Within 2 Weeks):**
1. Add detailed metrics and dashboards
2. Implement automated resilience testing
3. Create operational runbooks

**P2 (Medium - Within 1 Month):**
1. Adaptive timeout mechanism
2. Health scoring system
3. Enhanced observability

With systematic implementation of these solutions, the RustFS cluster will achieve production-grade resilience against abrupt node failures.

---

## Document Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2024-12-08 | GitHub Copilot (Architecture Analysis) | Initial comprehensive solution document |

---

## References

1. [TCP Keepalive HOWTO](https://tldp.org/HOWTO/TCP-Keepalive-HOWTO/)
2. [HTTP/2 PING Frame Specification](https://httpwg.org/specs/rfc7540.html#PING)
3. [Tonic gRPC Documentation](https://docs.rs/tonic/)
4. [Reqwest HTTP Client Documentation](https://docs.rs/reqwest/)
5. [Circuit Breaker Pattern](https://martinfowler.com/bliki/CircuitBreaker.html)
6. [RustFS Issue #1001](https://github.com/rustfs/rustfs/issues/1001)
7. [RustFS PR #1035](https://github.com/rustfs/rustfs/pull/1035)
8. [RustFS PR #1054](https://github.com/rustfs/rustfs/pull/1054)
