# Additional Analysis: Metadata Lock and Disk State Deadlock Issues

## Executive Summary

Based on real-world testing with a 4x4 cluster (4 nodes, 4 disks per node), after implementing the P0 critical items (HTTP client factory and circuit breaker), the cluster still experiences complete service outage when one node is abruptly powered off. This document analyzes the new failure modes discovered and provides additional solutions.

## Test Environment

- **Cluster**: 4 nodes, 4 disks per node
- **Erasure Coding**: 4+4 (4 data + 4 parity)
- **Test Scenario**: Node4 abrupt power-off during 3.1GB Ubuntu ISO upload
- **Result**: Complete cluster failure despite 3 nodes remaining operational

## Root Cause Analysis from Real Logs

### Issue 1: IAM Metadata Lock Timeout

**Log Evidence:**
```
[2025-12-09 10:34:58.316420] ERROR Io error: read lock acquisition timed out on 
.rustfs.sys/config/iam/sts/AZO0FRYKVUULQHPDW1X1/identity.json (owner=node1)
```

**Problem:**
- IAM identity configuration file lock acquisition times out
- When Node4 powers off mid-operation, distributed locks may not be released
- Surviving nodes wait for lock responses from the dead node
- Since IAM is the "gatekeeper" for all requests, when IAM operations block, all upload/download operations fail

**Impact:**
- All authenticated operations block
- Console operations hang
- File uploads cannot proceed

### Issue 2: Disk Identification Failure Cascade

**Log Evidence:**
```
[2025-12-09 10:35:03.712527] ERROR [rustfs_ecstore::sets] sets new set_drive 0-3 get_disk_id is none
```

**Problem:**
- System attempts to check disk status but `get_disk_id()` returns None
- For remote disks, the operation may be blocked by network I/O waiting for Node4
- The check happens during set initialization without proper timeout protection

**Impact:**
- Set initialization hangs or fails
- System cannot determine which disks are actually available

### Issue 3: False Quorum Loss

**Log Evidence:**
```
[2025-12-09 10:37:03.028245] ERROR reduce_write_quorum_errs: ErasureWriteQuorum, offline-disks=8/8
```

**Problem:**
- System incorrectly reports all 8 disks in a set as offline
- This is a cascade failure from Issues #1 and #2
- The metadata lock deadlock and disk ID check failures cause the system to misinterpret the cluster state
- With 3 nodes still operational (6 disks), the cluster should be in degraded mode, not completely offline

**Impact:**
- Write operations rejected entirely
- Cluster refuses all I/O despite having sufficient quorum

## Why P0 Fixes Were Insufficient

The P0 implementation addressed:
1. ✅ Console aggregation operations (server_info, storage_info)
2. ✅ HTTP client keepalive for peer communication
3. ✅ Circuit breaker for repeated dead peer attempts

**What was NOT addressed:**
1. ❌ Timeout protection for IAM metadata operations
2. ❌ Timeout protection for disk state checking (get_disk_id)
3. ❌ Timeout protection for erasure coding set initialization
4. ❌ Distributed lock deadlock detection and recovery
5. ❌ Graceful degradation when some disks are unavailable

## Additional Solutions Required

### Solution 1: Add Timeout to Disk State Operations

**Target Files:**
- `crates/ecstore/src/sets.rs` - Set initialization
- `crates/ecstore/src/rpc/remote_disk.rs` - Remote disk operations

**Implementation:**
```rust
// In sets.rs, wrap get_disk_id with timeout
use tokio::time::{timeout, Duration};

const DISK_STATE_CHECK_TIMEOUT: Duration = Duration::from_secs(3);

let has_disk_id = match timeout(
    DISK_STATE_CHECK_TIMEOUT,
    disk.as_ref().unwrap().get_disk_id()
).await {
    Ok(Ok(id)) => id,
    Ok(Err(err)) => {
        warn!("get_disk_id err {:?}", err);
        None
    }
    Err(_) => {
        error!("get_disk_id timed out after {:?} for set_drive {}-{}", 
               DISK_STATE_CHECK_TIMEOUT, i, j);
        // Mark disk as temporarily unavailable
        None
    }
};
```

### Solution 2: Add Timeout to IAM Metadata Read Operations

**Target Files:**
- `crates/iam/src/store/object.rs` - IAM metadata operations
- `crates/ecstore/src/config/com.rs` - Config read operations

**Implementation:**
```rust
// Add timeout wrapper for read_config operations
const IAM_CONFIG_READ_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn read_config_with_timeout<S: StorageAPI>(
    api: Arc<S>,
    file: &str,
    opts: &ObjectOptions,
) -> Result<(Vec<u8>, ObjectInfo)> {
    match timeout(IAM_CONFIG_READ_TIMEOUT, read_config_with_metadata(api, file, opts)).await {
        Ok(result) => result,
        Err(_) => {
            error!("read_config timed out after {:?} for file: {}", IAM_CONFIG_READ_TIMEOUT, file);
            Err(Error::ConfigNotFound)
        }
    }
}
```

### Solution 3: Enhanced Error Handling in Erasure Code Layer

**Target Files:**
- `crates/ecstore/src/erasure.rs` - Erasure coding operations
- `crates/ecstore/src/disk/error_reduce.rs` - Error aggregation

**Implementation:**
```rust
// Improve error reduction to better handle partial failures
pub fn reduce_write_quorum_errs_with_circuit_breaker(
    errors: &[Option<Error>], 
    ignored_errs: &[Error], 
    quorum: usize,
    disk_endpoints: &[String],
) -> Option<Error> {
    // Check circuit breaker state for each disk/node
    let mut available_count = 0;
    let mut actual_errors = 0;
    
    for (i, err) in errors.iter().enumerate() {
        if let Some(endpoint) = disk_endpoints.get(i) {
            // Skip counting as error if circuit breaker is open for this node
            if rustfs_common::globals::GLOBAL_CircuitBreakers
                .get_state(endpoint)
                .map(|s| s == CircuitState::Open)
                .unwrap_or(false) 
            {
                continue; // Node is known dead, don't count as unexpected error
            }
        }
        
        match err {
            None => available_count += 1,
            Some(e) if ignored_errs.contains(e) => continue,
            Some(_) => actual_errors += 1,
        }
    }
    
    if available_count >= quorum {
        None // We have quorum
    } else {
        Some(Error::ErasureWriteQuorum)
    }
}
```

### Solution 4: Distributed Lock Timeout Detection

**Target Files:**
- `crates/ecstore/src/config/com.rs` - Configuration read/write operations

**Implementation:**
```rust
// Add retry logic with exponential backoff for lock acquisition
const MAX_LOCK_RETRIES: usize = 3;
const LOCK_RETRY_BASE_DELAY_MS: u64 = 100;

pub async fn read_config_with_retry<S: StorageAPI>(
    api: Arc<S>,
    file: &str,
    opts: &ObjectOptions,
) -> Result<(Vec<u8>, ObjectInfo)> {
    let mut attempts = 0;
    
    loop {
        match timeout(
            Duration::from_secs(5),
            read_config_with_metadata(api.clone(), file, opts)
        ).await {
            Ok(Ok(result)) => return Ok(result),
            Ok(Err(err)) => {
                if is_lock_timeout_error(&err) && attempts < MAX_LOCK_RETRIES {
                    attempts += 1;
                    let delay = LOCK_RETRY_BASE_DELAY_MS * 2_u64.pow(attempts as u32);
                    warn!("Lock timeout on {}, retry {}/{} after {}ms", 
                          file, attempts, MAX_LOCK_RETRIES, delay);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    continue;
                }
                return Err(err);
            }
            Err(_) => {
                error!("read_config timed out for file: {}", file);
                return Err(Error::ConfigNotFound);
            }
        }
    }
}

fn is_lock_timeout_error(err: &Error) -> bool {
    match err {
        Error::Io(io_err) => io_err.to_string().contains("lock acquisition timed out"),
        _ => false,
    }
}
```

## Implementation Priority

### P1 (Critical - Immediate)
1. ✅ Add timeout to disk state checks in `sets.rs`
2. ✅ Add timeout to IAM metadata read operations
3. ✅ Improve error reduction logic to consider circuit breaker state

### P2 (High - This Week)
1. Add distributed lock timeout detection and retry logic
2. Implement graceful degradation markers for partially available sets
3. Add metrics for lock timeout incidents

### P3 (Medium - Next Sprint)
1. Implement proactive lock health checking
2. Add automatic lock breaking after prolonged timeout
3. Enhanced monitoring for cascading failure detection

## Testing Recommendations

### Test 1: Verify Disk State Check Timeout
1. Start 4-node cluster
2. Begin large file upload
3. Power off Node4
4. Verify: System detects Node4 offline within 8 seconds
5. Verify: Remaining nodes continue to operate
6. Verify: New sets can be initialized without hanging

### Test 2: Verify IAM Operation Resilience
1. Start 4-node cluster
2. Perform IAM operations (create user, login)
3. Power off Node4 mid-operation
4. Verify: IAM operations complete or fail fast (< 10s)
5. Verify: Subsequent IAM operations work normally

### Test 3: Verify Upload Resilience
1. Start 4-node cluster
2. Begin uploading 1GB+ file
3. Power off Node4 at 30% progress
4. Verify: Upload either completes using remaining nodes OR fails cleanly within 30s
5. Verify: New uploads can be started immediately

## Monitoring and Alerting

### Key Metrics to Add

```rust
// Disk state check metrics
counter!("rustfs_disk_state_check_timeouts_total", disk => disk_id);
histogram!("rustfs_disk_state_check_duration_seconds");

// IAM operation metrics
counter!("rustfs_iam_lock_timeouts_total", operation => op_type);
histogram!("rustfs_iam_operation_duration_seconds", operation => op_type);

// Quorum metrics
gauge!("rustfs_set_available_disks", set_id => id);
gauge!("rustfs_set_required_quorum", set_id => id);
counter!("rustfs_false_quorum_loss_total", set_id => id);
```

### Alert Rules

```yaml
groups:
  - name: rustfs_metadata_health
    rules:
      - alert: IAMLockTimeoutHigh
        expr: rate(rustfs_iam_lock_timeouts_total[5m]) > 0.1
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "High rate of IAM lock timeouts"
          
      - alert: DiskStateCheckTimeoutHigh
        expr: rate(rustfs_disk_state_check_timeouts_total[5m]) > 0.5
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "High rate of disk state check timeouts"
          
      - alert: FalseQuorumLoss
        expr: rustfs_false_quorum_loss_total > 0
        labels:
          severity: critical
        annotations:
          summary: "Quorum loss detected despite sufficient disks available"
```

## Temporary Workaround

Until all fixes are implemented, operators can use this emergency recovery procedure:

1. **Detect the issue**: Monitor for stuck uploads or console hangs
2. **Restart surviving nodes**: 
   ```bash
   # On node1, node2, node3
   systemctl restart rustfs
   ```
3. **Verify recovery**: Check that cluster enters degraded mode with 3 nodes
4. **Restore failed node**: Power on Node4 when ready
5. **Monitor healing**: Watch automatic data rebalancing

## Conclusion

The P0 fixes successfully addressed console-level resilience but exposed deeper issues in the metadata and disk state management layers. The additional fixes focus on:

1. **Aggressive Timeouts**: Every blocking operation must have a timeout
2. **Graceful Degradation**: Partial failures should not cascade to total failure
3. **Better State Detection**: Use circuit breaker state to avoid false quorum loss
4. **Retry with Backoff**: Transient lock contention should not cause permanent failures

With these P1 fixes implemented, the cluster should remain operational when any single node fails abruptly, as designed by the 4+4 erasure coding architecture.

## Document Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2024-12-09 | Senior Rust Architect | Initial analysis based on real 4x4 cluster failure logs |
