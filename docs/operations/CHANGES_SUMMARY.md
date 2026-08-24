# Summary of Changes for Issue #5803

## Overview

This document summarizes all changes made to fix [rustfs/rustfs#5803](https://github.com/rustfs/rustfs/issues/5803) - Memory RSS regression since beta.9.

## Problem

- **Symptom**: RSS memory steps ~+300 MiB on tiny S3 bursts and never returns
- **Impact**: Daily OOMKills in 1 GiB containers
- **Root Cause**: RustFS uses host memory/CPU values instead of container cgroup limits

## Solution

Implemented **cgroup-aware resource detection** that automatically detects and applies container resource limits.

## Files Modified

### New Files

1. **`rustfs/src/cgroup_resources.rs`**
   - Core cgroup detection logic
   - Detects CPU and memory limits from cgroup v1/v2
   - Thread-safe with `OnceLock` caching
   - Returns effective (host ∩ cgroup) values

2. **`rustfs/src/container_config.rs`**
   - Container configuration with environment variable overrides
   - Startup logging of detected resources
   - Runtime metrics for verification

3. **`rustfs/src/cgroup_resources_test.rs`**
   - Unit tests for cgroup resource detection

4. **`docs/operations/container-resource-detection.md`**
   - Comprehensive documentation for container resource detection

5. **`docs/examples/kubernetes-deployment.yaml`**
   - Example Kubernetes deployment with resource limits

6. **`docs/fixes/issue-5803-memory-regression-fix.md`**
   - Detailed fix documentation

7. **`docs/migration/container-upgrade-guide.md`**
   - Migration guide for upgrading to new version

### Modified Files

1. **`rustfs/src/memory_observability.rs`**
   - Updated `record_memory_snapshot()` to use effective memory
   - Added `record_effective_memory()` function
   - Added `record_cgroup_resource_detection()` function
   - Added startup logging of cgroup detection

2. **`rustfs/src/server/runtime.rs`**
   - Updated `detect_cores()` to use cgroup-aware CPU detection
   - Updated `compute_default_max_blocking_threads()` to cap for small containers

3. **`rustfs/src/startup_entrypoint.rs`**
   - Added container configuration logging at startup

4. **`rustfs/src/lib.rs`**
   - Added `cgroup_resources` module
   - Added `container_config` module

## Key Changes

### 1. Cgroup Resource Detection

```rust
// rustfs/src/cgroup_resources.rs

/// Get effective CPU cores, considering cgroup limits.
pub fn effective_cpu_cores(host_cores: usize) -> usize {
    let resources = cgroup_resources();
    match resources.cpu_cores {
        Some(cgroup_cores) => cgroup_cores.min(host_cores).max(1),
        None => host_cores.max(1),
    }
}

/// Get effective memory, considering cgroup limits.
pub fn effective_memory(host_memory: u64) -> u64 {
    let resources = cgroup_resources();
    match resources.memory_bytes {
        Some(cgroup_memory) => cgroup_memory.min(host_memory),
        None => host_memory,
    }
}
```

### 2. Memory Metrics Fix

```rust
// rustfs/src/memory_observability.rs

// Before (WRONG):
let total_memory = refresh_total_memory();  // Returns host memory
record_memory_usage(process.resident_memory_bytes, total_memory);

// After (CORRECT):
let (effective_memory, memory_basis) = refresh_effective_memory();  // Returns cgroup-aware memory
record_memory_usage(process.resident_memory_bytes, effective_memory);
record_effective_memory(effective_memory, memory_basis);
```

### 3. Tokio Runtime Fix

```rust
// rustfs/src/server/runtime.rs

// Before (WRONG):
fn detect_cores() -> usize {
    let mut sys = System::new_with_specifics(...);
    sys.refresh_cpu_all();
    sys.cpus().len().max(1)  // Returns host CPU count
}

// After (CORRECT):
fn detect_cores() -> usize {
    let host_cores = {
        let mut sys = System::new_with_specifics(...);
        sys.refresh_cpu_all();
        sys.cpus().len().max(1)
    };
    crate::cgroup_resources::effective_cpu_cores(host_cores)  // Returns cgroup-aware value
}
```

### 4. Blocking Threads Cap

```rust
// rustfs/src/server/runtime.rs

fn compute_default_max_blocking_threads() -> usize {
    // ... existing logic ...

    // For small containers (<=4 cores), cap the blocking threads
    // This prevents a 1 GiB container from allocating up to 1 GiB just for thread stacks
    if cores <= 4 {
        threads = threads.min(256);
    }

    threads
}
```

## New Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `rustfs_memory_effective_total_bytes` | Gauge | Effective memory total (host or cgroup) |
| `rustfs_cgroup_detected` | Gauge | Whether cgroup limits were detected |
| `rustfs_cgroup_cpu_cores_limit` | Gauge | Detected CPU cores limit |
| `rustfs_cgroup_memory_limit_bytes` | Gauge | Detected memory limit |

## Environment Variables

### Disable Cgroup Detection
```bash
RUSTFS_DISABLE_CGROUP_DETECTION=1
```

### Override CPU Cores
```bash
RUSTFS_OVERRIDE_CPU_CORES=4
```

### Override Memory Limit
```bash
RUSTFS_OVERRIDE_MEMORY_BYTES=2147483648  # 2 GiB
```

## Expected Behavior

### Before Fix

```
rustfs_memory_total_bytes = 10405969920  # 9.7 GiB (host)
rustfs_memory_usage_percent = 3.6%       # Wrong!
Tokio: worker_threads=6                  # Too many for 200m CPU
Tokio: max_blocking_threads=1024         # Excessive for 1 GiB container
```

### After Fix

```
rustfs_memory_total_bytes = 1073741824   # 1 GiB (cgroup limit)
rustfs_memory_effective_total_bytes{basis="cgroup"} = 1073741824
rustfs_memory_usage_percent = 75%        # Correct!
rustfs_cgroup_detected{resource="cpu"} = 1
rustfs_cgroup_detected{resource="memory"} = 1
rustfs_cgroup_cpu_cores_limit = 2
rustfs_cgroup_memory_limit_bytes = 1073741824
Tokio: worker_threads=2                  # Matches CPU limit
Tokio: max_blocking_threads=256          # Capped for small container
```

## Performance Impact

- **Startup**: One-time detection adds ~1ms overhead
- **Runtime**: Cached values, no repeated filesystem reads
- **Memory**: Negligible (<1KB for cached values)
- **Thread Stacks**: Reduced from 1 GiB potential to 256 MiB for small containers

## Testing

### Unit Tests

```bash
# Test cgroup resource detection
cargo test -p rustfs cgroup_resources

# Test memory observability
cargo test -p rustfs memory_observability

# Test container configuration
cargo test -p rustfs container_config
```

### Integration Testing

1. Deploy to Kubernetes with resource limits
2. Verify metrics show correct values
3. Check startup logs for cgroup detection
4. Monitor memory usage and OOMKills

## Documentation

1. **Container Resource Detection**: `docs/operations/container-resource-detection.md`
2. **Kubernetes Example**: `docs/examples/kubernetes-deployment.yaml`
3. **Fix Details**: `docs/fixes/issue-5803-memory-regression-fix.md`
4. **Migration Guide**: `docs/migration/container-upgrade-guide.md`

## Related Issues

- [rustfs/rustfs#5803](https://github.com/rustfs/rustfs/issues/5803) - Original issue
- [rustfs/backlog#2012](https://github.com/rustfs/backlog/issues/2012) - Backlog tracking

## Future Improvements

1. **io_uring Memory Detection**: Investigate io_uring ring buffer sizing for containers
2. **Dynamic Resource Updates**: Support for runtime resource limit changes (e.g., Kubernetes VPA)
3. **Memory Growth Tracking**: Add metrics for memory growth events and triggers
4. **Adaptive Thread Pool**: Dynamically adjust thread pool size based on container resources

## Credits

- Issue reported by: @alexander-zimmermann
- Analysis and fix: RustFS team
- Related work: `crates/object-data-cache/src/runtime_memory.rs` (cgroup-aware memory for cache)
