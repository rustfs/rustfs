# Container Resource Detection

RustFS automatically detects container resource limits (CPU and memory) from cgroup v1/v2. This ensures correct resource allocation and accurate metrics in containerized environments (Kubernetes, Docker, etc.).

## Problem

When RustFS runs in a container, the underlying system libraries report the **host's** total CPU cores and memory, not the container's limits. This leads to:

1. **Over-provisioned Tokio threads**: Too many worker and blocking threads
2. **Incorrect memory metrics**: `rustfs_memory_usage_percent` shows host-based percentage
3. **Memory budget errors**: Object data cache sized to host RAM instead of container limit
4. **OOMKills**: Container exceeds its memory limit and gets killed

## Solution

RustFS now detects cgroup limits directly from the filesystem:

- **CPU**: `/sys/fs/cgroup/cpu.max` (v2) or `/sys/fs/cgroup/cpu/cpu.cfs_quota_us` (v1)
- **Memory**: `/sys/fs/cgroup/memory.max` (v2) or `/sys/fs/cgroup/memory/memory.limit_in_bytes` (v1)

The effective resource limits are the **minimum** of host and cgroup values.

## Detection Logic

### CPU Detection

1. Read cgroup v2 `/sys/fs/cgroup/cpu.max`
   - Format: `"$QUOTA $PERIOD"` or `"max"` (unlimited)
   - Calculate: `cores = ceil(quota / period)`
2. Fallback to cgroup v1 `/sys/fs/cgroup/cpu/cpu.cfs_quota_us`
   - Calculate: `cores = ceil(quota / period)`
3. Fallback to host CPU count from `sysinfo`

### Memory Detection

1. Read cgroup v2 `/sys/fs/cgroup/memory.max`
   - Value in bytes or `"max"` (unlimited)
2. Fallback to cgroup v1 `/sys/fs/cgroup/memory/memory.limit_in_bytes`
   - Very large values (≥2^62) indicate unlimited
3. Fallback to host memory from `sysinfo`

## Environment Variables

### Disable Cgroup Detection

```bash
RUSTFS_DISABLE_CGROUP_DETECTION=1
```

Disables cgroup detection entirely. Useful for testing or when cgroup filesystem is not accessible.

### Override CPU Cores

```bash
RUSTFS_OVERRIDE_CPU_CORES=4
```

Overrides detected CPU cores. Takes precedence over cgroup detection.

### Override Memory Limit

```bash
RUSTFS_OVERRIDE_MEMORY_BYTES=2147483648
```

Overrides detected memory limit in bytes. Takes precedence over cgroup detection.

## Metrics

### New Metrics

| Metric | Description |
|--------|-------------|
| `rustfs_memory_effective_total_bytes` | Effective memory total (host or cgroup) |
| `rustfs_cgroup_detected` | Whether cgroup limits were detected (1=yes, 0=no) |
| `rustfs_cgroup_cpu_cores_limit` | Detected CPU cores limit |
| `rustfs_cgroup_memory_limit_bytes` | Detected memory limit |

### Updated Metrics

| Metric | Change |
|--------|--------|
| `rustfs_memory_total_bytes` | Now uses effective memory (cgroup-aware) |
| `rustfs_memory_usage_percent` | Now calculated against effective memory |

## Startup Logging

RustFS logs detected container resources at startup:

```
INFO container resources (detected from cgroup) cpu_cores=2 memory_bytes=1073741824 memory_mib=1024
```

or

```
INFO container resources (overridden by environment variables) cpu_cores=4 memory_bytes=2147483648 memory_mib=2048
```

## Examples

### Kubernetes with Resource Limits

```yaml
resources:
  limits:
    cpu: "2"
    memory: "1Gi"
  requests:
    cpu: "500m"
    memory: "512Mi"
```

RustFS will detect:
- CPU cores: 2
- Memory: 1 GiB (1073741824 bytes)

### Docker with CPU and Memory Limits

```bash
docker run --cpus=2 --memory=1g rustfs/rustfs:latest
```

RustFS will detect:
- CPU cores: 2
- Memory: 1 GiB

### Manual Override

```bash
export RUSTFS_OVERRIDE_CPU_CORES=4
export RUSTFS_OVERRIDE_MEMORY_BYTES=2147483648
```

RustFS will use:
- CPU cores: 4
- Memory: 2 GiB

## Troubleshooting

### Cgroup Detection Not Working

1. Check if cgroup filesystem is mounted:
   ```bash
   ls -la /sys/fs/cgroup/
   ```

2. Check cgroup version:
   ```bash
   stat -fc %T /sys/fs/cgroup/
   ```
   - `cgroup2fs` = cgroup v2
   - `tmpfs` = cgroup v1

3. Check if limits are set:
   ```bash
   # cgroup v2
   cat /sys/fs/cgroup/cpu.max
   cat /sys/fs/cgroup/memory.max

   # cgroup v1
   cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us
   cat /sys/fs/cgroup/memory/memory.limit_in_bytes
   ```

### Metrics Show Host Values

If `rustfs_memory_effective_total_bytes` shows host memory instead of cgroup limit:

1. Verify cgroup detection is not disabled:
   ```bash
   echo $RUSTFS_DISABLE_CGROUP_DETECTION
   ```

2. Check startup logs for cgroup detection:
   ```bash
   grep "container resources" /logs/rustfs.log
   ```

3. Use environment variable override as workaround:
   ```bash
   export RUSTFS_OVERRIDE_MEMORY_BYTES=1073741824
   ```

## Implementation Details

### Files Modified

- `rustfs/src/cgroup_resources.rs` - Core cgroup detection logic
- `rustfs/src/container_config.rs` - Container configuration with overrides
- `rustfs/src/memory_observability.rs` - Updated memory metrics
- `rustfs/src/server/runtime.rs` - Updated Tokio runtime configuration
- `rustfs/src/startup_entrypoint.rs` - Startup logging

### Performance Impact

- **Startup**: One-time detection adds ~1ms overhead
- **Runtime**: Cached values, no repeated filesystem reads
- **Memory**: Negligible (<1KB for cached values)

### Thread Safety

All detection functions are thread-safe and use `OnceLock` for caching.
