# Adaptive Buffer Sizing Optimization

RustFS implements intelligent adaptive buffer sizing optimization that automatically adjusts buffer sizes based on file size and workload type to achieve optimal balance between performance, memory usage, and security.

## Overview

The adaptive buffer sizing system provides:

- **Automatic buffer size selection** based on file size
- **Workload-specific optimizations** for different use cases
- **Special environment support** (Kylin, NeoKylin, Unity OS, etc.)
- **Memory pressure awareness** with configurable limits
- **Unknown file size handling** for streaming scenarios

## Workload Profiles

### GeneralPurpose (Default)

Balanced performance and memory usage for general-purpose workloads.

**Buffer Sizing:**
- Small files (< 1MB): 64KB buffer
- Medium files (1MB-100MB): 256KB buffer
- Large files (≥ 100MB): 1MB buffer

**Best for:**
- General file storage
- Mixed workloads
- Default configuration when workload type is unknown

### AiTraining

Optimized for AI/ML training workloads with large sequential reads.

**Buffer Sizing:**
- Small files (< 10MB): 512KB buffer
- Medium files (10MB-500MB): 2MB buffer
- Large files (≥ 500MB): 4MB buffer

**Best for:**
- Machine learning model files
- Training datasets
- Large sequential data processing
- Maximum throughput requirements

### DataAnalytics

Optimized for data analytics with mixed read-write patterns.

**Buffer Sizing:**
- Small files (< 5MB): 128KB buffer
- Medium files (5MB-200MB): 512KB buffer
- Large files (≥ 200MB): 2MB buffer

**Best for:**
- Data warehouse operations
- Analytics workloads
- Business intelligence
- Mixed access patterns

### WebWorkload

Optimized for web applications with small file intensive operations.

**Buffer Sizing:**
- Small files (< 512KB): 32KB buffer
- Medium files (512KB-10MB): 128KB buffer
- Large files (≥ 10MB): 256KB buffer

**Best for:**
- Web assets (images, CSS, JavaScript)
- Static content delivery
- CDN origin storage
- High concurrency scenarios

### IndustrialIoT

Optimized for industrial IoT with real-time streaming requirements.

**Buffer Sizing:**
- Small files (< 1MB): 64KB buffer
- Medium files (1MB-50MB): 256KB buffer
- Large files (≥ 50MB): 512KB buffer (capped for memory constraints)

**Best for:**
- Sensor data streams
- Real-time telemetry
- Edge computing scenarios
- Low latency requirements
- Memory-constrained devices

### SecureStorage

Security-first configuration with strict memory limits for compliance.

**Buffer Sizing:**
- Small files (< 1MB): 32KB buffer
- Medium files (1MB-50MB): 128KB buffer
- Large files (≥ 50MB): 256KB buffer (strict limit)

**Best for:**
- Compliance-heavy environments
- Secure government systems (Kylin, NeoKylin, UOS)
- Financial services
- Healthcare data storage
- Memory-constrained secure environments

**Auto-Detection:**
This profile is automatically selected when running on Chinese secure operating systems:
- Kylin
- NeoKylin
- UOS (Unity OS)
- OpenKylin

## Usage

### Using Default Configuration

The system automatically uses the `GeneralPurpose` profile by default:

```rust
// The buffer size is automatically calculated based on file size
// Uses GeneralPurpose profile by default
let buffer_size = get_adaptive_buffer_size(file_size);
```

### Using Specific Workload Profile

```rust
use rustfs::config::workload_profiles::WorkloadProfile;

// For AI/ML workloads
let buffer_size = get_adaptive_buffer_size_with_profile(
    file_size,
    Some(WorkloadProfile::AiTraining)
);

// For web workloads
let buffer_size = get_adaptive_buffer_size_with_profile(
    file_size,
    Some(WorkloadProfile::WebWorkload)
);

// For secure storage
let buffer_size = get_adaptive_buffer_size_with_profile(
    file_size,
    Some(WorkloadProfile::SecureStorage)
);
```

### Auto-Detection Mode

The system can automatically detect the runtime environment:

```rust
// Auto-detects OS environment or falls back to GeneralPurpose
let buffer_size = get_adaptive_buffer_size_with_profile(file_size, None);
```

### Custom Configuration

For specialized requirements, create a custom configuration:

```rust
use rustfs::config::workload_profiles::{BufferConfig, WorkloadProfile};

let custom_config = BufferConfig {
    min_size: 16 * 1024,        // 16KB minimum
    max_size: 512 * 1024,       // 512KB maximum
    default_unknown: 128 * 1024, // 128KB for unknown sizes
    thresholds: vec![
        (1024 * 1024, 64 * 1024),       // < 1MB: 64KB
        (50 * 1024 * 1024, 256 * 1024), // 1MB-50MB: 256KB
        (i64::MAX, 512 * 1024),         // >= 50MB: 512KB
    ],
};

let profile = WorkloadProfile::Custom(custom_config);
let buffer_size = get_adaptive_buffer_size_with_profile(file_size, Some(profile));
```

## Phase 3: Default Enablement (Current Implementation)

**⚡ NEW: Workload profiles are now enabled by default!**

Starting from Phase 3, adaptive buffer sizing with workload profiles is **enabled by default** using the `GeneralPurpose` profile. This provides improved performance out-of-the-box while maintaining full backward compatibility.

### Default Behavior

```bash
# Phase 3: Profile-aware buffer sizing enabled by default with GeneralPurpose profile
./rustfs /data
```

This now automatically uses intelligent buffer sizing based on file size and workload characteristics.

### Changing the Workload Profile

```bash
# Use a different profile (AI/ML workloads)
export RUSTFS_BUFFER_PROFILE=AiTraining
./rustfs /data

# Or via command-line
./rustfs --buffer-profile AiTraining /data

# Use web workload profile
./rustfs --buffer-profile WebWorkload /data
```

### Opt-Out (Legacy Behavior)

If you need the exact behavior from PR #869 (fixed algorithm), you can disable profiling:

```bash
# Disable buffer profiling (revert to PR #869 behavior)
export RUSTFS_BUFFER_PROFILE_DISABLE=true
./rustfs /data

# Or via command-line
./rustfs --buffer-profile-disable /data
```

### Available Profile Names

The following profile names are supported (case-insensitive):

| Profile Name | Aliases | Description |
|-------------|---------|-------------|
| `GeneralPurpose` | `general` | Default balanced configuration (same as PR #869 for most files) |
| `AiTraining` | `ai` | Optimized for AI/ML workloads |
| `DataAnalytics` | `analytics` | Mixed read-write patterns |
| `WebWorkload` | `web` | Small file intensive operations |
| `IndustrialIoT` | `iot` | Real-time streaming |
| `SecureStorage` | `secure` | Security-first, memory constrained |

### Behavior Summary

**Phase 3 Default (Enabled):**
- Uses workload-aware buffer sizing with `GeneralPurpose` profile
- Provides same buffer sizes as PR #869 for most scenarios
- Allows easy switching to specialized profiles
- Buffer sizes: 64KB, 256KB, 1MB based on file size (GeneralPurpose)

**With `RUSTFS_BUFFER_PROFILE_DISABLE=true`:**
- Uses the exact original adaptive buffer sizing from PR #869
- For users who want guaranteed legacy behavior
- Buffer sizes: 64KB, 256KB, 1MB based on file size

**With Different Profiles:**
- `AiTraining`: 512KB, 2MB, 4MB - maximize throughput
- `WebWorkload`: 32KB, 128KB, 256KB - optimize concurrency
- `SecureStorage`: 32KB, 128KB, 256KB - compliance-focused
- And more...

### Migration Examples

**Phase 2 → Phase 3 Migration:**

```bash
# Phase 2 (Opt-In): Had to explicitly enable
export RUSTFS_BUFFER_PROFILE_ENABLE=true
export RUSTFS_BUFFER_PROFILE=GeneralPurpose
./rustfs /data

# Phase 3 (Default): Enabled automatically
./rustfs /data  # ← Same behavior, no configuration needed!
```

**Using Different Profiles:**

```bash
# AI/ML workloads - larger buffers for maximum throughput
export RUSTFS_BUFFER_PROFILE=AiTraining
./rustfs /data

# Web workloads - smaller buffers for high concurrency
export RUSTFS_BUFFER_PROFILE=WebWorkload
./rustfs /data

# Secure environments - compliance-focused
export RUSTFS_BUFFER_PROFILE=SecureStorage
./rustfs /data
```

**Reverting to Legacy Behavior:**

```bash
# If you encounter issues or need exact PR #869 behavior
export RUSTFS_BUFFER_PROFILE_DISABLE=true
./rustfs /data
```

## Phase 4: Full Integration (Current Implementation)

**🚀 NEW: Profile-only implementation with performance metrics!**

Phase 4 represents the final stage of the adaptive buffer sizing system, providing a unified, profile-based approach with optional performance monitoring.

### Key Features

1. **Deprecated Legacy Function**
   - `get_adaptive_buffer_size()` is now deprecated
   - Maintained for backward compatibility only
   - All new code uses the workload profile system

2. **Profile-Only Implementation**
   - Single entry point: `get_buffer_size_opt_in()`
   - All buffer sizes come from workload profiles
   - Even "disabled" mode uses GeneralPurpose profile (no hardcoded values)

3. **Performance Metrics** (Optional)
   - Built-in metrics collection with `metrics` feature flag
   - Tracks buffer size selections
   - Monitors buffer-to-file size ratios
   - Helps optimize profile configurations

### Unified Buffer Sizing

```rust
// Phase 4: Single, unified implementation
fn get_buffer_size_opt_in(file_size: i64) -> usize {
    // Enabled by default (Phase 3)
    // Uses workload profiles exclusively
    // Optional metrics collection
}
```

### Performance Monitoring

When compiled with the `metrics` feature flag:

```bash
# Build with metrics support
cargo build --features metrics

# Run and collect metrics
./rustfs /data

# Metrics collected:
# - buffer_size_bytes: Histogram of selected buffer sizes
# - buffer_size_selections: Counter of buffer size calculations
# - buffer_to_file_ratio: Ratio of buffer size to file size
```

### Migration from Phase 3

No action required! Phase 4 is fully backward compatible with Phase 3:

```bash
# Phase 3 usage continues to work
./rustfs /data
export RUSTFS_BUFFER_PROFILE=AiTraining
./rustfs /data

# Phase 4 adds deprecation warnings for direct legacy function calls
# (if you have custom code calling get_adaptive_buffer_size)
```

### What Changed

| Aspect | Phase 3 | Phase 4 |
|--------|---------|---------|
| Legacy Function | Active | Deprecated (still works) |
| Implementation | Hybrid (legacy fallback) | Profile-only |
| Metrics | None | Optional via feature flag |
| Buffer Source | Profiles or hardcoded | Profiles only |

### Benefits

1. **Simplified Codebase**
   - Single implementation path
   - Easier to maintain and optimize
   - Consistent behavior across all scenarios

2. **Better Observability**
   - Optional metrics for performance monitoring
   - Data-driven profile optimization
   - Production usage insights

3. **Future-Proof**
   - No legacy code dependencies
   - Easy to add new profiles
   - Extensible for future enhancements

### Code Example

**Phase 3 (Still Works):**
```rust
// Enabled by default
let buffer_size = get_buffer_size_opt_in(file_size);
```

**Phase 4 (Recommended):**
```rust
// Same call, but now with optional metrics and profile-only implementation
let buffer_size = get_buffer_size_opt_in(file_size);
// Metrics automatically collected if feature enabled
```

**Deprecated (Backward Compatible):**
```rust
// This still works but generates deprecation warnings
#[allow(deprecated)]
let buffer_size = get_adaptive_buffer_size(file_size);
```

### Enabling Metrics

Add to `Cargo.toml`:
```toml
[dependencies]
rustfs = { version = "*", features = ["metrics"] }
```

Or build with feature flag:
```bash
cargo build --features metrics --release
```

### Metrics Dashboard

When metrics are enabled, you can visualize:

- **Buffer Size Distribution**: Most common buffer sizes used
- **Profile Effectiveness**: How well profiles match actual workloads
- **Memory Efficiency**: Buffer-to-file size ratios
- **Usage Patterns**: File size distribution and buffer selection trends

Use your preferred metrics backend (Prometheus, InfluxDB, etc.) to collect and visualize these metrics.

## Phase 2: Opt-In Usage (Previous Implementation)

**Note:** Phase 2 documentation is kept for historical reference. The current version uses Phase 4 (Full Integration).

<details>
<summary>Click to expand Phase 2 documentation</summary>

Starting from Phase 2 of the migration path, workload profiles can be enabled via environment variables or command-line arguments.

### Environment Variables

Enable workload profiling using these environment variables:

```bash
# Enable buffer profiling (opt-in)
export RUSTFS_BUFFER_PROFILE_ENABLE=true

# Set the workload profile
export RUSTFS_BUFFER_PROFILE=AiTraining

# Start RustFS
./rustfs /data
```

### Command-Line Arguments

Alternatively, use command-line flags:

```bash
# Enable buffer profiling with AI training profile
./rustfs --buffer-profile-enable --buffer-profile AiTraining /data

# Enable buffer profiling with web workload profile
./rustfs --buffer-profile-enable --buffer-profile WebWorkload /data

# Disable buffer profiling (use legacy behavior)
./rustfs /data
```

### Behavior

When `RUSTFS_BUFFER_PROFILE_ENABLE=false` (default in Phase 2):
- Uses the original adaptive buffer sizing from PR #869
- No breaking changes to existing deployments
- Buffer sizes: 64KB, 256KB, 1MB based on file size

When `RUSTFS_BUFFER_PROFILE_ENABLE=true`:
- Uses the configured workload profile
- Allows for workload-specific optimizations
- Buffer sizes vary based on the selected profile

</details>



## Configuration Validation

All buffer configurations are validated to ensure correctness:

```rust
let config = BufferConfig { /* ... */ };
config.validate()?; // Returns Err if invalid
```

**Validation Rules:**
- `min_size` must be > 0
- `max_size` must be >= `min_size`
- `default_unknown` must be between `min_size` and `max_size`
- Thresholds must be in ascending order
- Buffer sizes in thresholds must be within `[min_size, max_size]`

## Environment Detection

The system automatically detects special operating system environments by reading `/etc/os-release` on Linux systems:

```rust
if let Some(profile) = WorkloadProfile::detect_os_environment() {
    // Returns SecureStorage profile for Kylin, NeoKylin, UOS, etc.
    let buffer_size = profile.config().calculate_buffer_size(file_size);
}
```

**Detected Environments:**
- Kylin (麒麟)
- NeoKylin (中标麒麟)
- UOS / Unity OS (统信)
- OpenKylin (开放麒麟)

## Performance Considerations

### Memory Usage

Different profiles have different memory footprints:

| Profile | Min Buffer | Max Buffer | Typical Memory |
|---------|-----------|-----------|----------------|
| GeneralPurpose | 64KB | 1MB | Low-Medium |
| AiTraining | 512KB | 4MB | High |
| DataAnalytics | 128KB | 2MB | Medium |
| WebWorkload | 32KB | 256KB | Low |
| IndustrialIoT | 64KB | 512KB | Low |
| SecureStorage | 32KB | 256KB | Low |

### Throughput Impact

Larger buffers generally provide better throughput for large files by reducing system call overhead:

- **Small buffers (32-64KB)**: Lower memory, more syscalls, suitable for many small files
- **Medium buffers (128-512KB)**: Balanced approach for mixed workloads
- **Large buffers (1-4MB)**: Maximum throughput, best for large sequential reads

### Concurrency Considerations

For high-concurrency scenarios (e.g., WebWorkload):
- Smaller buffers reduce per-connection memory
- Allows more concurrent connections
- Better overall system resource utilization

## Best Practices

### 1. Choose the Right Profile

Select the profile that matches your primary workload:

```rust
// AI/ML training
WorkloadProfile::AiTraining

// Web application
WorkloadProfile::WebWorkload

// General purpose storage
WorkloadProfile::GeneralPurpose
```

### 2. Monitor Memory Usage

In production, monitor memory consumption:

```rust
// For memory-constrained environments, use smaller buffers
WorkloadProfile::SecureStorage  // or IndustrialIoT
```

### 3. Test Performance

Benchmark your specific workload to verify the profile choice:

```bash
# Run performance tests with different profiles
cargo test --release -- --ignored performance_tests
```

### 4. Consider File Size Distribution

If you know your typical file sizes:

- Mostly small files (< 1MB): Use `WebWorkload` or `SecureStorage`
- Mostly large files (> 100MB): Use `AiTraining` or `DataAnalytics`
- Mixed sizes: Use `GeneralPurpose`

### 5. Compliance Requirements

For regulated environments:

```rust
// Automatically uses SecureStorage on detected secure OS
let config = RustFSBufferConfig::with_auto_detect();

// Or explicitly set SecureStorage
let config = RustFSBufferConfig::new(WorkloadProfile::SecureStorage);
```

## Integration Examples

### S3 Put Object

```rust
async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
    let size = req.input.content_length.unwrap_or(-1);
    
    // Use workload-aware buffer sizing
    let buffer_size = get_adaptive_buffer_size_with_profile(
        size,
        Some(WorkloadProfile::GeneralPurpose)
    );
    
    let body = tokio::io::BufReader::with_capacity(
        buffer_size,
        StreamReader::new(body)
    );
    
    // Process upload...
}
```

### Multipart Upload

```rust
async fn upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<S3Response<UploadPartOutput>> {
    let size = req.input.content_length.unwrap_or(-1);
    
    // For large multipart uploads, consider using AiTraining profile
    let buffer_size = get_adaptive_buffer_size_with_profile(
        size,
        Some(WorkloadProfile::AiTraining)
    );
    
    let body = tokio::io::BufReader::with_capacity(
        buffer_size,
        StreamReader::new(body_stream)
    );
    
    // Process part upload...
}
```

## Troubleshooting

### High Memory Usage

If experiencing high memory usage:

1. Switch to a more conservative profile:
   ```rust
   WorkloadProfile::WebWorkload  // or SecureStorage
   ```

2. Set explicit memory limits in custom configuration:
   ```rust
   let config = BufferConfig {
       min_size: 16 * 1024,
       max_size: 128 * 1024,  // Cap at 128KB
       // ...
   };
   ```

### Low Throughput

If experiencing low throughput for large files:

1. Use a more aggressive profile:
   ```rust
   WorkloadProfile::AiTraining  // or DataAnalytics
   ```

2. Increase buffer sizes in custom configuration:
   ```rust
   let config = BufferConfig {
       max_size: 4 * 1024 * 1024,  // 4MB max buffer
       // ...
   };
   ```

### Streaming/Unknown Size Handling

For chunked transfers or streaming:

```rust
// Pass -1 for unknown size
let buffer_size = get_adaptive_buffer_size_with_profile(-1, None);
// Returns the profile's default_unknown size
```

## Technical Implementation

### Algorithm

The buffer size is selected based on file size thresholds:

```rust
pub fn calculate_buffer_size(&self, file_size: i64) -> usize {
    if file_size < 0 {
        return self.default_unknown;
    }
    
    for (threshold, buffer_size) in &self.thresholds {
        if file_size < *threshold {
            return (*buffer_size).clamp(self.min_size, self.max_size);
        }
    }
    
    self.max_size
}
```

### Thread Safety

All configuration structures are:
- Immutable after creation
- Safe to share across threads
- Cloneable for per-thread customization

### Performance Overhead

- Configuration lookup: O(n) where n = number of thresholds (typically 2-4)
- Negligible overhead compared to I/O operations
- Configuration can be cached per-connection

## Migration Guide

### From PR #869

The original `get_adaptive_buffer_size` function is preserved for backward compatibility:

```rust
// Old code (still works)
let buffer_size = get_adaptive_buffer_size(file_size);

// New code (recommended)
let buffer_size = get_adaptive_buffer_size_with_profile(
    file_size,
    Some(WorkloadProfile::GeneralPurpose)
);
```

### Upgrading Existing Code

1. **Identify workload type** for each use case
2. **Replace** `get_adaptive_buffer_size` with `get_adaptive_buffer_size_with_profile`
3. **Choose** appropriate profile
4. **Test** performance impact

## References

- [PR #869: Fix large file upload freeze with adaptive buffer sizing](https://github.com/rustfs/rustfs/pull/869)
- [Performance Testing Guide](./PERFORMANCE_TESTING.md)
- [Configuration Documentation](./ENVIRONMENT_VARIABLES.md)

## License

Copyright 2024 RustFS Team

Licensed under the Apache License, Version 2.0.
