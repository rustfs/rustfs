# rustfs-io-metrics

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml">
    <img src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" alt="CI Status" />
  </a>
  <a href="https://crates.io/crates/rustfs-io-metrics">
    <img src="https://img.shields.io/crates/v/rustfs-io-metrics.svg" alt="Crates.io" />
  </a>
</p>

<p align="center">
  · <a href="https://github.com/rustfs/rustfs">Home</a>
  · <a href="#documentation">Docs</a>
  · <a href="https://github.com/rustfs/rustfs/issues">Issues</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">Discussions</a>
</p>

---

## Overview

**rustfs-io-metrics** is the metrics and configuration module for [RustFS](https://rustfs.com), a distributed object storage system. It provides:

- **Cache Configuration**: L1/L2 tiered cache configuration management
- **Adaptive TTL**: Dynamic TTL adjustment based on access frequency
- **Metrics Collection**: Unified metrics recording and reporting
- **Bandwidth Monitoring**: Real-time bandwidth observation and analysis
- **Performance Metrics**: I/O performance metrics collection
- **Unified Configuration**: Centralized configuration management
- **Exporter Boundary**: Emit via `metrics`, export via `rustfs-obs`, no Prometheus HTTP endpoint

## Features

### Cache Configuration

Tiered cache configuration management:

```rust
use rustfs_io_metrics::{CacheConfig, CacheConfigError};

// Create configuration
let config = CacheConfig::new();

// Validate configuration
if let Err(e) = config.validate() {
    println!("Invalid configuration: {}", e);
}

// Custom configuration
let config = CacheConfig {
    max_capacity: 10_000,
    default_ttl_seconds: 300,
    max_memory_bytes: 100 * 1024 * 1024,  // 100 MB
    ..Default::default()
};
```

### Adaptive TTL

Dynamic TTL adjustment based on access frequency:

```rust
use rustfs_io_metrics::{AdaptiveTTL, AdaptiveTTLStats};
use std::time::Duration;

let config = CacheConfig::new().with_ttl_range(60, 300, 3600);
let ttl = AdaptiveTTL::new(config);

// Cold object (few accesses)
let cold_ttl = ttl.calculate_ttl(Duration::from_secs(60), 1, 0.8);
println!("Cold object TTL: {:?}", cold_ttl);

// Hot object (many accesses)
let hot_ttl = ttl.calculate_ttl(Duration::from_secs(60), 100, 0.8);
println!("Hot object TTL: {:?}", hot_ttl);
```

### Access Tracking

Track cache item access patterns:

```rust
use rustfs_io_metrics::{AccessTracker, AccessRecord};
use std::time::Duration;

let mut tracker = AccessTracker::new(1000, Duration::from_secs(300));

// Record accesses
tracker.record_access("object-key-1", 1024);
tracker.record_access("object-key-1", 1024);
tracker.record_access("object-key-2", 2048);

// Get access count
let count = tracker.get_access_count("object-key-1");
println!("Access count: {}", count);

// Detect hot/cold
if tracker.is_hot("object-key-1", 1) {
    println!("Hot object");
}

// Get top keys
let top_keys = tracker.top_keys(10);
for (key, count) in top_keys {
    println!("{}: {} accesses", key, count);
}
```

### Metrics Recording

Unified metrics recording functions:

```rust
use rustfs_io_metrics::{
    // I/O scheduler metrics
    record_io_scheduler_decision,
    record_io_strategy_change,
    record_io_load_level,
    
    // Cache metrics
    record_cache_size,
    
    // Backpressure metrics
    record_backpressure_event,
    record_backpressure_state,
    
    // Timeout metrics
    record_timeout_event,
    record_operation_duration,
};

// Record I/O scheduler decision
record_io_scheduler_decision("sequential", "high_priority");

// Record cache size
record_cache_size("L1", 1024, 1);

// Record backpressure event
record_backpressure_event("warning", 0.85);

// Record operation timeout
record_timeout_event("GetObject", Duration::from_secs(30));
```

### Unified Configuration

Centralized configuration management:

```rust
use rustfs_io_metrics::{
    IoConfig, CacheSettings, IoSchedulerSettings,
    BackpressureSettings, TimeoutSettings,
};

let config = IoConfig::new()
    .with_cache(CacheSettings::new()
        .with_max_capacity(10_000)
        .with_ttl(std::time::Duration::from_secs(300)))
    .with_scheduler(IoSchedulerSettings::new()
        .with_max_concurrent_reads(64))
    .with_backpressure(BackpressureSettings::new())
    .with_timeout(TimeoutSettings::new());

// Access configuration
println!("Cache capacity: {}", config.cache.max_capacity);
println!("Max concurrent reads: {}", config.scheduler.max_concurrent_reads);
```

## Module Structure

```
rustfs-io-metrics/
├── src/
│   ├── lib.rs               # Module entry
│   ├── cache_config.rs      # Cache configuration
│   ├── adaptive_ttl.rs      # Adaptive TTL
│   ├── config.rs            # Unified configuration
│   ├── io_metrics.rs        # I/O metrics
│   ├── backpressure_metrics.rs # Backpressure metrics
│   ├── deadlock_metrics.rs  # Deadlock metrics
│   ├── lock_metrics.rs      # Lock metrics
│   ├── timeout_metrics.rs   # Timeout metrics
│   ├── bandwidth.rs         # Bandwidth monitoring
│   ├── global_metrics.rs    # Global metrics
│   └── performance.rs       # Performance metrics
└── Cargo.toml
```

## Testing

```bash
# Run all tests
cargo test --package rustfs-io-metrics

# Run specific tests
cargo test --package rustfs-io-metrics --lib adaptive_ttl

# Run benchmarks
cargo bench --package rustfs-io-metrics --bench metrics_pipeline
```

## Documentation

This crate records metrics through the Rust `metrics` crate and leaves
exporting to `rustfs-obs` or the application-level observability pipeline. It
does not expose Prometheus-compatible HTTP endpoints such as
`/rustfs/v2/metrics/cluster` or `/rustfs/v2/metrics/node`.

API documentation can be generated locally:

```bash
cargo doc --package rustfs-io-metrics --no-deps --open
```

Useful source references:

- [Crate API overview](./src/lib.rs)
- [Metrics example](./examples/metrics_example.rs)
- [Configuration module](./src/config.rs)
- [Adaptive TTL module](./src/adaptive_ttl.rs)

## Related Modules

- **rustfs-io-core**: Core I/O scheduling
- **rustfs**: Main storage service

## License

Apache License 2.0
