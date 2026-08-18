# rustfs-io-core

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml">
    <img src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" alt="CI Status" />
  </a>
  <a href="https://docs.rs/rustfs-io-core">
    <img src="https://docs.rs/rustfs-io-core/badge.svg" alt="Documentation" />
  </a>
  <a href="https://crates.io/crates/rustfs-io-core">
    <img src="https://img.shields.io/crates/v/rustfs-io-core.svg" alt="Crates.io" />
  </a>
</p>

<p align="center">
  · <a href="https://github.com/rustfs/rustfs">Home</a>
  · <a href="https://docs.rs/rustfs-io-core">Docs</a>
  · <a href="https://github.com/rustfs/rustfs/issues">Issues</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">Discussions</a>
</p>

---

## Overview

**rustfs-io-core** holds the shared I/O primitives for [RustFS](https://rustfs.com), a distributed object storage system. It provides:

- **Buffer Pool**: Tiered `BytesPool` for buffer reuse
- **Storage Profiling**: Storage-media and access-pattern model (`io_profile`)
- **Scheduler Configuration**: The `IoSchedulerConfig` / `IoPriorityQueueConfig` shapes the storage layer projects into
- **Backpressure Control**: System overload protection with graceful degradation
- **Deadlock Detection**: Wait-for graph based deadlock detection algorithm
- **Lock Optimizer**: Adaptive spin lock optimization
- **Progress Tracking**: Byte progress and staleness for long-running operations

The scheduling algorithm itself lives in `rustfs/src/storage/concurrency/io_schedule.rs`; this crate carries the configuration shapes it projects into, not a second implementation.

## Features

### Backpressure Control

System overload protection:

```rust
use rustfs_io_core::{BackpressureMonitor, BackpressureState, BackpressureConfig};

let config = BackpressureConfig {
    high_watermark: 0.8,  // 80% triggers backpressure
    low_watermark: 0.5,   // 50% releases backpressure
    ..Default::default()
};
let monitor = BackpressureMonitor::new(config);

// Check state
match monitor.state() {
    BackpressureState::Normal => println!("System normal"),
    BackpressureState::Warning => println!("System warning"),
    BackpressureState::Critical => println!("System overloaded"),
}
```

### Deadlock Detection

Wait-for graph based deadlock detection:

```rust
use rustfs_io_core::{DeadlockDetector, LockType};

let detector = DeadlockDetector::with_defaults();

// Register locks
let lock1 = detector.register_lock(LockType::Mutex);
let lock2 = detector.register_lock(LockType::RwLockWrite);

// Record lock acquisition
detector.record_acquire(lock1, 1);  // Thread 1 acquires lock1
detector.record_wait(lock2, 1);     // Thread 1 waits for lock2

// Detect deadlock
if let Some(deadlock) = detector.detect_deadlock() {
    println!("Deadlock detected: {:?}", deadlock);
}
```

### Lock Optimizer

Adaptive spin lock optimization:

```rust
use rustfs_io_core::{LockOptimizer, LockOptimizeConfig};

let optimizer = LockOptimizer::with_defaults();

// Record lock operations
optimizer.on_acquire();
// ... do work ...
optimizer.on_release(std::time::Duration::from_millis(10));

// View statistics
let stats = optimizer.stats();
println!("Locks acquired: {}", stats.total_acquired());
```

### Progress Tracking

Byte progress and staleness for long-running operations:

```rust
use rustfs_io_core::OperationProgress;
use std::time::Duration;

let progress = OperationProgress::new(Some(1000), Duration::from_secs(5));

progress.update(500);
assert_eq!(progress.progress_percent(), Some(50.0));
assert!(!progress.is_stale());
```

## Configuration

### Code Configuration

```rust
use rustfs_io_core::IoSchedulerConfig;

let config = IoSchedulerConfig {
    max_concurrent_reads: 128,
    base_buffer_size: 128 * 1024,
    max_buffer_size: 4 * 1024 * 1024,
    high_priority_threshold: 64 * 1024,
    low_priority_threshold: 4 * 1024 * 1024,
    ..Default::default()
};

// Validate configuration
if let Err(e) = config.validate() {
    panic!("Invalid configuration: {}", e);
}
```

## Module Structure

```
rustfs-io-core/
├── src/
│   ├── lib.rs              # Module entry
│   ├── config.rs           # Configuration types
│   ├── pool.rs             # Tiered buffer pool
│   ├── backpressure.rs     # Backpressure control
│   ├── deadlock_detector.rs # Deadlock detection
│   ├── lock_optimizer.rs   # Lock optimization
│   ├── progress.rs         # Operation progress tracking
│   └── io_profile.rs       # I/O profile
└── Cargo.toml
```

## Testing

```bash
# Run all tests
cargo nextest run --package rustfs-io-core

# Run specific tests
cargo nextest run --package rustfs-io-core -E 'test(backpressure)'
```

## Documentation

- [API Documentation](https://docs.rs/rustfs-io-core)

## Related Modules

- **rustfs-io-metrics**: Metrics collection and configuration
- **rustfs**: Main storage service

## License

Apache License 2.0
