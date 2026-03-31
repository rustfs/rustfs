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

**rustfs-io-core** is the core I/O scheduling module for [RustFS](https://rustfs.com), a distributed object storage system. It provides:

- **I/O Scheduler**: Adaptive buffer size calculation and load management
- **Priority Queue**: Request priority scheduling with starvation prevention
- **Backpressure Control**: System overload protection with graceful degradation
- **Deadlock Detection**: Wait-for graph based deadlock detection algorithm
- **Lock Optimizer**: Adaptive spin lock optimization
- **Timeout Wrapper**: Dynamic timeout calculation and operation progress tracking

## Features

### I/O Scheduler

Adaptive I/O scheduling with dynamic buffer size calculation based on file size, access pattern, and system load:

```rust
use rustfs_io_core::{IoScheduler, IoSchedulerConfig, IoLoadLevel};
use rustfs_io_core::io_profile::{StorageMedia, AccessPattern};

// Create scheduler
let config = IoSchedulerConfig {
    max_concurrent_reads: 64,
    base_buffer_size: 64 * 1024,  // 64 KB
    max_buffer_size: 1024 * 1024, // 1 MB
    ..Default::default()
};
let scheduler = IoScheduler::new(config);

// Calculate optimal buffer size
let buffer_size = calculate_optimal_buffer_size(
    10 * 1024 * 1024,  // 10 MB file
    64 * 1024,         // base buffer
    true,              // sequential access
    4,                 // concurrent requests
    StorageMedia::Ssd,
    IoLoadLevel::Low,
);
```

### Priority Queue

Priority queue with starvation prevention:

```rust
use rustfs_io_core::{IoPriorityQueue, IoPriority, IoQueueStatus};

let queue = IoPriorityQueue::<()>::new(100);

// Enqueue request
let request_id = queue.enqueue(IoPriority::High, (), 1024);

// Dequeue request
if let Some((priority, data)) = queue.dequeue() {
    println!("Processing priority {:?} request", priority);
}

// Check queue status
let status = queue.status();
println!("High priority waiting: {}", status.high_priority_waiting);
```

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

### Timeout Wrapper

Dynamic timeout calculation:

```rust
use rustfs_io_core::{RequestTimeoutWrapper, TimeoutConfig};
use std::time::Duration;

let config = TimeoutConfig {
    base_timeout: Duration::from_secs(5),
    timeout_per_mb: Duration::from_millis(100),
    max_timeout: Duration::from_secs(300),
    ..Default::default()
};
let wrapper = RequestTimeoutWrapper::new(config);

// Calculate operation timeout
let timeout = wrapper.calculate_timeout(10 * 1024 * 1024);  // 10 MB
```

## Buffer Size Calculation

Multiple buffer size calculation functions are provided:

```rust
use rustfs_io_core::{
    get_concurrency_aware_buffer_size,
    get_advanced_buffer_size,
    get_buffer_size_for_media,
    calculate_optimal_buffer_size,
    KI_B, MI_B,
};
use rustfs_io_core::io_profile::StorageMedia;

// Basic calculation
let size1 = get_concurrency_aware_buffer_size(1024 * 1024, 64 * 1024);

// Advanced calculation (considering access pattern)
let size2 = get_advanced_buffer_size(10 * 1024 * 1024, 64 * 1024, true);

// Media type optimization
let size3 = get_buffer_size_for_media(64 * 1024, StorageMedia::Ssd);

// Comprehensive calculation
let size4 = calculate_optimal_buffer_size(
    100 * 1024 * 1024,  // 100 MB file
    64 * 1024,          // base buffer
    true,               // sequential access
    4,                  // concurrent requests
    StorageMedia::Nvme,
    IoLoadLevel::Low,
);
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `RUSTFS_MAX_CONCURRENT_READS` | Max concurrent reads | 64 |
| `RUSTFS_BASE_BUFFER_SIZE` | Base buffer size | 65536 |
| `RUSTFS_MAX_BUFFER_SIZE` | Max buffer size | 1048576 |
| `RUSTFS_IO_TIMEOUT_SECS` | I/O timeout seconds | 30 |

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
│   ├── scheduler.rs        # I/O scheduler
│   ├── io_priority_queue.rs # Priority queue
│   ├── backpressure.rs     # Backpressure control
│   ├── deadlock_detector.rs # Deadlock detection
│   ├── lock_optimizer.rs   # Lock optimization
│   ├── timeout_wrapper.rs  # Timeout wrapper
│   └── io_profile.rs       # I/O profile
└── Cargo.toml
```

## Testing

```bash
# Run all tests
cargo test --package rustfs-io-core

# Run specific tests
cargo test --package rustfs-io-core --lib scheduler

# Run benchmarks
cargo bench --package rustfs-io-core
```

## Documentation

- [API Documentation](https://docs.rs/rustfs-io-core)
- [I/O Scheduler Design](./docs/scheduler-design.md)
- [Backpressure Control Design](./docs/backpressure-design.md)
- [Deadlock Detection Algorithm](./docs/deadlock-detection.md)

## Related Modules

- **rustfs-io-metrics**: Metrics collection and configuration
- **rustfs**: Main storage service

## License

Apache License 2.0
