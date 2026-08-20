# Changelog

All notable changes to the rustfs-io-core and rustfs-io-metrics crates will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Removed

#### rustfs-io-core
- **Zero-consumer modules** (added in 0.0.5): `reader`, `writer`, `bufreader_optimizer`, `shared_memory`, `direct_io`, `timeout_wrapper`, `io_priority_queue`, and `scheduler` had no caller in the workspace and were removed (rustfs/backlog#1824). The scheduling algorithm and the request timeout wrapper that RustFS actually runs live in `rustfs/src/storage/`; this crate keeps the config shapes they project into. `OperationProgress` moved to the new `progress` module and is still exported as `rustfs_io_core::OperationProgress`.

#### rustfs-io-metrics
- **Unified configuration** (added in 0.0.5): the zero-consumer `IoConfig`, `CacheSettings`, `IoSchedulerSettings`, `BackpressureSettings`, `TimeoutSettings`, `DeadlockDetectionSettings` types and their `DEFAULT_*` constants were removed (rustfs/rustfs#6008); rustfs-io-core's `IoSchedulerConfig`/`BackpressureConfig` remain the canonical configuration types.

## [0.0.5] - 2025-01-XX

### Added

#### rustfs-io-core
- **IoScheduler**: Adaptive I/O scheduler with buffer size calculation
- **IoPriorityQueue**: Priority queue with starvation prevention
- **BackpressureMonitor**: System overload protection with dual watermark
- **DeadlockDetector**: Wait-for graph based deadlock detection
- **LockOptimizer**: Adaptive spin lock optimization
- **RequestTimeoutWrapper**: Dynamic timeout calculation
- **Buffer size functions**: `calculate_optimal_buffer_size`, `get_buffer_size_for_media`, etc.
- **Configuration types**: `IoSchedulerConfig`, `BackpressureConfig`, `DeadlockDetectorConfig`, etc.

#### rustfs-io-metrics
- **CacheConfig**: L1/L2 tiered cache configuration
- **AdaptiveTTL**: Dynamic TTL adjustment based on access frequency
- **AccessTracker**: Cache item access pattern tracking
- **Metrics recording functions**: I/O, cache, backpressure, deadlock, lock, timeout metrics
- **Unified configuration**: `IoConfig`, `CacheSettings`, `IoSchedulerSettings`, etc.
- **Bandwidth monitoring**: Real-time bandwidth observation

### Changed
- Migrated core I/O scheduling algorithms from `rustfs::storage::concurrency` to `rustfs-io-core`
- Migrated metrics and configuration to `rustfs-io-metrics`
- Updated `rustfs::storage::concurrency::mod.rs` to re-export new module types
- Added API compatibility tests

### Fixed
- Improved buffer size calculation for different storage media
- Enhanced deadlock detection with cycle detection algorithm
- Better backpressure state transitions

### Documentation
- Added comprehensive README.md for both crates
- Added design documentation for I/O scheduler, backpressure, deadlock detection
- Added metrics guide and configuration reference
- Added runnable example code

### Migration Notes
- All original APIs in `rustfs::storage::concurrency` are preserved
- New types are re-exported for gradual migration
- No breaking changes to existing code

## [0.0.4] - Previous Version

### Note
This changelog starts with version 0.0.5 which includes the concurrency module migration.
For previous versions, see the git history.
