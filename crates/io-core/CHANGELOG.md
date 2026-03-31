# Changelog

All notable changes to the rustfs-io-core and rustfs-io-metrics crates will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
