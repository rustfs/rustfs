// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Shared I/O primitives for RustFS.
//!
//! This crate holds the buffer pool and the concurrency-control primitives
//! that the storage layer builds on:
//!
//! - Tiered `BytesPool` for buffer management
//! - Storage-media and access-pattern profiling (`io_profile`)
//! - Scheduler and priority-queue configuration shapes
//! - Backpressure admission, deadlock detection, lock optimization
//! - Progress tracking for long-running operations
//!
//! # Example
//!
//! ```ignore
//! use rustfs_io_core::BytesPool;
//!
//! let pool = BytesPool::new_tiered();
//! let mut buffer = pool.acquire_buffer(8192).await;
//! ```

pub mod backpressure;
pub mod config;
pub mod deadlock_detector;
pub mod io_profile;
pub mod lock_optimizer;
pub mod pool;
pub mod progress;

pub use pool::{BytesPool, BytesPoolConfig, BytesPoolMetrics, PooledBuffer};

// Config exports
pub use config::{ConfigError, IoPriorityQueueConfig, IoSchedulerConfig};

// Backpressure exports
pub use backpressure::{BackpressureConfig, BackpressureError, BackpressureMonitor, BackpressureState};

// Deadlock detector exports
pub use deadlock_detector::{DeadlockDetector, DeadlockDetectorConfig, LockInfo, LockType, WaitGraphEdge};

// Lock optimizer exports
pub use lock_optimizer::{LockGuard, LockOptimizeConfig, LockOptimizer, LockStats};

// Progress tracking exports
pub use progress::OperationProgress;
