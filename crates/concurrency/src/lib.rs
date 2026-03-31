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

//! # RustFS Concurrency Management
//!
//! This crate provides comprehensive concurrency management for RustFS,
//! including timeout control, lock optimization, deadlock detection,
//! backpressure management, and I/O scheduling.
//!
//! ## Features
//!
//! All features are controlled by feature flags and can be enabled/disabled at compile time:
//!
//! - **timeout**: Dynamic timeout calculation based on data size and transfer rate
//! - **lock**: Early lock release to reduce contention
//! - **deadlock**: Request tracking and cycle detection
//! - **backpressure**: Buffer-based flow control
//! - **scheduler**: Adaptive buffer sizing and priority queuing
//!
//! ## Architecture
//!
//! ```text
//! rustfs-concurrency (Business Layer)
//!     ├── timeout (Timeout Control)
//!     ├── lock (Lock Optimization)
//!     ├── deadlock (Deadlock Detection)
//!     ├── backpressure (Backpressure Management)
//!     └── scheduler (I/O Scheduling)
//!          │
//!          ├── rustfs-io-core (Core Algorithms)
//!          └── rustfs-io-metrics (Metrics Collection)
//! ```
//!
//! ## Usage
//!
//! ```rust,no_run
//! use rustfs_concurrency::{ConcurrencyConfig, ConcurrencyManager};
//!
//! # #[tokio::main]
//! # async fn main() {
//! // Create manager with all features enabled
//! let config = ConcurrencyConfig::default();
//! let manager = ConcurrencyManager::new(config);
//!
//! // Start services
//! manager.start().await;
//!
//! // Use timeout control (if enabled)
//! if manager.is_timeout_enabled() {
//!     let timeout_manager = manager.timeout();
//!     let _ = timeout_manager;
//! }
//!
//! // Use lock optimization (if enabled)
//! if manager.is_lock_enabled() {
//!     let lock_manager = manager.lock();
//!     let _ = lock_manager;
//! }
//!
//! // Stop services
//! manager.stop().await;
//! # }
//! ```

#![deny(missing_docs)]
#![deny(unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]

// Re-export core types from io-core
pub use rustfs_io_core::{
    // Backpressure types
    BackpressureConfig as CoreBackpressureConfig,
    BackpressureMonitor as CoreBackpressureMonitor,
    BackpressureState,

    // Deadlock types
    DeadlockDetector as CoreDeadlockDetector,
    IoLoadLevel,
    IoLoadMetrics,
    IoPriority,
    // Scheduler types
    IoScheduler,
    IoSchedulingContext,
    LockInfo,
    LockOptimizer as CoreLockOptimizer,

    // Lock types
    LockStats as CoreLockStats,
    LockType,
    // Timeout types
    OperationProgress,
    TimeoutError,
    TimeoutStats,
    WaitGraphEdge,

    calculate_adaptive_timeout,
    estimate_bytes_per_second,
};

// Module declarations with feature gates
#[cfg(feature = "timeout")]
mod timeout;

#[cfg(feature = "lock")]
mod lock;

#[cfg(feature = "deadlock")]
mod deadlock;

#[cfg(feature = "backpressure")]
mod backpressure;

#[cfg(feature = "scheduler")]
mod scheduler;

// Public module exports with feature gates
#[cfg(feature = "timeout")]
pub use timeout::{TimeoutConfig, TimeoutGuard, TimeoutManager};

#[cfg(feature = "lock")]
pub use lock::{LockConfig, LockManager, LockScopeGuard, OptimizedLockGuard};

#[cfg(feature = "deadlock")]
pub use deadlock::{DeadlockConfig, DeadlockManager, RequestTracker};

#[cfg(feature = "backpressure")]
pub use backpressure::{BackpressureConfig, BackpressureManager, BackpressurePipe};

#[cfg(feature = "scheduler")]
pub use scheduler::{IoStrategy, SchedulerConfig, SchedulerManager};

// Configuration
mod config;
pub use config::{ConcurrencyConfig, ConcurrencyFeatures};

// Manager
mod manager;
pub use manager::{ConcurrencyManager, GetObjectCacheEligibility, GetObjectQueueSnapshot};

// Prelude for convenient imports
pub mod prelude {
    //! Prelude module for convenient imports

    #[cfg(feature = "timeout")]
    pub use crate::timeout::{TimeoutConfig, TimeoutGuard, TimeoutManager};

    #[cfg(feature = "lock")]
    pub use crate::lock::{LockConfig, LockManager, LockScopeGuard, OptimizedLockGuard};

    #[cfg(feature = "deadlock")]
    pub use crate::deadlock::{DeadlockConfig, DeadlockManager, RequestTracker};

    #[cfg(feature = "backpressure")]
    pub use crate::backpressure::{BackpressureConfig, BackpressureManager, BackpressurePipe};

    #[cfg(feature = "scheduler")]
    pub use crate::scheduler::{IoStrategy, SchedulerConfig, SchedulerManager};

    pub use crate::{ConcurrencyConfig, ConcurrencyFeatures, ConcurrencyManager};
}
