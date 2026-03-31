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

//! Zero-copy core reader and writer implementations for RustFS.
//!
//! This crate provides zero-copy readers and writers that minimize memory
//! allocations and data copying during I/O operations. It depends on
//! `rustfs-io-metrics` for metrics reporting and is designed to avoid
//! introducing cyclic dependencies in the RustFS crate graph.
//!
//! # Features
//!
//! - Memory-mapped file reading (mmap) on Unix platforms
//! - Bytes-based zero-copy wrapping
//! - AsyncRead trait implementations
//! - Tiered BytesPool for buffer management
//! - Optional Direct I/O support (Linux only)
//!
//! # Example
//!
//! ```ignore
//! use rustfs_io_core::{ZeroCopyObjectReader, BytesPool};
//! use bytes::Bytes;
//!
//! // Create from existing bytes (zero-copy)
//! let data = Bytes::from("hello world");
//! let reader = ZeroCopyObjectReader::from_bytes(data);
//!
//! // Create from file using mmap (Unix only)
//! #[cfg(unix)]
//! let reader = ZeroCopyObjectReader::from_file_mmap(&file, 0, 1024).await?;
//!
//! // Use BytesPool
//! let pool = BytesPool::new_tiered();
//! let mut buffer = pool.acquire_buffer(8192).await;
//! ```

pub mod backpressure;
pub mod bufreader_optimizer;
pub mod config;
pub mod deadlock_detector;
pub mod direct_io;
pub mod io_priority_queue;
pub mod io_profile;
pub mod lock_optimizer;
pub mod pool;
pub mod reader;
pub mod scheduler;
pub mod shared_memory;
pub mod timeout_wrapper;
pub mod writer;

#[cfg(target_os = "linux")]
pub use direct_io::{DirectIoError, DirectIoReader};
pub use pool::{BytesPool, BytesPoolConfig, BytesPoolMetrics, PooledBuffer};
pub use reader::{ZeroCopyObjectReader, ZeroCopyReadError};
pub use writer::{ZeroCopyObjectWriter, ZeroCopyWriteError};

// BufReader optimizer exports
pub use bufreader_optimizer::{BufReaderConfig, BufReaderOptimizer, BufReaderStats, BufferedSource};

// Shared memory exports
pub use shared_memory::{ArcData, ArcMetadata, SharedMemoryConfig, SharedMemoryPool, SharedMemoryStats};

// Config exports
pub use config::{ConfigError, IoPriorityQueueConfig, IoSchedulerConfig};

// Scheduler exports
pub use scheduler::{
    BandwidthTier, IoLoadLevel, IoLoadMetrics, IoPriority, IoScheduler, IoSchedulingContext, IoStrategy, KI_B, MI_B,
    calculate_optimal_buffer_size, get_advanced_buffer_size, get_buffer_size_for_media, get_concurrency_aware_buffer_size,
};

// Priority queue exports
pub use io_priority_queue::{IoPriorityQueue, IoQueueStatus, IoRequest};

// Backpressure exports
pub use backpressure::{BackpressureConfig, BackpressureError, BackpressureMonitor, BackpressureState};

// Deadlock detector exports
pub use deadlock_detector::{DeadlockDetector, DeadlockDetectorConfig, LockInfo, LockType, WaitGraphEdge};

// Lock optimizer exports
pub use lock_optimizer::{LockGuard, LockOptimizeConfig, LockOptimizer, LockStats};

// Timeout wrapper exports
pub use timeout_wrapper::{
    OperationProgress, RequestTimeoutWrapper, TimeoutConfig, TimeoutError, TimeoutStats, calculate_adaptive_timeout,
    estimate_bytes_per_second,
};
