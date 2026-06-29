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

//! Buffered I/O reader and writer implementations for RustFS.
//!
//! This crate provides buffered readers and writers for I/O operations.
//! Prefer `BytesBufferedReader`, `BytesMutWriter`, and `AlignedPreadReader`
//! for new code. Historical `ZeroCopy*` and `DirectIo*` names remain exported
//! for backward compatibility.
//!
//! # Features
//!
//! - Memory-mapped file reading (mmap-then-copy) on Unix platforms
//! - Bytes-based buffered wrapping
//! - AsyncRead trait implementations
//! - Tiered BytesPool for buffer management
//! - Aligned pread-based reader (NOT true Direct I/O / O_DIRECT)
//!
//! # Example
//!
//! ```ignore
//! use rustfs_io_core::{BytesBufferedReader, BytesPool};
//! use bytes::Bytes;
//!
//! // Create from existing bytes (zero-copy)
//! let data = Bytes::from("hello world");
//! let reader = BytesBufferedReader::from_bytes(data);
//!
//! // Create from file using buffered reads
//! let reader = BytesBufferedReader::from_file_read(&file, 0, 1024).await?;
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
pub use direct_io::{AlignedPreadError, AlignedPreadReader};
#[cfg(target_os = "linux")]
#[allow(deprecated)]
pub use direct_io::{DirectIoError, DirectIoReader};
pub use pool::{BytesPool, BytesPoolConfig, BytesPoolMetrics, PooledBuffer};
#[allow(deprecated)]
pub use reader::ZeroCopyObjectReader;
pub use reader::{BytesBufferedReader, ZeroCopyReadError};
#[allow(deprecated)]
pub use writer::ZeroCopyObjectWriter;
pub use writer::{BytesMutWriter, ZeroCopyWriteError};

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
