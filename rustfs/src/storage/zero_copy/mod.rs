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

//! Zero-copy data path module for optimized I/O operations.
//!
//! This module re-exports zero-copy readers, writers, and buffer pools from
//! rustfs-zero-copy-core, minimizing memory allocations and data copying during I/O operations.
//!
//! # Features
//!
//! - Memory-mapped file reading (mmap)
//! - Bytes-based zero-copy wrapping
//! - Tiered BytesPool for buffer management
//! - AsyncRead/AsyncWrite trait implementations
//! - Optional Direct I/O support (Linux only)
//!
//! # Example
//!
//! ```ignore
//! use rustfs::storage::zero_copy::{ZeroCopyObjectReader, BytesPool};
//!
//! // Create from file using mmap
//! let reader = ZeroCopyObjectReader::from_file_mmap(&file, offset, size).await?;
//!
//! // Create from existing bytes (zero-copy)
//! let reader = ZeroCopyObjectReader::from_bytes(bytes_data);
//!
//! // Use BytesPool
//! let pool = BytesPool::new_tiered();
//! let mut buffer = pool.acquire_buffer(8192).await;
//! ```

// Re-export from rustfs-zero-copy-core for unified zero-copy types
pub use rustfs_zero_copy_core::{
    BytesPool, BytesPoolConfig, BytesPoolMetrics, PooledBuffer,
    ZeroCopyObjectReader, ZeroCopyReadError,
};

#[cfg(target_os = "linux")]
pub use rustfs_zero_copy_core::{DirectIoReader, DirectIoError};

// Re-export from rustfs-zero-copy-metrics crate for unified metrics handling
pub use rustfs_zero_copy_metrics::{
    record_bytes_saved, record_memory_copy_saved, record_zero_copy_read,
    // BytesPool metrics
    record_bytes_pool_acquire, record_bytes_pool_allocated, record_bytes_pool_hit_rate,
    record_bytes_pool_return,
};


