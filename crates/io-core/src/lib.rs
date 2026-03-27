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
//! allocations and data copying during I/O operations. It has no dependencies
//! on other RustFS crates, making it safe to use from anywhere in the codebase
//! without creating cyclic dependencies.
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

pub mod bufreader_optimizer;
pub mod direct_io;
pub mod io_profile;
pub mod pool;
pub mod reader;
pub mod shared_memory;
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
