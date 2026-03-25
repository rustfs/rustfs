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
//! This module provides zero-copy readers and writers that minimize memory
//! allocations and data copying during I/O operations.
//!
//! # Features
//!
//! - Memory-mapped file reading (mmap)
//! - Bytes-based zero-copy wrapping
//! - AsyncRead/AsyncWrite trait implementations
//! - Optional Direct I/O support (Linux only)
//!
//! # Example
//!
//! ```ignore
//! use rustfs::storage::zero_copy::ZeroCopyObjectReader;
//!
//! // Create from file using mmap
//! let reader = ZeroCopyObjectReader::from_file_mmap(&file, offset, size).await?;
//!
//! // Create from existing bytes (zero-copy)
//! let reader = ZeroCopyObjectReader::from_bytes(bytes_data);
//! ```

pub mod reader;

pub use reader::{ZeroCopyObjectReader, ZeroCopyReadError};

#[cfg(feature = "direct-io")]
pub mod direct_io;

#[cfg(feature = "direct-io")]
pub use direct_io::DirectIoReader;
