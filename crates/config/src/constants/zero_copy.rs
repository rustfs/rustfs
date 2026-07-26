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

//! Mmap-based read I/O configuration constants.
//!
//! This module defines environment variables and default values for mmap-based
//! read operations. Note: the legacy "zero_copy" env var is kept as a
//! deprecated compatibility alias; the actual implementation performs
//! mmap-then-copy, not true zero-copy.

// =============================================================================
// Mmap Read Configuration
// =============================================================================

/// Environment variable for mmap-based read enable.
///
/// When enabled, uses mmap (Unix) or optimized reads for file access.
/// This reduces memory copies from 3-4 to 1, lowering CPU usage and
/// improving latency for large object reads.
///
/// - Purpose: Enable or disable mmap-based read operations
/// - Acceptable values: `"true"` / `"false"` (case-insensitive) or a boolean typed config
/// - Semantics: When enabled, uses mmap on Unix systems for memory-mapped file reads;
///   falls back to regular I/O on non-Unix platforms or when mmap fails
/// - Example: `export RUSTFS_OBJECT_MMAP_READ_ENABLE=true`
pub const ENV_OBJECT_MMAP_READ_ENABLE: &str = "RUSTFS_OBJECT_MMAP_READ_ENABLE";

/// Deprecated compatibility alias for mmap-based read enable.
///
/// Prefer [`ENV_OBJECT_MMAP_READ_ENABLE`]. When both variables are set, the
/// canonical mmap-read variable takes precedence.
pub const ENV_OBJECT_ZERO_COPY_ENABLE: &str = "RUSTFS_OBJECT_ZERO_COPY_ENABLE";

/// Default: mmap-based reads are enabled.
///
/// Uses memory mapping (mmap) on Unix systems, then copies data into owned
/// Bytes. This is faster than multiple read+copy passes but is NOT true
/// zero-copy (the data is still copied once from the mmap region).
///
/// On non-Unix platforms or when mmap fails, the system automatically falls back
/// to regular I/O without errors.
pub const DEFAULT_OBJECT_MMAP_READ_ENABLE: bool = true;

/// Deprecated compatibility alias for mmap-based read default.
///
/// Prefer [`DEFAULT_OBJECT_MMAP_READ_ENABLE`].
pub const DEFAULT_OBJECT_ZERO_COPY_ENABLE: bool = DEFAULT_OBJECT_MMAP_READ_ENABLE;

/// Environment variable capping the byte length a single mmap-copy read may
/// materialize in memory.
///
/// The mmap-copy read path returns the whole requested range as one owned
/// allocation before the first byte is served. GET/heal shard reads request
/// the entire part span in one call, so for a large single-part object
/// (e.g. a multi-gigabyte non-multipart upload) an uncapped mmap-copy read
/// allocates the whole shard in memory — stalling first-byte latency past the
/// disk-read timeout and OOM-killing memory-limited deployments
/// (<https://github.com/rustfs/rustfs/issues/5123>). Reads longer than this
/// cap fall back to the bounded streaming reader instead.
///
/// - Purpose: Bound per-shard-read memory for mmap-based reads
/// - Acceptable values: byte count as an unsigned integer; `0` disables
///   mmap-copy for all non-empty reads (every read streams)
/// - Example: `export RUSTFS_OBJECT_MMAP_READ_MAX_LENGTH=8388608`
pub const ENV_OBJECT_MMAP_READ_MAX_LENGTH: &str = "RUSTFS_OBJECT_MMAP_READ_MAX_LENGTH";

/// Default mmap-copy read length cap: 32 MiB per shard read.
///
/// Large enough that typical multipart part shards (parts up to a few hundred
/// megabytes across the erasure set) keep the mmap fast path, small enough
/// that whole-part reads of huge single-part objects stream instead of
/// materializing gigabytes per shard.
///
/// The cap bounds memory per shard reader, so a single part read can still
/// materialize up to `data_shards x cap` bytes; raising the cap raises that
/// per-request bound proportionally.
pub const DEFAULT_OBJECT_MMAP_READ_MAX_LENGTH: usize = 32 * 1024 * 1024;
