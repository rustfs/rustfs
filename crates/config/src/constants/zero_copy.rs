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
