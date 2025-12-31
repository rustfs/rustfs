//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//! HTTP Response Compression Configuration
//!
//! This module provides configuration options for HTTP response compression.
//! By default, compression is disabled (aligned with MinIO behavior).
//! When enabled via `RUSTFS_COMPRESS_ENABLE=on`, compression can be configured
//! to apply only to specific file extensions, MIME types, and minimum file sizes.

/// Environment variable to enable/disable HTTP response compression
/// Default: off (disabled)
/// Values: on, off, true, false, yes, no, 1, 0
/// Example: RUSTFS_COMPRESS_ENABLE=on
pub const ENV_COMPRESS_ENABLE: &str = "RUSTFS_COMPRESS_ENABLE";

/// Default compression enable state
/// Aligned with MinIO behavior - compression is disabled by default
pub const DEFAULT_COMPRESS_ENABLE: bool = false;

/// Environment variable for file extensions that should be compressed
/// Comma-separated list of file extensions (with or without leading dot)
/// Default: "" (empty, meaning use MIME type matching only)
/// Example: RUSTFS_COMPRESS_EXTENSIONS=.txt,.log,.csv,.json,.xml,.html,.css,.js
pub const ENV_COMPRESS_EXTENSIONS: &str = "RUSTFS_COMPRESS_EXTENSIONS";

/// Default file extensions for compression
/// Empty by default - relies on MIME type matching
pub const DEFAULT_COMPRESS_EXTENSIONS: &str = "";

/// Environment variable for MIME types that should be compressed
/// Comma-separated list of MIME types, supports wildcard (*) for subtypes
/// Default: "text/*,application/json,application/xml,application/javascript"
/// Example: RUSTFS_COMPRESS_MIME_TYPES=text/*,application/json,application/xml
pub const ENV_COMPRESS_MIME_TYPES: &str = "RUSTFS_COMPRESS_MIME_TYPES";

/// Default MIME types for compression
/// Includes common text-based content types that benefit from compression
pub const DEFAULT_COMPRESS_MIME_TYPES: &str = "text/*,application/json,application/xml,application/javascript";

/// Environment variable for minimum file size to apply compression
/// Files smaller than this size will not be compressed
/// Default: 1000 (bytes)
/// Example: RUSTFS_COMPRESS_MIN_SIZE=1000
pub const ENV_COMPRESS_MIN_SIZE: &str = "RUSTFS_COMPRESS_MIN_SIZE";

/// Default minimum file size for compression (in bytes)
/// Files smaller than 1000 bytes typically don't benefit from compression
/// and the compression overhead may outweigh the benefits
pub const DEFAULT_COMPRESS_MIN_SIZE: u64 = 1000;
