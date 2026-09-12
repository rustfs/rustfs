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

//! TFTP operational limits and S3 contract constants.

/// S3 minimum multipart part size (5 MiB).
pub const S3_MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// S3 maximum parts per multipart upload.
pub const S3_MAX_MULTIPART_PARTS: i32 = 10_000;

/// Lower bound for `RUSTFS_TFTP_MAX_BLOCK_SIZE`.
pub const MAX_BLOCK_SIZE_MIN: u16 = 512;

/// Upper bound for `RUSTFS_TFTP_MAX_BLOCK_SIZE` (RFC 2348).
pub const MAX_BLOCK_SIZE_MAX: u16 = 65464;

/// Lower bound for `RUSTFS_TFTP_MAX_WINDOW_SIZE` (RFC 7440).
pub const MAX_WINDOW_SIZE_MIN: u16 = 1;

/// Upper bound for `RUSTFS_TFTP_MAX_WINDOW_SIZE`.
pub const MAX_WINDOW_SIZE_MAX: u16 = 65535;

/// Lower bound for `RUSTFS_TFTP_MAX_CONCURRENT_TRANSFERS`.
pub const MAX_CONCURRENT_TRANSFERS_MIN: usize = 1;

/// Upper bound for `RUSTFS_TFTP_MAX_CONCURRENT_TRANSFERS`.
pub const MAX_CONCURRENT_TRANSFERS_MAX: usize = 4096;

/// Lower bound for `RUSTFS_TFTP_MAX_TRANSFER_BYTES`.
pub const MAX_TRANSFER_BYTES_MIN: u64 = 512;

/// Upper bound for `RUSTFS_TFTP_MAX_TRANSFER_BYTES` (1 GiB).
pub const MAX_TRANSFER_BYTES_MAX: u64 = 1024 * 1024 * 1024;

/// Lower bound for `RUSTFS_TFTP_BACKEND_OP_TIMEOUT_SECS`.
pub const BACKEND_OP_TIMEOUT_MIN_SECS: u64 = 5;

/// Upper bound for `RUSTFS_TFTP_BACKEND_OP_TIMEOUT_SECS`.
pub const BACKEND_OP_TIMEOUT_MAX_SECS: u64 = 600;

/// Lower bound for `RUSTFS_TFTP_READ_FETCH_BYTES`.
pub const READ_FETCH_BYTES_MIN: u64 = 512;

/// Upper bound for `RUSTFS_TFTP_READ_FETCH_BYTES`.
pub const READ_FETCH_BYTES_MAX: u64 = 64 * 1024 * 1024;

/// Lower bound for `RUSTFS_TFTP_MAX_SEND_RETRIES`.
pub const MAX_SEND_RETRIES_MIN: u32 = 1;

/// Upper bound for `RUSTFS_TFTP_MAX_SEND_RETRIES`.
pub const MAX_SEND_RETRIES_MAX: u32 = 100;
