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

use crate::MI_B;

// Tokio runtime ENV keys
pub const ENV_WORKER_THREADS: &str = "RUSTFS_RUNTIME_WORKER_THREADS";
pub const ENV_MAX_BLOCKING_THREADS: &str = "RUSTFS_RUNTIME_MAX_BLOCKING_THREADS";
pub const ENV_THREAD_PRINT_ENABLED: &str = "RUSTFS_RUNTIME_THREAD_PRINT_ENABLED";
pub const ENV_THREAD_STACK_SIZE: &str = "RUSTFS_RUNTIME_THREAD_STACK_SIZE";
pub const ENV_THREAD_KEEP_ALIVE: &str = "RUSTFS_RUNTIME_THREAD_KEEP_ALIVE";
pub const ENV_GLOBAL_QUEUE_INTERVAL: &str = "RUSTFS_RUNTIME_GLOBAL_QUEUE_INTERVAL";
pub const ENV_THREAD_NAME: &str = "RUSTFS_RUNTIME_THREAD_NAME";
pub const ENV_MAX_IO_EVENTS_PER_TICK: &str = "RUSTFS_RUNTIME_MAX_IO_EVENTS_PER_TICK";
pub const ENV_RNG_SEED: &str = "RUSTFS_RUNTIME_RNG_SEED";
/// Event polling interval
pub const ENV_EVENT_INTERVAL: &str = "RUSTFS_RUNTIME_EVENT_INTERVAL";

// Dial9 Tokio Telemetry Configuration
pub const ENV_RUNTIME_DIAL9_ENABLED: &str = "RUSTFS_RUNTIME_DIAL9_ENABLED";
pub const ENV_RUNTIME_DIAL9_OUTPUT_DIR: &str = "RUSTFS_RUNTIME_DIAL9_OUTPUT_DIR";
pub const ENV_RUNTIME_DIAL9_FILE_PREFIX: &str = "RUSTFS_RUNTIME_DIAL9_FILE_PREFIX";
pub const ENV_RUNTIME_DIAL9_MAX_FILE_SIZE: &str = "RUSTFS_RUNTIME_DIAL9_MAX_FILE_SIZE";
pub const ENV_RUNTIME_DIAL9_ROTATION_COUNT: &str = "RUSTFS_RUNTIME_DIAL9_ROTATION_COUNT";
pub const ENV_RUNTIME_DIAL9_S3_BUCKET: &str = "RUSTFS_RUNTIME_DIAL9_S3_BUCKET";
pub const ENV_RUNTIME_DIAL9_S3_PREFIX: &str = "RUSTFS_RUNTIME_DIAL9_S3_PREFIX";
pub const ENV_RUNTIME_DIAL9_SAMPLING_RATE: &str = "RUSTFS_RUNTIME_DIAL9_SAMPLING_RATE";

// Default values for Tokio runtime
pub const DEFAULT_WORKER_THREADS: usize = 16;
pub const DEFAULT_MAX_BLOCKING_THREADS: usize = 1024;
pub const DEFAULT_THREAD_PRINT_ENABLED: bool = false;
pub const DEFAULT_THREAD_STACK_SIZE: usize = MI_B; // 1 MiB
pub const DEFAULT_THREAD_KEEP_ALIVE: u64 = 60; // seconds
pub const DEFAULT_GLOBAL_QUEUE_INTERVAL: u32 = 31;
pub const DEFAULT_THREAD_NAME: &str = "rustfs-worker";
pub const DEFAULT_MAX_IO_EVENTS_PER_TICK: usize = 1024;
/// Event polling default (Tokio default 61)
pub const DEFAULT_EVENT_INTERVAL: u32 = 61;
pub const DEFAULT_RNG_SEED: Option<u64> = None; // None means random

// Dial9 Tokio Telemetry Default values
pub const DEFAULT_RUNTIME_DIAL9_ENABLED: bool = false; // Disabled by default
pub const DEFAULT_RUNTIME_DIAL9_OUTPUT_DIR: &str = "/var/log/rustfs/telemetry";
pub const DEFAULT_RUNTIME_DIAL9_FILE_PREFIX: &str = "rustfs-tokio";
pub const DEFAULT_RUNTIME_DIAL9_MAX_FILE_SIZE: u64 = 100 * 1024 * 1024; // 100MB
pub const DEFAULT_RUNTIME_DIAL9_ROTATION_COUNT: usize = 10;
pub const DEFAULT_RUNTIME_DIAL9_SAMPLING_RATE: f64 = 1.0; // 100% sampling
// Note: S3 bucket/prefix have no default; absence means upload is disabled (modeled as Option<String>)

// Allocator reclaim configuration
pub const ENV_ALLOCATOR_RECLAIM_ENABLED: &str = "RUSTFS_ALLOCATOR_RECLAIM_ENABLED";
pub const ENV_ALLOCATOR_RECLAIM_INTERVAL_SECS: &str = "RUSTFS_ALLOCATOR_RECLAIM_INTERVAL_SECS";
pub const ENV_ALLOCATOR_RECLAIM_FORCE: &str = "RUSTFS_ALLOCATOR_RECLAIM_FORCE";
pub const ENV_ALLOCATOR_RECLAIM_IDLE_INTERVALS: &str = "RUSTFS_ALLOCATOR_RECLAIM_IDLE_INTERVALS";
pub const DEFAULT_ALLOCATOR_RECLAIM_ENABLED: bool = false;
pub const DEFAULT_ALLOCATOR_RECLAIM_INTERVAL_SECS: u64 = 30;
pub const DEFAULT_ALLOCATOR_RECLAIM_FORCE: bool = true;
pub const DEFAULT_ALLOCATOR_RECLAIM_IDLE_INTERVALS: u64 = 3;

// File page-cache reclaim configuration
pub const ENV_OBJECT_FILE_CACHE_RECLAIM_WRITE_ENABLE: &str = "RUSTFS_OBJECT_FILE_CACHE_RECLAIM_WRITE_ENABLE";
pub const ENV_OBJECT_FILE_CACHE_RECLAIM_READ_ENABLE: &str = "RUSTFS_OBJECT_FILE_CACHE_RECLAIM_READ_ENABLE";
pub const ENV_OBJECT_FILE_CACHE_RECLAIM_THRESHOLD: &str = "RUSTFS_OBJECT_FILE_CACHE_RECLAIM_THRESHOLD";
pub const DEFAULT_OBJECT_FILE_CACHE_RECLAIM_WRITE_ENABLE: bool = false;
pub const DEFAULT_OBJECT_FILE_CACHE_RECLAIM_READ_ENABLE: bool = false;
pub const DEFAULT_OBJECT_FILE_CACHE_RECLAIM_THRESHOLD: usize = 4 * 1024 * 1024;

/// Threshold for small object seek support in megabytes.
///
/// When an object is smaller than this size, rustfs will provide seek support.
///
/// Default is set to 10MB.
pub const ENV_OBJECT_SEEK_SUPPORT_THRESHOLD: &str = "RUSTFS_OBJECT_SEEK_SUPPORT_THRESHOLD";
pub const DEFAULT_OBJECT_SEEK_SUPPORT_THRESHOLD: usize = 10 * 1024 * 1024;
