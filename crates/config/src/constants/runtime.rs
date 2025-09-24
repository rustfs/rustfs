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
pub const ENV_RNG_SEED: &str = "RUSTFS_RUNTIME_RNG_SEED";

// Default values for Tokio runtime
pub const DEFAULT_WORKER_THREADS: usize = 16;
pub const DEFAULT_MAX_BLOCKING_THREADS: usize = 1024;
pub const DEFAULT_THREAD_PRINT_ENABLED: bool = false;
pub const DEFAULT_THREAD_STACK_SIZE: usize = MI_B; // 1 MiB
pub const DEFAULT_THREAD_KEEP_ALIVE: u64 = 60; // seconds
pub const DEFAULT_GLOBAL_QUEUE_INTERVAL: u32 = 31;
pub const DEFAULT_THREAD_NAME: &str = "rustfs-worker";
pub const DEFAULT_RNG_SEED: Option<u64> = None; // None means random
