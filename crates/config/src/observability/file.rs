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

// RUSTFS_SINKS_FILE_PATH
pub const ENV_SINKS_FILE_PATH: &str = "RUSTFS_SINKS_FILE_PATH";
// RUSTFS_SINKS_FILE_BUFFER_SIZE
pub const ENV_SINKS_FILE_BUFFER_SIZE: &str = "RUSTFS_SINKS_FILE_BUFFER_SIZE";
// RUSTFS_SINKS_FILE_FLUSH_INTERVAL_MS
pub const ENV_SINKS_FILE_FLUSH_INTERVAL_MS: &str = "RUSTFS_SINKS_FILE_FLUSH_INTERVAL_MS";
// RUSTFS_SINKS_FILE_FLUSH_THRESHOLD
pub const ENV_SINKS_FILE_FLUSH_THRESHOLD: &str = "RUSTFS_SINKS_FILE_FLUSH_THRESHOLD";

pub const DEFAULT_SINKS_FILE_BUFFER_SIZE: usize = 8192;

pub const DEFAULT_SINKS_FILE_FLUSH_INTERVAL_MS: u64 = 1000;

pub const DEFAULT_SINKS_FILE_FLUSH_THRESHOLD: usize = 100;
