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

use serde::{Deserialize, Serialize};
use std::env;

/// File sink configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSink {
    pub path: String,
    #[serde(default = "default_buffer_size")]
    pub buffer_size: Option<usize>,
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: Option<u64>,
    #[serde(default = "default_flush_threshold")]
    pub flush_threshold: Option<usize>,
}

impl FileSink {
    pub fn new() -> Self {
        Self {
            path: env::var("RUSTFS_SINKS_FILE_PATH")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(default_path),
            buffer_size: default_buffer_size(),
            flush_interval_ms: default_flush_interval_ms(),
            flush_threshold: default_flush_threshold(),
        }
    }
}

impl Default for FileSink {
    fn default() -> Self {
        Self::new()
    }
}

fn default_buffer_size() -> Option<usize> {
    Some(8192)
}
fn default_flush_interval_ms() -> Option<u64> {
    Some(1000)
}
fn default_flush_threshold() -> Option<usize> {
    Some(100)
}

fn default_path() -> String {
    let temp_dir = env::temp_dir().join("rustfs");

    if let Err(e) = std::fs::create_dir_all(&temp_dir) {
        eprintln!("Failed to create log directory: {e}");
        return "rustfs/rustfs.log".to_string();
    }

    temp_dir
        .join("rustfs.log")
        .to_str()
        .unwrap_or("rustfs/rustfs.log")
        .to_string()
}
