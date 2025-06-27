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
