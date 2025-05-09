use serde::Deserialize;

/// File sink configuration
#[derive(Debug, Deserialize, Clone)]
pub struct FileSinkConfig {
    pub path: String,
    pub max_size: u64,
    pub max_backups: u64,
}

impl FileSinkConfig {
    pub fn new() -> Self {
        Self {
            path: "".to_string(),
            max_size: 0,
            max_backups: 0,
        }
    }
}

impl Default for FileSinkConfig {
    fn default() -> Self {
        Self::new()
    }
}
