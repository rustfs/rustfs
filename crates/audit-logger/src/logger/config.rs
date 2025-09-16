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
use std::time::Duration;

/// Audit target configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditTargetConfig {
    /// Unique identifier for the target
    pub id: String,
    /// Type of target (mqtt, webhook, kafka, file, etc.)
    #[serde(rename = "type")]
    pub kind: String,
    /// Target-specific configuration arguments
    pub args: serde_json::Value,
    /// Whether the target is enabled
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Batch size for processing
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Queue size for buffering
    #[serde(default = "default_queue_size")]
    pub queue_size: usize,
    /// Maximum retry attempts
    #[serde(default = "default_max_retry")]
    pub max_retry: u32,
    /// Retry interval in seconds
    #[serde(default = "default_retry_interval_secs", with = "duration_serde")]
    pub retry_interval: Duration,
}

/// Audit config summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditConfig {
    /// List of audit targets
    #[serde(default)]
    pub targets: Vec<AuditTargetConfig>,
    /// Global audit system enabled flag
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Global batch size override
    #[serde(default = "default_batch_size")]
    pub global_batch_size: usize,
    /// Default queue size for new targets
    #[serde(default = "default_queue_size")]
    pub default_queue_size: usize,
    /// Global maximum concurrent targets
    #[serde(default = "default_max_concurrent_targets")]
    pub max_concurrent_targets: usize,
}

/// Target status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetStatus {
    /// Target ID
    pub id: String,
    /// Target type
    pub kind: String,
    /// Whether the target is enabled
    pub enabled: bool,
    /// Whether the target is currently running
    pub running: bool,
    /// Last error message if any
    pub last_error: Option<String>,
    /// Number of successful dispatches
    pub success_count: u64,
    /// Number of failed dispatches
    pub error_count: u64,
    /// Last successful dispatch timestamp
    pub last_success: Option<chrono::DateTime<chrono::Utc>>,
    /// Last error timestamp
    pub last_error_time: Option<chrono::DateTime<chrono::Utc>>,
}

// Default value functions
fn default_enabled() -> bool {
    true
}

fn default_batch_size() -> usize {
    10
}

fn default_queue_size() -> usize {
    10000
}

fn default_max_retry() -> u32 {
    5
}

fn default_retry_interval_secs() -> Duration {
    Duration::from_secs(3)
}

fn default_max_concurrent_targets() -> usize {
    16
}

// Custom serde for Duration to serialize as seconds
mod duration_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(duration.as_secs())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(secs))
    }
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            targets: Vec::new(),
            enabled: default_enabled(),
            global_batch_size: default_batch_size(),
            default_queue_size: default_queue_size(),
            max_concurrent_targets: default_max_concurrent_targets(),
        }
    }
}

impl Default for AuditTargetConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            kind: String::new(),
            args: serde_json::Value::Object(serde_json::Map::new()),
            enabled: default_enabled(),
            batch_size: default_batch_size(),
            queue_size: default_queue_size(),
            max_retry: default_max_retry(),
            retry_interval: default_retry_interval_secs(),
        }
    }
}

impl AuditTargetConfig {
    /// Create a new audit target configuration
    pub fn new(id: String, kind: String) -> Self {
        Self {
            id,
            kind,
            ..Default::default()
        }
    }

    /// Set the target arguments
    pub fn with_args(mut self, args: serde_json::Value) -> Self {
        self.args = args;
        self
    }

    /// Enable or disable the target
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set the batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the queue size
    pub fn with_queue_size(mut self, queue_size: usize) -> Self {
        self.queue_size = queue_size;
        self
    }
}
