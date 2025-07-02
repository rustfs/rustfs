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

/// Kafka sink configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSink {
    pub brokers: String,
    pub topic: String,
    #[serde(default = "default_batch_size")]
    pub batch_size: Option<usize>,
    #[serde(default = "default_batch_timeout_ms")]
    pub batch_timeout_ms: Option<u64>,
}

impl KafkaSink {
    pub fn new() -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            topic: "rustfs".to_string(),
            batch_size: default_batch_size(),
            batch_timeout_ms: default_batch_timeout_ms(),
        }
    }
}

impl Default for KafkaSink {
    fn default() -> Self {
        Self::new()
    }
}

fn default_batch_size() -> Option<usize> {
    Some(100)
}
fn default_batch_timeout_ms() -> Option<u64> {
    Some(1000)
}
