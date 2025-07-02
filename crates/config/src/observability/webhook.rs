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
use std::collections::HashMap;

/// Webhook sink configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct WebhookSink {
    pub endpoint: String,
    pub auth_token: String,
    pub headers: Option<HashMap<String, String>>,
    #[serde(default = "default_max_retries")]
    pub max_retries: Option<usize>,
    #[serde(default = "default_retry_delay_ms")]
    pub retry_delay_ms: Option<u64>,
}

impl WebhookSink {
    pub fn new() -> Self {
        Self {
            endpoint: "".to_string(),
            auth_token: "".to_string(),
            headers: Some(HashMap::new()),
            max_retries: default_max_retries(),
            retry_delay_ms: default_retry_delay_ms(),
        }
    }
}

impl Default for WebhookSink {
    fn default() -> Self {
        Self::new()
    }
}

fn default_max_retries() -> Option<usize> {
    Some(3)
}
fn default_retry_delay_ms() -> Option<u64> {
    Some(100)
}
