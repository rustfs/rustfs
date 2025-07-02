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

use crate::observability::file::FileSink;
use crate::observability::kafka::KafkaSink;
use crate::observability::webhook::WebhookSink;
use serde::{Deserialize, Serialize};

/// Sink configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SinkConfig {
    Kafka(KafkaSink),
    Webhook(WebhookSink),
    File(FileSink),
}

impl SinkConfig {
    pub fn new() -> Self {
        Self::File(FileSink::new())
    }
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self::new()
    }
}
