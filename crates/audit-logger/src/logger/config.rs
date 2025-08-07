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

use serde::Deserialize;
use std::collections::HashMap;
use url::Url;

#[derive(Deserialize, Debug, Default)]
pub struct Config {
    #[serde(default)]
    pub console: ConsoleConfig,
    #[serde(default)]
    pub logger_webhook: HashMap<String, LoggerWebhookConfig>,
    #[serde(default)]
    pub audit_webhook: HashMap<String, AuditWebhookConfig>,
    #[serde(default)]
    pub audit_kafka: HashMap<String, AuditKafkaConfig>,
}

#[derive(Deserialize, Debug)]
#[serde(default)]
pub struct ConsoleConfig {
    pub enabled: bool,
}

impl Default for ConsoleConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct HttpConfig {
    pub enabled: bool,
    pub endpoint: Url,
    #[serde(default)]
    pub auth_token: String,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_queue_size")]
    pub queue_size: usize,
    #[serde(default = "default_max_retry")]
    pub max_retry: u32,
    #[serde(with = "humantime_serde")]
    #[serde(default = "default_retry_interval")]
    pub retry_interval: std::time::Duration,
}

// 为 HttpConfig 提供别名以区分 logger 和 audit
pub type LoggerWebhookConfig = HttpConfig;
pub type AuditWebhookConfig = HttpConfig;

#[derive(Deserialize, Debug, Clone)]
pub struct AuditKafkaConfig {
    pub enabled: bool,
    pub brokers: Vec<String>,
    pub topic: String,
    // ... 其他 Kafka 特定字段
}

// 默认值函数
fn default_batch_size() -> usize {
    10
}
fn default_queue_size() -> usize {
    10000
}
fn default_max_retry() -> u32 {
    5
}
fn default_retry_interval() -> std::time::Duration {
    std::time::Duration::from_secs(3)
}
