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

use crate::{AppConfig, SinkConfig, UnifiedLogEntry};
use async_trait::async_trait;
use std::sync::Arc;

#[cfg(feature = "file")]
mod file;
#[cfg(all(feature = "kafka", target_os = "linux"))]
mod kafka;
#[cfg(feature = "webhook")]
mod webhook;

/// Sink Trait definition, asynchronously write logs
#[async_trait]
pub trait Sink: Send + Sync {
    async fn write(&self, entry: &UnifiedLogEntry);
}

/// Create a list of Sink instances
pub async fn create_sinks(config: &AppConfig) -> Vec<Arc<dyn Sink>> {
    let mut sinks: Vec<Arc<dyn Sink>> = Vec::new();

    for sink_config in &config.sinks {
        match sink_config {
            #[cfg(all(feature = "kafka", target_os = "linux"))]
            SinkConfig::Kafka(kafka_config) => {
                match rdkafka::config::ClientConfig::new()
                    .set("bootstrap.servers", &kafka_config.brokers)
                    .set("message.timeout.ms", "5000")
                    .create()
                {
                    Ok(producer) => {
                        sinks.push(Arc::new(kafka::KafkaSink::new(
                            producer,
                            kafka_config.topic.clone(),
                            kafka_config
                                .batch_size
                                .unwrap_or(rustfs_config::observability::DEFAULT_SINKS_KAFKA_BATCH_SIZE),
                            kafka_config
                                .batch_timeout_ms
                                .unwrap_or(rustfs_config::observability::DEFAULT_SINKS_KAFKA_BATCH_TIMEOUT_MS),
                        )));
                        tracing::info!("Kafka sink created for topic: {}", kafka_config.topic);
                    }
                    Err(e) => {
                        tracing::error!("Failed to create Kafka producer: {}", e);
                    }
                }
            }

            #[cfg(feature = "webhook")]
            SinkConfig::Webhook(webhook_config) => {
                sinks.push(Arc::new(webhook::WebhookSink::new(
                    webhook_config.endpoint.clone(),
                    webhook_config.auth_token.clone(),
                    webhook_config
                        .max_retries
                        .unwrap_or(rustfs_config::observability::DEFAULT_SINKS_WEBHOOK_MAX_RETRIES),
                    webhook_config
                        .retry_delay_ms
                        .unwrap_or(rustfs_config::observability::DEFAULT_SINKS_WEBHOOK_RETRY_DELAY_MS),
                )));
                tracing::info!("Webhook sink created for endpoint: {}", webhook_config.endpoint);
            }
            #[cfg(feature = "file")]
            SinkConfig::File(file_config) => {
                tracing::debug!("FileSink: Using path: {}", file_config.path);
                match file::FileSink::new(
                    format!("{}/{}", file_config.path.clone(), rustfs_config::DEFAULT_SINK_FILE_LOG_FILE),
                    file_config
                        .buffer_size
                        .unwrap_or(rustfs_config::observability::DEFAULT_SINKS_FILE_BUFFER_SIZE),
                    file_config
                        .flush_interval_ms
                        .unwrap_or(rustfs_config::observability::DEFAULT_SINKS_FILE_FLUSH_INTERVAL_MS),
                    file_config
                        .flush_threshold
                        .unwrap_or(rustfs_config::observability::DEFAULT_SINKS_FILE_FLUSH_THRESHOLD),
                )
                .await
                {
                    Ok(sink) => {
                        sinks.push(Arc::new(sink));
                        tracing::info!("File sink created for path: {}", file_config.path);
                    }
                    Err(e) => {
                        tracing::error!("Failed to create File sink: {}", e);
                    }
                }
            }
            #[cfg(any(not(feature = "kafka"), not(target_os = "linux")))]
            SinkConfig::Kafka(_) => {
                tracing::warn!("Kafka sink is configured but the 'kafka' feature is not enabled");
            }
            #[cfg(not(feature = "webhook"))]
            SinkConfig::Webhook(_) => {
                tracing::warn!("Webhook sink is configured but the 'webhook' feature is not enabled");
            }
            #[cfg(not(feature = "file"))]
            SinkConfig::File(_) => {
                tracing::warn!("File sink is configured but the 'file' feature is not enabled");
            }
        }
    }

    sinks
}
