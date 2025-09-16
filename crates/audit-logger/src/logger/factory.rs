//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use super::config::AuditTargetConfig;
use crate::entry::audit::AuditLogEntry;
use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;

/// Error types for target creation and operations
#[derive(Debug, Error)]
pub enum TargetError {
    #[error("Unsupported target type: {0}")]
    UnsupportedType(String),
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("Target initialization failed: {0}")]
    InitializationFailed(String),
    #[error("Target operation failed: {0}")]
    OperationFailed(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Network error: {0}")]
    Network(String),
}

/// Result type for target operations
pub type TargetResult<T> = Result<T, TargetError>;

/// Trait for audit targets that can receive log entries
#[async_trait]
pub trait AuditTarget: Send + Sync {
    /// Send a single audit log entry
    async fn send(&self, entry: Arc<AuditLogEntry>) -> TargetResult<()>;

    /// Send multiple audit log entries in batch
    async fn send_batch(&self, entries: Vec<Arc<AuditLogEntry>>) -> TargetResult<()> {
        // Default implementation sends each entry individually
        for entry in entries {
            self.send(entry).await?;
        }
        Ok(())
    }

    /// Returns the unique identifier of the target
    fn id(&self) -> &str;

    /// Returns the type of the target
    fn kind(&self) -> &str;

    /// Start the target (perform any initialization)
    async fn start(&self) -> TargetResult<()> {
        Ok(())
    }

    /// Pause the target (temporarily stop accepting new entries)
    async fn pause(&self) -> TargetResult<()> {
        Ok(())
    }

    /// Resume the target (continue accepting entries after pause)
    async fn resume(&self) -> TargetResult<()> {
        Ok(())
    }

    /// Close target gracefully, ensuring all buffered logs are processed
    async fn shutdown(&self) -> TargetResult<()> {
        Ok(())
    }

    /// Check if the target is healthy
    async fn health_check(&self) -> TargetResult<bool> {
        Ok(true)
    }
}

/// Factory trait for creating audit targets
#[async_trait]
pub trait AuditTargetFactory: Send + Sync {
    /// Create a new audit target from configuration
    async fn create_target(&self, config: &AuditTargetConfig) -> TargetResult<Box<dyn AuditTarget>>;

    /// Get supported target types
    fn supported_types(&self) -> Vec<&str>;

    /// Validate target configuration without creating the target
    fn validate_config(&self, config: &AuditTargetConfig) -> TargetResult<()>;
}

/// Default factory implementation that supports built-in target types
pub struct DefaultAuditTargetFactory;

impl DefaultAuditTargetFactory {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AuditTargetFactory for DefaultAuditTargetFactory {
    async fn create_target(&self, config: &AuditTargetConfig) -> TargetResult<Box<dyn AuditTarget>> {
        match config.kind.as_str() {
            "webhook" => {
                let webhook_target = WebhookTarget::from_config(config).await?;
                Ok(Box::new(webhook_target))
            }
            "mqtt" => {
                let mqtt_target = MqttTarget::from_config(config).await?;
                Ok(Box::new(mqtt_target))
            }
            _ => Err(TargetError::UnsupportedType(config.kind.clone())),
        }
    }

    fn supported_types(&self) -> Vec<&str> {
        vec!["webhook", "mqtt"]
    }

    fn validate_config(&self, config: &AuditTargetConfig) -> TargetResult<()> {
        if config.id.is_empty() {
            return Err(TargetError::InvalidConfig("Target ID cannot be empty".to_string()));
        }

        if config.kind.is_empty() {
            return Err(TargetError::InvalidConfig("Target type cannot be empty".to_string()));
        }

        if !self.supported_types().contains(&config.kind.as_str()) {
            return Err(TargetError::UnsupportedType(config.kind.clone()));
        }

        // Validate type-specific configuration
        match config.kind.as_str() {
            "webhook" => WebhookTarget::validate_config(config),
            "mqtt" => MqttTarget::validate_config(config),
            _ => Ok(()),
        }
    }
}

/// Webhook target implementation
pub struct WebhookTarget {
    id: String,
    endpoint: String,
    auth_token: Option<String>,
    client: reqwest::Client,
}

impl WebhookTarget {
    async fn from_config(config: &AuditTargetConfig) -> TargetResult<Self> {
        Self::validate_config(config)?;

        let endpoint = config
            .args
            .get("endpoint")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TargetError::InvalidConfig("Missing endpoint".to_string()))?
            .to_string();

        let auth_token = config.args.get("auth_token").and_then(|v| v.as_str()).map(|s| s.to_string());

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| TargetError::InitializationFailed(e.to_string()))?;

        Ok(Self {
            id: config.id.clone(),
            endpoint,
            auth_token,
            client,
        })
    }

    fn validate_config(config: &AuditTargetConfig) -> TargetResult<()> {
        let endpoint = config
            .args
            .get("endpoint")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TargetError::InvalidConfig("Missing endpoint".to_string()))?;

        url::Url::parse(endpoint).map_err(|e| TargetError::InvalidConfig(format!("Invalid endpoint URL: {}", e)))?;

        Ok(())
    }
}

#[async_trait]
impl AuditTarget for WebhookTarget {
    async fn send(&self, entry: Arc<AuditLogEntry>) -> TargetResult<()> {
        let json = serde_json::to_string(&*entry).map_err(|e| TargetError::Serialization(e.to_string()))?;

        let mut request = self
            .client
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .body(json);

        if let Some(token) = &self.auth_token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        request
            .send()
            .await
            .map_err(|e| TargetError::Network(e.to_string()))?
            .error_for_status()
            .map_err(|e| TargetError::OperationFailed(e.to_string()))?;

        Ok(())
    }

    async fn send_batch(&self, entries: Vec<Arc<AuditLogEntry>>) -> TargetResult<()> {
        let batch_data: Vec<_> = entries.iter().map(|entry| &**entry).collect();
        let json = serde_json::to_string(&batch_data).map_err(|e| TargetError::Serialization(e.to_string()))?;

        let mut request = self
            .client
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .body(json);

        if let Some(token) = &self.auth_token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        request
            .send()
            .await
            .map_err(|e| TargetError::Network(e.to_string()))?
            .error_for_status()
            .map_err(|e| TargetError::OperationFailed(e.to_string()))?;

        Ok(())
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> &str {
        "webhook"
    }
}

/// MQTT target implementation
pub struct MqttTarget {
    id: String,
    topic: String,
    client: rumqttc::AsyncClient,
}

impl MqttTarget {
    async fn from_config(config: &AuditTargetConfig) -> TargetResult<Self> {
        Self::validate_config(config)?;

        let broker = config
            .args
            .get("broker")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TargetError::InvalidConfig("Missing broker".to_string()))?;

        let topic = config
            .args
            .get("topic")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TargetError::InvalidConfig("Missing topic".to_string()))?
            .to_string();

        let port = config.args.get("port").and_then(|v| v.as_u64()).unwrap_or(1883) as u16;

        let mut mqttoptions = rumqttc::MqttOptions::new(&config.id, broker, port);
        mqttoptions.set_keep_alive(std::time::Duration::from_secs(5));

        if let Some(username) = config.args.get("username").and_then(|v| v.as_str()) {
            let password = config.args.get("password").and_then(|v| v.as_str()).unwrap_or("");
            mqttoptions.set_credentials(username, password);
        }

        let (client, mut eventloop) = rumqttc::AsyncClient::new(mqttoptions, 10);

        // Start the event loop in a background task
        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!("MQTT connection error: {}", e);
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                }
            }
        });

        Ok(Self {
            id: config.id.clone(),
            topic,
            client,
        })
    }

    fn validate_config(config: &AuditTargetConfig) -> TargetResult<()> {
        if config.args.get("broker").and_then(|v| v.as_str()).is_none() {
            return Err(TargetError::InvalidConfig("Missing broker".to_string()));
        }

        if config.args.get("topic").and_then(|v| v.as_str()).is_none() {
            return Err(TargetError::InvalidConfig("Missing topic".to_string()));
        }

        Ok(())
    }
}

#[async_trait]
impl AuditTarget for MqttTarget {
    async fn send(&self, entry: Arc<AuditLogEntry>) -> TargetResult<()> {
        let json = serde_json::to_string(&*entry).map_err(|e| TargetError::Serialization(e.to_string()))?;

        self.client
            .publish(&self.topic, rumqttc::QoS::AtLeastOnce, false, json)
            .await
            .map_err(|e| TargetError::OperationFailed(e.to_string()))?;

        Ok(())
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> &str {
        "mqtt"
    }
}
