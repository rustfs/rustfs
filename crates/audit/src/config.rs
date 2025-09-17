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

//! Configuration module for audit system integration with rustfs-config and rustfs-ecstore.
//!
//! This module provides configuration loading with precedence: ENV > file instance > file default.
//! It integrates with existing RustFS configuration constants and persistence infrastructure.

use crate::error::AuditError;
use rustfs_config::audit::{webhook::*, mqtt::*};
use rustfs_config::*;
use rustfs_config::{EnableState, ENABLE_KEY};
use rustfs_ecstore::config::audit::{DEFAULT_AUDIT_MQTT_KVS, DEFAULT_AUDIT_WEBHOOK_KVS};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use tracing::{debug, info, warn};

/// Main audit configuration structure integrating with RustFS config infrastructure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditConfig {
    /// Global audit system enabled state
    pub enabled: bool,
    /// List of configured audit targets
    pub targets: Vec<AuditTargetConfig>,
    /// Header redaction configuration for security
    pub redaction: RedactionConfig,
    /// Performance tuning settings
    pub performance: PerformanceConfig,
}

/// Configuration for individual audit targets
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditTargetConfig {
    /// Unique target identifier
    pub id: String,
    /// Target type ("webhook", "mqtt", etc.)
    pub kind: String,
    /// Target enabled state
    pub enabled: bool,
    /// Target-specific configuration arguments
    pub args: serde_json::Value,
}

/// Header redaction configuration for security compliance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactionConfig {
    /// List of header names to redact in audit logs
    pub headers_blacklist: Vec<String>,
}

/// Performance tuning configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Batch size for audit log processing
    pub batch_size: usize,
    /// Batch timeout in milliseconds
    pub batch_timeout_ms: u64,
    /// Maximum concurrent target dispatches
    pub max_concurrent: usize,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            targets: Vec::new(),
            redaction: RedactionConfig::default(),
            performance: PerformanceConfig::default(),
        }
    }
}

impl Default for RedactionConfig {
    fn default() -> Self {
        Self {
            headers_blacklist: vec![
                "Authorization".to_string(),
                "Cookie".to_string(),
                "X-Api-Key".to_string(),
                "X-Auth-Token".to_string(),
            ],
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            batch_size: 50,
            batch_timeout_ms: 100,
            max_concurrent: 10,
        }
    }
}

/// Configuration loader with ENV precedence and ecstore integration
pub struct ConfigLoader {
    env_prefix: String,
}

impl ConfigLoader {
    /// Create a new configuration loader with the specified environment prefix
    pub fn new(env_prefix: Option<String>) -> Self {
        Self {
            env_prefix: env_prefix.unwrap_or_else(|| "RUSTFS_".to_string()),
        }
    }

    /// Load configuration with precedence: ENV > file instance > file default
    /// 
    /// This method:
    /// 1. Loads default configuration from DEFAULT_AUDIT_*_KVS templates
    /// 2. Overlays file-based instance configuration
    /// 3. Applies environment variable overrides
    /// 4. Persists merged successful configuration to .rustfs.sys
    pub async fn load_config(&self) -> Result<AuditConfig, AuditError> {
        debug!("Loading audit configuration with ENV precedence");

        // Start with default configuration
        let mut config = AuditConfig::default();

        // Load default KVS templates from ecstore
        self.apply_default_kvs(&mut config)?;

        // Apply environment variable overrides (highest precedence)
        self.apply_env_overrides(&mut config)?;

        // TODO: Load from .rustfs.sys via rustfs-ecstore (when available)
        // This would be inserted between default KVS and ENV overrides

        // TODO: Persist merged configuration back to .rustfs.sys
        // self.persist_config(&config).await?;

        info!("Loaded audit configuration with {} targets", config.targets.len());
        Ok(config)
    }

    /// Apply default configuration from KVS templates
    fn apply_default_kvs(&self, config: &mut AuditConfig) -> Result<(), AuditError> {
        debug!("Applying default KVS templates");

        // Apply webhook defaults
        if self.is_enabled_from_kvs(&DEFAULT_AUDIT_WEBHOOK_KVS) {
            let webhook_config = self.create_webhook_config_from_kvs(&DEFAULT_AUDIT_WEBHOOK_KVS)?;
            config.targets.push(webhook_config);
        }

        // Apply MQTT defaults
        if self.is_enabled_from_kvs(&DEFAULT_AUDIT_MQTT_KVS) {
            let mqtt_config = self.create_mqtt_config_from_kvs(&DEFAULT_AUDIT_MQTT_KVS)?;
            config.targets.push(mqtt_config);
        }

        Ok(())
    }

    /// Apply environment variable overrides with priority for audit_webhook* and audit_mqtt*
    fn apply_env_overrides(&self, config: &mut AuditConfig) -> Result<(), AuditError> {
        debug!("Applying environment variable overrides");

        // Check for webhook environment variables
        if let Some(webhook_config) = self.create_webhook_config_from_env()? {
            // Remove any existing webhook target and add the env-configured one
            config.targets.retain(|t| t.kind != "webhook");
            config.targets.push(webhook_config);
        }

        // Check for MQTT environment variables
        if let Some(mqtt_config) = self.create_mqtt_config_from_env()? {
            // Remove any existing MQTT target and add the env-configured one
            config.targets.retain(|t| t.kind != "mqtt");
            config.targets.push(mqtt_config);
        }

        // Apply global audit settings from environment
        if let Ok(enabled) = env::var(format!("{}AUDIT_ENABLED", self.env_prefix)) {
            config.enabled = enabled.to_lowercase() == "on" || enabled.to_lowercase() == "true";
        }

        Ok(())
    }

    /// Create webhook target configuration from environment variables using rustfs-config constants
    fn create_webhook_config_from_env(&self) -> Result<Option<AuditTargetConfig>, AuditError> {
        // Check if webhook is enabled via environment using rustfs-config constants
        let enabled = env::var(ENV_AUDIT_WEBHOOK_ENABLE)
            .map(|v| v.to_lowercase() == "on" || v.to_lowercase() == "true")
            .unwrap_or(false);

        if !enabled {
            return Ok(None);
        }

        let endpoint = env::var(ENV_AUDIT_WEBHOOK_ENDPOINT)
            .map_err(|_| AuditError::ConfigurationError("Webhook endpoint required when enabled".to_string()))?;

        let mut args = serde_json::Map::new();
        args.insert("target_type".to_string(), serde_json::Value::String("AuditLog".to_string()));
        args.insert("url".to_string(), serde_json::Value::String(endpoint));

        // Optional webhook settings using rustfs-config constants
        if let Ok(auth_token) = env::var(ENV_AUDIT_WEBHOOK_AUTH_TOKEN) {
            args.insert("auth_token".to_string(), serde_json::Value::String(auth_token));
        }

        if let Ok(client_cert) = env::var(ENV_AUDIT_WEBHOOK_CLIENT_CERT) {
            args.insert("client_cert".to_string(), serde_json::Value::String(client_cert));
        }

        if let Ok(client_key) = env::var(ENV_AUDIT_WEBHOOK_CLIENT_KEY) {
            args.insert("client_key".to_string(), serde_json::Value::String(client_key));
        }

        if let Ok(queue_limit) = env::var(ENV_AUDIT_WEBHOOK_QUEUE_LIMIT) {
            if let Ok(limit) = queue_limit.parse::<u64>() {
                args.insert("queue_limit".to_string(), serde_json::Value::Number(serde_json::Number::from(limit)));
            }
        }

        if let Ok(queue_dir) = env::var(ENV_AUDIT_WEBHOOK_QUEUE_DIR) {
            args.insert("queue_dir".to_string(), serde_json::Value::String(queue_dir));
        }

        Ok(Some(AuditTargetConfig {
            id: "env-webhook".to_string(),
            kind: "webhook".to_string(),
            enabled: true,
            args: serde_json::Value::Object(args),
        }))
    }

    /// Create MQTT target configuration from environment variables using rustfs-config constants
    fn create_mqtt_config_from_env(&self) -> Result<Option<AuditTargetConfig>, AuditError> {
        // Check if MQTT is enabled via environment using rustfs-config constants
        let enabled = env::var(ENV_AUDIT_MQTT_ENABLE)
            .map(|v| v.to_lowercase() == "on" || v.to_lowercase() == "true")
            .unwrap_or(false);

        if !enabled {
            return Ok(None);
        }

        let broker = env::var(ENV_AUDIT_MQTT_BROKER)
            .map_err(|_| AuditError::ConfigurationError("MQTT broker required when enabled".to_string()))?;

        let topic = env::var(ENV_AUDIT_MQTT_TOPIC)
            .map_err(|_| AuditError::ConfigurationError("MQTT topic required when enabled".to_string()))?;

        let mut args = serde_json::Map::new();
        args.insert("target_type".to_string(), serde_json::Value::String("AuditLog".to_string()));
        args.insert("broker_url".to_string(), serde_json::Value::String(broker));
        args.insert("topic".to_string(), serde_json::Value::String(topic));

        // Optional MQTT settings using rustfs-config constants
        if let Ok(username) = env::var(ENV_AUDIT_MQTT_USERNAME) {
            args.insert("username".to_string(), serde_json::Value::String(username));
        }

        if let Ok(password) = env::var(ENV_AUDIT_MQTT_PASSWORD) {
            args.insert("password".to_string(), serde_json::Value::String(password));
        }

        if let Ok(qos) = env::var(ENV_AUDIT_MQTT_QOS) {
            if let Ok(qos_level) = qos.parse::<u8>() {
                args.insert("qos".to_string(), serde_json::Value::Number(serde_json::Number::from(qos_level)));
            }
        }

        if let Ok(keep_alive) = env::var(ENV_AUDIT_MQTT_KEEP_ALIVE_INTERVAL) {
            if let Ok(interval_ms) = keep_alive.trim_end_matches('s').parse::<u64>() {
                args.insert("keep_alive_ms".to_string(), serde_json::Value::Number(serde_json::Number::from(interval_ms * 1000)));
            }
        }

        if let Ok(reconnect) = env::var(ENV_AUDIT_MQTT_RECONNECT_INTERVAL) {
            if let Ok(interval_ms) = reconnect.trim_end_matches('s').parse::<u64>() {
                args.insert("reconnect_interval_ms".to_string(), serde_json::Value::Number(serde_json::Number::from(interval_ms * 1000)));
            }
        }

        if let Ok(queue_limit) = env::var(ENV_AUDIT_MQTT_QUEUE_LIMIT) {
            if let Ok(limit) = queue_limit.parse::<u64>() {
                args.insert("queue_limit".to_string(), serde_json::Value::Number(serde_json::Number::from(limit)));
            }
        }

        if let Ok(queue_dir) = env::var(ENV_AUDIT_MQTT_QUEUE_DIR) {
            args.insert("queue_dir".to_string(), serde_json::Value::String(queue_dir));
        }

        Ok(Some(AuditTargetConfig {
            id: "env-mqtt".to_string(),
            kind: "mqtt".to_string(),
            enabled: true,
            args: serde_json::Value::Object(args),
        }))
    }

    /// Check if a target is enabled in KVS
    fn is_enabled_from_kvs(&self, kvs: &rustfs_ecstore::config::KVS) -> bool {
        kvs.0.iter()
            .find(|kv| kv.key == ENABLE_KEY)
            .map(|kv| kv.value.to_lowercase() == "on" || kv.value.to_lowercase() == "true")
            .unwrap_or(false)
    }

    /// Create webhook config from KVS template
    fn create_webhook_config_from_kvs(&self, kvs: &rustfs_ecstore::config::KVS) -> Result<AuditTargetConfig, AuditError> {
        let mut args = serde_json::Map::new();
        args.insert("target_type".to_string(), serde_json::Value::String("AuditLog".to_string()));

        for kv in &kvs.0 {
            match kv.key.as_str() {
                WEBHOOK_ENDPOINT => {
                    if !kv.value.is_empty() {
                        args.insert("url".to_string(), serde_json::Value::String(kv.value.clone()));
                    }
                }
                WEBHOOK_AUTH_TOKEN => {
                    if !kv.value.is_empty() {
                        args.insert("auth_token".to_string(), serde_json::Value::String(kv.value.clone()));
                    }
                }
                WEBHOOK_HTTP_TIMEOUT => {
                    if let Ok(timeout_ms) = kv.value.trim_end_matches('s').parse::<u64>() {
                        args.insert("timeout_ms".to_string(), serde_json::Value::Number(serde_json::Number::from(timeout_ms * 1000)));
                    }
                }
                WEBHOOK_MAX_RETRY => {
                    if let Ok(retries) = kv.value.parse::<u64>() {
                        args.insert("retries".to_string(), serde_json::Value::Number(serde_json::Number::from(retries)));
                    }
                }
                WEBHOOK_BATCH_SIZE => {
                    if let Ok(batch_size) = kv.value.parse::<u64>() {
                        args.insert("batch_size".to_string(), serde_json::Value::Number(serde_json::Number::from(batch_size)));
                    }
                }
                _ => {} // Ignore other keys
            }
        }

        Ok(AuditTargetConfig {
            id: "default-webhook".to_string(),
            kind: "webhook".to_string(),
            enabled: true,
            args: serde_json::Value::Object(args),
        })
    }

    /// Create MQTT config from KVS template
    fn create_mqtt_config_from_kvs(&self, kvs: &rustfs_ecstore::config::KVS) -> Result<AuditTargetConfig, AuditError> {
        let mut args = serde_json::Map::new();
        args.insert("target_type".to_string(), serde_json::Value::String("AuditLog".to_string()));

        for kv in &kvs.0 {
            match kv.key.as_str() {
                MQTT_BROKER => {
                    if !kv.value.is_empty() {
                        args.insert("broker_url".to_string(), serde_json::Value::String(kv.value.clone()));
                    }
                }
                MQTT_TOPIC => {
                    if !kv.value.is_empty() {
                        args.insert("topic".to_string(), serde_json::Value::String(kv.value.clone()));
                    }
                }
                MQTT_USERNAME => {
                    if !kv.value.is_empty() {
                        args.insert("username".to_string(), serde_json::Value::String(kv.value.clone()));
                    }
                }
                MQTT_PASSWORD => {
                    if !kv.value.is_empty() {
                        args.insert("password".to_string(), serde_json::Value::String(kv.value.clone()));
                    }
                }
                MQTT_QOS => {
                    if let Ok(qos) = kv.value.parse::<u8>() {
                        args.insert("qos".to_string(), serde_json::Value::Number(serde_json::Number::from(qos)));
                    }
                }
                _ => {} // Ignore other keys
            }
        }

        Ok(AuditTargetConfig {
            id: "default-mqtt".to_string(),
            kind: "mqtt".to_string(),
            enabled: true,
            args: serde_json::Value::Object(args),
        })
    }

    /// Persist configuration to .rustfs.sys via rustfs-ecstore
    /// TODO: Implement when ecstore persistence API is available
    #[allow(dead_code)]
    async fn persist_config(&self, _config: &AuditConfig) -> Result<(), AuditError> {
        debug!("Persisting audit configuration to .rustfs.sys");
        // Implementation depends on rustfs-ecstore API for persistence
        warn!("Configuration persistence not yet implemented - requires rustfs-ecstore integration");
        Ok(())
    }
}

/// Convenience function to load configuration with default settings
pub async fn load_config_from_env_and_ecstore() -> Result<AuditConfig, AuditError> {
    let loader = ConfigLoader::new(None);
    loader.load_config().await
}

/// Convenience function to load configuration with custom environment prefix
pub async fn load_config_with_prefix(env_prefix: String) -> Result<AuditConfig, AuditError> {
    let loader = ConfigLoader::new(Some(env_prefix));
    loader.load_config().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[tokio::test]
    async fn test_default_config_creation() {
        let config = AuditConfig::default();
        assert!(!config.enabled);
        assert!(config.targets.is_empty());
        assert_eq!(config.redaction.headers_blacklist.len(), 4);
        assert_eq!(config.performance.batch_size, 50);
    }

    #[tokio::test]
    async fn test_env_webhook_config() {
        env::set_var(ENV_AUDIT_WEBHOOK_ENABLE, "on");
        env::set_var(ENV_AUDIT_WEBHOOK_ENDPOINT, "https://test.example.com/webhook");
        env::set_var(ENV_AUDIT_WEBHOOK_AUTH_TOKEN, "Bearer test123");

        let loader = ConfigLoader::new(None);
        let config = loader.load_config().await.unwrap();

        // Should have webhook target from environment
        let webhook = config.targets.iter().find(|t| t.kind == "webhook");
        assert!(webhook.is_some());
        
        let webhook = webhook.unwrap();
        assert_eq!(webhook.id, "env-webhook");
        assert!(webhook.enabled);

        // Clean up environment
        env::remove_var(ENV_AUDIT_WEBHOOK_ENABLE);
        env::remove_var(ENV_AUDIT_WEBHOOK_ENDPOINT);
        env::remove_var(ENV_AUDIT_WEBHOOK_AUTH_TOKEN);
    }

    #[tokio::test]
    async fn test_env_mqtt_config() {
        env::set_var(ENV_AUDIT_MQTT_ENABLE, "true");
        env::set_var(ENV_AUDIT_MQTT_BROKER, "mqtt://test.broker:1883");
        env::set_var(ENV_AUDIT_MQTT_TOPIC, "test/audit");

        let loader = ConfigLoader::new(None);
        let config = loader.load_config().await.unwrap();

        // Should have MQTT target from environment
        let mqtt = config.targets.iter().find(|t| t.kind == "mqtt");
        assert!(mqtt.is_some());
        
        let mqtt = mqtt.unwrap();
        assert_eq!(mqtt.id, "env-mqtt");
        assert!(mqtt.enabled);

        // Clean up environment
        env::remove_var(ENV_AUDIT_MQTT_ENABLE);
        env::remove_var(ENV_AUDIT_MQTT_BROKER);
        env::remove_var(ENV_AUDIT_MQTT_TOPIC);
    }

    #[tokio::test]
    async fn test_custom_env_prefix() {
        env::set_var("CUSTOM_AUDIT_WEBHOOK_ENABLED", "on");
        env::set_var("CUSTOM_AUDIT_WEBHOOK_ENDPOINT", "https://custom.example.com/webhook");

        let loader = ConfigLoader::new(Some("CUSTOM_".to_string()));
        let config = loader.load_config().await.unwrap();

        let webhook = config.targets.iter().find(|t| t.kind == "webhook");
        assert!(webhook.is_some());

        // Clean up environment  
        env::remove_var("CUSTOM_AUDIT_WEBHOOK_ENABLED");
        env::remove_var("CUSTOM_AUDIT_WEBHOOK_ENDPOINT");
    }
}