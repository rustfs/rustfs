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

//! Configuration management for the audit system with environment-first precedence

use crate::error::{AuditError, AuditResult};
use rustfs_config::EnableState;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;

/// Environment variable prefix for audit configuration
const ENV_PREFIX: &str = "RUSTFS_AUDIT";

/// Environment variable constants
pub const ENV_AUDIT_WEBHOOK_ENABLE: &str = "RUSTFS_AUDIT_WEBHOOK_ENABLE"; 
pub const ENV_AUDIT_WEBHOOK_ENDPOINT: &str = "RUSTFS_AUDIT_WEBHOOK_ENDPOINT";
pub const ENV_AUDIT_WEBHOOK_AUTH_TOKEN: &str = "RUSTFS_AUDIT_WEBHOOK_AUTH_TOKEN";
pub const ENV_AUDIT_WEBHOOK_CLIENT_CERT: &str = "RUSTFS_AUDIT_WEBHOOK_CLIENT_CERT";
pub const ENV_AUDIT_WEBHOOK_CLIENT_KEY: &str = "RUSTFS_AUDIT_WEBHOOK_CLIENT_KEY";
pub const ENV_AUDIT_WEBHOOK_BATCH_SIZE: &str = "RUSTFS_AUDIT_WEBHOOK_BATCH_SIZE";
pub const ENV_AUDIT_WEBHOOK_QUEUE_SIZE: &str = "RUSTFS_AUDIT_WEBHOOK_QUEUE_SIZE";
pub const ENV_AUDIT_WEBHOOK_QUEUE_DIR: &str = "RUSTFS_AUDIT_WEBHOOK_QUEUE_DIR";
pub const ENV_AUDIT_WEBHOOK_MAX_RETRY: &str = "RUSTFS_AUDIT_WEBHOOK_MAX_RETRY";
pub const ENV_AUDIT_WEBHOOK_RETRY_INTERVAL: &str = "RUSTFS_AUDIT_WEBHOOK_RETRY_INTERVAL";
pub const ENV_AUDIT_WEBHOOK_HTTP_TIMEOUT: &str = "RUSTFS_AUDIT_WEBHOOK_HTTP_TIMEOUT";

pub const ENV_AUDIT_MQTT_ENABLE: &str = "RUSTFS_AUDIT_MQTT_ENABLE";
pub const ENV_AUDIT_MQTT_BROKER: &str = "RUSTFS_AUDIT_MQTT_BROKER";
pub const ENV_AUDIT_MQTT_TOPIC: &str = "RUSTFS_AUDIT_MQTT_TOPIC";
pub const ENV_AUDIT_MQTT_USERNAME: &str = "RUSTFS_AUDIT_MQTT_USERNAME";
pub const ENV_AUDIT_MQTT_PASSWORD: &str = "RUSTFS_AUDIT_MQTT_PASSWORD";
pub const ENV_AUDIT_MQTT_QOS: &str = "RUSTFS_AUDIT_MQTT_QOS";
pub const ENV_AUDIT_MQTT_KEEP_ALIVE: &str = "RUSTFS_AUDIT_MQTT_KEEP_ALIVE";
pub const ENV_AUDIT_MQTT_RECONNECT_INTERVAL: &str = "RUSTFS_AUDIT_MQTT_RECONNECT_INTERVAL";
pub const ENV_AUDIT_MQTT_QUEUE_DIR: &str = "RUSTFS_AUDIT_MQTT_QUEUE_DIR";
pub const ENV_AUDIT_MQTT_QUEUE_SIZE: &str = "RUSTFS_AUDIT_MQTT_QUEUE_SIZE";

/// Main audit configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditConfig {
    /// Enable audit logging globally
    pub enabled: bool,
    /// List of audit targets
    pub targets: Vec<AuditTargetConfig>,
    /// Header redaction configuration
    pub redaction: RedactionConfig,
    /// Performance configuration
    pub performance: PerformanceConfig,
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

/// Configuration for a specific audit target
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditTargetConfig {
    /// Unique identifier for the target
    pub id: String,
    /// Target type (webhook, mqtt, etc.)
    pub kind: String,
    /// Whether this target is enabled
    pub enabled: bool,
    /// Target-specific configuration arguments
    pub args: serde_json::Value,
}

/// Header redaction configuration for security
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactionConfig {
    /// List of header names to redact
    pub headers_blacklist: Vec<String>,
}

impl Default for RedactionConfig {
    fn default() -> Self {
        Self {
            headers_blacklist: vec![
                "Authorization".to_string(),
                "Cookie".to_string(),
                "X-Api-Key".to_string(),
                "X-Auth-Token".to_string(),
                "X-Access-Token".to_string(),
            ],
        }
    }
}

/// Performance configuration for the audit system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Maximum batch size for processing
    pub batch_size: usize,
    /// Batch timeout in milliseconds
    pub batch_timeout_ms: u64,
    /// Maximum concurrent dispatches
    pub max_concurrent: usize,
    /// Enable performance metrics
    pub enable_metrics: bool,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            batch_size: 50,
            batch_timeout_ms: 100,
            max_concurrent: 10,
            enable_metrics: true,
        }
    }
}

/// Load audit configuration from environment variables with precedence
pub fn load_config_from_env() -> AuditResult<AuditConfig> {
    let mut config = AuditConfig::default();
    let mut targets = Vec::new();

    // Load webhook configuration if enabled
    if let Ok(enabled) = env::var(ENV_AUDIT_WEBHOOK_ENABLE) {
        if parse_bool(&enabled)? {
            let webhook_config = load_webhook_config_from_env()?;
            targets.push(webhook_config);
            config.enabled = true;
        }
    }

    // Load MQTT configuration if enabled
    if let Ok(enabled) = env::var(ENV_AUDIT_MQTT_ENABLE) {
        if parse_bool(&enabled)? {
            let mqtt_config = load_mqtt_config_from_env()?;
            targets.push(mqtt_config);
            config.enabled = true;
        }
    }

    config.targets = targets;
    Ok(config)
}

/// Load webhook target configuration from environment variables
fn load_webhook_config_from_env() -> AuditResult<AuditTargetConfig> {
    let mut args = serde_json::Map::new();
    
    // Required fields
    let endpoint = env::var(ENV_AUDIT_WEBHOOK_ENDPOINT)
        .map_err(|_| AuditError::ConfigurationError("WEBHOOK_ENDPOINT is required".to_string()))?;
    args.insert("target_type".to_string(), "audit_log".into());
    args.insert("url".to_string(), endpoint.into());
    
    // Optional fields with defaults
    if let Ok(token) = env::var(ENV_AUDIT_WEBHOOK_AUTH_TOKEN) {
        args.insert("auth_token".to_string(), token.into());
    }
    
    if let Ok(cert) = env::var(ENV_AUDIT_WEBHOOK_CLIENT_CERT) {
        args.insert("client_cert".to_string(), cert.into());
    }
    
    if let Ok(key) = env::var(ENV_AUDIT_WEBHOOK_CLIENT_KEY) {
        args.insert("client_key".to_string(), key.into());
    }
    
    let batch_size = env::var(ENV_AUDIT_WEBHOOK_BATCH_SIZE)
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(1);
    args.insert("batch_size".to_string(), batch_size.into());
    
    let queue_size = env::var(ENV_AUDIT_WEBHOOK_QUEUE_SIZE)
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(1000);
    args.insert("queue_size".to_string(), queue_size.into());
    
    if let Ok(queue_dir) = env::var(ENV_AUDIT_WEBHOOK_QUEUE_DIR) {
        args.insert("queue_dir".to_string(), queue_dir.into());
    }
    
    let max_retry = env::var(ENV_AUDIT_WEBHOOK_MAX_RETRY)
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(3);
    args.insert("max_retry".to_string(), max_retry.into());
    
    let retry_interval = env::var(ENV_AUDIT_WEBHOOK_RETRY_INTERVAL)
        .unwrap_or_else(|_| "3s".to_string());
    args.insert("retry_interval".to_string(), retry_interval.into());
    
    let timeout = env::var(ENV_AUDIT_WEBHOOK_HTTP_TIMEOUT)
        .unwrap_or_else(|_| "5s".to_string());
    args.insert("timeout_ms".to_string(), timeout.into());

    Ok(AuditTargetConfig {
        id: "audit-webhook-1".to_string(),
        kind: "webhook".to_string(),
        enabled: true,
        args: serde_json::Value::Object(args),
    })
}

/// Load MQTT target configuration from environment variables
fn load_mqtt_config_from_env() -> AuditResult<AuditTargetConfig> {
    let mut args = serde_json::Map::new();
    
    // Required fields
    let broker = env::var(ENV_AUDIT_MQTT_BROKER)
        .map_err(|_| AuditError::ConfigurationError("MQTT_BROKER is required".to_string()))?;
    let topic = env::var(ENV_AUDIT_MQTT_TOPIC)
        .unwrap_or_else(|_| "rustfs/audit".to_string());
    
    args.insert("target_type".to_string(), "audit_log".into());
    args.insert("broker_url".to_string(), broker.into());
    args.insert("topic".to_string(), topic.into());
    
    // Optional authentication
    if let Ok(username) = env::var(ENV_AUDIT_MQTT_USERNAME) {
        args.insert("username".to_string(), username.into());
    }
    
    if let Ok(password) = env::var(ENV_AUDIT_MQTT_PASSWORD) {
        args.insert("password".to_string(), password.into());
    }
    
    // Optional configuration
    let qos = env::var(ENV_AUDIT_MQTT_QOS)
        .ok()
        .and_then(|s| s.parse::<u8>().ok())
        .unwrap_or(1);
    args.insert("qos".to_string(), qos.into());
    
    let keep_alive = env::var(ENV_AUDIT_MQTT_KEEP_ALIVE)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(60);
    args.insert("keep_alive".to_string(), keep_alive.into());
    
    let reconnect_interval = env::var(ENV_AUDIT_MQTT_RECONNECT_INTERVAL)
        .unwrap_or_else(|_| "10s".to_string());
    args.insert("reconnect_interval".to_string(), reconnect_interval.into());
    
    if let Ok(queue_dir) = env::var(ENV_AUDIT_MQTT_QUEUE_DIR) {
        args.insert("queue_dir".to_string(), queue_dir.into());
    }
    
    let queue_size = env::var(ENV_AUDIT_MQTT_QUEUE_SIZE)
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(1000);
    args.insert("queue_size".to_string(), queue_size.into());

    Ok(AuditTargetConfig {
        id: "audit-mqtt-1".to_string(),
        kind: "mqtt".to_string(),
        enabled: true,
        args: serde_json::Value::Object(args),
    })
}

/// Parse boolean value from string (supports multiple formats)
fn parse_bool(value: &str) -> AuditResult<bool> {
    match value.to_lowercase().as_str() {
        "true" | "on" | "yes" | "1" | "enabled" => Ok(true),
        "false" | "off" | "no" | "0" | "disabled" => Ok(false),
        _ => Err(AuditError::ConfigurationError(format!(
            "Invalid boolean value: {}",
            value
        ))),
    }
}

/// Validate audit configuration
pub fn validate_config(config: &AuditConfig) -> AuditResult<()> {
    if config.enabled && config.targets.is_empty() {
        return Err(AuditError::ConfigurationError(
            "No targets configured but audit is enabled".to_string(),
        ));
    }

    for target in &config.targets {
        validate_target_config(target)?;
    }

    Ok(())
}

/// Validate individual target configuration
fn validate_target_config(target: &AuditTargetConfig) -> AuditResult<()> {
    if target.id.is_empty() {
        return Err(AuditError::ConfigurationError(
            "Target ID cannot be empty".to_string(),
        ));
    }

    match target.kind.as_str() {
        "webhook" => validate_webhook_config(&target.args)?,
        "mqtt" => validate_mqtt_config(&target.args)?,
        _ => {
            return Err(AuditError::UnsupportedTargetType(target.kind.clone()));
        }
    }

    Ok(())
}

/// Validate webhook target configuration
fn validate_webhook_config(args: &serde_json::Value) -> AuditResult<()> {
    let obj = args.as_object().ok_or_else(|| {
        AuditError::ConfigurationError("Webhook args must be an object".to_string())
    })?;

    if !obj.contains_key("url") {
        return Err(AuditError::ConfigurationError(
            "Webhook URL is required".to_string(),
        ));
    }

    // Validate target_type if present
    if let Some(target_type) = obj.get("target_type") {
        if target_type.as_str() != Some("audit_log") {
            return Err(AuditError::ConfigurationError(
                "Target type must be 'audit_log' for audit targets".to_string(),
            ));
        }
    }

    Ok(())
}

/// Validate MQTT target configuration
fn validate_mqtt_config(args: &serde_json::Value) -> AuditResult<()> {
    let obj = args.as_object().ok_or_else(|| {
        AuditError::ConfigurationError("MQTT args must be an object".to_string())
    })?;

    if !obj.contains_key("broker_url") {
        return Err(AuditError::ConfigurationError(
            "MQTT broker URL is required".to_string(),
        ));
    }

    if !obj.contains_key("topic") {
        return Err(AuditError::ConfigurationError(
            "MQTT topic is required".to_string(),
        ));
    }

    // Validate target_type if present
    if let Some(target_type) = obj.get("target_type") {
        if target_type.as_str() != Some("audit_log") {
            return Err(AuditError::ConfigurationError(
                "Target type must be 'audit_log' for audit targets".to_string(),
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_parse_bool() {
        assert!(parse_bool("true").unwrap());
        assert!(parse_bool("on").unwrap());
        assert!(parse_bool("yes").unwrap());
        assert!(parse_bool("1").unwrap());
        assert!(parse_bool("enabled").unwrap());
        
        assert!(!parse_bool("false").unwrap());
        assert!(!parse_bool("off").unwrap());
        assert!(!parse_bool("no").unwrap());
        assert!(!parse_bool("0").unwrap());
        assert!(!parse_bool("disabled").unwrap());
        
        assert!(parse_bool("invalid").is_err());
    }

    #[test]
    fn test_config_validation() {
        let mut config = AuditConfig::default();
        assert!(validate_config(&config).is_ok()); // Disabled config is valid
        
        config.enabled = true;
        assert!(validate_config(&config).is_err()); // Enabled with no targets is invalid
        
        config.targets.push(AuditTargetConfig {
            id: "test".to_string(),
            kind: "webhook".to_string(),
            enabled: true,
            args: serde_json::json!({
                "url": "http://localhost:8080/webhook",
                "target_type": "audit_log"
            }),
        });
        assert!(validate_config(&config).is_ok()); // Valid configuration
    }

    #[test]
    fn test_webhook_config_validation() {
        let valid_args = serde_json::json!({
            "url": "http://localhost:8080/webhook",
            "target_type": "audit_log"
        });
        assert!(validate_webhook_config(&valid_args).is_ok());
        
        let invalid_args = serde_json::json!({
            "no_url": "missing"
        });
        assert!(validate_webhook_config(&invalid_args).is_err());
    }

    #[test]
    fn test_mqtt_config_validation() {
        let valid_args = serde_json::json!({
            "broker_url": "mqtt://localhost:1883",
            "topic": "test/topic",
            "target_type": "audit_log"
        });
        assert!(validate_mqtt_config(&valid_args).is_ok());
        
        let invalid_args = serde_json::json!({
            "no_broker": "missing"
        });
        assert!(validate_mqtt_config(&invalid_args).is_err());
    }
}