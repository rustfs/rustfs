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

//! Target implementations for audit logging

pub mod mqtt;
pub mod webhook;

pub use mqtt::MqttAuditTarget;
pub use webhook::WebhookAuditTarget;

use crate::error::{TargetError, TargetResult};
use crate::registry::{AuditTarget, AuditTargetConfig, AuditTargetFactory};
use async_trait::async_trait;

/// Default audit target factory that supports webhook and MQTT targets
pub struct DefaultAuditTargetFactory;

impl DefaultAuditTargetFactory {
    /// Create a new default factory
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AuditTargetFactory for DefaultAuditTargetFactory {
    async fn create_target(&self, config: &AuditTargetConfig) -> TargetResult<Box<dyn AuditTarget>> {
        // Validate target_type field name (should be "kind" per specification)
        let target_type = if config.target_type == "webhook" || config.target_type == "mqtt" {
            &config.target_type
        } else {
            // Check if using the spec'd "kind" field in args
            config
                .args
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or(&config.target_type)
        };

        match target_type {
            "webhook" => {
                let target = WebhookAuditTarget::from_config(config).await?;
                Ok(Box::new(target))
            }
            "mqtt" => {
                let target = MqttAuditTarget::from_config(config).await?;
                Ok(Box::new(target))
            }
            _ => Err(TargetError::UnsupportedType {
                target_type: target_type.to_string(),
            }),
        }
    }

    fn validate_config(&self, config: &AuditTargetConfig) -> TargetResult<()> {
        if config.id.is_empty() {
            return Err(TargetError::InvalidConfig {
                message: "Target ID cannot be empty".to_string(),
            });
        }

        // Validate target_type/kind field
        let target_type = if config.target_type == "webhook" || config.target_type == "mqtt" {
            &config.target_type
        } else {
            config
                .args
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or(&config.target_type)
        };

        match target_type {
            "webhook" => WebhookAuditTarget::validate_config(config),
            "mqtt" => MqttAuditTarget::validate_config(config),
            _ => Err(TargetError::UnsupportedType {
                target_type: target_type.to_string(),
            }),
        }
    }

    fn supported_types(&self) -> Vec<&'static str> {
        vec!["webhook", "mqtt"]
    }
}

impl Default for DefaultAuditTargetFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_factory_supported_types() {
        let factory = DefaultAuditTargetFactory::new();
        let types = factory.supported_types();
        assert_eq!(types, vec!["webhook", "mqtt"]);
    }

    #[test]
    fn test_factory_validate_config_empty_id() {
        let factory = DefaultAuditTargetFactory::new();
        let config = AuditTargetConfig {
            id: String::new(),
            target_type: "webhook".to_string(),
            enabled: true,
            args: json!({"url": "http://example.com"}),
        };

        let result = factory.validate_config(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Target ID cannot be empty"));
    }

    #[test]
    fn test_factory_validate_config_unsupported_type() {
        let factory = DefaultAuditTargetFactory::new();
        let config = AuditTargetConfig {
            id: "test".to_string(),
            target_type: "unsupported".to_string(),
            enabled: true,
            args: json!({}),
        };

        let result = factory.validate_config(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unsupported target type"));
    }

    #[test]
    fn test_factory_validate_config_kind_field() {
        let factory = DefaultAuditTargetFactory::new();
        let config = AuditTargetConfig {
            id: "test".to_string(),
            target_type: "legacy_type".to_string(),
            enabled: true,
            args: json!({
                "kind": "webhook",
                "url": "http://example.com"
            }),
        };

        // Should not fail due to unsupported target_type since kind is valid
        let result = factory.validate_config(&config);
        println!("Validation result: {:?}", result);
        // This should succeed since we have a valid URL and webhook type via kind field
        assert!(result.is_ok(), "Validation should succeed with valid kind field");
    }
}
