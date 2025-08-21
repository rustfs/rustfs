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

use async_trait::async_trait;
use rumqttc::QoS;
use rustfs_config::notify::{
    ENV_NOTIFY_MQTT_KEYS, ENV_NOTIFY_WEBHOOK_KEYS, MQTT_BROKER, MQTT_KEEP_ALIVE_INTERVAL, MQTT_PASSWORD, MQTT_QOS,
    MQTT_QUEUE_DIR, MQTT_QUEUE_LIMIT, MQTT_RECONNECT_INTERVAL, MQTT_TOPIC, MQTT_USERNAME, NOTIFY_MQTT_KEYS, NOTIFY_WEBHOOK_KEYS,
    WEBHOOK_AUTH_TOKEN, WEBHOOK_CLIENT_CERT, WEBHOOK_CLIENT_KEY, WEBHOOK_ENDPOINT, WEBHOOK_QUEUE_DIR, WEBHOOK_QUEUE_LIMIT,
};

use crate::Event;
use rustfs_config::{DEFAULT_DIR, DEFAULT_LIMIT};
use rustfs_ecstore::config::KVS;
use rustfs_targets::{
    Target,
    error::TargetError,
    target::{mqtt::MQTTArgs, webhook::WebhookArgs},
};
use std::collections::HashSet;
use std::time::Duration;
use tracing::{debug, warn};
use url::Url;

/// Trait for creating targets from configuration
#[async_trait]
pub trait TargetFactory: Send + Sync {
    /// Creates a target from configuration
    async fn create_target(&self, id: String, config: &KVS) -> Result<Box<dyn Target<Event> + Send + Sync>, TargetError>;

    /// Validates target configuration
    fn validate_config(&self, id: &str, config: &KVS) -> Result<(), TargetError>;

    /// Returns a set of valid configuration field names for this target type.
    /// This is used to filter environment variables.
    fn get_valid_fields(&self) -> HashSet<String>;

    /// Returns a set of valid configuration env field names for this target type.
    /// This is used to filter environment variables.
    fn get_valid_env_fields(&self) -> HashSet<String>;
}

/// Factory for creating Webhook targets
pub struct WebhookTargetFactory;

#[async_trait]
impl TargetFactory for WebhookTargetFactory {
    async fn create_target(&self, id: String, config: &KVS) -> Result<Box<dyn Target<Event> + Send + Sync>, TargetError> {
        // All config values are now read directly from the merged `config` KVS.
        let endpoint = config
            .lookup(WEBHOOK_ENDPOINT)
            .ok_or_else(|| TargetError::Configuration("Missing webhook endpoint".to_string()))?;
        let endpoint_url = Url::parse(&endpoint)
            .map_err(|e| TargetError::Configuration(format!("Invalid endpoint URL: {e} (value: '{endpoint}')")))?;

        let args = WebhookArgs {
            enable: true, // If we are here, it's already enabled.
            endpoint: endpoint_url,
            auth_token: config.lookup(WEBHOOK_AUTH_TOKEN).unwrap_or_default(),
            queue_dir: config.lookup(WEBHOOK_QUEUE_DIR).unwrap_or(DEFAULT_DIR.to_string()),
            queue_limit: config
                .lookup(WEBHOOK_QUEUE_LIMIT)
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(DEFAULT_LIMIT),
            client_cert: config.lookup(WEBHOOK_CLIENT_CERT).unwrap_or_default(),
            client_key: config.lookup(WEBHOOK_CLIENT_KEY).unwrap_or_default(),
            target_type: rustfs_targets::target::TargetType::NotifyEvent,
        };

        let target = rustfs_targets::target::webhook::WebhookTarget::new(id, args)?;
        Ok(Box::new(target))
    }

    fn validate_config(&self, _id: &str, config: &KVS) -> Result<(), TargetError> {
        // Validation also uses the merged `config` KVS directly.
        let endpoint = config
            .lookup(WEBHOOK_ENDPOINT)
            .ok_or_else(|| TargetError::Configuration("Missing webhook endpoint".to_string()))?;
        debug!("endpoint: {}", endpoint);
        let parsed_endpoint = endpoint.trim();
        Url::parse(parsed_endpoint)
            .map_err(|e| TargetError::Configuration(format!("Invalid endpoint URL: {e} (value: '{parsed_endpoint}')")))?;

        let client_cert = config.lookup(WEBHOOK_CLIENT_CERT).unwrap_or_default();
        let client_key = config.lookup(WEBHOOK_CLIENT_KEY).unwrap_or_default();

        if client_cert.is_empty() != client_key.is_empty() {
            return Err(TargetError::Configuration(
                "Both client_cert and client_key must be specified together".to_string(),
            ));
        }

        let queue_dir = config.lookup(WEBHOOK_QUEUE_DIR).unwrap_or(DEFAULT_DIR.to_string());
        if !queue_dir.is_empty() && !std::path::Path::new(&queue_dir).is_absolute() {
            return Err(TargetError::Configuration("Webhook queue directory must be an absolute path".to_string()));
        }

        Ok(())
    }

    fn get_valid_fields(&self) -> HashSet<String> {
        NOTIFY_WEBHOOK_KEYS.iter().map(|s| s.to_string()).collect()
    }

    fn get_valid_env_fields(&self) -> HashSet<String> {
        ENV_NOTIFY_WEBHOOK_KEYS.iter().map(|s| s.to_string()).collect()
    }
}

/// Factory for creating MQTT targets
pub struct MQTTTargetFactory;

#[async_trait]
impl TargetFactory for MQTTTargetFactory {
    async fn create_target(&self, id: String, config: &KVS) -> Result<Box<dyn Target<Event> + Send + Sync>, TargetError> {
        let broker = config
            .lookup(MQTT_BROKER)
            .ok_or_else(|| TargetError::Configuration("Missing MQTT broker".to_string()))?;
        let broker_url = Url::parse(&broker)
            .map_err(|e| TargetError::Configuration(format!("Invalid broker URL: {e} (value: '{broker}')")))?;

        let topic = config
            .lookup(MQTT_TOPIC)
            .ok_or_else(|| TargetError::Configuration("Missing MQTT topic".to_string()))?;

        let args = MQTTArgs {
            enable: true, // Assumed enabled.
            broker: broker_url,
            topic,
            qos: config
                .lookup(MQTT_QOS)
                .and_then(|v| v.parse::<u8>().ok())
                .map(|q| match q {
                    0 => QoS::AtMostOnce,
                    1 => QoS::AtLeastOnce,
                    2 => QoS::ExactlyOnce,
                    _ => QoS::AtLeastOnce,
                })
                .unwrap_or(QoS::AtLeastOnce),
            username: config.lookup(MQTT_USERNAME).unwrap_or_default(),
            password: config.lookup(MQTT_PASSWORD).unwrap_or_default(),
            max_reconnect_interval: config
                .lookup(MQTT_RECONNECT_INTERVAL)
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or_else(|| Duration::from_secs(5)),
            keep_alive: config
                .lookup(MQTT_KEEP_ALIVE_INTERVAL)
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or_else(|| Duration::from_secs(30)),
            queue_dir: config.lookup(MQTT_QUEUE_DIR).unwrap_or(DEFAULT_DIR.to_string()),
            queue_limit: config
                .lookup(MQTT_QUEUE_LIMIT)
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(DEFAULT_LIMIT),
            target_type: rustfs_targets::target::TargetType::NotifyEvent,
        };

        let target = rustfs_targets::target::mqtt::MQTTTarget::new(id, args)?;
        Ok(Box::new(target))
    }

    fn validate_config(&self, _id: &str, config: &KVS) -> Result<(), TargetError> {
        let broker = config
            .lookup(MQTT_BROKER)
            .ok_or_else(|| TargetError::Configuration("Missing MQTT broker".to_string()))?;
        let url = Url::parse(&broker)
            .map_err(|e| TargetError::Configuration(format!("Invalid broker URL: {e} (value: '{broker}')")))?;

        match url.scheme() {
            "tcp" | "ssl" | "ws" | "wss" | "mqtt" | "mqtts" => {}
            _ => {
                return Err(TargetError::Configuration("Unsupported broker URL scheme".to_string()));
            }
        }

        if config.lookup(MQTT_TOPIC).is_none() {
            return Err(TargetError::Configuration("Missing MQTT topic".to_string()));
        }

        if let Some(qos_str) = config.lookup(MQTT_QOS) {
            let qos = qos_str
                .parse::<u8>()
                .map_err(|_| TargetError::Configuration("Invalid QoS value".to_string()))?;
            if qos > 2 {
                return Err(TargetError::Configuration("QoS must be 0, 1, or 2".to_string()));
            }
        }

        let queue_dir = config.lookup(MQTT_QUEUE_DIR).unwrap_or_default();
        if !queue_dir.is_empty() {
            if !std::path::Path::new(&queue_dir).is_absolute() {
                return Err(TargetError::Configuration("MQTT queue directory must be an absolute path".to_string()));
            }
            if let Some(qos_str) = config.lookup(MQTT_QOS) {
                if qos_str == "0" {
                    warn!("Using queue_dir with QoS 0 may result in event loss");
                }
            }
        }

        Ok(())
    }

    fn get_valid_fields(&self) -> HashSet<String> {
        NOTIFY_MQTT_KEYS.iter().map(|s| s.to_string()).collect()
    }

    fn get_valid_env_fields(&self) -> HashSet<String> {
        ENV_NOTIFY_MQTT_KEYS.iter().map(|s| s.to_string()).collect()
    }
}
