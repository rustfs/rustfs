use crate::store::DEFAULT_LIMIT;
use crate::{
    config::KVS,
    error::TargetError,
    target::{mqtt::MQTTArgs, webhook::WebhookArgs, Target},
};
use async_trait::async_trait;
use rumqttc::QoS;
use std::time::Duration;
use tracing::warn;
use url::Url;

/// Trait for creating targets from configuration
#[async_trait]
pub trait TargetFactory: Send + Sync {
    /// Creates a target from configuration
    async fn create_target(
        &self,
        id: String,
        config: &KVS,
    ) -> Result<Box<dyn Target + Send + Sync>, TargetError>;

    /// Validates target configuration
    fn validate_config(&self, config: &KVS) -> Result<(), TargetError>;
}

/// Factory for creating Webhook targets
pub struct WebhookTargetFactory;

#[async_trait]
impl TargetFactory for WebhookTargetFactory {
    async fn create_target(
        &self,
        id: String,
        config: &KVS,
    ) -> Result<Box<dyn Target + Send + Sync>, TargetError> {
        // Parse configuration values
        let enable = config.lookup("enable").unwrap_or("off") == "on";
        if !enable {
            return Err(TargetError::Configuration("Target is disabled".to_string()));
        }

        let endpoint = config
            .lookup("endpoint")
            .ok_or_else(|| TargetError::Configuration("Missing endpoint".to_string()))?;
        let endpoint_url = Url::parse(endpoint)
            .map_err(|e| TargetError::Configuration(format!("Invalid endpoint URL: {}", e)))?;

        let auth_token = config.lookup("auth_token").unwrap_or("").to_string();
        let queue_dir = config.lookup("queue_dir").unwrap_or("").to_string();

        let queue_limit = config
            .lookup("queue_limit")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_LIMIT);

        let client_cert = config.lookup("client_cert").unwrap_or("").to_string();
        let client_key = config.lookup("client_key").unwrap_or("").to_string();

        // Create and return Webhook target
        let args = WebhookArgs {
            enable,
            endpoint: endpoint_url,
            auth_token,
            queue_dir,
            queue_limit,
            client_cert,
            client_key,
        };

        let target = crate::target::webhook::WebhookTarget::new(id, args)?;
        Ok(Box::new(target))
    }

    fn validate_config(&self, config: &KVS) -> Result<(), TargetError> {
        let enable = config.lookup("enable").unwrap_or("off") == "on";
        if !enable {
            return Ok(());
        }

        // Validate endpoint
        let endpoint = config
            .lookup("endpoint")
            .ok_or_else(|| TargetError::Configuration("Missing endpoint".to_string()))?;
        Url::parse(endpoint)
            .map_err(|e| TargetError::Configuration(format!("Invalid endpoint URL: {}", e)))?;

        // Validate TLS certificates
        let client_cert = config.lookup("client_cert").unwrap_or("");
        let client_key = config.lookup("client_key").unwrap_or("");

        if (!client_cert.is_empty() && client_key.is_empty())
            || (client_cert.is_empty() && !client_key.is_empty())
        {
            return Err(TargetError::Configuration(
                "Both client_cert and client_key must be specified if using client certificates"
                    .to_string(),
            ));
        }

        // Validate queue directory
        let queue_dir = config.lookup("queue_dir").unwrap_or("");
        if !queue_dir.is_empty() && !std::path::Path::new(queue_dir).is_absolute() {
            return Err(TargetError::Configuration(
                "Webhook Queue directory must be an absolute path".to_string(),
            ));
        }

        Ok(())
    }
}

/// Factory for creating MQTT targets
pub struct MQTTTargetFactory;

#[async_trait]
impl TargetFactory for MQTTTargetFactory {
    async fn create_target(
        &self,
        id: String,
        config: &KVS,
    ) -> Result<Box<dyn Target + Send + Sync>, TargetError> {
        // Parse configuration values
        let enable = config.lookup("enable").unwrap_or("off") == "on";
        if !enable {
            return Err(TargetError::Configuration("Target is disabled".to_string()));
        }

        let broker = config
            .lookup("broker")
            .ok_or_else(|| TargetError::Configuration("Missing broker".to_string()))?;
        let broker_url = Url::parse(broker)
            .map_err(|e| TargetError::Configuration(format!("Invalid broker URL: {}", e)))?;

        let topic = config
            .lookup("topic")
            .ok_or_else(|| TargetError::Configuration("Missing topic".to_string()))?;

        let qos = config
            .lookup("qos")
            .and_then(|v| v.parse::<u8>().ok())
            .map(|q| match q {
                0 => QoS::AtMostOnce,
                1 => QoS::AtLeastOnce,
                2 => QoS::ExactlyOnce,
                _ => QoS::AtMostOnce,
            })
            .unwrap_or(QoS::AtLeastOnce);

        let username = config.lookup("username").unwrap_or("").to_string();
        let password = config.lookup("password").unwrap_or("").to_string();

        let reconnect_interval = config
            .lookup("reconnect_interval")
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(5));

        let keep_alive = config
            .lookup("keep_alive_interval")
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(30));

        let queue_dir = config.lookup("queue_dir").unwrap_or("").to_string();
        let queue_limit = config
            .lookup("queue_limit")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_LIMIT);

        // Create and return MQTT target
        let args = MQTTArgs {
            enable,
            broker: broker_url,
            topic: topic.to_string(),
            qos,
            username,
            password,
            max_reconnect_interval: reconnect_interval,
            keep_alive,
            queue_dir,
            queue_limit,
        };

        let target = crate::target::mqtt::MQTTTarget::new(id, args)?;
        Ok(Box::new(target))
    }

    fn validate_config(&self, config: &KVS) -> Result<(), TargetError> {
        let enable = config.lookup("enable").unwrap_or("off") == "on";
        if !enable {
            return Ok(());
        }

        // Validate broker URL
        let broker = config
            .lookup("broker")
            .ok_or_else(|| TargetError::Configuration("Missing broker".to_string()))?;
        let url = Url::parse(broker)
            .map_err(|e| TargetError::Configuration(format!("Invalid broker URL: {}", e)))?;

        // Validate supported schemes
        match url.scheme() {
            "tcp" | "ssl" | "ws" | "wss" | "mqtt" | "mqtts" => {}
            _ => {
                return Err(TargetError::Configuration(
                    "Unsupported broker URL scheme".to_string(),
                ));
            }
        }

        // Validate topic
        if config.lookup("topic").is_none() {
            return Err(TargetError::Configuration("Missing topic".to_string()));
        }

        // Validate QoS
        if let Some(qos_str) = config.lookup("qos") {
            let qos = qos_str
                .parse::<u8>()
                .map_err(|_| TargetError::Configuration("Invalid QoS value".to_string()))?;
            if qos > 2 {
                return Err(TargetError::Configuration(
                    "QoS must be 0, 1, or 2".to_string(),
                ));
            }
        }

        // Validate queue directory
        let queue_dir = config.lookup("queue_dir").unwrap_or("");
        if !queue_dir.is_empty() {
            if !std::path::Path::new(queue_dir).is_absolute() {
                return Err(TargetError::Configuration(
                    "mqtt Queue directory must be an absolute path".to_string(),
                ));
            }

            if let Some(qos_str) = config.lookup("qos") {
                if qos_str == "0" {
                    warn!("Using queue_dir with QoS 0 may result in event loss");
                }
            }
        }

        Ok(())
    }
}
