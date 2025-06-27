use crate::{
    error::TargetError,
    target::{Target, mqtt::MQTTArgs, webhook::WebhookArgs},
};
use async_trait::async_trait;
use ecstore::config::{ENABLE_KEY, ENABLE_ON, KVS};
use rumqttc::QoS;
use rustfs_config::notify::{
    DEFAULT_DIR, DEFAULT_LIMIT, ENV_MQTT_BROKER, ENV_MQTT_ENABLE, ENV_MQTT_KEEP_ALIVE_INTERVAL, ENV_MQTT_PASSWORD, ENV_MQTT_QOS,
    ENV_MQTT_QUEUE_DIR, ENV_MQTT_QUEUE_LIMIT, ENV_MQTT_RECONNECT_INTERVAL, ENV_MQTT_TOPIC, ENV_MQTT_USERNAME,
    ENV_WEBHOOK_AUTH_TOKEN, ENV_WEBHOOK_CLIENT_CERT, ENV_WEBHOOK_CLIENT_KEY, ENV_WEBHOOK_ENABLE, ENV_WEBHOOK_ENDPOINT,
    ENV_WEBHOOK_QUEUE_DIR, ENV_WEBHOOK_QUEUE_LIMIT, MQTT_BROKER, MQTT_KEEP_ALIVE_INTERVAL, MQTT_PASSWORD, MQTT_QOS,
    MQTT_QUEUE_DIR, MQTT_QUEUE_LIMIT, MQTT_RECONNECT_INTERVAL, MQTT_TOPIC, MQTT_USERNAME, WEBHOOK_AUTH_TOKEN,
    WEBHOOK_CLIENT_CERT, WEBHOOK_CLIENT_KEY, WEBHOOK_ENDPOINT, WEBHOOK_QUEUE_DIR, WEBHOOK_QUEUE_LIMIT,
};
use rustfs_config::{DEFAULT_DELIMITER, ENV_WORD_DELIMITER_DASH};
use std::time::Duration;
use tracing::{debug, warn};
use url::Url;

/// Helper function to get values from environment variables or KVS configurations.
///
/// It will give priority to reading from environment variables such as `BASE_ENV_KEY_ID` and fall back to the KVS configuration if it fails.
fn get_config_value(id: &str, base_env_key: &str, config_key: &str, config: &KVS) -> Option<String> {
    let env_key = if id != DEFAULT_DELIMITER {
        format!(
            "{}{}{}",
            base_env_key,
            DEFAULT_DELIMITER,
            id.to_uppercase().replace(ENV_WORD_DELIMITER_DASH, DEFAULT_DELIMITER)
        )
    } else {
        base_env_key.to_string()
    };

    match std::env::var(&env_key) {
        Ok(val) => Some(val),
        Err(_) => config.lookup(config_key),
    }
}

/// Trait for creating targets from configuration
#[async_trait]
pub trait TargetFactory: Send + Sync {
    /// Creates a target from configuration
    async fn create_target(&self, id: String, config: &KVS) -> Result<Box<dyn Target + Send + Sync>, TargetError>;

    /// Validates target configuration
    fn validate_config(&self, id: &str, config: &KVS) -> Result<(), TargetError>;
}

/// Factory for creating Webhook targets
pub struct WebhookTargetFactory;

#[async_trait]
impl TargetFactory for WebhookTargetFactory {
    async fn create_target(&self, id: String, config: &KVS) -> Result<Box<dyn Target + Send + Sync>, TargetError> {
        let get = |base_env_key: &str, config_key: &str| get_config_value(&id, base_env_key, config_key, config);

        let enable = get(ENV_WEBHOOK_ENABLE, ENABLE_KEY)
            .map(|v| v.eq_ignore_ascii_case(ENABLE_ON) || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        if !enable {
            return Err(TargetError::Configuration("Target is disabled".to_string()));
        }

        let endpoint = get(ENV_WEBHOOK_ENDPOINT, WEBHOOK_ENDPOINT)
            .ok_or_else(|| TargetError::Configuration("Missing webhook endpoint".to_string()))?;
        let endpoint_url = Url::parse(&endpoint)
            .map_err(|e| TargetError::Configuration(format!("Invalid endpoint URL: {} (value: '{}')", e, endpoint)))?;

        let auth_token = get(ENV_WEBHOOK_AUTH_TOKEN, WEBHOOK_AUTH_TOKEN).unwrap_or_default();
        let queue_dir = get(ENV_WEBHOOK_QUEUE_DIR, WEBHOOK_QUEUE_DIR).unwrap_or(DEFAULT_DIR.to_string());

        let queue_limit = get(ENV_WEBHOOK_QUEUE_LIMIT, WEBHOOK_QUEUE_LIMIT)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_LIMIT);

        let client_cert = get(ENV_WEBHOOK_CLIENT_CERT, WEBHOOK_CLIENT_CERT).unwrap_or_default();
        let client_key = get(ENV_WEBHOOK_CLIENT_KEY, WEBHOOK_CLIENT_KEY).unwrap_or_default();

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

    fn validate_config(&self, id: &str, config: &KVS) -> Result<(), TargetError> {
        let get = |base_env_key: &str, config_key: &str| get_config_value(id, base_env_key, config_key, config);

        let enable = get(ENV_WEBHOOK_ENABLE, ENABLE_KEY)
            .map(|v| v.eq_ignore_ascii_case(ENABLE_ON) || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        if !enable {
            return Ok(());
        }

        let endpoint = get(ENV_WEBHOOK_ENDPOINT, WEBHOOK_ENDPOINT)
            .ok_or_else(|| TargetError::Configuration("Missing webhook endpoint".to_string()))?;
        debug!("endpoint: {}", endpoint);
        let parsed_endpoint = endpoint.trim();
        Url::parse(parsed_endpoint)
            .map_err(|e| TargetError::Configuration(format!("Invalid endpoint URL: {} (value: '{}')", e, parsed_endpoint)))?;

        let client_cert = get(ENV_WEBHOOK_CLIENT_CERT, WEBHOOK_CLIENT_CERT).unwrap_or_default();
        let client_key = get(ENV_WEBHOOK_CLIENT_KEY, WEBHOOK_CLIENT_KEY).unwrap_or_default();

        if client_cert.is_empty() != client_key.is_empty() {
            return Err(TargetError::Configuration(
                "Both client_cert and client_key must be specified together".to_string(),
            ));
        }

        let queue_dir = get(ENV_WEBHOOK_QUEUE_DIR, WEBHOOK_QUEUE_DIR)
            .and_then(|v| v.parse::<String>().ok())
            .unwrap_or(DEFAULT_DIR.to_string());
        if !queue_dir.is_empty() && !std::path::Path::new(&queue_dir).is_absolute() {
            return Err(TargetError::Configuration("Webhook queue directory must be an absolute path".to_string()));
        }

        Ok(())
    }
}

/// Factory for creating MQTT targets
pub struct MQTTTargetFactory;

#[async_trait]
impl TargetFactory for MQTTTargetFactory {
    async fn create_target(&self, id: String, config: &KVS) -> Result<Box<dyn Target + Send + Sync>, TargetError> {
        let get = |base_env_key: &str, config_key: &str| get_config_value(&id, base_env_key, config_key, config);

        let enable = get(ENV_MQTT_ENABLE, ENABLE_KEY)
            .map(|v| v.eq_ignore_ascii_case(ENABLE_ON) || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        if !enable {
            return Err(TargetError::Configuration("Target is disabled".to_string()));
        }

        let broker =
            get(ENV_MQTT_BROKER, MQTT_BROKER).ok_or_else(|| TargetError::Configuration("Missing MQTT broker".to_string()))?;
        let broker_url = Url::parse(&broker)
            .map_err(|e| TargetError::Configuration(format!("Invalid broker URL: {} (value: '{}')", e, broker)))?;

        let topic =
            get(ENV_MQTT_TOPIC, MQTT_TOPIC).ok_or_else(|| TargetError::Configuration("Missing MQTT topic".to_string()))?;

        let qos = get(ENV_MQTT_QOS, MQTT_QOS)
            .and_then(|v| v.parse::<u8>().ok())
            .map(|q| match q {
                0 => QoS::AtMostOnce,
                1 => QoS::AtLeastOnce,
                2 => QoS::ExactlyOnce,
                _ => QoS::AtLeastOnce,
            })
            .unwrap_or(QoS::AtLeastOnce);

        let username = get(ENV_MQTT_USERNAME, MQTT_USERNAME).unwrap_or_default();
        let password = get(ENV_MQTT_PASSWORD, MQTT_PASSWORD).unwrap_or_default();

        let reconnect_interval = get(ENV_MQTT_RECONNECT_INTERVAL, MQTT_RECONNECT_INTERVAL)
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(5));

        let keep_alive = get(ENV_MQTT_KEEP_ALIVE_INTERVAL, MQTT_KEEP_ALIVE_INTERVAL)
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(30));

        let queue_dir = get(ENV_MQTT_QUEUE_DIR, MQTT_QUEUE_DIR)
            .and_then(|v| v.parse::<String>().ok())
            .unwrap_or(DEFAULT_DIR.to_string());
        let queue_limit = get(ENV_MQTT_QUEUE_LIMIT, MQTT_QUEUE_LIMIT)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_LIMIT);

        let args = MQTTArgs {
            enable,
            broker: broker_url,
            topic,
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

    fn validate_config(&self, id: &str, config: &KVS) -> Result<(), TargetError> {
        let get = |base_env_key: &str, config_key: &str| get_config_value(id, base_env_key, config_key, config);

        let enable = get(ENV_MQTT_ENABLE, ENABLE_KEY)
            .map(|v| v.eq_ignore_ascii_case(ENABLE_ON) || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        if !enable {
            return Ok(());
        }

        let broker =
            get(ENV_MQTT_BROKER, MQTT_BROKER).ok_or_else(|| TargetError::Configuration("Missing MQTT broker".to_string()))?;
        let url = Url::parse(&broker)
            .map_err(|e| TargetError::Configuration(format!("Invalid broker URL: {} (value: '{}')", e, broker)))?;

        match url.scheme() {
            "tcp" | "ssl" | "ws" | "wss" | "mqtt" | "mqtts" => {}
            _ => {
                return Err(TargetError::Configuration("Unsupported broker URL scheme".to_string()));
            }
        }

        if get(ENV_MQTT_TOPIC, MQTT_TOPIC).is_none() {
            return Err(TargetError::Configuration("Missing MQTT topic".to_string()));
        }

        if let Some(qos_str) = get(ENV_MQTT_QOS, MQTT_QOS) {
            let qos = qos_str
                .parse::<u8>()
                .map_err(|_| TargetError::Configuration("Invalid QoS value".to_string()))?;
            if qos > 2 {
                return Err(TargetError::Configuration("QoS must be 0, 1, or 2".to_string()));
            }
        }

        let queue_dir = get(ENV_MQTT_QUEUE_DIR, MQTT_QUEUE_DIR)
            .and_then(|v| v.parse::<String>().ok())
            .unwrap_or(DEFAULT_DIR.to_string());
        if !queue_dir.is_empty() {
            if !std::path::Path::new(&queue_dir).is_absolute() {
                return Err(TargetError::Configuration("MQTT queue directory must be an absolute path".to_string()));
            }
            if let Some(qos_str) = get(ENV_MQTT_QOS, MQTT_QOS) {
                if qos_str == "0" {
                    warn!("Using queue_dir with QoS 0 may result in event loss");
                }
            }
        }

        Ok(())
    }
}
