use crate::store::DEFAULT_LIMIT;
use crate::{
    error::TargetError,
    target::{Target, mqtt::MQTTArgs, webhook::WebhookArgs},
};
use async_trait::async_trait;
use ecstore::config::{ENABLE_KEY, ENABLE_ON, KVS};
use rumqttc::QoS;
use std::time::Duration;
use tracing::warn;
use url::Url;

// --- Configuration Constants ---

// General

pub const DEFAULT_TARGET: &str = "1";

#[allow(dead_code)]
pub const NOTIFY_KAFKA_SUB_SYS: &str = "notify_kafka";
#[allow(dead_code)]
pub const NOTIFY_MQTT_SUB_SYS: &str = "notify_mqtt";
#[allow(dead_code)]
pub const NOTIFY_MY_SQL_SUB_SYS: &str = "notify_mysql";
#[allow(dead_code)]
pub const NOTIFY_NATS_SUB_SYS: &str = "notify_nats";
#[allow(dead_code)]
pub const NOTIFY_NSQ_SUB_SYS: &str = "notify_nsq";
#[allow(dead_code)]
pub const NOTIFY_ES_SUB_SYS: &str = "notify_elasticsearch";
#[allow(dead_code)]
pub const NOTIFY_AMQP_SUB_SYS: &str = "notify_amqp";
#[allow(dead_code)]
pub const NOTIFY_POSTGRES_SUB_SYS: &str = "notify_postgres";
#[allow(dead_code)]
pub const NOTIFY_REDIS_SUB_SYS: &str = "notify_redis";
pub const NOTIFY_WEBHOOK_SUB_SYS: &str = "notify_webhook";

#[allow(dead_code)]
pub const NOTIFY_SUB_SYSTEMS: &[&str] = &[NOTIFY_MQTT_SUB_SYS, NOTIFY_WEBHOOK_SUB_SYS];

// Webhook Keys
pub const WEBHOOK_ENDPOINT: &str = "endpoint";
pub const WEBHOOK_AUTH_TOKEN: &str = "auth_token";
pub const WEBHOOK_QUEUE_LIMIT: &str = "queue_limit";
pub const WEBHOOK_QUEUE_DIR: &str = "queue_dir";
pub const WEBHOOK_CLIENT_CERT: &str = "client_cert";
pub const WEBHOOK_CLIENT_KEY: &str = "client_key";

// Webhook Environment Variables
const ENV_WEBHOOK_ENABLE: &str = "RUSTFS_NOTIFY_WEBHOOK_ENABLE";
const ENV_WEBHOOK_ENDPOINT: &str = "RUSTFS_NOTIFY_WEBHOOK_ENDPOINT";
const ENV_WEBHOOK_AUTH_TOKEN: &str = "RUSTFS_NOTIFY_WEBHOOK_AUTH_TOKEN";
const ENV_WEBHOOK_QUEUE_LIMIT: &str = "RUSTFS_NOTIFY_WEBHOOK_QUEUE_LIMIT";
const ENV_WEBHOOK_QUEUE_DIR: &str = "RUSTFS_NOTIFY_WEBHOOK_QUEUE_DIR";
const ENV_WEBHOOK_CLIENT_CERT: &str = "RUSTFS_NOTIFY_WEBHOOK_CLIENT_CERT";
const ENV_WEBHOOK_CLIENT_KEY: &str = "RUSTFS_NOTIFY_WEBHOOK_CLIENT_KEY";

// MQTT Keys
pub const MQTT_BROKER: &str = "broker";
pub const MQTT_TOPIC: &str = "topic";
pub const MQTT_QOS: &str = "qos";
pub const MQTT_USERNAME: &str = "username";
pub const MQTT_PASSWORD: &str = "password";
pub const MQTT_RECONNECT_INTERVAL: &str = "reconnect_interval";
pub const MQTT_KEEP_ALIVE_INTERVAL: &str = "keep_alive_interval";
pub const MQTT_QUEUE_DIR: &str = "queue_dir";
pub const MQTT_QUEUE_LIMIT: &str = "queue_limit";

// MQTT Environment Variables
const ENV_MQTT_ENABLE: &str = "RUSTFS_NOTIFY_MQTT_ENABLE";
const ENV_MQTT_BROKER: &str = "RUSTFS_NOTIFY_MQTT_BROKER";
const ENV_MQTT_TOPIC: &str = "RUSTFS_NOTIFY_MQTT_TOPIC";
const ENV_MQTT_QOS: &str = "RUSTFS_NOTIFY_MQTT_QOS";
const ENV_MQTT_USERNAME: &str = "RUSTFS_NOTIFY_MQTT_USERNAME";
const ENV_MQTT_PASSWORD: &str = "RUSTFS_NOTIFY_MQTT_PASSWORD";
const ENV_MQTT_RECONNECT_INTERVAL: &str = "RUSTFS_NOTIFY_MQTT_RECONNECT_INTERVAL";
const ENV_MQTT_KEEP_ALIVE_INTERVAL: &str = "RUSTFS_NOTIFY_MQTT_KEEP_ALIVE_INTERVAL";
const ENV_MQTT_QUEUE_DIR: &str = "RUSTFS_NOTIFY_MQTT_QUEUE_DIR";
const ENV_MQTT_QUEUE_LIMIT: &str = "RUSTFS_NOTIFY_MQTT_QUEUE_LIMIT";

/// Helper function to get values from environment variables or KVS configurations.
///
/// It will give priority to reading from environment variables such as `BASE_ENV_KEY_ID` and fall back to the KVS configuration if it fails.
fn get_config_value(id: &str, base_env_key: &str, config_key: &str, config: &KVS) -> Option<String> {
    let env_key = if id != DEFAULT_TARGET {
        format!("{}_{}", base_env_key, id.to_uppercase().replace('-', "_"))
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
        let endpoint_url =
            Url::parse(&endpoint).map_err(|e| TargetError::Configuration(format!("Invalid endpoint URL: {}", e)))?;

        let auth_token = get(ENV_WEBHOOK_AUTH_TOKEN, WEBHOOK_AUTH_TOKEN).unwrap_or_default();
        let queue_dir = get(ENV_WEBHOOK_QUEUE_DIR, WEBHOOK_QUEUE_DIR).unwrap_or_default();

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
        Url::parse(&endpoint).map_err(|e| TargetError::Configuration(format!("Invalid endpoint URL: {}", e)))?;

        let client_cert = get(ENV_WEBHOOK_CLIENT_CERT, WEBHOOK_CLIENT_CERT).unwrap_or_default();
        let client_key = get(ENV_WEBHOOK_CLIENT_KEY, WEBHOOK_CLIENT_KEY).unwrap_or_default();

        if client_cert.is_empty() != client_key.is_empty() {
            return Err(TargetError::Configuration(
                "Both client_cert and client_key must be specified together".to_string(),
            ));
        }

        let queue_dir = get(ENV_WEBHOOK_QUEUE_DIR, WEBHOOK_QUEUE_DIR).unwrap_or_default();
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
        let broker_url = Url::parse(&broker).map_err(|e| TargetError::Configuration(format!("Invalid broker URL: {}", e)))?;

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

        let queue_dir = get(ENV_MQTT_QUEUE_DIR, MQTT_QUEUE_DIR).unwrap_or_default();
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
        let url = Url::parse(&broker).map_err(|e| TargetError::Configuration(format!("Invalid broker URL: {}", e)))?;

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

        let queue_dir = get(ENV_MQTT_QUEUE_DIR, MQTT_QUEUE_DIR).unwrap_or_default();
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
