use crate::config::adapter::AdapterConfig;
use crate::{Error, Event};
use async_trait::async_trait;
use std::sync::Arc;

#[cfg(all(feature = "kafka", target_os = "linux"))]
pub(crate) mod kafka;
#[cfg(feature = "mqtt")]
pub(crate) mod mqtt;
#[cfg(feature = "webhook")]
pub(crate) mod webhook;

#[allow(dead_code)]
const NOTIFY_KAFKA_SUB_SYS: &str = "notify_kafka";
#[allow(dead_code)]
const NOTIFY_MQTT_SUB_SYS: &str = "notify_mqtt";
#[allow(dead_code)]
const NOTIFY_MY_SQL_SUB_SYS: &str = "notify_mysql";
#[allow(dead_code)]
const NOTIFY_NATS_SUB_SYS: &str = "notify_nats";
#[allow(dead_code)]
const NOTIFY_NSQ_SUB_SYS: &str = "notify_nsq";
#[allow(dead_code)]
const NOTIFY_ES_SUB_SYS: &str = "notify_elasticsearch";
#[allow(dead_code)]
const NOTIFY_AMQP_SUB_SYS: &str = "notify_amqp";
#[allow(dead_code)]
const NOTIFY_POSTGRES_SUB_SYS: &str = "notify_postgres";
#[allow(dead_code)]
const NOTIFY_REDIS_SUB_SYS: &str = "notify_redis";
const NOTIFY_WEBHOOK_SUB_SYS: &str = "notify_webhook";

/// The `ChannelAdapterType` enum represents the different types of channel adapters.
///
/// It is used to identify the type of adapter being used in the system.
///
/// # Variants
///
/// - `Webhook`: Represents a webhook adapter.
/// - `Kafka`: Represents a Kafka adapter.
/// - `Mqtt`: Represents an MQTT adapter.
///
/// # Example
///
/// ```
/// use rustfs_notify::ChannelAdapterType;
///
/// let adapter_type = ChannelAdapterType::Webhook;
/// match adapter_type {
///    ChannelAdapterType::Webhook => println!("Using webhook adapter"),
///    ChannelAdapterType::Kafka => println!("Using Kafka adapter"),
///    ChannelAdapterType::Mqtt => println!("Using MQTT adapter"),
/// }
pub enum ChannelAdapterType {
    Webhook,
    Kafka,
    Mqtt,
}

impl ChannelAdapterType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChannelAdapterType::Webhook => "webhook",
            ChannelAdapterType::Kafka => "kafka",
            ChannelAdapterType::Mqtt => "mqtt",
        }
    }
}

impl std::fmt::Display for ChannelAdapterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelAdapterType::Webhook => write!(f, "webhook"),
            ChannelAdapterType::Kafka => write!(f, "kafka"),
            ChannelAdapterType::Mqtt => write!(f, "mqtt"),
        }
    }
}

/// The `ChannelAdapter` trait defines the interface for all channel adapters.
#[async_trait]
pub trait ChannelAdapter: Send + Sync + 'static {
    /// Sends an event to the channel.
    fn name(&self) -> String;
    /// Sends an event to the channel.
    async fn send(&self, event: &Event) -> Result<(), Error>;
}

/// Creates channel adapters based on the provided configuration.
pub fn create_adapters(configs: Vec<AdapterConfig>) -> Result<Vec<Arc<dyn ChannelAdapter>>, Error> {
    let mut adapters: Vec<Arc<dyn ChannelAdapter>> = Vec::new();

    for config in configs {
        match config {
            #[cfg(feature = "webhook")]
            AdapterConfig::Webhook(webhook_config) => {
                webhook_config.validate().map_err(Error::ConfigError)?;
                adapters.push(Arc::new(webhook::WebhookAdapter::new(webhook_config.clone())));
            }
            #[cfg(all(feature = "kafka", target_os = "linux"))]
            AdapterConfig::Kafka(kafka_config) => {
                adapters.push(Arc::new(kafka::KafkaAdapter::new(kafka_config)?));
            }
            #[cfg(feature = "mqtt")]
            AdapterConfig::Mqtt(mqtt_config) => {
                let (mqtt, mut event_loop) = mqtt::MqttAdapter::new(mqtt_config);
                tokio::spawn(async move { while event_loop.poll().await.is_ok() {} });
                adapters.push(Arc::new(mqtt));
            }
            #[cfg(not(feature = "webhook"))]
            AdapterConfig::Webhook(_) => return Err(Error::FeatureDisabled("webhook")),
            #[cfg(any(not(feature = "kafka"), not(target_os = "linux")))]
            AdapterConfig::Kafka(_) => return Err(Error::FeatureDisabled("kafka")),
            #[cfg(not(feature = "mqtt"))]
            AdapterConfig::Mqtt(_) => return Err(Error::FeatureDisabled("mqtt")),
        }
    }

    Ok(adapters)
}
