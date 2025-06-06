mod adapter;
mod config;
mod error;
mod event;
mod notifier;
mod store;
mod system;

pub use adapter::create_adapters;
#[cfg(all(feature = "kafka", target_os = "linux"))]
pub use adapter::kafka::KafkaAdapter;
#[cfg(feature = "mqtt")]
pub use adapter::mqtt::MqttAdapter;
#[cfg(feature = "webhook")]
pub use adapter::webhook::WebhookAdapter;

pub use adapter::ChannelAdapter;
pub use adapter::ChannelAdapterType;
pub use config::adapter::AdapterCommon;
pub use config::adapter::AdapterConfig;
pub use config::notifier::EventNotifierConfig;
pub use config::{DEFAULT_MAX_RETRIES, DEFAULT_RETRY_INTERVAL};
pub use error::Error;
pub use event::{Bucket, Event, EventBuilder, Identity, Log, Metadata, Name, Object, Source};
pub use store::queue::QueueStore;

#[cfg(all(feature = "kafka", target_os = "linux"))]
pub use config::kafka::KafkaConfig;
#[cfg(feature = "mqtt")]
pub use config::mqtt::MqttConfig;

#[cfg(feature = "webhook")]
pub use config::webhook::WebhookConfig;
