mod adapter;
mod config;
mod error;
mod event;
mod event_notifier;
mod event_system;
mod global;
mod notifier;
mod store;

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
#[cfg(all(feature = "kafka", target_os = "linux"))]
pub use config::kafka::KafkaConfig;
#[cfg(feature = "mqtt")]
pub use config::mqtt::MqttConfig;
pub use config::notifier::EventNotifierConfig;
#[cfg(feature = "webhook")]
pub use config::webhook::WebhookConfig;
pub use config::{DEFAULT_MAX_RETRIES, DEFAULT_RETRY_INTERVAL};
pub use error::Error;

pub use event::{Bucket, Event, EventBuilder, Identity, Log, Metadata, Name, Object, Source};
pub use global::{initialize, is_initialized, is_ready, send_event, shutdown};
pub use notifier::NotifierSystem;
pub use store::queue::QueueStore;
