mod adapter;
mod bus;
mod config;
mod error;
mod event;
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
pub use bus::event_bus;
pub use config::AdapterCommon;
pub use config::EventNotifierConfig;
#[cfg(all(feature = "kafka", target_os = "linux"))]
pub use config::KafkaConfig;
#[cfg(feature = "mqtt")]
pub use config::MqttConfig;
#[cfg(feature = "webhook")]
pub use config::WebhookConfig;
pub use config::{AdapterConfig, NotifierConfig, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_INTERVAL};
pub use error::Error;

pub use event::{Bucket, Event, EventBuilder, Identity, Log, Metadata, Name, Object, Source};
pub use global::{initialize, is_initialized, is_ready, send_event, shutdown};
pub use notifier::NotifierSystem;
pub use store::event::EventStore;
pub use store::queue::QueueStore;
