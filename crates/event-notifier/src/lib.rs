mod adapter;
mod bus;
mod config;
mod error;
mod event;
mod global;
mod notifier;
mod producer;
mod store;

pub use adapter::create_adapters;
#[cfg(feature = "kafka")]
pub use adapter::kafka::KafkaAdapter;
#[cfg(feature = "mqtt")]
pub use adapter::mqtt::MqttAdapter;
#[cfg(feature = "webhook")]
pub use adapter::webhook::WebhookAdapter;
pub use adapter::ChannelAdapter;
pub use bus::event_bus;
#[cfg(feature = "http-producer")]
pub use config::HttpProducerConfig;
#[cfg(feature = "kafka")]
pub use config::KafkaConfig;
#[cfg(feature = "mqtt")]
pub use config::MqttConfig;
#[cfg(feature = "webhook")]
pub use config::WebhookConfig;
pub use config::{AdapterConfig, NotificationConfig};
pub use error::Error;

pub use event::{Bucket, Event, EventBuilder, Identity, Log, Metadata, Name, Object, Source};
pub use global::{
    initialize, initialize_and_start, initialize_and_start_with_ready_check, initialize_safe, send_event, shutdown, start,
    start_safe,
};
pub use notifier::NotificationSystem;
pub use store::EventStore;
