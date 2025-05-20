mod adapter;
mod bus;
mod config;
mod error;
mod event;
mod event_sys;
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
pub use bus::event_bus;
#[cfg(all(feature = "kafka", target_os = "linux"))]
pub use config::KafkaConfig;
#[cfg(feature = "mqtt")]
pub use config::MqttConfig;
#[cfg(feature = "webhook")]
pub use config::WebhookConfig;
pub use config::{AdapterConfig, NotifierConfig};
pub use error::Error;

pub use event::{Bucket, Event, EventBuilder, Identity, Log, Metadata, Name, Object, Source};
pub use global::{initialize, is_initialized, is_ready, send_event, shutdown};
pub use notifier::NotifierSystem;
pub use store::EventStore;

pub use event_sys::delete_config;
pub use event_sys::get_event_notifier_config;
pub use event_sys::read_config;
pub use event_sys::save_config;

pub use event_sys::EventSys;
pub use event_sys::GLOBAL_EventSys;
