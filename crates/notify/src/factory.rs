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

use crate::Event;
use async_trait::async_trait;
use rustfs_config::EVENT_DEFAULT_DIR;
use rustfs_config::notify::{
    NOTIFY_KAFKA_KEYS, NOTIFY_MQTT_KEYS, NOTIFY_MYSQL_KEYS, NOTIFY_NATS_KEYS, NOTIFY_PULSAR_KEYS, NOTIFY_WEBHOOK_KEYS,
};
use rustfs_ecstore::config::KVS;
use rustfs_targets::{
    Target,
    config::{
        build_kafka_args, build_mqtt_args, build_mysql_args, build_nats_args, build_pulsar_args, build_webhook_args,
        validate_kafka_config, validate_mqtt_config, validate_mysql_config, validate_nats_config, validate_pulsar_config,
        validate_webhook_config,
    },
    error::TargetError,
    target::TargetType,
};
use std::collections::HashSet;

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
}

/// Factory for creating Webhook targets
pub struct WebhookTargetFactory;

#[async_trait]
impl TargetFactory for WebhookTargetFactory {
    async fn create_target(&self, id: String, config: &KVS) -> Result<Box<dyn Target<Event> + Send + Sync>, TargetError> {
        let args = build_webhook_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
        let target = rustfs_targets::target::webhook::WebhookTarget::new(id, args)?;
        Ok(Box::new(target))
    }

    fn validate_config(&self, _id: &str, config: &KVS) -> Result<(), TargetError> {
        validate_webhook_config(config, EVENT_DEFAULT_DIR)
    }

    fn get_valid_fields(&self) -> HashSet<String> {
        NOTIFY_WEBHOOK_KEYS.iter().map(|s| s.to_string()).collect()
    }
}

/// Factory for creating MQTT targets
pub struct MQTTTargetFactory;

#[async_trait]
impl TargetFactory for MQTTTargetFactory {
    async fn create_target(&self, id: String, config: &KVS) -> Result<Box<dyn Target<Event> + Send + Sync>, TargetError> {
        let args = build_mqtt_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
        let target = rustfs_targets::target::mqtt::MQTTTarget::new(id, args)?;
        Ok(Box::new(target))
    }

    fn validate_config(&self, _id: &str, config: &KVS) -> Result<(), TargetError> {
        validate_mqtt_config(config)
    }

    fn get_valid_fields(&self) -> HashSet<String> {
        NOTIFY_MQTT_KEYS.iter().map(|s| s.to_string()).collect()
    }
}

pub struct NATSTargetFactory;

#[async_trait]
impl TargetFactory for NATSTargetFactory {
    async fn create_target(&self, id: String, config: &KVS) -> Result<Box<dyn Target<Event> + Send + Sync>, TargetError> {
        let args = build_nats_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
        let target = rustfs_targets::target::nats::NATSTarget::new(id, args)?;
        Ok(Box::new(target))
    }

    fn validate_config(&self, _id: &str, config: &KVS) -> Result<(), TargetError> {
        validate_nats_config(config, EVENT_DEFAULT_DIR)
    }

    fn get_valid_fields(&self) -> HashSet<String> {
        NOTIFY_NATS_KEYS.iter().map(|s| s.to_string()).collect()
    }
}

pub struct PulsarTargetFactory;

#[async_trait]
impl TargetFactory for PulsarTargetFactory {
    async fn create_target(&self, id: String, config: &KVS) -> Result<Box<dyn Target<Event> + Send + Sync>, TargetError> {
        let args = build_pulsar_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
        let target = rustfs_targets::target::pulsar::PulsarTarget::new(id, args)?;
        Ok(Box::new(target))
    }

    fn validate_config(&self, _id: &str, config: &KVS) -> Result<(), TargetError> {
        validate_pulsar_config(config, EVENT_DEFAULT_DIR)
    }

    fn get_valid_fields(&self) -> HashSet<String> {
        NOTIFY_PULSAR_KEYS.iter().map(|s| s.to_string()).collect()
    }
}

pub struct KafkaTargetFactory;

#[async_trait]
impl TargetFactory for KafkaTargetFactory {
    async fn create_target(&self, id: String, config: &KVS) -> Result<Box<dyn Target<Event> + Send + Sync>, TargetError> {
        let args = build_kafka_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
        let target = rustfs_targets::target::kafka::KafkaTarget::new(id, args)?;
        Ok(Box::new(target))
    }

    fn validate_config(&self, _id: &str, config: &KVS) -> Result<(), TargetError> {
        validate_kafka_config(config, EVENT_DEFAULT_DIR)
    }

    fn get_valid_fields(&self) -> HashSet<String> {
        NOTIFY_KAFKA_KEYS.iter().map(|s| s.to_string()).collect()
    }
}

pub struct MySqlTargetFactory;

#[async_trait]
impl TargetFactory for MySqlTargetFactory {
    async fn create_target(&self, id: String, config: &KVS) -> Result<Box<dyn Target<Event> + Send + Sync>, TargetError> {
        let args = build_mysql_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
        let target = rustfs_targets::target::mysql::MySqlTarget::new(id, args)?;
        Ok(Box::new(target))
    }

    fn validate_config(&self, _id: &str, config: &KVS) -> Result<(), TargetError> {
        validate_mysql_config(config, EVENT_DEFAULT_DIR)
    }

    fn get_valid_fields(&self) -> HashSet<String> {
        NOTIFY_MYSQL_KEYS.iter().map(|s| s.to_string()).collect()
    }
}
