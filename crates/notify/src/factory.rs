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
<<<<<<< HEAD
    NOTIFY_KAFKA_KEYS, NOTIFY_MQTT_KEYS, NOTIFY_MYSQL_KEYS, NOTIFY_NATS_KEYS, NOTIFY_POSTGRES_KEYS, NOTIFY_PULSAR_KEYS,
    NOTIFY_REDIS_DEFAULT_CHANNEL, NOTIFY_REDIS_KEYS, NOTIFY_WEBHOOK_KEYS,
=======
    NOTIFY_AMQP_KEYS, NOTIFY_KAFKA_KEYS, NOTIFY_MQTT_KEYS, NOTIFY_NATS_KEYS, NOTIFY_PULSAR_KEYS, NOTIFY_WEBHOOK_KEYS,
>>>>>>> 56f1dc85 (feat(targets): implement AMQP notification target)
};
use rustfs_ecstore::config::KVS;
use rustfs_targets::{
    Target,
    config::{
<<<<<<< HEAD
        build_kafka_args, build_mqtt_args, build_mysql_args, build_nats_args, build_postgres_args, build_pulsar_args,
        build_redis_args, build_webhook_args, validate_kafka_config, validate_mqtt_config, validate_mysql_config,
        validate_nats_config, validate_postgres_config, validate_pulsar_config, validate_redis_config, validate_webhook_config,
=======
        build_amqp_args, build_kafka_args, build_mqtt_args, build_nats_args, build_pulsar_args, build_webhook_args,
        validate_amqp_config, validate_kafka_config, validate_mqtt_config, validate_nats_config, validate_pulsar_config,
        validate_webhook_config,
>>>>>>> 56f1dc85 (feat(targets): implement AMQP notification target)
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

pub struct AMQPTargetFactory;

#[async_trait]
impl TargetFactory for AMQPTargetFactory {
    async fn create_target(&self, id: String, config: &KVS) -> Result<Box<dyn Target<Event> + Send + Sync>, TargetError> {
        let args = build_amqp_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
        let target = rustfs_targets::target::amqp::AMQPTarget::new(id, args)?;
        Ok(Box::new(target))
    }

    fn validate_config(&self, _id: &str, config: &KVS) -> Result<(), TargetError> {
        validate_amqp_config(config, EVENT_DEFAULT_DIR)
    }

    fn get_valid_fields(&self) -> HashSet<String> {
        NOTIFY_AMQP_KEYS.iter().map(|s| s.to_string()).collect()
    }
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

pub struct PostgresTargetFactory;

#[async_trait]
impl TargetFactory for PostgresTargetFactory {
    async fn create_target(&self, id: String, config: &KVS) -> Result<Box<dyn Target<Event> + Send + Sync>, TargetError> {
        let args = build_postgres_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
        let target = rustfs_targets::target::postgres::PostgresTarget::new(id, args)?;
        Ok(Box::new(target))
    }

    fn validate_config(&self, _id: &str, config: &KVS) -> Result<(), TargetError> {
        validate_postgres_config(config, EVENT_DEFAULT_DIR)
    }

    fn get_valid_fields(&self) -> HashSet<String> {
        NOTIFY_POSTGRES_KEYS.iter().map(|s| s.to_string()).collect()
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

<<<<<<< HEAD
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

pub struct RedisTargetFactory;

#[async_trait]
impl TargetFactory for RedisTargetFactory {
    async fn create_target(&self, id: String, config: &KVS) -> Result<Box<dyn Target<Event> + Send + Sync>, TargetError> {
        let args = build_redis_args(config, EVENT_DEFAULT_DIR, NOTIFY_REDIS_DEFAULT_CHANNEL, TargetType::NotifyEvent)?;
        let target = rustfs_targets::target::redis::RedisTarget::new(id, args)?;
        Ok(Box::new(target))
    }

    fn validate_config(&self, _id: &str, config: &KVS) -> Result<(), TargetError> {
        validate_redis_config(config, EVENT_DEFAULT_DIR, NOTIFY_REDIS_DEFAULT_CHANNEL)
    }

    fn get_valid_fields(&self) -> HashSet<String> {
        NOTIFY_REDIS_KEYS.iter().map(|s| s.to_string()).collect()
=======
#[cfg(test)]
mod tests {
    use super::{AMQPTargetFactory, TargetFactory};
    use rustfs_config::notify::NOTIFY_AMQP_KEYS;
    use rustfs_config::{AMQP_EXCHANGE, AMQP_QUEUE_DIR, AMQP_ROUTING_KEY, AMQP_URL};
    use rustfs_ecstore::config::KVS;

    fn amqp_base_config() -> KVS {
        let mut config = KVS::new();
        config.insert(AMQP_URL.to_string(), "amqp://127.0.0.1:5672/%2f".to_string());
        config.insert(AMQP_EXCHANGE.to_string(), "rustfs.events".to_string());
        config.insert(AMQP_ROUTING_KEY.to_string(), "objects".to_string());
        config.insert(AMQP_QUEUE_DIR.to_string(), String::new());
        config
    }

    #[test]
    fn amqp_factory_valid_fields_include_amqp_keys() {
        let fields = AMQPTargetFactory.get_valid_fields();

        assert!(fields.contains(AMQP_URL));
        assert!(fields.contains(AMQP_EXCHANGE));
        assert!(fields.contains(AMQP_ROUTING_KEY));
        assert_eq!(fields.len(), NOTIFY_AMQP_KEYS.len());
    }

    #[tokio::test]
    async fn amqp_factory_creates_target() {
        let target = AMQPTargetFactory
            .create_target("primary".to_string(), &amqp_base_config())
            .await
            .expect("AMQP target should be created");

        let target_id = target.id();
        assert_eq!(target_id.id, "primary");
        assert_eq!(target_id.name, "amqp");
        assert!(target.store().is_none());
>>>>>>> 56f1dc85 (feat(targets): implement AMQP notification target)
    }
}
