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
use rustfs_config::EVENT_DEFAULT_DIR;
use rustfs_config::notify::{
    NOTIFY_AMQP_KEYS, NOTIFY_KAFKA_KEYS, NOTIFY_MQTT_KEYS, NOTIFY_MYSQL_KEYS, NOTIFY_NATS_KEYS, NOTIFY_POSTGRES_KEYS,
    NOTIFY_PULSAR_KEYS, NOTIFY_REDIS_DEFAULT_CHANNEL, NOTIFY_REDIS_KEYS, NOTIFY_WEBHOOK_KEYS,
};
use rustfs_targets::config::{
    build_amqp_args, build_kafka_args, build_mqtt_args, build_mysql_args, build_nats_args, build_postgres_args,
    build_pulsar_args, build_redis_args, build_webhook_args, validate_amqp_config, validate_kafka_config, validate_mqtt_config,
    validate_mysql_config, validate_nats_config, validate_postgres_config, validate_pulsar_config, validate_redis_config,
    validate_webhook_config,
};
use rustfs_targets::target::{ChannelTargetType, TargetType};
use rustfs_targets::{TargetPluginDescriptor, boxed_target};

pub fn builtin_target_plugins() -> Vec<TargetPluginDescriptor<Event>> {
    vec![
        TargetPluginDescriptor::new(
            ChannelTargetType::Amqp.as_str(),
            NOTIFY_AMQP_KEYS,
            |config| validate_amqp_config(config, EVENT_DEFAULT_DIR),
            |id, config| {
                let args = build_amqp_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
                Ok(boxed_target(rustfs_targets::target::amqp::AMQPTarget::new(id, args)?))
            },
        ),
        TargetPluginDescriptor::new(
            ChannelTargetType::Webhook.as_str(),
            NOTIFY_WEBHOOK_KEYS,
            |config| validate_webhook_config(config, EVENT_DEFAULT_DIR),
            |id, config| {
                let args = build_webhook_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
                Ok(boxed_target(rustfs_targets::target::webhook::WebhookTarget::new(id, args)?))
            },
        ),
        TargetPluginDescriptor::new(ChannelTargetType::Mqtt.as_str(), NOTIFY_MQTT_KEYS, validate_mqtt_config, |id, config| {
            let args = build_mqtt_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
            Ok(boxed_target(rustfs_targets::target::mqtt::MQTTTarget::new(id, args)?))
        }),
        TargetPluginDescriptor::new(
            ChannelTargetType::Nats.as_str(),
            NOTIFY_NATS_KEYS,
            |config| validate_nats_config(config, EVENT_DEFAULT_DIR),
            |id, config| {
                let args = build_nats_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
                Ok(boxed_target(rustfs_targets::target::nats::NATSTarget::new(id, args)?))
            },
        ),
        TargetPluginDescriptor::new(
            ChannelTargetType::Postgres.as_str(),
            NOTIFY_POSTGRES_KEYS,
            |config| validate_postgres_config(config, EVENT_DEFAULT_DIR),
            |id, config| {
                let args = build_postgres_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
                Ok(boxed_target(rustfs_targets::target::postgres::PostgresTarget::new(id, args)?))
            },
        ),
        TargetPluginDescriptor::new(
            ChannelTargetType::Pulsar.as_str(),
            NOTIFY_PULSAR_KEYS,
            |config| validate_pulsar_config(config, EVENT_DEFAULT_DIR),
            |id, config| {
                let args = build_pulsar_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
                Ok(boxed_target(rustfs_targets::target::pulsar::PulsarTarget::new(id, args)?))
            },
        ),
        TargetPluginDescriptor::new(
            ChannelTargetType::Kafka.as_str(),
            NOTIFY_KAFKA_KEYS,
            |config| validate_kafka_config(config, EVENT_DEFAULT_DIR),
            |id, config| {
                let args = build_kafka_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
                Ok(boxed_target(rustfs_targets::target::kafka::KafkaTarget::new(id, args)?))
            },
        ),
        TargetPluginDescriptor::new(
            ChannelTargetType::MySql.as_str(),
            NOTIFY_MYSQL_KEYS,
            |config| validate_mysql_config(config, EVENT_DEFAULT_DIR),
            |id, config| {
                let args = build_mysql_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
                Ok(boxed_target(rustfs_targets::target::mysql::MySqlTarget::new(id, args)?))
            },
        ),
        TargetPluginDescriptor::new(
            ChannelTargetType::Redis.as_str(),
            NOTIFY_REDIS_KEYS,
            |config| validate_redis_config(config, EVENT_DEFAULT_DIR, NOTIFY_REDIS_DEFAULT_CHANNEL),
            |id, config| {
                let args = build_redis_args(config, EVENT_DEFAULT_DIR, NOTIFY_REDIS_DEFAULT_CHANNEL, TargetType::NotifyEvent)?;
                Ok(boxed_target(rustfs_targets::target::redis::RedisTarget::new(id, args)?))
            },
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::builtin_target_plugins;
    use rustfs_config::notify::NOTIFY_AMQP_KEYS;
    use rustfs_config::{AMQP_EXCHANGE, AMQP_QUEUE_DIR, AMQP_ROUTING_KEY, AMQP_URL};
    use rustfs_ecstore::config::KVS;
    use rustfs_targets::target::ChannelTargetType;

    fn amqp_base_config() -> KVS {
        let mut config = KVS::new();
        config.insert(AMQP_URL.to_string(), "amqp://127.0.0.1:5672/%2f".to_string());
        config.insert(AMQP_EXCHANGE.to_string(), "rustfs.events".to_string());
        config.insert(AMQP_ROUTING_KEY.to_string(), "objects".to_string());
        config.insert(AMQP_QUEUE_DIR.to_string(), String::new());
        config
    }

    #[test]
    fn builtin_plugins_include_amqp_descriptor() {
        let plugin = builtin_target_plugins()
            .into_iter()
            .find(|plugin| plugin.target_type() == ChannelTargetType::Amqp.as_str())
            .expect("amqp plugin should exist");

        assert!(plugin.valid_fields().contains(&AMQP_URL));
        assert!(plugin.valid_fields().contains(&AMQP_EXCHANGE));
        assert!(plugin.valid_fields().contains(&AMQP_ROUTING_KEY));
        assert_eq!(plugin.valid_fields().len(), NOTIFY_AMQP_KEYS.len());
    }

    #[test]
    fn builtin_plugins_create_notify_amqp_target() {
        let plugin = builtin_target_plugins()
            .into_iter()
            .find(|plugin| plugin.target_type() == ChannelTargetType::Amqp.as_str())
            .expect("amqp plugin should exist");

        let target = plugin
            .create_target("primary".to_string(), &amqp_base_config())
            .expect("AMQP target should be created");

        let target_id = target.id();
        assert_eq!(target_id.id, "primary");
        assert_eq!(target_id.name, "amqp");
        assert!(target.store().is_none());
    }
}
