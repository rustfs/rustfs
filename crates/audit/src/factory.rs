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

use crate::AuditEntry;
use rustfs_config::AUDIT_DEFAULT_DIR;
use rustfs_config::audit::{
    AUDIT_AMQP_KEYS, AUDIT_KAFKA_KEYS, AUDIT_MQTT_KEYS, AUDIT_MYSQL_KEYS, AUDIT_NATS_KEYS, AUDIT_POSTGRES_KEYS,
    AUDIT_PULSAR_KEYS, AUDIT_REDIS_DEFAULT_CHANNEL, AUDIT_REDIS_KEYS, AUDIT_WEBHOOK_KEYS,
};
use rustfs_targets::config::{
    build_amqp_args, build_kafka_args, build_mqtt_args, build_mysql_args, build_nats_args, build_postgres_args,
    build_pulsar_args, build_redis_args, build_webhook_args, validate_amqp_config, validate_kafka_config, validate_mqtt_config,
    validate_mysql_config, validate_nats_config, validate_postgres_config, validate_pulsar_config, validate_redis_config,
    validate_webhook_config,
};
use rustfs_targets::target::{ChannelTargetType, TargetType};
use rustfs_targets::{BuiltinTargetDescriptor, TargetPluginDescriptor, TargetRequestValidator, boxed_target};

pub fn builtin_target_descriptors() -> Vec<BuiltinTargetDescriptor<AuditEntry>> {
    vec![
        BuiltinTargetDescriptor::new(
            rustfs_config::audit::AUDIT_AMQP_SUB_SYS,
            TargetRequestValidator::Amqp(TargetType::AuditLog),
            TargetPluginDescriptor::new(
                ChannelTargetType::Amqp.as_str(),
                AUDIT_AMQP_KEYS,
                |config| validate_amqp_config(config, AUDIT_DEFAULT_DIR),
                |id, config| {
                    let args = build_amqp_args(config, AUDIT_DEFAULT_DIR, TargetType::AuditLog)?;
                    Ok(boxed_target(rustfs_targets::target::amqp::AMQPTarget::new(id, args)?))
                },
            ),
        ),
        BuiltinTargetDescriptor::new(
            rustfs_config::audit::AUDIT_WEBHOOK_SUB_SYS,
            TargetRequestValidator::Webhook,
            TargetPluginDescriptor::new(
                ChannelTargetType::Webhook.as_str(),
                AUDIT_WEBHOOK_KEYS,
                |config| validate_webhook_config(config, AUDIT_DEFAULT_DIR),
                |id, config| {
                    let args = build_webhook_args(config, AUDIT_DEFAULT_DIR, TargetType::AuditLog)?;
                    Ok(boxed_target(rustfs_targets::target::webhook::WebhookTarget::new(id, args)?))
                },
            ),
        ),
        BuiltinTargetDescriptor::new(
            rustfs_config::audit::AUDIT_MQTT_SUB_SYS,
            TargetRequestValidator::Mqtt,
            TargetPluginDescriptor::new(ChannelTargetType::Mqtt.as_str(), AUDIT_MQTT_KEYS, validate_mqtt_config, |id, config| {
                let args = build_mqtt_args(config, AUDIT_DEFAULT_DIR, TargetType::AuditLog)?;
                Ok(boxed_target(rustfs_targets::target::mqtt::MQTTTarget::new(id, args)?))
            }),
        ),
        BuiltinTargetDescriptor::new(
            rustfs_config::audit::AUDIT_NATS_SUB_SYS,
            TargetRequestValidator::Nats(TargetType::AuditLog),
            TargetPluginDescriptor::new(
                ChannelTargetType::Nats.as_str(),
                AUDIT_NATS_KEYS,
                |config| validate_nats_config(config, AUDIT_DEFAULT_DIR),
                |id, config| {
                    let args = build_nats_args(config, AUDIT_DEFAULT_DIR, TargetType::AuditLog)?;
                    Ok(boxed_target(rustfs_targets::target::nats::NATSTarget::new(id, args)?))
                },
            ),
        ),
        BuiltinTargetDescriptor::new(
            rustfs_config::audit::AUDIT_PULSAR_SUB_SYS,
            TargetRequestValidator::Pulsar(TargetType::AuditLog),
            TargetPluginDescriptor::new(
                ChannelTargetType::Pulsar.as_str(),
                AUDIT_PULSAR_KEYS,
                |config| validate_pulsar_config(config, AUDIT_DEFAULT_DIR),
                |id, config| {
                    let args = build_pulsar_args(config, AUDIT_DEFAULT_DIR, TargetType::AuditLog)?;
                    Ok(boxed_target(rustfs_targets::target::pulsar::PulsarTarget::new(id, args)?))
                },
            ),
        ),
        BuiltinTargetDescriptor::new(
            rustfs_config::audit::AUDIT_KAFKA_SUB_SYS,
            TargetRequestValidator::Kafka(TargetType::AuditLog),
            TargetPluginDescriptor::new(
                ChannelTargetType::Kafka.as_str(),
                AUDIT_KAFKA_KEYS,
                |config| validate_kafka_config(config, AUDIT_DEFAULT_DIR),
                |id, config| {
                    let args = build_kafka_args(config, AUDIT_DEFAULT_DIR, TargetType::AuditLog)?;
                    Ok(boxed_target(rustfs_targets::target::kafka::KafkaTarget::new(id, args)?))
                },
            ),
        ),
        BuiltinTargetDescriptor::new(
            rustfs_config::audit::AUDIT_REDIS_SUB_SYS,
            TargetRequestValidator::Redis {
                default_channel: AUDIT_REDIS_DEFAULT_CHANNEL,
                target_type: TargetType::AuditLog,
            },
            TargetPluginDescriptor::new(
                ChannelTargetType::Redis.as_str(),
                AUDIT_REDIS_KEYS,
                |config| validate_redis_config(config, AUDIT_DEFAULT_DIR, AUDIT_REDIS_DEFAULT_CHANNEL),
                |id, config| {
                    let args = build_redis_args(config, AUDIT_DEFAULT_DIR, AUDIT_REDIS_DEFAULT_CHANNEL, TargetType::AuditLog)?;
                    Ok(boxed_target(rustfs_targets::target::redis::RedisTarget::new(id, args)?))
                },
            ),
        ),
        BuiltinTargetDescriptor::new(
            rustfs_config::audit::AUDIT_MYSQL_SUB_SYS,
            TargetRequestValidator::MySql(TargetType::AuditLog),
            TargetPluginDescriptor::new(
                ChannelTargetType::MySql.as_str(),
                AUDIT_MYSQL_KEYS,
                |config| validate_mysql_config(config, AUDIT_DEFAULT_DIR),
                |id, config| {
                    let args = build_mysql_args(config, AUDIT_DEFAULT_DIR, TargetType::AuditLog)?;
                    Ok(boxed_target(rustfs_targets::target::mysql::MySqlTarget::new(id, args)?))
                },
            ),
        ),
        BuiltinTargetDescriptor::new(
            rustfs_config::audit::AUDIT_POSTGRES_SUB_SYS,
            TargetRequestValidator::Postgres(TargetType::AuditLog),
            TargetPluginDescriptor::new(
                ChannelTargetType::Postgres.as_str(),
                AUDIT_POSTGRES_KEYS,
                |config| validate_postgres_config(config, AUDIT_DEFAULT_DIR),
                |id, config| {
                    let args = build_postgres_args(config, AUDIT_DEFAULT_DIR, TargetType::AuditLog)?;
                    Ok(boxed_target(rustfs_targets::target::postgres::PostgresTarget::new(id, args)?))
                },
            ),
        ),
    ]
}

pub fn builtin_target_plugins() -> Vec<TargetPluginDescriptor<AuditEntry>> {
    builtin_target_descriptors()
        .into_iter()
        .map(|descriptor| descriptor.plugin().clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::builtin_target_descriptors;
    use rustfs_config::audit::AUDIT_AMQP_KEYS;
    use rustfs_config::{AMQP_EXCHANGE, AMQP_QUEUE_DIR, AMQP_ROUTING_KEY, AMQP_URL};
    use rustfs_ecstore::config::KVS;
    use rustfs_targets::target::ChannelTargetType;

    fn amqp_base_config() -> KVS {
        let mut config = KVS::new();
        config.insert(AMQP_URL.to_string(), "amqp://127.0.0.1:5672/%2f".to_string());
        config.insert(AMQP_EXCHANGE.to_string(), "rustfs.audit".to_string());
        config.insert(AMQP_ROUTING_KEY.to_string(), "audit".to_string());
        config.insert(AMQP_QUEUE_DIR.to_string(), String::new());
        config
    }

    #[test]
    fn builtin_plugins_include_amqp_descriptor() {
        let plugin = builtin_target_descriptors()
            .into_iter()
            .find(|plugin| plugin.plugin().target_type() == ChannelTargetType::Amqp.as_str())
            .expect("amqp plugin should exist");

        assert!(plugin.plugin().valid_fields().contains(&AMQP_URL));
        assert!(plugin.plugin().valid_fields().contains(&AMQP_EXCHANGE));
        assert!(plugin.plugin().valid_fields().contains(&AMQP_ROUTING_KEY));
        assert_eq!(plugin.plugin().valid_fields().len(), AUDIT_AMQP_KEYS.len());
    }

    #[test]
    fn builtin_plugins_create_audit_amqp_target() {
        let plugin = builtin_target_descriptors()
            .into_iter()
            .find(|plugin| plugin.plugin().target_type() == ChannelTargetType::Amqp.as_str())
            .expect("amqp plugin should exist");

        let target = plugin
            .plugin()
            .create_target("primary".to_string(), &amqp_base_config())
            .expect("AMQP audit target should be created");

        let target_id = target.id();
        assert_eq!(target_id.id, "primary");
        assert_eq!(target_id.name, "amqp");
        assert!(target.store().is_none());
    }
}
