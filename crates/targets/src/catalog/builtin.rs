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

use crate::plugin::{
    BuiltinTargetAdminDescriptor, BuiltinTargetDescriptor, TargetAdminMetadata, TargetPluginDescriptor, TargetRequestValidator,
    boxed_target,
};
use crate::target::{ChannelTargetType, TargetType};
use crate::{Target, TargetError};
use rustfs_config::audit::{
    AUDIT_AMQP_KEYS, AUDIT_KAFKA_KEYS, AUDIT_MQTT_KEYS, AUDIT_MYSQL_KEYS, AUDIT_NATS_KEYS, AUDIT_POSTGRES_KEYS,
    AUDIT_PULSAR_KEYS, AUDIT_REDIS_DEFAULT_CHANNEL, AUDIT_REDIS_KEYS, AUDIT_WEBHOOK_KEYS,
};
use rustfs_config::notify::{
    NOTIFY_AMQP_KEYS, NOTIFY_AMQP_SUB_SYS, NOTIFY_KAFKA_KEYS, NOTIFY_KAFKA_SUB_SYS, NOTIFY_MQTT_KEYS, NOTIFY_MQTT_SUB_SYS,
    NOTIFY_MYSQL_KEYS, NOTIFY_MYSQL_SUB_SYS, NOTIFY_NATS_KEYS, NOTIFY_NATS_SUB_SYS, NOTIFY_POSTGRES_KEYS,
    NOTIFY_POSTGRES_SUB_SYS, NOTIFY_PULSAR_KEYS, NOTIFY_PULSAR_SUB_SYS, NOTIFY_REDIS_DEFAULT_CHANNEL, NOTIFY_REDIS_KEYS,
    NOTIFY_REDIS_SUB_SYS, NOTIFY_WEBHOOK_KEYS, NOTIFY_WEBHOOK_SUB_SYS,
};
use rustfs_config::{
    AUDIT_DEFAULT_DIR, EVENT_DEFAULT_DIR,
    audit::{
        AUDIT_AMQP_SUB_SYS, AUDIT_KAFKA_SUB_SYS, AUDIT_MQTT_SUB_SYS, AUDIT_MYSQL_SUB_SYS, AUDIT_NATS_SUB_SYS,
        AUDIT_POSTGRES_SUB_SYS, AUDIT_PULSAR_SUB_SYS, AUDIT_REDIS_SUB_SYS, AUDIT_WEBHOOK_SUB_SYS,
    },
};
use rustfs_ecstore::config::KVS;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::config::{
    build_amqp_args, build_kafka_args, build_mqtt_args, build_mysql_args, build_nats_args, build_postgres_args,
    build_pulsar_args, build_redis_args, build_webhook_args, validate_amqp_config, validate_kafka_config, validate_mqtt_config,
    validate_mysql_config, validate_nats_config, validate_postgres_config, validate_pulsar_config, validate_redis_config,
    validate_webhook_config,
};

type BoxedTarget<E> = Box<dyn Target<E> + Send + Sync>;

fn build_descriptor<E, Create, Validate>(
    subsystem: &'static str,
    request_validator: TargetRequestValidator,
    target_type: &'static str,
    valid_fields: &'static [&'static str],
    validate_config: Validate,
    create_target: Create,
) -> BuiltinTargetDescriptor<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
    Create: Fn(String, &KVS) -> Result<BoxedTarget<E>, TargetError> + Send + Sync + 'static,
    Validate: Fn(&KVS) -> Result<(), TargetError> + Send + Sync + 'static,
{
    BuiltinTargetDescriptor::new(
        subsystem,
        request_validator,
        TargetPluginDescriptor::new(target_type, valid_fields, validate_config, create_target),
    )
}

fn build_admin_descriptor(
    subsystem: &'static str,
    request_validator: TargetRequestValidator,
    target_type: &'static str,
    valid_fields: &'static [&'static str],
) -> BuiltinTargetAdminDescriptor {
    BuiltinTargetAdminDescriptor::new(
        crate::manifest::builtin_target_manifest(target_type),
        valid_fields,
        TargetAdminMetadata::new(subsystem, request_validator),
    )
}

pub fn builtin_audit_target_admin_descriptors() -> Vec<BuiltinTargetAdminDescriptor> {
    vec![
        build_admin_descriptor(
            AUDIT_AMQP_SUB_SYS,
            TargetRequestValidator::Amqp(TargetType::AuditLog),
            ChannelTargetType::Amqp.as_str(),
            AUDIT_AMQP_KEYS,
        ),
        build_admin_descriptor(
            AUDIT_WEBHOOK_SUB_SYS,
            TargetRequestValidator::Webhook,
            ChannelTargetType::Webhook.as_str(),
            AUDIT_WEBHOOK_KEYS,
        ),
        build_admin_descriptor(
            AUDIT_MQTT_SUB_SYS,
            TargetRequestValidator::Mqtt,
            ChannelTargetType::Mqtt.as_str(),
            AUDIT_MQTT_KEYS,
        ),
        build_admin_descriptor(
            AUDIT_NATS_SUB_SYS,
            TargetRequestValidator::Nats(TargetType::AuditLog),
            ChannelTargetType::Nats.as_str(),
            AUDIT_NATS_KEYS,
        ),
        build_admin_descriptor(
            AUDIT_PULSAR_SUB_SYS,
            TargetRequestValidator::Pulsar(TargetType::AuditLog),
            ChannelTargetType::Pulsar.as_str(),
            AUDIT_PULSAR_KEYS,
        ),
        build_admin_descriptor(
            AUDIT_KAFKA_SUB_SYS,
            TargetRequestValidator::Kafka(TargetType::AuditLog),
            ChannelTargetType::Kafka.as_str(),
            AUDIT_KAFKA_KEYS,
        ),
        build_admin_descriptor(
            AUDIT_REDIS_SUB_SYS,
            TargetRequestValidator::Redis {
                default_channel: AUDIT_REDIS_DEFAULT_CHANNEL,
                target_type: TargetType::AuditLog,
            },
            ChannelTargetType::Redis.as_str(),
            AUDIT_REDIS_KEYS,
        ),
        build_admin_descriptor(
            AUDIT_MYSQL_SUB_SYS,
            TargetRequestValidator::MySql(TargetType::AuditLog),
            ChannelTargetType::MySql.as_str(),
            AUDIT_MYSQL_KEYS,
        ),
        build_admin_descriptor(
            AUDIT_POSTGRES_SUB_SYS,
            TargetRequestValidator::Postgres(TargetType::AuditLog),
            ChannelTargetType::Postgres.as_str(),
            AUDIT_POSTGRES_KEYS,
        ),
    ]
}

pub fn builtin_audit_target_descriptors<E>() -> Vec<BuiltinTargetDescriptor<E>>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    vec![
        build_descriptor(
            AUDIT_AMQP_SUB_SYS,
            TargetRequestValidator::Amqp(TargetType::AuditLog),
            ChannelTargetType::Amqp.as_str(),
            AUDIT_AMQP_KEYS,
            |config| validate_amqp_config(config, AUDIT_DEFAULT_DIR),
            |id, config| {
                let args = build_amqp_args(config, AUDIT_DEFAULT_DIR, TargetType::AuditLog)?;
                Ok(boxed_target(crate::target::amqp::AMQPTarget::<E>::new(id, args)?))
            },
        ),
        build_descriptor(
            AUDIT_WEBHOOK_SUB_SYS,
            TargetRequestValidator::Webhook,
            ChannelTargetType::Webhook.as_str(),
            AUDIT_WEBHOOK_KEYS,
            |config| validate_webhook_config(config, AUDIT_DEFAULT_DIR),
            |id, config| {
                let args = build_webhook_args(config, AUDIT_DEFAULT_DIR, TargetType::AuditLog)?;
                Ok(boxed_target(crate::target::webhook::WebhookTarget::<E>::new(id, args)?))
            },
        ),
        build_descriptor(
            AUDIT_MQTT_SUB_SYS,
            TargetRequestValidator::Mqtt,
            ChannelTargetType::Mqtt.as_str(),
            AUDIT_MQTT_KEYS,
            validate_mqtt_config,
            |id, config| {
                let args = build_mqtt_args(config, AUDIT_DEFAULT_DIR, TargetType::AuditLog)?;
                Ok(boxed_target(crate::target::mqtt::MQTTTarget::<E>::new(id, args)?))
            },
        ),
        build_descriptor(
            AUDIT_NATS_SUB_SYS,
            TargetRequestValidator::Nats(TargetType::AuditLog),
            ChannelTargetType::Nats.as_str(),
            AUDIT_NATS_KEYS,
            |config| validate_nats_config(config, AUDIT_DEFAULT_DIR),
            |id, config| {
                let args = build_nats_args(config, AUDIT_DEFAULT_DIR, TargetType::AuditLog)?;
                Ok(boxed_target(crate::target::nats::NATSTarget::<E>::new(id, args)?))
            },
        ),
        build_descriptor(
            AUDIT_PULSAR_SUB_SYS,
            TargetRequestValidator::Pulsar(TargetType::AuditLog),
            ChannelTargetType::Pulsar.as_str(),
            AUDIT_PULSAR_KEYS,
            |config| validate_pulsar_config(config, AUDIT_DEFAULT_DIR),
            |id, config| {
                let args = build_pulsar_args(config, AUDIT_DEFAULT_DIR, TargetType::AuditLog)?;
                Ok(boxed_target(crate::target::pulsar::PulsarTarget::<E>::new(id, args)?))
            },
        ),
        build_descriptor(
            AUDIT_KAFKA_SUB_SYS,
            TargetRequestValidator::Kafka(TargetType::AuditLog),
            ChannelTargetType::Kafka.as_str(),
            AUDIT_KAFKA_KEYS,
            |config| validate_kafka_config(config, AUDIT_DEFAULT_DIR),
            |id, config| {
                let args = build_kafka_args(config, AUDIT_DEFAULT_DIR, TargetType::AuditLog)?;
                Ok(boxed_target(crate::target::kafka::KafkaTarget::<E>::new(id, args)?))
            },
        ),
        build_descriptor(
            AUDIT_REDIS_SUB_SYS,
            TargetRequestValidator::Redis {
                default_channel: AUDIT_REDIS_DEFAULT_CHANNEL,
                target_type: TargetType::AuditLog,
            },
            ChannelTargetType::Redis.as_str(),
            AUDIT_REDIS_KEYS,
            |config| validate_redis_config(config, AUDIT_DEFAULT_DIR, AUDIT_REDIS_DEFAULT_CHANNEL),
            |id, config| {
                let args = build_redis_args(config, AUDIT_DEFAULT_DIR, AUDIT_REDIS_DEFAULT_CHANNEL, TargetType::AuditLog)?;
                Ok(boxed_target(crate::target::redis::RedisTarget::<E>::new(id, args)?))
            },
        ),
        build_descriptor(
            AUDIT_MYSQL_SUB_SYS,
            TargetRequestValidator::MySql(TargetType::AuditLog),
            ChannelTargetType::MySql.as_str(),
            AUDIT_MYSQL_KEYS,
            |config| validate_mysql_config(config, AUDIT_DEFAULT_DIR),
            |id, config| {
                let args = build_mysql_args(config, AUDIT_DEFAULT_DIR, TargetType::AuditLog)?;
                Ok(boxed_target(crate::target::mysql::MySqlTarget::<E>::new(id, args)?))
            },
        ),
        build_descriptor(
            AUDIT_POSTGRES_SUB_SYS,
            TargetRequestValidator::Postgres(TargetType::AuditLog),
            ChannelTargetType::Postgres.as_str(),
            AUDIT_POSTGRES_KEYS,
            |config| validate_postgres_config(config, AUDIT_DEFAULT_DIR),
            |id, config| {
                let args = build_postgres_args(config, AUDIT_DEFAULT_DIR, TargetType::AuditLog)?;
                Ok(boxed_target(crate::target::postgres::PostgresTarget::<E>::new(id, args)?))
            },
        ),
    ]
}

pub fn builtin_notify_target_admin_descriptors() -> Vec<BuiltinTargetAdminDescriptor> {
    vec![
        build_admin_descriptor(
            NOTIFY_WEBHOOK_SUB_SYS,
            TargetRequestValidator::Webhook,
            ChannelTargetType::Webhook.as_str(),
            NOTIFY_WEBHOOK_KEYS,
        ),
        build_admin_descriptor(
            NOTIFY_AMQP_SUB_SYS,
            TargetRequestValidator::Amqp(TargetType::NotifyEvent),
            ChannelTargetType::Amqp.as_str(),
            NOTIFY_AMQP_KEYS,
        ),
        build_admin_descriptor(
            NOTIFY_KAFKA_SUB_SYS,
            TargetRequestValidator::Kafka(TargetType::NotifyEvent),
            ChannelTargetType::Kafka.as_str(),
            NOTIFY_KAFKA_KEYS,
        ),
        build_admin_descriptor(
            NOTIFY_MQTT_SUB_SYS,
            TargetRequestValidator::Mqtt,
            ChannelTargetType::Mqtt.as_str(),
            NOTIFY_MQTT_KEYS,
        ),
        build_admin_descriptor(
            NOTIFY_MYSQL_SUB_SYS,
            TargetRequestValidator::MySql(TargetType::NotifyEvent),
            ChannelTargetType::MySql.as_str(),
            NOTIFY_MYSQL_KEYS,
        ),
        build_admin_descriptor(
            NOTIFY_NATS_SUB_SYS,
            TargetRequestValidator::Nats(TargetType::NotifyEvent),
            ChannelTargetType::Nats.as_str(),
            NOTIFY_NATS_KEYS,
        ),
        build_admin_descriptor(
            NOTIFY_POSTGRES_SUB_SYS,
            TargetRequestValidator::Postgres(TargetType::NotifyEvent),
            ChannelTargetType::Postgres.as_str(),
            NOTIFY_POSTGRES_KEYS,
        ),
        build_admin_descriptor(
            NOTIFY_REDIS_SUB_SYS,
            TargetRequestValidator::Redis {
                default_channel: NOTIFY_REDIS_DEFAULT_CHANNEL,
                target_type: TargetType::NotifyEvent,
            },
            ChannelTargetType::Redis.as_str(),
            NOTIFY_REDIS_KEYS,
        ),
        build_admin_descriptor(
            NOTIFY_PULSAR_SUB_SYS,
            TargetRequestValidator::Pulsar(TargetType::NotifyEvent),
            ChannelTargetType::Pulsar.as_str(),
            NOTIFY_PULSAR_KEYS,
        ),
    ]
}

pub fn builtin_notify_target_descriptors<E>() -> Vec<BuiltinTargetDescriptor<E>>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    vec![
        build_descriptor(
            NOTIFY_WEBHOOK_SUB_SYS,
            TargetRequestValidator::Webhook,
            ChannelTargetType::Webhook.as_str(),
            NOTIFY_WEBHOOK_KEYS,
            |config| validate_webhook_config(config, EVENT_DEFAULT_DIR),
            |id, config| {
                let args = build_webhook_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
                Ok(boxed_target(crate::target::webhook::WebhookTarget::<E>::new(id, args)?))
            },
        ),
        build_descriptor(
            NOTIFY_AMQP_SUB_SYS,
            TargetRequestValidator::Amqp(TargetType::NotifyEvent),
            ChannelTargetType::Amqp.as_str(),
            NOTIFY_AMQP_KEYS,
            |config| validate_amqp_config(config, EVENT_DEFAULT_DIR),
            |id, config| {
                let args = build_amqp_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
                Ok(boxed_target(crate::target::amqp::AMQPTarget::<E>::new(id, args)?))
            },
        ),
        build_descriptor(
            NOTIFY_KAFKA_SUB_SYS,
            TargetRequestValidator::Kafka(TargetType::NotifyEvent),
            ChannelTargetType::Kafka.as_str(),
            NOTIFY_KAFKA_KEYS,
            |config| validate_kafka_config(config, EVENT_DEFAULT_DIR),
            |id, config| {
                let args = build_kafka_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
                Ok(boxed_target(crate::target::kafka::KafkaTarget::<E>::new(id, args)?))
            },
        ),
        build_descriptor(
            NOTIFY_MQTT_SUB_SYS,
            TargetRequestValidator::Mqtt,
            ChannelTargetType::Mqtt.as_str(),
            NOTIFY_MQTT_KEYS,
            validate_mqtt_config,
            |id, config| {
                let args = build_mqtt_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
                Ok(boxed_target(crate::target::mqtt::MQTTTarget::<E>::new(id, args)?))
            },
        ),
        build_descriptor(
            NOTIFY_MYSQL_SUB_SYS,
            TargetRequestValidator::MySql(TargetType::NotifyEvent),
            ChannelTargetType::MySql.as_str(),
            NOTIFY_MYSQL_KEYS,
            |config| validate_mysql_config(config, EVENT_DEFAULT_DIR),
            |id, config| {
                let args = build_mysql_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
                Ok(boxed_target(crate::target::mysql::MySqlTarget::<E>::new(id, args)?))
            },
        ),
        build_descriptor(
            NOTIFY_NATS_SUB_SYS,
            TargetRequestValidator::Nats(TargetType::NotifyEvent),
            ChannelTargetType::Nats.as_str(),
            NOTIFY_NATS_KEYS,
            |config| validate_nats_config(config, EVENT_DEFAULT_DIR),
            |id, config| {
                let args = build_nats_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
                Ok(boxed_target(crate::target::nats::NATSTarget::<E>::new(id, args)?))
            },
        ),
        build_descriptor(
            NOTIFY_POSTGRES_SUB_SYS,
            TargetRequestValidator::Postgres(TargetType::NotifyEvent),
            ChannelTargetType::Postgres.as_str(),
            NOTIFY_POSTGRES_KEYS,
            |config| validate_postgres_config(config, EVENT_DEFAULT_DIR),
            |id, config| {
                let args = build_postgres_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
                Ok(boxed_target(crate::target::postgres::PostgresTarget::<E>::new(id, args)?))
            },
        ),
        build_descriptor(
            NOTIFY_REDIS_SUB_SYS,
            TargetRequestValidator::Redis {
                default_channel: NOTIFY_REDIS_DEFAULT_CHANNEL,
                target_type: TargetType::NotifyEvent,
            },
            ChannelTargetType::Redis.as_str(),
            NOTIFY_REDIS_KEYS,
            |config| validate_redis_config(config, EVENT_DEFAULT_DIR, NOTIFY_REDIS_DEFAULT_CHANNEL),
            |id, config| {
                let args = build_redis_args(config, EVENT_DEFAULT_DIR, NOTIFY_REDIS_DEFAULT_CHANNEL, TargetType::NotifyEvent)?;
                Ok(boxed_target(crate::target::redis::RedisTarget::<E>::new(id, args)?))
            },
        ),
        build_descriptor(
            NOTIFY_PULSAR_SUB_SYS,
            TargetRequestValidator::Pulsar(TargetType::NotifyEvent),
            ChannelTargetType::Pulsar.as_str(),
            NOTIFY_PULSAR_KEYS,
            |config| validate_pulsar_config(config, EVENT_DEFAULT_DIR),
            |id, config| {
                let args = build_pulsar_args(config, EVENT_DEFAULT_DIR, TargetType::NotifyEvent)?;
                Ok(boxed_target(crate::target::pulsar::PulsarTarget::<E>::new(id, args)?))
            },
        ),
    ]
}
