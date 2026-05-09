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

use super::common::{parse_target_bool, parse_url, validate_nats_server_config, validate_pulsar_broker_config};
use crate::error::TargetError;
use crate::target::{
    TargetType,
    amqp::AMQPArgs,
    kafka::KafkaArgs,
    mqtt::{MQTTArgs, MQTTTlsConfig, validate_mqtt_broker_url},
    mysql::MySqlArgs,
    nats::{NATSArgs, validate_nats_address},
    postgres::{PostgresArgs, PostgresDsn, parse_postgres_format},
    pulsar::{PulsarArgs, validate_pulsar_broker},
    redis::{RedisArgs, RedisTlsConfig, validate_redis_url},
    webhook::WebhookArgs,
};
use rumqttc::QoS;
use rustfs_config::{
    AMQP_EXCHANGE, AMQP_MANDATORY, AMQP_PASSWORD, AMQP_PERSISTENT, AMQP_QUEUE_DIR, AMQP_QUEUE_LIMIT, AMQP_ROUTING_KEY,
    AMQP_TLS_CA, AMQP_TLS_CLIENT_CERT, AMQP_TLS_CLIENT_KEY, AMQP_URL, AMQP_USERNAME, DEFAULT_LIMIT, KAFKA_ACKS, KAFKA_BROKERS,
    KAFKA_QUEUE_DIR, KAFKA_QUEUE_LIMIT, KAFKA_TLS_CA, KAFKA_TLS_CLIENT_CERT, KAFKA_TLS_CLIENT_KEY, KAFKA_TLS_ENABLE, KAFKA_TOPIC,
    MQTT_BROKER, MQTT_KEEP_ALIVE_INTERVAL, MQTT_PASSWORD, MQTT_QOS, MQTT_QUEUE_DIR, MQTT_QUEUE_LIMIT, MQTT_RECONNECT_INTERVAL,
    MQTT_TLS_CA, MQTT_TLS_CLIENT_CERT, MQTT_TLS_CLIENT_KEY, MQTT_TLS_POLICY, MQTT_TLS_TRUST_LEAF_AS_CA, MQTT_TOPIC,
    MQTT_USERNAME, MQTT_WS_PATH_ALLOWLIST, MYSQL_DSN_STRING, MYSQL_FORMAT, MYSQL_MAX_OPEN_CONNECTIONS, MYSQL_QUEUE_DIR,
    MYSQL_QUEUE_LIMIT, MYSQL_TABLE, MYSQL_TLS_CA, MYSQL_TLS_CLIENT_CERT, MYSQL_TLS_CLIENT_KEY, NATS_ADDRESS,
    NATS_CREDENTIALS_FILE, NATS_PASSWORD, NATS_QUEUE_DIR, NATS_QUEUE_LIMIT, NATS_SUBJECT, NATS_TLS_CA, NATS_TLS_CLIENT_CERT,
    NATS_TLS_CLIENT_KEY, NATS_TLS_REQUIRED, NATS_TOKEN, NATS_USERNAME, POSTGRES_DSN_STRING, POSTGRES_FORMAT, POSTGRES_QUEUE_DIR,
    POSTGRES_QUEUE_LIMIT, POSTGRES_TABLE, POSTGRES_TLS_CA, POSTGRES_TLS_CLIENT_CERT, POSTGRES_TLS_CLIENT_KEY,
    POSTGRES_TLS_REQUIRED, PULSAR_AUTH_TOKEN, PULSAR_BROKER, PULSAR_PASSWORD, PULSAR_QUEUE_DIR, PULSAR_QUEUE_LIMIT,
    PULSAR_TLS_ALLOW_INSECURE, PULSAR_TLS_CA, PULSAR_TLS_HOSTNAME_VERIFICATION, PULSAR_TOPIC, PULSAR_USERNAME, REDIS_CHANNEL,
    REDIS_CONNECTION_TIMEOUT, REDIS_KEEP_ALIVE_INTERVAL, REDIS_MAX_RETRY_ATTEMPTS, REDIS_MAX_RETRY_DELAY, REDIS_MIN_RETRY_DELAY,
    REDIS_PASSWORD, REDIS_PIPELINE_BUFFER_SIZE, REDIS_QUEUE_DIR, REDIS_QUEUE_LIMIT, REDIS_RECONNECT_RETRY_ATTEMPTS,
    REDIS_RESPONSE_TIMEOUT, REDIS_TLS_ALLOW_INSECURE, REDIS_TLS_CA, REDIS_TLS_CLIENT_CERT, REDIS_TLS_CLIENT_KEY,
    REDIS_TLS_POLICY, REDIS_URL, REDIS_USERNAME, RUSTFS_WEBHOOK_SKIP_TLS_VERIFY_DEFAULT, WEBHOOK_AUTH_TOKEN, WEBHOOK_CLIENT_CA,
    WEBHOOK_CLIENT_CERT, WEBHOOK_CLIENT_KEY, WEBHOOK_ENDPOINT, WEBHOOK_QUEUE_DIR, WEBHOOK_QUEUE_LIMIT, WEBHOOK_SKIP_TLS_VERIFY,
};
use rustfs_ecstore::config::KVS;
use std::path::Path;
use std::time::Duration;

fn parse_kafka_acks_value(value: Option<&str>) -> Result<i16, TargetError> {
    let Some(value) = value else {
        return Ok(1);
    };

    let normalized = value.trim();
    if normalized.is_empty() {
        return Err(TargetError::Configuration("Kafka acks must be one of: 0, 1, -1, all".to_string()));
    }

    match normalized.to_ascii_lowercase().as_str() {
        "0" => Ok(0),
        "1" => Ok(1),
        "-1" | "all" => Ok(-1),
        _ => Err(TargetError::Configuration("Kafka acks must be one of: 0, 1, -1, all".to_string())),
    }
}

fn parse_amqp_bool_value(field: &str, config: &KVS, default: bool) -> Result<bool, TargetError> {
    match config.lookup(field) {
        Some(value) => parse_target_bool(Some(value.as_str()))
            .ok_or_else(|| TargetError::Configuration(format!("Invalid AMQP {field} boolean value: {value}"))),
        None => Ok(default),
    }
}

pub fn build_amqp_args(config: &KVS, default_queue_dir: &str, target_type: TargetType) -> Result<AMQPArgs, TargetError> {
    let url = config
        .lookup(AMQP_URL)
        .ok_or_else(|| TargetError::Configuration("Missing AMQP url".to_string()))?;
    let url = parse_url(url.trim(), "AMQP URL")?;

    let exchange = config
        .lookup(AMQP_EXCHANGE)
        .ok_or_else(|| TargetError::Configuration("Missing AMQP exchange".to_string()))?;
    let routing_key = config
        .lookup(AMQP_ROUTING_KEY)
        .ok_or_else(|| TargetError::Configuration("Missing AMQP routing_key".to_string()))?;

    let args = AMQPArgs {
        enable: true,
        url,
        exchange,
        routing_key,
        mandatory: parse_amqp_bool_value(AMQP_MANDATORY, config, false)?,
        persistent: parse_amqp_bool_value(AMQP_PERSISTENT, config, true)?,
        username: config.lookup(AMQP_USERNAME).unwrap_or_default(),
        password: config.lookup(AMQP_PASSWORD).unwrap_or_default(),
        tls_ca: config.lookup(AMQP_TLS_CA).unwrap_or_default(),
        tls_client_cert: config.lookup(AMQP_TLS_CLIENT_CERT).unwrap_or_default(),
        tls_client_key: config.lookup(AMQP_TLS_CLIENT_KEY).unwrap_or_default(),
        queue_dir: config.lookup(AMQP_QUEUE_DIR).unwrap_or_else(|| default_queue_dir.to_string()),
        queue_limit: config
            .lookup(AMQP_QUEUE_LIMIT)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_LIMIT),
        target_type,
    };
    args.validate()?;
    Ok(args)
}

pub fn validate_amqp_config(config: &KVS, default_queue_dir: &str) -> Result<(), TargetError> {
    let _ = build_amqp_args(config, default_queue_dir, TargetType::NotifyEvent)?;
    Ok(())
}

pub fn build_webhook_args(config: &KVS, default_queue_dir: &str, target_type: TargetType) -> Result<WebhookArgs, TargetError> {
    let endpoint = config
        .lookup(WEBHOOK_ENDPOINT)
        .ok_or_else(|| TargetError::Configuration("Missing webhook endpoint".to_string()))?;
    let parsed_endpoint = endpoint.trim();
    let endpoint_url = parse_url(parsed_endpoint, "endpoint URL")?;

    Ok(WebhookArgs {
        enable: true,
        endpoint: endpoint_url,
        auth_token: config.lookup(WEBHOOK_AUTH_TOKEN).unwrap_or_default(),
        queue_dir: config
            .lookup(WEBHOOK_QUEUE_DIR)
            .unwrap_or_else(|| default_queue_dir.to_string()),
        queue_limit: config
            .lookup(WEBHOOK_QUEUE_LIMIT)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_LIMIT),
        client_cert: config.lookup(WEBHOOK_CLIENT_CERT).unwrap_or_default(),
        client_key: config.lookup(WEBHOOK_CLIENT_KEY).unwrap_or_default(),
        client_ca: config.lookup(WEBHOOK_CLIENT_CA).unwrap_or_default(),
        skip_tls_verify: config
            .lookup(WEBHOOK_SKIP_TLS_VERIFY)
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(RUSTFS_WEBHOOK_SKIP_TLS_VERIFY_DEFAULT),
        target_type,
    })
}

pub fn validate_webhook_config(config: &KVS, default_queue_dir: &str) -> Result<(), TargetError> {
    let endpoint = config
        .lookup(WEBHOOK_ENDPOINT)
        .ok_or_else(|| TargetError::Configuration("Missing webhook endpoint".to_string()))?;
    let parsed_endpoint = endpoint.trim();
    let _ = parse_url(parsed_endpoint, "endpoint URL")?;

    let client_cert = config.lookup(WEBHOOK_CLIENT_CERT).unwrap_or_default();
    let client_key = config.lookup(WEBHOOK_CLIENT_KEY).unwrap_or_default();
    if client_cert.is_empty() != client_key.is_empty() {
        return Err(TargetError::Configuration(
            "Both client_cert and client_key must be specified together".to_string(),
        ));
    }

    let queue_dir = config
        .lookup(WEBHOOK_QUEUE_DIR)
        .unwrap_or_else(|| default_queue_dir.to_string());
    if !queue_dir.is_empty() && !Path::new(&queue_dir).is_absolute() {
        return Err(TargetError::Configuration("Webhook queue directory must be an absolute path".to_string()));
    }

    Ok(())
}

pub fn build_mqtt_args(config: &KVS, default_queue_dir: &str, target_type: TargetType) -> Result<MQTTArgs, TargetError> {
    let broker = config
        .lookup(MQTT_BROKER)
        .ok_or_else(|| TargetError::Configuration("Missing MQTT broker".to_string()))?;
    let broker_url = parse_url(&broker, "broker URL")?;

    let topic = config
        .lookup(MQTT_TOPIC)
        .ok_or_else(|| TargetError::Configuration("Missing MQTT topic".to_string()))?;

    Ok(MQTTArgs {
        enable: true,
        broker: broker_url,
        topic,
        qos: config
            .lookup(MQTT_QOS)
            .and_then(|v| v.parse::<u8>().ok())
            .map(|q| match q {
                0 => QoS::AtMostOnce,
                1 => QoS::AtLeastOnce,
                2 => QoS::ExactlyOnce,
                _ => QoS::AtLeastOnce,
            })
            .unwrap_or(QoS::AtLeastOnce),
        username: config.lookup(MQTT_USERNAME).unwrap_or_default(),
        password: config.lookup(MQTT_PASSWORD).unwrap_or_default(),
        max_reconnect_interval: config
            .lookup(MQTT_RECONNECT_INTERVAL)
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(5)),
        keep_alive: config
            .lookup(MQTT_KEEP_ALIVE_INTERVAL)
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(30)),
        tls: MQTTTlsConfig::from_values(
            config.lookup(MQTT_TLS_POLICY).as_deref(),
            config.lookup(MQTT_TLS_CA).as_deref(),
            config.lookup(MQTT_TLS_CLIENT_CERT).as_deref(),
            config.lookup(MQTT_TLS_CLIENT_KEY).as_deref(),
            config.lookup(MQTT_TLS_TRUST_LEAF_AS_CA).as_deref(),
            config.lookup(MQTT_WS_PATH_ALLOWLIST).as_deref(),
        )?,
        queue_dir: config.lookup(MQTT_QUEUE_DIR).unwrap_or_else(|| default_queue_dir.to_string()),
        queue_limit: config
            .lookup(MQTT_QUEUE_LIMIT)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_LIMIT),
        target_type,
    })
}

pub fn validate_mqtt_config(config: &KVS) -> Result<(), TargetError> {
    let broker = config
        .lookup(MQTT_BROKER)
        .ok_or_else(|| TargetError::Configuration("Missing MQTT broker".to_string()))?;
    let url = parse_url(&broker, "broker URL")?;

    let tls = MQTTTlsConfig::from_values(
        config.lookup(MQTT_TLS_POLICY).as_deref(),
        config.lookup(MQTT_TLS_CA).as_deref(),
        config.lookup(MQTT_TLS_CLIENT_CERT).as_deref(),
        config.lookup(MQTT_TLS_CLIENT_KEY).as_deref(),
        config.lookup(MQTT_TLS_TRUST_LEAF_AS_CA).as_deref(),
        config.lookup(MQTT_WS_PATH_ALLOWLIST).as_deref(),
    )?;
    validate_mqtt_broker_url(&url, &tls)?;

    if config.lookup(MQTT_TOPIC).is_none() {
        return Err(TargetError::Configuration("Missing MQTT topic".to_string()));
    }

    if let Some(qos_str) = config.lookup(MQTT_QOS) {
        let qos = qos_str
            .parse::<u8>()
            .map_err(|_| TargetError::Configuration("Invalid QoS value".to_string()))?;
        if qos > 2 {
            return Err(TargetError::Configuration("QoS must be 0, 1, or 2".to_string()));
        }
    }

    let queue_dir = config.lookup(MQTT_QUEUE_DIR).unwrap_or_default();
    if !queue_dir.is_empty() {
        if !Path::new(&queue_dir).is_absolute() {
            return Err(TargetError::Configuration("MQTT queue directory must be an absolute path".to_string()));
        }
        if let Some(qos_str) = config.lookup(MQTT_QOS)
            && qos_str == "0"
        {
            return Err(TargetError::Configuration(
                "QoS should be AtLeastOnce (1) or ExactlyOnce (2) if queue_dir is set".to_string(),
            ));
        }
    }

    Ok(())
}

pub fn build_nats_args(config: &KVS, default_queue_dir: &str, target_type: TargetType) -> Result<NATSArgs, TargetError> {
    let address = config
        .lookup(NATS_ADDRESS)
        .ok_or_else(|| TargetError::Configuration("Missing NATS address".to_string()))?;
    validate_nats_address(&address)?;

    let subject = config
        .lookup(NATS_SUBJECT)
        .ok_or_else(|| TargetError::Configuration("Missing NATS subject".to_string()))?;

    Ok(NATSArgs {
        enable: true,
        address,
        subject,
        username: config.lookup(NATS_USERNAME).unwrap_or_default(),
        password: config.lookup(NATS_PASSWORD).unwrap_or_default(),
        token: config.lookup(NATS_TOKEN).unwrap_or_default(),
        credentials_file: config.lookup(NATS_CREDENTIALS_FILE).unwrap_or_default(),
        tls_ca: config.lookup(NATS_TLS_CA).unwrap_or_default(),
        tls_client_cert: config.lookup(NATS_TLS_CLIENT_CERT).unwrap_or_default(),
        tls_client_key: config.lookup(NATS_TLS_CLIENT_KEY).unwrap_or_default(),
        tls_required: parse_target_bool(config.lookup(NATS_TLS_REQUIRED).as_deref()).unwrap_or(false),
        queue_dir: config.lookup(NATS_QUEUE_DIR).unwrap_or_else(|| default_queue_dir.to_string()),
        queue_limit: config
            .lookup(NATS_QUEUE_LIMIT)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_LIMIT),
        target_type,
    })
}

pub fn validate_nats_config(config: &KVS, default_queue_dir: &str) -> Result<(), TargetError> {
    let address = config
        .lookup(NATS_ADDRESS)
        .ok_or_else(|| TargetError::Configuration("Missing NATS address".to_string()))?;
    let server = validate_nats_address(&address)?;
    validate_nats_server_config(&server, config, default_queue_dir)
}

pub fn build_pulsar_args(config: &KVS, default_queue_dir: &str, target_type: TargetType) -> Result<PulsarArgs, TargetError> {
    let broker = config
        .lookup(PULSAR_BROKER)
        .ok_or_else(|| TargetError::Configuration("Missing Pulsar broker".to_string()))?;
    validate_pulsar_broker(&broker)?;

    let topic = config
        .lookup(PULSAR_TOPIC)
        .ok_or_else(|| TargetError::Configuration("Missing Pulsar topic".to_string()))?;

    Ok(PulsarArgs {
        enable: true,
        broker,
        topic,
        auth_token: config.lookup(PULSAR_AUTH_TOKEN).unwrap_or_default(),
        username: config.lookup(PULSAR_USERNAME).unwrap_or_default(),
        password: config.lookup(PULSAR_PASSWORD).unwrap_or_default(),
        tls_ca: config.lookup(PULSAR_TLS_CA).unwrap_or_default(),
        tls_allow_insecure: parse_target_bool(config.lookup(PULSAR_TLS_ALLOW_INSECURE).as_deref()).unwrap_or(false),
        tls_hostname_verification: parse_target_bool(config.lookup(PULSAR_TLS_HOSTNAME_VERIFICATION).as_deref()).unwrap_or(true),
        queue_dir: config
            .lookup(PULSAR_QUEUE_DIR)
            .unwrap_or_else(|| default_queue_dir.to_string()),
        queue_limit: config
            .lookup(PULSAR_QUEUE_LIMIT)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_LIMIT),
        target_type,
    })
}

pub fn validate_pulsar_config(config: &KVS, default_queue_dir: &str) -> Result<(), TargetError> {
    let broker = config
        .lookup(PULSAR_BROKER)
        .ok_or_else(|| TargetError::Configuration("Missing Pulsar broker".to_string()))?;
    validate_pulsar_broker_config(&broker, config, default_queue_dir)
}

pub fn build_redis_args(
    config: &KVS,
    default_queue_dir: &str,
    default_channel: &str,
    target_type: TargetType,
) -> Result<RedisArgs, TargetError> {
    let url = config
        .lookup(REDIS_URL)
        .ok_or_else(|| TargetError::Configuration("Missing Redis URL".to_string()))?;
    let url = parse_url(&url, "Redis URL")?;

    let channel = config
        .lookup(REDIS_CHANNEL)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default_channel.to_string());

    Ok(RedisArgs {
        enable: true,
        url,
        channel,
        username: config.lookup(REDIS_USERNAME).filter(|value| !value.trim().is_empty()),
        password: config.lookup(REDIS_PASSWORD).filter(|value| !value.trim().is_empty()),
        tls: RedisTlsConfig::from_values(
            config.lookup(REDIS_TLS_POLICY).as_deref(),
            config.lookup(REDIS_TLS_CA).as_deref(),
            config.lookup(REDIS_TLS_CLIENT_CERT).as_deref(),
            config.lookup(REDIS_TLS_CLIENT_KEY).as_deref(),
            config.lookup(REDIS_TLS_ALLOW_INSECURE).as_deref(),
        )?,
        keep_alive: config
            .lookup(REDIS_KEEP_ALIVE_INTERVAL)
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(15)),
        queue_dir: config
            .lookup(REDIS_QUEUE_DIR)
            .unwrap_or_else(|| default_queue_dir.to_string()),
        queue_limit: config
            .lookup(REDIS_QUEUE_LIMIT)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_LIMIT),
        max_retry_attempts: config
            .lookup(REDIS_MAX_RETRY_ATTEMPTS)
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(3),
        reconnect_retry_attempts: config
            .lookup(REDIS_RECONNECT_RETRY_ATTEMPTS)
            .and_then(|v| v.parse::<usize>().ok()),
        min_retry_delay: config
            .lookup(REDIS_MIN_RETRY_DELAY)
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_millis),
        max_retry_delay: config
            .lookup(REDIS_MAX_RETRY_DELAY)
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_millis),
        connection_timeout: config
            .lookup(REDIS_CONNECTION_TIMEOUT)
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs),
        response_timeout: config
            .lookup(REDIS_RESPONSE_TIMEOUT)
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs),
        pipeline_buffer_size: config
            .lookup(REDIS_PIPELINE_BUFFER_SIZE)
            .and_then(|v| v.parse::<usize>().ok()),
        target_type,
    })
}

pub fn build_postgres_args(config: &KVS, default_queue_dir: &str, target_type: TargetType) -> Result<PostgresArgs, TargetError> {
    let dsn_string = config
        .lookup(POSTGRES_DSN_STRING)
        .ok_or_else(|| TargetError::Configuration("Missing PostgreSQL dsn_string".to_string()))?;
    let table = config
        .lookup(POSTGRES_TABLE)
        .ok_or_else(|| TargetError::Configuration("Missing PostgreSQL table".to_string()))?;

    let schema = PostgresDsn::parse(&dsn_string)?.schema;
    let format = parse_postgres_format(config.lookup(POSTGRES_FORMAT).as_deref())?;

    Ok(PostgresArgs {
        enable: true,
        dsn_string,
        schema,
        table,
        format,
        tls_required: parse_target_bool(config.lookup(POSTGRES_TLS_REQUIRED).as_deref()).unwrap_or(false),
        tls_ca: config.lookup(POSTGRES_TLS_CA).unwrap_or_default(),
        tls_client_cert: config.lookup(POSTGRES_TLS_CLIENT_CERT).unwrap_or_default(),
        tls_client_key: config.lookup(POSTGRES_TLS_CLIENT_KEY).unwrap_or_default(),
        queue_dir: config
            .lookup(POSTGRES_QUEUE_DIR)
            .unwrap_or_else(|| default_queue_dir.to_string()),
        queue_limit: config
            .lookup(POSTGRES_QUEUE_LIMIT)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_LIMIT),
        target_type,
    })
}

pub fn validate_redis_config(config: &KVS, default_queue_dir: &str, default_channel: &str) -> Result<(), TargetError> {
    let url = config
        .lookup(REDIS_URL)
        .ok_or_else(|| TargetError::Configuration("Missing Redis URL".to_string()))?;
    let url = parse_url(&url, "Redis URL")?;
    validate_redis_url(&url)?;

    let args = build_redis_args(config, default_queue_dir, default_channel, TargetType::NotifyEvent)?;
    args.validate()
}
pub fn validate_postgres_config(config: &KVS, default_queue_dir: &str) -> Result<(), TargetError> {
    let args = build_postgres_args(config, default_queue_dir, TargetType::NotifyEvent)?;
    args.validate()
}

pub fn build_kafka_args(config: &KVS, default_queue_dir: &str, target_type: TargetType) -> Result<KafkaArgs, TargetError> {
    let brokers_raw = config
        .lookup(KAFKA_BROKERS)
        .ok_or_else(|| TargetError::Configuration("Missing Kafka brokers".to_string()))?;
    if brokers_raw.split(',').all(|s| s.trim().is_empty()) {
        return Err(TargetError::Configuration("Kafka brokers cannot be empty".to_string()));
    }
    let brokers: Vec<String> = brokers_raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let topic = config
        .lookup(KAFKA_TOPIC)
        .ok_or_else(|| TargetError::Configuration("Missing Kafka topic".to_string()))?;

    Ok(KafkaArgs {
        enable: true,
        brokers,
        topic,
        acks: parse_kafka_acks_value(config.lookup(KAFKA_ACKS).as_deref())?,
        tls_enable: parse_target_bool(config.lookup(KAFKA_TLS_ENABLE).as_deref()).unwrap_or(false),
        tls_ca: config.lookup(KAFKA_TLS_CA).unwrap_or_default(),
        tls_client_cert: config.lookup(KAFKA_TLS_CLIENT_CERT).unwrap_or_default(),
        tls_client_key: config.lookup(KAFKA_TLS_CLIENT_KEY).unwrap_or_default(),
        queue_dir: config
            .lookup(KAFKA_QUEUE_DIR)
            .unwrap_or_else(|| default_queue_dir.to_string()),
        queue_limit: config
            .lookup(KAFKA_QUEUE_LIMIT)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_LIMIT),
        target_type,
    })
}

pub fn validate_kafka_config(config: &KVS, default_queue_dir: &str) -> Result<(), TargetError> {
    let brokers_raw = config
        .lookup(KAFKA_BROKERS)
        .ok_or_else(|| TargetError::Configuration("Missing Kafka brokers".to_string()))?;
    if brokers_raw.split(',').map(|s| s.trim()).all(|s| s.is_empty()) {
        return Err(TargetError::Configuration("Kafka brokers cannot be empty".to_string()));
    }

    if config.lookup(KAFKA_TOPIC).is_none() {
        return Err(TargetError::Configuration("Missing Kafka topic".to_string()));
    }

    parse_kafka_acks_value(config.lookup(KAFKA_ACKS).as_deref())?;

    let tls_client_cert = config.lookup(KAFKA_TLS_CLIENT_CERT).unwrap_or_default();
    let tls_client_key = config.lookup(KAFKA_TLS_CLIENT_KEY).unwrap_or_default();
    if tls_client_cert.is_empty() != tls_client_key.is_empty() {
        return Err(TargetError::Configuration(
            "Kafka tls_client_cert and tls_client_key must be specified together".to_string(),
        ));
    }

    let queue_dir = config
        .lookup(KAFKA_QUEUE_DIR)
        .unwrap_or_else(|| default_queue_dir.to_string());
    if !queue_dir.is_empty() && !Path::new(&queue_dir).is_absolute() {
        return Err(TargetError::Configuration("Kafka queue directory must be an absolute path".to_string()));
    }

    Ok(())
}

/// Builds `MySqlArgs` from a KVS configuration.
///
/// Parses all MySQL target configuration keys, applies defaults for
/// missing optional values, and validates that all required fields
/// are present and well-formed.
pub fn build_mysql_args(config: &KVS, default_queue_dir: &str, target_type: TargetType) -> Result<MySqlArgs, TargetError> {
    let dsn_string = config
        .lookup(MYSQL_DSN_STRING)
        .ok_or_else(|| TargetError::Configuration("Missing MySQL dsn_string".to_string()))?;

    let table = config
        .lookup(MYSQL_TABLE)
        .ok_or_else(|| TargetError::Configuration("Missing MySQL table".to_string()))?;

    let args = MySqlArgs {
        enable: true,
        dsn_string,
        table,
        format: config.lookup(MYSQL_FORMAT).unwrap_or_else(|| "access".to_string()),
        tls_ca: config.lookup(MYSQL_TLS_CA).unwrap_or_default(),
        tls_client_cert: config.lookup(MYSQL_TLS_CLIENT_CERT).unwrap_or_default(),
        tls_client_key: config.lookup(MYSQL_TLS_CLIENT_KEY).unwrap_or_default(),
        queue_dir: config
            .lookup(MYSQL_QUEUE_DIR)
            .unwrap_or_else(|| default_queue_dir.to_string()),
        queue_limit: config
            .lookup(MYSQL_QUEUE_LIMIT)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_LIMIT),
        max_open_connections: config
            .lookup(MYSQL_MAX_OPEN_CONNECTIONS)
            .map(|value| {
                value.trim().parse::<usize>().map_err(|_| {
                    TargetError::Configuration(format!("MySQL max_open_connections value '{}' is not a valid number", value))
                })
            })
            .transpose()?
            .unwrap_or(2),
        target_type,
    };

    args.validate()?;
    Ok(args)
}

/// Validates MySQL target configuration from a KVS without building args.
///
/// Performs the same checks as `build_mysql_args` but discards the result,
/// used for pre-validation before target creation.
pub fn validate_mysql_config(config: &KVS, default_queue_dir: &str) -> Result<(), TargetError> {
    let _ = build_mysql_args(config, default_queue_dir, TargetType::NotifyEvent)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        build_amqp_args, build_kafka_args, build_mysql_args, build_postgres_args, build_redis_args, validate_amqp_config,
        validate_kafka_config, validate_mysql_config, validate_postgres_config, validate_redis_config,
    };
    use crate::target::{TargetType, postgres::PostgresFormat};
    use rustfs_config::{
        AMQP_EXCHANGE, AMQP_MANDATORY, AMQP_PASSWORD, AMQP_PERSISTENT, AMQP_QUEUE_DIR, AMQP_ROUTING_KEY, AMQP_TLS_CLIENT_CERT,
        AMQP_TLS_CLIENT_KEY, AMQP_URL, AMQP_USERNAME, KAFKA_ACKS, KAFKA_BROKERS, KAFKA_TOPIC, MYSQL_DSN_STRING,
        MYSQL_MAX_OPEN_CONNECTIONS, MYSQL_QUEUE_DIR, MYSQL_TABLE, MYSQL_TLS_CA, MYSQL_TLS_CLIENT_CERT, MYSQL_TLS_CLIENT_KEY,
        POSTGRES_DSN_STRING, POSTGRES_FORMAT, POSTGRES_QUEUE_DIR, POSTGRES_TABLE, POSTGRES_TLS_CA, POSTGRES_TLS_CLIENT_CERT,
        POSTGRES_TLS_CLIENT_KEY, REDIS_CHANNEL, REDIS_CONNECTION_TIMEOUT, REDIS_MAX_RETRY_DELAY, REDIS_MIN_RETRY_DELAY,
        REDIS_PIPELINE_BUFFER_SIZE, REDIS_RECONNECT_RETRY_ATTEMPTS, REDIS_RESPONSE_TIMEOUT, REDIS_TLS_ALLOW_INSECURE, REDIS_URL,
    };
    use rustfs_ecstore::config::KVS;

    fn amqp_base_config() -> KVS {
        let mut config = KVS::new();
        config.insert(AMQP_URL.to_string(), "amqp://127.0.0.1:5672/%2f".to_string());
        config.insert(AMQP_EXCHANGE.to_string(), "rustfs.events".to_string());
        config.insert(AMQP_ROUTING_KEY.to_string(), "objects".to_string());
        config
    }

    fn kafka_base_config() -> KVS {
        let mut config = KVS::new();
        config.insert(KAFKA_BROKERS.to_string(), "127.0.0.1:9092".to_string());
        config.insert(KAFKA_TOPIC.to_string(), "events".to_string());
        config
    }

    fn mysql_base_config() -> KVS {
        let mut config = KVS::new();
        config.insert(
            MYSQL_DSN_STRING.to_string(),
            "rustfs:password@tcp(127.0.0.1:3306)/rustfs_events".to_string(),
        );
        config.insert(MYSQL_TABLE.to_string(), "rustfs_events".to_string());
        config
    }

    #[test]
    fn build_amqp_args_accepts_valid_config() {
        let args = build_amqp_args(&amqp_base_config(), "", TargetType::NotifyEvent).expect("valid AMQP args");

        assert_eq!(args.url.as_str(), "amqp://127.0.0.1:5672/%2f");
        assert_eq!(args.exchange, "rustfs.events");
        assert_eq!(args.routing_key, "objects");
        assert!(!args.mandatory);
        assert!(args.persistent);
    }

    #[test]
    fn build_amqp_args_accepts_bool_aliases() {
        let mut config = amqp_base_config();
        config.insert(AMQP_MANDATORY.to_string(), "on".to_string());
        config.insert(AMQP_PERSISTENT.to_string(), "no".to_string());

        let args = build_amqp_args(&config, "", TargetType::NotifyEvent).expect("valid AMQP bool aliases");

        assert!(args.mandatory);
        assert!(!args.persistent);
    }

    #[test]
    fn validate_amqp_config_rejects_invalid_bool() {
        let mut config = amqp_base_config();
        config.insert(AMQP_MANDATORY.to_string(), "sometimes".to_string());

        let err = validate_amqp_config(&config, "").expect_err("invalid AMQP bool should fail");

        assert!(err.to_string().contains("Invalid AMQP mandatory boolean"));
    }

    #[test]
    fn validate_amqp_config_rejects_invalid_scheme() {
        let mut config = amqp_base_config();
        config.insert(AMQP_URL.to_string(), "http://127.0.0.1:5672".to_string());

        let err = validate_amqp_config(&config, "").expect_err("invalid AMQP scheme should fail");

        assert!(err.to_string().contains("only amqp and amqps"));
    }

    #[test]
    fn validate_amqp_config_rejects_missing_url_host() {
        let mut config = amqp_base_config();
        config.insert(AMQP_URL.to_string(), "amqp:///objects".to_string());

        let err = validate_amqp_config(&config, "").expect_err("missing AMQP host should fail");

        assert!(err.to_string().contains("missing host"));
    }

    #[test]
    fn validate_amqp_config_rejects_missing_exchange() {
        let mut config = amqp_base_config();
        config.0.retain(|kv| kv.key != AMQP_EXCHANGE);

        let err = validate_amqp_config(&config, "").expect_err("missing AMQP exchange should fail");

        assert!(err.to_string().contains("Missing AMQP exchange"));
    }

    #[test]
    fn validate_amqp_config_rejects_missing_routing_key() {
        let mut config = amqp_base_config();
        config.0.retain(|kv| kv.key != AMQP_ROUTING_KEY);

        let err = validate_amqp_config(&config, "").expect_err("missing AMQP routing_key should fail");

        assert!(err.to_string().contains("Missing AMQP routing_key"));
    }

    #[test]
    fn validate_amqp_config_rejects_relative_queue_dir() {
        let mut config = amqp_base_config();
        config.insert(AMQP_QUEUE_DIR.to_string(), "relative-queue".to_string());

        let err = validate_amqp_config(&config, "").expect_err("relative queue_dir should fail");

        assert!(err.to_string().contains("absolute path"));
    }

    #[test]
    fn validate_amqp_config_rejects_unpaired_tls_client_cert_key() {
        let mut config = amqp_base_config();
        config.insert(AMQP_URL.to_string(), "amqps://127.0.0.1:5671/%2f".to_string());
        config.insert(AMQP_TLS_CLIENT_CERT.to_string(), "/tmp/client.crt".to_string());

        let err = validate_amqp_config(&config, "").expect_err("unpaired TLS cert should fail");

        assert!(err.to_string().contains("tls_client_cert and tls_client_key"));
    }

    #[test]
    fn validate_amqp_config_rejects_tls_paths_without_amqps() {
        let mut config = amqp_base_config();
        config.insert(AMQP_TLS_CLIENT_CERT.to_string(), "/tmp/client.crt".to_string());
        config.insert(AMQP_TLS_CLIENT_KEY.to_string(), "/tmp/client.key".to_string());

        let err = validate_amqp_config(&config, "").expect_err("TLS paths without amqps should fail");

        assert!(err.to_string().contains("only allowed with amqps"));
    }

    #[test]
    fn validate_amqp_config_rejects_ambiguous_credentials() {
        let mut config = amqp_base_config();
        config.insert(AMQP_URL.to_string(), "amqp://guest:guest@127.0.0.1:5672/%2f".to_string());
        config.insert(AMQP_USERNAME.to_string(), "user".to_string());
        config.insert(AMQP_PASSWORD.to_string(), "password".to_string());

        let err = validate_amqp_config(&config, "").expect_err("ambiguous credentials should fail");

        assert!(err.to_string().contains("either in url or username/password"));
    }

    #[test]
    fn build_kafka_args_accepts_all_ack_alias() {
        let mut config = kafka_base_config();
        config.insert(KAFKA_ACKS.to_string(), "all".to_string());

        let args = build_kafka_args(&config, "", TargetType::NotifyEvent).expect("valid kafka args");
        assert_eq!(args.acks, -1);
    }

    #[test]
    fn build_kafka_args_rejects_invalid_acks() {
        let mut config = kafka_base_config();
        config.insert(KAFKA_ACKS.to_string(), "leader".to_string());

        let err = build_kafka_args(&config, "", TargetType::NotifyEvent).expect_err("invalid acks should fail");
        assert!(err.to_string().contains("Kafka acks must be one of"));
    }

    #[test]
    fn validate_kafka_config_rejects_invalid_acks() {
        let mut config = kafka_base_config();
        config.insert(KAFKA_ACKS.to_string(), "2".to_string());

        let err = validate_kafka_config(&config, "").expect_err("invalid acks should fail");
        assert!(err.to_string().contains("Kafka acks must be one of"));
    }

    #[test]
    fn build_mysql_args_accepts_minimal_config() {
        let args = build_mysql_args(&mysql_base_config(), "", TargetType::NotifyEvent).expect("valid mysql args");
        assert!(args.enable);
        assert_eq!(args.dsn_string, "rustfs:password@tcp(127.0.0.1:3306)/rustfs_events");
        assert_eq!(args.table, "rustfs_events");
        assert_eq!(args.format, "access");
        assert_eq!(args.max_open_connections, 2);
        assert_eq!(args.queue_limit, rustfs_config::DEFAULT_LIMIT);
    }

    #[test]
    fn build_mysql_args_applies_defaults() {
        let args = build_mysql_args(&mysql_base_config(), "/custom/queue", TargetType::NotifyEvent).expect("valid mysql args");
        assert_eq!(args.queue_dir, "/custom/queue");
        assert_eq!(args.queue_limit, 100000);
        assert_eq!(args.max_open_connections, 2);
    }

    #[test]
    fn build_mysql_args_rejects_missing_dsn() {
        let mut config = KVS::new();
        config.insert(MYSQL_TABLE.to_string(), "events".to_string());

        let err = build_mysql_args(&config, "", TargetType::NotifyEvent).expect_err("missing dsn should fail");
        assert!(err.to_string().contains("dsn_string"));
    }

    #[test]
    fn build_mysql_args_rejects_relative_queue_dir() {
        let mut config = mysql_base_config();
        config.insert(MYSQL_QUEUE_DIR.to_string(), "relative/path".to_string());

        let err = build_mysql_args(&config, "", TargetType::NotifyEvent).expect_err("relative path should fail");
        assert!(err.to_string().contains("absolute"));
    }

    #[test]
    fn validate_mysql_config_rejects_invalid_max_open_connections() {
        let mut config = mysql_base_config();
        config.insert(MYSQL_MAX_OPEN_CONNECTIONS.to_string(), "not-a-number".to_string());

        let err = validate_mysql_config(&config, "").expect_err("invalid max_open_connections should be rejected");
        assert!(err.to_string().contains("max_open_connections"));
    }

    #[test]
    fn validate_mysql_config_rejects_empty_dsn() {
        let mut config = mysql_base_config();
        config.insert(MYSQL_DSN_STRING.to_string(), "".to_string());

        let err = validate_mysql_config(&config, "").expect_err("empty dsn should fail");
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn validate_mysql_config_rejects_unpaired_tls_client_fields() {
        let mut config = mysql_base_config();
        config.insert(MYSQL_TLS_CLIENT_CERT.to_string(), "/etc/ssl/mysql/client.pem".to_string());

        let err = validate_mysql_config(&config, "").expect_err("unpaired mysql TLS client cert should fail");
        assert!(err.to_string().contains("must be specified together"));
    }

    #[test]
    fn validate_mysql_config_rejects_relative_tls_paths() {
        let mut config = mysql_base_config();
        config.insert(MYSQL_TLS_CA.to_string(), "ca.pem".to_string());

        let err = validate_mysql_config(&config, "").expect_err("relative tls_ca should fail");
        assert!(err.to_string().contains("tls_ca must be an absolute path"));

        config.insert(MYSQL_TLS_CA.to_string(), "/etc/ssl/mysql/ca.pem".to_string());
        config.insert(MYSQL_TLS_CLIENT_CERT.to_string(), "client.pem".to_string());
        config.insert(MYSQL_TLS_CLIENT_KEY.to_string(), "client.key".to_string());

        let err = validate_mysql_config(&config, "").expect_err("relative tls client paths should fail");
        assert!(err.to_string().contains("absolute path"));
    }

    #[test]
    fn build_mysql_args_accepts_absolute_tls_paths() {
        let mut config = mysql_base_config();
        config.insert(MYSQL_TLS_CA.to_string(), "/etc/ssl/mysql/ca.pem".to_string());
        config.insert(MYSQL_TLS_CLIENT_CERT.to_string(), "/etc/ssl/mysql/client.pem".to_string());
        config.insert(MYSQL_TLS_CLIENT_KEY.to_string(), "/etc/ssl/mysql/client.key".to_string());

        let args = build_mysql_args(&config, "", TargetType::NotifyEvent).expect("absolute mysql TLS paths should pass");
        assert_eq!(args.tls_ca, "/etc/ssl/mysql/ca.pem");
        assert_eq!(args.tls_client_cert, "/etc/ssl/mysql/client.pem");
        assert_eq!(args.tls_client_key, "/etc/ssl/mysql/client.key");
    }

    fn redis_base_config() -> KVS {
        let mut config = KVS::new();
        config.insert(REDIS_URL.to_string(), "redis://127.0.0.1:6379/0".to_string());
        config.insert(REDIS_CHANNEL.to_string(), "events".to_string());
        config
    }
    fn postgres_base_config() -> KVS {
        let mut config = KVS::new();
        config.insert(
            POSTGRES_DSN_STRING.to_string(),
            "postgres://postgres:rustfs@localhost:5432/rustfs_events?search_path=public".to_string(),
        );
        config.insert(POSTGRES_TABLE.to_string(), "rustfs_events_namespace".to_string());
        config
    }

    #[test]
    fn build_redis_args_keeps_manager_tuning_fields_none_when_unset() {
        let config = redis_base_config();

        let args = build_redis_args(&config, "/tmp/queue", "default-channel", TargetType::NotifyEvent).expect("valid redis args");

        assert_eq!(args.channel, "events");
        assert_eq!(args.reconnect_retry_attempts, None);
        assert_eq!(args.min_retry_delay, None);
        assert_eq!(args.max_retry_delay, None);
        assert_eq!(args.connection_timeout, None);
        assert_eq!(args.response_timeout, None);
        assert_eq!(args.pipeline_buffer_size, None);
    }

    #[test]
    fn build_redis_args_uses_default_channel_when_missing() {
        let mut config = KVS::new();
        config.insert(REDIS_URL.to_string(), "redis://127.0.0.1:6379/0".to_string());

        let args =
            build_redis_args(&config, "/tmp/queue", "fallback-channel", TargetType::NotifyEvent).expect("valid redis args");

        assert_eq!(args.channel, "fallback-channel");
    }

    #[test]
    fn build_redis_args_uses_default_channel_when_empty() {
        let mut config = KVS::new();
        config.insert(REDIS_URL.to_string(), "redis://127.0.0.1:6379/0".to_string());
        config.insert(REDIS_CHANNEL.to_string(), "   ".to_string());

        let args =
            build_redis_args(&config, "/tmp/queue", "fallback-channel", TargetType::NotifyEvent).expect("valid redis args");

        assert_eq!(args.channel, "fallback-channel");
    }

    #[test]
    fn build_redis_args_parses_optional_tuning_values_when_present() {
        let mut config = redis_base_config();
        config.insert(REDIS_RECONNECT_RETRY_ATTEMPTS.to_string(), "9".to_string());
        config.insert(REDIS_MIN_RETRY_DELAY.to_string(), "250".to_string());
        config.insert(REDIS_MAX_RETRY_DELAY.to_string(), "5000".to_string());
        config.insert(REDIS_CONNECTION_TIMEOUT.to_string(), "7".to_string());
        config.insert(REDIS_RESPONSE_TIMEOUT.to_string(), "11".to_string());
        config.insert(REDIS_PIPELINE_BUFFER_SIZE.to_string(), "64".to_string());

        let args = build_redis_args(&config, "/tmp/queue", "default-channel", TargetType::NotifyEvent).expect("valid redis args");

        assert_eq!(args.reconnect_retry_attempts, Some(9));
        assert_eq!(args.min_retry_delay, Some(std::time::Duration::from_millis(250)));
        assert_eq!(args.max_retry_delay, Some(std::time::Duration::from_millis(5000)));
        assert_eq!(args.connection_timeout, Some(std::time::Duration::from_secs(7)));
        assert_eq!(args.response_timeout, Some(std::time::Duration::from_secs(11)));
        assert_eq!(args.pipeline_buffer_size, Some(64));
    }

    #[test]
    fn build_redis_args_parses_tls_allow_insecure_when_present() {
        let mut config = redis_base_config();
        config.insert(REDIS_URL.to_string(), "rediss://127.0.0.1:6379/0".to_string());
        config.insert(REDIS_TLS_ALLOW_INSECURE.to_string(), "on".to_string());

        let args = build_redis_args(&config, "/tmp/queue", "default-channel", TargetType::NotifyEvent).expect("valid redis args");

        assert!(args.tls.allow_insecure);
    }

    #[test]
    fn validate_redis_config_rejects_missing_url() {
        let config = KVS::new();

        let err = validate_redis_config(&config, "/tmp/queue", "default-channel").expect_err("missing redis url should fail");
        assert!(err.to_string().contains("Missing Redis URL"));
    }
    #[test]
    fn build_postgres_args_accepts_minimal_config() {
        let config = postgres_base_config();
        let args = build_postgres_args(&config, "", TargetType::NotifyEvent).expect("valid postgres args");
        assert_eq!(
            args.dsn_string,
            "postgres://postgres:rustfs@localhost:5432/rustfs_events?search_path=public"
        );
        assert_eq!(args.format, PostgresFormat::Namespace);
    }

    #[test]
    fn build_postgres_args_parses_access_format() {
        let mut config = postgres_base_config();
        config.insert(POSTGRES_FORMAT.to_string(), "access".to_string());
        let args = build_postgres_args(&config, "", TargetType::NotifyEvent).expect("valid postgres args");
        assert_eq!(args.format, PostgresFormat::Access);
    }

    #[test]
    fn validate_postgres_config_rejects_missing_dsn_string() {
        let mut config = postgres_base_config();
        config.0.retain(|kv| kv.key != POSTGRES_DSN_STRING);
        let err = validate_postgres_config(&config, "").expect_err("missing dsn_string should fail");
        assert!(err.to_string().contains("Missing PostgreSQL dsn_string"));
    }

    #[test]
    fn validate_postgres_config_rejects_empty_dsn_string() {
        let mut config = postgres_base_config();
        config.insert(POSTGRES_DSN_STRING.to_string(), "".to_string());
        let err = validate_postgres_config(&config, "").expect_err("empty dsn_string should fail");
        assert!(err.to_string().contains("dsn_string cannot be empty"));
    }

    #[test]
    fn validate_postgres_config_rejects_missing_table() {
        let mut config = postgres_base_config();
        config.0.retain(|kv| kv.key != POSTGRES_TABLE);
        let err = validate_postgres_config(&config, "").expect_err("missing table should fail");
        assert!(err.to_string().contains("Missing PostgreSQL table"));
    }

    #[test]
    fn validate_postgres_config_rejects_invalid_dsn() {
        let mut config = postgres_base_config();
        config.insert(POSTGRES_DSN_STRING.to_string(), "postgres://".to_string());
        let err = validate_postgres_config(&config, "").expect_err("invalid dsn should fail");
        assert!(err.to_string().contains("invalid PostgreSQL dsn_string"));
    }

    #[test]
    fn validate_postgres_config_rejects_relative_queue_dir() {
        let mut config = postgres_base_config();
        config.insert(POSTGRES_QUEUE_DIR.to_string(), "relative/path".to_string());
        let err = validate_postgres_config(&config, "").expect_err("relative queue_dir should fail");
        assert!(err.to_string().contains("absolute path"));
    }

    #[test]
    fn validate_postgres_config_rejects_mtls_without_key() {
        let mut config = postgres_base_config();
        config.insert(POSTGRES_TLS_CLIENT_CERT.to_string(), "/etc/ssl/cert.pem".to_string());
        let err = validate_postgres_config(&config, "").expect_err("missing key should fail");
        assert!(err.to_string().contains("must be specified together"));
    }

    #[test]
    fn validate_postgres_config_rejects_relative_tls_ca() {
        let mut config = postgres_base_config();
        config.insert(POSTGRES_TLS_CA.to_string(), "relative/ca.pem".to_string());
        let err = validate_postgres_config(&config, "").expect_err("relative tls_ca should fail");
        assert!(err.to_string().contains("must be an absolute path"));
    }

    #[test]
    fn validate_postgres_config_rejects_relative_tls_client_cert() {
        let mut config = postgres_base_config();
        config.insert(POSTGRES_TLS_CLIENT_CERT.to_string(), "relative/client.pem".to_string());
        config.insert(POSTGRES_TLS_CLIENT_KEY.to_string(), "/etc/ssl/client.key".to_string());
        let err = validate_postgres_config(&config, "").expect_err("relative tls_client_cert should fail");
        assert!(err.to_string().contains("must be an absolute path"));
    }
}
