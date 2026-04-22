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
    kafka::KafkaArgs,
    mqtt::{MQTTArgs, MQTTTlsConfig, validate_mqtt_broker_url},
    nats::{NATSArgs, validate_nats_address},
    pulsar::{PulsarArgs, validate_pulsar_broker},
    webhook::WebhookArgs,
};
use rumqttc::QoS;
use rustfs_config::{
    DEFAULT_LIMIT, KAFKA_ACKS, KAFKA_BROKERS, KAFKA_QUEUE_DIR, KAFKA_QUEUE_LIMIT, KAFKA_TLS_CA, KAFKA_TLS_CLIENT_CERT,
    KAFKA_TLS_CLIENT_KEY, KAFKA_TLS_ENABLE, KAFKA_TOPIC, MQTT_BROKER, MQTT_KEEP_ALIVE_INTERVAL, MQTT_PASSWORD, MQTT_QOS,
    MQTT_QUEUE_DIR, MQTT_QUEUE_LIMIT, MQTT_RECONNECT_INTERVAL, MQTT_TLS_CA, MQTT_TLS_CLIENT_CERT, MQTT_TLS_CLIENT_KEY,
    MQTT_TLS_POLICY, MQTT_TLS_TRUST_LEAF_AS_CA, MQTT_TOPIC, MQTT_USERNAME, MQTT_WS_PATH_ALLOWLIST, NATS_ADDRESS,
    NATS_CREDENTIALS_FILE, NATS_PASSWORD, NATS_QUEUE_DIR, NATS_QUEUE_LIMIT, NATS_SUBJECT, NATS_TLS_CA, NATS_TLS_CLIENT_CERT,
    NATS_TLS_CLIENT_KEY, NATS_TLS_REQUIRED, NATS_TOKEN, NATS_USERNAME, PULSAR_AUTH_TOKEN, PULSAR_BROKER, PULSAR_PASSWORD,
    PULSAR_QUEUE_DIR, PULSAR_QUEUE_LIMIT, PULSAR_TLS_ALLOW_INSECURE, PULSAR_TLS_CA, PULSAR_TLS_HOSTNAME_VERIFICATION,
    PULSAR_TOPIC, PULSAR_USERNAME, RUSTFS_WEBHOOK_SKIP_TLS_VERIFY_DEFAULT, WEBHOOK_AUTH_TOKEN, WEBHOOK_CLIENT_CA,
    WEBHOOK_CLIENT_CERT, WEBHOOK_CLIENT_KEY, WEBHOOK_ENDPOINT, WEBHOOK_QUEUE_DIR, WEBHOOK_QUEUE_LIMIT, WEBHOOK_SKIP_TLS_VERIFY,
};
use rustfs_ecstore::config::KVS;
use std::path::Path;
use std::time::Duration;

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
        acks: config.lookup(KAFKA_ACKS).and_then(|v| v.parse::<i16>().ok()).unwrap_or(1),
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
    if !queue_dir.is_empty() && !std::path::Path::new(&queue_dir).is_absolute() {
        return Err(TargetError::Configuration("Kafka queue directory must be an absolute path".to_string()));
    }

    Ok(())
}
