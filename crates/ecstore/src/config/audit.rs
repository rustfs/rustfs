//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::config::{KV, KVS};
use rustfs_config::{
    COMMENT_KEY, DEFAULT_LIMIT, ENABLE_KEY, EVENT_DEFAULT_DIR, EnableState, KAFKA_ACKS, KAFKA_BROKERS, KAFKA_QUEUE_DIR,
    KAFKA_QUEUE_LIMIT, KAFKA_TLS_CA, KAFKA_TLS_CLIENT_CERT, KAFKA_TLS_CLIENT_KEY, KAFKA_TLS_ENABLE, KAFKA_TOPIC, MQTT_BROKER,
    MQTT_KEEP_ALIVE_INTERVAL, MQTT_PASSWORD, MQTT_QOS, MQTT_QUEUE_DIR, MQTT_QUEUE_LIMIT, MQTT_RECONNECT_INTERVAL, MQTT_TLS_CA,
    MQTT_TLS_CLIENT_CERT, MQTT_TLS_CLIENT_KEY, MQTT_TLS_POLICY, MQTT_TLS_TRUST_LEAF_AS_CA, MQTT_TOPIC, MQTT_USERNAME,
    MQTT_WS_PATH_ALLOWLIST, MYSQL_DSN_STRING, MYSQL_FORMAT, MYSQL_MAX_OPEN_CONNECTIONS, MYSQL_QUEUE_DIR, MYSQL_QUEUE_LIMIT,
    MYSQL_TABLE, MYSQL_TLS_CA, MYSQL_TLS_CLIENT_CERT, MYSQL_TLS_CLIENT_KEY, NATS_ADDRESS, NATS_CREDENTIALS_FILE, NATS_PASSWORD,
    NATS_QUEUE_DIR, NATS_QUEUE_LIMIT, NATS_SUBJECT, NATS_TLS_CA, NATS_TLS_CLIENT_CERT, NATS_TLS_CLIENT_KEY, NATS_TLS_REQUIRED,
    NATS_TOKEN, NATS_USERNAME, POSTGRES_DSN_STRING, POSTGRES_FORMAT, POSTGRES_QUEUE_DIR, POSTGRES_QUEUE_LIMIT, POSTGRES_TABLE,
    POSTGRES_TLS_CA, POSTGRES_TLS_CLIENT_CERT, POSTGRES_TLS_CLIENT_KEY, POSTGRES_TLS_REQUIRED, PULSAR_AUTH_TOKEN, PULSAR_BROKER,
    PULSAR_PASSWORD, PULSAR_QUEUE_DIR, PULSAR_QUEUE_LIMIT, PULSAR_TLS_ALLOW_INSECURE, PULSAR_TLS_CA,
    PULSAR_TLS_HOSTNAME_VERIFICATION, PULSAR_TOPIC, PULSAR_USERNAME, REDIS_CHANNEL, REDIS_CONNECTION_TIMEOUT,
    REDIS_KEEP_ALIVE_INTERVAL, REDIS_MAX_RETRY_ATTEMPTS, REDIS_MAX_RETRY_DELAY, REDIS_MIN_RETRY_DELAY, REDIS_PASSWORD,
    REDIS_PIPELINE_BUFFER_SIZE, REDIS_QUEUE_DIR, REDIS_QUEUE_LIMIT, REDIS_RECONNECT_RETRY_ATTEMPTS, REDIS_RESPONSE_TIMEOUT,
    REDIS_TLS_ALLOW_INSECURE, REDIS_TLS_CA, REDIS_TLS_CLIENT_CERT, REDIS_TLS_CLIENT_KEY, REDIS_TLS_POLICY, REDIS_URL,
    REDIS_USERNAME, WEBHOOK_AUTH_TOKEN, WEBHOOK_BATCH_SIZE, WEBHOOK_CLIENT_CA, WEBHOOK_CLIENT_CERT, WEBHOOK_CLIENT_KEY,
    WEBHOOK_ENDPOINT, WEBHOOK_HTTP_TIMEOUT, WEBHOOK_MAX_RETRY, WEBHOOK_QUEUE_DIR, WEBHOOK_QUEUE_LIMIT, WEBHOOK_RETRY_INTERVAL,
    WEBHOOK_SKIP_TLS_VERIFY,
};
use std::sync::LazyLock;

#[allow(dead_code)]
#[allow(clippy::declare_interior_mutable_const)]
/// Default KVS for audit webhook settings.
pub static DEFAULT_AUDIT_WEBHOOK_KVS: LazyLock<KVS> = LazyLock::new(|| {
    KVS(vec![
        KV {
            key: ENABLE_KEY.to_owned(),
            value: EnableState::Off.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: WEBHOOK_ENDPOINT.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: WEBHOOK_AUTH_TOKEN.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: WEBHOOK_CLIENT_CERT.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: WEBHOOK_CLIENT_KEY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: WEBHOOK_CLIENT_CA.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: WEBHOOK_SKIP_TLS_VERIFY.to_owned(),
            value: EnableState::Off.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: WEBHOOK_BATCH_SIZE.to_owned(),
            value: "1".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: WEBHOOK_QUEUE_LIMIT.to_owned(),
            value: DEFAULT_LIMIT.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: WEBHOOK_QUEUE_DIR.to_owned(),
            value: EVENT_DEFAULT_DIR.to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: WEBHOOK_MAX_RETRY.to_owned(),
            value: "0".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: WEBHOOK_RETRY_INTERVAL.to_owned(),
            value: "3s".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: WEBHOOK_HTTP_TIMEOUT.to_owned(),
            value: "5s".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: COMMENT_KEY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
    ])
});

#[allow(dead_code)]
#[allow(clippy::declare_interior_mutable_const)]
/// Default KVS for audit MQTT settings.
pub static DEFAULT_AUDIT_MQTT_KVS: LazyLock<KVS> = LazyLock::new(|| {
    KVS(vec![
        KV {
            key: ENABLE_KEY.to_owned(),
            value: EnableState::Off.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: MQTT_BROKER.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: MQTT_TOPIC.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: MQTT_USERNAME.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: MQTT_PASSWORD.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true, // Sensitive field
        },
        KV {
            key: MQTT_QOS.to_owned(),
            value: "1".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: MQTT_KEEP_ALIVE_INTERVAL.to_owned(),
            value: "60s".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: MQTT_RECONNECT_INTERVAL.to_owned(),
            value: "5s".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: MQTT_QUEUE_DIR.to_owned(),
            value: EVENT_DEFAULT_DIR.to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: MQTT_QUEUE_LIMIT.to_owned(),
            value: DEFAULT_LIMIT.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: MQTT_TLS_POLICY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: MQTT_TLS_CA.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: MQTT_TLS_CLIENT_CERT.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: MQTT_TLS_CLIENT_KEY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: MQTT_TLS_TRUST_LEAF_AS_CA.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: MQTT_WS_PATH_ALLOWLIST.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: COMMENT_KEY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
    ])
});

pub static DEFAULT_AUDIT_NATS_KVS: LazyLock<KVS> = LazyLock::new(|| {
    KVS(vec![
        KV {
            key: ENABLE_KEY.to_owned(),
            value: EnableState::Off.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: NATS_ADDRESS.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: NATS_SUBJECT.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: NATS_USERNAME.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: NATS_PASSWORD.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: NATS_TOKEN.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: NATS_CREDENTIALS_FILE.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: NATS_TLS_CA.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: NATS_TLS_CLIENT_CERT.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: NATS_TLS_CLIENT_KEY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: NATS_TLS_REQUIRED.to_owned(),
            value: EnableState::Off.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: NATS_QUEUE_DIR.to_owned(),
            value: EVENT_DEFAULT_DIR.to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: NATS_QUEUE_LIMIT.to_owned(),
            value: DEFAULT_LIMIT.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: COMMENT_KEY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
    ])
});

#[allow(dead_code)]
pub static DEFAULT_AUDIT_PULSAR_KVS: LazyLock<KVS> = LazyLock::new(|| {
    KVS(vec![
        KV {
            key: ENABLE_KEY.to_owned(),
            value: EnableState::Off.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: PULSAR_BROKER.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: PULSAR_TOPIC.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: PULSAR_AUTH_TOKEN.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: PULSAR_USERNAME.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: PULSAR_PASSWORD.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: PULSAR_TLS_CA.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: PULSAR_TLS_ALLOW_INSECURE.to_owned(),
            value: EnableState::Off.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: PULSAR_TLS_HOSTNAME_VERIFICATION.to_owned(),
            value: EnableState::On.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: PULSAR_QUEUE_DIR.to_owned(),
            value: EVENT_DEFAULT_DIR.to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: PULSAR_QUEUE_LIMIT.to_owned(),
            value: DEFAULT_LIMIT.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: COMMENT_KEY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
    ])
});

#[allow(dead_code)]
pub static DEFAULT_AUDIT_REDIS_KVS: LazyLock<KVS> = LazyLock::new(|| {
    KVS(vec![
        KV {
            key: ENABLE_KEY.to_owned(),
            value: EnableState::Off.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: REDIS_URL.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: REDIS_CHANNEL.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: REDIS_USERNAME.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: REDIS_PASSWORD.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: REDIS_KEEP_ALIVE_INTERVAL.to_owned(),
            value: "15".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: REDIS_QUEUE_DIR.to_owned(),
            value: EVENT_DEFAULT_DIR.to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: REDIS_QUEUE_LIMIT.to_owned(),
            value: DEFAULT_LIMIT.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: REDIS_MAX_RETRY_ATTEMPTS.to_owned(),
            value: "3".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: REDIS_RECONNECT_RETRY_ATTEMPTS.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: REDIS_MIN_RETRY_DELAY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: REDIS_MAX_RETRY_DELAY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: REDIS_CONNECTION_TIMEOUT.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: REDIS_RESPONSE_TIMEOUT.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: REDIS_PIPELINE_BUFFER_SIZE.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: REDIS_TLS_POLICY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: REDIS_TLS_CA.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: REDIS_TLS_CLIENT_CERT.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: REDIS_TLS_CLIENT_KEY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: REDIS_TLS_ALLOW_INSECURE.to_owned(),
            value: EnableState::Off.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: COMMENT_KEY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
    ])
});

pub static DEFAULT_AUDIT_POSTGRES_KVS: LazyLock<KVS> = LazyLock::new(|| {
    KVS(vec![
        KV {
            key: ENABLE_KEY.to_owned(),
            value: EnableState::Off.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: POSTGRES_DSN_STRING.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: POSTGRES_TABLE.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: POSTGRES_FORMAT.to_owned(),
            value: "namespace".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: POSTGRES_TLS_REQUIRED.to_owned(),
            value: EnableState::Off.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: POSTGRES_TLS_CA.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: POSTGRES_TLS_CLIENT_CERT.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: POSTGRES_TLS_CLIENT_KEY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: POSTGRES_QUEUE_DIR.to_owned(),
            value: EVENT_DEFAULT_DIR.to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: POSTGRES_QUEUE_LIMIT.to_owned(),
            value: DEFAULT_LIMIT.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: COMMENT_KEY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
    ])
});

pub static DEFAULT_AUDIT_KAFKA_KVS: LazyLock<KVS> = LazyLock::new(|| {
    KVS(vec![
        KV {
            key: ENABLE_KEY.to_owned(),
            value: EnableState::Off.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: KAFKA_BROKERS.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: KAFKA_TOPIC.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: KAFKA_ACKS.to_owned(),
            value: "1".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: KAFKA_TLS_ENABLE.to_owned(),
            value: EnableState::Off.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: KAFKA_TLS_CA.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: KAFKA_TLS_CLIENT_CERT.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: KAFKA_TLS_CLIENT_KEY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: KAFKA_QUEUE_DIR.to_owned(),
            value: EVENT_DEFAULT_DIR.to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: KAFKA_QUEUE_LIMIT.to_owned(),
            value: DEFAULT_LIMIT.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: COMMENT_KEY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
    ])
});

pub static DEFAULT_AUDIT_MYSQL_KVS: LazyLock<KVS> = LazyLock::new(|| {
    KVS(vec![
        KV {
            key: ENABLE_KEY.to_owned(),
            value: EnableState::Off.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: MYSQL_DSN_STRING.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: MYSQL_TABLE.to_owned(),
            value: "rustfs_audit_logs".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: MYSQL_FORMAT.to_owned(),
            value: "access".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: MYSQL_TLS_CA.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: MYSQL_TLS_CLIENT_CERT.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: MYSQL_TLS_CLIENT_KEY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: MYSQL_QUEUE_DIR.to_owned(),
            value: EVENT_DEFAULT_DIR.to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: MYSQL_QUEUE_LIMIT.to_owned(),
            value: DEFAULT_LIMIT.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: MYSQL_MAX_OPEN_CONNECTIONS.to_owned(),
            value: "2".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: COMMENT_KEY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
    ])
});
