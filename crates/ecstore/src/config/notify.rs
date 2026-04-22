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

use crate::config::{KV, KVS};
use rustfs_config::{
    COMMENT_KEY, DEFAULT_LIMIT, ENABLE_KEY, EVENT_DEFAULT_DIR, EnableState, KAFKA_ACKS, KAFKA_BROKERS, KAFKA_QUEUE_DIR,
    KAFKA_QUEUE_LIMIT, KAFKA_TLS_CA, KAFKA_TLS_CLIENT_CERT, KAFKA_TLS_CLIENT_KEY, KAFKA_TLS_ENABLE, KAFKA_TOPIC, MQTT_BROKER,
    MQTT_KEEP_ALIVE_INTERVAL, MQTT_PASSWORD, MQTT_QOS, MQTT_QUEUE_DIR, MQTT_QUEUE_LIMIT, MQTT_RECONNECT_INTERVAL, MQTT_TLS_CA,
    MQTT_TLS_CLIENT_CERT, MQTT_TLS_CLIENT_KEY, MQTT_TLS_POLICY, MQTT_TLS_TRUST_LEAF_AS_CA, MQTT_TOPIC, MQTT_USERNAME,
    MQTT_WS_PATH_ALLOWLIST, NATS_ADDRESS, NATS_CREDENTIALS_FILE, NATS_PASSWORD, NATS_QUEUE_DIR, NATS_QUEUE_LIMIT, NATS_SUBJECT,
    NATS_TLS_CA, NATS_TLS_CLIENT_CERT, NATS_TLS_CLIENT_KEY, NATS_TLS_REQUIRED, NATS_TOKEN, NATS_USERNAME, PULSAR_AUTH_TOKEN,
    PULSAR_BROKER, PULSAR_PASSWORD, PULSAR_QUEUE_DIR, PULSAR_QUEUE_LIMIT, PULSAR_TLS_ALLOW_INSECURE, PULSAR_TLS_CA,
    PULSAR_TLS_HOSTNAME_VERIFICATION, PULSAR_TOPIC, PULSAR_USERNAME, WEBHOOK_AUTH_TOKEN, WEBHOOK_CLIENT_CA, WEBHOOK_CLIENT_CERT,
    WEBHOOK_CLIENT_KEY, WEBHOOK_ENDPOINT, WEBHOOK_QUEUE_DIR, WEBHOOK_QUEUE_LIMIT, WEBHOOK_SKIP_TLS_VERIFY,
};
use std::sync::LazyLock;

/// The default configuration collection of webhooks，
/// Initialized only once during the program life cycle, enabling high-performance lazy loading.
pub static DEFAULT_NOTIFY_WEBHOOK_KVS: LazyLock<KVS> = LazyLock::new(|| {
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
        // Sensitive information such as authentication tokens is hidden when the value is empty, enhancing security
        KV {
            key: WEBHOOK_AUTH_TOKEN.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
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
            key: COMMENT_KEY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
    ])
});

/// MQTT's default configuration collection
pub static DEFAULT_NOTIFY_MQTT_KVS: LazyLock<KVS> = LazyLock::new(|| {
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
        // Sensitive information such as passwords are hidden when the value is empty
        KV {
            key: MQTT_PASSWORD.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: MQTT_USERNAME.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: MQTT_QOS.to_owned(),
            value: "0".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: MQTT_KEEP_ALIVE_INTERVAL.to_owned(),
            value: "0s".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: MQTT_RECONNECT_INTERVAL.to_owned(),
            value: "0s".to_owned(),
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

pub static DEFAULT_NOTIFY_NATS_KVS: LazyLock<KVS> = LazyLock::new(|| {
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

pub static DEFAULT_NOTIFY_PULSAR_KVS: LazyLock<KVS> = LazyLock::new(|| {
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

pub static DEFAULT_NOTIFY_KAFKA_KVS: LazyLock<KVS> = LazyLock::new(|| {
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
