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
    COMMENT_KEY, DEFAULT_LIMIT, ENABLE_KEY, EVENT_DEFAULT_DIR, EnableState, MQTT_BROKER, MQTT_KEEP_ALIVE_INTERVAL, MQTT_PASSWORD,
    MQTT_QOS, MQTT_QUEUE_DIR, MQTT_QUEUE_LIMIT, MQTT_RECONNECT_INTERVAL, MQTT_TLS_CA, MQTT_TLS_CLIENT_CERT, MQTT_TLS_CLIENT_KEY,
    MQTT_TLS_POLICY, MQTT_TLS_TRUST_LEAF_AS_CA, MQTT_TOPIC, MQTT_USERNAME, MQTT_WS_PATH_ALLOWLIST, NATS_ADDRESS,
    NATS_CREDENTIALS_FILE, NATS_PASSWORD, NATS_QUEUE_DIR, NATS_QUEUE_LIMIT, NATS_SUBJECT, NATS_TLS_CA, NATS_TLS_CLIENT_CERT,
    NATS_TLS_CLIENT_KEY, NATS_TLS_REQUIRED, NATS_TOKEN, NATS_USERNAME, PULSAR_AUTH_TOKEN, PULSAR_BROKER, PULSAR_PASSWORD,
    PULSAR_QUEUE_DIR, PULSAR_QUEUE_LIMIT, PULSAR_TLS_ALLOW_INSECURE, PULSAR_TLS_CA, PULSAR_TLS_HOSTNAME_VERIFICATION,
    PULSAR_TOPIC, PULSAR_USERNAME, WEBHOOK_AUTH_TOKEN, WEBHOOK_BATCH_SIZE, WEBHOOK_CLIENT_CA, WEBHOOK_CLIENT_CERT,
    WEBHOOK_CLIENT_KEY, WEBHOOK_ENDPOINT, WEBHOOK_HTTP_TIMEOUT, WEBHOOK_MAX_RETRY, WEBHOOK_QUEUE_DIR, WEBHOOK_QUEUE_LIMIT,
    WEBHOOK_RETRY_INTERVAL, WEBHOOK_SKIP_TLS_VERIFY,
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

#[allow(dead_code)]
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
