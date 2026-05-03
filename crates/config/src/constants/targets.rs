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

pub const WEBHOOK_ENDPOINT: &str = "endpoint";
pub const WEBHOOK_AUTH_TOKEN: &str = "auth_token";
pub const WEBHOOK_CLIENT_CERT: &str = "client_cert";
pub const WEBHOOK_CLIENT_KEY: &str = "client_key";
pub const WEBHOOK_CLIENT_CA: &str = "client_ca";
pub const WEBHOOK_SKIP_TLS_VERIFY: &str = "skip_tls_verify";
pub const WEBHOOK_BATCH_SIZE: &str = "batch_size";
pub const WEBHOOK_QUEUE_LIMIT: &str = "queue_limit";
pub const WEBHOOK_QUEUE_DIR: &str = "queue_dir";
pub const WEBHOOK_MAX_RETRY: &str = "max_retry";
pub const WEBHOOK_RETRY_INTERVAL: &str = "retry_interval";
pub const WEBHOOK_HTTP_TIMEOUT: &str = "http_timeout";

pub const MQTT_BROKER: &str = "broker";
pub const MQTT_TOPIC: &str = "topic";
pub const MQTT_QOS: &str = "qos";
pub const MQTT_USERNAME: &str = "username";
pub const MQTT_PASSWORD: &str = "password";
pub const MQTT_RECONNECT_INTERVAL: &str = "reconnect_interval";
pub const MQTT_KEEP_ALIVE_INTERVAL: &str = "keep_alive_interval";
pub const MQTT_QUEUE_DIR: &str = "queue_dir";
pub const MQTT_QUEUE_LIMIT: &str = "queue_limit";
pub const MQTT_TLS_POLICY: &str = "tls_policy";
pub const MQTT_TLS_CA: &str = "tls_ca";
pub const MQTT_TLS_CLIENT_CERT: &str = "tls_client_cert";
pub const MQTT_TLS_CLIENT_KEY: &str = "tls_client_key";
pub const MQTT_TLS_TRUST_LEAF_AS_CA: &str = "tls_trust_leaf_as_ca";
pub const MQTT_WS_PATH_ALLOWLIST: &str = "ws_path_allowlist";
pub const KAFKA_BROKERS: &str = "brokers";
pub const KAFKA_TOPIC: &str = "topic";
pub const KAFKA_ACKS: &str = "acks";
pub const KAFKA_QUEUE_DIR: &str = "queue_dir";
pub const KAFKA_QUEUE_LIMIT: &str = "queue_limit";
pub const KAFKA_TLS_ENABLE: &str = "tls_enable";
pub const KAFKA_TLS_CA: &str = "tls_ca";
pub const KAFKA_TLS_CLIENT_CERT: &str = "tls_client_cert";
pub const KAFKA_TLS_CLIENT_KEY: &str = "tls_client_key";

pub const NATS_ADDRESS: &str = "address";
pub const NATS_SUBJECT: &str = "subject";
pub const NATS_USERNAME: &str = "username";
pub const NATS_PASSWORD: &str = "password";
pub const NATS_TOKEN: &str = "token";
pub const NATS_CREDENTIALS_FILE: &str = "credentials_file";
pub const NATS_TLS_CA: &str = "tls_ca";
pub const NATS_TLS_CLIENT_CERT: &str = "tls_client_cert";
pub const NATS_TLS_CLIENT_KEY: &str = "tls_client_key";
pub const NATS_TLS_REQUIRED: &str = "tls_required";
pub const NATS_QUEUE_DIR: &str = "queue_dir";
pub const NATS_QUEUE_LIMIT: &str = "queue_limit";

pub const PULSAR_BROKER: &str = "broker";
pub const PULSAR_TOPIC: &str = "topic";
pub const PULSAR_AUTH_TOKEN: &str = "auth_token";
pub const PULSAR_USERNAME: &str = "username";
pub const PULSAR_PASSWORD: &str = "password";
pub const PULSAR_TLS_CA: &str = "tls_ca";
pub const PULSAR_TLS_ALLOW_INSECURE: &str = "tls_allow_insecure";
pub const PULSAR_TLS_HOSTNAME_VERIFICATION: &str = "tls_hostname_verification";
pub const PULSAR_QUEUE_DIR: &str = "queue_dir";
pub const PULSAR_QUEUE_LIMIT: &str = "queue_limit";

pub const MYSQL_DSN_STRING: &str = "dsn_string";
pub const MYSQL_TABLE: &str = "table";
pub const MYSQL_QUEUE_DIR: &str = "queue_dir";
pub const MYSQL_QUEUE_LIMIT: &str = "queue_limit";
pub const MYSQL_MAX_OPEN_CONNECTIONS: &str = "max_open_connections";

/// Environment variable controlling whether target queue files are Snappy-compressed.
/// Applies to both notify and audit target queue stores.
pub const ENV_TARGET_STORE_COMPRESS: &str = "RUSTFS_TARGET_STORE_COMPRESS";

/// Queue-store compression is enabled by default to reduce disk footprint.
pub const DEFAULT_TARGET_STORE_COMPRESS: bool = true;
