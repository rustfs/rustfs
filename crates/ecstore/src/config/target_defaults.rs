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

//! Shared default KVS tables for delivery targets that audit and notify declare identically.
//!
//! The audit and notify subsystems register one default KVS per delivery target. For amqp, nats,
//! pulsar, postgres and kafka both sides declare byte-identical tables; for redis and mysql they
//! differ only in a single default literal, which the caller passes in.
//!
//! Webhook and mqtt are deliberately absent: audit's webhook table carries extra batching/retry
//! keys and both tables disagree on key order and on several defaults (mqtt qos, keep-alive and
//! reconnect intervals), so they are real behavioral forks, not duplication.
//!
//! Key order is part of the contract: it drives the order admin config output lists the keys in,
//! so every constructor reproduces the existing order exactly.

use rustfs_config::server_config::{KV, KVS};
use rustfs_config::{
    AMQP_EXCHANGE, AMQP_MANDATORY, AMQP_PASSWORD, AMQP_PERSISTENT, AMQP_QUEUE_DIR, AMQP_QUEUE_LIMIT, AMQP_ROUTING_KEY,
    AMQP_TLS_CA, AMQP_TLS_CLIENT_CERT, AMQP_TLS_CLIENT_KEY, AMQP_URL, AMQP_USERNAME, COMMENT_KEY, DEFAULT_LIMIT, ENABLE_KEY,
    EVENT_DEFAULT_DIR, EnableState, KAFKA_ACKS, KAFKA_BROKERS, KAFKA_QUEUE_DIR, KAFKA_QUEUE_LIMIT, KAFKA_SASL_ENABLE,
    KAFKA_SASL_MECHANISM, KAFKA_SASL_PASSWORD, KAFKA_SASL_USERNAME, KAFKA_TLS_CA, KAFKA_TLS_CLIENT_CERT, KAFKA_TLS_CLIENT_KEY,
    KAFKA_TLS_ENABLE, KAFKA_TOPIC, MYSQL_DSN_STRING, MYSQL_FORMAT, MYSQL_MAX_OPEN_CONNECTIONS, MYSQL_QUEUE_DIR,
    MYSQL_QUEUE_LIMIT, MYSQL_TABLE, MYSQL_TLS_CA, MYSQL_TLS_CLIENT_CERT, MYSQL_TLS_CLIENT_KEY, NATS_ADDRESS,
    NATS_CREDENTIALS_FILE, NATS_JETSTREAM_ACK_TIMEOUT_DEFAULT_SECS, NATS_JETSTREAM_ACK_TIMEOUT_SECS, NATS_JETSTREAM_ENABLE,
    NATS_JETSTREAM_STREAM_NAME, NATS_PASSWORD, NATS_QUEUE_DIR, NATS_QUEUE_LIMIT, NATS_SUBJECT, NATS_TLS_CA, NATS_TLS_CLIENT_CERT,
    NATS_TLS_CLIENT_KEY, NATS_TLS_REQUIRED, NATS_TOKEN, NATS_USERNAME, POSTGRES_DSN_STRING, POSTGRES_FORMAT, POSTGRES_QUEUE_DIR,
    POSTGRES_QUEUE_LIMIT, POSTGRES_TABLE, POSTGRES_TLS_CA, POSTGRES_TLS_CLIENT_CERT, POSTGRES_TLS_CLIENT_KEY,
    POSTGRES_TLS_REQUIRED, PULSAR_AUTH_TOKEN, PULSAR_BROKER, PULSAR_PASSWORD, PULSAR_QUEUE_DIR, PULSAR_QUEUE_LIMIT,
    PULSAR_TLS_ALLOW_INSECURE, PULSAR_TLS_CA, PULSAR_TLS_HOSTNAME_VERIFICATION, PULSAR_TOPIC, PULSAR_USERNAME, REDIS_CHANNEL,
    REDIS_CONNECTION_TIMEOUT, REDIS_KEEP_ALIVE_INTERVAL, REDIS_MAX_RETRY_ATTEMPTS, REDIS_MAX_RETRY_DELAY, REDIS_MIN_RETRY_DELAY,
    REDIS_PASSWORD, REDIS_PIPELINE_BUFFER_SIZE, REDIS_QUEUE_DIR, REDIS_QUEUE_LIMIT, REDIS_RECONNECT_RETRY_ATTEMPTS,
    REDIS_RESPONSE_TIMEOUT, REDIS_TLS_ALLOW_INSECURE, REDIS_TLS_CA, REDIS_TLS_CLIENT_CERT, REDIS_TLS_CLIENT_KEY,
    REDIS_TLS_POLICY, REDIS_URL, REDIS_USERNAME,
};

/// Builds one default entry. `hidden_if_empty` marks values the admin API elides when unset.
fn kv(key: &str, value: impl Into<String>, hidden_if_empty: bool) -> KV {
    KV {
        key: key.to_owned(),
        value: value.into(),
        hidden_if_empty,
    }
}

/// Default KVS for the amqp delivery target.
// Unused until the audit/notify tables are migrated onto these constructors.
#[allow(dead_code)]
pub fn amqp_kvs() -> KVS {
    KVS(vec![
        kv(ENABLE_KEY, EnableState::Off.to_string(), false),
        kv(AMQP_URL, "", false),
        kv(AMQP_EXCHANGE, "", false),
        kv(AMQP_ROUTING_KEY, "", false),
        kv(AMQP_MANDATORY, EnableState::Off.to_string(), false),
        kv(AMQP_PERSISTENT, EnableState::On.to_string(), false),
        kv(AMQP_USERNAME, "", false),
        kv(AMQP_PASSWORD, "", true),
        kv(AMQP_TLS_CA, "", true),
        kv(AMQP_TLS_CLIENT_CERT, "", true),
        kv(AMQP_TLS_CLIENT_KEY, "", true),
        kv(AMQP_QUEUE_DIR, EVENT_DEFAULT_DIR, false),
        kv(AMQP_QUEUE_LIMIT, DEFAULT_LIMIT.to_string(), false),
        kv(COMMENT_KEY, "", false),
    ])
}

/// Default KVS for the nats delivery target.
// Unused until the audit/notify tables are migrated onto these constructors.
#[allow(dead_code)]
pub fn nats_kvs() -> KVS {
    KVS(vec![
        kv(ENABLE_KEY, EnableState::Off.to_string(), false),
        kv(NATS_ADDRESS, "", false),
        kv(NATS_SUBJECT, "", false),
        kv(NATS_USERNAME, "", false),
        kv(NATS_PASSWORD, "", true),
        kv(NATS_TOKEN, "", true),
        kv(NATS_CREDENTIALS_FILE, "", true),
        kv(NATS_TLS_CA, "", true),
        kv(NATS_TLS_CLIENT_CERT, "", true),
        kv(NATS_TLS_CLIENT_KEY, "", true),
        kv(NATS_TLS_REQUIRED, EnableState::Off.to_string(), false),
        kv(NATS_QUEUE_DIR, EVENT_DEFAULT_DIR, false),
        kv(NATS_QUEUE_LIMIT, DEFAULT_LIMIT.to_string(), false),
        kv(NATS_JETSTREAM_ENABLE, EnableState::Off.to_string(), false),
        kv(NATS_JETSTREAM_STREAM_NAME, "", false),
        kv(
            NATS_JETSTREAM_ACK_TIMEOUT_SECS,
            NATS_JETSTREAM_ACK_TIMEOUT_DEFAULT_SECS.to_string(),
            false,
        ),
        kv(COMMENT_KEY, "", false),
    ])
}

/// Default KVS for the pulsar delivery target.
// Unused until the audit/notify tables are migrated onto these constructors.
#[allow(dead_code)]
pub fn pulsar_kvs() -> KVS {
    KVS(vec![
        kv(ENABLE_KEY, EnableState::Off.to_string(), false),
        kv(PULSAR_BROKER, "", false),
        kv(PULSAR_TOPIC, "", false),
        kv(PULSAR_AUTH_TOKEN, "", true),
        kv(PULSAR_USERNAME, "", false),
        kv(PULSAR_PASSWORD, "", true),
        kv(PULSAR_TLS_CA, "", true),
        kv(PULSAR_TLS_ALLOW_INSECURE, EnableState::Off.to_string(), false),
        kv(PULSAR_TLS_HOSTNAME_VERIFICATION, EnableState::On.to_string(), false),
        kv(PULSAR_QUEUE_DIR, EVENT_DEFAULT_DIR, false),
        kv(PULSAR_QUEUE_LIMIT, DEFAULT_LIMIT.to_string(), false),
        kv(COMMENT_KEY, "", false),
    ])
}

/// Default KVS for the postgres delivery target.
// Unused until the audit/notify tables are migrated onto these constructors.
#[allow(dead_code)]
pub fn postgres_kvs() -> KVS {
    KVS(vec![
        kv(ENABLE_KEY, EnableState::Off.to_string(), false),
        kv(POSTGRES_DSN_STRING, "", true),
        kv(POSTGRES_TABLE, "", false),
        kv(POSTGRES_FORMAT, "namespace", false),
        kv(POSTGRES_TLS_REQUIRED, EnableState::Off.to_string(), false),
        kv(POSTGRES_TLS_CA, "", true),
        kv(POSTGRES_TLS_CLIENT_CERT, "", true),
        kv(POSTGRES_TLS_CLIENT_KEY, "", true),
        kv(POSTGRES_QUEUE_DIR, EVENT_DEFAULT_DIR, false),
        kv(POSTGRES_QUEUE_LIMIT, DEFAULT_LIMIT.to_string(), false),
        kv(COMMENT_KEY, "", false),
    ])
}

/// Default KVS for the kafka delivery target.
// Unused until the audit/notify tables are migrated onto these constructors.
#[allow(dead_code)]
pub fn kafka_kvs() -> KVS {
    KVS(vec![
        kv(ENABLE_KEY, EnableState::Off.to_string(), false),
        kv(KAFKA_BROKERS, "", false),
        kv(KAFKA_TOPIC, "", false),
        kv(KAFKA_ACKS, "1", false),
        kv(KAFKA_TLS_ENABLE, EnableState::Off.to_string(), false),
        kv(KAFKA_TLS_CA, "", true),
        kv(KAFKA_TLS_CLIENT_CERT, "", true),
        kv(KAFKA_TLS_CLIENT_KEY, "", true),
        kv(KAFKA_SASL_ENABLE, EnableState::Off.to_string(), false),
        kv(KAFKA_SASL_MECHANISM, "", false),
        kv(KAFKA_SASL_USERNAME, "", false),
        kv(KAFKA_SASL_PASSWORD, "", true),
        kv(KAFKA_QUEUE_DIR, EVENT_DEFAULT_DIR, false),
        kv(KAFKA_QUEUE_LIMIT, DEFAULT_LIMIT.to_string(), false),
        kv(COMMENT_KEY, "", false),
    ])
}

/// Default KVS for the redis delivery target. `channel` is the subsystem's default pub/sub channel,
/// which is the only value audit and notify disagree on.
// Unused until the audit/notify tables are migrated onto these constructors.
#[allow(dead_code)]
pub fn redis_kvs(channel: &str) -> KVS {
    KVS(vec![
        kv(ENABLE_KEY, EnableState::Off.to_string(), false),
        kv(REDIS_URL, "", false),
        kv(REDIS_CHANNEL, channel, false),
        kv(REDIS_USERNAME, "", false),
        kv(REDIS_PASSWORD, "", true),
        kv(REDIS_KEEP_ALIVE_INTERVAL, "15", false),
        kv(REDIS_QUEUE_DIR, EVENT_DEFAULT_DIR, false),
        kv(REDIS_QUEUE_LIMIT, DEFAULT_LIMIT.to_string(), false),
        kv(REDIS_MAX_RETRY_ATTEMPTS, "3", false),
        kv(REDIS_RECONNECT_RETRY_ATTEMPTS, "", false),
        kv(REDIS_MIN_RETRY_DELAY, "", false),
        kv(REDIS_MAX_RETRY_DELAY, "", false),
        kv(REDIS_CONNECTION_TIMEOUT, "", false),
        kv(REDIS_RESPONSE_TIMEOUT, "", false),
        kv(REDIS_PIPELINE_BUFFER_SIZE, "", false),
        kv(REDIS_TLS_POLICY, "", true),
        kv(REDIS_TLS_CA, "", true),
        kv(REDIS_TLS_CLIENT_CERT, "", true),
        kv(REDIS_TLS_CLIENT_KEY, "", true),
        kv(REDIS_TLS_ALLOW_INSECURE, EnableState::Off.to_string(), false),
        kv(COMMENT_KEY, "", false),
    ])
}

/// Default KVS for the mysql delivery target. `table` is the subsystem's default destination table,
/// which is the only value audit and notify disagree on.
// Unused until the audit/notify tables are migrated onto these constructors.
#[allow(dead_code)]
pub fn mysql_kvs(table: &str) -> KVS {
    KVS(vec![
        kv(ENABLE_KEY, EnableState::Off.to_string(), false),
        kv(MYSQL_DSN_STRING, "", true),
        kv(MYSQL_TABLE, table, false),
        kv(MYSQL_FORMAT, "access", false),
        kv(MYSQL_TLS_CA, "", true),
        kv(MYSQL_TLS_CLIENT_CERT, "", true),
        kv(MYSQL_TLS_CLIENT_KEY, "", true),
        kv(MYSQL_QUEUE_DIR, EVENT_DEFAULT_DIR, false),
        kv(MYSQL_QUEUE_LIMIT, DEFAULT_LIMIT.to_string(), false),
        kv(MYSQL_MAX_OPEN_CONNECTIONS, "2", false),
        kv(COMMENT_KEY, "", false),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Expected values are spelled out as literals on purpose: they mirror the tables currently
    /// declared in `audit.rs` and `notify.rs`, so a drift in key order or in any default breaks
    /// the test instead of silently changing admin config output.
    fn assert_table(actual: &KVS, expected: &[(&str, &str, bool)]) {
        let actual: Vec<(&str, &str, bool)> = actual
            .0
            .iter()
            .map(|kv| (kv.key.as_str(), kv.value.as_str(), kv.hidden_if_empty))
            .collect();
        assert_eq!(actual, expected);
    }

    const QUEUE_DIR: &str = "/opt/rustfs/events";
    const QUEUE_LIMIT: &str = "100000";

    #[test]
    fn amqp_table_matches_audit_and_notify() {
        assert_table(
            &amqp_kvs(),
            &[
                ("enable", "off", false),
                ("url", "", false),
                ("exchange", "", false),
                ("routing_key", "", false),
                ("mandatory", "off", false),
                ("persistent", "on", false),
                ("username", "", false),
                ("password", "", true),
                ("tls_ca", "", true),
                ("tls_client_cert", "", true),
                ("tls_client_key", "", true),
                ("queue_dir", QUEUE_DIR, false),
                ("queue_limit", QUEUE_LIMIT, false),
                ("comment", "", false),
            ],
        );
    }

    #[test]
    fn nats_table_matches_audit_and_notify() {
        assert_table(
            &nats_kvs(),
            &[
                ("enable", "off", false),
                ("address", "", false),
                ("subject", "", false),
                ("username", "", false),
                ("password", "", true),
                ("token", "", true),
                ("credentials_file", "", true),
                ("tls_ca", "", true),
                ("tls_client_cert", "", true),
                ("tls_client_key", "", true),
                ("tls_required", "off", false),
                ("queue_dir", QUEUE_DIR, false),
                ("queue_limit", QUEUE_LIMIT, false),
                ("jetstream_enable", "off", false),
                ("jetstream_stream_name", "", false),
                ("jetstream_ack_timeout_secs", "30", false),
                ("comment", "", false),
            ],
        );
    }

    #[test]
    fn pulsar_table_matches_audit_and_notify() {
        assert_table(
            &pulsar_kvs(),
            &[
                ("enable", "off", false),
                ("broker", "", false),
                ("topic", "", false),
                ("auth_token", "", true),
                ("username", "", false),
                ("password", "", true),
                ("tls_ca", "", true),
                ("tls_allow_insecure", "off", false),
                ("tls_hostname_verification", "on", false),
                ("queue_dir", QUEUE_DIR, false),
                ("queue_limit", QUEUE_LIMIT, false),
                ("comment", "", false),
            ],
        );
    }

    #[test]
    fn postgres_table_matches_audit_and_notify() {
        assert_table(
            &postgres_kvs(),
            &[
                ("enable", "off", false),
                ("dsn_string", "", true),
                ("table", "", false),
                ("format", "namespace", false),
                ("tls_required", "off", false),
                ("tls_ca", "", true),
                ("tls_client_cert", "", true),
                ("tls_client_key", "", true),
                ("queue_dir", QUEUE_DIR, false),
                ("queue_limit", QUEUE_LIMIT, false),
                ("comment", "", false),
            ],
        );
    }

    #[test]
    fn kafka_table_matches_audit_and_notify() {
        assert_table(
            &kafka_kvs(),
            &[
                ("enable", "off", false),
                ("brokers", "", false),
                ("topic", "", false),
                ("acks", "1", false),
                ("tls_enable", "off", false),
                ("tls_ca", "", true),
                ("tls_client_cert", "", true),
                ("tls_client_key", "", true),
                ("sasl_enable", "off", false),
                ("sasl_mechanism", "", false),
                ("sasl_username", "", false),
                ("sasl_password", "", true),
                ("queue_dir", QUEUE_DIR, false),
                ("queue_limit", QUEUE_LIMIT, false),
                ("comment", "", false),
            ],
        );
    }

    fn expected_redis(channel: &str) -> Vec<(&str, &str, bool)> {
        vec![
            ("enable", "off", false),
            ("url", "", false),
            ("channel", channel, false),
            ("username", "", false),
            ("password", "", true),
            ("keep_alive_interval", "15", false),
            ("queue_dir", QUEUE_DIR, false),
            ("queue_limit", QUEUE_LIMIT, false),
            ("max_retry_attempts", "3", false),
            ("reconnect_retry_attempts", "", false),
            ("min_retry_delay", "", false),
            ("max_retry_delay", "", false),
            ("connection_timeout", "", false),
            ("response_timeout", "", false),
            ("pipeline_buffer_size", "", false),
            ("tls_policy", "", true),
            ("tls_ca", "", true),
            ("tls_client_cert", "", true),
            ("tls_client_key", "", true),
            ("tls_allow_insecure", "off", false),
            ("comment", "", false),
        ]
    }

    #[test]
    fn redis_table_matches_audit() {
        assert_table(&redis_kvs("rustfs_audit_channel"), &expected_redis("rustfs_audit_channel"));
    }

    #[test]
    fn redis_table_matches_notify() {
        assert_table(&redis_kvs("rustfs_notify_channel"), &expected_redis("rustfs_notify_channel"));
    }

    fn expected_mysql(table: &str) -> Vec<(&str, &str, bool)> {
        vec![
            ("enable", "off", false),
            ("dsn_string", "", true),
            ("table", table, false),
            ("format", "access", false),
            ("tls_ca", "", true),
            ("tls_client_cert", "", true),
            ("tls_client_key", "", true),
            ("queue_dir", QUEUE_DIR, false),
            ("queue_limit", QUEUE_LIMIT, false),
            ("max_open_connections", "2", false),
            ("comment", "", false),
        ]
    }

    #[test]
    fn mysql_table_matches_audit() {
        assert_table(&mysql_kvs("rustfs_audit_logs"), &expected_mysql("rustfs_audit_logs"));
    }

    #[test]
    fn mysql_table_matches_notify() {
        assert_table(&mysql_kvs("rustfs_events"), &expected_mysql("rustfs_events"));
    }
}
