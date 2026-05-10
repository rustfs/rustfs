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

use crate::domain::TargetDomain;
use rustfs_config::{
    AMQP_PASSWORD, AMQP_TLS_CLIENT_CERT, AMQP_TLS_CLIENT_KEY, KAFKA_TLS_CLIENT_CERT, KAFKA_TLS_CLIENT_KEY, MQTT_PASSWORD,
    MQTT_TLS_CLIENT_CERT, MQTT_TLS_CLIENT_KEY, MYSQL_DSN_STRING, MYSQL_TLS_CLIENT_CERT, MYSQL_TLS_CLIENT_KEY,
    NATS_CREDENTIALS_FILE, NATS_PASSWORD, NATS_TLS_CLIENT_CERT, NATS_TLS_CLIENT_KEY, NATS_TOKEN, POSTGRES_DSN_STRING,
    POSTGRES_TLS_CLIENT_CERT, POSTGRES_TLS_CLIENT_KEY, PULSAR_AUTH_TOKEN, PULSAR_PASSWORD, REDIS_PASSWORD, REDIS_TLS_CLIENT_CERT,
    REDIS_TLS_CLIENT_KEY, WEBHOOK_AUTH_TOKEN, WEBHOOK_CLIENT_CERT, WEBHOOK_CLIENT_KEY,
};

/// Shared plugin manifest metadata for a target implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetPluginManifest {
    pub plugin_id: &'static str,
    pub display_name: &'static str,
    pub provider: &'static str,
    pub version: &'static str,
    pub target_type: &'static str,
    pub supported_domains: &'static [TargetDomain],
    pub secret_fields: &'static [&'static str],
}

const SUPPORTED_BUILTIN_DOMAINS: &[TargetDomain] = &[TargetDomain::Audit, TargetDomain::Notify];
const NO_SECRET_FIELDS: &[&str] = &[];

const WEBHOOK_SECRET_FIELDS: &[&str] = &[WEBHOOK_AUTH_TOKEN, WEBHOOK_CLIENT_CERT, WEBHOOK_CLIENT_KEY];
const MQTT_SECRET_FIELDS: &[&str] = &[MQTT_PASSWORD, MQTT_TLS_CLIENT_CERT, MQTT_TLS_CLIENT_KEY];
const KAFKA_SECRET_FIELDS: &[&str] = &[KAFKA_TLS_CLIENT_CERT, KAFKA_TLS_CLIENT_KEY];
const AMQP_SECRET_FIELDS: &[&str] = &[AMQP_PASSWORD, AMQP_TLS_CLIENT_CERT, AMQP_TLS_CLIENT_KEY];
const NATS_SECRET_FIELDS: &[&str] = &[
    NATS_PASSWORD,
    NATS_TOKEN,
    NATS_CREDENTIALS_FILE,
    NATS_TLS_CLIENT_CERT,
    NATS_TLS_CLIENT_KEY,
];
const PULSAR_SECRET_FIELDS: &[&str] = &[PULSAR_AUTH_TOKEN, PULSAR_PASSWORD];
const MYSQL_SECRET_FIELDS: &[&str] = &[MYSQL_DSN_STRING, MYSQL_TLS_CLIENT_CERT, MYSQL_TLS_CLIENT_KEY];
const REDIS_SECRET_FIELDS: &[&str] = &[REDIS_PASSWORD, REDIS_TLS_CLIENT_CERT, REDIS_TLS_CLIENT_KEY];
const POSTGRES_SECRET_FIELDS: &[&str] = &[POSTGRES_DSN_STRING, POSTGRES_TLS_CLIENT_CERT, POSTGRES_TLS_CLIENT_KEY];

#[inline]
pub fn builtin_target_manifest(target_type: &'static str) -> TargetPluginManifest {
    let (display_name, secret_fields) = match target_type {
        "webhook" => ("Webhook", WEBHOOK_SECRET_FIELDS),
        "mqtt" => ("MQTT", MQTT_SECRET_FIELDS),
        "kafka" => ("Kafka", KAFKA_SECRET_FIELDS),
        "amqp" => ("AMQP", AMQP_SECRET_FIELDS),
        "nats" => ("NATS", NATS_SECRET_FIELDS),
        "pulsar" => ("Pulsar", PULSAR_SECRET_FIELDS),
        "mysql" => ("MySQL", MYSQL_SECRET_FIELDS),
        "redis" => ("Redis", REDIS_SECRET_FIELDS),
        "postgres" => ("Postgres", POSTGRES_SECRET_FIELDS),
        _ => ("Custom Target", NO_SECRET_FIELDS),
    };

    TargetPluginManifest {
        plugin_id: builtin_plugin_id(target_type),
        display_name,
        provider: "rustfs",
        version: env!("CARGO_PKG_VERSION"),
        target_type,
        supported_domains: SUPPORTED_BUILTIN_DOMAINS,
        secret_fields,
    }
}

#[inline]
fn builtin_plugin_id(target_type: &'static str) -> &'static str {
    match target_type {
        "webhook" => "builtin:webhook",
        "mqtt" => "builtin:mqtt",
        "kafka" => "builtin:kafka",
        "amqp" => "builtin:amqp",
        "nats" => "builtin:nats",
        "pulsar" => "builtin:pulsar",
        "mysql" => "builtin:mysql",
        "redis" => "builtin:redis",
        "postgres" => "builtin:postgres",
        _ => "custom:target",
    }
}

#[cfg(test)]
mod tests {
    use super::builtin_target_manifest;
    use rustfs_config::{WEBHOOK_AUTH_TOKEN, WEBHOOK_CLIENT_CERT, WEBHOOK_CLIENT_KEY};

    #[test]
    fn builtin_webhook_manifest_marks_secret_fields() {
        let manifest = builtin_target_manifest("webhook");

        assert_eq!(manifest.plugin_id, "builtin:webhook");
        assert_eq!(manifest.display_name, "Webhook");
        assert!(manifest.secret_fields.contains(&WEBHOOK_AUTH_TOKEN));
        assert!(manifest.secret_fields.contains(&WEBHOOK_CLIENT_CERT));
        assert!(manifest.secret_fields.contains(&WEBHOOK_CLIENT_KEY));
    }
}
