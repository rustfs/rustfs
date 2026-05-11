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

/// Declares how a plugin is packaged relative to the RustFS process boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetPluginPackaging {
    Builtin,
    External,
}

/// Declares what kind of entrypoint a plugin would use when instantiated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetPluginEntrypointKind {
    Builtin,
    Sidecar,
    Wasm,
}

/// Declares the transport boundary RustFS would use to communicate with a
/// plugin runtime without committing to any concrete loader implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetPluginRuntimeTransport {
    InProcess,
    Grpc,
    WasmHost,
}

/// Declarative external runtime contract for future installable plugins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetPluginExternalRuntimeContract {
    pub protocol_version: &'static str,
    pub transport: TargetPluginRuntimeTransport,
}

/// Declarative distribution metadata for an installable target plugin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetPluginDistributionManifest {
    pub download_uri: &'static str,
    pub digest_sha256: &'static str,
    pub size_bytes: u64,
}

/// Marketplace-oriented manifest metadata that is explicit about future
/// installable plugin boundaries without introducing any loading behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetPluginMarketplaceManifest {
    pub plugin_id: &'static str,
    pub display_name: &'static str,
    pub provider: &'static str,
    pub version: &'static str,
    pub target_type: &'static str,
    pub supported_domains: &'static [TargetDomain],
    pub secret_fields: &'static [&'static str],
    pub packaging: TargetPluginPackaging,
    pub entrypoint_kind: TargetPluginEntrypointKind,
    pub api_compatibility_version: &'static str,
    pub runtime_contract: TargetPluginExternalRuntimeContract,
    pub distribution: Option<TargetPluginDistributionManifest>,
}

const BUILTIN_PLUGIN_API_COMPATIBILITY_VERSION: &str = "rustfs.target-plugin.v1";
const BUILTIN_PLUGIN_RUNTIME_PROTOCOL_VERSION: &str = "rustfs.target-runtime.v1";

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
pub fn builtin_target_marketplace_manifest(target_type: &'static str) -> TargetPluginMarketplaceManifest {
    TargetPluginMarketplaceManifest::from(builtin_target_manifest(target_type))
}

impl From<TargetPluginManifest> for TargetPluginMarketplaceManifest {
    fn from(value: TargetPluginManifest) -> Self {
        Self {
            plugin_id: value.plugin_id,
            display_name: value.display_name,
            provider: value.provider,
            version: value.version,
            target_type: value.target_type,
            supported_domains: value.supported_domains,
            secret_fields: value.secret_fields,
            packaging: TargetPluginPackaging::Builtin,
            entrypoint_kind: TargetPluginEntrypointKind::Builtin,
            api_compatibility_version: BUILTIN_PLUGIN_API_COMPATIBILITY_VERSION,
            runtime_contract: TargetPluginExternalRuntimeContract {
                protocol_version: BUILTIN_PLUGIN_RUNTIME_PROTOCOL_VERSION,
                transport: TargetPluginRuntimeTransport::InProcess,
            },
            distribution: None,
        }
    }
}

#[inline]
pub fn installable_target_marketplace_manifest(
    base: TargetPluginManifest,
    entrypoint_kind: TargetPluginEntrypointKind,
    runtime_contract: TargetPluginExternalRuntimeContract,
    distribution: TargetPluginDistributionManifest,
) -> TargetPluginMarketplaceManifest {
    TargetPluginMarketplaceManifest {
        plugin_id: base.plugin_id,
        display_name: base.display_name,
        provider: base.provider,
        version: base.version,
        target_type: base.target_type,
        supported_domains: base.supported_domains,
        secret_fields: base.secret_fields,
        packaging: TargetPluginPackaging::External,
        entrypoint_kind,
        api_compatibility_version: BUILTIN_PLUGIN_API_COMPATIBILITY_VERSION,
        runtime_contract,
        distribution: Some(distribution),
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
    use super::{
        TargetPluginDistributionManifest, TargetPluginEntrypointKind, TargetPluginExternalRuntimeContract,
        TargetPluginMarketplaceManifest, TargetPluginPackaging, TargetPluginRuntimeTransport, builtin_target_manifest,
        builtin_target_marketplace_manifest, installable_target_marketplace_manifest,
    };
    use crate::domain::TargetDomain;
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

    #[test]
    fn builtin_manifest_derives_marketplace_boundary_metadata() {
        let manifest = builtin_target_marketplace_manifest("webhook");

        assert_eq!(manifest.plugin_id, "builtin:webhook");
        assert_eq!(manifest.display_name, "Webhook");
        assert_eq!(manifest.target_type, "webhook");
        assert_eq!(manifest.packaging, TargetPluginPackaging::Builtin);
        assert_eq!(manifest.entrypoint_kind, TargetPluginEntrypointKind::Builtin);
        assert_eq!(manifest.api_compatibility_version, "rustfs.target-plugin.v1");
        assert_eq!(
            manifest.runtime_contract,
            TargetPluginExternalRuntimeContract {
                protocol_version: "rustfs.target-runtime.v1",
                transport: TargetPluginRuntimeTransport::InProcess,
            }
        );
        assert_eq!(manifest.distribution, None);
    }

    #[test]
    fn marketplace_manifest_preserves_supported_domains() {
        let manifest = builtin_target_marketplace_manifest("kafka");

        assert_eq!(manifest.supported_domains, &[TargetDomain::Audit, TargetDomain::Notify]);
    }

    #[test]
    fn marketplace_manifest_from_builtin_manifest_is_stable() {
        let base = builtin_target_manifest("redis");
        let derived = TargetPluginMarketplaceManifest::from(base);

        assert_eq!(derived.plugin_id, "builtin:redis");
        assert_eq!(derived.target_type, "redis");
        assert_eq!(derived.packaging, TargetPluginPackaging::Builtin);
        assert_eq!(derived.entrypoint_kind, TargetPluginEntrypointKind::Builtin);
        assert_eq!(derived.runtime_contract.transport, TargetPluginRuntimeTransport::InProcess);
        assert_eq!(derived.distribution, None);
    }

    #[test]
    fn installable_manifest_expresses_external_boundary_declaratively() {
        let base = builtin_target_manifest("webhook");
        let manifest = installable_target_marketplace_manifest(
            base,
            TargetPluginEntrypointKind::Sidecar,
            TargetPluginExternalRuntimeContract {
                protocol_version: "rustfs.target-runtime.v1",
                transport: TargetPluginRuntimeTransport::Grpc,
            },
            TargetPluginDistributionManifest {
                download_uri: "https://plugins.example.test/webhook-plugin.tar.zst",
                digest_sha256: "0123456789abcdef",
                size_bytes: 4096,
            },
        );

        assert_eq!(manifest.packaging, TargetPluginPackaging::External);
        assert_eq!(manifest.entrypoint_kind, TargetPluginEntrypointKind::Sidecar);
        assert_eq!(manifest.runtime_contract.transport, TargetPluginRuntimeTransport::Grpc);
        assert_eq!(
            manifest.distribution,
            Some(TargetPluginDistributionManifest {
                download_uri: "https://plugins.example.test/webhook-plugin.tar.zst",
                digest_sha256: "0123456789abcdef",
                size_bytes: 4096,
            })
        );
    }
}
