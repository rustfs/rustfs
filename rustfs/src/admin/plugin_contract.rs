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

use rustfs_targets::{
    TargetDomain, TargetPluginEntrypointKind, TargetPluginExternalRuntimeContract, TargetPluginPackaging,
    TargetPluginRuntimeTransport,
};
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PluginContractDomain {
    Audit,
    Notify,
}

impl From<TargetDomain> for PluginContractDomain {
    fn from(value: TargetDomain) -> Self {
        match value {
            TargetDomain::Audit => Self::Audit,
            TargetDomain::Notify => Self::Notify,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PluginContractPackaging {
    Builtin,
    External,
}

impl From<TargetPluginPackaging> for PluginContractPackaging {
    fn from(value: TargetPluginPackaging) -> Self {
        match value {
            TargetPluginPackaging::Builtin => Self::Builtin,
            TargetPluginPackaging::External => Self::External,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PluginContractEntrypointKind {
    Builtin,
    Sidecar,
    Wasm,
}

impl From<TargetPluginEntrypointKind> for PluginContractEntrypointKind {
    fn from(value: TargetPluginEntrypointKind) -> Self {
        match value {
            TargetPluginEntrypointKind::Builtin => Self::Builtin,
            TargetPluginEntrypointKind::Sidecar => Self::Sidecar,
            TargetPluginEntrypointKind::Wasm => Self::Wasm,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PluginRuntimeTransport {
    InProcess,
    Grpc,
    WasmHost,
}

impl From<TargetPluginRuntimeTransport> for PluginRuntimeTransport {
    fn from(value: TargetPluginRuntimeTransport) -> Self {
        match value {
            TargetPluginRuntimeTransport::InProcess => Self::InProcess,
            TargetPluginRuntimeTransport::Grpc => Self::Grpc,
            TargetPluginRuntimeTransport::WasmHost => Self::WasmHost,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct PluginRuntimeContract {
    pub protocol_version: String,
    pub transport: PluginRuntimeTransport,
}

impl From<TargetPluginExternalRuntimeContract> for PluginRuntimeContract {
    fn from(value: TargetPluginExternalRuntimeContract) -> Self {
        Self {
            protocol_version: value.protocol_version.to_string(),
            transport: PluginRuntimeTransport::from(value.transport),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PluginInstanceSource {
    Config,
    Env,
    Mixed,
    Runtime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct PluginCatalogDomainEntry {
    pub domain: PluginContractDomain,
    pub subsystem: String,
    pub valid_fields: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct PluginCatalogEntry {
    pub plugin_id: String,
    pub target_type: String,
    pub display_name: String,
    pub provider: String,
    pub version: String,
    pub packaging: PluginContractPackaging,
    pub entrypoint_kind: PluginContractEntrypointKind,
    pub api_compatibility_version: String,
    pub runtime_contract: PluginRuntimeContract,
    pub supported_domains: Vec<PluginContractDomain>,
    pub secret_fields: Vec<String>,
    pub domain_configs: Vec<PluginCatalogDomainEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct PluginCatalogResponse {
    pub plugins: Vec<PluginCatalogEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct PluginInstanceEntry {
    pub id: String,
    pub plugin_id: String,
    pub domain: PluginContractDomain,
    pub subsystem: String,
    pub account_id: String,
    pub service: String,
    pub status: String,
    pub source: PluginInstanceSource,
    pub enabled: bool,
    pub config: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct PluginInstancesResponse {
    pub instances: Vec<PluginInstanceEntry>,
    pub truncated: bool,
    pub next_marker: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{
        PluginCatalogDomainEntry, PluginCatalogEntry, PluginCatalogResponse, PluginContractDomain, PluginContractEntrypointKind,
        PluginContractPackaging, PluginInstanceEntry, PluginInstanceSource, PluginInstancesResponse, PluginRuntimeContract,
        PluginRuntimeTransport,
    };
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn plugin_catalog_contract_serializes_stable_json_shape() {
        let response = PluginCatalogResponse {
            plugins: vec![PluginCatalogEntry {
                plugin_id: "builtin:webhook".to_string(),
                target_type: "webhook".to_string(),
                display_name: "Webhook".to_string(),
                provider: "rustfs".to_string(),
                version: "1.0.0".to_string(),
                packaging: PluginContractPackaging::Builtin,
                entrypoint_kind: PluginContractEntrypointKind::Builtin,
                api_compatibility_version: "rustfs.target-plugin.v1".to_string(),
                runtime_contract: PluginRuntimeContract {
                    protocol_version: "rustfs.target-runtime.v1".to_string(),
                    transport: PluginRuntimeTransport::InProcess,
                },
                supported_domains: vec![PluginContractDomain::Audit, PluginContractDomain::Notify],
                secret_fields: vec!["auth_token".to_string()],
                domain_configs: vec![PluginCatalogDomainEntry {
                    domain: PluginContractDomain::Notify,
                    subsystem: "notify_webhook".to_string(),
                    valid_fields: vec!["endpoint".to_string(), "auth_token".to_string()],
                }],
            }],
        };

        let value = serde_json::to_value(response).expect("catalog response should serialize");
        assert_eq!(
            value,
            json!({
                "plugins": [{
                    "plugin_id": "builtin:webhook",
                    "target_type": "webhook",
                    "display_name": "Webhook",
                    "provider": "rustfs",
                    "version": "1.0.0",
                    "packaging": "builtin",
                    "entrypoint_kind": "builtin",
                    "api_compatibility_version": "rustfs.target-plugin.v1",
                    "runtime_contract": {
                        "protocol_version": "rustfs.target-runtime.v1",
                        "transport": "in_process"
                    },
                    "supported_domains": ["audit", "notify"],
                    "secret_fields": ["auth_token"],
                    "domain_configs": [{
                        "domain": "notify",
                        "subsystem": "notify_webhook",
                        "valid_fields": ["endpoint", "auth_token"]
                    }]
                }]
            })
        );
    }

    #[test]
    fn plugin_instance_contract_serializes_stable_json_shape() {
        let response = PluginInstancesResponse {
            instances: vec![PluginInstanceEntry {
                id: "builtin:webhook:notify:primary".to_string(),
                plugin_id: "builtin:webhook".to_string(),
                domain: PluginContractDomain::Notify,
                subsystem: "notify_webhook".to_string(),
                account_id: "primary".to_string(),
                service: "webhook".to_string(),
                status: "offline".to_string(),
                source: PluginInstanceSource::Config,
                enabled: true,
                config: HashMap::from([
                    ("enable".to_string(), "on".to_string()),
                    ("endpoint".to_string(), "https://example.com/hook".to_string()),
                ]),
            }],
            truncated: false,
            next_marker: None,
        };

        let value = serde_json::to_value(response).expect("instance response should serialize");
        assert_eq!(
            value,
            json!({
                "instances": [{
                    "id": "builtin:webhook:notify:primary",
                    "plugin_id": "builtin:webhook",
                    "domain": "notify",
                    "subsystem": "notify_webhook",
                    "account_id": "primary",
                    "service": "webhook",
                    "status": "offline",
                    "source": "config",
                    "enabled": true,
                    "config": {
                        "enable": "on",
                        "endpoint": "https://example.com/hook"
                    }
                }],
                "truncated": false,
                "next_marker": null
            })
        );
    }
}
