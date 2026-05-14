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

use crate::manifest::{
    TargetPluginDistributionManifest, TargetPluginExternalRuntimeContract, TargetPluginManifest, TargetPluginRuntimeTransport,
};
use crate::runtime::sidecar_protocol::SIDECAR_RUNTIME_PROTOCOL_VERSION;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetPluginInstallState {
    NotInstalled,
    Installed,
    InstallFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetPluginEnableState {
    Enabled,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetPluginRuntimeState {
    Running,
    Offline,
    Error,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TargetPluginRevision {
    pub version: String,
    pub digest_sha256: Option<String>,
    pub source: String,
    pub installed_at: Option<String>,
    pub artifact_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TargetPluginInstallation {
    pub install_state: TargetPluginInstallState,
    pub current_revision: Option<TargetPluginRevision>,
    pub previous_revision: Option<TargetPluginRevision>,
    pub validation_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TargetPluginOperationalState {
    pub install_state: TargetPluginInstallState,
    pub enable_state: TargetPluginEnableState,
    pub runtime_state: TargetPluginRuntimeState,
}

pub fn builtin_target_plugin_installation(manifest: &TargetPluginManifest) -> TargetPluginInstallation {
    TargetPluginInstallation {
        install_state: TargetPluginInstallState::Installed,
        current_revision: Some(TargetPluginRevision {
            version: manifest.version.to_string(),
            digest_sha256: None,
            source: "builtin".to_string(),
            installed_at: None,
            artifact_id: None,
        }),
        previous_revision: None,
        validation_error: None,
    }
}

pub fn external_target_plugin_installation(
    version: impl Into<String>,
    digest_sha256: impl Into<String>,
    artifact_id: impl Into<String>,
    installed_at: Option<String>,
) -> TargetPluginInstallation {
    TargetPluginInstallation {
        install_state: TargetPluginInstallState::Installed,
        current_revision: Some(TargetPluginRevision {
            version: version.into(),
            digest_sha256: Some(digest_sha256.into()),
            source: "external".to_string(),
            installed_at,
            artifact_id: Some(artifact_id.into()),
        }),
        previous_revision: None,
        validation_error: None,
    }
}

pub fn failed_external_target_plugin_installation(
    version: impl Into<String>,
    artifact_id: impl Into<String>,
    validation_error: impl Into<String>,
) -> TargetPluginInstallation {
    TargetPluginInstallation {
        install_state: TargetPluginInstallState::InstallFailed,
        current_revision: Some(TargetPluginRevision {
            version: version.into(),
            digest_sha256: None,
            source: "external".to_string(),
            installed_at: None,
            artifact_id: Some(artifact_id.into()),
        }),
        previous_revision: None,
        validation_error: Some(validation_error.into()),
    }
}

pub fn rollback_target_plugin_installation(
    current: TargetPluginRevision,
    previous: TargetPluginRevision,
) -> TargetPluginInstallation {
    TargetPluginInstallation {
        install_state: TargetPluginInstallState::Installed,
        current_revision: Some(previous),
        previous_revision: Some(current),
        validation_error: None,
    }
}

pub fn builtin_target_plugin_operational_state(
    enabled: bool,
    runtime_state: TargetPluginRuntimeState,
) -> TargetPluginOperationalState {
    TargetPluginOperationalState {
        install_state: TargetPluginInstallState::Installed,
        enable_state: if enabled {
            TargetPluginEnableState::Enabled
        } else {
            TargetPluginEnableState::Disabled
        },
        runtime_state,
    }
}

pub fn runtime_state_from_status_label(status: &str) -> TargetPluginRuntimeState {
    if status.eq_ignore_ascii_case("online") {
        TargetPluginRuntimeState::Running
    } else if status.eq_ignore_ascii_case("offline") {
        TargetPluginRuntimeState::Offline
    } else if status.eq_ignore_ascii_case("error") {
        TargetPluginRuntimeState::Error
    } else {
        TargetPluginRuntimeState::Unknown
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetPluginInstallPolicy {
    pub allowed_providers: Vec<String>,
    pub allowed_download_hosts: Vec<String>,
    pub require_https: bool,
    pub require_signature: bool,
}

impl Default for TargetPluginInstallPolicy {
    fn default() -> Self {
        Self {
            allowed_providers: vec!["rustfs".to_string(), "rustfs-labs".to_string()],
            allowed_download_hosts: vec!["plugins.example.test".to_string()],
            require_https: true,
            require_signature: false,
        }
    }
}

pub fn validate_external_plugin_installation(
    manifest: &TargetPluginManifest,
    runtime_contract: &TargetPluginExternalRuntimeContract,
    distribution: Option<TargetPluginDistributionManifest>,
    policy: &TargetPluginInstallPolicy,
) -> Result<(), String> {
    if !policy.allowed_providers.iter().any(|provider| provider == manifest.provider) {
        return Err(format!("provider {} is not allowed by install policy", manifest.provider));
    }

    if runtime_contract.transport == TargetPluginRuntimeTransport::Grpc
        && runtime_contract.protocol_version != SIDECAR_RUNTIME_PROTOCOL_VERSION
    {
        return Err(format!(
            "sidecar runtime protocol mismatch: expected {}, got {}",
            SIDECAR_RUNTIME_PROTOCOL_VERSION, runtime_contract.protocol_version
        ));
    }

    if policy.require_signature {
        return Err(
            "signature verification is required by install policy but manifests do not expose signatures yet".to_string(),
        );
    }

    let distribution = distribution.ok_or_else(|| "external plugin is missing distribution metadata".to_string())?;
    if distribution.artifacts.is_empty() {
        return Err("external plugin distribution has no artifacts".to_string());
    }

    for artifact in distribution.artifacts {
        let parsed_uri = Url::parse(artifact.download_uri)
            .map_err(|err| format!("invalid artifact download uri {}: {}", artifact.download_uri, err))?;
        if policy.require_https && parsed_uri.scheme() != "https" {
            return Err(format!(
                "artifact {} must use https download uri, got {}",
                artifact.artifact_id, artifact.download_uri
            ));
        }
        let host = parsed_uri
            .host_str()
            .ok_or_else(|| format!("artifact {} download uri has no host", artifact.artifact_id))?;
        if !policy.allowed_download_hosts.iter().any(|allowed| allowed == host) {
            return Err(format!("artifact {} download host {} is not allowed", artifact.artifact_id, host));
        }
        if artifact.size_bytes == 0 {
            return Err(format!("artifact {} must declare a non-zero size", artifact.artifact_id));
        }
        if artifact.digest_sha256.len() < 16 || !artifact.digest_sha256.chars().all(|ch| ch.is_ascii_hexdigit()) {
            return Err(format!(
                "artifact {} has invalid digest_sha256 {}",
                artifact.artifact_id, artifact.digest_sha256
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        TargetPluginEnableState, TargetPluginInstallPolicy, TargetPluginInstallState, TargetPluginRevision,
        TargetPluginRuntimeState, builtin_target_plugin_installation, builtin_target_plugin_operational_state,
        external_target_plugin_installation, failed_external_target_plugin_installation, rollback_target_plugin_installation,
        runtime_state_from_status_label, validate_external_plugin_installation,
    };
    use crate::manifest::{
        TargetPluginArtifactManifest, TargetPluginDistributionManifest, TargetPluginExternalRuntimeContract,
        TargetPluginManifest, TargetPluginRuntimeTransport, builtin_target_manifest,
    };

    #[test]
    fn builtin_installation_maps_to_virtual_installed_revision() {
        let installation = builtin_target_plugin_installation(&builtin_target_manifest("webhook"));

        assert_eq!(installation.install_state, TargetPluginInstallState::Installed);
        assert_eq!(
            installation
                .current_revision
                .as_ref()
                .expect("builtin installation should expose current revision")
                .source,
            "builtin"
        );
        assert_eq!(
            installation
                .current_revision
                .as_ref()
                .expect("builtin installation should expose current revision")
                .artifact_id,
            None
        );
        assert!(installation.previous_revision.is_none());
        assert_eq!(installation.validation_error, None);
    }

    #[test]
    fn builtin_operational_state_tracks_enablement_and_runtime() {
        let enabled = builtin_target_plugin_operational_state(true, TargetPluginRuntimeState::Running);
        let disabled = builtin_target_plugin_operational_state(false, TargetPluginRuntimeState::Offline);

        assert_eq!(enabled.install_state, TargetPluginInstallState::Installed);
        assert_eq!(enabled.enable_state, TargetPluginEnableState::Enabled);
        assert_eq!(enabled.runtime_state, TargetPluginRuntimeState::Running);

        assert_eq!(disabled.enable_state, TargetPluginEnableState::Disabled);
        assert_eq!(disabled.runtime_state, TargetPluginRuntimeState::Offline);
    }

    #[test]
    fn runtime_state_from_status_maps_known_labels() {
        assert_eq!(runtime_state_from_status_label("online"), TargetPluginRuntimeState::Running);
        assert_eq!(runtime_state_from_status_label("offline"), TargetPluginRuntimeState::Offline);
        assert_eq!(runtime_state_from_status_label("error"), TargetPluginRuntimeState::Error);
        assert_eq!(runtime_state_from_status_label("unexpected"), TargetPluginRuntimeState::Unknown);
    }

    #[test]
    fn external_installation_captures_revision_metadata() {
        let installation = external_target_plugin_installation(
            "1.2.3",
            "0123456789abcdef",
            "sidecar-linux-amd64",
            Some("2026-05-13T12:00:00Z".to_string()),
        );

        let revision = installation
            .current_revision
            .as_ref()
            .expect("external installation should expose current revision");
        assert_eq!(installation.install_state, TargetPluginInstallState::Installed);
        assert_eq!(revision.source, "external");
        assert_eq!(revision.digest_sha256.as_deref(), Some("0123456789abcdef"));
        assert_eq!(revision.artifact_id.as_deref(), Some("sidecar-linux-amd64"));
        assert_eq!(installation.validation_error, None);
    }

    #[test]
    fn rollback_swaps_current_and_previous_revisions() {
        let current = TargetPluginRevision {
            version: "2.0.0".to_string(),
            digest_sha256: Some("new-digest".to_string()),
            source: "external".to_string(),
            installed_at: Some("2026-05-13T12:05:00Z".to_string()),
            artifact_id: Some("sidecar-linux-amd64-v2".to_string()),
        };
        let previous = TargetPluginRevision {
            version: "1.9.0".to_string(),
            digest_sha256: Some("old-digest".to_string()),
            source: "external".to_string(),
            installed_at: Some("2026-05-13T11:55:00Z".to_string()),
            artifact_id: Some("sidecar-linux-amd64-v1".to_string()),
        };

        let installation = rollback_target_plugin_installation(current.clone(), previous.clone());

        assert_eq!(installation.current_revision, Some(previous));
        assert_eq!(installation.previous_revision, Some(current));
        assert_eq!(installation.validation_error, None);
    }

    #[test]
    fn failed_external_installation_preserves_error_context() {
        let installation =
            failed_external_target_plugin_installation("1.2.3", "sidecar-linux-amd64", "digest mismatch during install");

        assert_eq!(installation.install_state, TargetPluginInstallState::InstallFailed);
        assert_eq!(installation.validation_error.as_deref(), Some("digest mismatch during install"));
    }

    #[test]
    fn validate_external_installation_accepts_allowed_https_artifact() {
        let manifest = TargetPluginManifest {
            plugin_id: "external:webhook-sidecar",
            display_name: "Webhook Sidecar",
            provider: "rustfs-labs",
            version: "1.0.0",
            target_type: "webhook",
            supported_domains: &[],
            secret_fields: &[],
        };
        let distribution = TargetPluginDistributionManifest {
            artifacts: &[TargetPluginArtifactManifest {
                artifact_id: "sidecar-linux-amd64",
                target_triple: "x86_64-unknown-linux-gnu",
                download_uri: "https://plugins.example.test/webhook-sidecar.tar.zst",
                digest_sha256: "0123456789abcdef0123456789abcdef",
                size_bytes: 8192,
            }],
        };
        let policy = TargetPluginInstallPolicy::default();

        let result = validate_external_plugin_installation(
            &manifest,
            &TargetPluginExternalRuntimeContract {
                protocol_version: crate::SIDECAR_RUNTIME_PROTOCOL_VERSION,
                transport: TargetPluginRuntimeTransport::Grpc,
            },
            Some(distribution),
            &policy,
        );

        assert!(result.is_ok());
    }

    #[test]
    fn validate_external_installation_rejects_disallowed_provider() {
        let manifest = TargetPluginManifest {
            plugin_id: "external:webhook-sidecar",
            display_name: "Webhook Sidecar",
            provider: "unknown-vendor",
            version: "1.0.0",
            target_type: "webhook",
            supported_domains: &[],
            secret_fields: &[],
        };
        let policy = TargetPluginInstallPolicy::default();

        let result = validate_external_plugin_installation(
            &manifest,
            &TargetPluginExternalRuntimeContract {
                protocol_version: crate::SIDECAR_RUNTIME_PROTOCOL_VERSION,
                transport: TargetPluginRuntimeTransport::Grpc,
            },
            Some(TargetPluginDistributionManifest {
                artifacts: &[TargetPluginArtifactManifest {
                    artifact_id: "sidecar-linux-amd64",
                    target_triple: "x86_64-unknown-linux-gnu",
                    download_uri: "https://plugins.example.test/webhook-sidecar.tar.zst",
                    digest_sha256: "0123456789abcdef0123456789abcdef",
                    size_bytes: 8192,
                }],
            }),
            &policy,
        );

        assert!(result.is_err());
    }
}
