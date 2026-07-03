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
    TargetPluginDistributionManifest, TargetPluginExternalRuntimeContract, TargetPluginManifest, TargetPluginMarketplaceManifest,
    TargetPluginPackaging, TargetPluginRuntimeTransport,
};
use crate::runtime::sidecar::{SidecarRuntimePolicy, SidecarRuntimeSafetyChecks};
use crate::runtime::sidecar_protocol::SIDECAR_RUNTIME_PROTOCOL_VERSION;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

const SHA256_HEX_DIGEST_LEN: usize = 64;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetPluginExternalAction {
    Install,
    Enable,
    Disable,
    Rollback,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TargetPluginExternalActionDecision {
    pub action: TargetPluginExternalAction,
    pub plugin_id: String,
    pub installation: TargetPluginInstallation,
    pub operational_state: TargetPluginOperationalState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetPluginExternalFlowGate {
    pub enabled: bool,
    pub install_policy: TargetPluginInstallPolicy,
    pub runtime_policy: SidecarRuntimePolicy,
    pub runtime_safety_checks: SidecarRuntimeSafetyChecks,
    pub circuit_breaker_closed: bool,
}

impl Default for TargetPluginExternalFlowGate {
    fn default() -> Self {
        Self {
            enabled: false,
            install_policy: TargetPluginInstallPolicy::default(),
            runtime_policy: SidecarRuntimePolicy::default(),
            runtime_safety_checks: SidecarRuntimeSafetyChecks {
                sandboxed: false,
                provenance_verified: false,
                queue_depth: 0,
            },
            circuit_breaker_closed: false,
        }
    }
}

impl TargetPluginExternalFlowGate {
    pub fn verified(runtime_policy: SidecarRuntimePolicy, runtime_safety_checks: SidecarRuntimeSafetyChecks) -> Self {
        Self {
            enabled: true,
            install_policy: TargetPluginInstallPolicy::default(),
            runtime_policy,
            runtime_safety_checks,
            circuit_breaker_closed: true,
        }
    }

    pub fn status(&self) -> TargetPluginExternalFlowGateStatus {
        TargetPluginExternalFlowGateStatus {
            enabled: self.enabled,
            install_requires_signature: self.install_policy.require_signature,
            install_requires_provenance: self.install_policy.require_provenance,
            runtime_allows_external_sidecars: self.runtime_policy.allow_external_sidecars,
            runtime_requires_sandbox: self.runtime_policy.require_sandbox,
            runtime_requires_provenance: self.runtime_policy.require_provenance,
            circuit_breaker_closed: self.circuit_breaker_closed,
            max_queue_depth: self.runtime_policy.max_queue_depth,
            failure_threshold: self.runtime_policy.failure_threshold(),
            redacts_error_details: self.runtime_policy.redact_error_details,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TargetPluginExternalFlowGateStatus {
    pub enabled: bool,
    pub install_requires_signature: bool,
    pub install_requires_provenance: bool,
    pub runtime_allows_external_sidecars: bool,
    pub runtime_requires_sandbox: bool,
    pub runtime_requires_provenance: bool,
    pub circuit_breaker_closed: bool,
    pub max_queue_depth: usize,
    pub failure_threshold: usize,
    pub redacts_error_details: bool,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TargetPluginExternalActionError {
    #[error("external plugin flow is disabled")]
    ExternalFlowDisabled,

    #[error("plugin {plugin_id} is not an external plugin")]
    NotExternalPlugin { plugin_id: String },

    #[error("plugin {plugin_id} is not installed")]
    NotInstalled { plugin_id: String },

    #[error("plugin {plugin_id} has no previous revision for rollback")]
    MissingPreviousRevision { plugin_id: String },

    #[error("external plugin install policy denied action: {reason}")]
    InstallPolicyDenied { reason: String },

    #[error("external plugin runtime policy denied action: {reason}")]
    RuntimePolicyDenied { reason: String },

    #[error("external plugin circuit breaker is open")]
    CircuitBreakerOpen,

    #[error("external plugin {plugin_id} has no installable artifact for host target triple {target_triple}")]
    MissingArtifactForHost { plugin_id: String, target_triple: String },
}

pub fn plan_external_target_plugin_action(
    manifest: &TargetPluginMarketplaceManifest,
    action: TargetPluginExternalAction,
    installation: &TargetPluginInstallation,
    gate: &TargetPluginExternalFlowGate,
    host_target_triple: &str,
) -> Result<TargetPluginExternalActionDecision, TargetPluginExternalActionError> {
    validate_external_action_subject(manifest)?;
    validate_external_action_gate(gate)?;

    match action {
        TargetPluginExternalAction::Install => plan_external_install(manifest, action, gate, host_target_triple),
        TargetPluginExternalAction::Enable => {
            require_installed(manifest.plugin_id, installation)?;
            Ok(TargetPluginExternalActionDecision {
                action,
                plugin_id: manifest.plugin_id.to_string(),
                installation: installation.clone(),
                operational_state: TargetPluginOperationalState {
                    install_state: TargetPluginInstallState::Installed,
                    enable_state: TargetPluginEnableState::Enabled,
                    runtime_state: TargetPluginRuntimeState::Running,
                },
            })
        }
        TargetPluginExternalAction::Disable => {
            require_installed(manifest.plugin_id, installation)?;
            Ok(TargetPluginExternalActionDecision {
                action,
                plugin_id: manifest.plugin_id.to_string(),
                installation: installation.clone(),
                operational_state: TargetPluginOperationalState {
                    install_state: TargetPluginInstallState::Installed,
                    enable_state: TargetPluginEnableState::Disabled,
                    runtime_state: TargetPluginRuntimeState::Offline,
                },
            })
        }
        TargetPluginExternalAction::Rollback => plan_external_rollback(manifest, action, installation),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetPluginInstallPolicy {
    pub allowed_providers: Vec<String>,
    pub allowed_download_hosts: Vec<String>,
    pub require_https: bool,
    pub require_signature: bool,
    pub require_provenance: bool,
}

impl Default for TargetPluginInstallPolicy {
    fn default() -> Self {
        Self {
            allowed_providers: vec!["rustfs".to_string(), "rustfs-labs".to_string()],
            // Deny-by-default: operators must explicitly allow the hosts
            // artifacts may be downloaded from before any install can plan.
            allowed_download_hosts: Vec::new(),
            require_https: true,
            require_signature: true,
            require_provenance: true,
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
        if artifact.digest_sha256.len() != SHA256_HEX_DIGEST_LEN
            || !artifact.digest_sha256.chars().all(|ch| ch.is_ascii_hexdigit())
        {
            return Err(format!(
                "artifact {} has invalid digest_sha256 {} (expected {} hex characters)",
                artifact.artifact_id, artifact.digest_sha256, SHA256_HEX_DIGEST_LEN
            ));
        }
        if policy.require_signature && artifact.signature_uri.is_empty() {
            return Err(format!("artifact {} must declare a signature uri", artifact.artifact_id));
        }
        if policy.require_provenance && artifact.provenance_uri.is_empty() {
            return Err(format!("artifact {} must declare a provenance uri", artifact.artifact_id));
        }
        validate_artifact_uri("signature", artifact.artifact_id, artifact.signature_uri, policy)?;
        validate_artifact_uri("provenance", artifact.artifact_id, artifact.provenance_uri, policy)?;
    }

    Ok(())
}

fn validate_external_action_subject(manifest: &TargetPluginMarketplaceManifest) -> Result<(), TargetPluginExternalActionError> {
    if manifest.packaging != TargetPluginPackaging::External {
        return Err(TargetPluginExternalActionError::NotExternalPlugin {
            plugin_id: manifest.plugin_id.to_string(),
        });
    }

    Ok(())
}

fn validate_external_action_gate(gate: &TargetPluginExternalFlowGate) -> Result<(), TargetPluginExternalActionError> {
    if !gate.enabled {
        return Err(TargetPluginExternalActionError::ExternalFlowDisabled);
    }
    if !gate.circuit_breaker_closed {
        return Err(TargetPluginExternalActionError::CircuitBreakerOpen);
    }
    gate.runtime_policy
        .validate_activation(&gate.runtime_safety_checks)
        .map_err(|reason| TargetPluginExternalActionError::RuntimePolicyDenied {
            reason: reason.to_string(),
        })
}

fn plan_external_install(
    manifest: &TargetPluginMarketplaceManifest,
    action: TargetPluginExternalAction,
    gate: &TargetPluginExternalFlowGate,
    host_target_triple: &str,
) -> Result<TargetPluginExternalActionDecision, TargetPluginExternalActionError> {
    let install_manifest = TargetPluginManifest {
        plugin_id: manifest.plugin_id,
        display_name: manifest.display_name,
        provider: manifest.provider,
        version: manifest.version,
        target_type: manifest.target_type,
        supported_domains: manifest.supported_domains,
        secret_fields: manifest.secret_fields,
    };
    validate_external_plugin_installation(
        &install_manifest,
        &manifest.runtime_contract,
        manifest.distribution,
        &gate.install_policy,
    )
    .map_err(|reason| TargetPluginExternalActionError::InstallPolicyDenied { reason })?;

    let artifact = manifest
        .distribution
        .and_then(|distribution| {
            distribution
                .artifacts
                .iter()
                .find(|artifact| artifact.target_triple == host_target_triple)
        })
        .ok_or_else(|| TargetPluginExternalActionError::MissingArtifactForHost {
            plugin_id: manifest.plugin_id.to_string(),
            target_triple: host_target_triple.to_string(),
        })?;

    Ok(TargetPluginExternalActionDecision {
        action,
        plugin_id: manifest.plugin_id.to_string(),
        installation: external_target_plugin_installation(manifest.version, artifact.digest_sha256, artifact.artifact_id, None),
        operational_state: TargetPluginOperationalState {
            install_state: TargetPluginInstallState::Installed,
            enable_state: TargetPluginEnableState::Disabled,
            runtime_state: TargetPluginRuntimeState::Offline,
        },
    })
}

fn require_installed(plugin_id: &str, installation: &TargetPluginInstallation) -> Result<(), TargetPluginExternalActionError> {
    if installation.install_state != TargetPluginInstallState::Installed || installation.current_revision.is_none() {
        return Err(TargetPluginExternalActionError::NotInstalled {
            plugin_id: plugin_id.to_string(),
        });
    }

    Ok(())
}

fn plan_external_rollback(
    manifest: &TargetPluginMarketplaceManifest,
    action: TargetPluginExternalAction,
    installation: &TargetPluginInstallation,
) -> Result<TargetPluginExternalActionDecision, TargetPluginExternalActionError> {
    require_installed(manifest.plugin_id, installation)?;

    let Some(current) = installation.current_revision.clone() else {
        return Err(TargetPluginExternalActionError::NotInstalled {
            plugin_id: manifest.plugin_id.to_string(),
        });
    };
    let Some(previous) = installation.previous_revision.clone() else {
        return Err(TargetPluginExternalActionError::MissingPreviousRevision {
            plugin_id: manifest.plugin_id.to_string(),
        });
    };

    Ok(TargetPluginExternalActionDecision {
        action,
        plugin_id: manifest.plugin_id.to_string(),
        installation: rollback_target_plugin_installation(current, previous),
        operational_state: TargetPluginOperationalState {
            install_state: TargetPluginInstallState::Installed,
            enable_state: TargetPluginEnableState::Disabled,
            runtime_state: TargetPluginRuntimeState::Offline,
        },
    })
}

fn validate_artifact_uri(label: &str, artifact_id: &str, uri: &str, policy: &TargetPluginInstallPolicy) -> Result<(), String> {
    if uri.is_empty() {
        return Ok(());
    }

    let parsed_uri = Url::parse(uri).map_err(|err| format!("invalid artifact {label} uri {uri}: {err}"))?;
    if policy.require_https && parsed_uri.scheme() != "https" {
        return Err(format!("artifact {artifact_id} must use https {label} uri, got {uri}"));
    }
    let host = parsed_uri
        .host_str()
        .ok_or_else(|| format!("artifact {artifact_id} {label} uri has no host"))?;
    if !policy.allowed_download_hosts.iter().any(|allowed| allowed == host) {
        return Err(format!("artifact {artifact_id} {label} host {host} is not allowed"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        TargetPluginEnableState, TargetPluginExternalAction, TargetPluginExternalActionError, TargetPluginExternalFlowGate,
        TargetPluginInstallPolicy, TargetPluginInstallState, TargetPluginInstallation, TargetPluginRevision,
        TargetPluginRuntimeState, builtin_target_plugin_installation, builtin_target_plugin_operational_state,
        external_target_plugin_installation, failed_external_target_plugin_installation, plan_external_target_plugin_action,
        rollback_target_plugin_installation, runtime_state_from_status_label, validate_external_plugin_installation,
    };
    use crate::catalog::example_external_webhook_plugin;
    use crate::manifest::{
        TargetPluginArtifactManifest, TargetPluginDistributionManifest, TargetPluginExternalRuntimeContract,
        TargetPluginManifest, TargetPluginRuntimeTransport, builtin_target_manifest, builtin_target_marketplace_manifest,
    };
    use crate::{SidecarRuntimePolicy, SidecarRuntimeSafetyChecks};
    use std::time::Duration;

    const TEST_HOST_TRIPLE: &str = "x86_64-unknown-linux-gnu";

    fn policy_allowing_example_host() -> TargetPluginInstallPolicy {
        TargetPluginInstallPolicy {
            allowed_download_hosts: vec!["plugins.example.test".to_string()],
            ..TargetPluginInstallPolicy::default()
        }
    }

    fn verified_gate_with_example_host() -> TargetPluginExternalFlowGate {
        let mut gate = TargetPluginExternalFlowGate::verified(
            SidecarRuntimePolicy::verified_external(16, Duration::from_secs(5), 3),
            SidecarRuntimeSafetyChecks::verified(0),
        );
        gate.install_policy = policy_allowing_example_host();
        gate
    }

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
    fn external_action_gate_is_disabled_by_default() {
        let example = example_external_webhook_plugin();
        let gate = TargetPluginExternalFlowGate::default();

        let result = plan_external_target_plugin_action(
            &example.manifest,
            TargetPluginExternalAction::Install,
            &TargetPluginInstallation {
                install_state: TargetPluginInstallState::NotInstalled,
                current_revision: None,
                previous_revision: None,
                validation_error: None,
            },
            &gate,
            TEST_HOST_TRIPLE,
        );

        assert_eq!(result, Err(TargetPluginExternalActionError::ExternalFlowDisabled));
        assert!(!gate.status().enabled);
        assert!(gate.status().install_requires_signature);
        assert!(gate.status().install_requires_provenance);
    }

    #[test]
    fn external_action_rejects_builtin_manifest() {
        let gate = TargetPluginExternalFlowGate::verified(
            SidecarRuntimePolicy::verified_external(16, Duration::from_secs(5), 3),
            SidecarRuntimeSafetyChecks::verified(0),
        );

        let result = plan_external_target_plugin_action(
            &builtin_target_marketplace_manifest("webhook"),
            TargetPluginExternalAction::Install,
            &TargetPluginInstallation {
                install_state: TargetPluginInstallState::NotInstalled,
                current_revision: None,
                previous_revision: None,
                validation_error: None,
            },
            &gate,
            TEST_HOST_TRIPLE,
        );

        assert_eq!(
            result,
            Err(TargetPluginExternalActionError::NotExternalPlugin {
                plugin_id: "builtin:webhook".to_string()
            })
        );
    }

    #[test]
    fn external_action_install_requires_signature_and_provenance() {
        const MISSING_PROVENANCE_ARTIFACTS: &[TargetPluginArtifactManifest] = &[TargetPluginArtifactManifest {
            artifact_id: "sidecar-linux-amd64",
            target_triple: "x86_64-unknown-linux-gnu",
            download_uri: "https://plugins.example.test/webhook-sidecar.tar.zst",
            digest_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            signature_uri: "https://plugins.example.test/webhook-sidecar.tar.zst.sig",
            provenance_uri: "",
            size_bytes: 8192,
        }];

        let mut example = example_external_webhook_plugin();
        example.manifest.distribution = Some(TargetPluginDistributionManifest {
            artifacts: MISSING_PROVENANCE_ARTIFACTS,
        });
        let gate = verified_gate_with_example_host();

        let result = plan_external_target_plugin_action(
            &example.manifest,
            TargetPluginExternalAction::Install,
            &TargetPluginInstallation {
                install_state: TargetPluginInstallState::NotInstalled,
                current_revision: None,
                previous_revision: None,
                validation_error: None,
            },
            &gate,
            TEST_HOST_TRIPLE,
        );

        assert_eq!(
            result,
            Err(TargetPluginExternalActionError::InstallPolicyDenied {
                reason: "artifact sidecar-linux-amd64 must declare a provenance uri".to_string()
            })
        );
    }

    #[test]
    fn external_action_enable_requires_sandbox_and_provenance() {
        let example = example_external_webhook_plugin();
        let gate = TargetPluginExternalFlowGate::verified(
            SidecarRuntimePolicy::verified_external(16, Duration::from_secs(5), 3),
            SidecarRuntimeSafetyChecks {
                sandboxed: false,
                provenance_verified: true,
                queue_depth: 0,
            },
        );

        let result = plan_external_target_plugin_action(
            &example.manifest,
            TargetPluginExternalAction::Enable,
            &example.installation,
            &gate,
            TEST_HOST_TRIPLE,
        );

        assert_eq!(
            result,
            Err(TargetPluginExternalActionError::RuntimePolicyDenied {
                reason: "sidecar runtime requires sandbox isolation".to_string()
            })
        );
    }

    #[test]
    fn external_action_enable_requires_closed_circuit_breaker() {
        let example = example_external_webhook_plugin();
        let mut gate = TargetPluginExternalFlowGate::verified(
            SidecarRuntimePolicy::verified_external(16, Duration::from_secs(5), 3),
            SidecarRuntimeSafetyChecks::verified(0),
        );
        gate.circuit_breaker_closed = false;

        let result = plan_external_target_plugin_action(
            &example.manifest,
            TargetPluginExternalAction::Enable,
            &example.installation,
            &gate,
            TEST_HOST_TRIPLE,
        );

        assert_eq!(result, Err(TargetPluginExternalActionError::CircuitBreakerOpen));
    }

    #[test]
    fn external_actions_plan_install_disable_and_rollback_without_execution() {
        let example = example_external_webhook_plugin();
        let gate = verified_gate_with_example_host();

        let install = plan_external_target_plugin_action(
            &example.manifest,
            TargetPluginExternalAction::Install,
            &TargetPluginInstallation {
                install_state: TargetPluginInstallState::NotInstalled,
                current_revision: None,
                previous_revision: None,
                validation_error: None,
            },
            &gate,
            TEST_HOST_TRIPLE,
        )
        .expect("verified external install action should plan");
        assert_eq!(install.installation.install_state, TargetPluginInstallState::Installed);
        assert_eq!(install.operational_state.enable_state, TargetPluginEnableState::Disabled);
        assert_eq!(install.operational_state.runtime_state, TargetPluginRuntimeState::Offline);

        let disable = plan_external_target_plugin_action(
            &example.manifest,
            TargetPluginExternalAction::Disable,
            &example.installation,
            &gate,
            TEST_HOST_TRIPLE,
        )
        .expect("verified external disable action should plan");
        assert_eq!(disable.operational_state.enable_state, TargetPluginEnableState::Disabled);
        assert_eq!(disable.operational_state.runtime_state, TargetPluginRuntimeState::Offline);

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
        let rollback = plan_external_target_plugin_action(
            &example.manifest,
            TargetPluginExternalAction::Rollback,
            &TargetPluginInstallation {
                install_state: TargetPluginInstallState::Installed,
                current_revision: Some(current.clone()),
                previous_revision: Some(previous.clone()),
                validation_error: None,
            },
            &gate,
            TEST_HOST_TRIPLE,
        )
        .expect("verified external rollback action should plan");

        assert_eq!(rollback.installation.current_revision, Some(previous));
        assert_eq!(rollback.installation.previous_revision, Some(current));
        assert_eq!(rollback.operational_state.enable_state, TargetPluginEnableState::Disabled);
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
                digest_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                signature_uri: "https://plugins.example.test/webhook-sidecar.tar.zst.sig",
                provenance_uri: "https://plugins.example.test/webhook-sidecar.tar.zst.intoto.jsonl",
                size_bytes: 8192,
            }],
        };
        let policy = policy_allowing_example_host();

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
    fn default_install_policy_denies_all_download_hosts() {
        let policy = TargetPluginInstallPolicy::default();
        assert!(policy.allowed_download_hosts.is_empty());

        let manifest = TargetPluginManifest {
            plugin_id: "external:webhook-sidecar",
            display_name: "Webhook Sidecar",
            provider: "rustfs-labs",
            version: "1.0.0",
            target_type: "webhook",
            supported_domains: &[],
            secret_fields: &[],
        };
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
                    digest_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                    signature_uri: "https://plugins.example.test/webhook-sidecar.tar.zst.sig",
                    provenance_uri: "https://plugins.example.test/webhook-sidecar.tar.zst.intoto.jsonl",
                    size_bytes: 8192,
                }],
            }),
            &policy,
        );

        assert_eq!(
            result.as_ref().map_err(String::as_str),
            Err("artifact sidecar-linux-amd64 download host plugins.example.test is not allowed")
        );
    }

    #[test]
    fn validate_external_installation_rejects_truncated_digest() {
        let manifest = TargetPluginManifest {
            plugin_id: "external:webhook-sidecar",
            display_name: "Webhook Sidecar",
            provider: "rustfs-labs",
            version: "1.0.0",
            target_type: "webhook",
            supported_domains: &[],
            secret_fields: &[],
        };
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
                    signature_uri: "https://plugins.example.test/webhook-sidecar.tar.zst.sig",
                    provenance_uri: "https://plugins.example.test/webhook-sidecar.tar.zst.intoto.jsonl",
                    size_bytes: 8192,
                }],
            }),
            &policy_allowing_example_host(),
        );

        assert_eq!(
            result.as_ref().map_err(String::as_str),
            Err(
                "artifact sidecar-linux-amd64 has invalid digest_sha256 0123456789abcdef0123456789abcdef (expected 64 hex characters)"
            )
        );
    }

    #[test]
    fn external_install_requires_artifact_for_host_target_triple() {
        let example = example_external_webhook_plugin();
        let gate = verified_gate_with_example_host();

        let result = plan_external_target_plugin_action(
            &example.manifest,
            TargetPluginExternalAction::Install,
            &TargetPluginInstallation {
                install_state: TargetPluginInstallState::NotInstalled,
                current_revision: None,
                previous_revision: None,
                validation_error: None,
            },
            &gate,
            "aarch64-apple-darwin",
        );

        assert_eq!(
            result,
            Err(TargetPluginExternalActionError::MissingArtifactForHost {
                plugin_id: "external:webhook-sidecar".to_string(),
                target_triple: "aarch64-apple-darwin".to_string(),
            })
        );
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
        let policy = policy_allowing_example_host();

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
                    digest_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                    signature_uri: "https://plugins.example.test/webhook-sidecar.tar.zst.sig",
                    provenance_uri: "https://plugins.example.test/webhook-sidecar.tar.zst.intoto.jsonl",
                    size_bytes: 8192,
                }],
            }),
            &policy,
        );

        assert!(result.is_err());
    }

    #[test]
    fn validate_external_installation_rejects_missing_artifact_signature() {
        let manifest = TargetPluginManifest {
            plugin_id: "external:webhook-sidecar",
            display_name: "Webhook Sidecar",
            provider: "rustfs-labs",
            version: "1.0.0",
            target_type: "webhook",
            supported_domains: &[],
            secret_fields: &[],
        };
        let policy = policy_allowing_example_host();

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
                    digest_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                    signature_uri: "",
                    provenance_uri: "https://plugins.example.test/webhook-sidecar.intoto.jsonl",
                    size_bytes: 8192,
                }],
            }),
            &policy,
        );

        assert_eq!(
            result.as_ref().map_err(String::as_str),
            Err("artifact sidecar-linux-amd64 must declare a signature uri")
        );
    }

    #[test]
    fn validate_external_installation_rejects_missing_artifact_provenance() {
        let manifest = TargetPluginManifest {
            plugin_id: "external:webhook-sidecar",
            display_name: "Webhook Sidecar",
            provider: "rustfs-labs",
            version: "1.0.0",
            target_type: "webhook",
            supported_domains: &[],
            secret_fields: &[],
        };
        let policy = policy_allowing_example_host();

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
                    digest_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                    signature_uri: "https://plugins.example.test/webhook-sidecar.sig",
                    provenance_uri: "",
                    size_bytes: 8192,
                }],
            }),
            &policy,
        );

        assert_eq!(
            result.as_ref().map_err(String::as_str),
            Err("artifact sidecar-linux-amd64 must declare a provenance uri")
        );
    }
}
