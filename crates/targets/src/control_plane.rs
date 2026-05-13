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

use crate::manifest::TargetPluginManifest;
use serde::{Deserialize, Serialize};

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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TargetPluginInstallation {
    pub install_state: TargetPluginInstallState,
    pub current_revision: Option<TargetPluginRevision>,
    pub previous_revision: Option<TargetPluginRevision>,
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
        }),
        previous_revision: None,
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

#[cfg(test)]
mod tests {
    use super::{
        TargetPluginEnableState, TargetPluginInstallState, TargetPluginRuntimeState, builtin_target_plugin_installation,
        builtin_target_plugin_operational_state, runtime_state_from_status_label,
    };
    use crate::manifest::builtin_target_manifest;

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
        assert!(installation.previous_revision.is_none());
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
}
