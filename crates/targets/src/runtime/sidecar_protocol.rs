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

use crate::TargetDomain;
use serde::{Deserialize, Serialize};

pub const SIDECAR_RUNTIME_PROTOCOL_VERSION: &str = "rustfs.target-runtime.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SidecarPluginCapability {
    HealthCheck,
    SendEvent,
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SidecarHandshake {
    pub protocol_version: String,
    pub plugin_id: String,
    pub plugin_version: String,
    pub supported_domains: Vec<TargetDomain>,
    pub capabilities: Vec<SidecarPluginCapability>,
}

impl SidecarHandshake {
    pub fn validate(&self, expected_plugin_id: &str) -> Result<(), String> {
        if self.protocol_version != SIDECAR_RUNTIME_PROTOCOL_VERSION {
            return Err(format!(
                "unsupported sidecar protocol version: expected {}, got {}",
                SIDECAR_RUNTIME_PROTOCOL_VERSION, self.protocol_version
            ));
        }

        if self.plugin_id != expected_plugin_id {
            return Err(format!(
                "sidecar plugin id mismatch: expected {}, got {}",
                expected_plugin_id, self.plugin_id
            ));
        }

        for capability in [
            SidecarPluginCapability::HealthCheck,
            SidecarPluginCapability::SendEvent,
            SidecarPluginCapability::Shutdown,
        ] {
            if !self.capabilities.contains(&capability) {
                return Err(format!("sidecar handshake missing required capability: {:?}", capability));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{SIDECAR_RUNTIME_PROTOCOL_VERSION, SidecarHandshake, SidecarPluginCapability};
    use crate::TargetDomain;

    #[test]
    fn sidecar_handshake_accepts_expected_contract() {
        let handshake = SidecarHandshake {
            protocol_version: SIDECAR_RUNTIME_PROTOCOL_VERSION.to_string(),
            plugin_id: "external:webhook".to_string(),
            plugin_version: "1.2.3".to_string(),
            supported_domains: vec![TargetDomain::Notify],
            capabilities: vec![
                SidecarPluginCapability::HealthCheck,
                SidecarPluginCapability::SendEvent,
                SidecarPluginCapability::Shutdown,
            ],
        };

        assert!(handshake.validate("external:webhook").is_ok());
    }

    #[test]
    fn sidecar_handshake_rejects_protocol_mismatch() {
        let handshake = SidecarHandshake {
            protocol_version: "rustfs.target-runtime.v0".to_string(),
            plugin_id: "external:webhook".to_string(),
            plugin_version: "1.2.3".to_string(),
            supported_domains: vec![TargetDomain::Notify],
            capabilities: vec![
                SidecarPluginCapability::HealthCheck,
                SidecarPluginCapability::SendEvent,
                SidecarPluginCapability::Shutdown,
            ],
        };

        assert!(handshake.validate("external:webhook").is_err());
    }
}
