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

pub mod builtin;

use crate::control_plane::external_target_plugin_installation;
use crate::domain::TargetDomain;
use crate::manifest::{
    TargetPluginArtifactManifest, TargetPluginDistributionManifest, TargetPluginEntrypointKind,
    TargetPluginExternalRuntimeContract, TargetPluginManifest, TargetPluginMarketplaceManifest, TargetPluginRuntimeTransport,
    installable_target_marketplace_manifest,
};
use crate::runtime::sidecar::SidecarPluginRuntime;
use crate::runtime::sidecar_protocol::{SIDECAR_RUNTIME_PROTOCOL_VERSION, SidecarHandshake, SidecarPluginCapability};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExampleInstallableTargetPlugin {
    pub manifest: TargetPluginMarketplaceManifest,
    pub installation: crate::TargetPluginInstallation,
    pub runtime: SidecarPluginRuntime,
    pub valid_fields: Vec<String>,
}

pub fn example_external_webhook_plugin() -> ExampleInstallableTargetPlugin {
    let base = TargetPluginManifest {
        plugin_id: "external:webhook-sidecar",
        display_name: "Webhook Sidecar",
        provider: "rustfs-labs",
        version: "1.0.0",
        target_type: "webhook",
        supported_domains: &[TargetDomain::Notify],
        secret_fields: &["auth_token"],
    };
    let manifest = installable_target_marketplace_manifest(
        base,
        TargetPluginEntrypointKind::Sidecar,
        TargetPluginExternalRuntimeContract {
            protocol_version: SIDECAR_RUNTIME_PROTOCOL_VERSION,
            transport: TargetPluginRuntimeTransport::Grpc,
        },
        TargetPluginDistributionManifest {
            artifacts: &[TargetPluginArtifactManifest {
                artifact_id: "sidecar-linux-amd64",
                target_triple: "x86_64-unknown-linux-gnu",
                download_uri: "https://plugins.example.test/webhook-sidecar.tar.zst",
                digest_sha256: "0123456789abcdef0123456789abcdef",
                size_bytes: 8192,
            }],
        },
    );

    let handshake = SidecarHandshake {
        protocol_version: SIDECAR_RUNTIME_PROTOCOL_VERSION.to_string(),
        plugin_id: base.plugin_id.to_string(),
        plugin_version: base.version.to_string(),
        supported_domains: vec![TargetDomain::Notify],
        capabilities: vec![
            SidecarPluginCapability::HealthCheck,
            SidecarPluginCapability::SendEvent,
            SidecarPluginCapability::Shutdown,
        ],
    };
    let mut runtime = SidecarPluginRuntime::new("grpc://127.0.0.1:50051", handshake);
    runtime
        .enable(base.plugin_id, TargetDomain::Notify)
        .expect("example sidecar plugin handshake should validate");

    ExampleInstallableTargetPlugin {
        manifest,
        installation: external_target_plugin_installation(
            base.version,
            "0123456789abcdef0123456789abcdef",
            "sidecar-linux-amd64",
            Some("2026-05-13T20:00:00Z".to_string()),
        ),
        runtime,
        valid_fields: vec!["endpoint".to_string(), "auth_token".to_string()],
    }
}

#[cfg(test)]
mod tests {
    use super::example_external_webhook_plugin;

    #[test]
    fn example_external_plugin_exposes_installation_and_runtime_metadata() {
        let example = example_external_webhook_plugin();

        assert_eq!(example.manifest.plugin_id, "external:webhook-sidecar");
        assert_eq!(example.installation.install_state, crate::TargetPluginInstallState::Installed);
        assert!(example.runtime.healthy);
        assert_eq!(example.valid_fields, vec!["endpoint".to_string(), "auth_token".to_string()]);
    }
}
