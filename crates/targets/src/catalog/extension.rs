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

use std::collections::BTreeMap;

use crate::catalog::builtin::{builtin_audit_target_admin_descriptors, builtin_notify_target_admin_descriptors};
use crate::domain::TargetDomain;
use crate::manifest::{
    TargetPluginEntrypointKind, TargetPluginMarketplaceManifest, TargetPluginPackaging, builtin_target_marketplace_manifest,
};
use rustfs_extension_schema::{
    EXTENSION_SCHEMA_VERSION, ExtensionCapabilityRef, ExtensionKind, ExtensionRuntimeBoundary, ExtensionRuntimeContract,
    ExtensionSchema,
};

pub const TARGET_AUDIT_CAPABILITY: &str = "target.audit.v1";
pub const TARGET_NOTIFY_CAPABILITY: &str = "target.notify.v1";

pub fn target_marketplace_extension_schema(manifest: &TargetPluginMarketplaceManifest) -> ExtensionSchema {
    let boundary = target_runtime_boundary(manifest.entrypoint_kind);

    ExtensionSchema {
        schema_version: EXTENSION_SCHEMA_VERSION.to_string(),
        extension_id: manifest.plugin_id.to_string(),
        display_name: manifest.display_name.to_string(),
        provider: manifest.provider.to_string(),
        version: manifest.version.to_string(),
        kind: ExtensionKind::TargetPlugin,
        runtime: ExtensionRuntimeContract {
            api_version: manifest.api_compatibility_version.to_string(),
            boundary,
        },
        capabilities: manifest
            .supported_domains
            .iter()
            .copied()
            .map(target_domain_capability)
            .collect(),
        disabled_by_default: target_disabled_by_default(manifest.packaging, boundary),
    }
}

pub fn builtin_target_extension_schemas() -> Vec<ExtensionSchema> {
    let mut target_types = BTreeMap::new();

    for descriptor in builtin_audit_target_admin_descriptors()
        .into_iter()
        .chain(builtin_notify_target_admin_descriptors())
    {
        target_types
            .entry(descriptor.manifest().target_type)
            .or_insert_with(|| builtin_target_marketplace_manifest(descriptor.manifest().target_type));
    }

    target_types.values().map(target_marketplace_extension_schema).collect()
}

pub const fn target_runtime_boundary(entrypoint_kind: TargetPluginEntrypointKind) -> ExtensionRuntimeBoundary {
    match entrypoint_kind {
        TargetPluginEntrypointKind::Builtin => ExtensionRuntimeBoundary::Builtin,
        TargetPluginEntrypointKind::Sidecar => ExtensionRuntimeBoundary::Sidecar,
        TargetPluginEntrypointKind::Wasm => ExtensionRuntimeBoundary::Wasm,
    }
}

const fn target_disabled_by_default(packaging: TargetPluginPackaging, boundary: ExtensionRuntimeBoundary) -> bool {
    matches!(packaging, TargetPluginPackaging::External) || boundary.requires_disabled_by_default()
}

fn target_domain_capability(domain: TargetDomain) -> ExtensionCapabilityRef {
    match domain {
        TargetDomain::Audit => ExtensionCapabilityRef::new(TARGET_AUDIT_CAPABILITY),
        TargetDomain::Notify => ExtensionCapabilityRef::new(TARGET_NOTIFY_CAPABILITY),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        TARGET_AUDIT_CAPABILITY, TARGET_NOTIFY_CAPABILITY, builtin_target_extension_schemas, target_marketplace_extension_schema,
    };
    use crate::catalog::example_external_webhook_plugin;
    use crate::domain::TargetDomain;
    use crate::manifest::{
        TargetPluginExternalRuntimeContract, TargetPluginPackaging, TargetPluginRuntimeTransport,
        builtin_target_marketplace_manifest,
    };
    use rustfs_extension_schema::{
        EXTENSION_SCHEMA_VERSION, ExtensionKind, ExtensionRuntimeBoundary, validate_extension_schemas,
    };

    #[test]
    fn builtin_target_marketplace_manifest_maps_to_builtin_extension_schema() {
        let schema = target_marketplace_extension_schema(&builtin_target_marketplace_manifest("webhook"));

        assert_eq!(schema.schema_version, EXTENSION_SCHEMA_VERSION);
        assert_eq!(schema.extension_id, "builtin:webhook");
        assert_eq!(schema.display_name, "Webhook");
        assert_eq!(schema.kind, ExtensionKind::TargetPlugin);
        assert_eq!(schema.runtime.boundary, ExtensionRuntimeBoundary::Builtin);
        assert!(!schema.disabled_by_default);
        assert_eq!(schema.runtime.api_version, "rustfs.target-plugin.v1");
        assert_eq!(schema.capabilities.len(), 2);
        assert_eq!(schema.capabilities[0].as_str(), TARGET_AUDIT_CAPABILITY);
        assert_eq!(schema.capabilities[1].as_str(), TARGET_NOTIFY_CAPABILITY);
    }

    #[test]
    fn external_sidecar_target_manifest_maps_to_disabled_sidecar_extension_schema() {
        let example = example_external_webhook_plugin();
        let schema = target_marketplace_extension_schema(&example.manifest);

        assert_eq!(schema.extension_id, "external:webhook-sidecar");
        assert_eq!(schema.kind, ExtensionKind::TargetPlugin);
        assert_eq!(schema.runtime.boundary, ExtensionRuntimeBoundary::Sidecar);
        assert!(schema.disabled_by_default);
        assert_eq!(schema.capabilities.len(), 1);
        assert_eq!(schema.capabilities[0].as_str(), TARGET_NOTIFY_CAPABILITY);
        assert!(validate_extension_schemas(&[schema]).is_ok());
    }

    #[test]
    fn external_packaging_stays_disabled_even_with_builtin_entrypoint_metadata() {
        let mut manifest = builtin_target_marketplace_manifest("webhook");
        manifest.packaging = TargetPluginPackaging::External;
        manifest.runtime_contract = TargetPluginExternalRuntimeContract {
            protocol_version: "rustfs.target-runtime.v1",
            transport: TargetPluginRuntimeTransport::InProcess,
        };
        manifest.supported_domains = &[TargetDomain::Notify];

        let schema = target_marketplace_extension_schema(&manifest);

        assert_eq!(schema.runtime.boundary, ExtensionRuntimeBoundary::Builtin);
        assert!(schema.disabled_by_default);
    }

    #[test]
    fn builtin_target_extension_catalog_is_unique_and_schema_valid() {
        let schemas = builtin_target_extension_schemas();
        let mut extension_ids = schemas.iter().map(|schema| schema.extension_id.as_str()).collect::<Vec<_>>();
        extension_ids.sort_unstable();

        assert_eq!(schemas.len(), 9);
        assert_eq!(
            extension_ids,
            vec![
                "builtin:amqp",
                "builtin:kafka",
                "builtin:mqtt",
                "builtin:mysql",
                "builtin:nats",
                "builtin:postgres",
                "builtin:pulsar",
                "builtin:redis",
                "builtin:webhook",
            ]
        );
        assert!(schemas.iter().all(|schema| schema.kind == ExtensionKind::TargetPlugin));
        assert!(
            schemas
                .iter()
                .all(|schema| schema.runtime.boundary == ExtensionRuntimeBoundary::Builtin)
        );
        assert!(schemas.iter().all(|schema| !schema.disabled_by_default));
        assert!(validate_extension_schemas(&schemas).is_ok());
    }
}
