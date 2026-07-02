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
    ExtensionSchema, OPS_DIAGNOSTICS_CAPABILITY, OPS_PROFILER_CAPABILITY, OpsDiagnosticSurface, OpsDiagnosticsContract,
    OpsProfilerBackendCapability, OpsProfilerBackendName, OpsProfilerBackendStatus, OpsProfilerContract, OpsProfilerContractMode,
    OpsProfilerProvenance, OpsProfilerRedactionField, OpsProfilerTrustLevel, S3_POST_AUTH_HOOK_CAPABILITY, S3HookContract,
    S3HookPoint,
};

pub const OPS_DIAGNOSTICS_EXTENSION_API_VERSION: &str = "rustfs.ops-diagnostics.v1";
pub const OPS_PROFILER_EXTENSION_API_VERSION: &str = "rustfs.ops-profiler.v1";
pub const S3_HOOK_EXTENSION_API_VERSION: &str = "rustfs.s3-hook.v1";
pub const TARGET_AUDIT_CAPABILITY: &str = "target.audit.v1";
pub const TARGET_NOTIFY_CAPABILITY: &str = "target.notify.v1";

pub fn builtin_extension_schemas() -> Vec<ExtensionSchema> {
    let mut schemas = builtin_target_extension_schemas();
    schemas.push(builtin_s3_hook_extension_schema());
    schemas.push(builtin_ops_diagnostics_extension_schema());
    schemas.push(builtin_ops_profiler_extension_schema());
    schemas
}

pub fn builtin_s3_hook_extension_schema() -> ExtensionSchema {
    ExtensionSchema {
        schema_version: EXTENSION_SCHEMA_VERSION.to_string(),
        extension_id: "builtin:s3-post-auth-hooks".to_string(),
        display_name: "S3 Post-Auth Hook Registry".to_string(),
        provider: "rustfs".to_string(),
        version: "1.0.0".to_string(),
        kind: ExtensionKind::S3Hook,
        runtime: ExtensionRuntimeContract {
            api_version: S3_HOOK_EXTENSION_API_VERSION.to_string(),
            boundary: ExtensionRuntimeBoundary::Builtin,
        },
        capabilities: vec![ExtensionCapabilityRef::new(S3_POST_AUTH_HOOK_CAPABILITY)],
        disabled_by_default: false,
    }
}

pub fn builtin_s3_hook_contract() -> S3HookContract {
    S3HookContract {
        hook_points: vec![
            S3HookPoint::PostAuthGetObject,
            S3HookPoint::PostAuthPutObject,
            S3HookPoint::PostAuthDeleteObject,
            S3HookPoint::PostAuthListObjects,
        ],
        mutates_object_data: false,
        bypasses_iam: false,
    }
}

pub fn builtin_ops_diagnostics_extension_schema() -> ExtensionSchema {
    ExtensionSchema {
        schema_version: EXTENSION_SCHEMA_VERSION.to_string(),
        extension_id: "builtin:ops-diagnostics".to_string(),
        display_name: "Ops Diagnostics".to_string(),
        provider: "rustfs".to_string(),
        version: "1.0.0".to_string(),
        kind: ExtensionKind::OpsDiagnostics,
        runtime: ExtensionRuntimeContract {
            api_version: OPS_DIAGNOSTICS_EXTENSION_API_VERSION.to_string(),
            boundary: ExtensionRuntimeBoundary::Builtin,
        },
        capabilities: vec![ExtensionCapabilityRef::new(OPS_DIAGNOSTICS_CAPABILITY)],
        disabled_by_default: false,
    }
}

pub fn builtin_ops_diagnostics_contract() -> OpsDiagnosticsContract {
    OpsDiagnosticsContract {
        surfaces: vec![
            OpsDiagnosticSurface::Metrics,
            OpsDiagnosticSurface::Trace,
            OpsDiagnosticSurface::Profile,
            OpsDiagnosticSurface::Health,
            OpsDiagnosticSurface::Diagnostics,
        ],
        mutates_object_data: false,
        requires_admin_action: true,
    }
}

pub fn builtin_ops_profiler_extension_schema() -> ExtensionSchema {
    ExtensionSchema {
        schema_version: EXTENSION_SCHEMA_VERSION.to_string(),
        extension_id: "builtin:ops-profiler".to_string(),
        display_name: "Ops Profiler".to_string(),
        provider: "rustfs".to_string(),
        version: "1.0.0".to_string(),
        kind: ExtensionKind::OpsProfiler,
        runtime: ExtensionRuntimeContract {
            api_version: OPS_PROFILER_EXTENSION_API_VERSION.to_string(),
            boundary: ExtensionRuntimeBoundary::Builtin,
        },
        capabilities: vec![ExtensionCapabilityRef::new(OPS_PROFILER_CAPABILITY)],
        disabled_by_default: false,
    }
}

pub fn builtin_ops_profiler_contract() -> OpsProfilerContract {
    OpsProfilerContract {
        mode: OpsProfilerContractMode::CapabilityDescription,
        backends: vec![
            builtin_ops_profiler_backend("cpu_pprof", OpsProfilerBackendStatus::Unsupported, false),
            builtin_ops_profiler_backend("memory_pprof", OpsProfilerBackendStatus::Unsupported, false),
            builtin_ops_profiler_backend("ebpf", OpsProfilerBackendStatus::Unsupported, false),
        ],
    }
}

fn builtin_ops_profiler_backend(
    backend: &str,
    status: OpsProfilerBackendStatus,
    supports_profile_export: bool,
) -> OpsProfilerBackendCapability {
    OpsProfilerBackendCapability {
        backend: OpsProfilerBackendName::new(backend),
        status,
        supports_profile_export,
        redaction_required: vec![
            OpsProfilerRedactionField::Secret,
            OpsProfilerRedactionField::Token,
            OpsProfilerRedactionField::LocalPath,
            OpsProfilerRedactionField::Host,
        ],
        provenance: OpsProfilerProvenance {
            source: "rustfs.profiling".to_string(),
            collection_boundary: "rustfs-process".to_string(),
            trust_level: OpsProfilerTrustLevel::RuntimeTrusted,
        },
    }
}

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
        OPS_DIAGNOSTICS_EXTENSION_API_VERSION, OPS_PROFILER_EXTENSION_API_VERSION, S3_HOOK_EXTENSION_API_VERSION,
        TARGET_AUDIT_CAPABILITY, TARGET_NOTIFY_CAPABILITY, builtin_extension_schemas, builtin_ops_diagnostics_contract,
        builtin_ops_diagnostics_extension_schema, builtin_ops_profiler_contract, builtin_ops_profiler_extension_schema,
        builtin_s3_hook_contract, builtin_s3_hook_extension_schema, builtin_target_extension_schemas,
        target_marketplace_extension_schema,
    };
    use crate::catalog::example_external_webhook_plugin;
    use crate::domain::TargetDomain;
    use crate::manifest::{
        TargetPluginExternalRuntimeContract, TargetPluginPackaging, TargetPluginRuntimeTransport,
        builtin_target_marketplace_manifest,
    };
    use rustfs_extension_schema::{
        EXTENSION_SCHEMA_VERSION, ExtensionKind, ExtensionRuntimeBoundary, OPS_DIAGNOSTICS_CAPABILITY, OPS_PROFILER_CAPABILITY,
        OpsDiagnosticSurface, OpsProfilerBackendStatus, S3_POST_AUTH_HOOK_CAPABILITY, S3HookPoint, validate_extension_schemas,
        validate_ops_diagnostics_contract, validate_ops_profiler_contract, validate_s3_hook_contract,
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

    #[test]
    fn builtin_extension_catalog_includes_hooks_and_ops_capabilities() {
        let schemas = builtin_extension_schemas();
        let mut extension_ids = schemas.iter().map(|schema| schema.extension_id.as_str()).collect::<Vec<_>>();
        extension_ids.sort_unstable();

        assert_eq!(schemas.len(), 12);
        assert!(extension_ids.contains(&"builtin:s3-post-auth-hooks"));
        assert!(extension_ids.contains(&"builtin:ops-diagnostics"));
        assert!(extension_ids.contains(&"builtin:ops-profiler"));
        assert!(validate_extension_schemas(&schemas).is_ok());
    }

    #[test]
    fn builtin_s3_hook_schema_declares_post_auth_noop_contract() {
        let schema = builtin_s3_hook_extension_schema();
        let contract = builtin_s3_hook_contract();

        assert_eq!(schema.kind, ExtensionKind::S3Hook);
        assert_eq!(schema.runtime.boundary, ExtensionRuntimeBoundary::Builtin);
        assert_eq!(schema.runtime.api_version, S3_HOOK_EXTENSION_API_VERSION);
        assert_eq!(schema.capabilities[0].as_str(), S3_POST_AUTH_HOOK_CAPABILITY);
        assert!(!schema.disabled_by_default);
        assert_eq!(
            contract.hook_points,
            vec![
                S3HookPoint::PostAuthGetObject,
                S3HookPoint::PostAuthPutObject,
                S3HookPoint::PostAuthDeleteObject,
                S3HookPoint::PostAuthListObjects,
            ]
        );
        assert!(contract.hook_points.iter().all(|hook_point| hook_point.is_post_auth()));
        assert!(!contract.mutates_object_data);
        assert!(!contract.bypasses_iam);
        assert!(validate_s3_hook_contract(&contract).is_ok());
    }

    #[test]
    fn builtin_ops_diagnostics_schema_is_admin_read_only() {
        let schema = builtin_ops_diagnostics_extension_schema();
        let contract = builtin_ops_diagnostics_contract();

        assert_eq!(schema.kind, ExtensionKind::OpsDiagnostics);
        assert_eq!(schema.runtime.boundary, ExtensionRuntimeBoundary::Builtin);
        assert_eq!(schema.runtime.api_version, OPS_DIAGNOSTICS_EXTENSION_API_VERSION);
        assert_eq!(schema.capabilities[0].as_str(), OPS_DIAGNOSTICS_CAPABILITY);
        assert!(!schema.disabled_by_default);
        assert_eq!(
            contract.surfaces,
            vec![
                OpsDiagnosticSurface::Metrics,
                OpsDiagnosticSurface::Trace,
                OpsDiagnosticSurface::Profile,
                OpsDiagnosticSurface::Health,
                OpsDiagnosticSurface::Diagnostics,
            ]
        );
        assert!(!contract.mutates_object_data);
        assert!(contract.requires_admin_action);
        assert!(validate_ops_diagnostics_contract(&contract).is_ok());
    }

    #[test]
    fn builtin_ops_profiler_schema_describes_read_only_profile_capabilities() {
        let schema = builtin_ops_profiler_extension_schema();
        let contract = builtin_ops_profiler_contract();

        assert_eq!(schema.kind, ExtensionKind::OpsProfiler);
        assert_eq!(schema.runtime.boundary, ExtensionRuntimeBoundary::Builtin);
        assert_eq!(schema.runtime.api_version, OPS_PROFILER_EXTENSION_API_VERSION);
        assert_eq!(schema.capabilities[0].as_str(), OPS_PROFILER_CAPABILITY);
        assert!(!schema.disabled_by_default);

        let backends = contract
            .backends
            .iter()
            .map(|backend| (backend.backend.as_str(), backend.status, backend.supports_profile_export))
            .collect::<Vec<_>>();
        assert_eq!(
            backends,
            vec![
                ("cpu_pprof", OpsProfilerBackendStatus::Unsupported, false),
                ("memory_pprof", OpsProfilerBackendStatus::Unsupported, false),
                ("ebpf", OpsProfilerBackendStatus::Unsupported, false),
            ]
        );
        assert!(
            contract
                .backends
                .iter()
                .filter(|backend| backend.supports_profile_export)
                .all(|backend| backend
                    .redaction_required
                    .contains(&rustfs_extension_schema::OpsProfilerRedactionField::LocalPath))
        );
        assert!(validate_ops_profiler_contract(&contract).is_ok());
    }
}
