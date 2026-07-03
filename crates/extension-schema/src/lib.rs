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

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const EXTENSION_SCHEMA_VERSION: &str = "rustfs.extension-schema.v1";
pub const OPS_DIAGNOSTICS_CAPABILITY: &str = "ops.diagnostics.v1";
pub const OPS_PROFILER_CAPABILITY: &str = "ops.profiler.v1";
pub const S3_POST_AUTH_HOOK_CAPABILITY: &str = "s3.hook.post_auth.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionKind {
    TargetPlugin,
    S3Hook,
    OpsDiagnostics,
    OpsProfiler,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionRuntimeBoundary {
    Builtin,
    Sidecar,
    Wasm,
}

impl ExtensionRuntimeBoundary {
    pub const fn requires_disabled_by_default(self) -> bool {
        matches!(self, Self::Sidecar | Self::Wasm)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtensionRuntimeContract {
    pub api_version: String,
    pub boundary: ExtensionRuntimeBoundary,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExtensionCapabilityRef(String);

impl ExtensionCapabilityRef {
    pub fn new(capability: impl Into<String>) -> Self {
        Self(capability.into())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtensionSchema {
    pub schema_version: String,
    pub extension_id: String,
    pub display_name: String,
    pub provider: String,
    pub version: String,
    pub kind: ExtensionKind,
    pub runtime: ExtensionRuntimeContract,
    pub capabilities: Vec<ExtensionCapabilityRef>,
    pub disabled_by_default: bool,
}

/// Every supported hook point runs strictly after authentication and
/// authorization; pre-auth hook points are intentionally not representable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum S3HookPoint {
    PostAuthGetObject,
    PostAuthPutObject,
    PostAuthDeleteObject,
    PostAuthListObjects,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3HookContract {
    pub hook_points: Vec<S3HookPoint>,
    pub mutates_object_data: bool,
    pub bypasses_iam: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpsDiagnosticSurface {
    Metrics,
    Trace,
    Profile,
    Health,
    Diagnostics,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpsDiagnosticsContract {
    pub surfaces: Vec<OpsDiagnosticSurface>,
    pub mutates_object_data: bool,
    pub requires_admin_action: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpsProfilerContractMode {
    CapabilityDescription,
    ExecutionRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OpsProfilerBackendName(String);

impl OpsProfilerBackendName {
    pub fn new(backend: impl Into<String>) -> Self {
        Self(backend.into())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpsProfilerBackendStatus {
    Enabled,
    Disabled,
    Unsupported,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpsProfilerRedactionField {
    Secret,
    Token,
    LocalPath,
    Host,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpsProfilerTrustLevel {
    RuntimeTrusted,
    AdminTrusted,
    ExtensionProvided,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpsProfilerProvenance {
    pub source: String,
    pub collection_boundary: String,
    pub trust_level: OpsProfilerTrustLevel,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpsProfilerBackendCapability {
    pub backend: OpsProfilerBackendName,
    pub status: OpsProfilerBackendStatus,
    pub supports_profile_export: bool,
    pub redaction_required: Vec<OpsProfilerRedactionField>,
    pub provenance: OpsProfilerProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpsProfilerContract {
    pub mode: OpsProfilerContractMode,
    pub backends: Vec<OpsProfilerBackendCapability>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpsProfilerRuntimeSnapshot {
    pub boundary: ExtensionRuntimeBoundary,
    pub disabled_by_default: bool,
    pub startup_fatal: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpsProfilerCapabilitySnapshot {
    pub capability: ExtensionCapabilityRef,
    pub runtime: OpsProfilerRuntimeSnapshot,
    pub contract: OpsProfilerContract,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExtensionSchemaError {
    #[error("extension schema at index {index} has an empty extension id")]
    EmptyExtensionId { index: usize },

    #[error("extension schema for {extension_id} has an unsupported schema version {schema_version}")]
    UnsupportedSchemaVersion { extension_id: String, schema_version: String },

    #[error("extension schema for {extension_id} has an empty display name")]
    EmptyDisplayName { extension_id: String },

    #[error("extension schema for {extension_id} has an empty provider")]
    EmptyProvider { extension_id: String },

    #[error("extension schema for {extension_id} has an empty version")]
    EmptyVersion { extension_id: String },

    #[error("extension schema for {extension_id} has an empty runtime API version")]
    EmptyRuntimeApiVersion { extension_id: String },

    #[error("extension schema for {extension_id} must declare at least one capability")]
    EmptyCapabilities { extension_id: String },

    #[error("extension schema for {extension_id} has an empty capability")]
    EmptyCapability { extension_id: String },

    #[error("extension schema for {extension_id} duplicates capability {capability}")]
    DuplicateCapability { extension_id: String, capability: String },

    #[error("external extension schema for {extension_id} must be disabled by default")]
    ExternalMustBeDisabledByDefault { extension_id: String },

    #[error("duplicate extension schema for {extension_id}")]
    DuplicateExtension { extension_id: String },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExtensionContractError {
    #[error("s3 hook contract must declare at least one hook point")]
    EmptyS3HookPoints,

    #[error("s3 hook contract duplicates hook point {hook_point:?}")]
    DuplicateS3HookPoint { hook_point: S3HookPoint },

    #[error("s3 hook contract cannot mutate object data")]
    S3HookMutatesObjectData,

    #[error("s3 hook contract cannot bypass IAM")]
    S3HookBypassesIam,

    #[error("ops diagnostics contract must declare at least one surface")]
    EmptyOpsDiagnosticSurfaces,

    #[error("ops diagnostics contract duplicates surface {surface:?}")]
    DuplicateOpsDiagnosticSurface { surface: OpsDiagnosticSurface },

    #[error("ops diagnostics contract cannot mutate object data")]
    OpsDiagnosticsMutatesObjectData,

    #[error("ops diagnostics contract must require an admin action")]
    OpsDiagnosticsMissingAdminAction,

    #[error("ops profiler contract must describe capabilities, not execution requests")]
    OpsProfilerExecutionRequest,

    #[error("ops profiler contract must declare at least one backend")]
    EmptyOpsProfilerBackends,

    #[error("ops profiler contract has an empty backend name")]
    EmptyOpsProfilerBackend,

    #[error("ops profiler contract duplicates backend {backend}")]
    DuplicateOpsProfilerBackend { backend: String },

    #[error("ops profiler backend {backend} duplicates redaction field {field:?}")]
    DuplicateOpsProfilerRedactionField {
        backend: String,
        field: OpsProfilerRedactionField,
    },

    #[error("ops profiler backend {backend} exports profiles without local path redaction")]
    OpsProfilerMissingLocalPathRedaction { backend: String },

    #[error("ops profiler backend {backend} has an empty provenance source")]
    EmptyOpsProfilerProvenanceSource { backend: String },

    #[error("ops profiler backend {backend} has an empty collection boundary")]
    EmptyOpsProfilerCollectionBoundary { backend: String },

    #[error("ops profiler snapshot has unsupported capability {capability}")]
    UnsupportedOpsProfilerCapability { capability: String },

    #[error("ops profiler external runtime must be disabled by default")]
    OpsProfilerExternalRuntimeEnabledByDefault,

    #[error("ops profiler runtime snapshot cannot add a startup fatal boundary")]
    OpsProfilerStartupFatalBoundary,
}

pub fn validate_extension_schemas(schemas: &[ExtensionSchema]) -> Result<(), ExtensionSchemaError> {
    let mut extension_ids = BTreeSet::new();

    for (index, schema) in schemas.iter().enumerate() {
        let extension_id = schema.extension_id.trim();
        if extension_id.is_empty() {
            return Err(ExtensionSchemaError::EmptyExtensionId { index });
        }

        if schema.schema_version != EXTENSION_SCHEMA_VERSION {
            return Err(ExtensionSchemaError::UnsupportedSchemaVersion {
                extension_id: schema.extension_id.clone(),
                schema_version: schema.schema_version.clone(),
            });
        }

        if schema.display_name.trim().is_empty() {
            return Err(ExtensionSchemaError::EmptyDisplayName {
                extension_id: schema.extension_id.clone(),
            });
        }

        if schema.provider.trim().is_empty() {
            return Err(ExtensionSchemaError::EmptyProvider {
                extension_id: schema.extension_id.clone(),
            });
        }

        if schema.version.trim().is_empty() {
            return Err(ExtensionSchemaError::EmptyVersion {
                extension_id: schema.extension_id.clone(),
            });
        }

        if schema.runtime.api_version.trim().is_empty() {
            return Err(ExtensionSchemaError::EmptyRuntimeApiVersion {
                extension_id: schema.extension_id.clone(),
            });
        }

        if schema.capabilities.is_empty() {
            return Err(ExtensionSchemaError::EmptyCapabilities {
                extension_id: schema.extension_id.clone(),
            });
        }

        let mut capabilities = BTreeSet::new();
        for capability in &schema.capabilities {
            if capability.as_str().trim().is_empty() {
                return Err(ExtensionSchemaError::EmptyCapability {
                    extension_id: schema.extension_id.clone(),
                });
            }

            if !capabilities.insert(capability.as_str()) {
                return Err(ExtensionSchemaError::DuplicateCapability {
                    extension_id: schema.extension_id.clone(),
                    capability: capability.as_str().to_string(),
                });
            }
        }

        if schema.runtime.boundary.requires_disabled_by_default() && !schema.disabled_by_default {
            return Err(ExtensionSchemaError::ExternalMustBeDisabledByDefault {
                extension_id: schema.extension_id.clone(),
            });
        }

        if !extension_ids.insert(schema.extension_id.as_str()) {
            return Err(ExtensionSchemaError::DuplicateExtension {
                extension_id: schema.extension_id.clone(),
            });
        }
    }

    Ok(())
}

pub fn validate_s3_hook_contract(contract: &S3HookContract) -> Result<(), ExtensionContractError> {
    if contract.hook_points.is_empty() {
        return Err(ExtensionContractError::EmptyS3HookPoints);
    }

    let mut hook_points = BTreeSet::new();
    for hook_point in &contract.hook_points {
        if !hook_points.insert(*hook_point) {
            return Err(ExtensionContractError::DuplicateS3HookPoint { hook_point: *hook_point });
        }
    }

    if contract.mutates_object_data {
        return Err(ExtensionContractError::S3HookMutatesObjectData);
    }

    if contract.bypasses_iam {
        return Err(ExtensionContractError::S3HookBypassesIam);
    }

    Ok(())
}

pub fn validate_ops_diagnostics_contract(contract: &OpsDiagnosticsContract) -> Result<(), ExtensionContractError> {
    if contract.surfaces.is_empty() {
        return Err(ExtensionContractError::EmptyOpsDiagnosticSurfaces);
    }

    let mut surfaces = BTreeSet::new();
    for surface in &contract.surfaces {
        if !surfaces.insert(*surface) {
            return Err(ExtensionContractError::DuplicateOpsDiagnosticSurface { surface: *surface });
        }
    }

    if contract.mutates_object_data {
        return Err(ExtensionContractError::OpsDiagnosticsMutatesObjectData);
    }

    if !contract.requires_admin_action {
        return Err(ExtensionContractError::OpsDiagnosticsMissingAdminAction);
    }

    Ok(())
}

pub fn validate_ops_profiler_contract(contract: &OpsProfilerContract) -> Result<(), ExtensionContractError> {
    if contract.mode != OpsProfilerContractMode::CapabilityDescription {
        return Err(ExtensionContractError::OpsProfilerExecutionRequest);
    }

    if contract.backends.is_empty() {
        return Err(ExtensionContractError::EmptyOpsProfilerBackends);
    }

    let mut backends = BTreeSet::new();
    for backend in &contract.backends {
        let backend_name = backend.backend.as_str().trim();
        if backend_name.is_empty() {
            return Err(ExtensionContractError::EmptyOpsProfilerBackend);
        }

        if !backends.insert(backend_name) {
            return Err(ExtensionContractError::DuplicateOpsProfilerBackend {
                backend: backend_name.to_string(),
            });
        }

        let mut redaction_fields = BTreeSet::new();
        for field in &backend.redaction_required {
            if !redaction_fields.insert(*field) {
                return Err(ExtensionContractError::DuplicateOpsProfilerRedactionField {
                    backend: backend_name.to_string(),
                    field: *field,
                });
            }
        }

        if backend.supports_profile_export && !redaction_fields.contains(&OpsProfilerRedactionField::LocalPath) {
            return Err(ExtensionContractError::OpsProfilerMissingLocalPathRedaction {
                backend: backend_name.to_string(),
            });
        }

        if backend.provenance.source.trim().is_empty() {
            return Err(ExtensionContractError::EmptyOpsProfilerProvenanceSource {
                backend: backend_name.to_string(),
            });
        }

        if backend.provenance.collection_boundary.trim().is_empty() {
            return Err(ExtensionContractError::EmptyOpsProfilerCollectionBoundary {
                backend: backend_name.to_string(),
            });
        }
    }

    Ok(())
}

pub fn validate_ops_profiler_capability_snapshot(snapshot: &OpsProfilerCapabilitySnapshot) -> Result<(), ExtensionContractError> {
    if snapshot.capability.as_str() != OPS_PROFILER_CAPABILITY {
        return Err(ExtensionContractError::UnsupportedOpsProfilerCapability {
            capability: snapshot.capability.as_str().to_string(),
        });
    }

    if snapshot.runtime.boundary.requires_disabled_by_default() && !snapshot.runtime.disabled_by_default {
        return Err(ExtensionContractError::OpsProfilerExternalRuntimeEnabledByDefault);
    }

    if snapshot.runtime.startup_fatal {
        return Err(ExtensionContractError::OpsProfilerStartupFatalBoundary);
    }

    validate_ops_profiler_contract(&snapshot.contract)
}

#[cfg(test)]
mod tests {
    use super::{
        EXTENSION_SCHEMA_VERSION, ExtensionCapabilityRef, ExtensionContractError, ExtensionKind, ExtensionRuntimeBoundary,
        ExtensionRuntimeContract, ExtensionSchema, ExtensionSchemaError, OPS_DIAGNOSTICS_CAPABILITY, OPS_PROFILER_CAPABILITY,
        OpsDiagnosticSurface, OpsDiagnosticsContract, OpsProfilerBackendCapability, OpsProfilerBackendName,
        OpsProfilerBackendStatus, OpsProfilerCapabilitySnapshot, OpsProfilerContract, OpsProfilerContractMode,
        OpsProfilerProvenance, OpsProfilerRedactionField, OpsProfilerRuntimeSnapshot, OpsProfilerTrustLevel,
        S3_POST_AUTH_HOOK_CAPABILITY, S3HookContract, S3HookPoint, validate_extension_schemas, validate_ops_diagnostics_contract,
        validate_ops_profiler_capability_snapshot, validate_ops_profiler_contract, validate_s3_hook_contract,
    };
    use serde_json::json;

    fn target_schema(
        extension_id: &str,
        capability: &str,
        boundary: ExtensionRuntimeBoundary,
        disabled_by_default: bool,
    ) -> ExtensionSchema {
        ExtensionSchema {
            schema_version: EXTENSION_SCHEMA_VERSION.to_string(),
            extension_id: extension_id.to_string(),
            display_name: "Webhook Target".to_string(),
            provider: "rustfs".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::TargetPlugin,
            runtime: ExtensionRuntimeContract {
                api_version: "rustfs.extension.v1".to_string(),
                boundary,
            },
            capabilities: vec![ExtensionCapabilityRef::new(capability)],
            disabled_by_default,
        }
    }

    #[test]
    fn extension_schema_serializes_stable_json_shape() {
        let schema = target_schema("rustfs.builtin.webhook", "target.notify.v1", ExtensionRuntimeBoundary::Builtin, false);

        let value = serde_json::to_value(schema).expect("extension schema should serialize");

        assert_eq!(
            value,
            json!({
                "schema_version": "rustfs.extension-schema.v1",
                "extension_id": "rustfs.builtin.webhook",
                "display_name": "Webhook Target",
                "provider": "rustfs",
                "version": "1.0.0",
                "kind": "target_plugin",
                "runtime": {
                    "api_version": "rustfs.extension.v1",
                    "boundary": "builtin"
                },
                "capabilities": ["target.notify.v1"],
                "disabled_by_default": false
            })
        );
    }

    #[test]
    fn validates_extension_schema_contracts() {
        let schemas = [
            ExtensionSchema {
                schema_version: EXTENSION_SCHEMA_VERSION.to_string(),
                extension_id: "rustfs.ops.diagnostics".to_string(),
                display_name: "Ops Diagnostics".to_string(),
                provider: "rustfs".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::OpsDiagnostics,
                runtime: ExtensionRuntimeContract {
                    api_version: "rustfs.extension.v1".to_string(),
                    boundary: ExtensionRuntimeBoundary::Builtin,
                },
                capabilities: vec![ExtensionCapabilityRef::new(OPS_DIAGNOSTICS_CAPABILITY)],
                disabled_by_default: false,
            },
            ExtensionSchema {
                schema_version: EXTENSION_SCHEMA_VERSION.to_string(),
                extension_id: "rustfs.ops.profiler".to_string(),
                display_name: "Ops Profiler".to_string(),
                provider: "rustfs".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::OpsProfiler,
                runtime: ExtensionRuntimeContract {
                    api_version: "rustfs.extension.v1".to_string(),
                    boundary: ExtensionRuntimeBoundary::Builtin,
                },
                capabilities: vec![ExtensionCapabilityRef::new(OPS_PROFILER_CAPABILITY)],
                disabled_by_default: false,
            },
        ];

        assert!(validate_extension_schemas(&schemas).is_ok());
    }

    #[test]
    fn rejects_external_extensions_enabled_by_default() {
        let schemas = [target_schema(
            "external.sidecar.audit",
            "target.audit.v1",
            ExtensionRuntimeBoundary::Sidecar,
            false,
        )];

        let err = validate_extension_schemas(&schemas).expect_err("external sidecars must start disabled");

        assert_eq!(
            err,
            ExtensionSchemaError::ExternalMustBeDisabledByDefault {
                extension_id: "external.sidecar.audit".to_string()
            }
        );
    }

    #[test]
    fn rejects_duplicate_extension_capabilities() {
        let mut schema = target_schema("rustfs.builtin.webhook", "target.notify.v1", ExtensionRuntimeBoundary::Builtin, false);
        schema.capabilities.push(ExtensionCapabilityRef::new("target.notify.v1"));
        let schemas = [schema];

        let err = validate_extension_schemas(&schemas).expect_err("duplicate capabilities should fail validation");

        assert_eq!(
            err,
            ExtensionSchemaError::DuplicateCapability {
                extension_id: "rustfs.builtin.webhook".to_string(),
                capability: "target.notify.v1".to_string()
            }
        );
    }

    #[test]
    fn rejects_duplicate_extension_ids() {
        let schemas = [
            target_schema("rustfs.builtin.webhook", "target.notify.v1", ExtensionRuntimeBoundary::Builtin, false),
            target_schema("rustfs.builtin.webhook", "target.notify.v2", ExtensionRuntimeBoundary::Builtin, false),
        ];

        let err = validate_extension_schemas(&schemas).expect_err("duplicate extension ids should fail validation");

        assert_eq!(
            err,
            ExtensionSchemaError::DuplicateExtension {
                extension_id: "rustfs.builtin.webhook".to_string()
            }
        );
    }

    #[test]
    fn rejects_unknown_schema_version() {
        let mut schema = target_schema("rustfs.builtin.webhook", "target.notify.v1", ExtensionRuntimeBoundary::Builtin, false);
        schema.schema_version = "rustfs.extension-schema.v0".to_string();
        let schemas = [schema];

        let err = validate_extension_schemas(&schemas).expect_err("unknown schema version should fail validation");

        assert_eq!(
            err,
            ExtensionSchemaError::UnsupportedSchemaVersion {
                extension_id: "rustfs.builtin.webhook".to_string(),
                schema_version: "rustfs.extension-schema.v0".to_string()
            }
        );
    }

    #[test]
    fn rejects_empty_capabilities() {
        let mut schema = target_schema("rustfs.builtin.webhook", "target.notify.v1", ExtensionRuntimeBoundary::Builtin, false);
        schema.capabilities.clear();
        let schemas = [schema];

        let err = validate_extension_schemas(&schemas).expect_err("extension capabilities must be explicit");

        assert_eq!(
            err,
            ExtensionSchemaError::EmptyCapabilities {
                extension_id: "rustfs.builtin.webhook".to_string()
            }
        );
    }

    #[test]
    fn validates_s3_post_auth_hook_contract() {
        let contract = S3HookContract {
            hook_points: vec![S3HookPoint::PostAuthGetObject, S3HookPoint::PostAuthPutObject],
            mutates_object_data: false,
            bypasses_iam: false,
        };

        assert!(validate_s3_hook_contract(&contract).is_ok());
        assert_eq!(S3_POST_AUTH_HOOK_CAPABILITY, "s3.hook.post_auth.v1");
    }

    #[test]
    fn rejects_s3_hooks_that_mutate_or_bypass_iam() {
        let mut contract = S3HookContract {
            hook_points: vec![S3HookPoint::PostAuthGetObject],
            mutates_object_data: true,
            bypasses_iam: false,
        };

        assert_eq!(
            validate_s3_hook_contract(&contract).expect_err("object mutation should be rejected"),
            ExtensionContractError::S3HookMutatesObjectData
        );

        contract.mutates_object_data = false;
        contract.bypasses_iam = true;

        assert_eq!(
            validate_s3_hook_contract(&contract).expect_err("IAM bypass should be rejected"),
            ExtensionContractError::S3HookBypassesIam
        );
    }

    #[test]
    fn rejects_duplicate_s3_hook_points() {
        let contract = S3HookContract {
            hook_points: vec![S3HookPoint::PostAuthDeleteObject, S3HookPoint::PostAuthDeleteObject],
            mutates_object_data: false,
            bypasses_iam: false,
        };

        assert_eq!(
            validate_s3_hook_contract(&contract).expect_err("duplicate hook points should fail validation"),
            ExtensionContractError::DuplicateS3HookPoint {
                hook_point: S3HookPoint::PostAuthDeleteObject
            }
        );
    }

    #[test]
    fn validates_ops_diagnostics_contract() {
        let contract = OpsDiagnosticsContract {
            surfaces: vec![
                OpsDiagnosticSurface::Metrics,
                OpsDiagnosticSurface::Trace,
                OpsDiagnosticSurface::Profile,
                OpsDiagnosticSurface::Health,
                OpsDiagnosticSurface::Diagnostics,
            ],
            mutates_object_data: false,
            requires_admin_action: true,
        };

        assert!(validate_ops_diagnostics_contract(&contract).is_ok());
        assert_eq!(OPS_DIAGNOSTICS_CAPABILITY, "ops.diagnostics.v1");
    }

    fn profiler_backend(
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

    #[test]
    fn validates_ops_profiler_contract_states_and_redaction() {
        let contract = OpsProfilerContract {
            mode: OpsProfilerContractMode::CapabilityDescription,
            backends: vec![
                profiler_backend("pyroscope", OpsProfilerBackendStatus::Enabled, true),
                profiler_backend("memory_pprof", OpsProfilerBackendStatus::Disabled, false),
                profiler_backend("ebpf", OpsProfilerBackendStatus::Unsupported, false),
                profiler_backend("future_kernel_profiler", OpsProfilerBackendStatus::Unknown, false),
            ],
        };

        assert!(validate_ops_profiler_contract(&contract).is_ok());
        assert_eq!(OPS_PROFILER_CAPABILITY, "ops.profiler.v1");
        assert!(
            contract
                .backends
                .iter()
                .all(|backend| !backend.provenance.source.contains("token"))
        );
    }

    #[test]
    fn ops_profiler_schema_serializes_stable_json_shape() {
        let contract = OpsProfilerContract {
            mode: OpsProfilerContractMode::CapabilityDescription,
            backends: vec![profiler_backend("pyroscope", OpsProfilerBackendStatus::Enabled, true)],
        };

        let value = serde_json::to_value(contract).expect("ops profiler schema should serialize");

        assert_eq!(
            value,
            json!({
                "mode": "capability_description",
                "backends": [{
                    "backend": "pyroscope",
                    "status": "enabled",
                    "supports_profile_export": true,
                    "redaction_required": ["secret", "token", "local_path", "host"],
                    "provenance": {
                        "source": "rustfs.profiling",
                        "collection_boundary": "rustfs-process",
                        "trust_level": "runtime_trusted"
                    }
                }]
            })
        );
    }

    #[test]
    fn ops_profiler_schema_accepts_unknown_future_backend_names() {
        let contract: OpsProfilerContract = serde_json::from_value(json!({
            "mode": "capability_description",
            "backends": [{
                "backend": "vendor.future-profiler",
                "status": "unknown",
                "supports_profile_export": false,
                "redaction_required": ["host"],
                "provenance": {
                    "source": "extension.schema",
                    "collection_boundary": "read_only_inventory",
                    "trust_level": "unknown"
                }
            }]
        }))
        .expect("unknown future backend names should remain representable");

        assert!(validate_ops_profiler_contract(&contract).is_ok());
    }

    #[test]
    fn ops_profiler_capability_snapshot_preserves_runtime_states() {
        let snapshot = OpsProfilerCapabilitySnapshot {
            capability: ExtensionCapabilityRef::new(OPS_PROFILER_CAPABILITY),
            runtime: OpsProfilerRuntimeSnapshot {
                boundary: ExtensionRuntimeBoundary::Sidecar,
                disabled_by_default: true,
                startup_fatal: false,
            },
            contract: OpsProfilerContract {
                mode: OpsProfilerContractMode::CapabilityDescription,
                backends: vec![
                    profiler_backend("pyroscope", OpsProfilerBackendStatus::Enabled, true),
                    profiler_backend("memory_pprof", OpsProfilerBackendStatus::Disabled, false),
                    profiler_backend("ebpf", OpsProfilerBackendStatus::Unsupported, false),
                ],
            },
        };

        assert!(validate_ops_profiler_capability_snapshot(&snapshot).is_ok());

        let encoded = serde_json::to_string(&snapshot).expect("ops profiler snapshot should serialize");
        let decoded: OpsProfilerCapabilitySnapshot =
            serde_json::from_str(&encoded).expect("ops profiler snapshot should deserialize");

        let states: Vec<_> = decoded.contract.backends.iter().map(|backend| backend.status).collect();
        assert_eq!(
            states,
            vec![
                OpsProfilerBackendStatus::Enabled,
                OpsProfilerBackendStatus::Disabled,
                OpsProfilerBackendStatus::Unsupported,
            ]
        );
        assert_eq!(decoded.runtime.boundary, ExtensionRuntimeBoundary::Sidecar);
        assert!(decoded.runtime.disabled_by_default);
        assert!(!decoded.runtime.startup_fatal);
    }

    #[test]
    fn ops_profiler_capability_snapshot_serializes_stable_json_shape() {
        let snapshot = OpsProfilerCapabilitySnapshot {
            capability: ExtensionCapabilityRef::new(OPS_PROFILER_CAPABILITY),
            runtime: OpsProfilerRuntimeSnapshot {
                boundary: ExtensionRuntimeBoundary::Builtin,
                disabled_by_default: false,
                startup_fatal: false,
            },
            contract: OpsProfilerContract {
                mode: OpsProfilerContractMode::CapabilityDescription,
                backends: vec![profiler_backend("pyroscope", OpsProfilerBackendStatus::Enabled, true)],
            },
        };

        let value = serde_json::to_value(snapshot).expect("ops profiler snapshot should serialize");

        assert_eq!(
            value,
            json!({
                "capability": "ops.profiler.v1",
                "runtime": {
                    "boundary": "builtin",
                    "disabled_by_default": false,
                    "startup_fatal": false
                },
                "contract": {
                    "mode": "capability_description",
                    "backends": [{
                        "backend": "pyroscope",
                        "status": "enabled",
                        "supports_profile_export": true,
                        "redaction_required": ["secret", "token", "local_path", "host"],
                        "provenance": {
                            "source": "rustfs.profiling",
                            "collection_boundary": "rustfs-process",
                            "trust_level": "runtime_trusted"
                        }
                    }]
                }
            })
        );
    }

    #[test]
    fn rejects_ops_profiler_snapshot_wrong_capability_or_fatal_runtime() {
        let mut snapshot = OpsProfilerCapabilitySnapshot {
            capability: ExtensionCapabilityRef::new("ops.not-profiler.v1"),
            runtime: OpsProfilerRuntimeSnapshot {
                boundary: ExtensionRuntimeBoundary::Sidecar,
                disabled_by_default: true,
                startup_fatal: false,
            },
            contract: OpsProfilerContract {
                mode: OpsProfilerContractMode::CapabilityDescription,
                backends: vec![profiler_backend("pyroscope", OpsProfilerBackendStatus::Enabled, true)],
            },
        };

        assert_eq!(
            validate_ops_profiler_capability_snapshot(&snapshot).expect_err("only ops.profiler.v1 snapshots are accepted"),
            ExtensionContractError::UnsupportedOpsProfilerCapability {
                capability: "ops.not-profiler.v1".to_string()
            }
        );

        snapshot.capability = ExtensionCapabilityRef::new(OPS_PROFILER_CAPABILITY);
        snapshot.runtime.disabled_by_default = false;

        assert_eq!(
            validate_ops_profiler_capability_snapshot(&snapshot)
                .expect_err("external profiler runtimes must stay disabled by default"),
            ExtensionContractError::OpsProfilerExternalRuntimeEnabledByDefault
        );

        snapshot.runtime.disabled_by_default = true;
        snapshot.runtime.startup_fatal = true;

        assert_eq!(
            validate_ops_profiler_capability_snapshot(&snapshot)
                .expect_err("optional profiler runtimes must not become startup fatal"),
            ExtensionContractError::OpsProfilerStartupFatalBoundary
        );
    }

    #[test]
    fn rejects_ops_profiler_execution_requests_and_empty_backends() {
        let mut contract = OpsProfilerContract {
            mode: OpsProfilerContractMode::ExecutionRequest,
            backends: vec![profiler_backend("pyroscope", OpsProfilerBackendStatus::Enabled, true)],
        };

        assert_eq!(
            validate_ops_profiler_contract(&contract).expect_err("execution requests are outside schema scope"),
            ExtensionContractError::OpsProfilerExecutionRequest
        );

        contract.mode = OpsProfilerContractMode::CapabilityDescription;
        contract.backends.clear();

        assert_eq!(
            validate_ops_profiler_contract(&contract).expect_err("profiler capability must declare backends"),
            ExtensionContractError::EmptyOpsProfilerBackends
        );
    }

    #[test]
    fn rejects_ops_profiler_missing_required_fields() {
        let err = serde_json::from_value::<OpsProfilerContract>(json!({
            "mode": "capability_description",
            "backends": [{
                "backend": "pyroscope",
                "status": "enabled",
                "supports_profile_export": true,
                "redaction_required": ["local_path"]
            }]
        }))
        .expect_err("provenance is required");

        assert!(err.to_string().contains("missing field `provenance`"));
    }

    #[test]
    fn rejects_ops_profiler_unknown_credential_fields() {
        let err = serde_json::from_value::<OpsProfilerContract>(json!({
            "mode": "capability_description",
            "backends": [{
                "backend": "pyroscope",
                "status": "enabled",
                "supports_profile_export": true,
                "redaction_required": ["local_path"],
                "provenance": {
                    "source": "rustfs.profiling",
                    "collection_boundary": "rustfs-process",
                    "trust_level": "runtime_trusted",
                    "credentials": "not-allowed"
                }
            }]
        }))
        .expect_err("credential-bearing fields are outside the schema");

        assert!(err.to_string().contains("unknown field `credentials`"));
    }

    #[test]
    fn rejects_ops_profiler_invalid_backend_and_redaction_shape() {
        let mut contract = OpsProfilerContract {
            mode: OpsProfilerContractMode::CapabilityDescription,
            backends: vec![profiler_backend("pyroscope", OpsProfilerBackendStatus::Enabled, true)],
        };
        contract.backends[0].backend = OpsProfilerBackendName::new(" ");

        assert_eq!(
            validate_ops_profiler_contract(&contract).expect_err("backend name should be required"),
            ExtensionContractError::EmptyOpsProfilerBackend
        );

        contract.backends[0] = profiler_backend("pyroscope", OpsProfilerBackendStatus::Enabled, true);
        contract
            .backends
            .push(profiler_backend("pyroscope", OpsProfilerBackendStatus::Disabled, false));

        assert_eq!(
            validate_ops_profiler_contract(&contract).expect_err("duplicate backends should fail validation"),
            ExtensionContractError::DuplicateOpsProfilerBackend {
                backend: "pyroscope".to_string()
            }
        );

        contract.backends.truncate(1);
        contract.backends[0].redaction_required.push(OpsProfilerRedactionField::Host);

        assert_eq!(
            validate_ops_profiler_contract(&contract).expect_err("duplicate redaction fields should fail validation"),
            ExtensionContractError::DuplicateOpsProfilerRedactionField {
                backend: "pyroscope".to_string(),
                field: OpsProfilerRedactionField::Host
            }
        );

        contract.backends[0].redaction_required = vec![OpsProfilerRedactionField::Host];

        assert_eq!(
            validate_ops_profiler_contract(&contract).expect_err("profile export requires local path redaction"),
            ExtensionContractError::OpsProfilerMissingLocalPathRedaction {
                backend: "pyroscope".to_string()
            }
        );
    }

    #[test]
    fn rejects_ops_profiler_missing_provenance_boundary() {
        let mut contract = OpsProfilerContract {
            mode: OpsProfilerContractMode::CapabilityDescription,
            backends: vec![profiler_backend("pyroscope", OpsProfilerBackendStatus::Enabled, true)],
        };
        contract.backends[0].provenance.source = " ".to_string();

        assert_eq!(
            validate_ops_profiler_contract(&contract).expect_err("provenance source should be required"),
            ExtensionContractError::EmptyOpsProfilerProvenanceSource {
                backend: "pyroscope".to_string()
            }
        );

        contract.backends[0].provenance.source = "rustfs.profiling".to_string();
        contract.backends[0].provenance.collection_boundary.clear();

        assert_eq!(
            validate_ops_profiler_contract(&contract).expect_err("collection boundary should be required"),
            ExtensionContractError::EmptyOpsProfilerCollectionBoundary {
                backend: "pyroscope".to_string()
            }
        );
    }

    #[test]
    fn rejects_ops_diagnostics_without_admin_action_or_read_only_contract() {
        let mut contract = OpsDiagnosticsContract {
            surfaces: vec![OpsDiagnosticSurface::Metrics],
            mutates_object_data: false,
            requires_admin_action: false,
        };

        assert_eq!(
            validate_ops_diagnostics_contract(&contract).expect_err("admin action should be required"),
            ExtensionContractError::OpsDiagnosticsMissingAdminAction
        );

        contract.requires_admin_action = true;
        contract.mutates_object_data = true;

        assert_eq!(
            validate_ops_diagnostics_contract(&contract).expect_err("object mutation should be rejected"),
            ExtensionContractError::OpsDiagnosticsMutatesObjectData
        );
    }
}
