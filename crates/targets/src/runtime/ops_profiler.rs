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

use rustfs_extension_schema::{
    ExtensionCapabilityRef, ExtensionContractError, ExtensionKind, ExtensionSchema, OPS_PROFILER_CAPABILITY,
    OpsProfilerBackendStatus, OpsProfilerCapabilitySnapshot, OpsProfilerContract, OpsProfilerRuntimeSnapshot,
    validate_ops_profiler_capability_snapshot,
};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OpsProfilerRegistryError {
    #[error(transparent)]
    InvalidContract(#[from] ExtensionContractError),

    #[error("extension {extension_id} is {kind:?}, not an ops profiler extension")]
    UnsupportedExtensionKind { extension_id: String, kind: ExtensionKind },

    #[error("ops profiler extension {extension_id} is missing capability {capability}")]
    MissingCapability { extension_id: String, capability: &'static str },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpsProfilerRegistration {
    pub extension_id: String,
    pub backend: String,
    pub status: OpsProfilerBackendStatus,
    pub supports_profile_export: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OpsProfilerRegistry {
    registrations: BTreeMap<String, OpsProfilerRegistration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpsProfilerReadRequest<'a> {
    pub backend: &'a str,
    pub capability: &'a str,
    pub admin_action_authorized: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpsProfilerAccessDecision {
    AllowReadOnly,
    DenyMissingAdminAction,
    DenyMissingCapability,
    DenyUnknownBackend,
}

impl OpsProfilerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_schema(
        &mut self,
        schema: &ExtensionSchema,
        contract: &OpsProfilerContract,
    ) -> Result<(), OpsProfilerRegistryError> {
        if schema.kind != ExtensionKind::OpsProfiler {
            return Err(OpsProfilerRegistryError::UnsupportedExtensionKind {
                extension_id: schema.extension_id.clone(),
                kind: schema.kind,
            });
        }

        if !schema
            .capabilities
            .iter()
            .any(|capability| capability.as_str() == OPS_PROFILER_CAPABILITY)
        {
            return Err(OpsProfilerRegistryError::MissingCapability {
                extension_id: schema.extension_id.clone(),
                capability: OPS_PROFILER_CAPABILITY,
            });
        }

        validate_ops_profiler_capability_snapshot(&OpsProfilerCapabilitySnapshot {
            capability: ExtensionCapabilityRef::new(OPS_PROFILER_CAPABILITY),
            runtime: OpsProfilerRuntimeSnapshot {
                boundary: schema.runtime.boundary,
                disabled_by_default: schema.disabled_by_default,
                startup_fatal: false,
            },
            contract: contract.clone(),
        })?;

        for backend in &contract.backends {
            self.registrations.insert(
                backend.backend.as_str().to_string(),
                OpsProfilerRegistration {
                    extension_id: schema.extension_id.clone(),
                    backend: backend.backend.as_str().to_string(),
                    status: backend.status,
                    supports_profile_export: backend.supports_profile_export,
                },
            );
        }

        Ok(())
    }

    pub fn registered_backend_count(&self) -> usize {
        self.registrations.len()
    }

    pub fn registration_for(&self, backend: &str) -> Option<&OpsProfilerRegistration> {
        self.registrations.get(backend)
    }

    pub fn authorize_read(&self, request: OpsProfilerReadRequest<'_>) -> OpsProfilerAccessDecision {
        if !self.registrations.contains_key(request.backend) {
            return OpsProfilerAccessDecision::DenyUnknownBackend;
        }

        if !request.admin_action_authorized {
            return OpsProfilerAccessDecision::DenyMissingAdminAction;
        }

        if request.capability != OPS_PROFILER_CAPABILITY {
            return OpsProfilerAccessDecision::DenyMissingCapability;
        }

        OpsProfilerAccessDecision::AllowReadOnly
    }
}

#[cfg(test)]
mod tests {
    use super::{OpsProfilerAccessDecision, OpsProfilerReadRequest, OpsProfilerRegistry, OpsProfilerRegistryError};
    use crate::{builtin_ops_profiler_contract, builtin_ops_profiler_extension_schema};
    use rustfs_extension_schema::{ExtensionContractError, ExtensionKind, OPS_PROFILER_CAPABILITY, OpsProfilerBackendStatus};

    #[test]
    fn default_registry_denies_unknown_profiler_backend() {
        let registry = OpsProfilerRegistry::new();

        assert_eq!(
            registry.authorize_read(OpsProfilerReadRequest {
                backend: "cpu_pprof",
                capability: OPS_PROFILER_CAPABILITY,
                admin_action_authorized: true,
            }),
            OpsProfilerAccessDecision::DenyUnknownBackend
        );
    }

    #[test]
    fn registered_ops_profiler_backends_are_read_only_and_capability_limited() {
        let mut registry = OpsProfilerRegistry::new();
        let schema = builtin_ops_profiler_extension_schema();
        let contract = builtin_ops_profiler_contract();

        registry
            .register_schema(&schema, &contract)
            .expect("builtin ops profiler contract should register");

        assert_eq!(registry.registered_backend_count(), contract.backends.len());
        assert_eq!(
            registry.registration_for("cpu_pprof").map(|registration| registration.status),
            Some(OpsProfilerBackendStatus::Enabled)
        );
        assert_eq!(
            registry
                .registration_for("memory_pprof")
                .map(|registration| registration.supports_profile_export),
            Some(true)
        );
        assert_eq!(
            registry.authorize_read(OpsProfilerReadRequest {
                backend: "cpu_pprof",
                capability: OPS_PROFILER_CAPABILITY,
                admin_action_authorized: true,
            }),
            OpsProfilerAccessDecision::AllowReadOnly
        );
        assert_eq!(
            registry.authorize_read(OpsProfilerReadRequest {
                backend: "cpu_pprof",
                capability: "ops.diagnostics.v1",
                admin_action_authorized: true,
            }),
            OpsProfilerAccessDecision::DenyMissingCapability
        );
        assert_eq!(
            registry.authorize_read(OpsProfilerReadRequest {
                backend: "cpu_pprof",
                capability: OPS_PROFILER_CAPABILITY,
                admin_action_authorized: false,
            }),
            OpsProfilerAccessDecision::DenyMissingAdminAction
        );
    }

    #[test]
    fn rejects_non_profiler_schema_and_invalid_runtime_boundaries() {
        let mut registry = OpsProfilerRegistry::new();
        let mut schema = builtin_ops_profiler_extension_schema();
        schema.kind = ExtensionKind::TargetPlugin;

        assert_eq!(
            registry
                .register_schema(&schema, &builtin_ops_profiler_contract())
                .expect_err("target plugin schema should not register as ops profiler"),
            OpsProfilerRegistryError::UnsupportedExtensionKind {
                extension_id: "builtin:ops-profiler".to_string(),
                kind: ExtensionKind::TargetPlugin
            }
        );

        schema.kind = ExtensionKind::OpsProfiler;
        schema.runtime.boundary = rustfs_extension_schema::ExtensionRuntimeBoundary::Sidecar;
        schema.disabled_by_default = false;

        assert_eq!(
            registry
                .register_schema(&schema, &builtin_ops_profiler_contract())
                .expect_err("external profiler runtimes must be disabled by default"),
            OpsProfilerRegistryError::InvalidContract(ExtensionContractError::OpsProfilerExternalRuntimeEnabledByDefault)
        );
    }
}
