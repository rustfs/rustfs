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
    ExtensionContractError, ExtensionKind, ExtensionSchema, OPS_DIAGNOSTICS_CAPABILITY, OpsDiagnosticSurface,
    OpsDiagnosticsContract, validate_ops_diagnostics_contract,
};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OpsDiagnosticsRegistryError {
    #[error(transparent)]
    InvalidContract(#[from] ExtensionContractError),

    #[error("extension {extension_id} is {kind:?}, not an ops diagnostics extension")]
    UnsupportedExtensionKind { extension_id: String, kind: ExtensionKind },

    #[error("ops diagnostics extension {extension_id} is missing capability {capability}")]
    MissingCapability { extension_id: String, capability: &'static str },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpsDiagnosticsRegistration {
    pub extension_id: String,
    pub surface: OpsDiagnosticSurface,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OpsDiagnosticsRegistry {
    registrations: BTreeMap<OpsDiagnosticSurface, Vec<OpsDiagnosticsRegistration>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpsDiagnosticsReadRequest<'a> {
    pub surface: OpsDiagnosticSurface,
    pub capability: &'a str,
    pub admin_action_authorized: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpsDiagnosticsAccessDecision {
    AllowReadOnly,
    DenyMissingAdminAction,
    DenyMissingCapability,
    DenyUnknownSurface,
}

impl OpsDiagnosticsRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_schema(
        &mut self,
        schema: &ExtensionSchema,
        contract: &OpsDiagnosticsContract,
    ) -> Result<(), OpsDiagnosticsRegistryError> {
        if schema.kind != ExtensionKind::OpsDiagnostics {
            return Err(OpsDiagnosticsRegistryError::UnsupportedExtensionKind {
                extension_id: schema.extension_id.clone(),
                kind: schema.kind,
            });
        }

        if !schema
            .capabilities
            .iter()
            .any(|capability| capability.as_str() == OPS_DIAGNOSTICS_CAPABILITY)
        {
            return Err(OpsDiagnosticsRegistryError::MissingCapability {
                extension_id: schema.extension_id.clone(),
                capability: OPS_DIAGNOSTICS_CAPABILITY,
            });
        }

        validate_ops_diagnostics_contract(contract)?;

        for surface in &contract.surfaces {
            self.registrations
                .entry(*surface)
                .or_default()
                .push(OpsDiagnosticsRegistration {
                    extension_id: schema.extension_id.clone(),
                    surface: *surface,
                });
        }

        Ok(())
    }

    pub fn registered_surface_count(&self) -> usize {
        self.registrations.values().map(Vec::len).sum()
    }

    pub fn registrations_for(&self, surface: OpsDiagnosticSurface) -> impl Iterator<Item = &OpsDiagnosticsRegistration> {
        self.registrations.get(&surface).into_iter().flatten()
    }

    pub fn authorize_read(&self, request: OpsDiagnosticsReadRequest<'_>) -> OpsDiagnosticsAccessDecision {
        if !self.registrations.contains_key(&request.surface) {
            return OpsDiagnosticsAccessDecision::DenyUnknownSurface;
        }

        if !request.admin_action_authorized {
            return OpsDiagnosticsAccessDecision::DenyMissingAdminAction;
        }

        if request.capability != OPS_DIAGNOSTICS_CAPABILITY {
            return OpsDiagnosticsAccessDecision::DenyMissingCapability;
        }

        OpsDiagnosticsAccessDecision::AllowReadOnly
    }
}

#[cfg(test)]
mod tests {
    use super::{OpsDiagnosticsAccessDecision, OpsDiagnosticsReadRequest, OpsDiagnosticsRegistry, OpsDiagnosticsRegistryError};
    use crate::{builtin_ops_diagnostics_contract, builtin_ops_diagnostics_extension_schema};
    use rustfs_extension_schema::{ExtensionContractError, ExtensionKind, OPS_DIAGNOSTICS_CAPABILITY, OpsDiagnosticSurface};

    #[test]
    fn default_registry_denies_unknown_diagnostic_surface() {
        let registry = OpsDiagnosticsRegistry::new();

        assert_eq!(
            registry.authorize_read(OpsDiagnosticsReadRequest {
                surface: OpsDiagnosticSurface::Health,
                capability: OPS_DIAGNOSTICS_CAPABILITY,
                admin_action_authorized: true,
            }),
            OpsDiagnosticsAccessDecision::DenyUnknownSurface
        );
    }

    #[test]
    fn registered_ops_diagnostics_are_read_only_and_capability_limited() {
        let mut registry = OpsDiagnosticsRegistry::new();
        let schema = builtin_ops_diagnostics_extension_schema();
        let contract = builtin_ops_diagnostics_contract();

        registry
            .register_schema(&schema, &contract)
            .expect("builtin ops diagnostics contract should register");

        assert_eq!(registry.registered_surface_count(), contract.surfaces.len());
        assert_eq!(registry.registrations_for(OpsDiagnosticSurface::Metrics).count(), 1);
        assert_eq!(
            registry.authorize_read(OpsDiagnosticsReadRequest {
                surface: OpsDiagnosticSurface::Health,
                capability: OPS_DIAGNOSTICS_CAPABILITY,
                admin_action_authorized: true,
            }),
            OpsDiagnosticsAccessDecision::AllowReadOnly
        );
        assert_eq!(
            registry.authorize_read(OpsDiagnosticsReadRequest {
                surface: OpsDiagnosticSurface::Health,
                capability: "target.notify.v1",
                admin_action_authorized: true,
            }),
            OpsDiagnosticsAccessDecision::DenyMissingCapability
        );
        assert_eq!(
            registry.authorize_read(OpsDiagnosticsReadRequest {
                surface: OpsDiagnosticSurface::Health,
                capability: OPS_DIAGNOSTICS_CAPABILITY,
                admin_action_authorized: false,
            }),
            OpsDiagnosticsAccessDecision::DenyMissingAdminAction
        );
    }

    #[test]
    fn rejects_non_diagnostics_schema_and_mutating_contracts() {
        let mut registry = OpsDiagnosticsRegistry::new();
        let mut schema = builtin_ops_diagnostics_extension_schema();
        schema.kind = ExtensionKind::TargetPlugin;

        assert_eq!(
            registry
                .register_schema(&schema, &builtin_ops_diagnostics_contract())
                .expect_err("target plugin schema should not register as ops diagnostics"),
            OpsDiagnosticsRegistryError::UnsupportedExtensionKind {
                extension_id: "builtin:ops-diagnostics".to_string(),
                kind: ExtensionKind::TargetPlugin
            }
        );

        schema.kind = ExtensionKind::OpsDiagnostics;
        let mut contract = builtin_ops_diagnostics_contract();
        contract.mutates_object_data = true;

        assert_eq!(
            registry
                .register_schema(&schema, &contract)
                .expect_err("object mutation should be rejected"),
            OpsDiagnosticsRegistryError::InvalidContract(ExtensionContractError::OpsDiagnosticsMutatesObjectData)
        );
    }
}
