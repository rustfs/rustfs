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
    ExtensionContractError, ExtensionKind, ExtensionSchema, S3_POST_AUTH_HOOK_CAPABILITY, S3HookContract, S3HookPoint,
    validate_s3_hook_contract,
};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum S3HookRegistryError {
    #[error(transparent)]
    InvalidContract(#[from] ExtensionContractError),

    #[error("extension {extension_id} is {kind:?}, not an S3 hook")]
    UnsupportedExtensionKind { extension_id: String, kind: ExtensionKind },

    #[error("s3 hook extension {extension_id} is missing capability {capability}")]
    MissingCapability { extension_id: String, capability: &'static str },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3HookRegistration {
    pub extension_id: String,
    pub hook_point: S3HookPoint,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct S3HookRegistry {
    registrations: BTreeMap<S3HookPoint, Vec<S3HookRegistration>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct S3HookContext<'a> {
    pub authenticated_principal: &'a str,
    pub bucket: &'a str,
    pub object: Option<&'a str>,
}

impl<'a> S3HookContext<'a> {
    pub fn post_auth(authenticated_principal: &'a str, bucket: &'a str, object: Option<&'a str>) -> Option<Self> {
        if authenticated_principal.trim().is_empty() {
            return None;
        }

        Some(Self {
            authenticated_principal,
            bucket,
            object,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3HookDecision {
    Continue,
}

impl S3HookRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_schema(&mut self, schema: &ExtensionSchema, contract: &S3HookContract) -> Result<(), S3HookRegistryError> {
        if schema.kind != ExtensionKind::S3Hook {
            return Err(S3HookRegistryError::UnsupportedExtensionKind {
                extension_id: schema.extension_id.clone(),
                kind: schema.kind,
            });
        }

        if !schema
            .capabilities
            .iter()
            .any(|capability| capability.as_str() == S3_POST_AUTH_HOOK_CAPABILITY)
        {
            return Err(S3HookRegistryError::MissingCapability {
                extension_id: schema.extension_id.clone(),
                capability: S3_POST_AUTH_HOOK_CAPABILITY,
            });
        }

        validate_s3_hook_contract(contract)?;

        for hook_point in &contract.hook_points {
            self.registrations.entry(*hook_point).or_default().push(S3HookRegistration {
                extension_id: schema.extension_id.clone(),
                hook_point: *hook_point,
            });
        }

        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.registrations.values().all(Vec::is_empty)
    }

    pub fn registered_hook_count(&self) -> usize {
        self.registrations.values().map(Vec::len).sum()
    }

    pub fn hooks_for(&self, hook_point: S3HookPoint) -> impl Iterator<Item = &S3HookRegistration> {
        self.registrations.get(&hook_point).into_iter().flatten()
    }

    pub fn dispatch_post_auth(&self, _hook_point: S3HookPoint, _context: &S3HookContext<'_>) -> S3HookDecision {
        S3HookDecision::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::{S3HookContext, S3HookDecision, S3HookRegistry, S3HookRegistryError};
    use crate::{builtin_s3_hook_contract, builtin_s3_hook_extension_schema};
    use rustfs_extension_schema::{ExtensionContractError, ExtensionKind, S3HookPoint};

    #[test]
    fn default_registry_has_identical_continue_behavior() {
        let registry = S3HookRegistry::new();
        let context = S3HookContext::post_auth("access-key", "photos", Some("2026/image.jpg"))
            .expect("post-auth context should require an authenticated principal");

        assert!(registry.is_empty());
        assert_eq!(registry.registered_hook_count(), 0);
        assert_eq!(
            registry.dispatch_post_auth(S3HookPoint::PostAuthGetObject, &context),
            S3HookDecision::Continue
        );
    }

    #[test]
    fn post_auth_context_rejects_missing_principal() {
        assert!(S3HookContext::post_auth("", "photos", Some("2026/image.jpg")).is_none());
    }

    #[test]
    fn registers_valid_post_auth_hook_contract_without_dispatch_side_effects() {
        let mut registry = S3HookRegistry::new();
        let schema = builtin_s3_hook_extension_schema();
        let contract = builtin_s3_hook_contract();
        let context = S3HookContext::post_auth("access-key", "photos", None).expect("post-auth context should be valid");

        registry
            .register_schema(&schema, &contract)
            .expect("builtin s3 hook contract should register");

        assert_eq!(registry.registered_hook_count(), contract.hook_points.len());
        assert_eq!(
            registry.hooks_for(S3HookPoint::PostAuthListObjects).count(),
            1,
            "registered hooks stay catalogued by allowlisted point"
        );
        assert_eq!(
            registry.dispatch_post_auth(S3HookPoint::PostAuthListObjects, &context),
            S3HookDecision::Continue
        );
    }

    #[test]
    fn rejects_non_s3_hook_schema_and_unsafe_contracts() {
        let mut registry = S3HookRegistry::new();
        let mut schema = builtin_s3_hook_extension_schema();
        schema.kind = ExtensionKind::TargetPlugin;

        assert_eq!(
            registry
                .register_schema(&schema, &builtin_s3_hook_contract())
                .expect_err("target plugin schema should not register as s3 hook"),
            S3HookRegistryError::UnsupportedExtensionKind {
                extension_id: "builtin:s3-post-auth-hooks".to_string(),
                kind: ExtensionKind::TargetPlugin
            }
        );

        schema.kind = ExtensionKind::S3Hook;
        let mut contract = builtin_s3_hook_contract();
        contract.bypasses_iam = true;

        assert_eq!(
            registry
                .register_schema(&schema, &contract)
                .expect_err("IAM bypass should be rejected"),
            S3HookRegistryError::InvalidContract(ExtensionContractError::S3HookBypassesIam)
        );
    }
}
