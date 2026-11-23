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

//! The caller an SSE operation is authorized as.

use crate::core::audit::{KmsRequestAudit, kms_request_audit};
#[cfg(any(test, feature = "test-util"))]
use crate::host::KmsKeyAuthorizer;
use crate::host::SseHost;
use rustfs_kms::types::OperationContext;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// The host and identity retained by one admitted maintenance operation.
#[derive(Clone)]
pub(crate) struct ManagedSseBinding {
    pub(crate) request_id: String,
    pub(crate) context: OperationContext,
    pub(crate) host: Arc<dyn SseHost>,
}

impl std::fmt::Debug for ManagedSseBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ManagedSseBinding").finish_non_exhaustive()
    }
}

/// Identity a data-path KMS key operation is attributed to.
///
/// Only requests that crossed the S3 authorization boundary carry one. Internal
/// callers — replication, lifecycle transition, heal, scanner — enter the object
/// layer directly and run with server credentials, so they never build a principal
/// and are exempt by construction; `None` therefore means "system principal".
#[derive(Debug, Clone)]
pub struct SseKmsPrincipal {
    pub(crate) maintenance: Option<Arc<ManagedSseBinding>>,
    pub(crate) account: String,
    groups: Option<Vec<String>>,
    is_owner: bool,
    claims: HashMap<String, Value>,
    conditions: HashMap<String, Vec<String>>,
    /// Audit slot of the S3 request this principal was built for, when the request
    /// is being audited. The principal is the only request-derived value that reaches
    /// the managed-SSE code paths, so it doubles as the carrier for the slot.
    pub(crate) request_audit: Option<Arc<KmsRequestAudit>>,
    /// Test-only decision overrides. Carried per principal rather than in a global slot so
    /// concurrent tests cannot observe each other's injection.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) test_hooks: Option<TestAuthorizationHooks>,
}

impl SseKmsPrincipal {
    /// Build the principal of a request that crossed the S3 authorization boundary.
    ///
    /// Anonymous requests use an empty account, matching the bucket-policy authorization
    /// path. This keeps every request that crossed the S3 boundary distinct from trusted
    /// internal callers, which are represented by `None`.
    pub fn new(
        account: String,
        groups: Option<Vec<String>>,
        is_owner: bool,
        claims: HashMap<String, Value>,
        conditions: HashMap<String, Vec<String>>,
    ) -> Self {
        Self {
            maintenance: None,
            account,
            groups,
            is_owner,
            claims,
            conditions,
            request_audit: None,
            #[cfg(any(test, feature = "test-util"))]
            test_hooks: None,
        }
    }

    /// Retain the host and identity of an admitted maintenance operation.
    ///
    /// The host owns the operation's captured KMS service, availability decision,
    /// local provider, and key authorizer. Managed SSE-KMS always asks that
    /// authorizer before using a key, including when ordinary key enforcement is
    /// disabled. Its KMS provider does not use process-wide test injection.
    ///
    /// Clones keep the same host alive. Dropping a future releases only its
    /// references; it does not roll back an already completed KMS operation.
    /// The caller must perform resource admission and bind the execution identity
    /// before using this principal. SSE does not grant access to stored objects.
    #[must_use]
    pub fn with_managed_operation(mut self, request_id: &str, context: OperationContext, host: Arc<dyn SseHost>) -> Self {
        self.request_audit = kms_request_audit(request_id);
        self.maintenance = Some(Arc::new(ManagedSseBinding {
            request_id: request_id.to_owned(),
            context,
            host,
        }));
        self
    }

    /// The request id and operation identity retained at maintenance admission.
    ///
    /// The server uses these together with the principal account to check that a
    /// principal belongs to the exact admitted resource operation.
    pub fn managed_operation_context(&self) -> Option<(&str, &OperationContext)> {
        self.maintenance
            .as_ref()
            .map(|binding| (binding.request_id.as_str(), &binding.context))
    }

    /// The access key the request authenticated with; empty for an anonymous request.
    pub fn account(&self) -> &str {
        &self.account
    }

    pub fn groups(&self) -> &Option<Vec<String>> {
        &self.groups
    }

    pub fn is_owner(&self) -> bool {
        self.is_owner
    }

    pub fn claims(&self) -> &HashMap<String, Value> {
        &self.claims
    }

    /// Policy condition values of the request that do not depend on the resource.
    pub fn conditions(&self) -> &HashMap<String, Vec<String>> {
        &self.conditions
    }

    /// Attach the audit slot opened for `request_id`, if that request is being audited
    /// (see [`KmsRequestAuditScope::register`]).
    #[must_use]
    pub fn with_request_id(mut self, request_id: &str) -> Self {
        self.request_audit = kms_request_audit(request_id);
        self
    }

    /// Override the enforcement switch and the authorizer for this principal only.
    #[cfg(any(test, feature = "test-util"))]
    #[must_use]
    pub fn with_test_hooks(mut self, hooks: Option<TestAuthorizationHooks>) -> Self {
        self.test_hooks = hooks;
        self
    }

    #[cfg(test)]
    pub(crate) fn for_test(account: &str, enforced: bool, authorizer: Arc<dyn KmsKeyAuthorizer>) -> Self {
        Self {
            maintenance: None,
            account: account.to_string(),
            groups: None,
            is_owner: false,
            claims: HashMap::new(),
            conditions: HashMap::new(),
            request_audit: None,
            test_hooks: Some(TestAuthorizationHooks { enforced, authorizer }),
        }
    }

    /// Bind this principal to `audit` so managed-SSE operations performed for it are
    /// summarised onto the request's S3 audit entry.
    #[cfg(test)]
    pub(crate) fn with_request_audit(mut self, audit: Arc<KmsRequestAudit>) -> Self {
        self.request_audit = Some(audit);
        self
    }
}

/// Use the service identity captured at maintenance admission throughout key resolution.
pub(crate) fn operation_host<'a>(host: &'a dyn SseHost, principal: Option<&'a SseKmsPrincipal>) -> &'a dyn SseHost {
    principal
        .and_then(|principal| principal.maintenance.as_ref())
        .map_or(host, |binding| binding.host.as_ref())
}

/// Per-principal override of SSE-KMS key authorization, for tests.
#[cfg(any(test, feature = "test-util"))]
#[derive(Clone)]
pub struct TestAuthorizationHooks {
    pub enforced: bool,
    pub authorizer: Arc<dyn KmsKeyAuthorizer>,
}

#[cfg(any(test, feature = "test-util"))]
impl std::fmt::Debug for TestAuthorizationHooks {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TestAuthorizationHooks")
            .field("enforced", &self.enforced)
            .finish_non_exhaustive()
    }
}
