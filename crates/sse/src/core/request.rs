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
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Identity a data-path KMS key operation is attributed to.
///
/// Only requests that crossed the S3 authorization boundary carry one. Internal
/// callers — replication, lifecycle transition, heal, scanner — enter the object
/// layer directly and run with server credentials, so they never build a principal
/// and are exempt by construction; `None` therefore means "system principal".
#[derive(Debug, Clone)]
pub struct SseKmsPrincipal {
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
