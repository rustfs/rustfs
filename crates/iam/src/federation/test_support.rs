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

use super::{FederatedAuthorization, FederatedClaims};
use serde_json::Value;
use std::collections::HashMap;

/// Exact authorization inputs for tests that exercise the session binding boundary.
#[derive(Default)]
pub struct FederatedAuthorizationFixture {
    pub provider_id: String,
    pub sub: String,
    pub email: String,
    pub username: String,
    pub source_groups: Vec<String>,
    pub attributes: HashMap<String, Value>,
    pub policies: Vec<String>,
    pub group_claim_policies: Vec<String>,
    pub mapped_iam_groups: Vec<String>,
    pub roles_claim_key: Option<String>,
    pub roles: Vec<String>,
}

impl FederatedAuthorizationFixture {
    /// Preserves supplied values without authentication, validation, or policy mapping.
    pub fn into_authorization(self) -> FederatedAuthorization {
        FederatedAuthorization {
            provider_id: self.provider_id,
            claims: FederatedClaims {
                sub: self.sub,
                email: self.email,
                username: self.username,
                groups: self.source_groups,
                raw: self.attributes,
            },
            policies: self.policies,
            group_claim_policies: self.group_claim_policies,
            groups: self.mapped_iam_groups,
            roles_claim_key: self.roles_claim_key,
            roles: self.roles,
        }
    }
}
