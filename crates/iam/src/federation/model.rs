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

use rustfs_credentials::Credentials;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct FederatedClaims {
    pub sub: String,
    pub email: String,
    pub username: String,
    pub groups: Vec<String>,
    pub raw: HashMap<String, Value>,
}

impl FederatedClaims {
    pub fn session_identity(&self) -> String {
        if !self.username.is_empty() {
            self.username.clone()
        } else if !self.email.is_empty() {
            self.email.clone()
        } else if !self.sub.is_empty() {
            self.sub.clone()
        } else {
            "oidc-user-unknown".to_string()
        }
    }
}

#[derive(Debug, Clone)]
pub struct FederatedAuthorization {
    pub provider_id: String,
    pub claims: FederatedClaims,
    pub policies: Vec<String>,
    pub groups: Vec<String>,
    pub roles_claim_key: Option<String>,
    pub roles: Vec<String>,
}

impl FederatedAuthorization {
    pub fn has_authorization_context(&self) -> bool {
        !self.policies.is_empty() || !self.groups.is_empty()
    }
}

#[derive(Debug)]
pub struct FederatedCodeExchange {
    pub authorization: FederatedAuthorization,
    pub redirect_after: Option<String>,
    pub id_token: String,
}

#[derive(Debug)]
pub struct FederatedSessionTransaction {
    pub authorization: FederatedAuthorization,
    pub duration_seconds: usize,
    pub session_policy: Option<String>,
}

#[derive(Debug)]
pub struct FederatedSession {
    pub credentials: Credentials,
    pub authorization: FederatedAuthorization,
}

#[derive(Debug)]
pub struct FederatedLoginSession {
    pub session: FederatedSession,
    pub redirect_after: Option<String>,
    pub logout_token: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims(username: &str, email: &str, sub: &str) -> FederatedClaims {
        FederatedClaims {
            sub: sub.to_string(),
            email: email.to_string(),
            username: username.to_string(),
            groups: Vec::new(),
            raw: HashMap::new(),
        }
    }

    fn authorization(policies: Vec<String>, groups: Vec<String>) -> FederatedAuthorization {
        FederatedAuthorization {
            provider_id: "standard_oidc".to_string(),
            claims: claims("", "", "subject"),
            policies,
            groups,
            roles_claim_key: None,
            roles: Vec::new(),
        }
    }

    #[test]
    fn session_identity_preserves_existing_fallback_order() {
        assert_eq!(claims("john", "john@example.com", "sub-1").session_identity(), "john");
        assert_eq!(claims("", "john@example.com", "sub-1").session_identity(), "john@example.com");
        assert_eq!(claims("", "", "sub-1").session_identity(), "sub-1");
        assert_eq!(claims("", "", "").session_identity(), "oidc-user-unknown");
    }

    #[test]
    fn authorization_context_accepts_policy_or_group() {
        assert!(!authorization(Vec::new(), Vec::new()).has_authorization_context());
        assert!(authorization(vec!["consoleAdmin".to_string()], Vec::new()).has_authorization_context());
        assert!(authorization(Vec::new(), vec!["RustFS.ConsoleAdmin".to_string()]).has_authorization_context());
    }
}
