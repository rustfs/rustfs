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
use rustfs_utils::HashAlgorithm;
use serde_json::Value;
use std::collections::HashMap;

pub const OIDC_VIRTUAL_PARENT_CLAIM: &str = "x-rustfs-internal-oidc-parent";
const OIDC_VIRTUAL_PARENT_PREFIX: &str = "openid=";

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

    pub fn oidc_virtual_parent(&self) -> Option<String> {
        let issuer = self.claims.raw.get("iss")?.as_str()?;
        let subject = self.claims.sub.as_str();
        if issuer.is_empty() || subject.is_empty() {
            return None;
        }

        let subject_len = u64::try_from(subject.len()).ok()?;
        let issuer_len = u64::try_from(issuer.len()).ok()?;
        let mut source = Vec::with_capacity(16 + subject.len() + issuer.len());
        source.extend_from_slice(&subject_len.to_be_bytes());
        source.extend_from_slice(subject.as_bytes());
        source.extend_from_slice(&issuer_len.to_be_bytes());
        source.extend_from_slice(issuer.as_bytes());
        let digest = HashAlgorithm::SHA256.hash_encode(&source);
        Some(format!(
            "{OIDC_VIRTUAL_PARENT_PREFIX}{}",
            base64_simd::URL_SAFE_NO_PAD.encode_to_string(digest.as_ref())
        ))
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
            claims: FederatedClaims {
                raw: HashMap::from([("iss".to_string(), Value::String("https://idp.example.test".to_string()))]),
                ..claims("", "", "subject")
            },
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

    #[test]
    fn oidc_virtual_parent_is_stable_and_issuer_scoped() {
        let first = authorization(Vec::new(), Vec::new());
        let mut second = first.clone();
        second
            .claims
            .raw
            .insert("iss".to_string(), Value::String("https://other-idp.example.test".to_string()));

        assert_eq!(
            first.oidc_virtual_parent().as_deref(),
            Some("openid=pUmguI1petsjVfDFQppmmR9yqdmWnBAXGJhHV_s9W3I")
        );
        assert!(rustfs_policy::auth::contains_reserved_chars(
            first.oidc_virtual_parent().as_deref().expect("virtual parent")
        ));
        assert_ne!(first.oidc_virtual_parent(), second.oidc_virtual_parent());
    }

    #[test]
    fn oidc_virtual_parent_requires_verified_identity_parts() {
        let mut missing_issuer = authorization(Vec::new(), Vec::new());
        missing_issuer.claims.raw.clear();
        let mut missing_subject = authorization(Vec::new(), Vec::new());
        missing_subject.claims.sub.clear();

        assert!(missing_issuer.oidc_virtual_parent().is_none());
        assert!(missing_subject.oidc_virtual_parent().is_none());
    }

    #[test]
    fn oidc_virtual_parent_preserves_exact_subject() {
        let plain = authorization(Vec::new(), Vec::new());
        let mut padded = plain.clone();
        padded.claims.sub = format!(" {} ", plain.claims.sub);

        assert_ne!(plain.oidc_virtual_parent(), padded.oidc_virtual_parent());
    }

    #[test]
    fn oidc_virtual_parent_encoding_is_unambiguous() {
        let mut first = authorization(Vec::new(), Vec::new());
        first.claims.sub = "subject".to_string();
        first.claims.raw.insert(
            "iss".to_string(),
            Value::String("https://issuer.example/path:https://other.example".to_string()),
        );
        let mut second = authorization(Vec::new(), Vec::new());
        second.claims.sub = "subject:https://issuer.example/path".to_string();
        second
            .claims
            .raw
            .insert("iss".to_string(), Value::String("https://other.example".to_string()));

        assert_ne!(first.oidc_virtual_parent(), second.oidc_virtual_parent());
    }
}
