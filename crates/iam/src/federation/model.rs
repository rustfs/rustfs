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
use std::{collections::HashMap, fmt};

pub const OIDC_VIRTUAL_PARENT_CLAIM: &str = "x-rustfs-internal-oidc-parent";

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FederatedProviderRef {
    provider_id: String,
}

impl FederatedProviderRef {
    /// Retains the configured provider instance ID exactly as selected by the authenticator.
    pub(crate) fn new(provider_id: String) -> Self {
        Self { provider_id }
    }

    pub fn as_str(&self) -> &str {
        &self.provider_id
    }

    pub(crate) fn into_string(self) -> String {
        self.provider_id
    }
}

#[derive(Clone)]
pub struct VerifiedFederatedIdentity {
    provider: FederatedProviderRef,
    claims: FederatedClaims,
}

impl fmt::Debug for VerifiedFederatedIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedFederatedIdentity")
            .field("provider", &self.provider)
            .field("issuer_present", &self.issuer().is_some())
            .field("subject_present", &!self.claims.sub.is_empty())
            .field("email_present", &!self.claims.email.is_empty())
            .field("username_present", &!self.claims.username.is_empty())
            .field("source_group_count", &self.claims.groups.len())
            .field("attribute_count", &self.claims.raw.len())
            .finish()
    }
}

impl VerifiedFederatedIdentity {
    pub(crate) fn from_claims(provider: FederatedProviderRef, claims: FederatedClaims) -> Self {
        Self { provider, claims }
    }

    pub fn provider(&self) -> &FederatedProviderRef {
        &self.provider
    }

    pub fn issuer(&self) -> Option<&str> {
        self.claims.raw.get("iss").and_then(Value::as_str)
    }

    pub fn subject(&self) -> &str {
        &self.claims.sub
    }

    pub fn email(&self) -> &str {
        &self.claims.email
    }

    pub fn username(&self) -> &str {
        &self.claims.username
    }

    pub fn source_groups(&self) -> &[String] {
        &self.claims.groups
    }

    pub fn attributes(&self) -> &HashMap<String, Value> {
        &self.claims.raw
    }

    pub fn session_identity(&self) -> String {
        self.claims.session_identity()
    }

    pub(crate) fn into_parts(self) -> (FederatedProviderRef, FederatedClaims) {
        (self.provider, self.claims)
    }
}

#[derive(Debug, Clone)]
pub struct FederatedAuthorization {
    pub provider_id: String,
    pub claims: FederatedClaims,
    pub policies: Vec<String>,
    /// Subset of `policies` derived only from the groups claim (including merged roles claim
    /// values); these may be ignored when no policy of that name exists. Everything else in
    /// `policies` must resolve.
    pub group_claim_policies: Vec<String>,
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
        let mut source = Vec::with_capacity(23 + subject.len() + issuer.len());
        source.extend_from_slice(b"openid:");
        source.extend_from_slice(&subject_len.to_be_bytes());
        source.extend_from_slice(subject.as_bytes());
        source.extend_from_slice(&issuer_len.to_be_bytes());
        source.extend_from_slice(issuer.as_bytes());
        let digest = HashAlgorithm::SHA256.hash_encode(&source);
        Some(base64_simd::URL_SAFE_NO_PAD.encode_to_string(digest.as_ref()))
    }
}

pub struct FederatedCodeExchange {
    pub authorization: FederatedAuthorization,
    pub redirect_after: Option<String>,
    pub id_token: String,
}

impl FederatedCodeExchange {
    pub(crate) fn into_parts(self) -> (FederatedAuthorization, Option<String>, OpaqueLogoutContinuation) {
        let Self {
            authorization,
            redirect_after,
            id_token,
        } = self;
        let continuation = OpaqueLogoutContinuation {
            provider: FederatedProviderRef::new(authorization.provider_id.clone()),
            id_token,
        };
        (authorization, redirect_after, continuation)
    }
}

impl fmt::Debug for FederatedCodeExchange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FederatedCodeExchange")
            .field("provider_id", &self.authorization.provider_id)
            .field("policy_count", &self.authorization.policies.len())
            .field("group_count", &self.authorization.groups.len())
            .field("redirect_after_present", &self.redirect_after.is_some())
            .field("id_token_present", &!self.id_token.is_empty())
            .finish()
    }
}

/// Carries the provider-bound ID token between code exchange and logout token creation.
pub(crate) struct OpaqueLogoutContinuation {
    provider: FederatedProviderRef,
    id_token: String,
}

impl OpaqueLogoutContinuation {
    pub(crate) fn new(provider: FederatedProviderRef, id_token: String) -> Self {
        Self { provider, id_token }
    }

    pub(crate) fn into_parts(self) -> (FederatedProviderRef, String) {
        (self.provider, self.id_token)
    }
}

impl fmt::Debug for OpaqueLogoutContinuation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpaqueLogoutContinuation")
            .field("provider", &self.provider)
            .field("id_token", &"[REDACTED]")
            .finish()
    }
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
            group_claim_policies: Vec::new(),
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
    fn oidc_virtual_parent_is_issuer_scoped() {
        let first = authorization(Vec::new(), Vec::new());
        let mut second = first.clone();
        second
            .claims
            .raw
            .insert("iss".to_string(), Value::String("https://other-idp.example.test".to_string()));

        assert_eq!(
            first.oidc_virtual_parent().as_deref(),
            Some("HwDfWftzOy4jiuS3WjKytC_Sg_A2hKhrRAFtBDhoBr0")
        );
        assert!(!rustfs_policy::auth::contains_reserved_chars(
            first.oidc_virtual_parent().as_deref().expect("virtual parent")
        ));
        assert_ne!(first.oidc_virtual_parent(), second.oidc_virtual_parent());
    }

    #[test]
    fn oidc_virtual_parent_length_delimits_identity_parts() {
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
    fn verified_identity_preserves_exact_values_and_distinct_groups() {
        let attributes = HashMap::from([
            ("iss".to_string(), Value::String(" https://issuer.example/path ".to_string())),
            ("department".to_string(), Value::String("identity-attribute-poison".to_string())),
        ]);
        let identity = VerifiedFederatedIdentity {
            provider: FederatedProviderRef::new(" Corp Provider ".to_string()),
            claims: FederatedClaims {
                sub: " subject ".to_string(),
                email: " email@example.test ".to_string(),
                username: " username ".to_string(),
                groups: vec![" source-b ".to_string(), "source-a".to_string(), " source-b ".to_string()],
                raw: attributes.clone(),
            },
        };
        let debug = format!("{identity:?}");

        assert_eq!(identity.provider().as_str(), " Corp Provider ");
        assert_eq!(identity.issuer(), Some(" https://issuer.example/path "));
        assert_eq!(identity.subject(), " subject ");
        assert_eq!(identity.email(), " email@example.test ");
        assert_eq!(identity.username(), " username ");
        assert_eq!(identity.source_groups(), [" source-b ", "source-a", " source-b "]);
        assert_eq!(identity.attributes(), &attributes);
        assert!(!debug.contains(" subject "));
        assert!(!debug.contains(" email@example.test "));
        assert!(!debug.contains(" username "));
        assert!(!debug.contains(" source-b "));
        assert!(!debug.contains("identity-attribute-poison"));

        let authorization = FederatedAuthorization {
            provider_id: " Corp Provider ".to_string(),
            claims: FederatedClaims {
                sub: "subject".to_string(),
                email: String::new(),
                username: String::new(),
                groups: vec!["different-source-group".to_string()],
                raw: HashMap::new(),
            },
            policies: Vec::new(),
            group_claim_policies: Vec::new(),
            groups: vec!["mapped-a".to_string(), "mapped-b".to_string()],
            roles_claim_key: None,
            roles: Vec::new(),
        };
        assert_eq!(authorization.claims.groups, ["different-source-group"]);
        assert_eq!(authorization.groups, ["mapped-a", "mapped-b"]);
    }

    #[test]
    fn logout_continuation_binds_provider_and_redacts_token() {
        let mut authorization = authorization(Vec::new(), Vec::new());
        authorization
            .claims
            .raw
            .insert("attribute_poison".to_string(), Value::String("secret-attribute".to_string()));
        let exchange = FederatedCodeExchange {
            authorization,
            redirect_after: Some("/console?poison=redirect-secret".to_string()),
            id_token: "secret-id-token".to_string(),
        };
        let debug = format!("{exchange:?}");
        let (_, _, continuation) = exchange.into_parts();
        let continuation_debug = format!("{continuation:?}");
        let (provider, id_token) = continuation.into_parts();

        assert_eq!(provider.as_str(), "standard_oidc");
        assert_eq!(id_token, "secret-id-token");
        assert!(!debug.contains("secret-id-token"));
        assert!(!debug.contains("secret-attribute"));
        assert!(!debug.contains("redirect-secret"));
        assert!(debug.contains("id_token_present: true"));
        assert!(!continuation_debug.contains("secret-id-token"));
        assert!(continuation_debug.contains("[REDACTED]"));
    }
}
