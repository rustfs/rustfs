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
use std::{collections::HashMap, fmt};

use super::FederatedAuthorization;

pub const OIDC_VIRTUAL_PARENT_CLAIM: &str = "x-rustfs-internal-oidc-parent";

#[derive(Debug, Clone)]
pub(crate) struct FederatedClaims {
    pub(crate) sub: String,
    pub(crate) email: String,
    pub(crate) username: String,
    pub(crate) groups: Vec<String>,
    pub(crate) raw: HashMap<String, Value>,
}

impl FederatedClaims {
    pub(crate) fn session_identity(&self) -> String {
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

    #[test]
    fn session_identity_preserves_existing_fallback_order() {
        assert_eq!(claims("john", "john@example.com", "sub-1").session_identity(), "john");
        assert_eq!(claims("", "john@example.com", "sub-1").session_identity(), "john@example.com");
        assert_eq!(claims("", "", "sub-1").session_identity(), "sub-1");
        assert_eq!(claims("", "", "").session_identity(), "oidc-user-unknown");
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
    }

    #[test]
    fn logout_continuation_binds_provider_and_redacts_token() {
        let continuation =
            OpaqueLogoutContinuation::new(FederatedProviderRef::new("standard_oidc".to_string()), "secret-id-token".to_string());
        let continuation_debug = format!("{continuation:?}");
        let (provider, id_token) = continuation.into_parts();

        assert_eq!(provider.as_str(), "standard_oidc");
        assert_eq!(id_token, "secret-id-token");
        assert!(!continuation_debug.contains("secret-id-token"));
        assert!(continuation_debug.contains("[REDACTED]"));
    }
}
