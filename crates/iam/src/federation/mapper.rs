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

use super::{FederatedAuthorization, VerifiedFederatedIdentity};
use crate::{EVENT_OIDC_DIAGNOSTICS, LOG_COMPONENT_IAM, LOG_SUBSYSTEM_OIDC};
use rustfs_policy::policy::{ClaimLookup, get_claim_case_insensitive};
use std::{collections::HashMap, sync::Arc};
use tracing::debug;

#[derive(Clone)]
pub(crate) struct FederatedAuthorizationRule {
    provider_id: String,
    claim_name: String,
    claim_prefix: String,
    role_policy: String,
    groups_claim: String,
    roles_claim: String,
}

impl FederatedAuthorizationRule {
    pub(crate) fn new(
        provider_id: String,
        claim_name: String,
        claim_prefix: String,
        role_policy: String,
        groups_claim: String,
        roles_claim: String,
    ) -> Self {
        Self {
            provider_id,
            claim_name,
            claim_prefix,
            role_policy,
            groups_claim,
            roles_claim,
        }
    }

    fn as_ref(&self) -> FederatedAuthorizationRuleRef<'_> {
        FederatedAuthorizationRuleRef::new(
            &self.claim_name,
            &self.claim_prefix,
            &self.role_policy,
            &self.groups_claim,
            &self.roles_claim,
        )
    }
}

#[derive(Clone, Copy)]
pub(crate) struct FederatedAuthorizationRuleRef<'a> {
    claim_name: &'a str,
    claim_prefix: &'a str,
    role_policy: &'a str,
    groups_claim: &'a str,
    roles_claim: &'a str,
}

impl<'a> FederatedAuthorizationRuleRef<'a> {
    pub(crate) fn new(
        claim_name: &'a str,
        claim_prefix: &'a str,
        role_policy: &'a str,
        groups_claim: &'a str,
        roles_claim: &'a str,
    ) -> Self {
        Self {
            claim_name,
            claim_prefix,
            role_policy,
            groups_claim,
            roles_claim,
        }
    }
}

/// Immutable provider-specific inputs for core federation authorization mapping.
#[derive(Clone, Default)]
pub struct FederatedAuthorizationRules {
    providers: Arc<HashMap<String, FederatedAuthorizationRule>>,
}

impl FederatedAuthorizationRules {
    pub(crate) fn new(rules: impl IntoIterator<Item = FederatedAuthorizationRule>) -> Self {
        Self {
            providers: Arc::new(rules.into_iter().map(|rule| (rule.provider_id.clone(), rule)).collect()),
        }
    }
}

/// Maps a verified external identity into RustFS authorization data.
#[derive(Clone)]
pub struct CoreFederatedAuthorizationMapper {
    rules: FederatedAuthorizationRules,
}

impl CoreFederatedAuthorizationMapper {
    pub fn new(rules: FederatedAuthorizationRules) -> Self {
        Self { rules }
    }

    pub(crate) fn map(&self, identity: VerifiedFederatedIdentity) -> FederatedAuthorization {
        let (provider, claims) = identity.into_parts();
        let provider_id = provider.into_string();
        let Some(rule) = self.rules.providers.get(&provider_id).map(FederatedAuthorizationRule::as_ref) else {
            return FederatedAuthorization {
                provider_id,
                claims,
                policies: Vec::new(),
                group_claim_policies: Vec::new(),
                groups: Vec::new(),
                roles_claim_key: None,
                roles: Vec::new(),
            };
        };
        let (policies, groups) = Self::map_policies_and_groups(&provider_id, rule, &claims.groups, &claims.raw, true);
        let group_claim_policies = Self::group_claim_policy_names(rule, &claims.groups, &claims.raw);
        let roles_claim_key = (!rule.roles_claim.trim().is_empty()).then(|| rule.roles_claim.trim().to_string());
        let roles = roles_claim_key
            .as_deref()
            .map(|claim_name| role_values(&claims.raw, claim_name))
            .unwrap_or_default();

        FederatedAuthorization {
            provider_id,
            claims,
            policies,
            group_claim_policies,
            groups,
            roles_claim_key,
            roles,
        }
    }

    pub(crate) fn map_policies_and_groups(
        provider_id: &str,
        rule: FederatedAuthorizationRuleRef<'_>,
        source_groups: &[String],
        raw_claims: &HashMap<String, serde_json::Value>,
        emit_diagnostics: bool,
    ) -> (Vec<String>, Vec<String>) {
        let has_role_policy = !rule.role_policy.trim().is_empty();
        let mut policies: Vec<String> = if has_role_policy {
            rule.role_policy
                .split(',')
                .map(str::trim)
                .filter(|policy| !policy.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        } else {
            source_groups
                .iter()
                .map(|group| policy_name(rule.claim_prefix, group))
                .collect()
        };

        if !has_role_policy && rule.claim_name != rule.groups_claim {
            policies.extend(
                claim_values(raw_claims, rule.claim_name)
                    .into_iter()
                    .map(|value| policy_name(rule.claim_prefix, &value)),
            );
        }
        policies.sort();
        policies.dedup();

        let mut groups = source_groups.to_vec();
        groups.sort();
        groups.dedup();

        if emit_diagnostics {
            record_mapping_diagnostics(provider_id, rule, raw_claims, &policies, &groups);
        }

        (policies, groups)
    }

    pub(crate) fn group_claim_policy_names(
        rule: FederatedAuthorizationRuleRef<'_>,
        source_groups: &[String],
        raw_claims: &HashMap<String, serde_json::Value>,
    ) -> Vec<String> {
        if !rule.role_policy.trim().is_empty() {
            return Vec::new();
        }

        let explicit_policy_names: Vec<String> = if rule.claim_name != rule.groups_claim {
            claim_values(raw_claims, rule.claim_name)
                .iter()
                .map(|value| policy_name(rule.claim_prefix, value))
                .collect()
        } else {
            Vec::new()
        };
        let mut group_policies: Vec<String> = source_groups
            .iter()
            .map(|group| policy_name(rule.claim_prefix, group))
            .filter(|policy| !explicit_policy_names.contains(policy))
            .collect();
        group_policies.sort();
        group_policies.dedup();
        group_policies
    }
}

fn record_mapping_diagnostics(
    provider_id: &str,
    rule: FederatedAuthorizationRuleRef<'_>,
    raw_claims: &HashMap<String, serde_json::Value>,
    policies: &[String],
    groups: &[String],
) {
    let (claim_name_lookup, claim_name_type) = claim_lookup_details(raw_claims, rule.claim_name);
    let (groups_claim_lookup, groups_claim_type) = claim_lookup_details(raw_claims, rule.groups_claim);
    let (roles_claim_lookup, roles_claim_type) = claim_lookup_details(raw_claims, rule.roles_claim);
    let claim_name_value_count = claim_values(raw_claims, rule.claim_name).len();
    let groups_claim_value_count = claim_values(raw_claims, rule.groups_claim).len();
    let roles_claim_value_count = claim_values(raw_claims, rule.roles_claim).len();

    debug!(
        event = EVENT_OIDC_DIAGNOSTICS,
        component = LOG_COMPONENT_IAM,
        subsystem = LOG_SUBSYSTEM_OIDC,
        result = "claims_policy_mapped",
        provider_id,
        claim_name = %rule.claim_name,
        claim_prefix = %rule.claim_prefix,
        groups_claim = %rule.groups_claim,
        roles_claim = %rule.roles_claim,
        role_policy_configured = !rule.role_policy.trim().is_empty(),
        policy_count = policies.len(),
        group_count = groups.len(),
        raw_claim_key_count = raw_claims.len(),
        claim_name_lookup,
        claim_name_type,
        claim_name_value_count,
        groups_claim_lookup,
        groups_claim_type,
        groups_claim_value_count,
        roles_claim_lookup,
        roles_claim_type,
        roles_claim_value_count,
        "oidc claims mapped to policies"
    );
}

fn policy_name(prefix: &str, value: &str) -> String {
    format!("{prefix}{value}")
}

fn claim_values(claims: &HashMap<String, serde_json::Value>, claim_name: &str) -> Vec<String> {
    match get_claim_case_insensitive(claims, claim_name) {
        ClaimLookup::Found(serde_json::Value::Array(values)) => values
            .iter()
            .filter_map(|value| value.as_str().map(ToOwned::to_owned))
            .collect(),
        ClaimLookup::Found(serde_json::Value::String(value)) => value.split(',').map(str::trim).map(ToOwned::to_owned).collect(),
        ClaimLookup::Missing | ClaimLookup::Ambiguous | ClaimLookup::Found(_) => Vec::new(),
    }
}

fn role_values(claims: &HashMap<String, serde_json::Value>, claim_name: &str) -> Vec<String> {
    match get_claim_case_insensitive(claims, claim_name) {
        ClaimLookup::Found(serde_json::Value::Array(values)) => values
            .iter()
            .filter_map(|value| value.as_str().map(ToOwned::to_owned))
            .collect(),
        ClaimLookup::Found(serde_json::Value::String(value)) => value
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect(),
        ClaimLookup::Missing | ClaimLookup::Ambiguous | ClaimLookup::Found(_) => Vec::new(),
    }
}

fn claim_lookup_details(claims: &HashMap<String, serde_json::Value>, claim_name: &str) -> (&'static str, &'static str) {
    match get_claim_case_insensitive(claims, claim_name) {
        ClaimLookup::Found(value) => ("found", claim_value_type(value)),
        ClaimLookup::Missing => ("missing", "none"),
        ClaimLookup::Ambiguous => ("ambiguous", "none"),
    }
}

fn claim_value_type(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::{FederatedClaims, FederatedProviderRef, VerifiedFederatedIdentity};
    use serde_json::json;

    fn mapper(rule: FederatedAuthorizationRule) -> CoreFederatedAuthorizationMapper {
        CoreFederatedAuthorizationMapper::new(FederatedAuthorizationRules::new([rule]))
    }

    fn identity(provider_id: &str) -> VerifiedFederatedIdentity {
        VerifiedFederatedIdentity::from_claims(
            FederatedProviderRef::new(provider_id.to_string()),
            FederatedClaims {
                sub: "subject".to_string(),
                email: "user@example.test".to_string(),
                username: "user".to_string(),
                groups: vec![
                    "source-ops".to_string(),
                    "source-developers".to_string(),
                    "source-ops".to_string(),
                ],
                raw: HashMap::from([
                    ("Policy".to_string(), json!(["explicit", "source-ops"])),
                    ("Roles".to_string(), json!("reader, writer, ,auditor")),
                ]),
            },
        )
    }

    #[test]
    fn claim_mapping_preserves_policy_group_role_and_case_behavior() {
        let mapper = mapper(FederatedAuthorizationRule::new(
            "corp".to_string(),
            "policy".to_string(),
            "mapped-".to_string(),
            String::new(),
            "groups".to_string(),
            "roles".to_string(),
        ));

        let authorization = mapper.map(identity("corp"));

        assert_eq!(
            authorization.policies,
            ["mapped-explicit", "mapped-source-developers", "mapped-source-ops"]
        );
        assert_eq!(authorization.group_claim_policies, ["mapped-source-developers"]);
        assert_eq!(authorization.groups, ["source-developers", "source-ops"]);
        assert_eq!(authorization.roles_claim_key.as_deref(), Some("roles"));
        assert_eq!(authorization.roles, ["reader", "writer", "auditor"]);
    }

    #[test]
    fn role_policy_mode_keeps_groups_without_group_derived_policies() {
        let mapper = mapper(FederatedAuthorizationRule::new(
            "corp".to_string(),
            "policy".to_string(),
            "mapped-".to_string(),
            " readonly, readwrite, readonly ".to_string(),
            "groups".to_string(),
            String::new(),
        ));

        let authorization = mapper.map(identity("corp"));

        assert_eq!(authorization.policies, ["readonly", "readwrite"]);
        assert!(authorization.group_claim_policies.is_empty());
        assert_eq!(authorization.groups, ["source-developers", "source-ops"]);
    }

    #[test]
    fn missing_provider_rules_preserve_identity_without_authorization_context() {
        let authorization =
            CoreFederatedAuthorizationMapper::new(FederatedAuthorizationRules::default()).map(identity("missing"));

        assert_eq!(authorization.provider_id, "missing");
        assert_eq!(authorization.claims.username, "user");
        assert!(!authorization.has_authorization_context());
    }

    #[test]
    fn exact_claim_name_wins_over_case_insensitive_candidates() {
        let mapper = mapper(FederatedAuthorizationRule::new(
            "corp".to_string(),
            "policy".to_string(),
            "mapped-".to_string(),
            String::new(),
            "groups".to_string(),
            String::new(),
        ));
        let identity = VerifiedFederatedIdentity::from_claims(
            FederatedProviderRef::new("corp".to_string()),
            FederatedClaims {
                sub: String::new(),
                email: String::new(),
                username: String::new(),
                groups: Vec::new(),
                raw: HashMap::from([
                    ("policy".to_string(), json!("exact")),
                    ("Policy".to_string(), json!("fallback")),
                ]),
            },
        );

        let authorization = mapper.map(identity);

        assert_eq!(authorization.policies, ["mapped-exact"]);
    }

    #[test]
    fn ambiguous_case_insensitive_claim_name_is_ignored() {
        let mapper = mapper(FederatedAuthorizationRule::new(
            "corp".to_string(),
            "pOlIcY".to_string(),
            "mapped-".to_string(),
            String::new(),
            "groups".to_string(),
            String::new(),
        ));
        let identity = VerifiedFederatedIdentity::from_claims(
            FederatedProviderRef::new("corp".to_string()),
            FederatedClaims {
                sub: String::new(),
                email: String::new(),
                username: String::new(),
                groups: vec!["source-ops".to_string()],
                raw: HashMap::from([
                    ("Policy".to_string(), json!("first")),
                    ("POLICY".to_string(), json!("second")),
                ]),
            },
        );

        let authorization = mapper.map(identity);

        assert_eq!(authorization.policies, ["mapped-source-ops"]);
        assert_eq!(claim_lookup_details(&authorization.claims.raw, "pOlIcY"), ("ambiguous", "none"));
        assert_eq!(claim_lookup_details(&authorization.claims.raw, "missing"), ("missing", "none"));
    }

    #[test]
    fn comma_separated_claim_values_keep_existing_empty_value_behavior() {
        let mapper = mapper(FederatedAuthorizationRule::new(
            "corp".to_string(),
            "Policy".to_string(),
            "mapped-".to_string(),
            String::new(),
            "groups".to_string(),
            String::new(),
        ));
        let identity = VerifiedFederatedIdentity::from_claims(
            FederatedProviderRef::new("corp".to_string()),
            FederatedClaims {
                sub: String::new(),
                email: String::new(),
                username: String::new(),
                groups: Vec::new(),
                raw: HashMap::from([("Policy".to_string(), json!(" explicit, ,audit "))]),
            },
        );

        let authorization = mapper.map(identity);

        assert_eq!(authorization.policies, ["mapped-", "mapped-audit", "mapped-explicit"]);
    }

    #[test]
    fn role_array_values_keep_existing_empty_value_behavior() {
        let mapper = mapper(FederatedAuthorizationRule::new(
            "corp".to_string(),
            "policy".to_string(),
            String::new(),
            String::new(),
            "groups".to_string(),
            "roles".to_string(),
        ));
        let identity = VerifiedFederatedIdentity::from_claims(
            FederatedProviderRef::new("corp".to_string()),
            FederatedClaims {
                sub: String::new(),
                email: String::new(),
                username: String::new(),
                groups: Vec::new(),
                raw: HashMap::from([("roles".to_string(), json!(["reader", "", 7, "writer"]))]),
            },
        );

        let authorization = mapper.map(identity);

        assert_eq!(authorization.roles, ["reader", "", "writer"]);
    }
}
