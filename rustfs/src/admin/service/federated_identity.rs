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

use super::session_policy::populate_session_policy;
use crate::admin::{
    handlers::site_replication::site_replication_iam_change_hook,
    runtime_sources::{current_action_credentials, current_ready_iam_handle, current_token_signing_key},
};
use rustfs_iam::{
    federation::{FederatedSessionBinding, FederatedSessionBindingError, FederatedSessionTransaction, OIDC_VIRTUAL_PARENT_CLAIM},
    store::{MappedPolicy, object::ObjectStore},
    sys::{IamSys, is_safe_claim_policy_name},
};
use rustfs_madmin::{SITE_REPL_API_VERSION, SRIAMItem, SRSTSCredential};
use rustfs_policy::auth::get_new_credentials_with_metadata;
use s3s::{S3Error, S3ErrorCode};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use time::{Duration, OffsetDateTime};
use tracing::{debug, warn};

pub(crate) struct DefaultFederatedSessionBinding;

// Legacy receivers reject an empty parsed mapping; current receivers ignore it when the virtual-parent marker is present.
const OIDC_STS_REQUIRES_VIRTUAL_PARENT_RECEIVER_POLICY: &str = " ";

fn all_oidc_policies_resolved(selected_policy_names: &[String], resolved_policy_mapping: &str) -> bool {
    let resolved_policy_names = MappedPolicy::new(resolved_policy_mapping).to_slice();
    !selected_policy_names.is_empty()
        && selected_policy_names
            .iter()
            .all(|policy_name| is_safe_claim_policy_name(policy_name) && resolved_policy_names.contains(policy_name))
}

fn build_oidc_token_claims(transaction: &FederatedSessionTransaction) -> HashMap<String, Value> {
    let authorization = &transaction.authorization;
    let claims = &authorization.claims;
    let mut token_claims = HashMap::new();
    token_claims.insert("sub".to_string(), Value::String(claims.sub.clone()));
    token_claims.insert("iss".to_string(), Value::String("rustfs-oidc".to_string()));
    token_claims.insert("oidc_provider".to_string(), Value::String(authorization.provider_id.clone()));

    if !claims.email.is_empty() {
        token_claims.insert("email".to_string(), Value::String(claims.email.clone()));
    }
    if !claims.username.is_empty() {
        token_claims.insert("preferred_username".to_string(), Value::String(claims.username.clone()));
    }
    if !authorization.groups.is_empty() {
        token_claims.insert(
            "groups".to_string(),
            Value::Array(
                authorization
                    .groups
                    .iter()
                    .map(|group| Value::String(group.clone()))
                    .collect(),
            ),
        );
    }
    if !authorization.roles.is_empty() {
        token_claims.insert(
            "roles".to_string(),
            Value::Array(authorization.roles.iter().cloned().map(Value::String).collect()),
        );
    }

    token_claims
}

async fn log_oidc_policy_diagnostics(
    iam_store: &IamSys<ObjectStore>,
    provider_id: &str,
    parent_user: &str,
    policies: &[String],
    groups: &[String],
) {
    if policies.is_empty() {
        let policy_documents = BTreeMap::<String, Value>::new();
        let missing_policies = Vec::<String>::new();
        let combined_policy = Value::Null;
        debug!(
            provider_id = %provider_id,
            parent_user = %parent_user,
            policy_count = 0,
            group_count = groups.len(),
            policies = ?policies,
            groups = ?groups,
            policy_documents = ?policy_documents,
            missing_policies = ?missing_policies,
            combined_policy = ?combined_policy,
            "OIDC STS policy diagnostics"
        );
        return;
    }

    match iam_store.list_policy_docs("").await {
        Ok(policy_docs) => {
            let mut policy_documents = BTreeMap::new();
            let mut missing_policies = Vec::new();
            for policy_name in policies {
                match policy_docs.get(policy_name) {
                    Some(policy_doc) => {
                        let policy_doc_json = serde_json::to_value(policy_doc).unwrap_or_else(|err| {
                            serde_json::json!({
                                "serialization_error": err.to_string(),
                            })
                        });
                        policy_documents.insert(policy_name.clone(), policy_doc_json);
                    }
                    None => missing_policies.push(policy_name.clone()),
                }
            }

            let combined_policy = iam_store.get_combined_policy(policies).await;
            let combined_policy_json = serde_json::to_value(&combined_policy).unwrap_or_else(|err| {
                serde_json::json!({
                    "serialization_error": err.to_string(),
                })
            });

            debug!(
                provider_id = %provider_id,
                parent_user = %parent_user,
                policy_count = policies.len(),
                group_count = groups.len(),
                policies = ?policies,
                groups = ?groups,
                policy_documents = ?policy_documents,
                missing_policies = ?missing_policies,
                combined_policy = ?combined_policy_json,
                "OIDC STS policy diagnostics"
            );
        }
        Err(err) => {
            warn!(
                provider_id = %provider_id,
                parent_user = %parent_user,
                policy_count = policies.len(),
                group_count = groups.len(),
                policies = ?policies,
                groups = ?groups,
                error = %err,
                "OIDC STS policy diagnostics failed"
            );
        }
    }
}

fn binding_error_from_s3(error: S3Error) -> FederatedSessionBindingError {
    let message = error.message().unwrap_or("federated session binding failed").to_string();
    if error.code() == &S3ErrorCode::InvalidRequest {
        FederatedSessionBindingError::InvalidRequest(message)
    } else {
        FederatedSessionBindingError::Internal(message)
    }
}

/// Returns true when a derived federated `parent_user` collides with the root access key.
///
/// The comparison mirrors the owner check performed at request time (a temporary credential
/// whose `parent_user` equals the root access key is treated as owner), so a federated login
/// whose `username`/`email`/`sub` happens to equal the root access key must be rejected at
/// issuance rather than silently granted owner.
fn parent_user_is_reserved(parent_user: &str, root_access_key: Option<&str>) -> bool {
    matches!(root_access_key, Some(root) if root == parent_user)
}

fn issue_credentials(
    transaction: &FederatedSessionTransaction,
    selected_policy_names: &[String],
    secret: Option<&str>,
) -> Result<rustfs_credentials::Credentials, FederatedSessionBindingError> {
    let authorization = &transaction.authorization;
    let claims = &authorization.claims;
    let parent_user = authorization.oidc_virtual_parent().ok_or_else(|| {
        FederatedSessionBindingError::InvalidRequest("verified OIDC identity is missing issuer or subject".to_string())
    })?;
    let mut token_claims = build_oidc_token_claims(transaction);
    let duration = i64::try_from(transaction.duration_seconds)
        .map_err(|_| FederatedSessionBindingError::InvalidRequest("invalid duration".to_string()))?;
    let exp = OffsetDateTime::now_utc().saturating_add(Duration::seconds(duration));
    token_claims.insert("exp".to_string(), Value::Number(serde_json::Number::from(exp.unix_timestamp())));

    // Fail closed if the derived federated parent collides with the root access key. At IAM
    // request time `parent_user == root access key` is treated as owner, so issuing such a
    // credential would silently grant a federated identity full owner access.
    let root_access_key = current_action_credentials().map(|cred| cred.access_key);
    if parent_user_is_reserved(&parent_user, root_access_key.as_deref()) {
        return Err(FederatedSessionBindingError::InvalidRequest(
            "federated identity resolves to a reserved root principal".to_string(),
        ));
    }
    debug!(
        provider_id = %authorization.provider_id,
        parent_user = %parent_user,
        email = %claims.email,
        username = %claims.username,
        sub = %claims.sub,
        policy_count = authorization.policies.len(),
        group_count = authorization.groups.len(),
        policies = ?authorization.policies,
        groups = ?authorization.groups,
        roles_claim_key = ?authorization.roles_claim_key,
        has_session_policy = transaction.session_policy.is_some(),
        "OIDC STS credential claims prepared"
    );
    token_claims.insert("parent".to_string(), Value::String(parent_user.clone()));
    token_claims.insert(OIDC_VIRTUAL_PARENT_CLAIM.to_string(), Value::String(parent_user.clone()));

    if !selected_policy_names.is_empty() {
        token_claims.insert("policy".to_string(), Value::String(selected_policy_names.join(",")));
    } else {
        token_claims.remove("policy");
    }
    if let Some(policy) = transaction.session_policy.as_deref() {
        populate_session_policy(&mut token_claims, policy).map_err(binding_error_from_s3)?;
    }

    let secret = secret.ok_or_else(|| FederatedSessionBindingError::Internal("token signing key not initialized".to_string()))?;
    let mut credentials = get_new_credentials_with_metadata(&token_claims, secret)
        .map_err(|error| FederatedSessionBindingError::Internal(format!("credential generation failed: {error}")))?;
    credentials.parent_user = parent_user;
    credentials.groups = Some(authorization.groups.clone());
    Ok(credentials)
}

fn site_replication_item(credentials: &rustfs_credentials::Credentials, updated_at: OffsetDateTime) -> SRIAMItem {
    SRIAMItem {
        r#type: "sts-credential".to_string(),
        sts_credential: Some(SRSTSCredential {
            access_key: credentials.access_key.clone(),
            secret_key: credentials.secret_key.clone(),
            session_token: credentials.session_token.clone(),
            parent_user: credentials.parent_user.clone(),
            parent_policy_mapping: OIDC_STS_REQUIRES_VIRTUAL_PARENT_RECEIVER_POLICY.to_string(),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
        }),
        updated_at: Some(updated_at),
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
        ..Default::default()
    }
}

#[async_trait::async_trait]
impl FederatedSessionBinding for DefaultFederatedSessionBinding {
    async fn bind(
        &self,
        transaction: &FederatedSessionTransaction,
    ) -> Result<rustfs_credentials::Credentials, FederatedSessionBindingError> {
        let authorization = &transaction.authorization;
        let iam_store =
            current_ready_iam_handle().map_err(|_| FederatedSessionBindingError::Internal("IAM not initialized".to_string()))?;
        let parent_user = authorization.oidc_virtual_parent().ok_or_else(|| {
            FederatedSessionBindingError::InvalidRequest("verified OIDC identity is missing issuer or subject".to_string())
        })?;
        let selected_policy_names = match iam_store
            .policy_db_get(&parent_user, &Some(authorization.groups.clone()))
            .await
            .map_err(|_| FederatedSessionBindingError::Internal("failed to resolve OIDC policy mapping".to_string()))?
        {
            mapped_policy_names if !mapped_policy_names.is_empty() => mapped_policy_names,
            _ => authorization.policies.clone(),
        };
        let selected_policy_mapping = selected_policy_names.join(",");
        let resolved_policy_mapping = iam_store.current_policies(&selected_policy_mapping).await;
        if !all_oidc_policies_resolved(&selected_policy_names, &resolved_policy_mapping) {
            return Err(FederatedSessionBindingError::InvalidRequest(
                "OIDC policy mapping did not resolve to current policies".to_string(),
            ));
        }

        let secret = current_token_signing_key();
        let credentials = issue_credentials(transaction, &selected_policy_names, secret.as_deref())?;
        if tracing::enabled!(tracing::Level::DEBUG) {
            log_oidc_policy_diagnostics(
                &iam_store,
                &authorization.provider_id,
                &credentials.parent_user,
                &selected_policy_names,
                &authorization.groups,
            )
            .await;
        }

        let updated_at = iam_store
            .set_temp_user(&credentials.access_key, &credentials, None)
            .await
            .map_err(|_| FederatedSessionBindingError::Internal("failed to store temp user".to_string()))?;

        if let Err(err) = site_replication_iam_change_hook(site_replication_item(&credentials, updated_at)).await {
            warn!("site replication OIDC STS hook failed, err: {err}");
        }

        Ok(credentials)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_iam::federation::{FederatedAuthorization, FederatedClaims};

    fn transaction() -> FederatedSessionTransaction {
        FederatedSessionTransaction {
            authorization: FederatedAuthorization {
                provider_id: "default".to_string(),
                claims: FederatedClaims {
                    sub: "subject".to_string(),
                    email: "user@example.com".to_string(),
                    username: "user".to_string(),
                    groups: vec!["source-group".to_string()],
                    raw: HashMap::from([("iss".to_string(), serde_json::json!("https://idp.example.test"))]),
                },
                policies: vec!["readwrite".to_string()],
                groups: vec!["devs".to_string()],
                roles_claim_key: Some("roles".to_string()),
                roles: vec!["admin".to_string(), "reader".to_string()],
            },
            duration_seconds: 3600,
            session_policy: None,
        }
    }

    #[test]
    fn token_claims_preserve_existing_oidc_shape() {
        let transaction = transaction();

        let claims = build_oidc_token_claims(&transaction);
        assert_eq!(claims.get("sub"), Some(&serde_json::json!("subject")));
        assert_eq!(claims.get("iss"), Some(&serde_json::json!("rustfs-oidc")));
        assert_eq!(claims.get("oidc_provider"), Some(&serde_json::json!("default")));
        assert_eq!(claims.get("email"), Some(&serde_json::json!("user@example.com")));
        assert_eq!(claims.get("preferred_username"), Some(&serde_json::json!("user")));
        assert_eq!(claims.get("groups"), Some(&serde_json::json!(["devs"])));
        assert_eq!(claims.get("roles"), Some(&serde_json::json!(["admin", "reader"])));
    }

    #[test]
    fn issued_credentials_and_replication_item_preserve_existing_shape() {
        let transaction = transaction();
        let secret = "federated-session-test-signing-secret";
        let selected_policy_names = vec!["readonly".to_string()];

        let credentials =
            issue_credentials(&transaction, &selected_policy_names, Some(secret)).expect("credential issuance should succeed");
        assert_eq!(credentials.parent_user, "openid=pUmguI1petsjVfDFQppmmR9yqdmWnBAXGJhHV_s9W3I");
        assert_eq!(credentials.groups, Some(vec!["devs".to_string()]));

        let claims = rustfs_iam::sys::get_claims_from_token_with_secret(&credentials.session_token, secret)
            .expect("issued session token should verify");
        assert_eq!(claims.get("iss"), Some(&serde_json::json!("rustfs-oidc")));
        assert_eq!(claims.get("oidc_provider"), Some(&serde_json::json!("default")));
        assert_eq!(
            claims.get("parent"),
            Some(&serde_json::json!("openid=pUmguI1petsjVfDFQppmmR9yqdmWnBAXGJhHV_s9W3I"))
        );
        assert_eq!(
            claims.get(OIDC_VIRTUAL_PARENT_CLAIM),
            Some(&serde_json::json!("openid=pUmguI1petsjVfDFQppmmR9yqdmWnBAXGJhHV_s9W3I"))
        );
        assert_eq!(claims.get("policy"), Some(&serde_json::json!("readonly")));
        assert_eq!(claims.get("groups"), Some(&serde_json::json!(["devs"])));
        assert_eq!(claims.get("roles"), Some(&serde_json::json!(["admin", "reader"])));
        assert!(!claims.contains_key("oidc_issuer"));

        let updated_at = OffsetDateTime::UNIX_EPOCH;
        let item = site_replication_item(&credentials, updated_at);
        assert_eq!(item.r#type, "sts-credential");
        assert_eq!(item.updated_at, Some(updated_at));
        assert_eq!(item.api_version.as_deref(), Some(SITE_REPL_API_VERSION));
        let replicated = item.sts_credential.expect("replication item should contain STS credentials");
        assert_eq!(replicated.access_key, credentials.access_key);
        assert_eq!(replicated.secret_key, credentials.secret_key);
        assert_eq!(replicated.session_token, credentials.session_token);
        assert_eq!(replicated.parent_user, "openid=pUmguI1petsjVfDFQppmmR9yqdmWnBAXGJhHV_s9W3I");
        assert_eq!(replicated.parent_policy_mapping, OIDC_STS_REQUIRES_VIRTUAL_PARENT_RECEIVER_POLICY);
        assert!(replicated.parent_policy_mapping.trim().is_empty());
        assert!(MappedPolicy::new(&replicated.parent_policy_mapping).to_slice().is_empty());
        assert_eq!(replicated.api_version.as_deref(), Some(SITE_REPL_API_VERSION));
    }

    #[test]
    fn invalid_session_policy_precedes_missing_signing_key() {
        let mut transaction = transaction();
        transaction.session_policy = Some("not-json".to_string());

        let error = issue_credentials(&transaction, &transaction.authorization.policies, None)
            .expect_err("invalid policy should fail first");
        assert!(matches!(error, FederatedSessionBindingError::InvalidRequest(_)));
    }

    #[test]
    fn parent_user_reserved_only_on_root_collision() {
        // Collides with the configured root access key -> reserved.
        assert!(parent_user_is_reserved("rustfsadmin", Some("rustfsadmin")));
        // A distinct federated identity is never reserved.
        assert!(!parent_user_is_reserved("user", Some("rustfsadmin")));
        // Access keys are case-sensitive; a case-different value does not collide.
        assert!(!parent_user_is_reserved("RustFsAdmin", Some("rustfsadmin")));
        // Without a resolved root access key there is nothing to collide with.
        assert!(!parent_user_is_reserved("rustfsadmin", None));
    }

    #[test]
    fn issue_credentials_rejects_root_collision_before_generation() {
        // Reuse the collision decision the issuance path applies: a federated identity whose
        // derived parent_user equals the root access key must be denied at issuance.
        let transaction = transaction();
        let parent_user = transaction
            .authorization
            .oidc_virtual_parent()
            .expect("fixture must contain issuer and subject");
        assert!(parent_user_is_reserved(&parent_user, Some(&parent_user)));
        assert!(!parent_user_is_reserved(&parent_user, Some("root")));
    }

    #[test]
    fn missing_oidc_issuer_fails_closed_before_credential_issuance() {
        let mut transaction = transaction();
        transaction.authorization.claims.raw.clear();

        let error = issue_credentials(&transaction, &transaction.authorization.policies, Some("signing-secret"))
            .expect_err("credential issuance should reject a missing issuer");
        assert!(matches!(error, FederatedSessionBindingError::InvalidRequest(_)));
    }

    #[test]
    fn oidc_replication_requires_all_selected_policies() {
        assert!(all_oidc_policies_resolved(&["readonly".to_string()], "readonly"));
        assert!(!all_oidc_policies_resolved(&["readonly".to_string(), "missing".to_string()], "readonly"));
        assert!(!all_oidc_policies_resolved(&[], ""));
        assert!(!all_oidc_policies_resolved(&["team+readonly".to_string()], "team+readonly"));
    }
}
