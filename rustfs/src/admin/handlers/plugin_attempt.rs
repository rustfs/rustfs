// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0
//! Restricted vending for explicitly configured brokers, never AssumeRole chaining.
use super::plugin_delegation::{owner, response, storage_error};
use crate::admin::operation::{AdminRequest, AdminResponse, Operation};
use crate::{
    admin::{
        router::{AdminOperation, S3Router},
        runtime_sources,
        storage_api::s3::{self, S3Error, S3ErrorCode, S3Result},
    },
    auth::check_key_valid_with_context,
    server::ADMIN_PREFIX,
};
use http::{Method, StatusCode};
use matchit::Params;
use rustfs_iam::{
    delegation::Delegation,
    sys::{PLUGIN_ATTEMPT_CREDENTIAL_CLAIM, SESSION_POLICY_NAME},
};
use rustfs_policy::{
    auth::get_new_credentials_with_metadata,
    policy::{
        Args, Policy,
        action::{Action, AdminAction, S3Action},
    },
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::OnceLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use time::OffsetDateTime;

// Operator configuration contains public signer access keys, not credentials.
// Empty/invalid configuration disables the endpoint. It is immutable per process.
static BROKERS: OnceLock<Option<HashMap<String, String>>> = OnceLock::new();
fn configured_deployment(key: &str) -> Option<&'static str> {
    BROKERS
        .get_or_init(|| {
            let raw = std::env::var("RUSTFS_PLUGIN_BROKERS").ok()?;
            let map: HashMap<String, String> = serde_json::from_str(&raw).ok()?;
            if map.is_empty()
                || map
                    .iter()
                    .any(|(k, v)| k.is_empty() || v.is_empty() || k.len() > 1024 || v.len() > 1024)
            {
                return None;
            }
            Some(map)
        })
        .as_ref()?
        .get(key)
        .map(String::as_str)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Attempt {
    tenant: String,
    run_id: uuid::Uuid,
    attempt_id: uuid::Uuid,
    revision: String,
    deployment: String,
    input_bucket: String,
    input_key: String,
    input_version: String,
    output_bucket: String,
    output_prefix: String,
    ttl_seconds: u64,
}
#[derive(Serialize)]
struct Issued {
    version: u32,
    delegation_id: String,
    delegation_revision: String,
    owner_access_key: String,
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
    expires_at_unix_seconds: i64,
}

pub fn register_plugin_attempt_route(router: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    router.insert(
        Method::POST,
        &format!("{ADMIN_PREFIX}/v3/plugin-delegations/{{id}}/issue-attempt"),
        AdminOperation(&Issue),
    )?;
    Ok(())
}
fn now() -> S3Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|_| s3::error(S3ErrorCode::InternalError, "clock unavailable"))
}
fn literal(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 1024
        && !value.contains("${")
        && !value.chars().any(|c| c.is_control() || matches!(c, '*' | '?'))
}
fn session_policy(record: &Delegation, attempt: &Attempt) -> S3Result<Policy> {
    let restriction = &record.restrictions;
    // Stored records from earlier versions must obey the same literal rules.
    restriction.validate(record.created_at_unix_seconds).map_err(storage_error)?;
    let exact_output = format!(
        "{}{}/{}/{}/",
        restriction.output_prefix,
        hex_simd::encode_to_string(attempt.tenant.as_bytes(), hex_simd::AsciiCase::Lower),
        attempt.run_id,
        attempt.attempt_id
    );
    if attempt.tenant.is_empty()
        || attempt.tenant.len() > 1024
        || attempt.run_id.is_nil()
        || attempt.attempt_id.is_nil()
        || attempt.output_prefix != exact_output
        || attempt.revision != record.revision
        || attempt.deployment != restriction.deployment
        || attempt.input_bucket != restriction.bucket
        || attempt.output_bucket != restriction.output_bucket().map_err(storage_error)?
        || !literal(&attempt.input_key)
        || !attempt.input_key.starts_with(&restriction.input_prefix)
        || !literal(&attempt.input_version)
        || attempt.input_version == "null"
        || !literal(&attempt.output_prefix)
        || !attempt.output_prefix.starts_with(&restriction.output_prefix)
        || attempt.output_prefix == restriction.output_prefix
        || !attempt.output_prefix.ends_with('/')
        || !(1..=900).contains(&attempt.ttl_seconds)
    {
        return Err(s3::error(S3ErrorCode::AccessDenied, "attempt restriction rejected"));
    }
    let value = serde_json::json!({"Version":"2012-10-17","Statement":[
        {"Effect":"Allow","Action":["s3:GetObjectVersion"],"Resource":[format!("arn:aws:s3:::{}/{}",attempt.input_bucket,attempt.input_key)],"Condition":{"StringEquals":{"s3:VersionId":attempt.input_version}}},
        {"Effect":"Allow","Action":["s3:PutObject"],"Resource":[format!("arn:aws:s3:::{}/{}*",attempt.output_bucket,attempt.output_prefix)]}
    ]});
    // Condition operators borrow their keys while the policy is decoded.
    let bytes = serde_json::to_vec(&value).map_err(|_| s3::error(S3ErrorCode::InternalError, "attempt policy unavailable"))?;
    serde_json::from_slice(&bytes).map_err(|_| s3::error(S3ErrorCode::InternalError, "attempt policy unavailable"))
}

fn issuance_error(code: &'static str, issued: bool, unknown: bool) -> S3Result<AdminResponse> {
    let (_, headers, body) =
        response(&serde_json::json!({"error_code":code,"issued":issued,"issuance_unknown":unknown}))?.into_parts();
    Ok(AdminResponse::with_headers(StatusCode::SERVICE_UNAVAILABLE, body, headers))
}
fn same_parent_authority(before_names: &[String], before_policy: &Policy, after_names: &[String], after_policy: &Policy) -> bool {
    fn names(names: &[String]) -> Vec<&str> {
        let mut names: Vec<_> = names.iter().map(String::as_str).collect();
        names.sort_unstable();
        names.dedup();
        names
    }
    fn statements(policy: &Policy) -> Option<Vec<Vec<u8>>> {
        let mut statements = policy
            .statements
            .iter()
            .map(serde_json::to_vec)
            .collect::<Result<Vec<_>, _>>()
            .ok()?;
        statements.sort_unstable();
        Some(statements)
    }

    names(before_names) == names(after_names)
        && matches!(
            (statements(before_policy), statements(after_policy)),
            (Some(before), Some(after)) if before == after
        )
}
async fn require_builtin_authorization() -> S3Result<()> {
    rustfs_iam::sys::IamSys::<rustfs_iam::store::object::ObjectStore>::ensure_restricted_credential_vending()
        .await
        .map_err(|_| s3::error(S3ErrorCode::AccessDenied, "external policy plugin unsupported for attempt credentials"))
}

struct Issue;
#[async_trait::async_trait]
impl Operation for Issue {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        require_builtin_authorization().await?;
        let (iam, broker) = owner(&req, AdminAction::IssuePluginAttemptAction).await?;
        let deployment =
            configured_deployment(&broker).ok_or_else(|| s3::error(S3ErrorCode::AccessDenied, "broker deployment rejected"))?;
        let context = runtime_sources::app_context_from_envelope(req.envelope())
            .ok_or_else(|| s3::error(S3ErrorCode::InternalError, "application context unavailable"))?;
        let bytes = tokio::time::timeout(Duration::from_secs(10), req.take_body().store_all_limited(16384))
            .await
            .map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "attempt body unavailable"))?
            .map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "attempt body unavailable"))?;
        let attempt: Attempt =
            serde_json::from_slice(&bytes).map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "invalid attempt request"))?;
        if attempt.deployment != deployment {
            return Err(s3::error(S3ErrorCode::AccessDenied, "broker deployment rejected"));
        }
        let id = params
            .get("id")
            .ok_or_else(|| s3::error(S3ErrorCode::InvalidRequest, "delegation id required"))?;
        let record = iam
            .read_active_plugin_delegation_for_broker(id, now()?)
            .await
            .map_err(storage_error)?;
        let policy = session_policy(&record, &attempt)?;
        let (principal, root) = check_key_valid_with_context("", &record.owner_access_key, Some(context.as_ref())).await?;
        if root || principal.is_temp() || principal.is_service_account() {
            return Err(s3::error(S3ErrorCode::AccessDenied, "original user required"));
        }
        // A broker request cannot reconstruct the original user's IP/MFA/tag
        // conditions. This first profile rejects conditional parent policies.
        let policies = iam
            .policy_db_get(&principal.access_key, &principal.groups)
            .await
            .map_err(|_| s3::error(S3ErrorCode::InternalError, "parent policy unavailable"))?;
        let parent_policy = iam.get_combined_policy(&policies).await;
        if policies.is_empty() || parent_policy.statements.iter().any(|s| !s.conditions.is_empty()) {
            return Err(s3::error(S3ErrorCode::AccessDenied, "parent condition context unsupported"));
        }
        let mut conditions = HashMap::new();
        conditions.insert("s3:VersionId".to_owned(), vec![attempt.input_version.clone()]);
        if !iam
            .is_allowed(&Args {
                account: &principal.access_key,
                groups: &principal.groups,
                action: Action::S3Action(S3Action::GetObjectVersionAction),
                bucket: &attempt.input_bucket,
                object: &attempt.input_key,
                conditions: &conditions,
                is_owner: false,
                claims: principal.claims_or_empty(),
                deny_only: false,
            })
            .await
        {
            return Err(s3::error(S3ErrorCode::AccessDenied, "parent input rejected"));
        }
        let expiration = now()?
            .checked_add(attempt.ttl_seconds)
            .ok_or_else(|| s3::error(S3ErrorCode::InvalidRequest, "invalid expiration"))?
            .min(record.restrictions.expires_at_unix_seconds);
        let expires_at = OffsetDateTime::from_unix_timestamp(
            i64::try_from(expiration).map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "invalid expiration"))?,
        )
        .map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "invalid expiration"))?;
        let mut claims = principal.claims.clone().unwrap_or_default();
        claims.insert(PLUGIN_ATTEMPT_CREDENTIAL_CLAIM.into(), serde_json::Value::Bool(true));
        claims.insert("exp".into(), serde_json::json!(expires_at.unix_timestamp()));
        claims.insert("parent".into(), serde_json::json!(principal.access_key));
        let bytes =
            serde_json::to_vec(&policy).map_err(|_| s3::error(S3ErrorCode::InternalError, "attempt policy unavailable"))?;
        claims.insert(
            SESSION_POLICY_NAME.into(),
            serde_json::json!(base64_simd::URL_SAFE_NO_PAD.encode_to_string(&bytes)),
        );
        let fresh = iam
            .read_active_plugin_delegation_for_broker(id, now()?)
            .await
            .map_err(storage_error)?;
        if fresh != record {
            return Err(s3::error(S3ErrorCode::AccessDenied, "attempt authority changed"));
        }
        owner(&req, AdminAction::IssuePluginAttemptAction).await?;
        require_builtin_authorization().await?;
        let signing = context
            .action_credentials()
            .get()
            .ok_or_else(|| s3::error(S3ErrorCode::InternalError, "token signer unavailable"))?;
        // The server key seals the token only. Authorization above uses the
        // original user's current IAM, and S3 continues its parent intersection.
        let mut credential = get_new_credentials_with_metadata(&claims, &signing.secret_key)
            .map_err(|_| s3::error(S3ErrorCode::InternalError, "attempt credential unavailable"))?;
        credential.parent_user = principal.access_key.clone();
        if iam.set_temp_user(&credential.access_key, &credential, None).await.is_err() {
            return issuance_error("PLUGIN_ATTEMPT_ISSUANCE_UNKNOWN", false, true);
        }
        // No atomic IAM/delegation transaction spans persistence. Recheck and
        // invalidate the generated credential if any authority changed.
        let valid = async {
            require_builtin_authorization().await?;
            let after = iam
                .read_active_plugin_delegation_for_broker(id, now()?)
                .await
                .map_err(storage_error)?;
            let (current, current_root) =
                check_key_valid_with_context("", &record.owner_access_key, Some(context.as_ref())).await?;
            let names = iam
                .policy_db_get(&current.access_key, &current.groups)
                .await
                .map_err(|_| s3::error(S3ErrorCode::InternalError, "parent policy unavailable"))?;
            let current_policy = iam.get_combined_policy(&names).await;
            if after != record
                || current_root
                || current.is_temp()
                || current.is_service_account()
                || current.secret_key != principal.secret_key
                || !same_parent_authority(&policies, &parent_policy, &names, &current_policy)
            {
                return Err(s3::error(S3ErrorCode::AccessDenied, "attempt authority changed"));
            }
            // Re-evaluate the broker against its real signed request context.
            owner(&req, AdminAction::IssuePluginAttemptAction).await?;
            Ok::<(), S3Error>(())
        }
        .await;
        if valid.is_err() {
            if iam.delete_temp_account(&credential.access_key, true).await.is_err() {
                return issuance_error("PLUGIN_ATTEMPT_INVALIDATION_UNKNOWN", true, true);
            }
            return issuance_error("PLUGIN_ATTEMPT_POST_ISSUANCE_REJECTED", true, false);
        }
        response(&Issued {
            version: 1,
            delegation_id: record.id,
            delegation_revision: record.revision,
            owner_access_key: record.owner_access_key,
            access_key_id: credential.access_key,
            secret_access_key: credential.secret_key,
            session_token: credential.session_token,
            expires_at_unix_seconds: expires_at.unix_timestamp(),
        })
    }
}

#[cfg(test)]
#[path = "plugin_attempt_v2_tests.rs"]
mod v2_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parent_authority_recheck_ignores_policy_order_but_detects_changes() {
        let first: Policy = serde_json::from_value(serde_json::json!({
            "Version":"2012-10-17",
            "Statement":[
                {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::photos/input/*"},
                {"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::photos/output/*"}
            ]
        }))
        .unwrap();
        let reordered: Policy = serde_json::from_value(serde_json::json!({
            "Version":"2012-10-17",
            "Statement":[
                {"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::photos/output/*"},
                {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::photos/input/*"}
            ]
        }))
        .unwrap();
        let before_names = vec!["owner-policy".to_owned(), "bucket-policy".to_owned()];
        let after_names = vec!["bucket-policy".to_owned(), "owner-policy".to_owned()];
        assert!(same_parent_authority(&before_names, &first, &after_names, &reordered));

        let changed: Policy = serde_json::from_value(serde_json::json!({
            "Version":"2012-10-17",
            "Statement":[
                {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::photos/input/*"},
                {"Effect":"Allow","Action":"s3:DeleteObject","Resource":"arn:aws:s3:::photos/output/*"}
            ]
        }))
        .unwrap();
        assert!(!same_parent_authority(&before_names, &first, &after_names, &changed));
        let changed_resource: Policy = serde_json::from_value(serde_json::json!({
            "Version":"2012-10-17",
            "Statement":[
                {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::photos/input/*"},
                {"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::photos/private/*"}
            ]
        }))
        .unwrap();
        assert!(!same_parent_authority(&before_names, &first, &after_names, &changed_resource));
        let added_deny: Policy = serde_json::from_value(serde_json::json!({
            "Version":"2012-10-17",
            "Statement":[
                {"Effect":"Deny","Action":"s3:GetObject","Resource":"arn:aws:s3:::photos/input/private/*"},
                {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::photos/input/*"},
                {"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::photos/output/*"}
            ]
        }))
        .unwrap();
        assert!(!same_parent_authority(&before_names, &first, &after_names, &added_deny));
        assert!(!same_parent_authority(&before_names, &first, &["other-policy".to_owned()], &first));
    }
    #[tokio::test]
    async fn attempt_policy_keeps_exact_version_and_narrow_output() {
        let record = Delegation {
            version: 1,
            id: "a".repeat(32),
            revision: "a".repeat(32),
            owner_access_key: "user".into(),
            created_at_unix_seconds: 1,
            restrictions: rustfs_iam::delegation::Restrictions {
                version: None,
                output_bucket: None,
                deployment: "runner".into(),
                bucket: "photos".into(),
                input_prefix: "input/".into(),
                output_prefix: "output/".into(),
                expires_at_unix_seconds: 1000,
            },
        };
        let run_id = uuid::Uuid::new_v4();
        let attempt_id = uuid::Uuid::new_v4();
        let output_prefix = format!(
            "output/{}/{}/{}/",
            hex_simd::encode_to_string(b"tenant", hex_simd::AsciiCase::Lower),
            run_id,
            attempt_id
        );
        let mut attempt = Attempt {
            tenant: "tenant".into(),
            run_id,
            attempt_id,
            revision: record.revision.clone(),
            deployment: "runner".into(),
            input_bucket: "photos".into(),
            input_key: "input/photo.png".into(),
            input_version: "version-1".into(),
            output_bucket: "photos".into(),
            output_prefix: output_prefix.clone(),
            ttl_seconds: 60,
        };
        let policy = session_policy(&record, &attempt).unwrap();
        let scoped_output = format!("{output_prefix}result");
        let groups = None;
        let claims = HashMap::new();
        for (action, object, version, expected) in [
            (S3Action::GetObjectVersionAction, "input/photo.png", Some("version-1"), true),
            (S3Action::GetObjectVersionAction, "input/photo.png", Some("version-2"), false),
            (S3Action::GetObjectVersionAction, "input/photo.png", None, false),
            (S3Action::GetObjectAction, "input/photo.png", None, false),
            (S3Action::PutObjectAction, scoped_output.as_str(), None, true),
            (S3Action::PutObjectAction, "outside/result", None, false),
            (S3Action::PutObjectAction, "output/result", None, false),
        ] {
            let conditions = version
                .map(|value| HashMap::from([("versionid".into(), vec![value.into()])]))
                .unwrap_or_default();
            assert_eq!(
                policy
                    .is_allowed(&Args {
                        account: "user",
                        groups: &groups,
                        action: Action::S3Action(action),
                        bucket: "photos",
                        object,
                        conditions: &conditions,
                        is_owner: false,
                        claims: &claims,
                        deny_only: false,
                    })
                    .await,
                expected,
                "attempt policy action={action:?} object={object} version={version:?}"
            );
        }
        attempt.output_bucket = "artifacts".into();
        assert!(
            session_policy(&record, &attempt).is_err(),
            "v1 still requires the input bucket for output"
        );
        attempt.output_bucket = "photos".into();
        attempt.output_prefix = "output/".into();
        assert!(session_policy(&record, &attempt).is_err());
        attempt.output_prefix = output_prefix;
        attempt.input_key = "outside/photo.png".into();
        assert!(session_policy(&record, &attempt).is_err());
    }
    #[tokio::test]
    async fn attempt_v2_policy_keeps_input_version_and_output_bucket() {
        let record: Delegation = serde_json::from_value(serde_json::json!({
            "version":2,"id":"a".repeat(32),"revision":"a".repeat(32),"owner_access_key":"user",
            "created_at_unix_seconds":1,"restrictions":{"version":2,"deployment":"runner","bucket":"photos",
            "input_prefix":"input/","output_bucket":"artifacts","output_prefix":"output/","expires_at_unix_seconds":1000}
        }))
        .expect("explicit v2 delegation");
        let run_id = uuid::Uuid::new_v4();
        let attempt_id = uuid::Uuid::new_v4();
        let output = format!("output/74656e616e74/{run_id}/{attempt_id}/");
        let mut attempt = Attempt {
            tenant: "tenant".into(),
            run_id,
            attempt_id,
            revision: record.revision.clone(),
            deployment: "runner".into(),
            input_bucket: "photos".into(),
            input_key: "input/photo.png".into(),
            input_version: "version-1".into(),
            output_bucket: "artifacts".into(),
            output_prefix: output.clone(),
            ttl_seconds: 60,
        };
        let policy = session_policy(&record, &attempt).expect("v2 scope");
        let result = format!("{output}result");
        for (action, bucket, object, version, expected) in [
            (S3Action::GetObjectVersionAction, "photos", "input/photo.png", Some("version-1"), true),
            (S3Action::GetObjectVersionAction, "photos", "input/photo.png", Some("version-2"), false),
            (S3Action::GetObjectVersionAction, "artifacts", "input/photo.png", Some("version-1"), false),
            (S3Action::PutObjectAction, "artifacts", result.as_str(), None, true),
            (S3Action::PutObjectAction, "photos", result.as_str(), None, false),
            (S3Action::PutObjectAction, "third", result.as_str(), None, false),
            (S3Action::PutObjectAction, "artifacts", "output/other/result", None, false),
        ] {
            let conditions = version
                .map(|v| HashMap::from([("versionid".into(), vec![v.into()])]))
                .unwrap_or_default();
            assert_eq!(
                policy
                    .is_allowed(&Args {
                        account: "user",
                        groups: &None,
                        action: Action::S3Action(action),
                        bucket,
                        object,
                        conditions: &conditions,
                        claims: &HashMap::new(),
                        is_owner: false,
                        deny_only: false
                    })
                    .await,
                expected,
                "A03_CROSS_BUCKET_SCOPE: bucket={bucket} object={object}"
            );
        }
        attempt.output_bucket = "third".into();
        assert!(session_policy(&record, &attempt).is_err(), "A03_CROSS_BUCKET_OUTPUT_BOUND");
        attempt.output_bucket = "photos".into();
        assert!(session_policy(&record, &attempt).is_err(), "A03_CROSS_BUCKET_OUTPUT_BOUND");
        attempt.output_bucket = "artifacts".into();
        attempt.input_bucket = "artifacts".into();
        assert!(session_policy(&record, &attempt).is_err(), "A03_CROSS_BUCKET_INPUT_BOUND");
    }

    #[tokio::test]
    async fn actual_policy_variable_resolution_is_rejected_by_literal_scope() {
        let resource = rustfs_policy::policy::resource::Resource::S3("photos/input/${aws:username}.png".into());
        let conditions = HashMap::from([("username".into(), vec!["alice".into()])]);
        let resolver =
            rustfs_policy::policy::variables::VariableResolver::new(rustfs_policy::policy::variables::VariableContext {
                username: Some("alice".into()),
                ..Default::default()
            });
        assert!(
            resource
                .is_match_with_resolver("photos/input/alice.png", &conditions, Some(&resolver))
                .await,
            "C07_LITERAL_SCOPE_BOUND: actual policy engine substitutes resource variables"
        );
        assert!(
            !literal("input/${aws:username}.png"),
            "C07_LITERAL_SCOPE_BOUND: requested resources must stay literal"
        );
        let restriction = rustfs_iam::delegation::Restrictions {
            version: None,
            output_bucket: None,
            deployment: "runner".into(),
            bucket: "photos".into(),
            input_prefix: "input/${aws:username}/".into(),
            output_prefix: "output/".into(),
            expires_at_unix_seconds: 200,
        };
        assert!(
            restriction.validate(100).is_err(),
            "C07_LITERAL_SCOPE_BOUND: legacy stored variable restrictions are rejected"
        );
    }
}
