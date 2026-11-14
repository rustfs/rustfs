// Copyright 2024 RustFS Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Advisory, caller-bound permission queries. A result never authorizes a later operation.
//!
//! `POST /rustfs/admin/v4/account/permissions` accepts at most 128 checks in a 64 KiB JSON body.
//! Each check has a unique `id`, concrete `action`, and optional `bucket`, `object`, `versionId`,
//! `keyId`, and list `prefix`. Supported operation facts are retention date/mode, legal hold,
//! request tags, standard planned S3 `authType`, and structured `copySource`. They cannot supply
//! identity claims, network facts, arbitrary headers, or another caller. A copy result checks
//! the destination action only; source reads and COPY tagging remain separate authorization.
//! Results contain only `id`, `decision`, and, for an unknown target,
//! optional `potential`: discoverability somewhere in the known bucket, never authorization of
//! a prefix or object. Unsupported operation facts and unavailable dependencies are `unknown`.
//! Invalid schema returns `InvalidRequest`; authentication errors retain the normal S3 codes.
//! Responses are uncached and expose no policies, claims, credentials, or internal denial reasons.
//! S3 bucket-only actions ignore unrelated object/version targets. Namespace-root checks explicitly
//! use an empty object; table/view creation checks use their parent namespace resource.

use crate::admin::auth::{
    AdminResourceScope, authenticate_envelope_credentials, validate_admin_action_with_bucket_object_for_iam,
};
use crate::admin::operation::{AdminRequest, AdminResponse, Operation};
use crate::admin::router::{AdminOperation, S3Router};
use crate::admin::runtime_sources::current_ready_iam_handle;
use crate::app::RequestEnvelope;
use crate::auth::get_condition_values;
use crate::server::ADMIN_PREFIX;
use crate::storage::access::{
    AuthorizationRequest, ReqInfo, RequestedCopySource, RequestedObjectLegalHold, RequestedObjectLockRetainUntil,
    RequestedObjectLockRetention, RequestedObjectTags, authorize_request_core, query_list_buckets_permission,
    s3_action_potential,
};
use http::{HeaderMap, HeaderValue, Method, StatusCode, Uri};
use matchit::Params;
use rustfs_credentials::Credentials;
use rustfs_policy::policy::{
    Args,
    action::{Action, S3Action},
};
use rustfs_s3_types::{Body, S3ErrorCode, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashSet;
use time::OffsetDateTime;

const MAX_CHECKS: usize = 128;
const MAX_BODY_SIZE: usize = 64 * 1024;
const MAX_OBJECT_SIZE: usize = 1024;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PermissionsRequest {
    checks: Vec<PermissionCheck>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PermissionCheck {
    id: String,
    action: String,
    bucket: Option<String>,
    object: Option<String>,
    version_id: Option<String>,
    key_id: Option<String>,
    prefix: Option<String>,
    retain_until_date: Option<String>,
    retention_mode: Option<String>,
    legal_hold_status: Option<String>,
    request_tags: Option<BTreeMap<String, String>>,
    auth_type: Option<String>,
    #[serde(default, deserialize_with = "present_option")]
    copy_source: Option<Option<CopySourceFacts>>,
    #[serde(default, deserialize_with = "present_option")]
    if_match: Option<Option<String>>,
    #[serde(default, deserialize_with = "present_option")]
    if_none_match: Option<Option<String>>,
    operation: Option<String>,
}

fn present_option<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CopySourceFacts {
    bucket: String,
    object: String,
    version_id: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Decision {
    Allowed,
    Denied,
    Unknown,
}

#[derive(Debug, Serialize)]
struct PermissionResult {
    id: String,
    decision: Decision,
    #[serde(skip_serializing_if = "Option::is_none")]
    potential: Option<bool>,
}

#[derive(Debug, Serialize)]
struct PermissionsResponse {
    results: Vec<PermissionResult>,
}

pub struct AccountPermissionsHandler;

pub fn register_account_permissions_route(router: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    router.insert(
        Method::POST,
        &format!("{ADMIN_PREFIX}/v4/account/permissions"),
        AdminOperation(&AccountPermissionsHandler),
    )
}

impl PermissionsRequest {
    fn validate(&self) -> S3Result<Vec<Action>> {
        if self.checks.is_empty() || self.checks.len() > MAX_CHECKS {
            return Err(s3_error!(InvalidRequest, "permission check count must be between 1 and 128"));
        }
        let mut ids = HashSet::new();
        self.checks
            .iter()
            .map(|check| {
                if check.id.is_empty() || check.id.len() > 128 || !ids.insert(&check.id) {
                    return Err(s3_error!(InvalidRequest, "permission check ids must be unique and non-empty"));
                }
                if check.action.contains(['*', '?']) {
                    return Err(s3_error!(InvalidRequest, "permission checks require concrete actions"));
                }
                let action = Action::try_from(check.action.as_str())
                    .map_err(|_| s3_error!(InvalidRequest, "invalid permission action"))?;
                if !matches!(action, Action::S3Action(_) | Action::AdminAction(_) | Action::KmsAction(_)) {
                    return Err(s3_error!(InvalidRequest, "unsupported permission action"));
                }
                if check
                    .bucket
                    .as_ref()
                    .is_some_and(|value| value.len() > 255 || value.contains(['/', '\0', '*', '?']))
                    || check
                        .object
                        .as_ref()
                        .is_some_and(|value| value.len() > MAX_OBJECT_SIZE || value.contains('\0'))
                    || check
                        .prefix
                        .as_ref()
                        .is_some_and(|value| value.len() > MAX_OBJECT_SIZE || value.contains('\0'))
                    || check
                        .key_id
                        .as_ref()
                        .is_some_and(|value| value.len() > 2048 || value.contains(['\0', '*', '?']))
                    || check
                        .version_id
                        .as_ref()
                        .is_some_and(|value| value.is_empty() || value.len() > 1024 || value.contains('\0'))
                {
                    return Err(s3_error!(InvalidRequest, "invalid permission target"));
                }
                if check.key_id.is_some() && !matches!(action, Action::KmsAction(_)) {
                    return Err(s3_error!(InvalidRequest, "keyId is only valid for KMS actions"));
                }
                if check.version_id.is_some() && !check.action.starts_with("s3:") {
                    return Err(s3_error!(InvalidRequest, "versionId is only valid for S3 actions"));
                }
                if check.version_id.is_some()
                    && matches!(
                        check.action.as_str(),
                        "s3:GetObject"
                            | "s3:GetObjectAttributes"
                            | "s3:DeleteObject"
                            | "s3:GetObjectTagging"
                            | "s3:PutObjectTagging"
                            | "s3:DeleteObjectTagging"
                    )
                {
                    return Err(s3_error!(InvalidRequest, "version targets require version-specific actions"));
                }
                check.validate_operation_facts()?;
                Ok(action)
            })
            .collect()
    }
}

impl PermissionCheck {
    fn validate_operation_facts(&self) -> S3Result<()> {
        if self.operation.as_deref().is_some_and(|operation| {
            self.action != "s3:PutObject"
                || !matches!(
                    operation,
                    "PutObject"
                        | "CopyObject"
                        | "CreateMultipartUpload"
                        | "UploadPart"
                        | "UploadPartCopy"
                        | "CompleteMultipartUpload"
                )
        }) {
            return Err(s3_error!(InvalidRequest, "invalid S3 operation shape"));
        }
        if (self.copy_source.is_some() && self.action != "s3:PutObject")
            || (self.if_match.is_some() || self.if_none_match.is_some()) && !self.action.starts_with("s3:")
        {
            return Err(s3_error!(InvalidRequest, "invalid nullable request facts"));
        }

        for (value, supported) in [
            (
                &self.if_match,
                matches!(
                    self.action.as_str(),
                    "s3:GetObject" | "s3:GetObjectVersion" | "s3:PutObject" | "s3:DeleteObject" | "s3:DeleteObjectVersion"
                ),
            ),
            (
                &self.if_none_match,
                matches!(self.action.as_str(), "s3:GetObject" | "s3:GetObjectVersion" | "s3:PutObject"),
            ),
        ] {
            if value.as_ref().and_then(Option::as_ref).is_some_and(|value| {
                !supported
                    || value.is_empty()
                    || value.len() > 2048
                    || value.contains(['\r', '\n'])
                    || HeaderValue::from_str(value).is_err()
            }) {
                return Err(s3_error!(InvalidRequest, "invalid conditional S3 request facts"));
            }
        }
        if self
            .auth_type
            .as_deref()
            .is_some_and(|auth_type| !self.action.starts_with("s3:") || !matches!(auth_type, "REST-HEADER" | "REST-QUERY-STRING"))
        {
            return Err(s3_error!(InvalidRequest, "invalid planned S3 authentication type"));
        }
        if self.copy_source.as_ref().and_then(Option::as_ref).is_some_and(|source| {
            self.action != "s3:PutObject"
                || source.bucket.is_empty()
                || source.bucket.len() > 255
                || source.bucket.contains(['/', '\0', '*', '?'])
                || source.object.is_empty()
                || source.object.len() > MAX_OBJECT_SIZE
                || source.object.contains('\0')
                || source
                    .version_id
                    .as_ref()
                    .is_some_and(|version| version.is_empty() || version.len() > 1024 || version.contains('\0'))
        }) {
            return Err(s3_error!(InvalidRequest, "invalid copy source facts"));
        }
        let retention = self.retain_until_date.is_some() || self.retention_mode.is_some();
        if retention && (self.action != "s3:PutObjectRetention" || self.retention_mode.is_none()) {
            return Err(s3_error!(InvalidRequest, "retention facts require PutObjectRetention and a mode"));
        }
        if self
            .retention_mode
            .as_deref()
            .is_some_and(|mode| !matches!(mode, "GOVERNANCE" | "COMPLIANCE"))
        {
            return Err(s3_error!(InvalidRequest, "invalid retention mode"));
        }
        if let Some(date) = &self.retain_until_date {
            OffsetDateTime::parse(date, &time::format_description::well_known::Rfc3339)
                .map_err(|_| s3_error!(InvalidRequest, "invalid retention date"))?;
        }
        if self
            .legal_hold_status
            .as_deref()
            .is_some_and(|status| self.action != "s3:PutObjectLegalHold" || !matches!(status, "ON" | "OFF"))
        {
            return Err(s3_error!(InvalidRequest, "invalid legal hold facts"));
        }
        if self.request_tags.as_ref().is_some_and(|tags| {
            !matches!(
                self.action.as_str(),
                "s3:PutObject" | "s3:PutObjectTagging" | "s3:PutObjectVersionTagging"
            ) || tags.len() > 10
                || tags.iter().any(|(key, value)| {
                    key.is_empty()
                        || key.chars().count() > 128
                        || value.chars().count() > 256
                        || key.contains('\0')
                        || value.contains('\0')
                })
        }) {
            return Err(s3_error!(InvalidRequest, "invalid request tagging facts"));
        }
        Ok(())
    }

    fn install_operation_facts(&self, probe: &mut RequestEnvelope) -> S3Result<()> {
        use crate::storage::access::PermissionProbeRequestFact;
        for (present, fact) in [
            (self.copy_source.is_some(), PermissionProbeRequestFact::CopySource),
            (self.if_match.is_some(), PermissionProbeRequestFact::IfMatch),
            (self.if_none_match.is_some(), PermissionProbeRequestFact::IfNoneMatch),
        ] {
            if present {
                probe.mark_permission_probe_request_fact_known(fact);
            }
        }
        if let Some(operation) = &self.operation {
            probe.set_permission_probe_operation(operation);
        }

        for (name, value) in [
            (http::header::IF_MATCH, &self.if_match),
            (http::header::IF_NONE_MATCH, &self.if_none_match),
        ] {
            if let Some(value) = value.as_ref().and_then(Option::as_ref) {
                let value =
                    HeaderValue::from_str(value).map_err(|_| s3_error!(InvalidRequest, "invalid conditional S3 header"))?;
                probe.headers_mut().insert(name, value);
            }
        }
        if let Some(auth_type) = &self.auth_type {
            let auth_type = match auth_type.as_str() {
                "REST-HEADER" => crate::auth::facts::AuthTypeCondition::RestHeader,
                "REST-QUERY-STRING" => crate::auth::facts::AuthTypeCondition::RestQueryString,
                _ => return Err(s3_error!(InvalidRequest, "invalid planned S3 authentication type")),
            };
            probe.set_permission_probe_auth_type(auth_type);
        }
        if let Some(source) = self.copy_source.as_ref().and_then(Option::as_ref) {
            probe.install_requested_copy_source(RequestedCopySource::from_object(
                &source.bucket,
                &source.object,
                source.version_id.as_deref(),
            ));
        }
        if self.retention_mode.is_some() {
            let retain_until = self
                .retain_until_date
                .as_deref()
                .map(|date| OffsetDateTime::parse(date, &time::format_description::well_known::Rfc3339))
                .transpose()
                .map_err(|_| s3_error!(InvalidRequest, "invalid retention date"))?;
            probe.install_requested_retention(RequestedObjectLockRetention {
                mode: self.retention_mode.clone(),
                retain_until,
            });
            probe.install_requested_retain_until(retain_until.map(RequestedObjectLockRetainUntil));
        }
        if let Some(status) = &self.legal_hold_status {
            probe.install_requested_legal_hold(RequestedObjectLegalHold(Some(status.clone())));
        }
        if let Some(tags) = &self.request_tags {
            probe.install_requested_object_tags(RequestedObjectTags(
                tags.iter().map(|(key, value)| (key.clone(), value.clone())).collect(),
            ));
        }
        Ok(())
    }
}

fn has_value(value: Option<&str>) -> bool {
    value.is_some_and(|value| !value.is_empty())
}

fn s3_object_target(action: S3Action) -> bool {
    matches!(
        action,
        S3Action::AbortMultipartUploadAction
            | S3Action::ForceDeleteObjectAction
            | S3Action::DeleteObjectAction
            | S3Action::GetObjectAction
            | S3Action::GetObjectAclAction
            | S3Action::GetObjectAttributesAction
            | S3Action::ListMultipartUploadPartsAction
            | S3Action::PutObjectAction
            | S3Action::PutObjectAclAction
            | S3Action::DeleteObjectVersionAction
            | S3Action::DeleteObjectVersionTaggingAction
            | S3Action::GetObjectVersionAction
            | S3Action::GetObjectVersionAttributesAction
            | S3Action::GetObjectVersionTaggingAction
            | S3Action::PutObjectVersionTaggingAction
            | S3Action::BypassGovernanceRetentionAction
            | S3Action::PutObjectRetentionAction
            | S3Action::GetObjectRetentionAction
            | S3Action::GetObjectLegalHoldAction
            | S3Action::PutObjectLegalHoldAction
            | S3Action::GetObjectTaggingAction
            | S3Action::PutObjectTaggingAction
            | S3Action::DeleteObjectTaggingAction
            | S3Action::ReplicateObjectAction
            | S3Action::ReplicateDeleteAction
            | S3Action::ReplicateTagsAction
            | S3Action::GetObjectVersionForReplicationAction
            | S3Action::RestoreObjectAction
            | S3Action::PutObjectFanOutAction
    )
}

fn canonical_object(check: &PermissionCheck, action: Action) -> Option<&str> {
    match action {
        Action::S3Action(action) if !s3_object_target(action) => None,
        _ => check.object.as_deref(),
    }
}

fn missing_target(check: &PermissionCheck, action: Action) -> bool {
    match action {
        Action::S3Action(s3_action) => {
            if s3_action == S3Action::ListAllMyBucketsAction {
                return false;
            }
            if !has_value(check.bucket.as_deref()) {
                return true;
            }
            s3_object_target(s3_action) && !has_value(check.object.as_deref())
                || matches!(
                    s3_action,
                    S3Action::GetObjectVersionAction
                        | S3Action::GetObjectVersionAttributesAction
                        | S3Action::DeleteObjectVersionAction
                        | S3Action::GetObjectVersionTaggingAction
                        | S3Action::PutObjectVersionTaggingAction
                        | S3Action::DeleteObjectVersionTaggingAction
                ) && !has_value(check.version_id.as_deref())
        }
        Action::KmsAction(_) => {
            !matches!(
                check.action.as_str(),
                "kms:Configure"
                    | "kms:ServiceControl"
                    | "kms:ClearCache"
                    | "kms:ListKeys"
                    | "kms:Backup"
                    | "kms:Restore"
                    | "kms:Rekey"
            ) && !has_value(check.key_id.as_deref())
        }
        Action::AdminAction(_) => {
            let name = check.action.trim_start_matches("admin:");
            let bucket_action = matches!(name, "SetTableBucket" | "GetTableBucket");
            let catalog_action = name.contains("Namespace")
                || name.ends_with("Table")
                || name.contains("TableMetadata")
                || name.contains("TableLifecycle")
                || name.contains("TableCredentials")
                || name.contains("TableLocation")
                || name.contains("TableMaintenance");
            (bucket_action || catalog_action) && !has_value(check.bucket.as_deref())
                || catalog_action
                    && if matches!(name, "GetTableNamespace" | "SetTableNamespace") {
                        check.object.is_none()
                    } else {
                        !has_value(canonical_object(check, action))
                    }
        }
        _ => true,
    }
}

fn probe_method(action: &str) -> Method {
    let name = action.split_once(':').map_or(action, |(_, name)| name);
    if name.starts_with("Get") || name.starts_with("List") || name.starts_with("Describe") {
        Method::GET
    } else if name.starts_with("Delete") || name.starts_with("Remove") {
        Method::DELETE
    } else {
        Method::PUT
    }
}

fn probe_uri(check: &PermissionCheck) -> S3Result<Uri> {
    let list_action = matches!(check.action.as_str(), "s3:ListBucket" | "s3:ListBucketVersions");
    let path = if list_action {
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("prefix", check.prefix.as_deref().unwrap_or_default())
            .finish();
        format!("/?{query}")
    } else {
        "/".to_owned()
    };
    path.parse()
        .map_err(|_| s3_error!(InvalidRequest, "invalid permission query"))
}

fn classify(result: S3Result<()>) -> Decision {
    match result {
        Ok(()) => Decision::Allowed,
        Err(error) if error.code() == &S3ErrorCode::AccessDenied => Decision::Denied,
        Err(_) => Decision::Unknown,
    }
}

async fn evaluate_check(
    envelope: &RequestEnvelope,
    cred: &Credentials,
    owner: bool,
    check: &PermissionCheck,
    action: Action,
) -> S3Result<PermissionResult> {
    let bucket = if action == Action::S3Action(S3Action::ListAllMyBucketsAction) {
        None
    } else {
        check.bucket.as_deref()
    };
    let object = canonical_object(check, action);
    let version_id = match action {
        Action::S3Action(action) if s3_object_target(action) => check.version_id.as_deref(),
        _ => None,
    };
    let info = ReqInfo {
        cred: Some(cred.clone()),
        is_owner: owner,
        bucket: bucket.map(str::to_owned),
        object: object.map(str::to_owned),
        version_id: version_id.map(str::to_owned),
        suppress_denial_log: true,
        suppress_denial_audit: true,
        ..Default::default()
    };
    let mut probe = envelope.fork_for_permission_probe(probe_method(&check.action), probe_uri(check)?, info);
    check.install_operation_facts(&mut probe)?;
    let missing = missing_target(check, action);
    let (decision, potential) = if matches!(action, Action::S3Action(_)) {
        if missing {
            (Decision::Unknown, Some(s3_action_potential(&probe, action).await.unwrap_or(false)))
        } else if check.action == "s3:ListAllMyBuckets" {
            (classify(query_list_buckets_permission(&probe).await.map_err(Into::into)), None)
        } else {
            let decision = classify(
                authorize_request_core(&mut probe, action, None, false)
                    .await
                    .map_err(Into::into),
            );
            let potential = if decision == Decision::Unknown {
                Some(s3_action_potential(&probe, action).await.unwrap_or(false))
            } else {
                None
            };
            (decision, potential)
        }
    } else {
        let iam = match envelope.server_context() {
            Some(context) => context
                .installed_app_context()
                .filter(|context| context.iam().is_ready())
                .map(|context| context.iam().handle()),
            None => current_ready_iam_handle().ok(),
        };
        let Some(iam) = iam else {
            return Ok(PermissionResult {
                id: check.id.clone(),
                decision: Decision::Unknown,
                potential: None,
            });
        };
        let conditions = get_condition_values(probe.headers(), cred, None, None, envelope.policy_source_addr());
        let bucket = bucket.unwrap_or_default();
        let object = check.key_id.as_deref().or(object).unwrap_or_default();
        let args = Args {
            account: &cred.access_key,
            groups: &cred.groups,
            action,
            bucket,
            object,
            conditions: &conditions,
            is_owner: owner,
            claims: cred.claims_or_empty(),
            deny_only: false,
        };
        let prepared = iam.prepare_auth(&args).await;
        if missing {
            (Decision::Unknown, Some(prepared.action_potential(&args).await))
        } else if prepared.has_unknown_probe_conditions(&args).await {
            (Decision::Unknown, None)
        } else {
            let scope = AdminResourceScope::bucket_object(bucket, object);
            (
                classify(
                    validate_admin_action_with_bucket_object_for_iam(
                        iam,
                        probe.headers(),
                        cred,
                        owner,
                        action,
                        envelope.policy_source_addr(),
                        scope,
                    )
                    .await,
                ),
                None,
            )
        }
    };
    Ok(PermissionResult {
        id: check.id.clone(),
        decision,
        potential,
    })
}

#[async_trait::async_trait]
impl Operation for AccountPermissionsHandler {
    async fn call(&self, mut req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        // Every authenticated caller may inspect their own permissions, including callers whose
        // grants live only in bucket policies. This route requires no admin or ServerInfo action.
        let credentials = req
            .credentials()
            .ok_or_else(|| s3_error!(AccessDenied, "authentication required"))?;
        let (cred, owner) = authenticate_envelope_credentials(credentials).await?;
        let body = req
            .take_body()
            .store_all_limited(MAX_BODY_SIZE)
            .await
            .map_err(|_| s3_error!(InvalidRequest, "invalid permission request body"))?;
        let request: PermissionsRequest =
            serde_json::from_slice(&body).map_err(|_| s3_error!(InvalidRequest, "invalid permission request"))?;
        let actions = request.validate()?;
        let mut results = Vec::with_capacity(request.checks.len());
        for (check, action) in request.checks.iter().zip(actions) {
            results.push(evaluate_check(req.envelope(), &cred, owner, check, action).await?);
        }
        let body = serde_json::to_vec(&PermissionsResponse { results })
            .map_err(|_| s3_error!(InternalError, "failed to serialize permissions"))?;
        let mut headers = HeaderMap::new();
        headers.insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(http::header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        Ok(AdminResponse::with_headers(StatusCode::OK, Body::from(body), headers))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::runtime_sources::{AppContext, IamInterface, ServerContextSlot};
    use crate::admin::storage_api::contract::bucket::{BucketOperations, MakeBucketOptions};
    use crate::admin::storage_api::contract::object::ObjectOperations as _;
    use crate::admin::storage_api::runtime::ECStore;
    use crate::app::{RequestCredentials, RequestHead};
    use crate::storage::access::AuthorizationRequest;
    use rustfs_iam::store::object::ObjectStore;
    use rustfs_iam::store::{Store, UserType};
    use rustfs_iam::sys::IamSys;
    use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
    use std::sync::Arc;

    struct ReadyIam(Arc<IamSys<ObjectStore>>);
    impl IamInterface for ReadyIam {
        fn handle(&self) -> Arc<IamSys<ObjectStore>> {
            self.0.clone()
        }
        fn is_ready(&self) -> bool {
            true
        }
    }

    async fn fixture() -> (tempfile::TempDir, Arc<ECStore>, Arc<IamSys<ObjectStore>>, RequestEnvelope, Credentials) {
        let (directory, _paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
        ObjectStore::new(store.clone())
            .save_iam_config(
                serde_json::json!({"version":1}),
                format!("{}/format.json", *rustfs_iam::store::object::IAM_CONFIG_PREFIX),
            )
            .await
            .expect("seed isolated IAM format");
        let iam = rustfs_iam::build_iam_sys(store.clone())
            .await
            .expect("initialize isolated IAM");
        iam.create_user(
            "permission-user",
            &AddOrUpdateUserReq {
                secret_key: "permission-user-fixture-secret".into(),
                policy: None,
                status: AccountStatus::Enabled,
            },
        )
        .await
        .expect("create ordinary permission caller");
        let cred = iam.get_user("permission-user").await.expect("resolve caller").credentials;
        let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
        let slot = ServerContextSlot::new();
        assert!(slot.install(Arc::new(AppContext::new(store.clone(), Arc::new(ReadyIam(iam.clone())), ambient.kms()))));
        let mut extensions = http::Extensions::new();
        extensions.insert(slot);
        let envelope = RequestEnvelope::from_parts(
            RequestHead {
                method: Method::POST,
                uri: "/rustfs/admin/v4/account/permissions".parse().expect("query URI"),
                headers: HeaderMap::new(),
                credentials: Some(RequestCredentials::without_secret_key(cred.access_key.clone(), None)),
                region: None,
                service: None,
            },
            extensions,
        );
        (directory, store, iam, envelope, cred)
    }

    async fn bucket_policy(store: &ECStore, bucket: &str, value: serde_json::Value) {
        store
            .update_bucket_metadata_config(
                bucket,
                crate::admin::storage_api::bucket::metadata::BUCKET_POLICY_CONFIG,
                serde_json::to_vec(&value).expect("serialize bucket policy"),
            )
            .await
            .expect("publish isolated bucket policy");
    }

    #[test]
    #[serial_test::serial]
    fn permission_queries_use_bucket_grants_and_never_execute_writes() {
        crate::app::gating_test_env::run_large_stack_test("permission-query-bucket", || async {
            let (_directory, store, _iam, envelope, mut cred) = fixture().await;
            let bucket = format!("permission-query-{}", uuid::Uuid::new_v4().simple());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create query bucket");
            cred.claims = Some(std::collections::HashMap::from([(
                "preferred_username".into(),
                serde_json::json!("alice"),
            )]));
            bucket_policy(
                &store,
                &bucket,
                serde_json::json!({"Version":"2012-10-17", "Statement":[
                    {"Effect":"Allow","Principal":"*","Action":["s3:PutObject","s3:ListBucket"],
                     "Resource":[format!("arn:aws:s3:::{bucket}"),format!("arn:aws:s3:::{bucket}/*")],
                     "Condition":{"StringEquals":{"jwt:preferred_username":"alice"}}},
                    {"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":format!("arn:aws:s3:::{bucket}/blocked")}
                ]}),
            )
            .await;
            let request = parse(serde_json::json!({"checks":[
                {"id":"exact","action":"s3:PutObject","bucket":bucket,"object":"object"},
                {"id":"entry","action":"s3:PutObject","bucket":bucket},
                {"id":"blocked","action":"s3:PutObject","bucket":bucket,"object":"blocked"},
                {"id":"list","action":"s3:ListBucket","bucket":bucket},
                {"id":"discover","action":"s3:ListAllMyBuckets"}
            ]}));
            let actions = request.validate().expect("valid exact checks");
            let mut results = Vec::new();
            for (check, action) in request.checks.iter().zip(actions) {
                results.push(
                    evaluate_check(&envelope, &cred, false, check, action)
                        .await
                        .expect("read-only query"),
                );
            }
            assert_eq!(
                results[0].decision,
                Decision::Allowed,
                "bucket grant supplements an absent identity grant"
            );
            assert_eq!((results[1].decision, results[1].potential), (Decision::Unknown, Some(true)));
            assert_eq!(results[2].decision, Decision::Denied, "an exact bucket denial still wins");
            assert_eq!(
                results[3].decision,
                Decision::Allowed,
                "known bucket is accessible through its bucket policy"
            );
            assert_eq!(
                results[4].decision,
                Decision::Denied,
                "bucket policy does not disclose bucket names through discovery"
            );
            let check = &request.checks[0];
            let mut probe = envelope.fork_for_permission_probe(
                Method::PUT,
                "/".parse().expect("probe URI"),
                ReqInfo {
                    cred: Some(cred.clone()),
                    bucket: Some(bucket.clone()),
                    object: Some("object".into()),
                    suppress_denial_log: true,
                    suppress_denial_audit: true,
                    ..Default::default()
                },
            );
            authorize_request_core(&mut probe, Action::try_from(check.action.as_str()).expect("write action"), None, false)
                .await
                .expect("policy-only query succeeds");
            assert!(
                probe.publication_guards().is_none(),
                "permission query must not acquire publication locks"
            );
            assert!(
                store.get_object_info(&bucket, "object", &Default::default()).await.is_err(),
                "permission query must not write an object"
            );
            cred.claims
                .as_mut()
                .expect("fixture claims")
                .insert("preferred_username".into(), serde_json::json!("bob"));
            let result = evaluate_check(
                &envelope,
                &cred,
                false,
                check,
                Action::try_from(check.action.as_str()).expect("write action"),
            )
            .await
            .expect("identity-conditioned query");
            assert_eq!(
                result.decision,
                Decision::Denied,
                "verified identity conditions are evaluated by the backend"
            );
            bucket_policy(
                &store,
                &bucket,
                serde_json::json!({"Version":"2012-10-17","Statement":[
                    {"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{bucket}/*")}
                ]}),
            )
            .await;
            let entry = &request.checks[1];
            let readonly = evaluate_check(
                &envelope,
                &cred,
                false,
                entry,
                Action::try_from(entry.action.as_str()).expect("write action"),
            )
            .await
            .expect("readonly entry query");
            assert_eq!(
                (readonly.decision, readonly.potential),
                (Decision::Unknown, Some(false)),
                "readonly callers have no upload entry hint"
            );
        });
    }

    #[test]
    #[serial_test::serial]
    fn permission_queries_preserve_table_and_kms_resource_scopes() {
        crate::app::gating_test_env::run_large_stack_test("permission-query-scopes", || async {
            let (_directory, _store, iam, envelope, cred) = fixture().await;
            let policy = rustfs_policy::policy::Policy::parse_config(
                br#"{"Version":"2012-10-17","Statement":[
                {"Effect":"Allow","Action":"admin:DeleteTable","Resource":"arn:aws:s3:::warehouse/namespaces/ns/tables/one"},
                {"Effect":"Allow","Action":["admin:GetTableNamespace","admin:SetTableNamespace"],"Resource":"arn:aws:s3:::warehouse"},
                {"Effect":"Allow","Action":"admin:CreateTable","Resource":"arn:aws:s3:::warehouse/namespaces/ns"},
                {"Effect":"Allow","Action":"admin:CreateTable","Resource":["arn:aws:s3:::warehouse/namespaces/sales/tables/orders","arn:aws:s3:::warehouse/namespaces/sales/views/orders"]},
                {"Effect":"Allow","Action":"admin:ClaimTableLocation","Resource":"arn:aws:s3:::warehouse/data/custom"},
                {"Effect":"Allow","Action":"kms:DisableKey","Resource":"arn:aws:kms:::key/one"}
            ]}"#,
            )
            .expect("scoped policy");
            iam.set_policy("scoped-permission-fixture", policy)
                .await
                .expect("store scoped policy");
            iam.policy_db_set(&cred.access_key, UserType::Reg, false, "scoped-permission-fixture")
                .await
                .expect("attach scoped policy");
            let request = parse(serde_json::json!({"checks":[
                {"id":"table-one","action":"admin:DeleteTable","bucket":"warehouse","object":"namespaces/ns/tables/one"},
                {"id":"table-two","action":"admin:DeleteTable","bucket":"warehouse","object":"namespaces/ns/tables/two"},
                {"id":"key-one","action":"kms:DisableKey","keyId":"one"},
                {"id":"key-two","action":"kms:DisableKey","keyId":"two"},
                {"id":"namespace-root-read","action":"admin:GetTableNamespace","bucket":"warehouse","object":""},
                {"id":"namespace-root-create","action":"admin:SetTableNamespace","bucket":"warehouse","object":""},
                {"id":"namespace-missing-read","action":"admin:GetTableNamespace","bucket":"warehouse"},
                {"id":"namespace-missing-create","action":"admin:SetTableNamespace","bucket":"warehouse"},
                {"id":"namespace-target-read","action":"admin:GetTableNamespace","bucket":"warehouse","object":"namespaces/ns"},
                {"id":"namespace-exact-resource","action":"admin:SetTableNamespace","bucket":"warehouse","object":"namespaces/new"},
                {"id":"table-create-parent","action":"admin:CreateTable","bucket":"warehouse","object":"namespaces/ns"},
                {"id":"table-create-longer-parent","action":"admin:CreateTable","bucket":"warehouse","object":"namespaces/ns/tables/new"},
                {"id":"view-create-longer-parent","action":"admin:CreateTable","bucket":"warehouse","object":"namespaces/ns/views/new"},
                {"id":"tables-segment-in-parent","action":"admin:CreateTable","bucket":"warehouse","object":"namespaces/sales/tables/orders"},
                {"id":"views-segment-in-parent","action":"admin:CreateTable","bucket":"warehouse","object":"namespaces/sales/views/orders"},
                {"id":"table-create-other-parent","action":"admin:CreateTable","bucket":"warehouse","object":"namespaces/other/tables/new"},
                {"id":"location-allowed","action":"admin:ClaimTableLocation","bucket":"warehouse","object":"data/custom"},
                {"id":"location-denied","action":"admin:ClaimTableLocation","bucket":"warehouse","object":"data/other"}
            ]}));
            let expected = [
                Decision::Allowed,
                Decision::Denied,
                Decision::Allowed,
                Decision::Denied,
                Decision::Allowed,
                Decision::Allowed,
                Decision::Unknown,
                Decision::Unknown,
                Decision::Denied,
                Decision::Denied,
                Decision::Allowed,
                Decision::Denied,
                Decision::Denied,
                Decision::Allowed,
                Decision::Allowed,
                Decision::Denied,
                Decision::Allowed,
                Decision::Denied,
            ];
            assert_eq!(expected.len(), request.checks.len(), "every catalog resource scenario must run");
            for ((check, action), expected) in request
                .checks
                .iter()
                .zip(request.validate().expect("valid scoped checks"))
                .zip(expected)
            {
                let result = evaluate_check(&envelope, &cred, false, check, action)
                    .await
                    .expect("scoped query");
                assert_eq!(result.decision, expected, "{}", check.id);
            }
        });
    }

    #[test]
    #[serial_test::serial]
    fn permission_queries_use_action_specific_s3_targets() {
        crate::app::gating_test_env::run_large_stack_test("permission-query-targets", || async {
            let (_directory, store, iam, envelope, cred) = fixture().await;
            let bucket = format!("permission-targets-{}", uuid::Uuid::new_v4().simple());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create target bucket");
            let policy = rustfs_policy::policy::Policy::parse_config(
                serde_json::to_vec(&serde_json::json!({"Version":"2012-10-17","Statement":[
                    {"Effect":"Allow","Action":["s3:GetBucketLocation","s3:BypassGovernanceRetention","s3:ReplicateDelete","s3:ReplicateTags","s3:GetObjectVersionAttributes"],
                     "Resource":[format!("arn:aws:s3:::{bucket}"),format!("arn:aws:s3:::{bucket}/*")]}
                ]})).expect("target policy JSON").as_slice(),
            ).expect("target policy");
            iam.set_policy("permission-targets", policy)
                .await
                .expect("store target policy");
            iam.policy_db_set(&cred.access_key, UserType::Reg, false, "permission-targets")
                .await
                .expect("attach target policy");
            bucket_policy(
                &store,
                &bucket,
                serde_json::json!({"Version":"2012-10-17","Statement":[
                    {"Effect":"Deny","Principal":"*","Action":"s3:GetBucketLocation","Resource":format!("arn:aws:s3:::{bucket}")}
                ]}),
            )
            .await;
            for action in ["s3:BypassGovernanceRetention", "s3:ReplicateDelete", "s3:ReplicateTags"] {
                for (object, expected) in [(None, Decision::Unknown), (Some("known"), Decision::Allowed)] {
                    let mut value = serde_json::json!({"id":"object-target","action":action,"bucket":bucket});
                    if let Some(object) = object {
                        value["object"] = object.into();
                    }
                    let request = parse(serde_json::json!({"checks":[value]}));
                    let result =
                        evaluate_check(&envelope, &cred, false, &request.checks[0], request.validate().expect("object check")[0])
                            .await
                            .expect("object query");
                    assert_eq!(result.decision, expected, "{action} {object:?}");
                    if expected == Decision::Unknown {
                        assert_eq!(result.potential, Some(true));
                    }
                }
            }
            for (extra_object, extra_version) in [(None, None), (Some("escape"), Some("unrelated"))] {
                let mut value = serde_json::json!({"id":"bucket-target","action":"s3:GetBucketLocation","bucket":bucket});
                if let Some(object) = extra_object {
                    value["object"] = object.into();
                }
                if let Some(version) = extra_version {
                    value["versionId"] = version.into();
                }
                let request = parse(serde_json::json!({"checks":[value]}));
                let result =
                    evaluate_check(&envelope, &cred, false, &request.checks[0], request.validate().expect("bucket check")[0])
                        .await
                        .expect("bucket query");
                assert_eq!(result.decision, Decision::Denied, "unused fields must not bypass a bucket deny");
            }
            for (version, expected) in [(None, Decision::Unknown), (Some("known-version"), Decision::Allowed)] {
                let mut value = serde_json::json!({"id":"version-target","action":"s3:GetObjectVersionAttributes","bucket":bucket,"object":"known"});
                if let Some(version) = version {
                    value["versionId"] = version.into();
                }
                let request = parse(serde_json::json!({"checks":[value]}));
                let result =
                    evaluate_check(&envelope, &cred, false, &request.checks[0], request.validate().expect("version check")[0])
                        .await
                        .expect("version query");
                assert_eq!(result.decision, expected, "{version:?}");
            }
        });
    }

    #[test]
    #[serial_test::serial]
    fn permission_queries_evaluate_typed_retention_legal_hold_and_tagging_facts() {
        crate::app::gating_test_env::run_large_stack_test("permission-query-operation-facts", || async {
            let (_directory, store, _iam, envelope, cred) = fixture().await;
            let bucket = format!("permission-facts-{}", uuid::Uuid::new_v4().simple());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create facts bucket");
            bucket_policy(&store, &bucket, serde_json::json!({"Version":"2012-10-17", "Statement":[
                {"Effect":"Allow","Principal":"*","Action":"s3:PutObjectRetention","Resource":format!("arn:aws:s3:::{bucket}/*"),
                 "Condition":{"NumericLessThanEquals":{"s3:object-lock-remaining-retention-days":"30"}}},
                {"Effect":"Allow","Principal":"*","Action":"s3:PutObjectRetention","Resource":format!("arn:aws:s3:::{bucket}/*"),
                 "Condition":{"Null":{"s3:object-lock-retain-until-date":"true"}}},
                {"Effect":"Allow","Principal":"*","Action":"s3:PutObjectLegalHold","Resource":format!("arn:aws:s3:::{bucket}/*"),
                 "Condition":{"StringEquals":{"s3:object-lock-legal-hold":"ON"}}},
                {"Effect":"Allow","Principal":"*","Action":"s3:PutObjectTagging","Resource":format!("arn:aws:s3:::{bucket}/*"),
                 "Condition":{"StringEquals":{"s3:RequestObjectTag/env":"prod"}}},
                {"Effect":"Allow","Principal":"*","Action":"s3:PutObjectTagging","Resource":format!("arn:aws:s3:::{bucket}/*"),
                 "Condition":{"Null":{"s3:RequestObjectTag/env":"true"}}},
                {"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":format!("arn:aws:s3:::{bucket}/*"),
                 "Condition":{"Null":{"s3:RequestObjectTag/foo":"true"}}},
                {"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{bucket}/*"),
                 "Condition":{"StringEquals":{"s3:authType":"REST-QUERY-STRING"}}},
                {"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":format!("arn:aws:s3:::{bucket}/*"),
                 "Condition":{"StringEquals":{"s3:x-amz-copy-source":"source/document"}}}
            ]})).await;
            let now = OffsetDateTime::now_utc();
            let date = |days| {
                (now + time::Duration::days(days))
                    .format(&time::format_description::well_known::Rfc3339)
                    .expect("date")
            };
            let request = parse(serde_json::json!({"checks":[
                {"id":"30days","action":"s3:PutObjectRetention","bucket":bucket,"object":"object","retainUntilDate":date(30),"retentionMode":"GOVERNANCE"},
                {"id":"31days","action":"s3:PutObjectRetention","bucket":bucket,"object":"object","retainUntilDate":date(31),"retentionMode":"GOVERNANCE"},
                {"id":"reset","action":"s3:PutObjectRetention","bucket":bucket,"object":"object","retentionMode":"GOVERNANCE"},
                {"id":"unknown-retention","action":"s3:PutObjectRetention","bucket":bucket,"object":"object"},
                {"id":"on","action":"s3:PutObjectLegalHold","bucket":bucket,"object":"object","legalHoldStatus":"ON"},
                {"id":"off","action":"s3:PutObjectLegalHold","bucket":bucket,"object":"object","legalHoldStatus":"OFF"},
                {"id":"prod","action":"s3:PutObjectTagging","bucket":bucket,"object":"object","requestTags":{"env":"prod"}},
                {"id":"dev","action":"s3:PutObjectTagging","bucket":bucket,"object":"object","requestTags":{"env":"dev"}},
                {"id":"clear-tags","action":"s3:PutObjectTagging","bucket":bucket,"object":"object","requestTags":{}},
                {"id":"unknown-tags","action":"s3:PutObjectTagging","bucket":bucket,"object":"object"},
                {"id":"untagged-upload","action":"s3:PutObject","bucket":bucket,"object":"object","requestTags":{},"copySource":null},
                {"id":"tagged-upload","action":"s3:PutObject","bucket":bucket,"object":"object","requestTags":{"foo":"present"},"copySource":null},
                {"id":"unknown-copy-source","action":"s3:PutObject","bucket":bucket,"object":"object","requestTags":{"foo":"present"}},
                {"id":"presigned-read","action":"s3:GetObject","bucket":bucket,"object":"object","authType":"REST-QUERY-STRING"},
                {"id":"header-read","action":"s3:GetObject","bucket":bucket,"object":"object","authType":"REST-HEADER"},
                {"id":"allowed-copy","action":"s3:PutObject","bucket":bucket,"object":"object","copySource":{"bucket":"source","object":"document"},"requestTags":{"foo":"present"}},
                {"id":"mismatched-copy","action":"s3:PutObject","bucket":bucket,"object":"object","copySource":{"bucket":"source","object":"different"},"requestTags":{"foo":"present"}}
            ]}));
            let expected = [
                Decision::Allowed,
                Decision::Denied,
                Decision::Allowed,
                Decision::Unknown,
                Decision::Allowed,
                Decision::Denied,
                Decision::Allowed,
                Decision::Denied,
                Decision::Allowed,
                Decision::Unknown,
                Decision::Allowed,
                Decision::Denied,
                Decision::Unknown,
                Decision::Allowed,
                Decision::Denied,
                Decision::Allowed,
                Decision::Denied,
            ];
            assert_eq!(expected.len(), request.checks.len(), "every typed-fact scenario must run");
            for ((check, action), expected) in request
                .checks
                .iter()
                .zip(request.validate().expect("valid typed checks"))
                .zip(expected)
            {
                let result = evaluate_check(&envelope, &cred, false, check, action)
                    .await
                    .expect("typed operation query");
                assert_eq!(result.decision, expected, "{}", check.id);
                if expected == Decision::Unknown {
                    assert_eq!(result.potential, Some(true), "unknown editing facts retain a possible entry hint");
                }
            }
        });
    }

    #[test]
    fn typed_operation_facts_refuse_wrong_actions_invalid_values_and_identity_spoofing() {
        for check in [
            serde_json::json!({"id":"x","action":"s3:DeleteObject","retainUntilDate":"2030-01-01T00:00:00Z","retentionMode":"GOVERNANCE"}),
            serde_json::json!({"id":"x","action":"s3:PutObjectRetention","retainUntilDate":"invalid","retentionMode":"GOVERNANCE"}),
            serde_json::json!({"id":"x","action":"s3:PutObjectRetention","retentionMode":"OTHER"}),
            serde_json::json!({"id":"x","action":"s3:PutObjectTagging","legalHoldStatus":"ON"}),
            serde_json::json!({"id":"x","action":"s3:PutObjectLegalHold","legalHoldStatus":"MAYBE"}),
            serde_json::json!({"id":"x","action":"s3:DeleteObject","requestTags":{}}),
            serde_json::json!({"id":"x","action":"admin:GetUser","authType":"REST-QUERY-STRING"}),
            serde_json::json!({"id":"x","action":"s3:GetObject","authType":"anonymous"}),
            serde_json::json!({"id":"x","action":"s3:GetObject","copySource":{"bucket":"source","object":"document"}}),
            serde_json::json!({"id":"x","action":"s3:PutObject","copySource":{"bucket":"source","object":""}}),
            serde_json::json!({"id":"x","action":"s3:GetObject","operation":"UploadPart"}),
        ] {
            assert!(parse(serde_json::json!({"checks":[check]})).validate().is_err());
        }
    }

    #[test]
    #[serial_test::serial]
    fn permission_queries_evaluate_declared_conditional_headers() {
        crate::app::gating_test_env::run_large_stack_test("permission-query-conditions", || async {
            let (_directory, store, _iam, envelope, cred) = fixture().await;
            let bucket = format!("permission-etag-{}", uuid::Uuid::new_v4().simple());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create conditional bucket");
            bucket_policy(
                &store,
                &bucket,
                serde_json::json!({"Version":"2012-10-17","Statement":[
                    {"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":format!("arn:aws:s3:::{bucket}/*"),
                     "Condition":{"StringEquals":{"s3:if-none-match":"*"}}},
                    {"Effect":"Allow","Principal":"*","Action":"s3:DeleteObject","Resource":format!("arn:aws:s3:::{bucket}/*"),
                     "Condition":{"StringEquals":{"s3:if-match":"\"etag\""}}}
                ]}),
            )
            .await;
            let request = parse(serde_json::json!({"checks":[
                {"id":"create","action":"s3:PutObject","bucket":bucket,"object":"object","ifNoneMatch":"*"},
                {"id":"overwrite","action":"s3:PutObject","bucket":bucket,"object":"object","ifNoneMatch":null},
                {"id":"delete-current","action":"s3:DeleteObject","bucket":bucket,"object":"object","ifMatch":"\"etag\""},
                {"id":"delete-wrong","action":"s3:DeleteObject","bucket":bucket,"object":"object","ifMatch":"\"different\""},
                {"id":"entry","action":"s3:PutObject","bucket":bucket}
            ]}));
            let expected = [
                Decision::Allowed,
                Decision::Denied,
                Decision::Allowed,
                Decision::Denied,
                Decision::Unknown,
            ];
            assert_eq!(expected.len(), request.checks.len());
            for ((check, action), expected) in request
                .checks
                .iter()
                .zip(request.validate().expect("conditional checks"))
                .zip(expected)
            {
                let result = evaluate_check(&envelope, &cred, false, check, action)
                    .await
                    .expect("conditional query");
                assert_eq!(result.decision, expected, "{}", check.id);
                if expected == Decision::Unknown {
                    assert_eq!(result.potential, Some(true));
                }
            }
            for invalid in [
                serde_json::json!({"id":"x","action":"s3:PutObject","ifMatch":"safe\r\ninjected:true"}),
                serde_json::json!({"id":"x","action":"s3:PutObjectTagging","ifNoneMatch":"*"}),
                serde_json::json!({"id":"x","action":"s3:DeleteObject","ifMatch":""}),
            ] {
                assert!(parse(serde_json::json!({"checks":[invalid]})).validate().is_err());
            }
        });
    }

    fn parse(value: serde_json::Value) -> PermissionsRequest {
        serde_json::from_value(value).expect("permission request must deserialize")
    }

    #[tokio::test]
    async fn permission_handler_refuses_unauthenticated_queries_before_policy_lookup() {
        let envelope = RequestEnvelope::from_parts(
            RequestHead {
                method: Method::POST,
                uri: "/".parse().expect("request URI"),
                headers: HeaderMap::new(),
                credentials: None,
                region: None,
                service: None,
            },
            http::Extensions::new(),
        );
        let request = AdminRequest::new(envelope, Body::from(r#"{"checks":[{"id":"x","action":"s3:PutObject"}]}"#));
        let mut router = matchit::Router::new();
        router.insert("/", ()).expect("parameter fixture route");
        let params = router.at("/").expect("parameter fixture match").params;
        let error = AccountPermissionsHandler
            .call(request, params)
            .await
            .expect_err("unauthenticated query must fail");
        assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
    }

    #[test]
    fn strict_schema_refuses_identity_and_unrecognized_facts() {
        for value in [
            serde_json::json!({"checks":[], "claims":{"preferred_username":"root"}}),
            serde_json::json!({"checks":[{"id":"x","action":"s3:PutObject","username":"root"}]}),
            serde_json::json!({"checks":[{"id":"x","action":"s3:PutObject","conditions":{}}]}),
        ] {
            assert!(serde_json::from_value::<PermissionsRequest>(value).is_err());
        }
    }

    #[test]
    fn validation_refuses_wildcards_duplicates_and_version_action_mismatch() {
        for checks in [
            serde_json::json!([{"id":"x","action":"s3:*"}]),
            serde_json::json!([{"id":"x","action":"s3:GetObject"},{"id":"x","action":"s3:PutObject"}]),
            serde_json::json!([{"id":"x","action":"s3:GetObject","versionId":"v1"}]),
            serde_json::json!([{"id":"x","action":"s3:GetObjectAttributes","versionId":"v1"}]),
        ] {
            assert!(parse(serde_json::json!({"checks":checks})).validate().is_err());
        }
    }

    #[test]
    fn missing_object_is_unknown_while_literal_leading_slashes_are_preserved() {
        let mut request = parse(serde_json::json!({"checks":[{"id":"x","action":"s3:PutObject","bucket":"alice-data"}]}));
        let action = request.validate().expect("concrete action accepted")[0];
        assert!(missing_target(&request.checks[0], action));
        request.checks[0].object = Some("/object".into());
        assert!(!missing_target(&request.checks[0], action));
        assert_eq!(request.checks[0].object.as_deref(), Some("/object"));
    }

    #[test]
    fn nullable_facts_distinguish_unknown_explicit_omission_and_values() {
        let absent = parse(serde_json::json!({"checks":[{"id":"x","action":"s3:PutObject"}]}));
        assert!(absent.checks[0].copy_source.is_none());
        assert!(absent.checks[0].if_match.is_none());
        let omitted = parse(
            serde_json::json!({"checks":[{"id":"x","action":"s3:PutObject","copySource":null,"ifMatch":null,"ifNoneMatch":null}]}),
        );
        assert!(matches!(omitted.checks[0].copy_source, Some(None)));
        assert_eq!(omitted.checks[0].if_match, Some(None));
        let present = parse(serde_json::json!({"checks":[{"id":"x","action":"s3:PutObject","ifNoneMatch":"*"}]}));
        assert_eq!(present.checks[0].if_none_match, Some(Some("*".into())));
    }

    #[test]
    #[serial_test::serial]
    fn operation_shape_matches_real_multipart_publication_facts() {
        crate::app::gating_test_env::run_large_stack_test("permission-query-multipart-shape", || async {
            let (_directory, store, _iam, envelope, cred) = fixture().await;
            let bucket = format!("permission-shape-{}", uuid::Uuid::new_v4().simple());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create shape bucket");
            bucket_policy(
                &store,
                &bucket,
                serde_json::json!({"Version":"2012-10-17","Statement":[
                    {"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":format!("arn:aws:s3:::{bucket}/*"),
                     "Condition":{"Bool":{"s3:ObjectCreationOperation":"true"}}}
                ]}),
            )
            .await;
            for (operation, expected) in [
                ("PutObject", Decision::Allowed),
                ("CopyObject", Decision::Allowed),
                ("CreateMultipartUpload", Decision::Denied),
                ("UploadPart", Decision::Denied),
                ("UploadPartCopy", Decision::Denied),
                ("CompleteMultipartUpload", Decision::Allowed),
            ] {
                let request = parse(
                    serde_json::json!({"checks":[{"id":"x","action":"s3:PutObject","bucket":bucket,"object":"object","operation":operation}]}),
                );
                let result = evaluate_check(
                    &envelope,
                    &cred,
                    false,
                    &request.checks[0],
                    request.validate().expect("operation check")[0],
                )
                .await
                .expect("shape query");
                assert_eq!(result.decision, expected, "{operation}");
            }
        });
    }

    #[test]
    #[serial_test::serial]
    fn regular_iam_denials_follow_real_bucket_fallback_rules() {
        crate::app::gating_test_env::run_large_stack_test("permission-query-denial-fallback", || async {
            let (_directory, store, iam, envelope, cred) = fixture().await;
            let bucket = format!("permission-denial-{}", uuid::Uuid::new_v4().simple());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create denial bucket");
            bucket_policy(&store, &bucket, serde_json::json!({"Version":"2012-10-17","Statement":[
                {"Effect":"Allow","Principal":"*","Action":["s3:PutObject","s3:DeleteObject"],"Resource":format!("arn:aws:s3:::{bucket}/*")}
            ]})).await;
            let denied = rustfs_policy::policy::Policy::parse_config(
                serde_json::to_string(&serde_json::json!({"Version":"2012-10-17","Statement":[
                    {"Effect":"Deny","Action":["s3:PutObject","s3:DeleteObject"],"Resource":format!("arn:aws:s3:::{bucket}/*")}
                ]}))
                .expect("identity denial JSON")
                .as_bytes(),
            )
            .expect("identity denial policy");
            iam.set_policy("permission-denial", denied).await.expect("store denial");
            iam.policy_db_set(&cred.access_key, UserType::Reg, false, "permission-denial")
                .await
                .expect("attach denial");
            for (action, expected) in [("s3:PutObject", Decision::Allowed), ("s3:DeleteObject", Decision::Denied)] {
                let request = parse(serde_json::json!({"checks":[{"id":"x","action":action,"bucket":bucket,"object":"object"}]}));
                let result = evaluate_check(
                    &envelope,
                    &cred,
                    false,
                    &request.checks[0],
                    request.validate().expect("valid denial query")[0],
                )
                .await
                .expect("non-owner query");
                assert_eq!(result.decision, expected, "real gate semantics for {action}");
            }
        });
    }

    #[test]
    fn denied_and_unavailable_results_do_not_expose_internal_errors() {
        assert_eq!(classify(Ok(())), Decision::Allowed);
        assert_eq!(classify(Err(s3_error!(AccessDenied, "private policy"))), Decision::Denied);
        assert_eq!(classify(Err(s3_error!(InternalError, "private backend"))), Decision::Unknown);
        let value = serde_json::to_value(PermissionResult {
            id: "x".into(),
            decision: Decision::Unknown,
            potential: Some(true),
        })
        .expect("result serializes");
        assert_eq!(value, serde_json::json!({"id":"x","decision":"unknown","potential":true}));
    }
}
