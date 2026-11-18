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

//! The condition values a policy decision evaluates, built from one request.
//!
//! Responsible for: the request facts and principal of a decision, the condition map built
//! from them (request object tags, object lock retention, copy source, permission-probe fact
//! markers), the existing-object-tag conditions fetched when a policy needs them
//! ([`ObjectTagConditions`]), and the resource-free condition values of a decision that names
//! no bucket.
//! Not responsible for: evaluating a policy (`decision`), or the request facts themselves
//! (`crate::auth::facts`).
//! Upstream: `decision` and `discovery`. Downstream: `crate::auth::facts`, the bucket policy
//! system, and the object store's tag metadata.

use super::{AuthorizationRequest, request_object_store};
use crate::auth::facts::{AuthzFacts, AuthzPrincipal, AuthzTarget, base_conditions, build_conditions, forward_secret_headers};
use crate::auth::{OBJECT_CREATION_OPERATION_CONDITION, OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION};
use crate::storage::ecfs::FS;
use crate::storage::request_markers::{
    PermissionProbeRequestFact, RequestedCopySource, RequestedObjectLegalHold, RequestedObjectLockRetainUntil,
    RequestedObjectTags,
};
use crate::storage::storage_api::runtime_sources_consumer::runtime_sources;
use crate::storage::{ECStore, StorageError, is_err_bucket_not_found};
use http::HeaderMap;
use metrics::counter;
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_policy::policy::{
    BucketPolicy, BucketPolicyArgs, bucket_policy_needs_existing_object_tag_for_args,
    bucket_policy_uses_existing_object_tag_conditions,
};
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result, s3_error};
use std::collections::HashMap;
use time::OffsetDateTime;

#[derive(Clone, Debug)]
pub(crate) struct ObjectTagConditions {
    pub(crate) bucket: String,
    pub(crate) object: String,
    pub(crate) version_id: Option<String>,
    pub(crate) values: HashMap<String, Vec<String>>,
}

impl ObjectTagConditions {
    pub(crate) fn new(bucket: &str, object: &str, version_id: Option<&str>, values: HashMap<String, Vec<String>>) -> Self {
        Self {
            bucket: bucket.to_string(),
            object: object.to_string(),
            version_id: version_id.map(str::to_string),
            values,
        }
    }

    pub(crate) fn matches(&self, bucket: &str, object: &str, version_id: Option<&str>) -> bool {
        self.bucket == bucket && self.object == object && self.version_id.as_deref() == version_id
    }
}

/// True when the bucket policy may evaluate `s3:ExistingObjectTag` for this request (statement
/// matches principal/action/resource and conditions reference ExistingObjectTag keys).
pub(crate) enum BucketPolicyExistingObjectTagHint {
    NoTagRequirement,
    ConservativeTagRequired,
    Parsed(BucketPolicy),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BucketPolicyRawLoadErrorKind {
    PolicyMissing,
    BucketMissing,
    Other,
}

pub(crate) fn classify_bucket_policy_raw_load_error(err: &StorageError) -> BucketPolicyRawLoadErrorKind {
    if err == &StorageError::ConfigNotFound {
        BucketPolicyRawLoadErrorKind::PolicyMissing
    } else if is_err_bucket_not_found(err) {
        BucketPolicyRawLoadErrorKind::BucketMissing
    } else {
        BucketPolicyRawLoadErrorKind::Other
    }
}

/// Reuse the parsed bucket metadata policy for ExistingObjectTag hint checks.
pub(crate) async fn load_bucket_policy_existing_object_tag_hint(
    store: &ECStore,
    bucket: &str,
    action: Action,
) -> BucketPolicyExistingObjectTagHint {
    let (policy, _) = match store.get_bucket_policy(bucket).await {
        Ok(v) => v,
        Err(err) => match classify_bucket_policy_raw_load_error(&err) {
            BucketPolicyRawLoadErrorKind::PolicyMissing => {
                tracing::debug!(
                    bucket = %bucket,
                    ?action,
                    "bucket policy not configured while checking ExistingObjectTag hint; treating as no tag requirement"
                );
                return BucketPolicyExistingObjectTagHint::NoTagRequirement;
            }
            BucketPolicyRawLoadErrorKind::BucketMissing => {
                tracing::debug!(
                    bucket = %bucket,
                    ?action,
                    error = %err,
                    "bucket missing while checking ExistingObjectTag hint; treating as no tag requirement"
                );
                return BucketPolicyExistingObjectTagHint::NoTagRequirement;
            }
            BucketPolicyRawLoadErrorKind::Other => {
                tracing::warn!(
                    bucket = %bucket,
                    ?action,
                    error = %err,
                    "failed to load bucket policy while checking ExistingObjectTag hint; conservatively enabling tag fetch"
                );
                return BucketPolicyExistingObjectTagHint::ConservativeTagRequired;
            }
        },
    };
    if bucket_policy_uses_existing_object_tag_conditions(&policy) {
        BucketPolicyExistingObjectTagHint::Parsed(policy)
    } else {
        BucketPolicyExistingObjectTagHint::NoTagRequirement
    }
}

pub(crate) async fn bucket_policy_needs_existing_object_tag_from_hint(
    hint: &BucketPolicyExistingObjectTagHint,
    args: &BucketPolicyArgs<'_>,
) -> bool {
    match hint {
        BucketPolicyExistingObjectTagHint::NoTagRequirement => false,
        BucketPolicyExistingObjectTagHint::ConservativeTagRequired => true,
        BucketPolicyExistingObjectTagHint::Parsed(policy) => bucket_policy_needs_existing_object_tag_for_args(policy, args).await,
    }
}

fn merge_object_tag_conditions(conditions: &mut HashMap<String, Vec<String>>, tags: &HashMap<String, Vec<String>>) {
    for (k, v) in tags {
        conditions
            .entry(k.clone())
            .and_modify(|existing| existing.extend(v.iter().cloned()))
            .or_insert_with(|| v.clone());
    }
}

fn action_tag_metric_label(action: &Action) -> &'static str {
    match action {
        Action::S3Action(S3Action::GetObjectAction) => "get_object",
        Action::S3Action(S3Action::GetObjectAttributesAction) => "get_object_attributes",
        Action::S3Action(S3Action::GetObjectVersionAction) => "get_object_version",
        Action::S3Action(S3Action::GetObjectVersionAttributesAction) => "get_object_version_attributes",
        Action::S3Action(S3Action::GetObjectTaggingAction) => "get_object_tagging",
        Action::S3Action(S3Action::DeleteObjectAction) => "delete_object",
        Action::S3Action(S3Action::DeleteObjectVersionAction) => "delete_object_version",
        Action::S3Action(S3Action::DeleteObjectTaggingAction) => "delete_object_tagging",
        Action::S3Action(S3Action::PutObjectTaggingAction) => "put_object_tagging",
        _ => "authorize",
    }
}

pub(crate) fn merge_request_object_tag_conditions(
    action: Action,
    headers: &HeaderMap,
    conditions: &mut HashMap<String, Vec<String>>,
) -> S3Result<()> {
    if action != Action::S3Action(S3Action::PutObjectAction) {
        return Ok(());
    }

    let Some(tagging) = headers.get("x-amz-tagging").and_then(|value| value.to_str().ok()) else {
        return Ok(());
    };

    let mut tag_keys = Vec::new();
    for tag in crate::storage::s3_api::tagging::parse_object_tag_header(tagging)? {
        let Some(key) = tag.key else {
            continue;
        };
        let Some(value) = tag.value else {
            continue;
        };
        tag_keys.push(key.clone());
        conditions.entry(format!("RequestObjectTag/{key}")).or_default().push(value);
    }
    conditions.insert("RequestObjectTagKeys".to_string(), tag_keys);
    Ok(())
}

/// The request facts of `req` for an authorization decision about `target`, read now.
pub(crate) fn request_authz_facts<R: AuthorizationRequest + ?Sized>(req: &R, mut target: AuthzTarget<'_>) -> AuthzFacts {
    if target.action != Action::S3Action(S3Action::CreateBucketAction) {
        target.location_constraint = None;
    }
    // A POST upload applies its form fields, not its headers, so the policy
    // decision must read the values the handler stores.
    AuthzFacts::from_request_parts(
        req.policy_headers(),
        req.uri().query(),
        req.remote_addr(),
        req.client_info(),
        target,
        OffsetDateTime::now_utc(),
    )
    .with_caller_authentication(if req.permission_probe() {
        req.permission_probe_caller_authentication()
    } else {
        None
    })
    .with_planned_auth_type(if req.permission_probe() {
        req.permission_probe_auth_type()
    } else {
        None
    })
}

pub(crate) fn request_principal<'a, R: AuthorizationRequest + ?Sized>(
    req: &R,
    cred: &'a rustfs_credentials::Credentials,
    root: &str,
) -> AuthzPrincipal<'a> {
    AuthzPrincipal::with_session_presence(cred, root, req.req_info().and_then(|info| info.keystone_session_present))
}

/// The root access key a principal is compared against to be the `Account`.
pub(crate) fn root_access_key() -> String {
    runtime_sources::current_action_credentials()
        .map(|root| root.access_key)
        .unwrap_or_default()
}

pub(crate) fn authorization_conditions<R: AuthorizationRequest + ?Sized>(
    req: &R,
    principal: &AuthzPrincipal<'_>,
    facts: &AuthzFacts,
) -> S3Result<HashMap<String, Vec<String>>> {
    authorization_conditions_with_internal(req, principal, facts, req.internal_object_authorization())
}

pub(crate) fn authorization_conditions_with_internal<R: AuthorizationRequest + ?Sized>(
    req: &R,
    principal: &AuthzPrincipal<'_>,
    facts: &AuthzFacts,
    internal_object_authorization: bool,
) -> S3Result<HashMap<String, Vec<String>>> {
    if let Some(key) = facts.duplicate_list_query_key() {
        return Err(s3_error!(InvalidRequest, "duplicate query: {key}"));
    }
    let mut conditions = if internal_object_authorization {
        base_conditions(principal, facts)
    } else {
        build_conditions(principal, facts)
    };
    forward_secret_headers(req.headers(), &mut conditions);
    if internal_object_authorization {
        retain_internal_object_authorization_conditions(&mut conditions, principal.claims());
    }
    if facts.action() == Action::S3Action(S3Action::PutObjectAction) {
        let creates_object = internal_object_authorization || !req.multipart_staging();
        conditions.insert(OBJECT_CREATION_OPERATION_CONDITION.to_owned(), vec![creates_object.to_string()]);
    }
    if internal_object_authorization {
        return Ok(conditions);
    }
    merge_request_object_tag_conditions(facts.action(), req.policy_headers(), &mut conditions)?;
    if let Some(RequestedCopySource(source)) = req.requested_copy_source() {
        conditions.insert("x-amz-copy-source".to_string(), vec![source.clone()]);
    }
    if let Some(RequestedObjectLegalHold(status)) = req.requested_legal_hold() {
        // Standalone legal hold writes apply the XML body, so headers must not
        // supply a different value to the policy decision.
        conditions.remove("object-lock-legal-hold");
        if let Some(status) = status {
            conditions.insert("object-lock-legal-hold".to_string(), vec![status.clone()]);
        }
    }
    if let Some(RequestedObjectTags(tags)) = req.requested_object_tags() {
        // Standalone tag writes apply the XML body; the x-amz-tagging header is
        // not stored and must not describe the request to the policy engine.
        conditions.retain(|key, _| {
            if req.permission_probe() {
                let key = key.to_ascii_lowercase();
                key != "requestobjecttagkeys" && !key.starts_with("requestobjecttag/")
            } else {
                key != "RequestObjectTagKeys" && !key.starts_with("RequestObjectTag/")
            }
        });
        let mut keys = Vec::new();
        for (key, value) in tags {
            keys.push(key.clone());
            conditions
                .entry(format!("RequestObjectTag/{key}"))
                .or_default()
                .push(value.clone());
        }
        conditions.insert("RequestObjectTagKeys".to_string(), keys);
    }
    if let Some(retention) = req.requested_retention() {
        // Standalone retention writes apply the XML body, so headers must not
        // supply a different value to the policy decision.
        conditions.remove("object-lock-mode");
        conditions.remove("object-lock-retain-until-date");
        if let Some(mode) = &retention.mode {
            conditions.insert("object-lock-mode".to_string(), vec![mode.clone()]);
        }
        if let Some(date) = retention.retain_until {
            let date = date
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidArgument, "Invalid retain-until date"))?;
            conditions.insert("object-lock-retain-until-date".to_string(), vec![date]);
        }
    }
    merge_object_lock_remaining_retention_days_condition(
        req.requested_retain_until(),
        OffsetDateTime::now_utc(),
        &mut conditions,
    );
    if req.permission_probe() {
        use rustfs_policy::policy::probe::{
            PROBE_COPY_SOURCE_KNOWN, PROBE_EXISTING_TAGS_RESOLVED, PROBE_IF_MATCH_KNOWN, PROBE_IF_NONE_MATCH_KNOWN,
            PROBE_RETENTION_FACTS_KNOWN,
        };
        conditions.remove(PROBE_EXISTING_TAGS_RESOLVED);
        conditions.remove(PROBE_RETENTION_FACTS_KNOWN);
        for (marker, fact) in [
            (PROBE_COPY_SOURCE_KNOWN, PermissionProbeRequestFact::CopySource),
            (PROBE_IF_MATCH_KNOWN, PermissionProbeRequestFact::IfMatch),
            (PROBE_IF_NONE_MATCH_KNOWN, PermissionProbeRequestFact::IfNoneMatch),
        ] {
            conditions.remove(marker);
            if req.permission_probe_request_fact_known(fact) {
                conditions.insert(marker.to_owned(), vec!["true".to_owned()]);
            }
        }
        if req.requested_copy_source().is_none() {
            conditions.remove("x-amz-copy-source");
        }
        for name in [http::header::IF_MATCH, http::header::IF_NONE_MATCH] {
            if !req.headers().contains_key(&name) {
                conditions.remove(name.as_str());
            }
        }
        if req.requested_object_tags().is_none() {
            conditions.retain(|key, _| {
                let key = key.to_ascii_lowercase();
                key != "requestobjecttagkeys" && !key.starts_with("requestobjecttag/")
            });
        }
        if req.requested_retention().is_some() {
            conditions.insert(PROBE_RETENTION_FACTS_KNOWN.to_owned(), vec!["true".to_owned()]);
        }
    }
    Ok(conditions)
}

fn merge_object_lock_remaining_retention_days_condition(
    requested: Option<&RequestedObjectLockRetainUntil>,
    now: OffsetDateTime,
    conditions: &mut HashMap<String, Vec<String>>,
) {
    if let Some(RequestedObjectLockRetainUntil(retain_until)) = requested {
        conditions.insert(
            OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION.to_string(),
            vec![remaining_retention_days(*retain_until, now).to_string()],
        );
    }
}

/// Whole days from `now` to `retain_until`, rounded up as MinIO does, so a policy
/// limit of N days rejects a date even one second past day N. A date that is not
/// in the future yields 0.
pub(crate) fn remaining_retention_days(retain_until: OffsetDateTime, now: OffsetDateTime) -> i64 {
    let remaining = retain_until - now;
    if remaining <= time::Duration::ZERO {
        return 0;
    }
    let days = remaining.whole_days();
    if remaining > time::Duration::days(days) {
        days + 1
    } else {
        days
    }
}

fn retain_internal_object_authorization_conditions(
    conditions: &mut HashMap<String, Vec<String>>,
    claims: Option<&HashMap<String, serde_json::Value>>,
) {
    conditions.retain(|key, _| {
        matches!(
            key.as_str(),
            "CurrentTime"
                | "EpochTime"
                | "SecureTransport"
                | "SourceIp"
                | "UserAgent"
                | "Referer"
                | "userid"
                | "username"
                | "principaltype"
                | "signatureversion"
                | "signatureAge"
                | "authType"
                | "LocationConstraint"
                | "groups"
                | "roles"
        ) || claims.is_some_and(|claims| {
            claims
                .keys()
                .any(|claim| claim.trim_start_matches("ldap").to_lowercase() == *key)
        })
    });
}

/// Condition values for an authorization decision that is not scoped to a bucket or object.
///
/// Lives here rather than at the call site because this module already owns the request
/// plumbing every such decision needs: the verified client address and `ReqInfo`. The KMS
/// grammar has no bucket/object/tag conditions, so none of the S3 merges apply.
pub(crate) fn resource_free_condition_values<R: AuthorizationRequest + ?Sized>(
    req: &R,
    cred: &rustfs_credentials::Credentials,
) -> HashMap<String, Vec<String>> {
    let root = root_access_key();
    let principal = request_principal(req, cred, &root);
    let target = AuthzTarget {
        action: Action::None,
        bucket: "",
        object: "",
        version_id: None,
        location_constraint: None,
    };
    let facts = AuthzFacts::from_request_parts(
        req.headers(),
        None,
        req.remote_addr(),
        req.client_info(),
        target,
        OffsetDateTime::now_utc(),
    );
    let mut conditions = build_conditions(&principal, &facts);
    forward_secret_headers(req.headers(), &mut conditions);
    conditions
}

pub(crate) async fn get_or_fetch_object_tag_conditions<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    store: &ECStore,
    bucket: &str,
    object: &str,
    version_id: Option<&str>,
    action: Action,
) -> S3Result<HashMap<String, Vec<String>>> {
    if let Some(cached) = req.object_tag_conditions()
        && cached.matches(bucket, object, version_id)
    {
        return Ok(cached.values.clone());
    }

    counter!("rustfs_object_tag_conditions_fetched_total", "op" => action_tag_metric_label(&action)).increment(1);
    let fetched = FS::get_object_tag_conditions_for_policy_from_store(store, bucket, object, version_id).await?;
    req.install_object_tag_conditions(ObjectTagConditions::new(bucket, object, version_id, fetched.clone()));
    Ok(fetched)
}

pub(crate) async fn maybe_merge_object_tag_conditions<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    action: Action,
    bucket: &str,
    object: &str,
    version_id: Option<&str>,
    conditions: &mut HashMap<String, Vec<String>>,
    needs_tag: bool,
) -> S3Result<()> {
    if !needs_tag || bucket.is_empty() || object.is_empty() {
        counter!("rustfs_object_tag_conditions_skipped_total", "op" => action_tag_metric_label(&action)).increment(1);
        return Ok(());
    }

    let store = request_object_store(req)?;
    let tags = get_or_fetch_object_tag_conditions(req, store.as_ref(), bucket, object, version_id, action).await?;
    merge_object_tag_conditions(conditions, &tags);
    Ok(())
}
