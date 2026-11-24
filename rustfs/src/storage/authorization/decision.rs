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

//! The policy decision shared by every request edge.
//!
//! Responsible for: [`authorize_request_core`] — the bucket-policy explicit-deny gate, the
//! prepared IAM evaluation, the bucket-policy fallback, the public-access block, and the
//! permission-probe refusals — and the thin entry points every access hook calls
//! ([`authorize_request`], [`authorize_bucket_request`], [`authorize_object_request`], the
//! bucket-config mutation and staged multipart part forms), with the structured denial log and
//! audit of each refusal.
//! Not responsible for: building the condition values (`conditions`), the table-resource
//! check that follows an Allow (`table_data_plane`), the generation fences (`guards`), or
//! which action an operation asks (each access hook decides).
//! Upstream: the legacy access hooks, `storage::access_owned`, the admin and app callers.
//! Downstream: IAM, the bucket policy system, and the denial auditor.

use super::conditions::{
    authorization_conditions_with_internal, bucket_policy_needs_existing_object_tag_from_hint,
    load_bucket_policy_existing_object_tag_hint, maybe_merge_object_tag_conditions, request_authz_facts, request_principal,
    root_access_key,
};
use super::guards::load_bucket_config_mutation_snapshot;
use super::table_data_plane::{
    TablePermissionConditions, authorize_table_data_plane_if_needed, deny_anonymous_table_data_plane_if_needed,
};
use super::{AuthorizationRequest, missing_req_info_error, req_info_mut, req_info_ref, request_iam_store, request_object_store};
use crate::auth::facts::{AuthzFacts, AuthzTarget, base_conditions, build_args};
use crate::error::ApiError;
use crate::storage::authorization_actions::versioned_read_action;
use crate::storage::denial_audit::DenialAuditor;
use crate::storage::storage_api::runtime_sources_consumer::ServerContextSlot;
use crate::storage::{PolicySys, StorageError, is_err_bucket_not_found};
use http::HeaderMap;
use rustfs_iam::error::Error as IamError;
use rustfs_policy::policy::BucketPolicyArgs;
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_types::{S3Error, S3Result, s3_error};
use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, SUFFIX_REPLICATION_ACTUAL_OBJECT_SIZE, SUFFIX_REPLICATION_SSEC_CRC, SUFFIX_SOURCE_ETAG,
    SUFFIX_SOURCE_MTIME, SUFFIX_SOURCE_REPLICATION_CHECK, SUFFIX_SOURCE_REPLICATION_REQUEST, SUFFIX_SOURCE_VERSION_ID,
    get_header,
};

pub(crate) fn iam_authorization_error(err: IamError) -> ApiError {
    match err {
        IamError::IAMActionNotAllowed => ApiError::access_denied(),
        other => ApiError::from(other),
    }
}

const EVENT_OBJECT_TAG_AUTHORIZATION: &str = "object_tag_authorization";
const LOG_COMPONENT_ACCESS: &str = "storage_access";
const LOG_SUBSYSTEM_AUTHORIZATION: &str = "authorization";

fn has_replication_only_put_headers(headers: &HeaderMap) -> bool {
    headers.contains_key(AMZ_BUCKET_REPLICATION_STATUS)
        || get_header(headers, SUFFIX_REPLICATION_ACTUAL_OBJECT_SIZE).is_some()
        || get_header(headers, SUFFIX_REPLICATION_SSEC_CRC).is_some()
        || get_header(headers, SUFFIX_SOURCE_ETAG).is_some()
        || get_header(headers, SUFFIX_SOURCE_MTIME).is_some()
        || get_header(headers, SUFFIX_SOURCE_REPLICATION_CHECK).is_some()
        || get_header(headers, SUFFIX_SOURCE_REPLICATION_REQUEST).is_some()
        || get_header(headers, SUFFIX_SOURCE_VERSION_ID).is_some()
        || rustfs_utils::http::has_ssec_transport_headers(headers)
}

/// Asks `s3:ReplicateObject` when the request headers carry a replication-only header, and on
/// Allow marks the request as an authorized replication request for the use case. A POST form
/// cannot carry these headers, so the request headers are read, not the policy headers.
pub(crate) async fn authorize_replication_only_put_headers<R: AuthorizationRequest + ?Sized>(req: &mut R) -> S3Result<()> {
    if !has_replication_only_put_headers(req.headers()) {
        return Ok(());
    }

    authorize_request(req, Action::S3Action(S3Action::ReplicateObjectAction)).await?;
    req_info_mut(req)?.replication_request_authorized = true;
    Ok(())
}

/// Authorizes a bucket-configuration mutation for either edge: `action` on `bucket`, with the
/// bucket incarnation snapshotted under the lifecycle namespace read lock BEFORE the decision, so
/// a bucket recreated during authorization is refused by the use case's conditional write. A
/// snapshot failure surfaces only after an Allow, which keeps AccessDenied first. On Allow the
/// snapshot is installed for [`bucket_config_mutation_incarnation`].
///
/// The legacy hooks pass `FS::server_ctx`; the gateway edge passes the envelope's server context.
///
/// # Errors
///
/// `InternalError` when the access phase installed no `ReqInfo`; what [`authorize_request`]
/// refuses; then the snapshot's lock or store error.
pub(crate) async fn authorize_bucket_config_mutation<R: AuthorizationRequest + ?Sized>(
    server_ctx: &ServerContextSlot,
    req: &mut R,
    bucket: &str,
    action: Action,
) -> S3Result<()> {
    req_info_mut(req)?.bucket = Some(bucket.to_owned());
    let snapshot = load_bucket_config_mutation_snapshot(server_ctx, req, bucket).await;
    authorize_request(req, action).await?;
    req.install_bucket_config_mutation(snapshot?);
    Ok(())
}

/// Authorizes tag deletion under the IAM repair scope for unreadable stored tags.
pub(crate) async fn authorize_bucket_tag_deletion<R: AuthorizationRequest + ?Sized>(
    server_ctx: &ServerContextSlot,
    req: &mut R,
    bucket: &str,
) -> S3Result<()> {
    rustfs_iam::sys::with_bucket_tag_deletion(authorize_bucket_config_mutation(
        server_ctx,
        req,
        bucket,
        Action::S3Action(S3Action::PutBucketTaggingAction),
    ))
    .await
}

/// Returns true when the owner (root or parent=root credentials) may bypass bucket policy
/// explicit Deny for this action. Per AWS S3, only GetBucketPolicy, PutBucketPolicy, and
/// DeleteBucketPolicy have this bypass so the admin can recover from a misconfigured policy.
pub(crate) fn owner_can_bypass_policy_deny(is_owner: bool, action: &Action) -> bool {
    is_owner
        && matches!(
            action,
            Action::S3Action(S3Action::GetBucketPolicyAction)
                | Action::S3Action(S3Action::PutBucketPolicyAction)
                | Action::S3Action(S3Action::DeleteBucketPolicyAction)
        )
}

/// Context shared by every denial exit of [`authorize_request`] (issue #5740).
///
/// The wire response intentionally stays the bare "Access Denied" so no policy
/// detail leaks to clients; the structured server-side event emitted here is
/// the only place the denial reason is recorded. Probe-style callers that use
/// denial as an expected filter outcome (per-bucket ListBuckets fallback,
/// per-object metadata permission collection) set
/// [`crate::storage::req_info::ReqInfo::suppress_denial_log`] so their routine denials log at `debug`
/// instead of `warn`.
pub(crate) struct DenialContext<'a> {
    pub(crate) quiet: bool,
    pub(crate) suppress_audit: bool,
    pub(crate) auditor: &'a DenialAuditor,
    pub(crate) bucket: &'a str,
    pub(crate) object: &'a str,
    pub(crate) version_id: Option<&'a str>,
    pub(crate) account: Option<&'a str>,
    pub(crate) is_owner: bool,
}

impl DenialContext<'_> {
    pub(crate) fn log(&self, reason: &'static str, action: Action) {
        if self.quiet {
            tracing::debug!(
                event = "s3_authorization_denied",
                reason,
                action = ?action,
                bucket = %self.bucket,
                object = %self.object,
                version_id = ?self.version_id,
                account = self.account.unwrap_or("<anonymous>"),
                is_owner = self.is_owner,
                "authorization probe denied"
            );
            return;
        }
        tracing::warn!(
            event = "s3_authorization_denied",
            reason,
            action = ?action,
            bucket = %self.bucket,
            object = %self.object,
            version_id = ?self.version_id,
            account = self.account.unwrap_or("<anonymous>"),
            is_owner = self.is_owner,
            "request denied by authorization layer"
        );
    }

    /// Log the denial and, unless it is a quiet probe denial or audited in aggregate by the
    /// caller, audit it.
    pub(crate) fn record<R: AuthorizationRequest + ?Sized>(&self, req: &R, reason: &'static str, action: Action) {
        self.log(reason, action);
        if !self.quiet && !self.suppress_audit {
            req.audit_denial(self.auditor, &action, reason);
        }
    }

    /// Build the generic AccessDenied response after recording the structured
    /// denial event and, unless this is a quiet probe denial, an audit entry.
    pub(crate) fn deny<R: AuthorizationRequest + ?Sized>(&self, req: &R, reason: &'static str, action: Action) -> S3Error {
        self.record(req, reason, action);
        s3_error!(AccessDenied, "Access Denied")
    }
}

/// Audits a denial at an exit that has no [`DenialContext`] of its own. It only audits: those exits
/// keep their existing logging. The request's own probe flags are honored.
pub(crate) fn audit_request_denial<R: AuthorizationRequest + ?Sized>(
    req: &R,
    auditor: &DenialAuditor,
    reason: &'static str,
    action: Action,
) {
    let info = req.req_info();
    if info.is_some_and(|info| info.suppress_denial_log || info.suppress_denial_audit) {
        return;
    }
    req.audit_denial(auditor, &action, reason);
}

/// Authorizes the request based on the action and credentials.
pub(crate) async fn authorize_request<R: AuthorizationRequest + ?Sized>(req: &mut R, action: Action) -> S3Result<()> {
    // Restore the `x-amz-meta-*` user metadata the SigV4 presigners hoist into
    // the query string. This runs after s3s has verified the signature — its own
    // verifier rejects an `x-amz-*` header outside `SignedHeaders`, which is
    // exactly what a hoisted parameter becomes — and before policy evaluation and
    // the handler read request properties, so every operation sees one request
    // shape (rustfs/rustfs#9142).
    let query = req.uri().query().map(str::to_owned);
    crate::auth::hoist_signed_query_params(req.headers_mut(), query.as_deref())?;
    let internal_object_authorization = req.internal_object_authorization();
    authorize_request_core(req, action, None, internal_object_authorization).await
}

/// Authorizes `action` on `bucket` for either edge: the whole access hook of an operation that
/// names one bucket and asks one action, such as HeadBucket.
///
/// # Errors
///
/// `InternalError` when the access phase installed no `ReqInfo`; otherwise what
/// [`authorize_request`] refuses.
pub(crate) async fn authorize_bucket_request<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    bucket: &str,
    action: Action,
) -> S3Result<()> {
    req.req_info_mut().ok_or_else(missing_req_info_error)?.bucket = Some(bucket.to_owned());
    authorize_request(req, action).await
}

/// Authorizes `action` on one object version for either edge: the whole access hook of an
/// operation that names a bucket, a key and an optional version and asks one action, such as
/// GetObjectAcl. A caller whose action depends on the version picks it first
/// ([`versioned_read_action`], [`versioned_tagging_action`]).
///
/// # Errors
///
/// `InternalError` when the access phase installed no `ReqInfo`; otherwise what
/// [`authorize_request`] refuses.
pub(crate) async fn authorize_object_request<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    action: Action,
) -> S3Result<()> {
    let req_info = req_info_mut(req)?;
    req_info.bucket = Some(bucket.to_owned());
    req_info.object = Some(key.to_owned());
    req_info.version_id = version_id.map(str::to_owned);
    authorize_request(req, action).await
}

/// A specific destination ETag reveals stored content and requires read access.
/// Wildcard existence conditions retain the existing write-only contract.
pub(crate) async fn authorize_write_etag_condition<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    specific_dto_etag: bool,
) -> S3Result<()> {
    let specific_header_etag = req
        .policy_headers()
        .get_all(http::header::IF_MATCH)
        .iter()
        .any(|value| value.to_str().map_or(true, |value| value.trim() != "*"));
    if specific_dto_etag || specific_header_etag {
        let action = versioned_read_action(req_info_ref(req)?.version_id.as_deref());
        authorize_request(req, action).await?;
    }
    Ok(())
}

pub(crate) async fn authorize_request_with_location_constraint<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    action: Action,
    location_constraint: Option<String>,
) -> S3Result<()> {
    let internal_object_authorization = req.internal_object_authorization();
    authorize_request_core(req, action, location_constraint, internal_object_authorization).await
}

/// The policy decision shared by every request edge. `internal_object_authorization`
/// marks an exact-object check inside an already authorized table-catalog operation.
pub(crate) async fn authorize_request_core<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    action: Action,
    location_constraint: Option<String>,
    internal_object_authorization: bool,
) -> S3Result<()> {
    let facts = policy_request_facts(req, action, location_constraint.as_deref())?;
    authorize_request_with_facts(req, &facts, internal_object_authorization).await
}

/// Checks whether a caller may learn that the current object is absent. Only called
/// on a terminal read error, after the original object authorization succeeded.
pub(crate) async fn authorize_missing_object_disclosure<R: AuthorizationRequest + ?Sized>(req: &mut R) -> S3Result<()> {
    let info = req_info_mut(req)?;
    info.object = None;
    info.version_id = None;
    info.suppress_denial_log = true;
    info.suppress_denial_audit = true;
    let mut facts = policy_request_facts(req, Action::S3Action(S3Action::ListBucketAction), None)?;
    // The caller made an object read. Only actual listings supply prefix, delimiter and max-keys.
    facts.clear_list_query();
    authorize_request_with_facts(req, &facts, false).await
}

fn policy_request_facts<R: AuthorizationRequest + ?Sized>(
    req: &R,
    action: Action,
    location_constraint: Option<&str>,
) -> S3Result<AuthzFacts> {
    let info = req_info_ref(req)?;
    Ok(request_authz_facts(
        req,
        AuthzTarget {
            action,
            bucket: info.bucket.as_deref().unwrap_or_default(),
            object: info.object.as_deref().unwrap_or_default(),
            version_id: info.version_id.as_deref(),
            location_constraint,
        },
    ))
}

async fn authorize_request_with_facts<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    facts: &AuthzFacts,
    internal_object_authorization: bool,
) -> S3Result<()> {
    let action = facts.action();
    let req_info = req.req_info().ok_or_else(missing_req_info_error)?;
    let cred = req_info.cred.clone();
    let is_owner = req_info.is_owner;
    let bucket = req_info.bucket.clone().unwrap_or_default();
    let object = req_info.object.clone().unwrap_or_default();
    let version_id = req_info.version_id.clone();
    let quiet_denial = req_info.suppress_denial_log;
    let suppress_denial_audit = req_info.suppress_denial_audit;
    let store = request_object_store(req)?;
    let denial = DenialContext {
        quiet: quiet_denial,
        suppress_audit: suppress_denial_audit,
        auditor: DenialAuditor::global(),
        bucket: bucket.as_str(),
        object: object.as_str(),
        version_id: version_id.as_deref(),
        account: cred.as_ref().map(|c| c.access_key.as_str()),
        is_owner,
    };

    if let Some(cred) = &cred {
        let iam_store = request_iam_store(req)?;

        let root_access_key = root_access_key();
        let principal = request_principal(req, cred, &root_access_key).with_owner(is_owner);
        let mut conditions = authorization_conditions_with_internal(req, &principal, facts, internal_object_authorization)?;

        let action_args = build_args(&principal, facts, &conditions);
        let prepared = iam_store.prepare_auth(&action_args).await;
        let needs_tag_from_iam = prepared.needs_existing_object_tag;

        let bucket_tag_hint = if !bucket.is_empty() && !object.is_empty() {
            Some(load_bucket_policy_existing_object_tag_hint(store.as_ref(), bucket.as_str(), action).await)
        } else {
            None
        };
        let needs_tag_from_bucket = if let Some(hint) = bucket_tag_hint.as_ref() {
            let bucket_args = BucketPolicyArgs {
                bucket: bucket.as_str(),
                action,
                is_owner,
                account: cred.access_key.as_str(),
                groups: &cred.groups,
                conditions: &conditions,
                object: object.as_str(),
            };
            bucket_policy_needs_existing_object_tag_from_hint(hint, &bucket_args).await
        } else {
            false
        };

        let needs_tag = needs_tag_from_iam || needs_tag_from_bucket;
        if needs_tag {
            tracing::debug!(
                event = EVENT_OBJECT_TAG_AUTHORIZATION,
                component = LOG_COMPONENT_ACCESS,
                subsystem = LOG_SUBSYSTEM_AUTHORIZATION,
                anonymous = false,
                bucket = %bucket,
                ?action,
                needs_tag_from_iam,
                needs_tag_from_bucket,
                "Object tag authorization conditions required"
            );
        }
        maybe_merge_object_tag_conditions(
            req,
            action,
            bucket.as_str(),
            object.as_str(),
            version_id.as_deref(),
            &mut conditions,
            needs_tag,
        )
        .await?;
        let bucket_name = bucket.as_str();

        let check_explicit_iam_deny = matches!(
            action,
            Action::S3Action(
                S3Action::DeleteObjectAction
                    | S3Action::DeleteObjectVersionAction
                    | S3Action::ForceDeleteBucketAction
                    | S3Action::ForceDeleteObjectAction
                    | S3Action::ListBucketAction
                    | S3Action::ListBucketVersionsAction
                    | S3Action::BypassGovernanceRetentionAction
                    | S3Action::ReplicateDeleteAction
            )
        ) && prepared.combined_policy_for_view().is_some();

        let probe_bucket_policy = if req.permission_probe() && !bucket.is_empty() {
            match store.get_bucket_policy(&bucket).await {
                Ok((policy, _)) => Some(policy),
                Err(error) if error == StorageError::ConfigNotFound || is_err_bucket_not_found(&error) => None,
                Err(_) => return Err(rustfs_s3_types::s3_error!(NotImplemented, "permission query is unavailable")),
            }
        } else {
            None
        };
        if req.permission_probe() {
            use rustfs_policy::policy::probe::{PROBE_EXISTING_TAGS_RESOLVED, bucket_policy_probe_decision};
            if needs_tag {
                conditions.insert(PROBE_EXISTING_TAGS_RESOLVED.to_owned(), vec!["true".to_owned()]);
            }
            let args = build_args(&principal, facts, &conditions);
            let bucket_args = BucketPolicyArgs {
                bucket: bucket.as_str(),
                action,
                is_owner,
                account: &cred.access_key,
                groups: &cred.groups,
                conditions: &conditions,
                object: object.as_str(),
            };
            let guard = if owner_can_bypass_policy_deny(is_owner, &action) {
                Some(true)
            } else if let Some(policy) = &probe_bucket_policy {
                bucket_policy_probe_decision(
                    policy,
                    &BucketPolicyArgs {
                        is_owner: true,
                        ..bucket_args.clone()
                    },
                )
                .await
            } else {
                Some(true)
            };
            if guard.is_none() {
                return Err(rustfs_s3_types::s3_error!(NotImplemented, "permission query lacks operation facts"));
            }
            if guard != Some(false) && prepared.has_unknown_probe_conditions(&args).await {
                let fallback = if let Some(policy) = &probe_bucket_policy {
                    bucket_policy_probe_decision(policy, &bucket_args).await
                } else {
                    Some(is_owner)
                };
                let iam_deny_known_absent = if check_explicit_iam_deny {
                    let mut deny_args = args.clone();
                    deny_args.deny_only = true;
                    prepared.probe_decision(&deny_args).await == Some(true)
                } else {
                    true
                };
                if !prepared.permits_bucket_policy_probe_fallback() || fallback != Some(true) || !iam_deny_known_absent {
                    return Err(rustfs_s3_types::s3_error!(NotImplemented, "permission query lacks operation facts"));
                }
            }
        }

        // Per AWS S3: root can always perform GetBucketPolicy, PutBucketPolicy, DeleteBucketPolicy
        // even if bucket policy explicitly denies. Other actions (ListBucket, GetObject, etc.) are
        // subject to bucket policy Deny for root as well. See: repost.aws/knowledge-center/s3-accidentally-denied-access
        // Here "owner" means root or credentials whose parent_user is root (e.g. Console admin via STS).
        let owner_can_bypass_deny = owner_can_bypass_policy_deny(is_owner, &action);
        if !bucket_name.is_empty()
            && !owner_can_bypass_deny
            && !PolicySys::try_is_allowed_for_store(
                store.as_ref(),
                &BucketPolicyArgs {
                    bucket: bucket_name,
                    action,
                    // Early explicit-deny gate for bucket policy: use owner short-circuit path so
                    // deny statements are enforced before IAM/bucket allow fallback evaluation.
                    is_owner: true,
                    account: &cred.access_key,
                    groups: &cred.groups,
                    conditions: &conditions,
                    object: object.as_str(),
                },
            )
            .await
            .map_err(ApiError::from)?
        {
            return Err(denial.deny(req, "bucket_policy_explicit_deny", action));
        }

        let probe_evaluation_conditions = if req.permission_probe() {
            use rustfs_policy::policy::probe::{
                PROBE_COPY_SOURCE_KNOWN, PROBE_EXISTING_TAGS_RESOLVED, PROBE_IF_MATCH_KNOWN, PROBE_IF_NONE_MATCH_KNOWN,
                PROBE_RETENTION_FACTS_KNOWN,
            };
            let mut values = conditions.clone();
            let original_values = base_conditions(&principal, facts);
            for marker in [
                PROBE_EXISTING_TAGS_RESOLVED,
                PROBE_RETENTION_FACTS_KNOWN,
                PROBE_COPY_SOURCE_KNOWN,
                PROBE_IF_MATCH_KNOWN,
                PROBE_IF_NONE_MATCH_KNOWN,
            ] {
                values.remove(marker);
                if let Some(value) = original_values.get(marker) {
                    values.insert(marker.to_owned(), value.clone());
                }
            }
            Some(values)
        } else {
            None
        };
        let evaluation_conditions = probe_evaluation_conditions.as_ref().unwrap_or(&conditions);
        let iam_allowed = {
            let mut final_args = build_args(&principal, facts, evaluation_conditions);
            let allowed = iam_store
                .try_eval_prepared(&prepared, &final_args)
                .await
                .map_err(iam_authorization_error)?;
            if !allowed && check_explicit_iam_deny {
                // Only an implicit IAM denial may fall back to a bucket policy Allow.
                final_args.deny_only = true;
                if !iam_store
                    .try_eval_prepared(&prepared, &final_args)
                    .await
                    .map_err(iam_authorization_error)?
                {
                    return Err(denial.deny(req, "iam_explicit_deny", action));
                }
            }
            allowed
        };

        if iam_allowed {
            if !internal_object_authorization && req.evaluates_table_data_plane() {
                authorize_table_data_plane_if_needed(
                    req,
                    action,
                    bucket.as_str(),
                    object.as_str(),
                    cred,
                    is_owner,
                    TablePermissionConditions {
                        evaluation: evaluation_conditions,
                        probe: Some(&conditions),
                    },
                )
                .await?;
            }
            return Ok(());
        }

        if action == Action::S3Action(S3Action::ListAllMyBucketsAction) {
            denial.log("iam_implicit_deny", action);
            return Err(ApiError::access_denied().into());
        }

        if req.permission_probe()
            && let Some(policy) = &probe_bucket_policy
        {
            use rustfs_policy::policy::probe::bucket_policy_probe_decision;
            if bucket_policy_probe_decision(
                policy,
                &BucketPolicyArgs {
                    bucket: bucket.as_str(),
                    action,
                    is_owner,
                    account: &cred.access_key,
                    groups: &cred.groups,
                    conditions: &conditions,
                    object: object.as_str(),
                },
            )
            .await
            .is_none()
            {
                return Err(rustfs_s3_types::s3_error!(NotImplemented, "permission query lacks operation facts"));
            }
        }

        let policy_allowed_fallback = PolicySys::try_is_allowed_for_store(
            store.as_ref(),
            &BucketPolicyArgs {
                bucket: bucket.as_str(),
                action,
                is_owner,
                account: &cred.access_key,
                groups: &cred.groups,
                conditions: evaluation_conditions,
                object: object.as_str(),
            },
        )
        .await
        .map_err(ApiError::from)?;

        if policy_allowed_fallback {
            if !internal_object_authorization && req.evaluates_table_data_plane() {
                authorize_table_data_plane_if_needed(
                    req,
                    action,
                    bucket.as_str(),
                    object.as_str(),
                    cred,
                    is_owner,
                    TablePermissionConditions {
                        evaluation: evaluation_conditions,
                        probe: Some(&conditions),
                    },
                )
                .await?;
            }
            return Ok(());
        }
    } else {
        let default_cred = rustfs_credentials::Credentials::default();
        let root_access_key = root_access_key();
        let principal = request_principal(req, &default_cred, &root_access_key);
        let mut conditions = authorization_conditions_with_internal(req, &principal, facts, internal_object_authorization)?;

        let no_groups: Option<Vec<String>> = None;
        let bucket_tag_hint = if !bucket.is_empty() && !object.is_empty() {
            Some(load_bucket_policy_existing_object_tag_hint(store.as_ref(), bucket.as_str(), action).await)
        } else {
            None
        };
        let needs_tag_from_bucket = if let Some(hint) = bucket_tag_hint.as_ref() {
            let bucket_args = BucketPolicyArgs {
                bucket: bucket.as_str(),
                action,
                is_owner: false,
                account: "",
                groups: &no_groups,
                conditions: &conditions,
                object: object.as_str(),
            };
            bucket_policy_needs_existing_object_tag_from_hint(hint, &bucket_args).await
        } else {
            false
        };
        if needs_tag_from_bucket {
            tracing::debug!(
                event = EVENT_OBJECT_TAG_AUTHORIZATION,
                component = LOG_COMPONENT_ACCESS,
                subsystem = LOG_SUBSYSTEM_AUTHORIZATION,
                anonymous = true,
                bucket = %bucket,
                ?action,
                "Object tag authorization conditions required"
            );
        }
        maybe_merge_object_tag_conditions(
            req,
            action,
            bucket.as_str(),
            object.as_str(),
            version_id.as_deref(),
            &mut conditions,
            needs_tag_from_bucket,
        )
        .await?;
        let bucket_name = bucket.as_str();

        if !bucket_name.is_empty()
            && !PolicySys::try_is_allowed_for_store(
                store.as_ref(),
                &BucketPolicyArgs {
                    bucket: bucket_name,
                    action,
                    // Early explicit-deny gate for bucket policy in anonymous path.
                    is_owner: true,
                    account: "",
                    groups: &None,
                    conditions: &conditions,
                    object: object.as_str(),
                },
            )
            .await
            .map_err(ApiError::from)?
        {
            return Err(denial.deny(req, "bucket_policy_explicit_deny", action));
        }

        if action != Action::S3Action(S3Action::ListAllMyBucketsAction) {
            let policy_allowed = PolicySys::try_is_allowed_for_store(
                store.as_ref(),
                &BucketPolicyArgs {
                    bucket: bucket.as_str(),
                    action,
                    is_owner: false,
                    account: "",
                    groups: &None,
                    conditions: &conditions,
                    object: object.as_str(),
                },
            )
            .await
            .map_err(ApiError::from)?;

            // A bucket policy granting s3:ListBucket also covers listing versions.
            // Keep this compatibility fallback inside the same public-access gate.
            let policy_allowed = policy_allowed
                || (action == Action::S3Action(S3Action::ListBucketVersionsAction)
                    && PolicySys::try_is_allowed_for_store(
                        store.as_ref(),
                        &BucketPolicyArgs {
                            bucket: bucket.as_str(),
                            action: Action::S3Action(S3Action::ListBucketAction),
                            is_owner: false,
                            account: "",
                            groups: &None,
                            conditions: &conditions,
                            object: "",
                        },
                    )
                    .await
                    .map_err(ApiError::from)?);

            if policy_allowed {
                if req.evaluates_table_data_plane() {
                    deny_anonymous_table_data_plane_if_needed(req, action, bucket.as_str(), object.as_str()).await?;
                }
                // RestrictPublicBuckets: when true, deny public access even if bucket policy allows it.
                match store.restricts_public_bucket_access(bucket_name).await {
                    Ok(true) => return Err(denial.deny(req, "restrict_public_buckets", action)),
                    Ok(false) => {}
                    Err(StorageError::ConfigNotFound) => {}
                    Err(_) => {
                        return Err(denial.deny(req, "public_access_block_unavailable", action));
                    }
                }
                return Ok(());
            }
        }
    }

    Err(denial.deny(req, "no_policy_allows_action", action))
}

// Multipart parts are staged and become visible only after CompleteMultipartUpload,
// which uses the normal publication-fenced authorization path.
/// Authorizes `s3:PutObject` for one staged multipart part: the part creates no object, and the
/// staged-part marker is set only for this decision.
pub(crate) async fn authorize_staged_multipart_part<R: AuthorizationRequest + ?Sized>(req: &mut R) -> S3Result<()> {
    req.set_staged_multipart_part(true);
    let result = authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await;
    req.set_staged_multipart_part(false);
    result
}
