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

use super::ObjectOptions;
use super::ecfs::FS;
use super::{ECStore, PolicySys, ReplicationStatusType, StorageError, get_lock_acquire_timeout, is_err_bucket_not_found};
use crate::auth::{
    check_key_valid_with_context, get_condition_values_with_client_info, get_condition_values_with_query_and_client_info,
    get_session_token,
};
use crate::error::ApiError;
use crate::license::license_check;
use crate::shared_types::RemoteAddr;
use crate::storage::request_context::RequestContext;
use crate::storage::storage_api::contract::bucket::BUCKET_LIFECYCLE_LOCK_OBJECT;
use crate::storage::storage_api::contract::namespace::NamespaceLocking as _;
use crate::storage::storage_api::runtime_sources_consumer::ServerContextSlot;
use crate::storage::storage_api::runtime_sources_consumer::runtime_sources;
use http::HeaderMap;
use metrics::counter;
use rustfs_iam::{
    error::Error as IamError,
    store::object::ObjectStore,
    sys::{IamSys, PreparedIamAuth},
};
use rustfs_policy::policy::action::{Action, AdminAction, S3Action};
use rustfs_policy::policy::{
    Args, BucketPolicy, BucketPolicyArgs, bucket_policy_needs_existing_object_tag_for_args,
    bucket_policy_uses_existing_object_tag_conditions,
};
use rustfs_trusted_proxies::ClientInfo;
use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, AMZ_OBJECT_LOCK_BYPASS_GOVERNANCE, SUFFIX_FORCE_DELETE, SUFFIX_REPLICATION_ACTUAL_OBJECT_SIZE,
    SUFFIX_REPLICATION_SSEC_CRC, SUFFIX_SOURCE_ETAG, SUFFIX_SOURCE_MTIME, SUFFIX_SOURCE_REPLICATION_CHECK,
    SUFFIX_SOURCE_REPLICATION_REQUEST, SUFFIX_SOURCE_VERSION_ID, get_header,
};
use s3s::access::{S3Access, S3AccessContext};
use s3s::{S3Error, S3ErrorCode, S3Request, S3Result, dto::*, s3_error};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
#[cfg(test)]
use std::sync::OnceLock;
use url::{Url, form_urlencoded};

#[derive(Default, Clone, Debug)]
pub(crate) struct ReqInfo {
    pub cred: Option<rustfs_credentials::Credentials>,
    pub is_owner: bool,
    pub bucket: Option<String>,
    pub object: Option<String>,
    pub version_id: Option<String>,
    pub replication_request_authorized: bool,
    #[allow(dead_code, reason = "written but never read back (backlog#1823)")]
    pub region: Option<s3s::region::Region>,
    pub request_context: Option<RequestContext>,
    /// Set by probe-style callers that treat AccessDenied as an expected filter
    /// outcome (ListBuckets per-bucket fallback, ListObjects metadata permission
    /// collection) so `authorize_request` logs those routine denials at `debug`
    /// instead of `warn` (issue #5740).
    pub suppress_denial_log: bool,
}

pub(crate) fn replication_request_authorized<T>(req: &S3Request<T>) -> bool {
    req.extensions
        .get::<ReqInfo>()
        .is_some_and(|req_info| req_info.replication_request_authorized)
}

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

async fn authorize_replication_only_put_headers<T>(req: &mut S3Request<T>) -> S3Result<()> {
    if !has_replication_only_put_headers(&req.headers) {
        return Ok(());
    }

    authorize_request(req, Action::S3Action(S3Action::ReplicateObjectAction)).await?;
    req_info_mut(req)?.replication_request_authorized = true;
    Ok(())
}

pub(crate) fn recursive_force_delete_is_authorized(headers: &HeaderMap, is_owner: bool, replica_request: bool) -> bool {
    !get_header(headers, SUFFIX_FORCE_DELETE).is_some_and(|value| value.eq_ignore_ascii_case("true"))
        || is_owner
        || replica_request
}

#[derive(Clone, Debug)]
pub(crate) struct PostObjectRequestMarker;

#[derive(Clone, Debug)]
struct InternalObjectAuthorization;

#[derive(Clone, Debug)]
struct StagedMultipartPartAuthorization;

#[derive(Clone, Default)]
struct TableDataPlanePublicationGuards {
    state: Arc<parking_lot::Mutex<TableDataPlanePublicationState>>,
}

#[derive(Default)]
struct TableDataPlanePublicationState {
    keys: BTreeSet<(String, String)>,
    guards: Vec<Box<dyn Send>>,
    resources: HashMap<(String, String), crate::table_catalog::TableDataPlaneResource>,
    missing_resources: BTreeSet<(String, String)>,
}

#[derive(Clone, Debug)]
pub(crate) struct BucketGenerationGuard {
    bucket: String,
    incarnation_id: uuid::Uuid,
}

#[derive(Clone, Debug)]
pub(crate) struct BucketConfigMutationSnapshot {
    bucket: String,
    incarnation_id: uuid::Uuid,
}

pub(crate) fn bucket_config_mutation_incarnation<T>(req: &S3Request<T>, bucket: &str) -> S3Result<Option<uuid::Uuid>> {
    let Some(snapshot) = req.extensions.get::<BucketConfigMutationSnapshot>() else {
        if req.extensions.get::<std::sync::Arc<ServerContextSlot>>().is_some() {
            return Err(s3_error!(InternalError, "bucket config mutation snapshot is missing"));
        }
        return Ok(None);
    };
    if snapshot.bucket != bucket {
        return Err(s3_error!(InternalError, "bucket config mutation snapshot does not match request bucket"));
    }
    Ok(Some(snapshot.incarnation_id))
}

async fn load_bucket_config_mutation_snapshot<T>(
    fs: &FS,
    req: &S3Request<T>,
    bucket: &str,
) -> S3Result<BucketConfigMutationSnapshot> {
    let store = fs
        .server_ctx()
        .object_store()
        .ok_or_else(|| s3_error!(InternalError, "object store is not initialized"))?;
    let lock = store
        .new_ns_lock(bucket, BUCKET_LIFECYCLE_LOCK_OBJECT)
        .await
        .map_err(ApiError::from)?;
    let _lifecycle_guard = lock.get_read_lock(get_lock_acquire_timeout()).await.map_err(|err| {
        ApiError::from(match err {
            rustfs_lock::LockError::QuorumNotReached { required, achieved } => StorageError::NamespaceLockQuorumUnavailable {
                mode: "bucket_config_mutation",
                bucket: bucket.to_string(),
                object: BUCKET_LIFECYCLE_LOCK_OBJECT.to_string(),
                required,
                achieved,
            },
            other => StorageError::Lock(other),
        })
    })?;
    let incarnation_id = load_bucket_generation_from_store(store.as_ref(), req, bucket)
        .await?
        .incarnation_id;
    Ok(BucketConfigMutationSnapshot {
        bucket: bucket.to_string(),
        incarnation_id,
    })
}

async fn authorize_bucket_config_mutation<T>(fs: &FS, req: &mut S3Request<T>, action: Action) -> S3Result<()> {
    let bucket = req_info_ref(req)?
        .bucket
        .clone()
        .ok_or_else(|| s3_error!(InternalError, "bucket config mutation request has no bucket"))?;
    let snapshot = load_bucket_config_mutation_snapshot(fs, req, &bucket).await;
    authorize_request(req, action).await?;
    req.extensions.insert(snapshot?);
    Ok(())
}

#[derive(Clone, Debug)]
struct CopySourceBucketGenerationGuard {
    bucket: String,
    incarnation_id: uuid::Uuid,
}

#[cfg(test)]
type RestoreAuthorizationTestHook = (String, tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>);

#[cfg(test)]
static RESTORE_AUTHORIZATION_TEST_HOOK: OnceLock<std::sync::Mutex<Option<RestoreAuthorizationTestHook>>> = OnceLock::new();

#[cfg(test)]
fn install_restore_authorization_test_hook(
    bucket: String,
    authorized: tokio::sync::oneshot::Sender<()>,
    resume: tokio::sync::oneshot::Receiver<()>,
) {
    *RESTORE_AUTHORIZATION_TEST_HOOK
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("restore authorization test hook lock should not be poisoned") = Some((bucket, authorized, resume));
}

#[cfg(test)]
async fn wait_for_restore_authorization_test_hook(bucket: &str) {
    let hook = {
        let mut slot = RESTORE_AUTHORIZATION_TEST_HOOK
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("restore authorization test hook lock should not be poisoned");
        if slot.as_ref().is_some_and(|(expected_bucket, _, _)| expected_bucket == bucket) {
            slot.take()
        } else {
            None
        }
    };
    if let Some((_bucket, authorized, resume)) = hook {
        let _ = authorized.send(());
        let _ = resume.await;
    }
}

#[derive(Clone, Debug)]
enum PendingDeleteBucketGenerationGuard {
    Ready(BucketGenerationGuard),
    Failed { code: S3ErrorCode, message: String },
}

impl PendingDeleteBucketGenerationGuard {
    fn from_result(result: S3Result<BucketGenerationGuard>) -> Self {
        match result {
            Ok(guard) => Self::Ready(guard),
            Err(err) => Self::Failed {
                code: err.code().clone(),
                message: err.message().unwrap_or_else(|| err.code().as_str()).to_string(),
            },
        }
    }
}

pub(crate) async fn load_bucket_generation_from_store<T>(
    store: &crate::storage::storage_api::ECStore,
    req: &S3Request<T>,
    bucket: &str,
) -> S3Result<BucketGenerationGuard> {
    if let Some(existing) = req.extensions.get::<BucketGenerationGuard>() {
        return if existing.bucket == bucket {
            Ok(existing.clone())
        } else {
            Err(s3_error!(InternalError, "bucket generation guard does not match request bucket"))
        };
    }

    let incarnation_id = store.bucket_incarnation_id(bucket).await.map_err(ApiError::from)?;
    Ok(BucketGenerationGuard {
        bucket: bucket.to_string(),
        incarnation_id,
    })
}

async fn load_bucket_generation<T>(fs: &FS, req: &S3Request<T>, bucket: &str) -> S3Result<BucketGenerationGuard> {
    let store = fs
        .server_ctx()
        .object_store()
        .ok_or_else(|| s3_error!(InternalError, "object store is not initialized"))?;
    load_bucket_generation_from_store(store.as_ref(), req, bucket).await
}

async fn load_copy_source_bucket_generation(fs: &FS, bucket: &str) -> S3Result<CopySourceBucketGenerationGuard> {
    let store = fs
        .server_ctx()
        .object_store()
        .ok_or_else(|| s3_error!(InternalError, "object store is not initialized"))?;
    let incarnation_id = store.bucket_incarnation_id(bucket).await.map_err(ApiError::from)?;
    Ok(CopySourceBucketGenerationGuard {
        bucket: bucket.to_string(),
        incarnation_id,
    })
}

pub(crate) fn apply_bucket_generation_guard<T>(req: &S3Request<T>, bucket: &str, opts: &mut ObjectOptions) -> S3Result<()> {
    let pending_guard = req.extensions.get::<PendingDeleteBucketGenerationGuard>();
    let guard = match pending_guard {
        Some(PendingDeleteBucketGenerationGuard::Ready(guard)) => Some(guard),
        Some(PendingDeleteBucketGenerationGuard::Failed { code, message }) => {
            return Err(S3Error::with_message(code.clone(), message.clone()));
        }
        None => req.extensions.get::<BucketGenerationGuard>(),
    };
    let Some(guard) = guard else {
        if req.extensions.get::<std::sync::Arc<ServerContextSlot>>().is_some() {
            return Err(s3_error!(InternalError, "bucket generation guard is missing"));
        }
        return Ok(());
    };
    if guard.bucket != bucket {
        return Err(s3_error!(InternalError, "bucket generation guard does not match request bucket"));
    }
    opts.expected_bucket_incarnation_id = Some(guard.incarnation_id);
    Ok(())
}

pub(crate) fn apply_copy_source_bucket_generation_guard<T>(
    req: &S3Request<T>,
    bucket: &str,
    opts: &mut ObjectOptions,
) -> S3Result<()> {
    let Some(guard) = req.extensions.get::<CopySourceBucketGenerationGuard>() else {
        if req.extensions.get::<std::sync::Arc<ServerContextSlot>>().is_some() {
            return Err(s3_error!(InternalError, "copy source bucket generation guard is missing"));
        }
        return Ok(());
    };
    if guard.bucket != bucket {
        return Err(s3_error!(
            InternalError,
            "copy source bucket generation guard does not match request bucket"
        ));
    }
    opts.expected_bucket_incarnation_id = Some(guard.incarnation_id);
    Ok(())
}

pub(crate) fn req_info_ref<T>(req: &S3Request<T>) -> S3Result<&ReqInfo> {
    req.extensions
        .get::<ReqInfo>()
        .ok_or_else(|| s3_error!(InternalError, "ReqInfo not found in request extensions"))
}

pub(crate) fn req_info_mut<T>(req: &mut S3Request<T>) -> S3Result<&mut ReqInfo> {
    req.extensions
        .get_mut::<ReqInfo>()
        .ok_or_else(|| s3_error!(InternalError, "ReqInfo not found in request extensions"))
}

fn ext_req_info_mut(ext: &mut http::Extensions) -> S3Result<&mut ReqInfo> {
    ext.get_mut::<ReqInfo>()
        .ok_or_else(|| s3_error!(InternalError, "ReqInfo not found in request extensions"))
}

/// Extract the canonical `RequestContext` from a request, checking both
/// the request extensions directly and the `ReqInfo.request_context` field.
pub(crate) fn request_context_from_req<T>(req: &S3Request<T>) -> Option<RequestContext> {
    request_context_from_extensions(&req.extensions)
}

/// Same lookup against the extensions alone, for callers that have already
/// moved a field out of the request and can no longer borrow it whole.
pub(crate) fn request_context_from_extensions(extensions: &http::Extensions) -> Option<RequestContext> {
    extensions
        .get::<RequestContext>()
        .cloned()
        .or_else(|| extensions.get::<ReqInfo>().and_then(|ri| ri.request_context.clone()))
}

#[derive(Clone, Debug)]
pub(crate) struct ObjectTagConditions {
    bucket: String,
    object: String,
    version_id: Option<String>,
    values: HashMap<String, Vec<String>>,
}

impl ObjectTagConditions {
    fn new(bucket: &str, object: &str, version_id: Option<&str>, values: HashMap<String, Vec<String>>) -> Self {
        Self {
            bucket: bucket.to_string(),
            object: object.to_string(),
            version_id: version_id.map(str::to_string),
            values,
        }
    }

    fn matches(&self, bucket: &str, object: &str, version_id: Option<&str>) -> bool {
        self.bucket == bucket && self.object == object && self.version_id.as_deref() == version_id
    }
}

const AMZ_WRITE_OFFSET_BYTES_HEADER: &str = "x-amz-write-offset-bytes";

fn has_write_offset_bytes_header(headers: &http::HeaderMap) -> bool {
    headers.contains_key(AMZ_WRITE_OFFSET_BYTES_HEADER)
}

/// True when the bucket policy may evaluate `s3:ExistingObjectTag` for this request (statement
/// matches principal/action/resource and conditions reference ExistingObjectTag keys).
enum BucketPolicyExistingObjectTagHint {
    NoTagRequirement,
    ConservativeTagRequired,
    Parsed(BucketPolicy),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BucketPolicyRawLoadErrorKind {
    PolicyMissing,
    BucketMissing,
    Other,
}

fn classify_bucket_policy_raw_load_error(err: &StorageError) -> BucketPolicyRawLoadErrorKind {
    if err == &StorageError::ConfigNotFound {
        BucketPolicyRawLoadErrorKind::PolicyMissing
    } else if is_err_bucket_not_found(err) {
        BucketPolicyRawLoadErrorKind::BucketMissing
    } else {
        BucketPolicyRawLoadErrorKind::Other
    }
}

/// Load and parse bucket policy once for ExistingObjectTag hint checks.
async fn load_bucket_policy_existing_object_tag_hint(
    store: &ECStore,
    bucket: &str,
    action: Action,
) -> BucketPolicyExistingObjectTagHint {
    let (policy_str, _) = match store.get_bucket_policy_raw(bucket).await {
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
    match serde_json::from_str::<BucketPolicy>(policy_str.as_str()) {
        Ok(policy) => {
            if bucket_policy_uses_existing_object_tag_conditions(&policy) {
                BucketPolicyExistingObjectTagHint::Parsed(policy)
            } else {
                BucketPolicyExistingObjectTagHint::NoTagRequirement
            }
        }
        Err(err) => {
            tracing::warn!(
                bucket = %bucket,
                ?action,
                error = %err,
                "malformed bucket policy while checking ExistingObjectTag hint; conservatively enabling tag fetch"
            );
            BucketPolicyExistingObjectTagHint::ConservativeTagRequired
        }
    }
}

async fn bucket_policy_needs_existing_object_tag_from_hint(
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

fn merge_list_bucket_query_conditions(action: Action, query: Option<&str>, conditions: &mut HashMap<String, Vec<String>>) {
    if !matches!(
        action,
        Action::S3Action(
            S3Action::ListBucketAction | S3Action::ListBucketVersionsAction | S3Action::ListBucketMultipartUploadsAction
        )
    ) {
        return;
    }

    let Some(query) = query else {
        return;
    };

    for (key, value) in form_urlencoded::parse(query.as_bytes()) {
        match key.as_ref() {
            "prefix" | "delimiter" | "max-keys" => {
                conditions.entry(key.into_owned()).or_default().push(value.into_owned());
            }
            _ => {}
        }
    }
}

fn merge_request_object_tag_conditions(
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

fn authorization_conditions<T>(
    req: &S3Request<T>,
    cred: &rustfs_credentials::Credentials,
    version_id: Option<&str>,
    region: Option<s3s::region::Region>,
    remote_addr: Option<std::net::SocketAddr>,
    client_info: Option<&ClientInfo>,
    action: Action,
) -> S3Result<HashMap<String, Vec<String>>> {
    let mut conditions = get_condition_values_with_query_and_client_info(
        &req.headers,
        cred,
        version_id,
        region,
        remote_addr,
        req.uri.query(),
        client_info,
    );
    if req.extensions.get::<InternalObjectAuthorization>().is_some() {
        retain_internal_object_authorization_conditions(&mut conditions, cred);
        return Ok(conditions);
    }
    merge_list_bucket_query_conditions(action, req.uri.query(), &mut conditions);
    merge_request_object_tag_conditions(action, &req.headers, &mut conditions)?;
    Ok(conditions)
}

fn retain_internal_object_authorization_conditions(
    conditions: &mut HashMap<String, Vec<String>>,
    cred: &rustfs_credentials::Credentials,
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
        ) || cred.claims.as_ref().is_some_and(|claims| {
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
pub(crate) fn resource_free_condition_values<T>(
    req: &S3Request<T>,
    cred: &rustfs_credentials::Credentials,
) -> HashMap<String, Vec<String>> {
    let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
    get_condition_values_with_client_info(&req.headers, cred, None, None, remote_addr, req.extensions.get::<ClientInfo>())
}

fn request_object_store<T>(req: &S3Request<T>) -> S3Result<Arc<ECStore>> {
    match req.extensions.get::<Arc<ServerContextSlot>>() {
        Some(server_ctx) => server_ctx
            .installed_object_store()
            .ok_or_else(object_store_not_initialized_error),
        None => runtime_sources::current_object_store_handle().ok_or_else(object_store_not_initialized_error),
    }
}

fn object_store_not_initialized_error() -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, "object store is not initialized")
}

fn request_iam_store<T>(req: &S3Request<T>) -> S3Result<Arc<IamSys<ObjectStore>>> {
    let iam_store = match req.extensions.get::<Arc<ServerContextSlot>>() {
        Some(server_ctx) => server_ctx
            .installed_app_context()
            .filter(|context| context.iam().is_ready())
            .map(|context| context.iam().handle())
            .ok_or(IamError::IamSysNotInitialized),
        None => runtime_sources::current_ready_iam_handle(),
    };
    iam_store.map_err(|_| {
        S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("authorize_request {:?}", IamError::IamSysNotInitialized),
        )
    })
}

pub(crate) struct ListBucketsIamAuthorization {
    iam_store: Arc<IamSys<ObjectStore>>,
    prepared: PreparedIamAuth,
    account: String,
    groups: Option<Vec<String>>,
    claims: HashMap<String, serde_json::Value>,
    is_owner: bool,
    base_conditions: HashMap<String, Vec<String>>,
    bucket_conditions: HashMap<String, Vec<String>>,
}

impl ListBucketsIamAuthorization {
    pub(crate) async fn is_allowed(&self, bucket: &str, action: S3Action) -> bool {
        let conditions = if bucket.is_empty() {
            &self.base_conditions
        } else {
            &self.bucket_conditions
        };
        self.iam_store
            .eval_prepared(
                &self.prepared,
                &Args {
                    account: &self.account,
                    groups: &self.groups,
                    action: Action::S3Action(action),
                    bucket,
                    conditions,
                    is_owner: self.is_owner,
                    object: "",
                    claims: &self.claims,
                    deny_only: false,
                },
            )
            .await
    }
}

/// Prepare the IAM-only authorization used to decide which buckets are visible in ListBuckets.
/// Bucket policies are intentionally excluded from this discovery decision, matching MinIO.
pub(crate) async fn prepare_list_buckets_iam_authorization<T>(req: &S3Request<T>) -> S3Result<ListBucketsIamAuthorization> {
    let req_info = req_info_ref(req)?;
    let Some(cred) = req_info.cred.as_ref() else {
        return Err(ApiError::access_denied().into());
    };
    let iam_store = request_iam_store(req)?;
    let account = cred.access_key.clone();
    let groups = cred.groups.clone();
    let claims = cred.claims.clone().unwrap_or_default();
    let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
    let client_info = req.extensions.get::<ClientInfo>();
    let action = Action::S3Action(S3Action::ListAllMyBucketsAction);
    let base_conditions = authorization_conditions(req, cred, None, None, remote_addr, client_info, action)?;
    let mut bucket_conditions = base_conditions.clone();
    bucket_conditions.insert("prefix".to_string(), vec![String::new()]);
    bucket_conditions.insert("delimiter".to_string(), vec!["/".to_string()]);
    let prepared = iam_store
        .prepare_auth(&Args {
            account: &account,
            groups: &groups,
            action,
            bucket: "",
            conditions: &base_conditions,
            is_owner: req_info.is_owner,
            object: "",
            claims: &claims,
            deny_only: false,
        })
        .await;

    Ok(ListBucketsIamAuthorization {
        iam_store,
        prepared,
        account,
        groups,
        claims,
        is_owner: req_info.is_owner,
        base_conditions,
        bucket_conditions,
    })
}

/// Preserve the top-level IAM denial audit emitted before ListBuckets falls back
/// to bucket-level visibility checks.
pub(crate) fn log_list_buckets_iam_implicit_deny<T>(req: &S3Request<T>) -> S3Result<()> {
    let req_info = req_info_ref(req)?;
    let denial = DenialContext {
        quiet: true,
        bucket: req_info.bucket.as_deref().unwrap_or_default(),
        object: req_info.object.as_deref().unwrap_or_default(),
        version_id: req_info.version_id.as_deref(),
        account: req_info.cred.as_ref().map(|cred| cred.access_key.as_str()),
        is_owner: req_info.is_owner,
    };
    denial.log("iam_implicit_deny", Action::S3Action(S3Action::ListAllMyBucketsAction));
    Ok(())
}

/// Extra action that may be evaluated in the same authorization flow and can
/// independently require `ExistingObjectTag` conditions.
fn secondary_tag_hint_action(action: Action, version_id: Option<&str>) -> Option<Action> {
    match action {
        Action::S3Action(S3Action::DeleteObjectAction) if version_id.is_some() => {
            Some(Action::S3Action(S3Action::DeleteObjectVersionAction))
        }
        _ => None,
    }
}

/// GHSA-3ppv: select the IAM action for an object read by whether the request
/// names an explicit object version. A read that targets a specific version must
/// authorize against `s3:GetObjectVersion`; a current-object read authorizes
/// against `s3:GetObject`. Reading a historical version while only holding
/// `s3:GetObject` is an information-disclosure bypass, so the two must not be
/// conflated at the authorization boundary.
///
/// Note: `ActionSet::is_match` still maps a `s3:GetObjectVersion` grant onto a
/// `s3:GetObject` request. That mapping is intentionally left in place until the
/// remaining version-aware read paths (HeadObject, GetObjectAcl, tagging) get the
/// same treatment — see the GHSA-3ppv follow-up audit. It does not re-open this
/// disclosure: it only broadens a Version grant toward current reads, never the
/// reverse.
fn versioned_read_action(version_id: Option<&str>) -> Action {
    if version_id.is_some() {
        Action::S3Action(S3Action::GetObjectVersionAction)
    } else {
        Action::S3Action(S3Action::GetObjectAction)
    }
}

async fn get_or_fetch_object_tag_conditions<T>(
    req: &mut S3Request<T>,
    store: &ECStore,
    bucket: &str,
    object: &str,
    version_id: Option<&str>,
    action: Action,
) -> S3Result<HashMap<String, Vec<String>>> {
    if let Some(cached) = req.extensions.get::<ObjectTagConditions>()
        && cached.matches(bucket, object, version_id)
    {
        return Ok(cached.values.clone());
    }

    counter!("rustfs_object_tag_conditions_fetched_total", "op" => action_tag_metric_label(&action)).increment(1);
    let fetched = FS::get_object_tag_conditions_for_policy_from_store(store, bucket, object, version_id).await?;
    req.extensions
        .insert(ObjectTagConditions::new(bucket, object, version_id, fetched.clone()));
    Ok(fetched)
}

async fn maybe_merge_object_tag_conditions<T>(
    req: &mut S3Request<T>,
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
/// [`ReqInfo::suppress_denial_log`] so their routine denials log at `debug`
/// instead of `warn`.
struct DenialContext<'a> {
    quiet: bool,
    bucket: &'a str,
    object: &'a str,
    version_id: Option<&'a str>,
    account: Option<&'a str>,
    is_owner: bool,
}

impl DenialContext<'_> {
    fn log(&self, reason: &'static str, action: Action) {
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

    /// Build the generic AccessDenied response after recording the structured
    /// denial event.
    fn deny(&self, reason: &'static str, action: Action) -> S3Error {
        self.log(reason, action);
        s3_error!(AccessDenied, "Access Denied")
    }
}

/// Authorizes the request based on the action and credentials.
pub async fn authorize_request<T>(req: &mut S3Request<T>, action: Action) -> S3Result<()> {
    let internal_object_authorization = req.extensions.get::<InternalObjectAuthorization>().is_some();
    let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
    let req_info = req_info_ref(req)?;
    let cred = req_info.cred.clone();
    let is_owner = req_info.is_owner;
    let bucket = req_info.bucket.clone().unwrap_or_default();
    let object = req_info.object.clone().unwrap_or_default();
    let version_id = req_info.version_id.clone();
    let quiet_denial = req_info.suppress_denial_log;
    let store = request_object_store(req)?;
    let denial = DenialContext {
        quiet: quiet_denial,
        bucket: bucket.as_str(),
        object: object.as_str(),
        version_id: version_id.as_deref(),
        account: cred.as_ref().map(|c| c.access_key.as_str()),
        is_owner,
    };

    if let Some(cred) = &cred {
        let iam_store = request_iam_store(req)?;

        let default_claims = HashMap::new();
        let claims = cred.claims.as_ref().unwrap_or(&default_claims);
        let client_info = req.extensions.get::<ClientInfo>();
        let mut conditions = authorization_conditions(req, cred, version_id.as_deref(), None, remote_addr, client_info, action)?;

        let action_args = Args {
            account: &cred.access_key,
            groups: &cred.groups,
            action,
            bucket: bucket.as_str(),
            conditions: &conditions,
            is_owner,
            object: object.as_str(),
            claims,
            deny_only: false,
        };
        let prepared = iam_store.prepare_auth(&action_args).await;
        let mut needs_tag_from_iam = prepared.needs_existing_object_tag;

        let bucket_tag_hint = if !bucket.is_empty() && !object.is_empty() {
            Some(load_bucket_policy_existing_object_tag_hint(store.as_ref(), bucket.as_str(), action).await)
        } else {
            None
        };
        let mut needs_tag_from_bucket = if let Some(hint) = bucket_tag_hint.as_ref() {
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

        let secondary_action = secondary_tag_hint_action(action, version_id.as_deref());
        if let Some(extra_action) = secondary_action {
            let extra_args = Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action: extra_action,
                bucket: bucket.as_str(),
                conditions: &conditions,
                is_owner,
                object: object.as_str(),
                claims,
                deny_only: false,
            };
            needs_tag_from_iam |= prepared.needs_existing_object_tag_for_args(&extra_args).await;

            if let Some(hint) = bucket_tag_hint.as_ref() {
                let extra_bucket_args = BucketPolicyArgs {
                    bucket: bucket.as_str(),
                    action: extra_action,
                    is_owner,
                    account: cred.access_key.as_str(),
                    groups: &cred.groups,
                    conditions: &conditions,
                    object: object.as_str(),
                };
                needs_tag_from_bucket |= bucket_policy_needs_existing_object_tag_from_hint(hint, &extra_bucket_args).await;
            }
        }

        let needs_tag = needs_tag_from_iam || needs_tag_from_bucket;
        if needs_tag {
            tracing::debug!(
                bucket = %bucket,
                ?action,
                ?secondary_action,
                needs_tag_from_iam,
                needs_tag_from_bucket,
                "authorize_request ExistingObjectTag hint requires tag conditions"
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
            return Err(denial.deny("bucket_policy_explicit_deny", action));
        }

        if action == Action::S3Action(S3Action::DeleteObjectAction) && version_id.is_some() {
            let delete_version_args = Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action: Action::S3Action(S3Action::DeleteObjectVersionAction),
                bucket: bucket.as_str(),
                conditions: &conditions,
                is_owner,
                object: object.as_str(),
                claims,
                deny_only: false,
            };
            let delete_version_allowed = iam_store.eval_prepared(&prepared, &delete_version_args).await;
            if !delete_version_allowed
                && !PolicySys::try_is_allowed_for_store(
                    store.as_ref(),
                    &BucketPolicyArgs {
                        bucket: bucket.as_str(),
                        action: Action::S3Action(S3Action::DeleteObjectVersionAction),
                        is_owner,
                        account: &cred.access_key,
                        groups: &cred.groups,
                        conditions: &conditions,
                        object: object.as_str(),
                    },
                )
                .await
                .map_err(ApiError::from)?
            {
                return Err(denial.deny("delete_object_version_denied", Action::S3Action(S3Action::DeleteObjectVersionAction)));
            }
        }

        let iam_allowed = {
            let final_args = Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action,
                bucket: bucket.as_str(),
                conditions: &conditions,
                is_owner,
                object: object.as_str(),
                claims,
                deny_only: false,
            };
            iam_store.eval_prepared(&prepared, &final_args).await
        };

        if iam_allowed {
            if !internal_object_authorization {
                authorize_table_data_plane_if_needed(req, action, bucket.as_str(), object.as_str(), cred, is_owner, &conditions)
                    .await?;
            }
            return Ok(());
        }

        if action == Action::S3Action(S3Action::ListAllMyBucketsAction) {
            denial.log("iam_implicit_deny", action);
            return Err(ApiError::access_denied().into());
        }

        let policy_allowed_fallback = PolicySys::try_is_allowed_for_store(
            store.as_ref(),
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
        .map_err(ApiError::from)?;

        if policy_allowed_fallback {
            if !internal_object_authorization {
                authorize_table_data_plane_if_needed(req, action, bucket.as_str(), object.as_str(), cred, is_owner, &conditions)
                    .await?;
            }
            return Ok(());
        }

        if action == Action::S3Action(S3Action::ListBucketVersionsAction) {
            let list_bucket_args = Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action: Action::S3Action(S3Action::ListBucketAction),
                bucket: bucket.as_str(),
                conditions: &conditions,
                is_owner,
                object: object.as_str(),
                claims,
                deny_only: false,
            };
            let list_bucket_allowed = iam_store.eval_prepared(&prepared, &list_bucket_args).await;
            if list_bucket_allowed {
                return Ok(());
            }

            if PolicySys::try_is_allowed_for_store(
                store.as_ref(),
                &BucketPolicyArgs {
                    bucket: bucket.as_str(),
                    action: Action::S3Action(S3Action::ListBucketAction),
                    is_owner,
                    account: &cred.access_key,
                    groups: &cred.groups,
                    conditions: &conditions,
                    object: object.as_str(),
                },
            )
            .await
            .map_err(ApiError::from)?
            {
                return Ok(());
            }
        }
    } else {
        let default_cred = rustfs_credentials::Credentials::default();
        let client_info = req.extensions.get::<ClientInfo>();
        let mut conditions = authorization_conditions(
            req,
            &default_cred,
            version_id.as_deref(),
            req.region.clone(),
            remote_addr,
            client_info,
            action,
        )?;

        let no_groups: Option<Vec<String>> = None;
        let bucket_tag_hint = if !bucket.is_empty() && !object.is_empty() {
            Some(load_bucket_policy_existing_object_tag_hint(store.as_ref(), bucket.as_str(), action).await)
        } else {
            None
        };
        let mut needs_tag_from_bucket = if let Some(hint) = bucket_tag_hint.as_ref() {
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
        let secondary_action = secondary_tag_hint_action(action, version_id.as_deref());
        if let Some(extra_action) = secondary_action
            && let Some(hint) = bucket_tag_hint.as_ref()
        {
            let extra_bucket_args = BucketPolicyArgs {
                bucket: bucket.as_str(),
                action: extra_action,
                is_owner: false,
                account: "",
                groups: &no_groups,
                conditions: &conditions,
                object: object.as_str(),
            };
            needs_tag_from_bucket |= bucket_policy_needs_existing_object_tag_from_hint(hint, &extra_bucket_args).await;
        }
        if needs_tag_from_bucket {
            tracing::debug!(
                bucket = %bucket,
                ?action,
                ?secondary_action,
                "anonymous authorize_request ExistingObjectTag hint requires tag conditions"
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
            return Err(denial.deny("bucket_policy_explicit_deny", action));
        }

        if action != Action::S3Action(S3Action::ListAllMyBucketsAction) {
            if action == Action::S3Action(S3Action::DeleteObjectAction) && version_id.is_some() {
                let delete_version_allowed = PolicySys::try_is_allowed_for_store(
                    store.as_ref(),
                    &BucketPolicyArgs {
                        bucket: bucket.as_str(),
                        action: Action::S3Action(S3Action::DeleteObjectVersionAction),
                        is_owner: false,
                        account: "",
                        groups: &None,
                        conditions: &conditions,
                        object: object.as_str(),
                    },
                )
                .await
                .map_err(ApiError::from)?;
                if !delete_version_allowed {
                    return Err(
                        denial.deny("delete_object_version_denied", Action::S3Action(S3Action::DeleteObjectVersionAction))
                    );
                }
            }

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

            // A bucket policy granting s3:ListBucket also covers listing versions. This
            // fallback has to feed the same post-authorization gates as the direct grant
            // below, otherwise a public bucket keeps serving anonymous
            // ListObjectVersions after RestrictPublicBuckets is turned on.
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
                deny_anonymous_table_data_plane_if_needed(req, action, bucket.as_str(), object.as_str()).await?;
                // RestrictPublicBuckets: when true, deny public access even if bucket policy allows it.
                match store.restricts_public_bucket_access(bucket_name).await {
                    Ok(true) => return Err(denial.deny("restrict_public_buckets", action)),
                    Ok(false) => {}
                    Err(StorageError::ConfigNotFound) => {}
                    Err(_) => {
                        return Err(denial.deny("public_access_block_unavailable", action));
                    }
                }
                return Ok(());
            }
        }
    }

    Err(denial.deny("no_policy_allows_action", action))
}

// Multipart parts are staged and become visible only after CompleteMultipartUpload,
// which uses the normal publication-fenced authorization path.
async fn authorize_staged_multipart_part<T>(req: &mut S3Request<T>) -> S3Result<()> {
    req.extensions.insert(StagedMultipartPartAuthorization);
    let result = authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await;
    req.extensions.remove::<StagedMultipartPartAuthorization>();
    result
}

/// Check if the request has the x-amz-bypass-governance-retention header set to true
pub fn has_bypass_governance_header(headers: &http::HeaderMap) -> bool {
    headers
        .get(AMZ_OBJECT_LOCK_BYPASS_GOVERNANCE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn legal_hold_write_requested(object_lock_legal_hold_status: Option<&ObjectLockLegalHoldStatus>) -> bool {
    object_lock_legal_hold_status.is_some()
}

fn retention_write_requested(
    object_lock_mode: Option<&ObjectLockMode>,
    object_lock_retain_until_date: Option<&Timestamp>,
) -> bool {
    object_lock_mode.is_some() || object_lock_retain_until_date.is_some()
}

fn get_bucket_policy_authorize_action() -> Action {
    Action::S3Action(S3Action::GetBucketPolicyAction)
}

fn put_bucket_policy_authorize_action() -> Action {
    Action::S3Action(S3Action::PutBucketPolicyAction)
}

/// Both website-config handlers authorize through this one function so the
/// write side and the delete side cannot drift apart. RustFS has no dedicated
/// `s3:PutBucketWebsite` / `s3:DeleteBucketWebsite` action, so the bucket-config
/// mutation convention applies (same as `put_bucket_request_payment` and
/// `put_bucket_accelerate_configuration`).
fn bucket_website_config_authorize_action() -> Action {
    Action::S3Action(S3Action::PutBucketPolicyAction)
}

fn post_object_authorize_action() -> Action {
    Action::S3Action(S3Action::PutObjectAction)
}

fn complete_multipart_upload_authorize_action() -> Action {
    Action::S3Action(S3Action::PutObjectAction)
}

fn list_parts_authorize_action() -> Action {
    Action::S3Action(S3Action::ListMultipartUploadPartsAction)
}

fn table_data_plane_admin_action(action: Action) -> Option<AdminAction> {
    match action {
        Action::S3Action(
            S3Action::GetObjectAction
            | S3Action::GetObjectAclAction
            | S3Action::GetObjectVersionAction
            | S3Action::GetObjectAttributesAction
            | S3Action::GetObjectVersionAttributesAction
            | S3Action::GetObjectTaggingAction
            | S3Action::GetObjectVersionTaggingAction
            | S3Action::GetObjectRetentionAction
            | S3Action::GetObjectLegalHoldAction
            | S3Action::GetObjectVersionForReplicationAction,
        ) => Some(AdminAction::GetTableMetadataAction),
        Action::S3Action(
            S3Action::PutObjectAction
            | S3Action::PutObjectAclAction
            | S3Action::DeleteObjectAction
            | S3Action::DeleteObjectVersionAction
            | S3Action::PutObjectTaggingAction
            | S3Action::PutObjectVersionTaggingAction
            | S3Action::DeleteObjectTaggingAction
            | S3Action::DeleteObjectVersionTaggingAction
            | S3Action::PutObjectRetentionAction
            | S3Action::PutObjectLegalHoldAction
            | S3Action::BypassGovernanceRetentionAction
            | S3Action::AbortMultipartUploadAction
            | S3Action::ListMultipartUploadPartsAction
            | S3Action::RestoreObjectAction
            | S3Action::ReplicateObjectAction
            | S3Action::ReplicateDeleteAction
            | S3Action::ReplicateTagsAction
            | S3Action::PutObjectFanOutAction,
        ) => Some(AdminAction::SetTableMetadataAction),
        _ => None,
    }
}

fn table_data_plane_content_mutation(action: Action) -> bool {
    matches!(
        action,
        Action::S3Action(
            S3Action::PutObjectAction
                | S3Action::DeleteObjectAction
                | S3Action::DeleteObjectVersionAction
                | S3Action::RestoreObjectAction
                | S3Action::ReplicateObjectAction
                | S3Action::ReplicateDeleteAction
                | S3Action::PutObjectFanOutAction
        )
    )
}

fn table_data_plane_publication_fence_required<T>(req: &S3Request<T>, action: Action) -> bool {
    req.extensions.get::<StagedMultipartPartAuthorization>().is_none() && table_data_plane_content_mutation(action)
}

fn table_catalog_backend_for_data_plane<T>(
    req: &S3Request<T>,
) -> S3Result<crate::table_catalog::EcStoreTableCatalogObjectBackend<ECStore>> {
    let context = match req.extensions.get::<Arc<ServerContextSlot>>() {
        Some(server_ctx) => server_ctx.installed_app_context(),
        None => runtime_sources::current_app_context(),
    }
    .ok_or_else(object_store_not_initialized_error)?;
    Ok(crate::table_catalog::EcStoreTableCatalogObjectBackend::new_with_strong_runtime(
        context.object_store(),
        context.table_catalog_strong_runtime(),
    ))
}

fn table_catalog_store_for_data_plane<T>(
    req: &S3Request<T>,
) -> S3Result<crate::table_catalog::EcStoreTableCatalogStore<ECStore>> {
    let backend = table_catalog_backend_for_data_plane(req)?;
    crate::table_catalog::ConfiguredTableCatalogStore::from_env(backend)
        .map_err(|err| s3_error!(InternalError, "failed to configure table catalog backing: {}", err))
}

async fn retain_table_data_plane_publication_guard<T>(
    req: &mut S3Request<T>,
    table_bucket: &str,
    lock_object: &str,
) -> S3Result<()> {
    let key = (table_bucket.to_string(), lock_object.to_string());
    let retained = req
        .extensions
        .get::<TableDataPlanePublicationGuards>()
        .cloned()
        .unwrap_or_default();
    if retained.state.lock().keys.contains(&key) {
        return Ok(());
    }

    let backend = table_catalog_backend_for_data_plane(req)?;
    let guard = crate::table_catalog::TableCatalogObjectBackend::acquire_read_lock(&backend, table_bucket, lock_object)
        .await
        .map_err(|err| s3_error!(InternalError, "failed to acquire table publication guard: {}", err))?;
    let mut state = retained.state.lock();
    state.keys.insert(key);
    state.guards.push(Box::new(guard));
    drop(state);
    req.extensions.insert(retained);
    Ok(())
}

async fn retain_table_bucket_publication_guard<T>(req: &mut S3Request<T>, table_bucket: &str) -> S3Result<()> {
    retain_table_data_plane_publication_guard(
        req,
        table_bucket,
        &crate::table_catalog::default_table_bucket_publication_lock_path(),
    )
    .await
}

async fn retain_table_publication_guard<T>(
    req: &mut S3Request<T>,
    resource: &crate::table_catalog::TableDataPlaneResource,
) -> S3Result<()> {
    let namespace = crate::table_catalog::Namespace::parse(&resource.namespace)
        .map_err(|err| s3_error!(InternalError, "persisted table namespace is invalid: {}", err))?;
    let table = crate::table_catalog::IdentifierSegment::parse(resource.table.clone())
        .map_err(|err| s3_error!(InternalError, "persisted table name is invalid: {}", err))?;
    retain_table_data_plane_publication_guard(
        req,
        &resource.table_bucket,
        &crate::table_catalog::default_table_publication_lock_path(&namespace, &table),
    )
    .await?;
    let retained = req
        .extensions
        .get::<TableDataPlanePublicationGuards>()
        .cloned()
        .ok_or_else(|| s3_error!(InternalError, "table publication guard state is missing"))?;
    retained.state.lock().resources.insert(
        (resource.table_bucket.clone(), resource.warehouse_object_prefix.clone()),
        resource.clone(),
    );
    Ok(())
}

async fn table_bucket_enabled_for_data_plane<T>(req: &S3Request<T>, bucket: &str) -> S3Result<bool> {
    if bucket.is_empty() {
        return Ok(false);
    }

    match request_object_store(req)?.get_bucket_metadata(bucket).await {
        Ok(metadata) => Ok(metadata.table_bucket_enabled()),
        Err(StorageError::ConfigNotFound) => Ok(false),
        Err(err) if is_err_bucket_not_found(&err) => Ok(false),
        Err(err) => {
            tracing::warn!(
                bucket = %bucket,
                error = %err,
                "failed to load bucket metadata while authorizing table data-plane access"
            );
            Err(s3_error!(AccessDenied, "Access Denied"))
        }
    }
}

async fn table_data_plane_resource_for_request<T>(
    req: &mut S3Request<T>,
    bucket: &str,
    object: &str,
    table_bucket_enabled: bool,
) -> S3Result<Option<crate::table_catalog::TableDataPlaneResource>> {
    if !table_bucket_enabled || bucket.is_empty() || object.is_empty() {
        return Ok(None);
    }

    let key = (bucket.to_string(), object.to_string());
    let retained = req
        .extensions
        .get::<TableDataPlanePublicationGuards>()
        .cloned()
        .unwrap_or_default();
    {
        let state = retained.state.lock();
        if state.missing_resources.contains(&key) {
            return Ok(None);
        }
        if let Some(resource) = state
            .resources
            .values()
            .find(|resource| resource.table_bucket == bucket && object.starts_with(&resource.warehouse_object_prefix))
            .cloned()
        {
            return Ok(Some(resource));
        }
    }

    let store = table_catalog_store_for_data_plane(req)?;
    let resource = crate::table_catalog::table_data_plane_resource_for_object(&store, bucket, object)
        .await
        .map_err(|err| {
            tracing::warn!(
                bucket = %bucket,
                object = %object,
                error = %err,
                "failed to resolve table data-plane resource"
            );
            s3_error!(AccessDenied, "Access Denied")
        })?;
    let bucket_fence_key = (bucket.to_string(), crate::table_catalog::default_table_bucket_publication_lock_path());
    let mut state = retained.state.lock();
    if resource.is_none() && state.keys.contains(&bucket_fence_key) {
        state.missing_resources.insert(key);
        drop(state);
        req.extensions.insert(retained);
    }
    Ok(resource)
}

async fn table_data_plane_resource_for_authorization<T>(
    req: &mut S3Request<T>,
    bucket: &str,
    object: &str,
    table_bucket_enabled: bool,
) -> S3Result<Option<crate::table_catalog::TableDataPlaneResource>> {
    table_data_plane_resource_for_request(req, bucket, object, table_bucket_enabled).await
}

async fn authorize_table_data_plane_if_needed<T>(
    req: &mut S3Request<T>,
    action: Action,
    bucket: &str,
    object: &str,
    cred: &rustfs_credentials::Credentials,
    is_owner: bool,
    conditions: &HashMap<String, Vec<String>>,
) -> S3Result<()> {
    let Some(admin_action) = table_data_plane_admin_action(action) else {
        return Ok(());
    };
    if table_data_plane_publication_fence_required(req, action) {
        retain_table_bucket_publication_guard(req, bucket).await?;
    }
    let table_bucket_enabled = table_bucket_enabled_for_data_plane(req, bucket).await?;
    let Some(resource) = table_data_plane_resource_for_authorization(req, bucket, object, table_bucket_enabled).await? else {
        return Ok(());
    };
    let iam_store = request_iam_store(req)?;
    let default_claims = HashMap::new();
    let claims = cred.claims.as_ref().unwrap_or(&default_claims);

    let resource_object = resource.catalog_resource_object();
    let allowed = iam_store
        .is_allowed(&Args {
            account: &cred.access_key,
            groups: &cred.groups,
            action: Action::AdminAction(admin_action),
            bucket: resource.table_bucket.as_str(),
            conditions,
            is_owner,
            object: resource_object.as_str(),
            claims,
            deny_only: false,
        })
        .await;
    if allowed {
        if table_data_plane_publication_fence_required(req, action) {
            retain_table_publication_guard(req, &resource).await?;
        }
        return Ok(());
    }

    tracing::debug!(
        bucket = %bucket,
        object = %object,
        table_bucket = %resource.table_bucket,
        table_namespace = %resource.namespace,
        table = %resource.table,
        table_id = %resource.table_id,
        ?admin_action,
        "table data-plane access denied by table resource policy"
    );
    Err(s3_error!(AccessDenied, "Access Denied"))
}

/// Authorizes one exact object key after its enclosing table-catalog operation has been authorized.
pub(crate) async fn authorize_internal_object_request<T>(req: &mut S3Request<T>, action: Action) -> S3Result<()> {
    req.extensions.insert(InternalObjectAuthorization);
    let result = authorize_request(req, action).await;
    req.extensions.remove::<InternalObjectAuthorization>();
    result
}

async fn deny_anonymous_table_data_plane_if_needed<T>(
    req: &mut S3Request<T>,
    action: Action,
    bucket: &str,
    object: &str,
) -> S3Result<()> {
    if table_data_plane_admin_action(action).is_none() {
        return Ok(());
    }
    if table_data_plane_publication_fence_required(req, action) {
        retain_table_bucket_publication_guard(req, bucket).await?;
    }
    let table_bucket_enabled = table_bucket_enabled_for_data_plane(req, bucket).await?;
    if table_data_plane_resource_for_authorization(req, bucket, object, table_bucket_enabled)
        .await?
        .is_some()
    {
        return Err(s3_error!(AccessDenied, "Access Denied"));
    }
    Ok(())
}

fn validate_post_object_success_controls(input: &PostObjectInput) -> S3Result<()> {
    if let Some(status) = input.success_action_status
        && !matches!(status, 200 | 201 | 204)
    {
        return Err(s3_error!(MalformedPOSTRequest, "success_action_status must be one of 200, 201, or 204"));
    }

    if let Some(redirect) = input.success_action_redirect.as_deref().map(str::trim)
        && !redirect.is_empty()
        && Url::parse(redirect).is_err()
    {
        return Err(s3_error!(MalformedPOSTRequest, "success_action_redirect must be a valid absolute URL"));
    }

    Ok(())
}

#[async_trait::async_trait]
impl S3Access for FS {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        // Upper layer has verified ak/sk
        // info!(
        //     "s3 check uri: {:?}, method: {:?} path: {:?}, s3_op: {:?}, cred: {:?}, headers:{:?}",
        //     cx.uri(),
        //     cx.method(),
        //     cx.s3_path(),
        //     cx.s3_op().name(),
        //     cx.credentials(),
        //     cx.headers(),
        //     // cx.extensions_mut(),
        // );

        // Resolve auth against this server's context (backlog#1052 S6) so a
        // second embedded server validates keys against its own root identity
        // and IAM domain; the slot resolves the process default when it has
        // not been installed, keeping single-instance behavior unchanged.
        let app_context = self.server_ctx().app_context();
        let (cred, is_owner) = if let Some(input_cred) = cx.credentials() {
            let (cred, is_owner) = check_key_valid_with_context(
                get_session_token(cx.uri(), cx.headers()).unwrap_or_default(),
                &input_cred.access_key,
                app_context.as_deref(),
            )
            .await?;
            (Some(cred), is_owner)
        } else {
            (None, false)
        };

        let request_context = cx.extensions_mut().get::<RequestContext>().cloned();

        let region = app_context
            .as_deref()
            .and_then(|context| context.region().get())
            .or_else(runtime_sources::current_region);

        let req_info = ReqInfo {
            cred,
            is_owner,
            region,
            request_context,
            ..Default::default()
        };

        // Publish this server's context slot so downstream data-plane handlers
        // resolve the same store (backlog#1052 S6).
        let ext = cx.extensions_mut();
        ext.insert(self.server_ctx().clone());
        ext.insert(req_info);
        license_check().map_err(|er| match er.kind() {
            std::io::ErrorKind::PermissionDenied => s3_error!(AccessDenied, "{er}"),
            _ => {
                tracing::error!("license check failed due to unexpected error: {er}");
                s3_error!(InternalError, "License validation failed")
            }
        })?;

        // Verify uniformly here? Or verify separately below?

        Ok(())
    }

    /// Checks whether the CreateBucket request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn create_bucket(&self, req: &mut S3Request<CreateBucketInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::CreateBucketAction)).await?;

        if req.input.object_lock_enabled_for_bucket.is_some_and(|v| v) {
            authorize_request(req, Action::S3Action(S3Action::PutBucketObjectLockConfigurationAction)).await?;
            authorize_request(req, Action::S3Action(S3Action::PutBucketVersioningAction)).await?;
        }

        Ok(())
    }
    /// Checks whether the AbortMultipartUpload request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn abort_multipart_upload(&self, req: &mut S3Request<AbortMultipartUploadInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self, req, &bucket).await;
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());

        authorize_request(req, Action::S3Action(S3Action::AbortMultipartUploadAction)).await?;
        req.extensions.insert(bucket_generation?);
        Ok(())
    }

    /// Checks whether the CompleteMultipartUpload request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn complete_multipart_upload(&self, req: &mut S3Request<CompleteMultipartUploadInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self, req, &bucket).await;
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());

        authorize_request(req, complete_multipart_upload_authorize_action()).await?;
        authorize_replication_only_put_headers(req).await?;
        req.extensions.insert(bucket_generation?);
        Ok(())
    }

    /// Checks whether the CopyObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn copy_object(&self, req: &mut S3Request<CopyObjectInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self, req, &bucket).await;
        let (src_bucket, src_key, version_id) = match &req.input.copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Outpost { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket { bucket, key, version_id } => {
                (bucket.to_string(), key.to_string(), version_id.as_ref().map(|v| v.to_string()))
            }
        };
        let source_bucket_generation = load_copy_source_bucket_generation(self, &src_bucket).await;

        {
            let req_info = ext_req_info_mut(&mut req.extensions)?;
            req_info.bucket = Some(src_bucket.clone());
            req_info.object = Some(src_key.clone());
            req_info.version_id = version_id.clone();

            // GHSA-3ppv: a versioned copy source must authorize against
            // s3:GetObjectVersion, not s3:GetObject.
            authorize_request(req, versioned_read_action(version_id.as_deref())).await?;
        }
        req.extensions.insert(source_bucket_generation?);

        let req_info = ext_req_info_mut(&mut req.extensions)?;

        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await?;

        authorize_replication_only_put_headers(req).await?;

        if legal_hold_write_requested(req.input.object_lock_legal_hold_status.as_ref()) {
            authorize_request(req, Action::S3Action(S3Action::PutObjectLegalHoldAction)).await?;
        }

        if retention_write_requested(req.input.object_lock_mode.as_ref(), req.input.object_lock_retain_until_date.as_ref()) {
            authorize_request(req, Action::S3Action(S3Action::PutObjectRetentionAction)).await?;
        }

        req.extensions.insert(bucket_generation?);
        Ok(())
    }

    /// Checks whether the CreateMultipartUpload request has accesses to the resources.
    async fn create_multipart_upload(&self, req: &mut S3Request<CreateMultipartUploadInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self, req, &bucket).await;
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());

        authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await?;

        authorize_replication_only_put_headers(req).await?;

        if legal_hold_write_requested(req.input.object_lock_legal_hold_status.as_ref()) {
            authorize_request(req, Action::S3Action(S3Action::PutObjectLegalHoldAction)).await?;
        }

        if retention_write_requested(req.input.object_lock_mode.as_ref(), req.input.object_lock_retain_until_date.as_ref()) {
            authorize_request(req, Action::S3Action(S3Action::PutObjectRetentionAction)).await?;
        }

        req.extensions.insert(bucket_generation?);
        Ok(())
    }

    /// Checks whether the DeleteBucket request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket(&self, req: &mut S3Request<DeleteBucketInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::DeleteBucketAction)).await?;

        if req.input.force_delete.is_some_and(|v| v) {
            authorize_request(req, Action::S3Action(S3Action::ForceDeleteBucketAction)).await?;
        }
        Ok(())
    }

    /// Checks whether the DeleteBucketAnalyticsConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_analytics_configuration(
        &self,
        _req: &mut S3Request<DeleteBucketAnalyticsConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the DeleteBucketCors request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_cors(&self, req: &mut S3Request<DeleteBucketCorsInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, Action::S3Action(S3Action::DeleteBucketCorsAction)).await
    }

    /// Checks whether the DeleteBucketEncryption request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_encryption(&self, req: &mut S3Request<DeleteBucketEncryptionInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, Action::S3Action(S3Action::PutBucketEncryptionAction)).await
    }

    /// Checks whether the DeleteBucketIntelligentTieringConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_intelligent_tiering_configuration(
        &self,
        _req: &mut S3Request<DeleteBucketIntelligentTieringConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the DeleteBucketInventoryConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_inventory_configuration(
        &self,
        _req: &mut S3Request<DeleteBucketInventoryConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the DeleteBucketLifecycle request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_lifecycle(&self, req: &mut S3Request<DeleteBucketLifecycleInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, Action::S3Action(S3Action::PutBucketLifecycleAction)).await
    }

    /// Checks whether the DeleteBucketMetricsConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_metrics_configuration(
        &self,
        _req: &mut S3Request<DeleteBucketMetricsConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the DeleteBucketOwnershipControls request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_ownership_controls(&self, _req: &mut S3Request<DeleteBucketOwnershipControlsInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the DeleteBucketPolicy request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_policy(&self, req: &mut S3Request<DeleteBucketPolicyInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, Action::S3Action(S3Action::DeleteBucketPolicyAction)).await
    }

    /// Checks whether the DeleteBucketReplication request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_replication(&self, req: &mut S3Request<DeleteBucketReplicationInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, Action::S3Action(S3Action::PutReplicationConfigurationAction)).await
    }

    /// Checks whether the DeleteBucketTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_tagging(&self, req: &mut S3Request<DeleteBucketTaggingInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, Action::S3Action(S3Action::PutBucketTaggingAction)).await
    }

    /// Checks whether the DeleteBucketWebsite request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_website(&self, req: &mut S3Request<DeleteBucketWebsiteInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, bucket_website_config_authorize_action()).await
    }

    /// Checks whether the DeleteObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_object(&self, req: &mut S3Request<DeleteObjectInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self, req, &bucket).await;
        // Preserve DeleteObject's established NoSuchBucket response instead of
        // letting policy lookup turn a missing bucket into AccessDenied.
        if let Err(err) = &bucket_generation
            && err.code() == &S3ErrorCode::NoSuchBucket
        {
            return Err(S3Error::with_message(
                S3ErrorCode::NoSuchBucket,
                err.message().unwrap_or("The specified bucket does not exist").to_string(),
            ));
        }

        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();
        let is_owner = req_info.is_owner;

        authorize_request(req, Action::S3Action(S3Action::DeleteObjectAction)).await?;

        let replica_request = req
            .headers
            .get(AMZ_BUCKET_REPLICATION_STATUS)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value == ReplicationStatusType::Replica.as_str());
        if !recursive_force_delete_is_authorized(&req.headers, is_owner, replica_request) {
            return Err(s3_error!(
                AccessDenied,
                "Recursive force-delete is restricted to internal or administrative requests"
            ));
        }

        // S3 Standard: When bypass_governance header is set, must have s3:BypassGovernanceRetention permission
        if has_bypass_governance_header(&req.headers) {
            authorize_request(req, Action::S3Action(S3Action::BypassGovernanceRetentionAction)).await?;
        }

        req.extensions
            .insert(PendingDeleteBucketGenerationGuard::from_result(bucket_generation));

        Ok(())
    }

    /// Checks whether the DeleteObjectTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_object_tagging(&self, req: &mut S3Request<DeleteObjectTaggingInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::DeleteObjectTaggingAction)).await
    }

    /// Checks whether the DeleteObjects request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_objects(&self, req: &mut S3Request<DeleteObjectsInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = None;
        req_info.version_id = None;

        Ok(())
    }

    /// Checks whether the DeletePublicAccessBlock request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_public_access_block(&self, req: &mut S3Request<DeletePublicAccessBlockInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, Action::S3Action(S3Action::DeleteBucketPublicAccessBlockAction)).await
    }

    /// Checks whether the GetBucketAccelerateConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_accelerate_configuration(
        &self,
        req: &mut S3Request<GetBucketAccelerateConfigurationInput>,
    ) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketPolicyAction)).await
    }

    /// Checks whether the GetBucketAcl request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_acl(&self, req: &mut S3Request<GetBucketAclInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketAclAction)).await
    }

    /// Checks whether the GetBucketAnalyticsConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_analytics_configuration(
        &self,
        _req: &mut S3Request<GetBucketAnalyticsConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetBucketCors request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_cors(&self, req: &mut S3Request<GetBucketCorsInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketCorsAction)).await
    }

    /// Checks whether the GetBucketEncryption request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_encryption(&self, req: &mut S3Request<GetBucketEncryptionInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketEncryptionAction)).await
    }

    /// Checks whether the GetBucketIntelligentTieringConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_intelligent_tiering_configuration(
        &self,
        _req: &mut S3Request<GetBucketIntelligentTieringConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetBucketInventoryConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_inventory_configuration(
        &self,
        _req: &mut S3Request<GetBucketInventoryConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetBucketLifecycleConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_lifecycle_configuration(
        &self,
        req: &mut S3Request<GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketLifecycleAction)).await
    }

    /// Checks whether the GetBucketLocation request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_location(&self, req: &mut S3Request<GetBucketLocationInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketLocationAction)).await
    }

    /// Checks whether the GetBucketLogging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_logging(&self, req: &mut S3Request<GetBucketLoggingInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketLoggingAction)).await
    }

    /// Checks whether the GetBucketMetricsConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_metrics_configuration(&self, _req: &mut S3Request<GetBucketMetricsConfigurationInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetBucketNotificationConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_notification_configuration(
        &self,
        req: &mut S3Request<GetBucketNotificationConfigurationInput>,
    ) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketNotificationAction)).await
    }

    /// Checks whether the GetBucketOwnershipControls request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_ownership_controls(&self, _req: &mut S3Request<GetBucketOwnershipControlsInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetBucketPolicy request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_policy(&self, req: &mut S3Request<GetBucketPolicyInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, get_bucket_policy_authorize_action()).await
    }

    /// Checks whether the GetBucketPolicyStatus request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_policy_status(&self, req: &mut S3Request<GetBucketPolicyStatusInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketPolicyStatusAction)).await
    }

    /// Checks whether the GetBucketReplication request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_replication(&self, req: &mut S3Request<GetBucketReplicationInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetReplicationConfigurationAction)).await
    }

    /// Checks whether the GetBucketRequestPayment request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_request_payment(&self, req: &mut S3Request<GetBucketRequestPaymentInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketPolicyAction)).await
    }

    /// Checks whether the GetBucketTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_tagging(&self, req: &mut S3Request<GetBucketTaggingInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketTaggingAction)).await
    }

    /// Checks whether the GetBucketVersioning request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_versioning(&self, req: &mut S3Request<GetBucketVersioningInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketVersioningAction)).await
    }

    /// Checks whether the GetBucketWebsite request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_website(&self, req: &mut S3Request<GetBucketWebsiteInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketPolicyAction)).await
    }

    /// Checks whether the GetObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object(&self, req: &mut S3Request<GetObjectInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        // GHSA-3ppv: a versioned read (?versionId=...) must authorize against
        // s3:GetObjectVersion, not s3:GetObject.
        authorize_request(req, versioned_read_action(req.input.version_id.as_deref())).await
    }

    /// Checks whether the GetObjectAcl request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_acl(&self, req: &mut S3Request<GetObjectAclInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::GetObjectAclAction)).await
    }

    /// Checks whether the GetObjectAttributes request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_attributes(&self, req: &mut S3Request<GetObjectAttributesInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        if req.input.version_id.is_some() {
            authorize_request(req, Action::S3Action(S3Action::GetObjectVersionAttributesAction)).await?;
            authorize_request(req, Action::S3Action(S3Action::GetObjectVersionAction)).await?;
        } else {
            authorize_request(req, Action::S3Action(S3Action::GetObjectAttributesAction)).await?;
            authorize_request(req, Action::S3Action(S3Action::GetObjectAction)).await?;
        }

        Ok(())
    }

    /// Checks whether the GetObjectLegalHold request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_legal_hold(&self, req: &mut S3Request<GetObjectLegalHoldInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::GetObjectLegalHoldAction)).await
    }

    /// Checks whether the GetObjectLockConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_lock_configuration(&self, req: &mut S3Request<GetObjectLockConfigurationInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketObjectLockConfigurationAction)).await
    }

    /// Checks whether the GetObjectRetention request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_retention(&self, req: &mut S3Request<GetObjectRetentionInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::GetObjectRetentionAction)).await
    }

    /// Checks whether the GetObjectTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_tagging(&self, req: &mut S3Request<GetObjectTaggingInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::GetObjectTaggingAction)).await
    }

    /// Checks whether the GetObjectTorrent request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_torrent(&self, _req: &mut S3Request<GetObjectTorrentInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetPublicAccessBlock request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_public_access_block(&self, req: &mut S3Request<GetPublicAccessBlockInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketPublicAccessBlockAction)).await
    }

    /// Checks whether the HeadBucket request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn head_bucket(&self, req: &mut S3Request<HeadBucketInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::ListBucketAction)).await
    }

    /// Checks whether the HeadObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn head_object(&self, req: &mut S3Request<HeadObjectInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        // A replication convergence check HEADs the replica to compare
        // etag/size/mtime. For SSE-C replicas the worker holds no customer key,
        // so authorize it as a replication action and let the handler skip the
        // SSE-C read validation.
        if get_header(&req.headers, SUFFIX_SOURCE_REPLICATION_CHECK).as_deref() == Some("true") {
            authorize_request(req, Action::S3Action(S3Action::ReplicateObjectAction)).await?;
            req_info_mut(req)?.replication_request_authorized = true;
            return Ok(());
        }

        authorize_request(req, Action::S3Action(S3Action::GetObjectAction)).await
    }

    /// Checks whether the ListBucketAnalyticsConfigurations request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_bucket_analytics_configurations(
        &self,
        _req: &mut S3Request<ListBucketAnalyticsConfigurationsInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the ListBucketIntelligentTieringConfigurations request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_bucket_intelligent_tiering_configurations(
        &self,
        _req: &mut S3Request<ListBucketIntelligentTieringConfigurationsInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the ListBucketInventoryConfigurations request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_bucket_inventory_configurations(
        &self,
        _req: &mut S3Request<ListBucketInventoryConfigurationsInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the ListBucketMetricsConfigurations request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_bucket_metrics_configurations(
        &self,
        _req: &mut S3Request<ListBucketMetricsConfigurationsInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the ListBuckets request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_buckets(&self, _req: &mut S3Request<ListBucketsInput>) -> S3Result<()> {
        // check inside
        Ok(())
    }

    /// Checks whether the ListMultipartUploads request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_multipart_uploads(&self, req: &mut S3Request<ListMultipartUploadsInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self, req, &bucket).await;
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::ListBucketMultipartUploadsAction)).await?;
        req.extensions.insert(bucket_generation?);
        Ok(())
    }

    /// Checks whether the `ListObjectVersions` request is authorized for the requested bucket.
    ///
    /// Returns `Ok(())` if the request is allowed, or an error if access is denied or another
    /// authorization-related issue occurs.
    async fn list_object_versions(&self, req: &mut S3Request<ListObjectVersionsInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        authorize_request(req, Action::S3Action(S3Action::ListBucketVersionsAction)).await
    }

    /// Checks whether the ListObjects request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_objects(&self, req: &mut S3Request<ListObjectsInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::ListBucketAction)).await
    }

    /// Checks whether the ListObjectsV2 request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_objects_v2(&self, req: &mut S3Request<ListObjectsV2Input>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::ListBucketAction)).await
    }

    /// Checks whether the ListParts request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_parts(&self, req: &mut S3Request<ListPartsInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self, req, &bucket).await;
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());

        authorize_request(req, list_parts_authorize_action()).await?;
        req.extensions.insert(bucket_generation?);
        Ok(())
    }

    /// Checks whether the PostObject request has accesses to the resources.
    async fn post_object(&self, req: &mut S3Request<PostObjectInput>) -> S3Result<()> {
        validate_post_object_success_controls(&req.input)?;

        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();
        req.extensions.insert(PostObjectRequestMarker);

        authorize_request(req, post_object_authorize_action()).await
    }

    /// Checks whether the PutBucketAccelerateConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_accelerate_configuration(
        &self,
        req: &mut S3Request<PutBucketAccelerateConfigurationInput>,
    ) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, Action::S3Action(S3Action::PutBucketPolicyAction)).await
    }

    /// Checks whether the PutBucketAcl request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_acl(&self, req: &mut S3Request<PutBucketAclInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketAclAction)).await
    }

    /// Checks whether the PutBucketAnalyticsConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_analytics_configuration(
        &self,
        _req: &mut S3Request<PutBucketAnalyticsConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutBucketCors request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_cors(&self, req: &mut S3Request<PutBucketCorsInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, Action::S3Action(S3Action::PutBucketCorsAction)).await
    }

    /// Checks whether the PutBucketEncryption request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_encryption(&self, req: &mut S3Request<PutBucketEncryptionInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, Action::S3Action(S3Action::PutBucketEncryptionAction)).await
    }

    /// Checks whether the PutBucketIntelligentTieringConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_intelligent_tiering_configuration(
        &self,
        _req: &mut S3Request<PutBucketIntelligentTieringConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutBucketInventoryConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_inventory_configuration(
        &self,
        _req: &mut S3Request<PutBucketInventoryConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutBucketLifecycleConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_lifecycle_configuration(
        &self,
        req: &mut S3Request<PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, Action::S3Action(S3Action::PutBucketLifecycleAction)).await
    }

    async fn put_bucket_logging(&self, req: &mut S3Request<PutBucketLoggingInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, Action::S3Action(S3Action::PutBucketLoggingAction)).await
    }

    /// Checks whether the PutBucketMetricsConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_metrics_configuration(&self, _req: &mut S3Request<PutBucketMetricsConfigurationInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutBucketNotificationConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_notification_configuration(
        &self,
        req: &mut S3Request<PutBucketNotificationConfigurationInput>,
    ) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, Action::S3Action(S3Action::PutBucketNotificationAction)).await
    }

    /// Checks whether the PutBucketOwnershipControls request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_ownership_controls(&self, _req: &mut S3Request<PutBucketOwnershipControlsInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutBucketPolicy request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_policy(&self, req: &mut S3Request<PutBucketPolicyInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, put_bucket_policy_authorize_action()).await
    }

    /// Checks whether the PutBucketReplication request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_replication(&self, req: &mut S3Request<PutBucketReplicationInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, Action::S3Action(S3Action::PutReplicationConfigurationAction)).await
    }

    /// Checks whether the PutBucketRequestPayment request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_request_payment(&self, req: &mut S3Request<PutBucketRequestPaymentInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, Action::S3Action(S3Action::PutBucketPolicyAction)).await
    }

    /// Checks whether the PutBucketTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_tagging(&self, req: &mut S3Request<PutBucketTaggingInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, Action::S3Action(S3Action::PutBucketTaggingAction)).await
    }

    /// Checks whether the PutBucketVersioning request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_versioning(&self, req: &mut S3Request<PutBucketVersioningInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, Action::S3Action(S3Action::PutBucketVersioningAction)).await
    }

    /// Checks whether the PutBucketWebsite request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_website(&self, req: &mut S3Request<PutBucketWebsiteInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, bucket_website_config_authorize_action()).await
    }

    /// Checks whether the PutObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object(&self, req: &mut S3Request<PutObjectInput>) -> S3Result<()> {
        crate::hp_guard!("S3Access::put_object");
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        if has_write_offset_bytes_header(&req.headers) {
            return Err(S3Error::with_message(
                S3ErrorCode::NotImplemented,
                ApiError::error_code_to_message(&S3ErrorCode::NotImplemented),
            ));
        }

        let bucket = req.input.bucket.clone();
        // Snapshot before authorization, but preserve AccessDenied precedence
        // by exposing any bucket-state error only after authorization succeeds.
        let bucket_generation = load_bucket_generation(self, req, &bucket).await;
        authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await?;
        req.extensions.insert(bucket_generation?);

        if req.method != http::Method::POST {
            authorize_replication_only_put_headers(req).await?;
        }

        if legal_hold_write_requested(req.input.object_lock_legal_hold_status.as_ref()) {
            authorize_request(req, Action::S3Action(S3Action::PutObjectLegalHoldAction)).await?;
        }

        if retention_write_requested(req.input.object_lock_mode.as_ref(), req.input.object_lock_retain_until_date.as_ref()) {
            authorize_request(req, Action::S3Action(S3Action::PutObjectRetentionAction)).await?;
        }

        Ok(())
    }

    /// Checks whether the PutObjectAcl request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_acl(&self, req: &mut S3Request<PutObjectAclInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::PutObjectAclAction)).await
    }

    /// Checks whether the PutObjectLegalHold request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_legal_hold(&self, req: &mut S3Request<PutObjectLegalHoldInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self, req, &bucket).await;
        authorize_request(req, Action::S3Action(S3Action::PutObjectLegalHoldAction)).await?;
        req.extensions.insert(bucket_generation?);
        Ok(())
    }

    /// Checks whether the PutObjectLockConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_lock_configuration(&self, req: &mut S3Request<PutObjectLockConfigurationInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, Action::S3Action(S3Action::PutBucketObjectLockConfigurationAction)).await
    }

    /// Checks whether the PutObjectRetention request has accesses to the resources.
    async fn put_object_retention(&self, req: &mut S3Request<PutObjectRetentionInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self, req, &bucket).await;
        authorize_request(req, Action::S3Action(S3Action::PutObjectRetentionAction)).await?;

        // S3 Standard: When bypass_governance header is set, must have s3:BypassGovernanceRetention permission
        if has_bypass_governance_header(&req.headers) {
            authorize_request(req, Action::S3Action(S3Action::BypassGovernanceRetentionAction)).await?;
        }

        req.extensions.insert(bucket_generation?);
        Ok(())
    }

    /// Checks whether the PutObjectTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_tagging(&self, req: &mut S3Request<PutObjectTaggingInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::PutObjectTaggingAction)).await
    }

    /// Checks whether the PutPublicAccessBlock request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_public_access_block(&self, req: &mut S3Request<PutPublicAccessBlockInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_config_mutation(self, req, Action::S3Action(S3Action::PutBucketPublicAccessBlockAction)).await
    }

    /// Checks whether the RestoreObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn restore_object(&self, req: &mut S3Request<RestoreObjectInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self, req, &bucket).await;
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::RestoreObjectAction)).await?;
        #[cfg(test)]
        wait_for_restore_authorization_test_hook(&bucket).await;
        req.extensions.insert(bucket_generation?);
        Ok(())
    }

    /// Checks whether the SelectObjectContent request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn select_object_content(&self, req: &mut S3Request<SelectObjectContentInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());

        authorize_request(req, Action::S3Action(S3Action::GetObjectAction)).await
    }

    /// Checks whether the UploadPart request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn upload_part(&self, req: &mut S3Request<UploadPartInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self, req, &bucket).await;
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());

        authorize_staged_multipart_part(req).await?;
        req.extensions.insert(bucket_generation?);
        Ok(())
    }

    /// Checks whether the UploadPartCopy request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn upload_part_copy(&self, req: &mut S3Request<UploadPartCopyInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self, req, &bucket).await;
        let (src_bucket, src_key, version_id) = match &req.input.copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Outpost { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket { bucket, key, version_id } => {
                (bucket.to_string(), key.to_string(), version_id.as_ref().map(|v| v.to_string()))
            }
        };
        let source_bucket_generation = load_copy_source_bucket_generation(self, &src_bucket).await;

        {
            let req_info = ext_req_info_mut(&mut req.extensions)?;
            req_info.bucket = Some(src_bucket.clone());
            req_info.object = Some(src_key.clone());
            req_info.version_id = version_id.clone();

            // GHSA-3ppv: a versioned copy source must authorize against
            // s3:GetObjectVersion, not s3:GetObject.
            authorize_request(req, versioned_read_action(version_id.as_deref())).await?;
        }
        req.extensions.insert(source_bucket_generation?);

        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = None;

        authorize_staged_multipart_part(req).await?;
        req.extensions.insert(bucket_generation?);
        Ok(())
    }

    /// Checks whether the WriteGetObjectResponse request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn write_get_object_response(&self, _req: &mut S3Request<WriteGetObjectResponseInput>) -> S3Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::{
        AMZ_WRITE_OFFSET_BYTES_HEADER, BucketGenerationGuard, BucketPolicyArgs, BucketPolicyExistingObjectTagHint,
        BucketPolicyRawLoadErrorKind, DenialContext, FS, InternalObjectAuthorization, ObjectTagConditions,
        PostObjectRequestMarker, ReqInfo, S3Access, StorageError, TableDataPlanePublicationGuards, apply_bucket_generation_guard,
        apply_copy_source_bucket_generation_guard, authorization_conditions, bucket_policy_needs_existing_object_tag_from_hint,
        bucket_website_config_authorize_action, classify_bucket_policy_raw_load_error,
        complete_multipart_upload_authorize_action, get_bucket_policy_authorize_action, has_write_offset_bytes_header,
        install_restore_authorization_test_hook, legal_hold_write_requested, list_parts_authorize_action,
        load_bucket_policy_existing_object_tag_hint, maybe_merge_object_tag_conditions, merge_list_bucket_query_conditions,
        merge_request_object_tag_conditions, owner_can_bypass_policy_deny, post_object_authorize_action,
        put_bucket_policy_authorize_action, request_context_from_req, request_object_store, retention_write_requested,
        secondary_tag_hint_action, table_data_plane_admin_action, table_data_plane_content_mutation,
        table_data_plane_resource_for_request, validate_post_object_success_controls, versioned_read_action,
    };
    use crate::error::ApiError;
    use crate::storage::storage_api::contract::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};
    use crate::storage::storage_api::contract::multipart::MultipartOperations as _;
    use crate::storage::storage_api::contract::object::{ObjectIO as _, ObjectOperations as _};
    use crate::storage::storage_api::runtime_sources_consumer::{AppContext, IamInterface, KmsInterface, ServerContextSlot};
    use http::{Extensions, HeaderMap, HeaderValue, Method, Uri};
    use rustfs_iam::{store::object::ObjectStore, sys::IamSys};
    use rustfs_kms::KmsServiceManager;
    use rustfs_policy::policy::action::{Action, S3Action};
    use rustfs_policy::policy::{BucketPolicy, bucket_policy_uses_existing_object_tag_conditions};
    use s3s::{S3ErrorCode, S3Request, dto::*};
    use serial_test::serial;
    use std::{collections::HashMap, sync::Arc};
    use time::OffsetDateTime;

    struct UnreadyIam;

    impl IamInterface for UnreadyIam {
        fn handle(&self) -> Arc<IamSys<ObjectStore>> {
            panic!("bucket generation guard tests do not resolve IAM")
        }

        fn is_ready(&self) -> bool {
            false
        }
    }

    struct TestKms;

    impl KmsInterface for TestKms {
        fn handle(&self) -> Arc<KmsServiceManager> {
            Arc::new(KmsServiceManager::new())
        }
    }

    fn build_request<T>(input: T, method: Method) -> S3Request<T> {
        S3Request {
            input,
            method,
            uri: Uri::from_static("/"),
            headers: HeaderMap::new(),
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn ensure_req_info<T>(req: &mut S3Request<T>) {
        req.extensions.insert(ReqInfo::default());
    }

    #[test]
    fn get_bucket_policy_uses_get_bucket_policy_action() {
        assert_eq!(get_bucket_policy_authorize_action(), Action::S3Action(S3Action::GetBucketPolicyAction));
    }

    #[test]
    fn ghsa_3ppv_versioned_read_selects_get_object_version_action() {
        // Regression (GHSA-3ppv): a read that names an explicit version must
        // authorize against s3:GetObjectVersion so that a principal holding only
        // s3:GetObject cannot read historical versions. Applies to GetObject and
        // the CopyObject / UploadPartCopy sources.
        assert_eq!(
            versioned_read_action(Some("0194e0f1-0000-7000-8000-000000000000")),
            Action::S3Action(S3Action::GetObjectVersionAction),
            "an explicit versionId must require GetObjectVersion"
        );
        assert_eq!(
            versioned_read_action(Some("null")),
            Action::S3Action(S3Action::GetObjectVersionAction),
            "the sentinel `null` version is still an explicit version selector"
        );
        assert_eq!(
            versioned_read_action(None),
            Action::S3Action(S3Action::GetObjectAction),
            "a current-object read (no versionId) authorizes against GetObject"
        );
    }

    #[test]
    fn put_bucket_policy_uses_put_bucket_policy_action() {
        assert_eq!(put_bucket_policy_authorize_action(), Action::S3Action(S3Action::PutBucketPolicyAction));
    }

    #[test]
    fn bucket_website_config_never_authorizes_through_a_read_action() {
        let action = bucket_website_config_authorize_action();

        // The regression: DeleteBucketWebsite permanently removes the persisted
        // website configuration but authorized through s3:GetBucketPolicy, so a
        // read-only "may read my bucket policy" grant was enough to destroy it.
        assert_ne!(action, Action::S3Action(S3Action::GetBucketPolicyAction));
        assert_eq!(action, Action::S3Action(S3Action::PutBucketPolicyAction));
    }

    #[test]
    fn post_object_uses_put_object_action() {
        assert_eq!(post_object_authorize_action(), Action::S3Action(S3Action::PutObjectAction));
    }

    #[test]
    fn complete_multipart_upload_uses_put_object_action() {
        assert_eq!(complete_multipart_upload_authorize_action(), Action::S3Action(S3Action::PutObjectAction));
    }

    #[test]
    fn list_parts_uses_list_multipart_upload_parts_action() {
        assert_eq!(list_parts_authorize_action(), Action::S3Action(S3Action::ListMultipartUploadPartsAction));
    }

    #[test]
    fn table_data_plane_admin_action_maps_s3_object_operations() {
        assert_eq!(
            table_data_plane_admin_action(Action::S3Action(S3Action::GetObjectAction)),
            Some(rustfs_policy::policy::action::AdminAction::GetTableMetadataAction)
        );
        assert_eq!(
            table_data_plane_admin_action(Action::S3Action(S3Action::PutObjectAction)),
            Some(rustfs_policy::policy::action::AdminAction::SetTableMetadataAction)
        );
        assert_eq!(
            table_data_plane_admin_action(Action::S3Action(S3Action::PutObjectTaggingAction)),
            Some(rustfs_policy::policy::action::AdminAction::SetTableMetadataAction)
        );
        assert_eq!(
            table_data_plane_admin_action(Action::S3Action(S3Action::DeleteObjectAction)),
            Some(rustfs_policy::policy::action::AdminAction::SetTableMetadataAction)
        );
        assert_eq!(
            table_data_plane_admin_action(Action::S3Action(S3Action::GetObjectTaggingAction)),
            Some(rustfs_policy::policy::action::AdminAction::GetTableMetadataAction)
        );
        assert_eq!(table_data_plane_admin_action(Action::S3Action(S3Action::ListBucketAction)), None);
    }

    #[test]
    fn table_data_plane_publication_guard_covers_content_mutations_only() {
        for action in [
            S3Action::PutObjectAction,
            S3Action::DeleteObjectAction,
            S3Action::DeleteObjectVersionAction,
            S3Action::RestoreObjectAction,
            S3Action::ReplicateObjectAction,
            S3Action::ReplicateDeleteAction,
            S3Action::PutObjectFanOutAction,
        ] {
            assert!(table_data_plane_content_mutation(Action::S3Action(action)));
        }
        for action in [
            S3Action::GetObjectAction,
            S3Action::PutObjectTaggingAction,
            S3Action::DeleteObjectTaggingAction,
            S3Action::ListBucketAction,
        ] {
            assert!(!table_data_plane_content_mutation(Action::S3Action(action)));
        }
    }

    #[test]
    fn table_data_plane_mutations_fence_before_table_bucket_marker_lookup() {
        let source = include_str!("access.rs");
        for (function, end_marker) in [
            ("async fn authorize_table_data_plane_if_needed", "/// Authorizes one exact object key"),
            (
                "async fn deny_anonymous_table_data_plane_if_needed",
                "fn validate_post_object_success_controls",
            ),
        ] {
            let start = source.find(function).expect("authorization helper should exist");
            let end = source[start..]
                .find(end_marker)
                .map(|offset| start + offset)
                .expect("authorization helper boundary should exist");
            let block = &source[start..end];
            let fence = block
                .find("retain_table_bucket_publication_guard(req, bucket).await?;")
                .expect("content mutation should retain the bucket publication fence");
            let marker = block
                .find("table_bucket_enabled_for_data_plane(req, bucket).await?")
                .expect("authorization should load the table bucket marker");
            assert!(fence < marker, "{function} must fence pre-enable writers before reading the marker");
        }
    }

    #[tokio::test]
    #[serial]
    async fn multipart_parts_skip_table_publication_fence_but_completion_retains_it() {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let server_ctx = ServerContextSlot::new();
        assert!(server_ctx.install(Arc::new(AppContext::new(Arc::clone(&store), Arc::new(UnreadyIam), Arc::new(TestKms),))));
        let fs = FS::with_server_ctx(server_ctx);
        let bucket = format!("multipart-table-fence-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("test bucket should be created");
        let policy_json = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:GetObject","s3:PutObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
        );
        let mut metadata = (*crate::storage::get_bucket_metadata(&bucket)
            .await
            .expect("new bucket metadata should be cached"))
        .clone();
        metadata.policy_config = Some(serde_json::from_str(&policy_json).expect("test bucket policy should parse"));
        metadata.policy_config_json = policy_json.into_bytes();
        crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
            .await
            .expect("test bucket policy should be published");

        let mut part_req = build_request(
            UploadPartInput::builder()
                .bucket(bucket.clone())
                .key("object".to_string())
                .upload_id("upload-id".to_string())
                .part_number(1)
                .build()
                .expect("upload part input should build"),
            Method::PUT,
        );
        ensure_req_info(&mut part_req);
        part_req.extensions.insert(fs.server_ctx().clone());
        fs.upload_part(&mut part_req)
            .await
            .expect("anonymous UploadPart should be authorized by the test policy");
        assert!(
            part_req.extensions.get::<TableDataPlanePublicationGuards>().is_none(),
            "staged part data must not retain a table publication fence"
        );

        let mut copy_part_req = build_request(
            UploadPartCopyInput::builder()
                .bucket(bucket.clone())
                .key("object".to_string())
                .upload_id("upload-id".to_string())
                .part_number(2)
                .copy_source(CopySource::Bucket {
                    bucket: bucket.clone().into(),
                    key: "source".into(),
                    version_id: None,
                })
                .build()
                .expect("upload part copy input should build"),
            Method::PUT,
        );
        ensure_req_info(&mut copy_part_req);
        copy_part_req.extensions.insert(fs.server_ctx().clone());
        fs.upload_part_copy(&mut copy_part_req)
            .await
            .expect("anonymous UploadPartCopy should be authorized by the test policy");
        assert!(
            copy_part_req.extensions.get::<TableDataPlanePublicationGuards>().is_none(),
            "staged copied part data must not retain a table publication fence"
        );

        let mut complete_req = build_request(
            CompleteMultipartUploadInput::builder()
                .bucket(bucket.clone())
                .key("object".to_string())
                .upload_id("upload-id".to_string())
                .multipart_upload(Some(CompletedMultipartUpload::default()))
                .build()
                .expect("complete multipart input should build"),
            Method::POST,
        );
        ensure_req_info(&mut complete_req);
        complete_req.extensions.insert(fs.server_ctx().clone());
        fs.complete_multipart_upload(&mut complete_req)
            .await
            .expect("anonymous CompleteMultipartUpload should be authorized by the test policy");
        let bucket_fence = (bucket, crate::table_catalog::default_table_bucket_publication_lock_path());
        let guards = complete_req
            .extensions
            .get::<TableDataPlanePublicationGuards>()
            .expect("completion should retain a table publication fence");
        assert!(guards.state.lock().keys.contains(&bucket_fence));
    }

    #[tokio::test]
    async fn table_data_plane_request_reuses_table_resource_for_distinct_object_while_commit_waits() {
        let resource = crate::table_catalog::TableDataPlaneResource {
            table_bucket: "warehouse".to_string(),
            namespace: "analytics".to_string(),
            table: "events".to_string(),
            table_id: "table-id".to_string(),
            warehouse_object_prefix: "tables/table-id/".to_string(),
        };
        let cached_key = ("warehouse".to_string(), "tables/table-id/".to_string());
        let requested_key = ("warehouse".to_string(), "tables/table-id/data/part-00002.parquet".to_string());
        let retained = TableDataPlanePublicationGuards::default();
        retained.state.lock().resources.insert(cached_key, resource.clone());

        let publication_lock = Arc::new(tokio::sync::Mutex::new(()));
        let request_publication_guard = Arc::clone(&publication_lock).lock_owned().await;
        retained.state.lock().guards.push(Box::new(request_publication_guard));
        let mut req = build_request((), Method::PUT);
        req.extensions.insert(retained.clone());

        let catalog_lock = Arc::new(tokio::sync::Mutex::new(()));
        let commit_started = Arc::new(tokio::sync::Notify::new());
        let commit_catalog_lock = Arc::clone(&catalog_lock);
        let commit_publication_lock = Arc::clone(&publication_lock);
        let commit_started_signal = Arc::clone(&commit_started);
        let commit = tokio::spawn(async move {
            let _catalog_guard = commit_catalog_lock.lock().await;
            commit_started_signal.notify_one();
            let _publication_guard = commit_publication_lock.lock().await;
        });
        commit_started.notified().await;

        let resolved = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            table_data_plane_resource_for_request(&mut req, &requested_key.0, &requested_key.1, true),
        )
        .await
        .expect("cached table resolution must not wait for the catalog lock")
        .expect("cached table resolution should succeed");
        assert_eq!(resolved, Some(resource));

        retained.state.lock().guards.clear();
        tokio::time::timeout(std::time::Duration::from_secs(1), commit)
            .await
            .expect("commit should continue after the request releases its publication guard")
            .expect("commit task should join");
    }

    #[tokio::test]
    async fn table_data_plane_request_reuses_absence_while_bucket_publication_guard_is_held() {
        let key = ("warehouse".to_string(), "tables/new-table/data/file.parquet".to_string());
        let bucket_fence = (key.0.clone(), crate::table_catalog::default_table_bucket_publication_lock_path());
        let retained = TableDataPlanePublicationGuards::default();
        {
            let mut state = retained.state.lock();
            state.keys.insert(bucket_fence);
            state.missing_resources.insert(key.clone());
        }

        let publication_lock = Arc::new(tokio::sync::Mutex::new(()));
        let request_publication_guard = Arc::clone(&publication_lock).lock_owned().await;
        retained.state.lock().guards.push(Box::new(request_publication_guard));
        let mut req = build_request((), Method::PUT);
        req.extensions.insert(retained.clone());

        let commit_publication_lock = Arc::clone(&publication_lock);
        let commit = tokio::spawn(async move {
            let _publication_guard = commit_publication_lock.lock().await;
        });
        tokio::task::yield_now().await;

        let resolved = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            table_data_plane_resource_for_request(&mut req, &key.0, &key.1, true),
        )
        .await
        .expect("cached absence must not reread the catalog while publication waits")
        .expect("cached absence should resolve successfully");
        assert!(resolved.is_none());

        retained.state.lock().guards.clear();
        tokio::time::timeout(std::time::Duration::from_secs(1), commit)
            .await
            .expect("publication should continue after the request releases its bucket guard")
            .expect("publication task should join");
    }

    #[test]
    fn legal_hold_write_requested_is_true_when_status_present() {
        assert!(legal_hold_write_requested(Some(&ObjectLockLegalHoldStatus::from_static(
            ObjectLockLegalHoldStatus::ON
        ))));
        assert!(!legal_hold_write_requested(None));
    }

    #[test]
    fn retention_write_requested_is_true_when_mode_or_date_present() {
        let retain_until = OffsetDateTime::now_utc().into();

        assert!(retention_write_requested(
            Some(&ObjectLockMode::from_static(ObjectLockMode::GOVERNANCE)),
            None
        ));
        assert!(retention_write_requested(None, Some(&retain_until)));
        assert!(!retention_write_requested(None, None));
    }

    #[test]
    fn validate_post_object_success_controls_accepts_supported_status_codes() {
        for status in [200, 201, 204] {
            let input = PostObjectInput::builder()
                .bucket("test-bucket".to_string())
                .key("test-key".to_string())
                .success_action_status(Some(status))
                .build()
                .expect("post object input should build");
            assert!(
                validate_post_object_success_controls(&input).is_ok(),
                "status {status} should be accepted"
            );
        }
    }

    #[test]
    fn validate_post_object_success_controls_rejects_invalid_status_code() {
        let input = PostObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .success_action_status(Some(202))
            .build()
            .expect("post object input should build");

        let err = validate_post_object_success_controls(&input).expect_err("status 202 should be rejected");
        assert_eq!(err.code(), &S3ErrorCode::MalformedPOSTRequest);
    }

    #[test]
    fn validate_post_object_success_controls_accepts_empty_redirect() {
        let input = PostObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .success_action_redirect(Some("".to_string()))
            .build()
            .expect("post object input should build");
        assert!(validate_post_object_success_controls(&input).is_ok());
    }

    #[test]
    fn validate_post_object_success_controls_rejects_invalid_redirect() {
        let input = PostObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .success_action_redirect(Some("://invalid-url".to_string()))
            .build()
            .expect("post object input should build");

        let err = validate_post_object_success_controls(&input).expect_err("invalid redirect should be rejected");
        assert_eq!(err.code(), &S3ErrorCode::MalformedPOSTRequest);
    }

    /// Object tag conditions must use keys like ExistingObjectTag/<tag-key> so that
    /// bucket policy conditions (e.g. s3:ExistingObjectTag/security) are evaluated correctly.
    #[test]
    fn test_merge_list_bucket_query_conditions_extracts_supported_keys() {
        let mut conditions = HashMap::new();
        merge_list_bucket_query_conditions(
            Action::S3Action(S3Action::ListBucketAction),
            Some("prefix=photos%2F2024%2F&delimiter=%2F&max-keys=10&encoding-type=url"),
            &mut conditions,
        );

        assert_eq!(conditions.get("prefix"), Some(&vec!["photos/2024/".to_string()]));
        assert_eq!(conditions.get("delimiter"), Some(&vec!["/".to_string()]));
        assert_eq!(conditions.get("max-keys"), Some(&vec!["10".to_string()]));
        assert!(!conditions.contains_key("encoding-type"));
    }

    #[test]
    fn test_merge_list_bucket_query_conditions_preserves_empty_prefix_signal() {
        let mut conditions = HashMap::new();
        merge_list_bucket_query_conditions(
            Action::S3Action(S3Action::ListBucketVersionsAction),
            Some("prefix=&delimiter=%2F"),
            &mut conditions,
        );

        assert_eq!(conditions.get("prefix"), Some(&vec![String::new()]));
        assert_eq!(conditions.get("delimiter"), Some(&vec!["/".to_string()]));
    }

    #[test]
    fn test_merge_list_bucket_query_conditions_ignores_non_list_actions() {
        let mut conditions = HashMap::new();
        merge_list_bucket_query_conditions(
            Action::S3Action(S3Action::GetObjectAction),
            Some("prefix=photos%2F2024%2F&delimiter=%2F&max-keys=10"),
            &mut conditions,
        );

        assert!(conditions.is_empty());
    }

    #[test]
    fn test_merge_request_object_tag_conditions_applies_only_to_put_object() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-tagging", HeaderValue::from_static("classification=restricted&label=copy%20test"));

        let mut source_conditions = HashMap::new();
        merge_request_object_tag_conditions(Action::S3Action(S3Action::GetObjectAction), &headers, &mut source_conditions)
            .expect("GetObject condition construction should succeed");
        assert!(
            source_conditions.is_empty(),
            "destination request tags must not affect CopyObject source authorization"
        );

        let mut destination_conditions = HashMap::new();
        merge_request_object_tag_conditions(Action::S3Action(S3Action::PutObjectAction), &headers, &mut destination_conditions)
            .expect("PutObject condition construction should succeed");
        assert_eq!(
            destination_conditions.get("RequestObjectTag/classification"),
            Some(&vec!["restricted".to_string()])
        );
        assert_eq!(destination_conditions.get("RequestObjectTag/label"), Some(&vec!["copy test".to_string()]));
        assert_eq!(
            destination_conditions.get("RequestObjectTagKeys"),
            Some(&vec!["classification".to_string(), "label".to_string()])
        );
    }

    #[test]
    fn internal_object_authorization_ignores_unapplied_request_options() {
        let mut req = build_request((), Method::POST);
        req.headers.insert(
            "authorization",
            HeaderValue::from_static("AWS4-HMAC-SHA256 Credential=test/20260801/us-east-1/s3tables/aws4_request"),
        );
        req.headers.insert("user-agent", HeaderValue::from_static("pyiceberg/test"));
        req.headers
            .insert("x-amz-server-side-encryption", HeaderValue::from_static("AES256"));
        req.headers
            .insert("x-amz-tagging", HeaderValue::from_static("classification=%ZZ"));
        req.extensions.insert(InternalObjectAuthorization);

        let credentials = rustfs_credentials::Credentials::default();
        let conditions =
            authorization_conditions(&req, &credentials, None, None, None, None, Action::S3Action(S3Action::PutObjectAction))
                .expect("internal object authorization conditions should build");

        assert_eq!(conditions.get("authType"), Some(&vec!["REST-HEADER".to_string()]));
        assert_eq!(conditions.get("signatureversion"), Some(&vec!["AWS4-HMAC-SHA256".to_string()]));
        assert_eq!(conditions.get("UserAgent"), Some(&vec!["pyiceberg/test".to_string()]));
        assert!(!conditions.contains_key("authorization"));
        assert!(!conditions.contains_key("x-amz-server-side-encryption"));
        assert!(!conditions.contains_key("RequestObjectTag/classification"));
        assert!(!conditions.contains_key("RequestObjectTagKeys"));
    }

    #[tokio::test]
    async fn test_required_existing_object_tags_are_merged_into_authorization_conditions() {
        let (_temp_dir, _disk_paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
        let server_ctx = ServerContextSlot::new();
        assert!(server_ctx.install(Arc::new(AppContext::new(store, Arc::new(UnreadyIam), Arc::new(TestKms),))));
        let mut request = build_request((), Method::GET);
        request.extensions.insert(server_ctx);
        request.extensions.insert(ObjectTagConditions::new(
            "bucket",
            "tagged-object",
            None,
            HashMap::from([
                ("ExistingObjectTag/security".to_string(), vec!["restricted".to_string()]),
                ("ExistingObjectTag/project".to_string(), vec!["webapp".to_string()]),
            ]),
        ));
        let mut conditions = HashMap::new();
        conditions.insert("delimiter".to_string(), vec!["/".to_string()]);

        maybe_merge_object_tag_conditions(
            &mut request,
            Action::S3Action(S3Action::GetObjectAction),
            "bucket",
            "tagged-object",
            None,
            &mut conditions,
            true,
        )
        .await
        .expect("merge required existing object tags into authorization conditions");

        assert_eq!(conditions.get("ExistingObjectTag/security"), Some(&vec!["restricted".to_string()]));
        assert_eq!(conditions.get("ExistingObjectTag/project"), Some(&vec!["webapp".to_string()]));
        assert_eq!(conditions.get("delimiter"), Some(&vec!["/".to_string()]));
    }

    #[test]
    fn test_bucket_policy_existing_object_tag_condition_key_detection() {
        let condition_key_policy = r#"{
  "Version":"2012-10-17",
  "Statement":[{
    "Effect":"Allow",
    "Principal":"*",
    "Action":["s3:GetObject"],
    "Resource":["arn:aws:s3:::bucket/*"],
    "Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}
  }]
}"#;
        let policy: BucketPolicy = serde_json::from_str(condition_key_policy).expect("valid bucket policy JSON");
        assert!(
            bucket_policy_uses_existing_object_tag_conditions(&policy),
            "ExistingObjectTag in condition key must be detected"
        );

        let value_only_policy = r#"{
  "Version":"2012-10-17",
  "Statement":[{
    "Effect":"Allow",
    "Principal":"*",
    "Action":["s3:GetObject"],
    "Resource":["arn:aws:s3:::bucket/*"],
    "Condition":{"StringEquals":{"s3:prefix":"ExistingObjectTag/security"}}
  }]
}"#;
        let policy: BucketPolicy = serde_json::from_str(value_only_policy).expect("valid bucket policy JSON");
        assert!(
            !bucket_policy_uses_existing_object_tag_conditions(&policy),
            "ExistingObjectTag text in values should not trigger tag dependency"
        );
    }

    #[test]
    fn test_unparsable_bucket_policy_json_implies_conservative_existing_object_tag_fetch() {
        // Matches `load_bucket_policy_existing_object_tag_hint`: unparsable policy => conservative tag fetch.
        let malformed = r#"{"Version":"2012-10-17","Statement":[INVALID]}"#;
        assert!(serde_json::from_str::<BucketPolicy>(malformed).is_err());
        let conservative_fetch = serde_json::from_str::<BucketPolicy>(malformed)
            .map(|p| bucket_policy_uses_existing_object_tag_conditions(&p))
            .unwrap_or(true);
        assert!(conservative_fetch);

        // Invalid JSON that still contains real ExistingObjectTag condition keys (trailing comma).
        let malformed_with_tag_keys = r#"{"Version":"2012-10-17","Statement":[{"Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}},]}"#;
        assert!(serde_json::from_str::<BucketPolicy>(malformed_with_tag_keys).is_err());
        let conservative_with_tag_keys = serde_json::from_str::<BucketPolicy>(malformed_with_tag_keys)
            .map(|p| bucket_policy_uses_existing_object_tag_conditions(&p))
            .unwrap_or(true);
        assert!(conservative_with_tag_keys);
    }

    #[test]
    fn test_classify_bucket_policy_raw_load_error() {
        assert_eq!(
            classify_bucket_policy_raw_load_error(&StorageError::ConfigNotFound),
            BucketPolicyRawLoadErrorKind::PolicyMissing
        );
        assert_eq!(
            classify_bucket_policy_raw_load_error(&StorageError::BucketNotFound("b".to_string())),
            BucketPolicyRawLoadErrorKind::BucketMissing
        );
        assert_eq!(
            classify_bucket_policy_raw_load_error(&StorageError::Io(std::io::Error::other("boom"))),
            BucketPolicyRawLoadErrorKind::Other
        );
    }

    /// Owner can bypass bucket policy Deny only for the three policy management APIs (per AWS S3).
    #[test]
    fn test_owner_can_bypass_policy_deny_only_for_policy_apis() {
        // Owner + policy management actions -> bypass allowed
        assert!(owner_can_bypass_policy_deny(true, &Action::S3Action(S3Action::GetBucketPolicyAction)));
        assert!(owner_can_bypass_policy_deny(true, &Action::S3Action(S3Action::PutBucketPolicyAction)));
        assert!(owner_can_bypass_policy_deny(true, &Action::S3Action(S3Action::DeleteBucketPolicyAction)));

        // Owner + other actions -> no bypass (still subject to bucket policy Deny)
        assert!(!owner_can_bypass_policy_deny(true, &Action::S3Action(S3Action::ListBucketAction)));
        assert!(!owner_can_bypass_policy_deny(true, &Action::S3Action(S3Action::GetObjectAction)));

        // Non-owner -> no bypass for any action
        assert!(!owner_can_bypass_policy_deny(false, &Action::S3Action(S3Action::GetBucketPolicyAction)));
        assert!(!owner_can_bypass_policy_deny(
            false,
            &Action::S3Action(S3Action::DeleteBucketPolicyAction)
        ));
    }

    /// Issue #5740: the denial helper must keep the client-facing error identical
    /// to the historical bare response — the added diagnostics are log-only.
    #[test]
    fn test_access_denied_helper_keeps_generic_wire_response() {
        let denial = DenialContext {
            quiet: false,
            bucket: "bucket",
            object: "object",
            version_id: Some("version"),
            account: Some("account"),
            is_owner: false,
        };
        let err = denial.deny("bucket_policy_explicit_deny", Action::S3Action(S3Action::GetObjectAction));
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
        assert_eq!(err.message(), Some("Access Denied"));
    }

    #[test]
    fn test_secondary_tag_hint_action_for_delete_object_version() {
        assert_eq!(
            secondary_tag_hint_action(Action::S3Action(S3Action::DeleteObjectAction), Some("v1")),
            Some(Action::S3Action(S3Action::DeleteObjectVersionAction))
        );
        assert_eq!(secondary_tag_hint_action(Action::S3Action(S3Action::DeleteObjectAction), None), None);
        assert_eq!(
            secondary_tag_hint_action(Action::S3Action(S3Action::ListBucketVersionsAction), None),
            None
        );
    }

    #[tokio::test]
    async fn test_anonymous_delete_object_with_version_requires_secondary_policy_and_tag_hint() {
        let policy: BucketPolicy = serde_json::from_str(
            r#"{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Principal":{"AWS":"*"},
      "Action":["s3:DeleteObject"],
      "Resource":["arn:aws:s3:::bucket/*"]
    },
    {
      "Effect":"Allow",
      "Principal":{"AWS":"*"},
      "Action":["s3:DeleteObjectVersion"],
      "Resource":["arn:aws:s3:::bucket/*"],
      "Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}
    }
  ]
}"#,
        )
        .expect("bucket policy should parse");
        let hint = BucketPolicyExistingObjectTagHint::Parsed(policy.clone());
        let no_groups: Option<Vec<String>> = None;
        let conditions = HashMap::new();

        let args_delete = BucketPolicyArgs {
            bucket: "bucket",
            action: Action::S3Action(S3Action::DeleteObjectAction),
            is_owner: false,
            account: "",
            groups: &no_groups,
            conditions: &conditions,
            object: "obj",
        };
        assert!(
            policy.is_allowed(&args_delete).await,
            "anonymous DeleteObject can be allowed by bucket policy"
        );

        let args_delete_version = BucketPolicyArgs {
            bucket: "bucket",
            action: Action::S3Action(S3Action::DeleteObjectVersionAction),
            is_owner: false,
            account: "",
            groups: &no_groups,
            conditions: &conditions,
            object: "obj",
        };
        assert!(
            !policy.is_allowed(&args_delete_version).await,
            "DeleteObjectVersion should still be denied without matching ExistingObjectTag conditions"
        );

        let needs_tag_main = bucket_policy_needs_existing_object_tag_from_hint(&hint, &args_delete).await;
        let needs_tag_secondary = bucket_policy_needs_existing_object_tag_from_hint(&hint, &args_delete_version).await;
        assert!(!needs_tag_main, "DeleteObject statement itself does not require ExistingObjectTag");
        assert!(
            needs_tag_secondary,
            "DeleteObjectVersion statement requires ExistingObjectTag when version delete is evaluated"
        );
        assert!(
            needs_tag_main || needs_tag_secondary,
            "combined primary+secondary check must require tag fetch for DeleteObject(versionId)"
        );
    }

    #[tokio::test]
    async fn post_object_marks_request_extensions() {
        let input = PostObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .expect("post object input should build");

        let mut req = S3Request {
            input,
            method: Method::POST,
            uri: Uri::from_static("/"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        req.extensions.insert(ReqInfo::default());

        let fs = FS::new();
        let _ = fs.post_object(&mut req).await;

        assert!(
            req.extensions.get::<PostObjectRequestMarker>().is_some(),
            "post object request should carry the marker for downstream handling"
        );
    }

    #[tokio::test]
    async fn put_object_rejects_write_offset_bytes_before_authorization() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .expect("put object input should build");

        let mut headers = HeaderMap::new();
        headers.insert(AMZ_WRITE_OFFSET_BYTES_HEADER, http::HeaderValue::from_static("0"));

        let mut req = S3Request {
            input,
            method: Method::PUT,
            uri: Uri::from_static("/test-bucket/test-key"),
            headers,
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        req.extensions.insert(ReqInfo::default());

        let fs = FS::new();
        let err = fs
            .put_object(&mut req)
            .await
            .expect_err("write-offset-bytes requests should be rejected");

        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
        assert_eq!(
            err.message(),
            Some(ApiError::error_code_to_message(&S3ErrorCode::NotImplemented).as_str())
        );

        let req_info = req.extensions.get::<ReqInfo>().expect("request info should remain available");
        assert_eq!(req_info.bucket.as_deref(), Some("test-bucket"));
        assert_eq!(req_info.object.as_deref(), Some("test-key"));
    }

    #[tokio::test]
    #[serial]
    async fn put_object_access_captures_authorized_bucket_incarnation() {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let server_ctx = ServerContextSlot::new();
        assert!(server_ctx.install(Arc::new(AppContext::new(Arc::clone(&store), Arc::new(UnreadyIam), Arc::new(TestKms),))));
        let fs = FS::with_server_ctx(server_ctx);

        let bucket = "generation-guard-collision";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("test bucket should be created");
        let policy_json = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
        );
        let mut metadata = (*crate::storage::get_bucket_metadata(bucket)
            .await
            .expect("new bucket metadata should be cached"))
        .clone();
        metadata.policy_config = Some(serde_json::from_str(&policy_json).expect("test bucket policy should parse"));
        metadata.policy_config_json = policy_json.into_bytes();
        crate::storage::storage_api::set_bucket_metadata(bucket.to_string(), metadata)
            .await
            .expect("test bucket policy should be published");

        let input = PutObjectInput::builder()
            .bucket(bucket.to_string())
            .key(bucket.to_string())
            .build()
            .expect("put object input should build");
        let mut req = build_request(input, Method::PUT);
        ensure_req_info(&mut req);
        req.extensions.insert(fs.server_ctx().clone());

        fs.put_object(&mut req)
            .await
            .expect("anonymous PutObject should be authorized by the test policy");
        let mut opts = crate::storage::ObjectOptions::default();
        apply_bucket_generation_guard(&req, bucket, &mut opts).expect("request snapshot should apply to PutObject options");
        assert_eq!(
            opts.expected_bucket_incarnation_id,
            Some(
                store
                    .bucket_incarnation_id(bucket)
                    .await
                    .expect("bucket incarnation should remain readable")
            )
        );
    }

    #[tokio::test]
    #[serial]
    async fn request_slot_keeps_bucket_policy_bound_to_its_store() {
        let store_a = crate::app::gating_test_env::shared_gating_ecstore().await;
        let (_store_b_temp, _store_b_paths, store_b) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
        let bucket = format!("request-policy-isolation-{}", uuid::Uuid::new_v4());
        for store in [&store_a, &store_b] {
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create isolated policy test bucket");
        }

        let allow_policy = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
        );
        let deny_policy = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Deny","Principal":{{"AWS":"*"}},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
        );
        for (store, policy) in [(&store_a, allow_policy), (&store_b, deny_policy)] {
            store
                .update_bucket_metadata_config(
                    &bucket,
                    crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                    policy.into_bytes(),
                )
                .await
                .expect("publish isolated bucket policy");
        }

        let slot_a = ServerContextSlot::new();
        assert!(slot_a.install(Arc::new(AppContext::new(Arc::clone(&store_a), Arc::new(UnreadyIam), Arc::new(TestKms),))));
        let slot_b = ServerContextSlot::new();
        assert!(slot_b.install(Arc::new(AppContext::new(Arc::clone(&store_b), Arc::new(UnreadyIam), Arc::new(TestKms),))));

        let mut request_a = build_request((), Method::PUT);
        request_a.extensions.insert(ReqInfo {
            bucket: Some(bucket.clone()),
            object: Some("object".to_string()),
            ..Default::default()
        });
        request_a.extensions.insert(slot_a);
        super::authorize_request(&mut request_a, Action::S3Action(S3Action::PutObjectAction))
            .await
            .expect("store A policy should authorize store A request");

        let mut request_b = build_request((), Method::PUT);
        request_b.extensions.insert(ReqInfo {
            bucket: Some(bucket.clone()),
            object: Some("object".to_string()),
            ..Default::default()
        });
        request_b.extensions.insert(slot_b);
        let err = super::authorize_request(&mut request_b, Action::S3Action(S3Action::PutObjectAction))
            .await
            .expect_err("store B policy must deny store B request");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);

        let allow_policy = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
        );
        store_b
            .update_bucket_metadata_config(
                &bucket,
                crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                allow_policy.into_bytes(),
            )
            .await
            .expect("replace store B bucket policy");
        let slot_b = ServerContextSlot::new();
        assert!(slot_b.install(Arc::new(AppContext::new(Arc::clone(&store_b), Arc::new(UnreadyIam), Arc::new(TestKms),))));
        let mut allowed_request_b = build_request((), Method::PUT);
        allowed_request_b.extensions.insert(ReqInfo {
            bucket: Some(bucket.clone()),
            object: Some("object".to_string()),
            ..Default::default()
        });
        allowed_request_b.extensions.insert(slot_b);
        super::authorize_request(&mut allowed_request_b, Action::S3Action(S3Action::PutObjectAction))
            .await
            .expect("store B policy should allow before its public access block is installed");

        store_b
            .update_bucket_metadata_config(
                &bucket,
                crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG,
                br#"<PublicAccessBlockConfiguration><RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>"#
                    .to_vec(),
            )
            .await
            .expect("publish store B public access block configuration");
        let slot_b = ServerContextSlot::new();
        assert!(slot_b.install(Arc::new(AppContext::new(Arc::clone(&store_b), Arc::new(UnreadyIam), Arc::new(TestKms),))));
        let mut restricted_request_b = build_request((), Method::PUT);
        restricted_request_b.extensions.insert(ReqInfo {
            bucket: Some(bucket.clone()),
            object: Some("object".to_string()),
            ..Default::default()
        });
        restricted_request_b.extensions.insert(slot_b);
        let err = super::authorize_request(&mut restricted_request_b, Action::S3Action(S3Action::PutObjectAction))
            .await
            .expect_err("store B public access block must restrict store B request");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);

        let tag_bucket = format!("request-tag-isolation-{}", uuid::Uuid::new_v4());
        for (store, tags) in [(&store_a, "classification=ambient"), (&store_b, "classification=request")] {
            store
                .make_bucket(&tag_bucket, &MakeBucketOptions::default())
                .await
                .expect("create isolated tag test bucket");
            let mut reader = crate::storage::PutObjReader::from_vec(b"tagged object".to_vec());
            store
                .put_object(&tag_bucket, "object", &mut reader, &crate::storage::ObjectOptions::default())
                .await
                .expect("create isolated tagged object");
            store
                .put_object_tags(&tag_bucket, "object", tags, &crate::storage::ObjectOptions::default())
                .await
                .expect("publish isolated object tags");
        }
        let tag_policy = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::{tag_bucket}/*"],"Condition":{{"StringEquals":{{"s3:ExistingObjectTag/classification":"request"}}}}}}]}}"#
        );
        store_b
            .update_bucket_metadata_config(
                &tag_bucket,
                crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                tag_policy.into_bytes(),
            )
            .await
            .expect("publish store B tag-conditioned policy");
        let slot_b = ServerContextSlot::new();
        assert!(slot_b.install(Arc::new(AppContext::new(Arc::clone(&store_b), Arc::new(UnreadyIam), Arc::new(TestKms),))));
        let mut tagged_request_b = build_request((), Method::PUT);
        tagged_request_b.extensions.insert(ReqInfo {
            bucket: Some(tag_bucket),
            object: Some("object".to_string()),
            ..Default::default()
        });
        tagged_request_b.extensions.insert(slot_b);
        super::authorize_request(&mut tagged_request_b, Action::S3Action(S3Action::PutObjectAction))
            .await
            .expect("store B existing object tags should authorize store B request");

        let mut unready_request = build_request((), Method::PUT);
        unready_request.extensions.insert(ServerContextSlot::new());
        let err = request_object_store(&unready_request).expect_err("an unready request slot must not use an ambient store");
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    #[serial]
    async fn delete_object_access_captures_authorized_bucket_incarnation() {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let server_ctx = ServerContextSlot::new();
        let app_context = Arc::new(AppContext::new(Arc::clone(&store), Arc::new(UnreadyIam), Arc::new(TestKms)));
        assert!(server_ctx.install(Arc::clone(&app_context)));
        let fs = FS::with_server_ctx(server_ctx);

        let bucket = format!("delete-generation-guard-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("test bucket should be created");
        assert!(
            fs.get_object_tag_conditions_for_policy(&bucket, "missing-object", None)
                .await
                .expect("a missing object must be treated as having no policy tags")
                .is_empty()
        );
        assert!(
            fs.get_object_tag_conditions_for_policy(&format!("missing-{bucket}"), "missing-object", None)
                .await
                .expect("a missing bucket must be indistinguishable during pre-authorization tag lookup")
                .is_empty()
        );
        let policy_json = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:DeleteObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
        );
        let mut metadata = (*crate::storage::get_bucket_metadata(&bucket)
            .await
            .expect("new bucket metadata should be cached"))
        .clone();
        metadata.policy_config = Some(serde_json::from_str(&policy_json).expect("test bucket policy should parse"));
        metadata.policy_config_json = policy_json.into_bytes();
        crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
            .await
            .expect("test bucket policy should be published");

        let input = DeleteObjectInput::builder()
            .bucket(bucket.clone())
            .key("object".to_string())
            .build()
            .expect("delete object input should build");
        let mut req = build_request(input, Method::DELETE);
        ensure_req_info(&mut req);
        req.extensions.insert(fs.server_ctx().clone());

        fs.delete_object(&mut req)
            .await
            .expect("anonymous DeleteObject should be authorized by the test policy");
        let mut opts = crate::storage::ObjectOptions::default();
        apply_bucket_generation_guard(&req, &bucket, &mut opts).expect("request snapshot should apply to DeleteObject options");
        assert_eq!(
            opts.expected_bucket_incarnation_id,
            Some(
                store
                    .bucket_incarnation_id(&bucket)
                    .await
                    .expect("bucket incarnation should remain readable")
            )
        );

        store
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("delete the authorized bucket generation");
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("recreate the same bucket name");
        let mut reader = crate::storage::PutObjReader::from_vec(b"new generation".to_vec());
        store
            .put_object(&bucket, "object", &mut reader, &crate::storage::ObjectOptions::default())
            .await
            .expect("put the new-generation object");

        let err = crate::app::object_usecase::DefaultObjectUsecase::with_context(Some(app_context))
            .execute_delete_object(req)
            .await
            .expect_err("the old authorization must not delete from the recreated bucket");
        assert_eq!(err.code(), &S3ErrorCode::NoSuchBucket);
        store
            .get_object_info(&bucket, "object", &crate::storage::ObjectOptions::default())
            .await
            .expect("the new-generation object must survive the stale request");
    }

    #[tokio::test]
    #[serial]
    async fn copy_operations_reject_recreated_source_bucket_after_authorization() {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let server_ctx = ServerContextSlot::new();
        let app_context = Arc::new(AppContext::new(Arc::clone(&store), Arc::new(UnreadyIam), Arc::new(TestKms)));
        assert!(server_ctx.install(Arc::clone(&app_context)));
        let fs = FS::with_server_ctx(server_ctx);

        let suffix = uuid::Uuid::new_v4();
        let src_bucket = format!("copy-src-generation-{suffix}");
        let dst_bucket = format!("copy-dst-generation-{suffix}");
        for bucket in [&src_bucket, &dst_bucket] {
            store
                .make_bucket(bucket, &MakeBucketOptions::default())
                .await
                .expect("create copy test bucket");
        }

        let src_policy_json = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::{src_bucket}/*"]}}]}}"#
        );
        let mut src_metadata = (*crate::storage::get_bucket_metadata(&src_bucket)
            .await
            .expect("source bucket metadata should be cached"))
        .clone();
        src_metadata.policy_config = Some(serde_json::from_str(&src_policy_json).expect("source policy should parse"));
        src_metadata.policy_config_json = src_policy_json.into_bytes();
        crate::storage::storage_api::set_bucket_metadata(src_bucket.clone(), src_metadata)
            .await
            .expect("publish source policy");

        let dst_policy_json = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::{dst_bucket}/*"]}}]}}"#
        );
        let mut dst_metadata = (*crate::storage::get_bucket_metadata(&dst_bucket)
            .await
            .expect("destination bucket metadata should be cached"))
        .clone();
        dst_metadata.policy_config = Some(serde_json::from_str(&dst_policy_json).expect("destination policy should parse"));
        dst_metadata.policy_config_json = dst_policy_json.into_bytes();
        crate::storage::storage_api::set_bucket_metadata(dst_bucket.clone(), dst_metadata)
            .await
            .expect("publish destination policy");

        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: src_bucket.clone().into(),
                key: "secret".into(),
                version_id: None,
            })
            .bucket(dst_bucket.clone())
            .key("copied".to_string())
            .build()
            .expect("copy input should build");
        let mut req = build_request(input, Method::PUT);
        ensure_req_info(&mut req);
        req.extensions.insert(fs.server_ctx().clone());

        fs.copy_object(&mut req)
            .await
            .expect("test policies should authorize the copy request");
        let mut source_opts = crate::storage::ObjectOptions::default();
        apply_copy_source_bucket_generation_guard(&req, &src_bucket, &mut source_opts)
            .expect("authorized source generation should be attached");
        let authorized_source_incarnation_id = source_opts
            .expected_bucket_incarnation_id
            .expect("source incarnation should be captured");

        let upload = store
            .new_multipart_upload(&dst_bucket, "multipart-copy", &crate::storage::ObjectOptions::default())
            .await
            .expect("create destination multipart upload");
        let upload_input = UploadPartCopyInput::builder()
            .bucket(dst_bucket.clone())
            .key("multipart-copy".to_string())
            .copy_source(CopySource::Bucket {
                bucket: src_bucket.clone().into(),
                key: "secret".into(),
                version_id: None,
            })
            .part_number(1)
            .upload_id(upload.upload_id)
            .build()
            .expect("part copy input should build");
        let mut upload_req = build_request(upload_input, Method::PUT);
        ensure_req_info(&mut upload_req);
        upload_req.extensions.insert(fs.server_ctx().clone());
        fs.upload_part_copy(&mut upload_req)
            .await
            .expect("test policies should authorize the part copy request");

        store
            .delete_bucket(&src_bucket, &DeleteBucketOptions::default())
            .await
            .expect("delete the authorized source generation");
        store
            .make_bucket(&src_bucket, &MakeBucketOptions::default())
            .await
            .expect("recreate the source bucket name");
        assert_ne!(
            authorized_source_incarnation_id,
            store
                .bucket_incarnation_id(&src_bucket)
                .await
                .expect("read recreated source incarnation")
        );
        let mut reader = crate::storage::PutObjReader::from_vec(b"new generation secret".to_vec());
        store
            .put_object(&src_bucket, "secret", &mut reader, &crate::storage::ObjectOptions::default())
            .await
            .expect("write the new-generation source object");

        let err = crate::app::object_usecase::DefaultObjectUsecase::with_context(Some(Arc::clone(&app_context)))
            .execute_copy_object(req)
            .await
            .expect_err("stale authorization must not read from the recreated source bucket");
        assert_eq!(err.code(), &S3ErrorCode::NoSuchBucket);
        let err = crate::app::multipart_usecase::DefaultMultipartUsecase::with_context(Some(app_context))
            .execute_upload_part_copy(upload_req)
            .await
            .expect_err("stale authorization must not read the recreated source into a part");
        assert_eq!(err.code(), &S3ErrorCode::NoSuchBucket);
        store
            .get_object_info(&dst_bucket, "copied", &crate::storage::ObjectOptions::default())
            .await
            .expect_err("the new-generation source object must not be copied");
    }

    #[tokio::test]
    #[serial]
    async fn restore_object_access_keeps_authorized_bucket_incarnation_across_recreation() {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let server_ctx = ServerContextSlot::new();
        let app_context = Arc::new(AppContext::new(Arc::clone(&store), Arc::new(UnreadyIam), Arc::new(TestKms)));
        assert!(server_ctx.install(Arc::clone(&app_context)));
        let fs = FS::with_server_ctx(server_ctx);

        let bucket = format!("restore-generation-guard-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the bucket generation being authorized");
        let authorized_incarnation_id = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("read the authorized bucket incarnation");
        let policy_json = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:RestoreObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
        );
        let mut metadata = (*crate::storage::get_bucket_metadata(&bucket)
            .await
            .expect("authorized bucket metadata should be cached"))
        .clone();
        metadata.policy_config = Some(serde_json::from_str(&policy_json).expect("test policy should parse"));
        metadata.policy_config_json = policy_json.into_bytes();
        crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
            .await
            .expect("publish the RestoreObject policy");

        let input = RestoreObjectInput::builder()
            .bucket(bucket.clone())
            .key("object".to_string())
            .restore_request(Some(RestoreRequest {
                days: Some(1),
                description: None,
                glacier_job_parameters: None,
                output_location: None,
                select_parameters: None,
                tier: None,
                type_: None,
            }))
            .build()
            .expect("restore object input should build");
        let mut req = build_request(input, Method::POST);
        ensure_req_info(&mut req);
        req.extensions.insert(fs.server_ctx().clone());
        let (authorized_tx, authorized_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
        install_restore_authorization_test_hook(bucket.clone(), authorized_tx, resume_rx);

        let access = tokio::spawn(async move {
            let result = fs.restore_object(&mut req).await;
            (result, req)
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), authorized_rx)
            .await
            .expect("RestoreObject authorization should reach the test hook")
            .expect("RestoreObject access must not fail before reaching the test hook");

        store
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("delete the authorized bucket generation");
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("recreate the bucket with the same name");
        let recreated_incarnation_id = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("read the recreated bucket incarnation");
        assert_ne!(authorized_incarnation_id, recreated_incarnation_id);
        resume_tx
            .send(())
            .expect("RestoreObject access should still be waiting at the test hook");

        let (result, req) = access.await.expect("RestoreObject access task should join");
        result.expect("the already-authorized request should retain its generation guard");
        let mut opts = crate::storage::ObjectOptions::default();
        apply_bucket_generation_guard(&req, &bucket, &mut opts).expect("apply the RestoreObject authorization guard");
        assert_eq!(opts.expected_bucket_incarnation_id, Some(authorized_incarnation_id));

        let err = crate::app::object_usecase::DefaultObjectUsecase::with_context(Some(app_context))
            .execute_restore_object(req)
            .await
            .expect_err("the old RestoreObject authorization must not reach the recreated bucket");
        assert_eq!(err.code(), &S3ErrorCode::NoSuchBucket);
        store
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("clean up the recreated bucket");
    }

    #[test]
    fn dispatched_put_object_fails_closed_without_generation_guard() {
        let input = PutObjectInput::builder()
            .bucket("generation-guard-bucket".to_string())
            .key("object".to_string())
            .build()
            .expect("put object input should build");
        let mut req = build_request(input, Method::PUT);
        req.extensions.insert(ServerContextSlot::new());

        let mut opts = crate::storage::ObjectOptions::default();
        let err = apply_bucket_generation_guard(&req, "generation-guard-bucket", &mut opts)
            .expect_err("a dispatched PutObject must not commit without its authorization fence");
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[test]
    fn write_offset_bytes_header_detection_is_case_insensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("X-Amz-Write-Offset-Bytes", http::HeaderValue::from_static("0"));

        assert!(has_write_offset_bytes_header(&headers));
    }

    #[tokio::test]
    async fn put_object_rejects_write_offset_bytes_before_authorize_request() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .expect("put object input should build");

        let mut req = S3Request {
            input,
            method: Method::PUT,
            uri: Uri::from_static("/test-bucket/test-key"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        req.headers
            .insert("x-amz-write-offset-bytes", http::HeaderValue::from_static("0"));
        req.extensions.insert(ReqInfo {
            cred: Some(rustfs_credentials::Credentials::default()),
            ..ReqInfo::default()
        });

        let err = FS::new()
            .put_object(&mut req)
            .await
            .expect_err("write-offset-bytes requests should be rejected before authorization");

        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
        assert_eq!(
            err.message(),
            Some(ApiError::error_code_to_message(&S3ErrorCode::NotImplemented).as_str())
        );

        let req_info = req.extensions.get::<ReqInfo>().expect("req info should remain available");
        assert_eq!(req_info.bucket.as_deref(), Some("test-bucket"));
        assert_eq!(req_info.object.as_deref(), Some("test-key"));
        assert_eq!(req_info.version_id, None);
    }

    #[tokio::test]
    async fn delete_objects_defers_object_authorization_to_usecase() {
        let input = DeleteObjectsInput::builder()
            .bucket("test-bucket".to_string())
            .delete(Delete {
                objects: vec![ObjectIdentifier {
                    key: "prefix/test-key".to_string(),
                    version_id: None,
                    ..Default::default()
                }],
                quiet: None,
            })
            .build()
            .expect("delete objects input should build");

        let mut req = build_request(input, Method::POST);
        req.extensions.insert(ReqInfo {
            cred: Some(rustfs_credentials::Credentials::default()),
            ..ReqInfo::default()
        });

        FS::new()
            .delete_objects(&mut req)
            .await
            .expect("DeleteObjects access hook should not require bucket-level DeleteObject");

        let req_info = req.extensions.get::<ReqInfo>().expect("req info should remain available");
        assert_eq!(req_info.bucket.as_deref(), Some("test-bucket"));
        assert_eq!(req_info.object, None);
        assert_eq!(req_info.version_id, None);
    }

    #[tokio::test]
    async fn abort_multipart_upload_fails_closed_when_policy_metadata_is_unavailable() {
        let fs = FS::new();
        let mut req = build_request(
            AbortMultipartUploadInput::builder()
                .bucket("bucket".to_string())
                .key("object".to_string())
                .upload_id("upload-id".to_string())
                .build()
                .unwrap(),
            Method::DELETE,
        );
        ensure_req_info(&mut req);

        let err = fs
            .abort_multipart_upload(&mut req)
            .await
            .expect_err("unavailable policy metadata must fail closed");
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn complete_multipart_upload_fails_closed_when_policy_metadata_is_unavailable() {
        let fs = FS::new();
        let mut req = build_request(
            CompleteMultipartUploadInput::builder()
                .bucket("bucket".to_string())
                .key("object".to_string())
                .upload_id("upload-id".to_string())
                .multipart_upload(Some(CompletedMultipartUpload::default()))
                .build()
                .unwrap(),
            Method::POST,
        );
        ensure_req_info(&mut req);

        let err = fs
            .complete_multipart_upload(&mut req)
            .await
            .expect_err("unavailable policy metadata must fail closed");
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn upload_part_copy_fails_closed_when_policy_metadata_is_unavailable() {
        let fs = FS::new();
        let mut req = build_request(
            UploadPartCopyInput::builder()
                .bucket("dst-bucket".to_string())
                .key("dst-object".to_string())
                .upload_id("upload-id".to_string())
                .part_number(1)
                .copy_source(CopySource::Bucket {
                    bucket: "src-bucket".into(),
                    key: "src-object".into(),
                    version_id: None,
                })
                .build()
                .unwrap(),
            Method::PUT,
        );
        ensure_req_info(&mut req);

        let err = fs
            .upload_part_copy(&mut req)
            .await
            .expect_err("unavailable policy metadata must fail closed");
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }
}
