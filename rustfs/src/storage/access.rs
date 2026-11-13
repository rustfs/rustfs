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

use self::access_owned::object_misc::authorize_delete_object;
use super::ObjectOptions;
use super::ecfs::FS;
use super::{ECStore, PolicySys, StorageError, get_lock_acquire_timeout, is_err_bucket_not_found};
use crate::auth::facts::{
    AuthzFacts, AuthzPrincipal, AuthzTarget, base_conditions, build_args, build_conditions, forward_secret_headers,
};
use crate::auth::{
    AuthType, OBJECT_CREATION_OPERATION_CONDITION, OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION,
    RUSTFS_MAX_CONTENT_LENGTH_QUERY, RUSTFS_MAX_TOTAL_OBJECT_SIZE_QUERY, VerifiedPresignedRequest, VerifiedSigV4Request,
    check_key_valid_with_context, get_condition_values_with_client_info, get_request_auth_type_with_query, get_session_token,
    parse_presigned_multipart_max_total_object_size, parse_presigned_put_max_content_length,
    reject_unsigned_amz_headers_on_sigv4_request,
};
use crate::error::ApiError;
use crate::license::license_check;
use crate::shared_types::RemoteAddr;
use crate::storage::denial_audit::DenialAuditor;
use crate::storage::helper::DenialScope;
use crate::storage::request_context::RequestContext;
use crate::storage::storage_api::contract::bucket::BUCKET_LIFECYCLE_LOCK_OBJECT;
use crate::storage::storage_api::contract::namespace::NamespaceLocking as _;
use crate::storage::storage_api::ecstore_bucket::lockless_cache::is_lockless_cache_bucket;
use crate::storage::storage_api::runtime_sources_consumer::ServerContextSlot;
use crate::storage::storage_api::runtime_sources_consumer::runtime_sources;
use chacha20poly1305::{
    ChaCha20Poly1305, KeyInit,
    aead::{Aead, Payload},
};
use http::HeaderMap;
use metrics::counter;
use rand::Rng;
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
    AMZ_BUCKET_REPLICATION_STATUS, AMZ_OBJECT_LOCK_BYPASS_GOVERNANCE, SUFFIX_REPLICATION_ACTUAL_OBJECT_SIZE,
    SUFFIX_REPLICATION_SSEC_CRC, SUFFIX_SOURCE_ETAG, SUFFIX_SOURCE_MTIME, SUFFIX_SOURCE_REPLICATION_CHECK,
    SUFFIX_SOURCE_REPLICATION_REQUEST, SUFFIX_SOURCE_VERSION_ID, force_delete_header, get_header,
};
use s3s::access::{S3Access, S3AccessContext};
use s3s::{S3Error, S3ErrorCode, S3Request, S3Result, dto::*, s3_error};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
#[cfg(test)]
use std::sync::OnceLock;
use time::OffsetDateTime;
use url::Url;

/// The authorization shapes one migration batch owns; a child module so they reach the guard
/// state this module keeps private.
#[path = "access_owned/mod.rs"]
pub(crate) mod access_owned;
#[cfg(test)]
#[path = "access_readers_test.rs"]
mod readers_test;

fn iam_authorization_error(err: IamError) -> ApiError {
    match err {
        IamError::IAMActionNotAllowed => ApiError::access_denied(),
        other => ApiError::from(other),
    }
}

const EVENT_OBJECT_TAG_AUTHORIZATION: &str = "object_tag_authorization";
const LOG_COMPONENT_ACCESS: &str = "storage_access";
const LOG_SUBSYSTEM_AUTHORIZATION: &str = "authorization";

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
    /// Set by callers that audit their denials in aggregate (DeleteObjects), so each per-key
    /// denial is not audited on its own. The denial is still logged.
    pub suppress_denial_audit: bool,
}

pub(crate) fn replication_request_authorized<R: AuthorizationRequest + ?Sized>(req: &R) -> bool {
    req.req_info().is_some_and(|req_info| req_info.replication_request_authorized)
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

pub(crate) fn recursive_force_delete_has_authenticated_caller(
    headers: &HeaderMap,
    authenticated: bool,
    replica_request: bool,
) -> bool {
    match force_delete_header(headers) {
        Ok(Some(true)) => authenticated || replica_request,
        _ => true,
    }
}

fn invalid_force_delete_header() -> S3Error {
    S3Error::with_message(S3ErrorCode::InvalidRequest, "Invalid force-delete header value")
}

#[derive(Clone, Debug)]
pub(crate) struct PostObjectRequestMarker;

#[derive(Clone, Debug)]
struct InternalObjectAuthorization;

/// Retain-until date the request asks to set. `authorization_conditions` turns it
/// into `object-lock-remaining-retention-days`.
#[derive(Clone, Debug)]
pub(crate) struct RequestedObjectLockRetainUntil(pub(crate) OffsetDateTime);

#[derive(Clone, Debug)]
pub(crate) struct RequestedObjectLockRetention(pub(crate) ObjectLockRetention);

/// Legal hold status a standalone PutObjectLegalHold applies from its XML body.
#[derive(Clone, Debug)]
pub(crate) struct RequestedObjectLegalHold(pub(crate) ObjectLockLegalHold);

/// Tag set a standalone PutObjectTagging applies from its XML body.
#[derive(Clone, Debug)]
pub(crate) struct RequestedObjectTags(pub(crate) Tagging);

/// Request headers with the object policy inputs a POST upload applies from its
/// form fields, so header-derived condition keys describe what is stored.
#[derive(Clone)]
pub(crate) struct PostObjectPolicyHeaders(pub(crate) http::HeaderMap);

// The map is a clone of the request headers, so it can carry `Authorization`
// and session tokens; its debug form names the headers without their values.
impl std::fmt::Debug for PostObjectPolicyHeaders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PostObjectPolicyHeaders")
            .field(&self.0.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Copy source as the handler resolves it (percent-decoded, versionId canonical).
#[derive(Clone)]
pub(crate) struct RequestedCopySource(pub(crate) String);

/// These operations stage upload state without publishing an object. Keep this
/// separate from the marker controlling multipart publication fences.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MultipartStagingOperation;

impl MultipartStagingOperation {
    fn for_operation(operation: &str) -> Option<Self> {
        matches!(operation, "CreateMultipartUpload" | "UploadPart" | "UploadPartCopy").then_some(Self)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StagedMultipartPartAuthorization;

#[derive(Clone, Default)]
pub(crate) struct TableDataPlanePublicationGuards {
    state: Arc<parking_lot::Mutex<TableDataPlanePublicationState>>,
}

#[derive(Default)]
struct TableDataPlanePublicationState {
    keys: BTreeSet<(String, String)>,
    guards: Vec<Box<dyn Send>>,
    lock_lost_signals: Vec<Arc<rustfs_lock::distributed_lock::LockLostSignal>>,
    resources: HashMap<(String, String), crate::table_catalog::TableDataPlaneResource>,
    missing_resources: BTreeSet<(String, String)>,
}

impl TableDataPlanePublicationGuards {
    pub(crate) fn add_lock_loss_fences(&self, options: &mut ObjectOptions) {
        let state = self.state.lock();
        for signal in &state.lock_lost_signals {
            options.add_namespace_lock_lost_signal(Arc::clone(signal));
        }
    }
}

pub(crate) const TABLE_DATA_PLANE_LIST_CURSOR_PREFIX: &str = "rustfs-table-list:v1:";
const TABLE_DATA_PLANE_LIST_CURSOR_VERSION: u8 = 1;
const TABLE_DATA_PLANE_LIST_CURSOR_KEY_CONTEXT: &[u8] = b"rustfs-table-list-cursor-key:v1";
const TABLE_DATA_PLANE_LIST_CURSOR_MAX_ENCODED_LEN: usize = 16 * 1024;

fn table_data_plane_list_internal_error(message: impl Into<String>) -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, message.into())
}

#[derive(Clone)]
struct TableDataPlaneListResource {
    resource: crate::table_catalog::TableDataPlaneResource,
    allowed: bool,
}

#[derive(Clone)]
pub(crate) struct TableDataPlaneListAccess {
    bucket: String,
    principal_fingerprint: String,
    snapshot_fingerprint: String,
    cursor_key: [u8; 32],
    resources: Vec<TableDataPlaneListResource>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableDataPlaneListCursor {
    version: u8,
    bucket: String,
    principal_fingerprint: String,
    snapshot_fingerprint: String,
    operation: String,
    prefix: String,
    delimiter: Option<String>,
    max_keys: usize,
    marker: Option<String>,
    version_marker: Option<String>,
    common_prefix: Option<String>,
}

pub(crate) struct TableDataPlaneListCursorPosition {
    pub(crate) marker: Option<String>,
    pub(crate) version_marker: Option<String>,
    pub(crate) common_prefix: Option<String>,
}

impl TableDataPlaneListCursor {
    pub(crate) fn marker(&self) -> Option<&str> {
        self.marker.as_deref()
    }

    pub(crate) fn version_marker(&self) -> Option<&str> {
        self.version_marker.as_deref()
    }

    pub(crate) fn common_prefix(&self) -> Option<&str> {
        self.common_prefix.as_deref()
    }
}

impl TableDataPlaneListAccess {
    #[cfg(test)]
    pub(crate) fn test_listing_fixture(bucket: &str) -> Self {
        let resource = |table: &str, prefix: &str, allowed| TableDataPlaneListResource {
            resource: crate::table_catalog::TableDataPlaneResource {
                table_bucket: bucket.to_owned(),
                namespace: "test".to_owned(),
                table: table.to_owned(),
                table_id: table.to_owned(),
                warehouse_object_prefix: prefix.to_owned(),
            },
            allowed,
        };
        Self {
            bucket: bucket.to_owned(),
            principal_fingerprint: "principal".to_owned(),
            snapshot_fingerprint: "snapshot".to_owned(),
            cursor_key: [0x42; 32],
            resources: vec![
                resource("table-id", "tables/table-id/", true),
                resource("denied-id", "tables/zz-denied/", false),
            ],
        }
    }

    pub(crate) fn allows_object(&self, object: &str) -> bool {
        if crate::table_catalog::is_reserved_table_object_key(object) {
            return false;
        }
        self.resources
            .iter()
            .find(|entry| object.starts_with(&entry.resource.warehouse_object_prefix))
            .is_none_or(|entry| entry.allowed)
    }

    pub(crate) fn encode_cursor(
        &self,
        operation: &str,
        prefix: &str,
        delimiter: Option<&str>,
        max_keys: usize,
        position: TableDataPlaneListCursorPosition,
    ) -> S3Result<String> {
        let cursor = TableDataPlaneListCursor {
            version: TABLE_DATA_PLANE_LIST_CURSOR_VERSION,
            bucket: self.bucket.clone(),
            principal_fingerprint: self.principal_fingerprint.clone(),
            snapshot_fingerprint: self.snapshot_fingerprint.clone(),
            operation: operation.to_string(),
            prefix: prefix.to_string(),
            delimiter: delimiter.map(str::to_string),
            max_keys,
            marker: position.marker,
            version_marker: position.version_marker,
            common_prefix: position.common_prefix,
        };
        let plaintext = serde_json::to_vec(&cursor).map_err(|err| {
            table_data_plane_list_internal_error(format!("failed to encode protected table list cursor: {err}"))
        })?;
        let cipher = ChaCha20Poly1305::new_from_slice(&self.cursor_key)
            .map_err(|_| table_data_plane_list_internal_error("failed to initialize protected table list cursor cipher"))?;
        let mut nonce_bytes = [0u8; 12];
        rand::rng().fill_bytes(&mut nonce_bytes);
        let nonce = chacha20poly1305::Nonce::from(nonce_bytes);
        let ciphertext = cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: &plaintext,
                    aad: TABLE_DATA_PLANE_LIST_CURSOR_PREFIX.as_bytes(),
                },
            )
            .map_err(|_| table_data_plane_list_internal_error("failed to seal protected table list cursor"))?;
        let mut token = Vec::with_capacity(nonce_bytes.len() + ciphertext.len());
        token.extend_from_slice(&nonce_bytes);
        token.extend_from_slice(&ciphertext);
        Ok(format!(
            "{}{}",
            TABLE_DATA_PLANE_LIST_CURSOR_PREFIX,
            base64_simd::URL_SAFE_NO_PAD.encode_to_string(token)
        ))
    }

    pub(crate) fn decode_cursor(
        &self,
        operation: &str,
        prefix: &str,
        delimiter: Option<&str>,
        max_keys: usize,
        token: &str,
    ) -> S3Result<TableDataPlaneListCursor> {
        if token.len() > TABLE_DATA_PLANE_LIST_CURSOR_MAX_ENCODED_LEN {
            return Err(invalid_table_data_plane_list_cursor());
        }
        let encoded = token
            .strip_prefix(TABLE_DATA_PLANE_LIST_CURSOR_PREFIX)
            .ok_or_else(invalid_table_data_plane_list_cursor)?;
        let sealed = base64_simd::URL_SAFE_NO_PAD
            .decode_to_vec(encoded.as_bytes())
            .map_err(|_| invalid_table_data_plane_list_cursor())?;
        let (nonce_bytes, ciphertext) = sealed
            .split_at_checked(12)
            .filter(|(_, ciphertext)| ciphertext.len() >= 16)
            .ok_or_else(invalid_table_data_plane_list_cursor)?;
        let cipher = ChaCha20Poly1305::new_from_slice(&self.cursor_key)
            .map_err(|_| table_data_plane_list_internal_error("failed to initialize protected table list cursor cipher"))?;
        let nonce = chacha20poly1305::Nonce::try_from(nonce_bytes).map_err(|_| invalid_table_data_plane_list_cursor())?;
        let plaintext = cipher
            .decrypt(
                &nonce,
                Payload {
                    msg: ciphertext,
                    aad: TABLE_DATA_PLANE_LIST_CURSOR_PREFIX.as_bytes(),
                },
            )
            .map_err(|_| invalid_table_data_plane_list_cursor())?;
        let cursor: TableDataPlaneListCursor =
            serde_json::from_slice(&plaintext).map_err(|_| invalid_table_data_plane_list_cursor())?;
        if cursor.version != TABLE_DATA_PLANE_LIST_CURSOR_VERSION
            || cursor.bucket != self.bucket
            || cursor.principal_fingerprint != self.principal_fingerprint
            || cursor.snapshot_fingerprint != self.snapshot_fingerprint
            || cursor.operation != operation
            || cursor.prefix != prefix
            || cursor.delimiter.as_deref() != delimiter
            || cursor.max_keys != max_keys
        {
            return Err(invalid_table_data_plane_list_cursor());
        }
        Ok(cursor)
    }
}

fn invalid_table_data_plane_list_cursor() -> S3Error {
    S3Error::with_message(S3ErrorCode::InvalidArgument, "Invalid table listing continuation token".to_string())
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

pub(crate) fn bucket_config_mutation_incarnation<R: AuthorizationRequest + ?Sized>(
    req: &R,
    bucket: &str,
) -> S3Result<Option<uuid::Uuid>> {
    let Some(snapshot) = req.bucket_config_mutation() else {
        if req.server_context().is_some() {
            return Err(s3_error!(InternalError, "bucket config mutation snapshot is missing"));
        }
        return Ok(None);
    };
    if snapshot.bucket != bucket {
        return Err(s3_error!(InternalError, "bucket config mutation snapshot does not match request bucket"));
    }
    Ok(Some(snapshot.incarnation_id))
}

async fn load_bucket_config_mutation_snapshot<R: AuthorizationRequest + ?Sized>(
    server_ctx: &ServerContextSlot,
    req: &R,
    bucket: &str,
) -> S3Result<BucketConfigMutationSnapshot> {
    let store = server_ctx
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

#[derive(Clone, Debug)]
pub(crate) struct CopySourceBucketGenerationGuard {
    bucket: String,
    incarnation_id: uuid::Uuid,
}

#[cfg(test)]
type ObjectGenerationTestHook = (String, tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>);

#[cfg(test)]
static OBJECT_GENERATION_TEST_HOOK: OnceLock<std::sync::Mutex<Option<ObjectGenerationTestHook>>> = OnceLock::new();

#[cfg(test)]
tokio::task_local! {
    static PUT_GENERATION_READ_QUORUM_FAILURE: ();
}

#[cfg(test)]
fn install_object_generation_test_hook(
    bucket: String,
    loaded: tokio::sync::oneshot::Sender<()>,
    resume: tokio::sync::oneshot::Receiver<()>,
) {
    *OBJECT_GENERATION_TEST_HOOK
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("object generation test hook lock should not be poisoned") = Some((bucket, loaded, resume));
}

#[cfg(test)]
async fn wait_for_object_generation_test_hook(bucket: &str) {
    let hook = {
        let mut slot = OBJECT_GENERATION_TEST_HOOK
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("object generation test hook lock should not be poisoned");
        if slot.as_ref().is_some_and(|(expected_bucket, _, _)| expected_bucket == bucket) {
            slot.take()
        } else {
            None
        }
    };
    if let Some((_bucket, loaded, resume)) = hook {
        let _ = loaded.send(());
        let _ = resume.await;
    }
}

#[derive(Clone, Debug)]
pub(crate) enum PendingDeleteBucketGenerationGuard {
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

pub(crate) async fn load_bucket_generation_from_store<R: AuthorizationRequest + ?Sized>(
    store: &crate::storage::storage_api::ECStore,
    req: &R,
    bucket: &str,
) -> S3Result<BucketGenerationGuard> {
    if let Some(existing) = req.bucket_generation() {
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

/// Loads the bucket incarnation through `server_ctx`'s object store, or returns the guard the
/// request already carries. The legacy hooks pass `FS::server_ctx`; the gateway edge passes the
/// envelope's server context.
pub(crate) async fn load_bucket_generation<R: AuthorizationRequest + ?Sized>(
    server_ctx: &ServerContextSlot,
    req: &R,
    bucket: &str,
) -> S3Result<BucketGenerationGuard> {
    let store = server_ctx
        .object_store()
        .ok_or_else(|| s3_error!(InternalError, "object store is not initialized"))?;
    load_bucket_generation_from_store(store.as_ref(), req, bucket).await
}

/// A read may consult only the source admitted before its authorization.
/// Capture failures are deferred until a local miss actually needs that source.
#[derive(Clone, Debug)]
pub(crate) enum OdmReadGenerationGuard {
    Unavailable,
    Ready(BucketGenerationGuard),
    Failed { code: S3ErrorCode, message: String },
}

impl OdmReadGenerationGuard {
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

fn odm_read_source_configured(bucket: &str) -> bool {
    let sys = crate::on_demand_migration::OnDemandMigrationSys::get();
    sys.is_module_enabled() && sys.state(bucket).is_some()
}

/// The on-demand-migration source generation a read may consult, captured before its
/// authorization. A capture failure is kept in the guard and surfaces only on a source miss.
pub(crate) async fn capture_odm_read_generation<R: AuthorizationRequest + ?Sized>(
    server_ctx: &ServerContextSlot,
    req: &R,
    bucket: &str,
) -> OdmReadGenerationGuard {
    if !odm_read_source_configured(bucket) {
        return OdmReadGenerationGuard::Unavailable;
    }
    OdmReadGenerationGuard::from_result(load_bucket_generation(server_ctx, req, bucket).await)
}

/// Direct usecase callers have no access middleware; capture at their entry.
/// A server request without the access marker must never bind to a later source.
pub(crate) async fn prepare_odm_read_generation<R: AuthorizationRequest + ?Sized>(
    store: &crate::storage::storage_api::ECStore,
    req: &mut R,
    bucket: &str,
) {
    if req.odm_read_generation().is_some() {
        return;
    }
    let guard = if req.server_context().is_some() || !odm_read_source_configured(bucket) {
        OdmReadGenerationGuard::Unavailable
    } else {
        OdmReadGenerationGuard::from_result(load_bucket_generation_from_store(store, req, bucket).await)
    };
    req.install_odm_read_generation(guard);
}

pub(crate) fn odm_read_generation<R: AuthorizationRequest + ?Sized>(req: &R, bucket: &str) -> S3Result<Option<uuid::Uuid>> {
    match req.odm_read_generation() {
        Some(OdmReadGenerationGuard::Ready(guard)) if guard.bucket == bucket => Ok(Some(guard.incarnation_id)),
        Some(OdmReadGenerationGuard::Ready(_)) => {
            Err(s3_error!(InternalError, "source generation guard does not match request bucket"))
        }
        Some(OdmReadGenerationGuard::Failed { code, message }) => Err(S3Error::with_message(code.clone(), message.clone())),
        Some(OdmReadGenerationGuard::Unavailable) | None => Ok(None),
    }
}

/// The legacy decoder's copy source as owned parts; `None` for an access-point or Outposts ARN.
fn legacy_copy_source(source: &CopySource) -> Option<(String, String, Option<String>)> {
    match source {
        CopySource::AccessPoint { .. } | CopySource::Outpost { .. } => None,
        CopySource::Bucket { bucket, key, version_id } => {
            Some((bucket.to_string(), key.to_string(), version_id.as_ref().map(|v| v.to_string())))
        }
    }
}

fn copy_source_object<'a>(
    bucket: &'a str,
    key: &'a str,
    version_id: Option<&'a str>,
) -> access_owned::object_commit::CopySourceObject<'a> {
    access_owned::object_commit::CopySourceObject { bucket, key, version_id }
}

pub(crate) fn apply_bucket_generation_guard<R: AuthorizationRequest + ?Sized>(
    req: &R,
    bucket: &str,
    opts: &mut ObjectOptions,
) -> S3Result<()> {
    let guard = match req.pending_delete_bucket_generation() {
        Some(PendingDeleteBucketGenerationGuard::Ready(guard)) => Some(guard),
        Some(PendingDeleteBucketGenerationGuard::Failed { code, message }) => {
            return Err(S3Error::with_message(code.clone(), message.clone()));
        }
        None => req.bucket_generation(),
    };
    apply_generation_fence(
        req,
        guard.map(|guard| (guard.bucket.as_str(), guard.incarnation_id)),
        bucket,
        opts,
        "bucket generation guard",
    )
}

pub(crate) fn retained_table_data_plane_publication_guards<R: AuthorizationRequest + ?Sized>(
    req: &R,
) -> Option<TableDataPlanePublicationGuards> {
    req.publication_guards().cloned()
}

pub(crate) fn apply_copy_source_bucket_generation_guard<R: AuthorizationRequest + ?Sized>(
    req: &R,
    bucket: &str,
    opts: &mut ObjectOptions,
) -> S3Result<()> {
    let guard = req.copy_source_bucket_generation();
    apply_generation_fence(
        req,
        guard.map(|guard| (guard.bucket.as_str(), guard.incarnation_id)),
        bucket,
        opts,
        "copy source bucket generation guard",
    )
}

/// Binds `opts` to the captured incarnation of `bucket` and to the retained table publication
/// locks. A server request without a captured guard is refused: only direct use-case callers,
/// which carry no server context, run unfenced.
fn apply_generation_fence<R: AuthorizationRequest + ?Sized>(
    req: &R,
    guard: Option<(&str, uuid::Uuid)>,
    bucket: &str,
    opts: &mut ObjectOptions,
    guard_name: &str,
) -> S3Result<()> {
    match guard {
        Some((guarded, _)) if guarded != bucket => {
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                format!("{guard_name} does not match request bucket"),
            ));
        }
        Some((_, incarnation_id)) => opts.expected_bucket_incarnation_id = Some(incarnation_id),
        None if req.server_context().is_some() => {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, format!("{guard_name} is missing")));
        }
        None => {}
    }
    if let Some(publication_guards) = req.publication_guards() {
        publication_guards.add_lock_loss_fences(opts);
    }
    Ok(())
}

fn missing_req_info_error() -> S3Error {
    s3_error!(InternalError, "ReqInfo not found in request extensions")
}

pub(crate) fn req_info_ref<R: AuthorizationRequest + ?Sized>(req: &R) -> S3Result<&ReqInfo> {
    req.req_info().ok_or_else(missing_req_info_error)
}

pub(crate) fn req_info_mut<R: AuthorizationRequest + ?Sized>(req: &mut R) -> S3Result<&mut ReqInfo> {
    req.req_info_mut().ok_or_else(missing_req_info_error)
}

fn ext_req_info_mut(ext: &mut http::Extensions) -> S3Result<&mut ReqInfo> {
    ext.get_mut::<ReqInfo>().ok_or_else(missing_req_info_error)
}

/// Extract the canonical `RequestContext` from a request, checking both
/// the request extensions directly and the `ReqInfo.request_context` field.
pub(crate) fn request_context_from_req<R: AuthorizationRequest + ?Sized>(req: &R) -> Option<RequestContext> {
    req.request_context()
        .cloned()
        .or_else(|| req.req_info().and_then(|req_info| req_info.request_context.clone()))
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

/// Authorization-phase payloads for request-envelope tests, which cannot reach
/// the private fields. Production code builds them only through the
/// authorization paths in this module.
#[cfg(test)]
pub(crate) mod envelope_payloads {
    use super::{
        BucketConfigMutationSnapshot, BucketGenerationGuard, CopySourceBucketGenerationGuard, HashMap, ObjectTagConditions,
        OdmReadGenerationGuard, PendingDeleteBucketGenerationGuard, TableDataPlaneListAccess,
    };

    fn incarnation() -> uuid::Uuid {
        uuid::Uuid::from_u128(0x7107)
    }

    pub(crate) fn bucket_generation(bucket: &str) -> BucketGenerationGuard {
        BucketGenerationGuard {
            bucket: bucket.to_owned(),
            incarnation_id: incarnation(),
        }
    }

    pub(crate) fn pending_delete_bucket_generation(bucket: &str) -> PendingDeleteBucketGenerationGuard {
        PendingDeleteBucketGenerationGuard::Ready(bucket_generation(bucket))
    }

    pub(crate) fn copy_source_bucket_generation(bucket: &str) -> CopySourceBucketGenerationGuard {
        CopySourceBucketGenerationGuard {
            bucket: bucket.to_owned(),
            incarnation_id: incarnation(),
        }
    }

    pub(crate) fn odm_read_generation(bucket: &str) -> OdmReadGenerationGuard {
        OdmReadGenerationGuard::Ready(bucket_generation(bucket))
    }

    pub(crate) fn bucket_config_mutation(bucket: &str) -> BucketConfigMutationSnapshot {
        BucketConfigMutationSnapshot {
            bucket: bucket.to_owned(),
            incarnation_id: incarnation(),
        }
    }

    pub(crate) fn table_list_access(bucket: &str) -> TableDataPlaneListAccess {
        TableDataPlaneListAccess {
            bucket: bucket.to_owned(),
            principal_fingerprint: String::new(),
            snapshot_fingerprint: String::new(),
            cursor_key: [0; 32],
            resources: Vec::new(),
        }
    }

    pub(crate) fn object_tag_conditions(bucket: &str) -> ObjectTagConditions {
        ObjectTagConditions::new(bucket, "object", None, HashMap::new())
    }
}

/// Local tag evidence cannot authorize an object that only exists on a replica.
/// Keep the local miss whenever authorization needed existing-object tags.
pub(crate) fn replication_proxy_allowed(extensions: &http::Extensions) -> bool {
    extensions.get::<ObjectTagConditions>().is_none()
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

/// Reuse the parsed bucket metadata policy for ExistingObjectTag hint checks.
async fn load_bucket_policy_existing_object_tag_hint(
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

/// The request facts the shared authorization core reads. Both edges feed the
/// same policy evaluation without constructing another request type; the
/// owned `RequestEnvelope` implements it in the app layer.
pub(crate) trait AuthorizationRequest {
    fn headers(&self) -> &HeaderMap;
    fn headers_mut(&mut self) -> &mut HeaderMap;
    fn uri(&self) -> &http::Uri;
    fn remote_addr(&self) -> Option<std::net::SocketAddr>;
    fn client_info(&self) -> Option<&ClientInfo>;
    fn server_context(&self) -> Option<&Arc<ServerContextSlot>>;
    fn req_info(&self) -> Option<&ReqInfo>;
    fn req_info_mut(&mut self) -> Option<&mut ReqInfo>;
    fn request_context(&self) -> Option<&RequestContext>;
    fn bucket_generation(&self) -> Option<&BucketGenerationGuard>;
    fn install_bucket_generation(&mut self, guard: BucketGenerationGuard);
    fn pending_delete_bucket_generation(&self) -> Option<&PendingDeleteBucketGenerationGuard>;
    fn install_pending_delete_bucket_generation(&mut self, guard: PendingDeleteBucketGenerationGuard);
    fn copy_source_bucket_generation(&self) -> Option<&CopySourceBucketGenerationGuard>;
    fn odm_read_generation(&self) -> Option<&OdmReadGenerationGuard>;
    fn install_odm_read_generation(&mut self, guard: OdmReadGenerationGuard);
    fn bucket_config_mutation(&self) -> Option<&BucketConfigMutationSnapshot>;
    fn install_bucket_config_mutation(&mut self, snapshot: BucketConfigMutationSnapshot);
    /// The extensions no typed accessor covers, for test-only payloads.
    #[cfg(test)]
    fn test_extensions(&self) -> &http::Extensions;
    fn internal_object_authorization(&self) -> bool;
    fn multipart_staging(&self) -> bool;
    fn staged_multipart_part(&self) -> bool;
    fn requested_retention(&self) -> Option<&RequestedObjectLockRetention>;
    fn requested_retain_until(&self) -> Option<&RequestedObjectLockRetainUntil>;
    fn requested_legal_hold(&self) -> Option<&RequestedObjectLegalHold>;
    fn requested_object_tags(&self) -> Option<&RequestedObjectTags>;
    fn requested_copy_source(&self) -> Option<&RequestedCopySource>;
    fn install_requested_copy_source(&mut self, source: RequestedCopySource);
    fn install_copy_source_bucket_generation(&mut self, guard: CopySourceBucketGenerationGuard);
    /// The headers that describe what the request stores: a POST upload's form
    /// fields when present, otherwise the request headers.
    fn policy_headers(&self) -> &HeaderMap;
    fn object_tag_conditions(&self) -> Option<&ObjectTagConditions>;
    fn install_object_tag_conditions(&mut self, conditions: ObjectTagConditions);
    fn publication_guards(&self) -> Option<&TableDataPlanePublicationGuards>;
    fn install_publication_guards(&mut self, guards: TableDataPlanePublicationGuards);
    fn audit_denial(&self, auditor: &DenialAuditor, action: &Action, reason: &'static str);
}

impl<T> AuthorizationRequest for S3Request<T> {
    fn headers(&self) -> &HeaderMap {
        &self.headers
    }
    fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.headers
    }
    fn uri(&self) -> &http::Uri {
        &self.uri
    }
    fn remote_addr(&self) -> Option<std::net::SocketAddr> {
        self.extensions
            .get::<Option<RemoteAddr>>()
            .and_then(|addr| addr.as_ref().map(|addr| addr.0))
    }
    fn client_info(&self) -> Option<&ClientInfo> {
        self.extensions.get::<ClientInfo>()
    }
    fn server_context(&self) -> Option<&Arc<ServerContextSlot>> {
        self.extensions.get::<Arc<ServerContextSlot>>()
    }
    fn req_info(&self) -> Option<&ReqInfo> {
        self.extensions.get::<ReqInfo>()
    }
    fn req_info_mut(&mut self) -> Option<&mut ReqInfo> {
        self.extensions.get_mut::<ReqInfo>()
    }
    fn request_context(&self) -> Option<&RequestContext> {
        self.extensions.get::<RequestContext>()
    }
    fn bucket_generation(&self) -> Option<&BucketGenerationGuard> {
        self.extensions.get::<BucketGenerationGuard>()
    }
    fn install_bucket_generation(&mut self, guard: BucketGenerationGuard) {
        self.extensions.insert(guard);
    }
    fn pending_delete_bucket_generation(&self) -> Option<&PendingDeleteBucketGenerationGuard> {
        self.extensions.get::<PendingDeleteBucketGenerationGuard>()
    }
    fn install_pending_delete_bucket_generation(&mut self, guard: PendingDeleteBucketGenerationGuard) {
        self.extensions.insert(guard);
    }
    fn copy_source_bucket_generation(&self) -> Option<&CopySourceBucketGenerationGuard> {
        self.extensions.get::<CopySourceBucketGenerationGuard>()
    }
    fn odm_read_generation(&self) -> Option<&OdmReadGenerationGuard> {
        self.extensions.get::<OdmReadGenerationGuard>()
    }
    fn install_odm_read_generation(&mut self, guard: OdmReadGenerationGuard) {
        self.extensions.insert(guard);
    }
    fn bucket_config_mutation(&self) -> Option<&BucketConfigMutationSnapshot> {
        self.extensions.get::<BucketConfigMutationSnapshot>()
    }
    fn install_bucket_config_mutation(&mut self, snapshot: BucketConfigMutationSnapshot) {
        self.extensions.insert(snapshot);
    }
    #[cfg(test)]
    fn test_extensions(&self) -> &http::Extensions {
        &self.extensions
    }
    fn internal_object_authorization(&self) -> bool {
        self.extensions.get::<InternalObjectAuthorization>().is_some()
    }
    fn multipart_staging(&self) -> bool {
        self.extensions.get::<MultipartStagingOperation>().is_some()
    }
    fn staged_multipart_part(&self) -> bool {
        self.extensions.get::<StagedMultipartPartAuthorization>().is_some()
    }
    fn requested_retention(&self) -> Option<&RequestedObjectLockRetention> {
        self.extensions.get::<RequestedObjectLockRetention>()
    }
    fn requested_retain_until(&self) -> Option<&RequestedObjectLockRetainUntil> {
        self.extensions.get::<RequestedObjectLockRetainUntil>()
    }
    fn requested_legal_hold(&self) -> Option<&RequestedObjectLegalHold> {
        self.extensions.get::<RequestedObjectLegalHold>()
    }
    fn requested_object_tags(&self) -> Option<&RequestedObjectTags> {
        self.extensions.get::<RequestedObjectTags>()
    }
    fn requested_copy_source(&self) -> Option<&RequestedCopySource> {
        self.extensions.get::<RequestedCopySource>()
    }
    fn install_requested_copy_source(&mut self, source: RequestedCopySource) {
        self.extensions.insert(source);
    }
    fn install_copy_source_bucket_generation(&mut self, guard: CopySourceBucketGenerationGuard) {
        self.extensions.insert(guard);
    }
    fn policy_headers(&self) -> &HeaderMap {
        self.extensions
            .get::<PostObjectPolicyHeaders>()
            .map_or(&self.headers, |headers| &headers.0)
    }
    fn object_tag_conditions(&self) -> Option<&ObjectTagConditions> {
        self.extensions.get::<ObjectTagConditions>()
    }
    fn install_object_tag_conditions(&mut self, conditions: ObjectTagConditions) {
        self.extensions.insert(conditions);
    }
    fn publication_guards(&self) -> Option<&TableDataPlanePublicationGuards> {
        self.extensions.get::<TableDataPlanePublicationGuards>()
    }
    fn install_publication_guards(&mut self, guards: TableDataPlanePublicationGuards) {
        self.extensions.insert(guards);
    }
    fn audit_denial(&self, auditor: &DenialAuditor, action: &Action, reason: &'static str) {
        auditor.audit(self, action, reason, DenialScope::Request);
    }
}

/// The request facts of `req` for an authorization decision about `target`, read now.
fn request_authz_facts<R: AuthorizationRequest + ?Sized>(req: &R, mut target: AuthzTarget<'_>) -> AuthzFacts {
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
}

/// The root access key a principal is compared against to be the `Account`.
fn root_access_key() -> String {
    runtime_sources::current_action_credentials()
        .map(|root| root.access_key)
        .unwrap_or_default()
}

fn authorization_conditions<R: AuthorizationRequest + ?Sized>(
    req: &R,
    principal: &AuthzPrincipal<'_>,
    facts: &AuthzFacts,
) -> S3Result<HashMap<String, Vec<String>>> {
    authorization_conditions_with_internal(req, principal, facts, req.internal_object_authorization())
}

fn authorization_conditions_with_internal<R: AuthorizationRequest + ?Sized>(
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
    if let Some(RequestedObjectLegalHold(legal_hold)) = req.requested_legal_hold() {
        // Standalone legal hold writes apply the XML body, so headers must not
        // supply a different value to the policy decision.
        conditions.remove("object-lock-legal-hold");
        if let Some(status) = &legal_hold.status {
            conditions.insert("object-lock-legal-hold".to_string(), vec![status.as_str().to_string()]);
        }
    }
    if let Some(RequestedObjectTags(tagging)) = req.requested_object_tags() {
        // Standalone tag writes apply the XML body; the x-amz-tagging header is
        // not stored and must not describe the request to the policy engine.
        conditions.retain(|key, _| key != "RequestObjectTagKeys" && !key.starts_with("RequestObjectTag/"));
        let mut keys = Vec::new();
        for tag in &tagging.tag_set {
            if let (Some(key), Some(value)) = (&tag.key, &tag.value) {
                keys.push(key.clone());
                conditions
                    .entry(format!("RequestObjectTag/{key}"))
                    .or_default()
                    .push(value.clone());
            }
        }
        conditions.insert("RequestObjectTagKeys".to_string(), keys);
    }
    if let Some(RequestedObjectLockRetention(retention)) = req.requested_retention() {
        // Standalone retention writes apply the XML body, so headers must not
        // supply a different value to the policy decision.
        conditions.remove("object-lock-mode");
        conditions.remove("object-lock-retain-until-date");
        if let Some(mode) = &retention.mode {
            conditions.insert("object-lock-mode".to_string(), vec![mode.as_str().to_string()]);
        }
        if let Some(date) = &retention.retain_until_date {
            let date = OffsetDateTime::from(date.clone())
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
    Ok(conditions)
}

/// Records the retain-until date a request asks to set, or clears it when the
/// request sets none. Call it before `authorize_request` with the parsed date from
/// headers, form fields, the PutObjectRetention XML body, or an archive member.
pub(crate) fn set_requested_object_lock_retain_until<T>(req: &mut S3Request<T>, retain_until: Option<&Timestamp>) {
    match retain_until {
        Some(retain_until) => {
            req.extensions
                .insert(RequestedObjectLockRetainUntil(OffsetDateTime::from(retain_until.clone())));
        }
        None => {
            req.extensions.remove::<RequestedObjectLockRetainUntil>();
        }
    }
}

/// Keep the XML value literal for IAM; endpoint aliases are resolved later.
fn requested_bucket_location_constraint(input: &CreateBucketInput) -> Option<String> {
    input
        .create_bucket_configuration
        .as_ref()
        .and_then(|configuration| configuration.location_constraint.as_ref())
        .map(BucketLocationConstraint::as_str)
        .filter(|location| !location.is_empty())
        .map(str::to_owned)
}

fn set_requested_object_lock_retention(req: &mut S3Request<PutObjectRetentionInput>) {
    let retention = req.input.retention.clone().unwrap_or_default();
    set_requested_object_lock_retain_until(req, retention.retain_until_date.as_ref());
    req.extensions.insert(RequestedObjectLockRetention(retention));
}

/// A POST upload stores the object policy inputs from its form fields, while
/// header-derived condition keys would otherwise read the HTTP headers. Project
/// the form fields over the headers for the policy decision, removing a header
/// whose field is absent so a client header cannot stand in for a stored value.
fn set_post_object_policy_headers(req: &mut S3Request<PutObjectInput>) -> S3Result<()> {
    if req.method != http::Method::POST {
        return Ok(());
    }
    let input = &req.input;
    let retain_until = input
        .object_lock_retain_until_date
        .as_ref()
        .map(|date| OffsetDateTime::from(date.clone()).format(&time::format_description::well_known::Rfc3339))
        .transpose()
        .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidArgument, "Invalid retain-until date"))?;
    let mut headers = req.headers.clone();
    for (key, value) in [
        ("x-amz-object-lock-mode", input.object_lock_mode.as_ref().map(ObjectLockMode::as_str)),
        (
            "x-amz-object-lock-legal-hold",
            input
                .object_lock_legal_hold_status
                .as_ref()
                .map(ObjectLockLegalHoldStatus::as_str),
        ),
        ("x-amz-object-lock-retain-until-date", retain_until.as_deref()),
        ("x-amz-tagging", input.tagging.as_deref()),
        ("x-amz-acl", input.acl.as_ref().map(ObjectCannedACL::as_str)),
        ("x-amz-storage-class", input.storage_class.as_ref().map(StorageClass::as_str)),
        (
            "x-amz-server-side-encryption",
            input.server_side_encryption.as_ref().map(ServerSideEncryption::as_str),
        ),
        ("x-amz-server-side-encryption-aws-kms-key-id", input.ssekms_key_id.as_deref()),
        ("x-amz-server-side-encryption-customer-algorithm", input.sse_customer_algorithm.as_deref()),
        ("x-amz-grant-full-control", input.grant_full_control.as_deref()),
        ("x-amz-grant-read", input.grant_read.as_deref()),
        ("x-amz-grant-write", None),
        ("x-amz-grant-read-acp", input.grant_read_acp.as_deref()),
        ("x-amz-grant-write-acp", input.grant_write_acp.as_deref()),
    ] {
        headers.remove(key);
        if let Some(value) = value {
            let value = value
                .parse()
                .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidArgument, "Invalid form field"))?;
            headers.insert(key, value);
        }
    }
    for (key, value) in [("if-match", &input.if_match), ("if-none-match", &input.if_none_match)] {
        headers.remove(key);
        if let Some(value) = value {
            headers.insert(
                key,
                value
                    .to_http_header()
                    .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidArgument, "Invalid form field"))?,
            );
        }
    }
    req.extensions.insert(PostObjectPolicyHeaders(headers));
    Ok(())
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
fn remaining_retention_days(retain_until: OffsetDateTime, now: OffsetDateTime) -> i64 {
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
    get_condition_values_with_client_info(req.headers(), cred, None, None, req.remote_addr(), req.client_info())
}

fn request_object_store<R: AuthorizationRequest + ?Sized>(req: &R) -> S3Result<Arc<ECStore>> {
    match req.server_context() {
        Some(server_ctx) => server_ctx
            .installed_object_store()
            .ok_or_else(object_store_not_initialized_error),
        None => runtime_sources::current_object_store_handle().ok_or_else(object_store_not_initialized_error),
    }
}

fn object_store_not_initialized_error() -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, "object store is not initialized")
}

fn request_iam_store<R: AuthorizationRequest + ?Sized>(req: &R) -> S3Result<Arc<IamSys<ObjectStore>>> {
    let iam_store = match req.server_context() {
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

/// What an edge's signature verification established about a request, as the access phase
/// records it for the readers of [`VerifiedSigV4Request`] and [`VerifiedPresignedRequest`].
///
/// Each edge derives it from its own verdict; the legacy edge from the signature form s3s verified.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct VerifiedSignature {
    /// A SigV4 signature covered the request line, in the header or the query.
    pub(crate) sigv4: bool,
    /// The SigV4 signature was carried in the query.
    pub(crate) presigned: bool,
}

impl VerifiedSignature {
    /// The legacy edge's reading: s3s verified the signature form these headers name.
    pub(crate) fn from_auth_type(auth_type: AuthType) -> Self {
        Self {
            sigv4: matches!(auth_type, AuthType::Presigned | AuthType::Signed),
            presigned: matches!(auth_type, AuthType::Presigned),
        }
    }
}

/// The payloads the access phase hands every later reader of a request: the signature markers
/// and, for an operation that stages an upload, [`MultipartStagingOperation`].
#[derive(Clone, Copy, Debug)]
pub(crate) struct AccessMarkers {
    staging: Option<MultipartStagingOperation>,
    signature: VerifiedSignature,
}

impl AccessMarkers {
    /// The markers of the S3 operation named `operation`, signed as `signature` says.
    pub(crate) fn new(operation: &str, signature: VerifiedSignature) -> Self {
        Self {
            staging: MultipartStagingOperation::for_operation(operation),
            signature,
        }
    }

    pub(crate) fn install(self, extensions: &mut http::Extensions) {
        if let Some(staging) = self.staging {
            extensions.insert(staging);
        }
        if self.signature.presigned {
            extensions.insert(VerifiedPresignedRequest);
        }
        if self.signature.sigv4 {
            extensions.insert(VerifiedSigV4Request);
        }
    }
}

/// Resolves the caller of one request as the access phase of either edge needs it: the credential
/// IAM holds for `access_key` (validated against `session_token`), whether it owns the deployment,
/// and the request-scoped facts later authorization steps read.
///
/// `access_key` is the key whose signature the edge verified; `None` is an anonymous request.
/// Resolved against `server_ctx` so each embedded server reads its own IAM domain.
///
/// # Errors
///
/// What [`check_key_valid_with_context`] refuses: an unknown, disabled or expired credential, a
/// session token that does not belong to it, or an IAM system that is not initialized.
pub(crate) async fn resolve_req_info(
    server_ctx: &Arc<ServerContextSlot>,
    access_key: Option<&str>,
    session_token: &str,
    request_context: Option<RequestContext>,
) -> S3Result<ReqInfo> {
    let app_context = server_ctx.app_context();
    let (cred, is_owner) = match access_key {
        Some(access_key) => {
            let (cred, is_owner) = check_key_valid_with_context(session_token, access_key, app_context.as_deref()).await?;
            (Some(cred), is_owner)
        }
        None => (None, false),
    };
    let region = match app_context.as_deref() {
        Some(context) => context.region().get(),
        None => runtime_sources::current_region(),
    };
    Ok(ReqInfo {
        cred,
        is_owner,
        region,
        request_context,
        ..Default::default()
    })
}

/// The operation-aware admission every S3 request passes after its caller is resolved and before
/// its own authorization: a signed upload-size capability is refused on any operation but the one
/// it is scoped to, and the license must be valid.
///
/// # Errors
///
/// `InvalidRequest` for a misplaced capability query, `AccessDenied` for a refused license, and
/// `InternalError` when the license cannot be checked.
pub(crate) fn admit_operation(
    headers: &HeaderMap,
    raw_query: Option<&str>,
    operation: &str,
    signature: VerifiedSignature,
) -> S3Result<()> {
    // The size capability is intentionally scoped to the single-object
    // PutObject operation. Validate this at the operation-aware access
    // boundary so unsupported GET/HEAD/DELETE/bucket routes cannot silently
    // ignore a signed capability query.
    if parse_presigned_put_max_content_length(headers, raw_query, signature.presigned)?.is_some() && operation != "PutObject" {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            format!("{RUSTFS_MAX_CONTENT_LENGTH_QUERY} is only supported for presigned PutObject"),
        ));
    }
    if parse_presigned_multipart_max_total_object_size(headers, raw_query, signature.sigv4)?.is_some()
        && operation != "CreateMultipartUpload"
    {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            format!("{RUSTFS_MAX_TOTAL_OBJECT_SIZE_QUERY} is only supported for CreateMultipartUpload"),
        ));
    }
    license_check().map_err(|er| match er.kind() {
        std::io::ErrorKind::PermissionDenied => s3_error!(AccessDenied, "{er}"),
        _ => {
            tracing::error!("license check failed due to unexpected error: {er}");
            s3_error!(InternalError, "License validation failed")
        }
    })
}

#[cfg(test)]
mod access_phase_tests {
    use super::{
        AccessMarkers, AuthType, MultipartStagingOperation, VerifiedPresignedRequest, VerifiedSigV4Request, VerifiedSignature,
    };

    #[test]
    fn only_a_sigv4_header_or_query_signature_is_marked_verified() {
        for (auth_type, expected) in [
            (AuthType::Signed, (true, false)),
            (AuthType::Presigned, (true, true)),
            (AuthType::SignedV2, (false, false)),
            (AuthType::PresignedV2, (false, false)),
            (AuthType::StreamingSigned, (false, false)),
            (AuthType::PostPolicy, (false, false)),
            (AuthType::Anonymous, (false, false)),
        ] {
            let signature = VerifiedSignature::from_auth_type(auth_type.clone());
            assert_eq!((signature.sigv4, signature.presigned), expected, "{auth_type:?}");
        }
    }

    #[test]
    fn each_marker_is_installed_exactly_when_its_fact_holds() {
        for (operation, sigv4, presigned) in [
            ("ListBuckets", false, false),
            ("ListBuckets", true, false),
            ("ListBuckets", true, true),
            ("CreateMultipartUpload", false, false),
            ("UploadPart", true, true),
            ("UploadPartCopy", true, false),
            ("CompleteMultipartUpload", true, false),
            ("PutObject", true, true),
        ] {
            let mut extensions = http::Extensions::new();
            AccessMarkers::new(operation, VerifiedSignature { sigv4, presigned }).install(&mut extensions);

            let staging = matches!(operation, "CreateMultipartUpload" | "UploadPart" | "UploadPartCopy");
            assert_eq!(extensions.get::<MultipartStagingOperation>().is_some(), staging, "{operation}");
            assert_eq!(extensions.get::<VerifiedSigV4Request>().is_some(), sigv4, "{operation}");
            assert_eq!(extensions.get::<VerifiedPresignedRequest>().is_some(), presigned, "{operation}");
        }
    }
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
pub(crate) async fn prepare_list_buckets_iam_authorization<R: AuthorizationRequest + ?Sized>(
    req: &R,
) -> S3Result<ListBucketsIamAuthorization> {
    let req_info = req.req_info().ok_or_else(missing_req_info_error)?;
    let Some(cred) = req_info.cred.as_ref() else {
        return Err(ApiError::access_denied().into());
    };
    let iam_store = request_iam_store(req)?;
    let account = cred.access_key.clone();
    let groups = cred.groups.clone();
    let claims = cred.claims.clone().unwrap_or_default();
    let action = Action::S3Action(S3Action::ListAllMyBucketsAction);
    let root_access_key = root_access_key();
    let principal = AuthzPrincipal::new(cred, &root_access_key);
    let facts = request_authz_facts(
        req,
        AuthzTarget {
            action,
            bucket: "",
            object: "",
            version_id: None,
            location_constraint: None,
        },
    );
    let base_conditions = authorization_conditions(req, &principal, &facts)?;
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
pub(crate) fn log_list_buckets_iam_implicit_deny<R: AuthorizationRequest + ?Sized>(req: &R) -> S3Result<()> {
    let req_info = req.req_info().ok_or_else(missing_req_info_error)?;
    let denial = DenialContext {
        quiet: true,
        suppress_audit: true,
        auditor: DenialAuditor::global(),
        bucket: req_info.bucket.as_deref().unwrap_or_default(),
        object: req_info.object.as_deref().unwrap_or_default(),
        version_id: req_info.version_id.as_deref(),
        account: req_info.cred.as_ref().map(|cred| cred.access_key.as_str()),
        is_owner: req_info.is_owner,
    };
    denial.log("iam_implicit_deny", Action::S3Action(S3Action::ListAllMyBucketsAction));
    Ok(())
}

pub(crate) fn delete_object_authorize_action(version_id: Option<&str>) -> Action {
    Action::S3Action(if version_id.is_some() {
        S3Action::DeleteObjectVersionAction
    } else {
        S3Action::DeleteObjectAction
    })
}

/// GHSA-3ppv: select the IAM action for an object read by whether the request
/// names an explicit object version. A read that targets a specific version must
/// authorize against `s3:GetObjectVersion`; a current-object read authorizes
/// against `s3:GetObject`. Reading a historical version while only holding
/// `s3:GetObject` is an information-disclosure bypass, so the two must not be
/// conflated at the authorization boundary.
///
/// Note: `ActionSet::is_match` still maps a `s3:GetObjectVersion` grant onto a
/// `s3:GetObject` request. It only broadens a Version grant toward current
/// reads, never the reverse.
pub(crate) fn versioned_read_action(version_id: Option<&str>) -> Action {
    if version_id.is_some() {
        Action::S3Action(S3Action::GetObjectVersionAction)
    } else {
        Action::S3Action(S3Action::GetObjectAction)
    }
}

pub(crate) fn versioned_tagging_action(action: S3Action, version_id: Option<&str>) -> Action {
    let action = if version_id.is_some() {
        match action {
            S3Action::GetObjectTaggingAction => S3Action::GetObjectVersionTaggingAction,
            S3Action::PutObjectTaggingAction => S3Action::PutObjectVersionTaggingAction,
            S3Action::DeleteObjectTaggingAction => S3Action::DeleteObjectVersionTaggingAction,
            action => action,
        }
    } else {
        action
    };
    Action::S3Action(action)
}

async fn get_or_fetch_object_tag_conditions<R: AuthorizationRequest + ?Sized>(
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

async fn maybe_merge_object_tag_conditions<R: AuthorizationRequest + ?Sized>(
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
    suppress_audit: bool,
    auditor: &'a DenialAuditor,
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

    /// Log the denial and, unless it is a quiet probe denial or audited in aggregate by the
    /// caller, audit it.
    fn record<R: AuthorizationRequest + ?Sized>(&self, req: &R, reason: &'static str, action: Action) {
        self.log(reason, action);
        if !self.quiet && !self.suppress_audit {
            req.audit_denial(self.auditor, &action, reason);
        }
    }

    /// Build the generic AccessDenied response after recording the structured
    /// denial event and, unless this is a quiet probe denial, an audit entry.
    fn deny<R: AuthorizationRequest + ?Sized>(&self, req: &R, reason: &'static str, action: Action) -> S3Error {
        self.record(req, reason, action);
        s3_error!(AccessDenied, "Access Denied")
    }
}

/// Audits a denial at an exit that has no [`DenialContext`] of its own. It only audits: those exits
/// keep their existing logging. The request's own probe flags are honored.
fn audit_request_denial<R: AuthorizationRequest + ?Sized>(
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

/// Audits the refused keys of a bulk request (DeleteObjects) in aggregate instead of once per key.
///
/// `start` marks the request so per-key denials are logged but not audited; `finish` clears the
/// mark and emits one entry per denied action with the number of keys refused for it.
pub(crate) struct BulkDenialAudit {
    denied: Vec<(Action, u64)>,
}

impl BulkDenialAudit {
    pub(crate) fn start<T>(req: &mut S3Request<T>) -> S3Result<Self> {
        ext_req_info_mut(&mut req.extensions)?.suppress_denial_audit = true;
        Ok(Self { denied: Vec::new() })
    }

    /// Counts one refused key for `action`.
    pub(crate) fn denied(&mut self, action: Action) {
        match self.denied.iter_mut().find(|(denied_action, _)| *denied_action == action) {
            Some((_, count)) => *count += 1,
            None => self.denied.push((action, 1)),
        }
    }

    pub(crate) fn finish<T>(self, req: &mut S3Request<T>) -> S3Result<()> {
        self.finish_with(req, DenialAuditor::global())
    }

    fn finish_with<T>(self, req: &mut S3Request<T>, auditor: &DenialAuditor) -> S3Result<()> {
        ext_req_info_mut(&mut req.extensions)?.suppress_denial_audit = false;
        for (action, keys) in self.denied {
            auditor.audit(req, &action, "bulk_request_keys_denied", DenialScope::Keys(keys));
        }
        Ok(())
    }
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

/// Authorize the exact tag set selected by CopyObject's source reader before publication.
/// The access hook also calls this with a preliminary snapshot, so tag-dependent
/// destination Allow statements can be evaluated before opening either object.
pub(crate) async fn authorize_copy_object_tags(
    req: &mut S3Request<CopyObjectInput>,
    source_tags: &str,
    effective_tags: &str,
) -> S3Result<()> {
    // Direct internal use-case calls with no tags retain their existing contract.
    // Public CopyObject requests always carry the access hook's requested-tag snapshot.
    if effective_tags.is_empty() && req.extensions.get::<RequestedObjectTags>().is_none() {
        return Ok(());
    }
    let tag_set = crate::storage::s3_api::tagging::parse_object_tag_header(effective_tags)?;
    let copies_tags = req.input.tagging_directive.as_ref().map(TaggingDirective::as_str) != Some(TaggingDirective::REPLACE);
    if copies_tags && !tag_set.is_empty() {
        let CopySource::Bucket { bucket, key, version_id } = &req.input.copy_source else {
            return Err(s3_error!(NotImplemented));
        };
        let (bucket, key, version_id) = (bucket.to_string(), key.to_string(), version_id.as_ref().map(ToString::to_string));
        let destination = req_info_ref(req)?.clone();
        let destination_tags = req.extensions.remove::<ObjectTagConditions>();
        req.extensions.remove::<RequestedObjectTags>();
        // Reuse the reader's metadata instead of reopening the source under its
        // read guard (or the self-copy write guard). These are the tags being copied.
        let source_conditions = crate::storage::s3_api::tagging::parse_object_tag_header(source_tags)?
            .into_iter()
            .filter_map(|tag| Some((format!("ExistingObjectTag/{}", tag.key?), vec![tag.value?])))
            .collect();
        req.extensions
            .insert(ObjectTagConditions::new(&bucket, &key, version_id.as_deref(), source_conditions));
        let info = req_info_mut(req)?;
        info.bucket = Some(bucket);
        info.object = Some(key);
        info.version_id = version_id.clone();
        let result =
            authorize_request(req, versioned_tagging_action(S3Action::GetObjectTaggingAction, version_id.as_deref())).await;
        req.extensions.insert(destination);
        req.extensions.remove::<ObjectTagConditions>();
        if let Some(tags) = destination_tags {
            req.extensions.insert(tags);
        }
        result?;
    }
    req.extensions.insert(RequestedObjectTags(Tagging { tag_set }));
    authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await
}

/// A specific destination ETag reveals stored content and requires read access.
/// Wildcard existence conditions retain the existing write-only contract.
async fn authorize_write_etag_condition<T>(req: &mut S3Request<T>, specific_dto_etag: bool) -> S3Result<()> {
    let headers = req
        .extensions
        .get::<PostObjectPolicyHeaders>()
        .map_or(&req.headers, |form| &form.0);
    let specific_header_etag = headers
        .get_all(http::header::IF_MATCH)
        .iter()
        .any(|value| value.to_str().map_or(true, |value| value.trim() != "*"));
    if specific_dto_etag || specific_header_etag {
        let action = versioned_read_action(req_info_ref(req)?.version_id.as_deref());
        authorize_request(req, action).await?;
    }
    Ok(())
}

async fn authorize_request_with_location_constraint<T>(
    req: &mut S3Request<T>,
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
        let principal = AuthzPrincipal::new(cred, &root_access_key).with_owner(is_owner);
        let facts = request_authz_facts(
            req,
            AuthzTarget {
                action,
                bucket: bucket.as_str(),
                object: object.as_str(),
                version_id: version_id.as_deref(),
                location_constraint: location_constraint.as_deref(),
            },
        );
        let mut conditions = authorization_conditions_with_internal(req, &principal, &facts, internal_object_authorization)?;

        let action_args = build_args(&principal, &facts, &conditions);
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

        let iam_allowed = {
            let mut final_args = build_args(&principal, &facts, &conditions);
            let allowed = iam_store
                .try_eval_prepared(&prepared, &final_args)
                .await
                .map_err(iam_authorization_error)?;
            if !allowed
                && matches!(
                    action,
                    Action::S3Action(
                        S3Action::DeleteObjectAction
                            | S3Action::DeleteObjectVersionAction
                            | S3Action::ForceDeleteBucketAction
                            | S3Action::ForceDeleteObjectAction
                            | S3Action::ListBucketVersionsAction
                            | S3Action::BypassGovernanceRetentionAction
                            | S3Action::ReplicateDeleteAction
                    )
                )
                && prepared.combined_policy_for_view().is_some()
            {
                // Bucket policy Allow may supplement an implicit IAM denial,
                // but must not override an explicit deletion-policy Deny.
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
    } else {
        let default_cred = rustfs_credentials::Credentials::default();
        let root_access_key = root_access_key();
        let principal = AuthzPrincipal::new(&default_cred, &root_access_key);
        let facts = request_authz_facts(
            req,
            AuthzTarget {
                action,
                bucket: bucket.as_str(),
                object: object.as_str(),
                version_id: version_id.as_deref(),
                location_constraint: location_constraint.as_deref(),
            },
        );
        let mut conditions = authorization_conditions_with_internal(req, &principal, &facts, internal_object_authorization)?;

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
                deny_anonymous_table_data_plane_if_needed(req, action, bucket.as_str(), object.as_str()).await?;
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

fn table_data_plane_publication_fence_required<R: AuthorizationRequest + ?Sized>(req: &R, action: Action) -> bool {
    !req.staged_multipart_part() && table_data_plane_content_mutation(action)
}

fn table_catalog_backend_for_data_plane<R: AuthorizationRequest + ?Sized>(
    req: &R,
) -> S3Result<crate::table_catalog::EcStoreTableCatalogObjectBackend<ECStore>> {
    let context = match req.server_context() {
        Some(server_ctx) => server_ctx.installed_app_context(),
        None => runtime_sources::current_app_context(),
    }
    .ok_or_else(object_store_not_initialized_error)?;
    Ok(crate::table_catalog::EcStoreTableCatalogObjectBackend::new(context.object_store()))
}

fn table_catalog_store_for_data_plane<R: AuthorizationRequest + ?Sized>(
    req: &R,
) -> S3Result<crate::table_catalog::EcStoreTableCatalogStore<ECStore>> {
    let backend = table_catalog_backend_for_data_plane(req)?;
    crate::table_catalog::ConfiguredTableCatalogStore::from_env(backend)
        .map_err(|err| s3_error!(InternalError, "failed to configure table catalog backing: {}", err))
}

fn table_publication_guard_error(err: crate::table_catalog::TableCatalogStoreError) -> S3Error {
    S3Error::from(ApiError::from(err))
}

async fn retain_table_data_plane_publication_guard<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    table_bucket: &str,
    lock_object: &str,
) -> S3Result<()> {
    let key = (table_bucket.to_string(), lock_object.to_string());
    let retained = req.publication_guards().cloned().unwrap_or_default();
    if retained.state.lock().keys.contains(&key) {
        return Ok(());
    }

    let backend = table_catalog_backend_for_data_plane(req)?;
    let guard = crate::table_catalog::TableCatalogObjectBackend::acquire_read_lock(&backend, table_bucket, lock_object)
        .await
        .map_err(table_publication_guard_error)?;
    let lock_lost_signal = guard.lock_lost_signal();
    let mut state = retained.state.lock();
    state.keys.insert(key);
    state.guards.push(Box::new(guard));
    if let Some(signal) = lock_lost_signal {
        state.lock_lost_signals.push(signal);
    }
    drop(state);
    req.install_publication_guards(retained);
    Ok(())
}

async fn retain_table_bucket_publication_guard<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    table_bucket: &str,
) -> S3Result<()> {
    retain_table_data_plane_publication_guard(
        req,
        table_bucket,
        &crate::table_catalog::default_table_bucket_publication_lock_path(),
    )
    .await
}

async fn retain_table_publication_guard<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
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
        .publication_guards()
        .cloned()
        .ok_or_else(|| s3_error!(InternalError, "table publication guard state is missing"))?;
    retained.state.lock().resources.insert(
        (resource.table_bucket.clone(), resource.warehouse_object_prefix.clone()),
        resource.clone(),
    );
    Ok(())
}

async fn table_bucket_enabled_for_data_plane<R: AuthorizationRequest + ?Sized>(req: &R, bucket: &str) -> S3Result<bool> {
    if bucket.is_empty() {
        return Ok(false);
    }

    match request_object_store(req)?.table_bucket_enabled(bucket).await {
        Ok(table_bucket_enabled) => Ok(table_bucket_enabled),
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

fn update_table_list_fingerprint_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn table_list_principal_fingerprint(cred: Option<&rustfs_credentials::Credentials>, is_owner: bool) -> String {
    let mut hasher = Sha256::new();
    update_table_list_fingerprint_field(
        &mut hasher,
        cred.map_or(b"anonymous".as_slice(), |credential| credential.access_key.as_bytes()),
    );
    hasher.update([u8::from(is_owner)]);
    hex_simd::encode_to_string(hasher.finalize(), hex_simd::AsciiCase::Lower)
}

fn table_list_snapshot_fingerprint(resources: &[TableDataPlaneListResource]) -> String {
    let mut hasher = Sha256::new();
    for entry in resources {
        for value in [
            entry.resource.table_bucket.as_bytes(),
            entry.resource.namespace.as_bytes(),
            entry.resource.table.as_bytes(),
            entry.resource.table_id.as_bytes(),
            entry.resource.warehouse_object_prefix.as_bytes(),
        ] {
            update_table_list_fingerprint_field(&mut hasher, value);
        }
        hasher.update([u8::from(entry.allowed)]);
    }
    hex_simd::encode_to_string(hasher.finalize(), hex_simd::AsciiCase::Lower)
}

fn table_list_cursor_key<T>(req: &S3Request<T>) -> S3Result<[u8; 32]> {
    let credentials = match req.extensions.get::<Arc<ServerContextSlot>>() {
        Some(server_ctx) => server_ctx
            .installed_app_context()
            .and_then(|context| context.action_credentials().get()),
        None => runtime_sources::current_action_credentials(),
    }
    .ok_or_else(|| table_data_plane_list_internal_error("action credentials are not initialized"))?;
    let mut hasher = Sha256::new();
    update_table_list_fingerprint_field(&mut hasher, TABLE_DATA_PLANE_LIST_CURSOR_KEY_CONTEXT);
    update_table_list_fingerprint_field(&mut hasher, credentials.access_key.as_bytes());
    update_table_list_fingerprint_field(&mut hasher, credentials.secret_key.as_bytes());
    Ok(hasher.finalize().into())
}

fn table_list_catalog_error(err: crate::table_catalog::TableCatalogStoreError) -> S3Error {
    if err.is_unavailable() {
        S3Error::from(ApiError::from(err))
    } else {
        S3Error::from(ApiError::access_denied())
    }
}

pub(crate) async fn authorize_bucket_listing<T>(req: &mut S3Request<T>, bucket: &str, action: S3Action) -> S3Result<()> {
    authorize_request(req, Action::S3Action(action)).await?;
    if action == S3Action::ListBucketAction
        && get_header(&req.headers, rustfs_utils::http::SUFFIX_INCLUDE_DELETED).as_deref() == Some("true")
    {
        authorize_request(req, Action::S3Action(S3Action::ListBucketVersionsAction)).await?;
    }
    prepare_table_data_plane_list_access(req, bucket, action).await
}

async fn prepare_table_data_plane_list_access<T>(req: &mut S3Request<T>, bucket: &str, action: S3Action) -> S3Result<()> {
    if !table_bucket_enabled_for_data_plane(req, bucket).await? {
        return Ok(());
    }
    retain_table_bucket_publication_guard(req, bucket).await?;
    if !table_bucket_enabled_for_data_plane(req, bucket).await? {
        return Err(S3Error::from(ApiError::access_denied()));
    }

    let (cred, is_owner) = {
        let req_info = req_info_ref(req)?;
        (req_info.cred.clone(), req_info.is_owner)
    };
    let default_cred = rustfs_credentials::Credentials::default();
    let condition_cred = cred.as_ref().unwrap_or(&default_cred);
    let root_access_key = root_access_key();
    let principal = AuthzPrincipal::new(condition_cred, &root_access_key);
    let facts = request_authz_facts(
        req,
        AuthzTarget {
            action: Action::S3Action(action),
            bucket,
            object: "",
            version_id: None,
            location_constraint: None,
        },
    );
    let conditions = authorization_conditions(req, &principal, &facts)?;
    let iam_store = cred.as_ref().map(|_| request_iam_store(req)).transpose()?;
    let store = table_catalog_store_for_data_plane(req)?;
    let tables = crate::table_catalog::TableCatalogStore::list_all_tables(&store, bucket)
        .await
        .map_err(table_list_catalog_error)?;
    let default_claims = HashMap::new();
    let claims = cred
        .as_ref()
        .and_then(|credential| credential.claims.as_ref())
        .unwrap_or(&default_claims);
    let mut resources = Vec::with_capacity(tables.len());
    for table in tables {
        let warehouse_object_prefix =
            crate::table_catalog::table_warehouse_object_prefix(&table).map_err(table_list_catalog_error)?;
        let resource = crate::table_catalog::table_data_plane_resource_from_entry(table.clone(), warehouse_object_prefix);
        let allowed = match (cred.as_ref(), iam_store.as_ref()) {
            (Some(credential), Some(iam_store)) => {
                let resource_object = resource.catalog_resource_object();
                iam_store
                    .is_allowed(&Args {
                        account: &credential.access_key,
                        groups: &credential.groups,
                        action: Action::AdminAction(AdminAction::GetTableMetadataAction),
                        bucket,
                        conditions: &conditions,
                        is_owner,
                        object: &resource_object,
                        claims,
                        deny_only: false,
                    })
                    .await
            }
            _ => false,
        };
        resources.push(TableDataPlaneListResource { resource, allowed });
    }
    resources.sort_by(|left, right| {
        left.resource
            .warehouse_object_prefix
            .cmp(&right.resource.warehouse_object_prefix)
    });
    if resources.windows(2).any(|window| {
        crate::table_catalog::warehouse_object_prefixes_overlap(
            &window[0].resource.warehouse_object_prefix,
            &window[1].resource.warehouse_object_prefix,
        )
    }) {
        return Err(S3Error::from(ApiError::access_denied()));
    }
    let access = TableDataPlaneListAccess {
        bucket: bucket.to_string(),
        principal_fingerprint: table_list_principal_fingerprint(cred.as_ref(), is_owner),
        snapshot_fingerprint: table_list_snapshot_fingerprint(&resources),
        cursor_key: table_list_cursor_key(req)?,
        resources,
    };
    req.extensions.insert(access);
    Ok(())
}

async fn table_data_plane_resource_for_request<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    bucket: &str,
    object: &str,
    table_bucket_enabled: bool,
) -> S3Result<Option<crate::table_catalog::TableDataPlaneResource>> {
    if !table_bucket_enabled || bucket.is_empty() || object.is_empty() {
        return Ok(None);
    }

    let catalog_metadata = crate::table_catalog::is_reserved_table_object_key(object)
        && crate::table_catalog::table_identity_from_metadata_object_key(object).is_some();
    let key = (bucket.to_string(), object.to_string());
    let retained = req.publication_guards().cloned().unwrap_or_default();
    let publication_held = {
        let state = retained.state.lock();
        if state.missing_resources.contains(&key) {
            return Ok(None);
        }
        if !catalog_metadata
            && let Some(resource) = state
                .resources
                .values()
                .find(|resource| resource.table_bucket == bucket && object.starts_with(&resource.warehouse_object_prefix))
                .cloned()
        {
            return Ok(Some(resource));
        }
        !state.keys.is_empty()
    };

    let store = table_catalog_store_for_data_plane(req)?;
    let resource = if catalog_metadata {
        crate::table_catalog::table_metadata_data_plane_resource_for_object(&store, bucket, object).await
    } else if publication_held {
        store
            .resolve_table_data_plane_resource_for_authorization(bucket, object)
            .await
    } else {
        crate::table_catalog::table_data_plane_resource_for_object(&store, bucket, object).await
    }
    .map_err(|err| {
        tracing::warn!(
            bucket = %bucket,
            object = %object,
            error = %err,
            "failed to resolve table data-plane resource"
        );
        table_list_catalog_error(err)
    })?;
    let resource = require_owned_reserved_table_object(object, resource)?;
    let bucket_fence_key = (bucket.to_string(), crate::table_catalog::default_table_bucket_publication_lock_path());
    let mut state = retained.state.lock();
    if resource.is_none() && state.keys.contains(&bucket_fence_key) {
        state.missing_resources.insert(key);
        drop(state);
        req.install_publication_guards(retained);
    }
    Ok(resource)
}

fn require_owned_reserved_table_object<T>(object: &str, resource: Option<T>) -> S3Result<Option<T>> {
    if crate::table_catalog::is_reserved_table_object_key(object) && resource.is_none() {
        return Err(S3Error::from(ApiError::access_denied()));
    }
    Ok(resource)
}

async fn table_data_plane_resource_for_authorization<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    bucket: &str,
    object: &str,
    table_bucket_enabled: bool,
) -> S3Result<Option<crate::table_catalog::TableDataPlaneResource>> {
    table_data_plane_resource_for_request(req, bucket, object, table_bucket_enabled).await
}

async fn authorize_table_data_plane_if_needed<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
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
    // A lockless cache bucket skips the publication fence that holds off a
    // concurrent table-bucket enablement; it still fences once the cached marker
    // shows a table bucket.
    let lockless_cache = is_lockless_cache_bucket(bucket);
    if table_data_plane_publication_fence_required(req, action) && !lockless_cache {
        retain_table_bucket_publication_guard(req, bucket).await?;
    }
    let mut table_bucket_enabled = table_bucket_enabled_for_data_plane(req, bucket).await?;
    if lockless_cache && table_bucket_enabled && table_data_plane_publication_fence_required(req, action) {
        retain_table_bucket_publication_guard(req, bucket).await?;
        table_bucket_enabled = table_bucket_enabled_for_data_plane(req, bucket).await?;
    }
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
    audit_request_denial(req, DenialAuditor::global(), "table_resource_policy_denied", action);
    Err(s3_error!(AccessDenied, "Access Denied"))
}

async fn deny_anonymous_table_data_plane_if_needed<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    action: Action,
    bucket: &str,
    object: &str,
) -> S3Result<()> {
    if table_data_plane_admin_action(action).is_none() {
        return Ok(());
    }
    // A lockless cache bucket skips the publication fence that holds off a
    // concurrent table-bucket enablement; it still fences once the cached marker
    // shows a table bucket.
    let lockless_cache = is_lockless_cache_bucket(bucket);
    if table_data_plane_publication_fence_required(req, action) && !lockless_cache {
        retain_table_bucket_publication_guard(req, bucket).await?;
    }
    let mut table_bucket_enabled = table_bucket_enabled_for_data_plane(req, bucket).await?;
    if lockless_cache && table_bucket_enabled && table_data_plane_publication_fence_required(req, action) {
        retain_table_bucket_publication_guard(req, bucket).await?;
        table_bucket_enabled = table_bucket_enabled_for_data_plane(req, bucket).await?;
    }
    if table_data_plane_resource_for_authorization(req, bucket, object, table_bucket_enabled)
        .await?
        .is_some()
    {
        audit_request_denial(req, DenialAuditor::global(), "anonymous_table_data_plane_access", action);
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
        // SigV4 only authorises the request properties covered by headers it
        // signed. Reject unsigned `x-amz-*` headers first, before the session
        // token lookup below or any handler reads a request header.
        reject_unsigned_amz_headers_on_sigv4_request(cx.headers(), cx.uri().query())?;

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
        let access_key = cx.credentials().map(|credentials| credentials.access_key.clone());
        let session_token = get_session_token(cx.uri(), cx.headers()).unwrap_or_default().to_owned();
        let request_context = cx.extensions_mut().get::<RequestContext>().cloned();
        let req_info = resolve_req_info(self.server_ctx(), access_key.as_deref(), &session_token, request_context).await?;

        let signature = VerifiedSignature::from_auth_type(get_request_auth_type_with_query(cx.headers(), cx.uri().query()));
        let markers = AccessMarkers::new(cx.s3_op().name(), signature);
        // Publish this server's context slot so downstream data-plane handlers
        // resolve the same store (backlog#1052 S6).
        let ext = cx.extensions_mut();
        ext.insert(self.server_ctx().clone());
        ext.insert(req_info);
        markers.install(ext);

        admit_operation(cx.headers(), cx.uri().query(), cx.s3_op().name(), signature)
    }

    /// Checks whether the CreateBucket request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn create_bucket(&self, req: &mut S3Request<CreateBucketInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        let location_constraint = requested_bucket_location_constraint(&req.input);
        authorize_request_with_location_constraint(req, Action::S3Action(S3Action::CreateBucketAction), location_constraint)
            .await?;

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
        let bucket_generation = load_bucket_generation(self.server_ctx(), req, &bucket).await;
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());

        authorize_request(req, Action::S3Action(S3Action::AbortMultipartUploadAction)).await?;
        req.install_bucket_generation(bucket_generation?);
        Ok(())
    }

    /// Checks whether the CompleteMultipartUpload request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn complete_multipart_upload(&self, req: &mut S3Request<CompleteMultipartUploadInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self.server_ctx(), req, &bucket).await;
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());

        authorize_request(req, complete_multipart_upload_authorize_action()).await?;
        let specific_etag = req.input.if_match.as_ref().is_some_and(|etag| !etag.is_any());
        authorize_write_etag_condition(req, specific_etag).await?;
        authorize_replication_only_put_headers(req).await?;
        req.install_bucket_generation(bucket_generation?);
        Ok(())
    }

    /// Checks whether the CopyObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn copy_object(&self, req: &mut S3Request<CopyObjectInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self.server_ctx(), req, &bucket).await;
        let Some((src_bucket, src_key, version_id)) = legacy_copy_source(&req.input.copy_source) else {
            return access_owned::object_commit::authorize_copy_source(req, self.server_ctx(), None).await;
        };
        let source = copy_source_object(&src_bucket, &src_key, version_id.as_deref());
        access_owned::object_commit::authorize_copy_source(req, self.server_ctx(), Some(source)).await?;

        let req_info = ext_req_info_mut(&mut req.extensions)?;

        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = super::options::destination_version_id(req.input.version_id.as_deref(), &req.headers);

        let requested_retain_until = req.input.object_lock_retain_until_date.clone();
        set_requested_object_lock_retain_until(req, requested_retain_until.as_ref());
        let replacement = crate::storage::s3_api::tagging::resolve_copy_object_tags(
            req.input.tagging.as_deref(),
            req.input.tagging_directive.as_ref(),
        )?;
        let store = request_object_store(req)?;
        let source_tags = if replacement.is_none() {
            use crate::storage::storage_api::contract::object::ObjectOperations as _;
            match store
                .get_object_tags(
                    &src_bucket,
                    &src_key,
                    &ObjectOptions {
                        // Keep the explicit version in the policy context, but
                        // address its storage representation just like COPY's reader.
                        version_id: version_id.as_deref().map(|version| {
                            let version = version.trim();
                            if version.eq_ignore_ascii_case("null") {
                                uuid::Uuid::nil().to_string()
                            } else {
                                version.to_owned()
                            }
                        }),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(tags) => tags,
                Err(error) if super::is_err_object_not_found(&error) || super::is_err_version_not_found(&error) => String::new(),
                Err(error) => return Err(ApiError::from(error).into()),
            }
        } else {
            String::new()
        };
        // Self-copy later holds the destination write lock. Capture its existing
        // tags now, including when only the final requested tags activate a policy.
        if src_bucket == req.input.bucket && src_key == req.input.key {
            let destination_version = req_info_ref(req)?.version_id.clone();
            get_or_fetch_object_tag_conditions(
                req,
                store.as_ref(),
                &src_bucket,
                &src_key,
                destination_version.as_deref(),
                Action::S3Action(S3Action::PutObjectAction),
            )
            .await?;
        }
        req.extensions.insert(RequestedObjectTags(Tagging::default()));
        authorize_copy_object_tags(req, &source_tags, replacement.as_deref().unwrap_or(&source_tags)).await?;

        authorize_replication_only_put_headers(req).await?;

        if legal_hold_write_requested(req.input.object_lock_legal_hold_status.as_ref()) {
            authorize_request(req, Action::S3Action(S3Action::PutObjectLegalHoldAction)).await?;
        }

        if retention_write_requested(req.input.object_lock_mode.as_ref(), req.input.object_lock_retain_until_date.as_ref()) {
            authorize_request(req, Action::S3Action(S3Action::PutObjectRetentionAction)).await?;
        }

        req.install_bucket_generation(bucket_generation?);
        Ok(())
    }

    /// Checks whether the CreateMultipartUpload request has accesses to the resources.
    async fn create_multipart_upload(&self, req: &mut S3Request<CreateMultipartUploadInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self.server_ctx(), req, &bucket).await;
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = super::options::destination_version_id(req.input.version_id.as_deref(), &req.headers);

        let requested_retain_until = req.input.object_lock_retain_until_date.clone();
        set_requested_object_lock_retain_until(req, requested_retain_until.as_ref());
        authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await?;

        authorize_replication_only_put_headers(req).await?;

        if legal_hold_write_requested(req.input.object_lock_legal_hold_status.as_ref()) {
            authorize_request(req, Action::S3Action(S3Action::PutObjectLegalHoldAction)).await?;
        }

        if retention_write_requested(req.input.object_lock_mode.as_ref(), req.input.object_lock_retain_until_date.as_ref()) {
            authorize_request(req, Action::S3Action(S3Action::PutObjectRetentionAction)).await?;
        }

        req.install_bucket_generation(bucket_generation?);
        Ok(())
    }

    /// Checks whether the DeleteBucket request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket(&self, req: &mut S3Request<DeleteBucketInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::DeleteBucketAction)).await?;

        // MinIO evaluates s3:ForceDeleteBucket whenever the header is present,
        // including the value `false`. Only a parsed `true` later skips the
        // emptiness check. `s3:*` does not grant this action.
        if force_delete_header(&req.headers)
            .map_err(|_| invalid_force_delete_header())?
            .is_some()
        {
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
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::DeleteBucketCorsAction))
            .await
    }

    /// Checks whether the DeleteBucketEncryption request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_encryption(&self, req: &mut S3Request<DeleteBucketEncryptionInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::PutBucketEncryptionAction))
            .await
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
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::PutBucketLifecycleAction))
            .await
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
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::DeleteBucketPolicyAction))
            .await
    }

    /// Checks whether the DeleteBucketReplication request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_replication(&self, req: &mut S3Request<DeleteBucketReplicationInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(
            self.server_ctx(),
            req,
            &bucket,
            Action::S3Action(S3Action::PutReplicationConfigurationAction),
        )
        .await
    }

    /// Checks whether the DeleteBucketTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_tagging(&self, req: &mut S3Request<DeleteBucketTaggingInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        rustfs_iam::sys::with_bucket_tag_deletion(authorize_bucket_config_mutation(
            self.server_ctx(),
            req,
            &bucket,
            Action::S3Action(S3Action::PutBucketTaggingAction),
        ))
        .await
    }

    /// Checks whether the DeleteBucketWebsite request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_website(&self, req: &mut S3Request<DeleteBucketWebsiteInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, bucket_website_config_authorize_action()).await
    }

    /// Checks whether the DeleteObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_object(&self, req: &mut S3Request<DeleteObjectInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self.server_ctx(), req, &bucket).await;
        #[cfg(test)]
        wait_for_object_generation_test_hook(&bucket).await;
        let key = req.input.key.clone();
        let version_id = req.input.version_id.clone();
        let guard = authorize_delete_object(req, &bucket, &key, version_id.as_deref(), bucket_generation).await?;
        req.install_pending_delete_bucket_generation(guard);
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

        authorize_request(
            req,
            versioned_tagging_action(S3Action::DeleteObjectTaggingAction, req.input.version_id.as_deref()),
        )
        .await
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
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(
            self.server_ctx(),
            req,
            &bucket,
            Action::S3Action(S3Action::DeleteBucketPublicAccessBlockAction),
        )
        .await
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
        let bucket = req.input.bucket.clone();
        let source_generation = capture_odm_read_generation(self.server_ctx(), req, &bucket).await;
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        // GHSA-3ppv: a versioned read (?versionId=...) must authorize against
        // s3:GetObjectVersion, not s3:GetObject.
        authorize_request(req, versioned_read_action(req.input.version_id.as_deref())).await?;
        req.install_odm_read_generation(source_generation);
        Ok(())
    }

    /// Checks whether the GetObjectAcl request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_acl(&self, req: &mut S3Request<GetObjectAclInput>) -> S3Result<()> {
        let (bucket, key, version_id) = (req.input.bucket.clone(), req.input.key.clone(), req.input.version_id.clone());
        authorize_object_request(req, &bucket, &key, version_id.as_deref(), Action::S3Action(S3Action::GetObjectAclAction)).await
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
        let (bucket, key, version_id) = (req.input.bucket.clone(), req.input.key.clone(), req.input.version_id.clone());
        authorize_object_request(
            req,
            &bucket,
            &key,
            version_id.as_deref(),
            Action::S3Action(S3Action::GetObjectLegalHoldAction),
        )
        .await
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
        let (bucket, key, version_id) = (req.input.bucket.clone(), req.input.key.clone(), req.input.version_id.clone());
        authorize_object_request(
            req,
            &bucket,
            &key,
            version_id.as_deref(),
            Action::S3Action(S3Action::GetObjectRetentionAction),
        )
        .await
    }

    /// Checks whether the GetObjectTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_tagging(&self, req: &mut S3Request<GetObjectTaggingInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(
            req,
            versioned_tagging_action(S3Action::GetObjectTaggingAction, req.input.version_id.as_deref()),
        )
        .await
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
        let bucket = req.input.bucket.clone();
        authorize_bucket_request(req, &bucket, Action::S3Action(S3Action::ListBucketAction)).await
    }

    /// Checks whether the HeadObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn head_object(&self, req: &mut S3Request<HeadObjectInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let source_generation = capture_odm_read_generation(self.server_ctx(), req, &bucket).await;
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, versioned_read_action(req.input.version_id.as_deref())).await?;
        // Replication permission additionally unlocks the SSE-C convergence
        // check; it does not replace permission to read this version's metadata.
        if get_header(&req.headers, SUFFIX_SOURCE_REPLICATION_CHECK).as_deref() == Some("true") {
            authorize_request(req, Action::S3Action(S3Action::ReplicateObjectAction)).await?;
            req_info_mut(req)?.replication_request_authorized = true;
        }

        req.install_odm_read_generation(source_generation);
        Ok(())
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
        let bucket_generation = load_bucket_generation(self.server_ctx(), req, &bucket).await;
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::ListBucketMultipartUploadsAction)).await?;
        prepare_table_data_plane_list_access(req, &bucket, S3Action::ListBucketMultipartUploadsAction).await?;
        req.install_bucket_generation(bucket_generation?);
        Ok(())
    }

    /// Checks whether the `ListObjectVersions` request is authorized for the requested bucket.
    ///
    /// Returns `Ok(())` if the request is allowed, or an error if access is denied or another
    /// authorization-related issue occurs.
    async fn list_object_versions(&self, req: &mut S3Request<ListObjectVersionsInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        authorize_bucket_listing(req, &bucket, S3Action::ListBucketVersionsAction).await
    }

    /// Checks whether the ListObjects request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_objects(&self, req: &mut S3Request<ListObjectsInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_listing(req, &bucket, S3Action::ListBucketAction).await
    }

    /// Checks whether the ListObjectsV2 request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_objects_v2(&self, req: &mut S3Request<ListObjectsV2Input>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let source_generation = capture_odm_read_generation(self.server_ctx(), req, &bucket).await;
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_bucket_listing(req, &bucket, S3Action::ListBucketAction).await?;
        req.install_odm_read_generation(source_generation);
        Ok(())
    }

    /// Checks whether the ListParts request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_parts(&self, req: &mut S3Request<ListPartsInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self.server_ctx(), req, &bucket).await;
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());

        authorize_request(req, list_parts_authorize_action()).await?;
        req.install_bucket_generation(bucket_generation?);
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
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::PutBucketPolicyAction)).await
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
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::PutBucketCorsAction)).await
    }

    /// Checks whether the PutBucketEncryption request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_encryption(&self, req: &mut S3Request<PutBucketEncryptionInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::PutBucketEncryptionAction))
            .await
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
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::PutBucketLifecycleAction))
            .await
    }

    async fn put_bucket_logging(&self, req: &mut S3Request<PutBucketLoggingInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::PutBucketLoggingAction))
            .await
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
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::PutBucketNotificationAction))
            .await
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
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, put_bucket_policy_authorize_action()).await
    }

    /// Checks whether the PutBucketReplication request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_replication(&self, req: &mut S3Request<PutBucketReplicationInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(
            self.server_ctx(),
            req,
            &bucket,
            Action::S3Action(S3Action::PutReplicationConfigurationAction),
        )
        .await
    }

    /// Checks whether the PutBucketRequestPayment request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_request_payment(&self, req: &mut S3Request<PutBucketRequestPaymentInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::PutBucketPolicyAction)).await
    }

    /// Checks whether the PutBucketTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_tagging(&self, req: &mut S3Request<PutBucketTaggingInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::PutBucketTaggingAction))
            .await
    }

    /// Checks whether the PutBucketVersioning request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_versioning(&self, req: &mut S3Request<PutBucketVersioningInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::PutBucketVersioningAction))
            .await
    }

    /// Checks whether the PutBucketWebsite request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_website(&self, req: &mut S3Request<PutBucketWebsiteInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, bucket_website_config_authorize_action()).await
    }

    /// Checks whether the PutObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object(&self, req: &mut S3Request<PutObjectInput>) -> S3Result<()> {
        crate::hp_guard!("S3Access::put_object");
        async {
            let req_info = ext_req_info_mut(&mut req.extensions)?;
            req_info.bucket = Some(req.input.bucket.clone());
            req_info.object = Some(req.input.key.clone());
            req_info.version_id = super::options::destination_version_id(req.input.version_id.as_deref(), &req.headers);

            if has_write_offset_bytes_header(&req.headers) {
                return Err(S3Error::with_message(
                    S3ErrorCode::NotImplemented,
                    ApiError::error_code_to_message(&S3ErrorCode::NotImplemented),
                ));
            }

            let bucket = req.input.bucket.clone();
            // Snapshot before authorization, but preserve AccessDenied precedence
            // by exposing any bucket-state error only after authorization succeeds.
            let bucket_generation = load_bucket_generation(self.server_ctx(), req, &bucket).await;
            #[cfg(test)]
            let bucket_generation = if PUT_GENERATION_READ_QUORUM_FAILURE.try_with(|_| ()).is_ok() {
                Err(ApiError::from(StorageError::ErasureReadQuorum).into())
            } else {
                bucket_generation
            };
            let requested_retain_until = req.input.object_lock_retain_until_date.clone();
            set_requested_object_lock_retain_until(req, requested_retain_until.as_ref());
            set_post_object_policy_headers(req)?;
            authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await?;
            let specific_etag = req.input.if_match.as_ref().is_some_and(|etag| !etag.is_any());
            authorize_write_etag_condition(req, specific_etag).await?;
            req.install_bucket_generation(bucket_generation?);

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
        .await
    }

    /// Checks whether the PutObjectAcl request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_acl(&self, req: &mut S3Request<PutObjectAclInput>) -> S3Result<()> {
        let (bucket, key, version_id) = (req.input.bucket.clone(), req.input.key.clone(), req.input.version_id.clone());
        authorize_object_request(req, &bucket, &key, version_id.as_deref(), Action::S3Action(S3Action::PutObjectAclAction)).await
    }

    /// Checks whether the PutObjectLegalHold request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_legal_hold(&self, req: &mut S3Request<PutObjectLegalHoldInput>) -> S3Result<()> {
        req.extensions
            .insert(RequestedObjectLegalHold(req.input.legal_hold.clone().unwrap_or_default()));
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self.server_ctx(), req, &bucket).await;
        authorize_request(req, Action::S3Action(S3Action::PutObjectLegalHoldAction)).await?;
        req.install_bucket_generation(bucket_generation?);
        Ok(())
    }

    /// Checks whether the PutObjectLockConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_lock_configuration(&self, req: &mut S3Request<PutObjectLockConfigurationInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(
            self.server_ctx(),
            req,
            &bucket,
            Action::S3Action(S3Action::PutBucketObjectLockConfigurationAction),
        )
        .await
    }

    /// Checks whether the PutObjectRetention request has accesses to the resources.
    async fn put_object_retention(&self, req: &mut S3Request<PutObjectRetentionInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self.server_ctx(), req, &bucket).await;
        set_requested_object_lock_retention(req);
        authorize_request(req, Action::S3Action(S3Action::PutObjectRetentionAction)).await?;

        // S3 Standard: When bypass_governance header is set, must have s3:BypassGovernanceRetention permission
        if has_bypass_governance_header(&req.headers) {
            authorize_request(req, Action::S3Action(S3Action::BypassGovernanceRetentionAction)).await?;
        }

        req.install_bucket_generation(bucket_generation?);
        Ok(())
    }

    /// Checks whether the PutObjectTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_tagging(&self, req: &mut S3Request<PutObjectTaggingInput>) -> S3Result<()> {
        req.extensions.insert(RequestedObjectTags(req.input.tagging.clone()));
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(
            req,
            versioned_tagging_action(S3Action::PutObjectTaggingAction, req.input.version_id.as_deref()),
        )
        .await
    }

    /// Checks whether the PutPublicAccessBlock request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_public_access_block(&self, req: &mut S3Request<PutPublicAccessBlockInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(
            self.server_ctx(),
            req,
            &bucket,
            Action::S3Action(S3Action::PutBucketPublicAccessBlockAction),
        )
        .await
    }

    /// Checks whether the RestoreObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn restore_object(&self, req: &mut S3Request<RestoreObjectInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self.server_ctx(), req, &bucket).await;
        #[cfg(test)]
        wait_for_object_generation_test_hook(&bucket).await;
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::RestoreObjectAction)).await?;
        req.install_bucket_generation(bucket_generation?);
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
        let bucket_generation = load_bucket_generation(self.server_ctx(), req, &bucket).await;
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());

        authorize_staged_multipart_part(req).await?;
        req.install_bucket_generation(bucket_generation?);
        Ok(())
    }

    /// Checks whether the UploadPartCopy request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn upload_part_copy(&self, req: &mut S3Request<UploadPartCopyInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let bucket_generation = load_bucket_generation(self.server_ctx(), req, &bucket).await;
        let source = legacy_copy_source(&req.input.copy_source);
        let source = source
            .as_ref()
            .map(|(bucket, key, version_id)| copy_source_object(bucket, key, version_id.as_deref()));
        access_owned::object_commit::authorize_copy_source(req, self.server_ctx(), source).await?;

        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = None;

        authorize_staged_multipart_part(req).await?;
        req.install_bucket_generation(bucket_generation?);
        Ok(())
    }

    /// Checks whether the WriteGetObjectResponse request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn write_get_object_response(&self, _req: &mut S3Request<WriteGetObjectResponseInput>) -> S3Result<()> {
        Ok(())
    }
}

/// Entry points into private authorization steps for the app-layer owned/legacy
/// parity tests, which live next to the envelope's facts impl.
#[cfg(test)]
pub(crate) mod parity_test_support {
    use super::{Action, Arc, CreateBucketInput, S3Request, S3Result};
    use super::{AuthorizationRequest, AuthzFacts, AuthzPrincipal, AuthzTarget, ECStore, HashMap};

    pub(crate) fn request_authz_facts<R: AuthorizationRequest + ?Sized>(req: &R, target: AuthzTarget<'_>) -> AuthzFacts {
        super::request_authz_facts(req, target)
    }

    pub(crate) fn authorization_conditions<R: AuthorizationRequest + ?Sized>(
        req: &R,
        principal: &AuthzPrincipal<'_>,
        facts: &AuthzFacts,
    ) -> S3Result<HashMap<String, Vec<String>>> {
        super::authorization_conditions(req, principal, facts)
    }

    pub(crate) fn requested_bucket_location_constraint(input: &CreateBucketInput) -> Option<String> {
        super::requested_bucket_location_constraint(input)
    }

    pub(crate) fn request_object_store<R: AuthorizationRequest + ?Sized>(req: &R) -> S3Result<Arc<ECStore>> {
        super::request_object_store(req)
    }

    /// The cached tag evidence as `(bucket, object, version_id, values)`.
    pub(crate) fn tag_conditions_parts(
        conditions: &super::ObjectTagConditions,
    ) -> (&str, &str, Option<&str>, &HashMap<String, Vec<String>>) {
        (
            conditions.bucket.as_str(),
            conditions.object.as_str(),
            conditions.version_id.as_deref(),
            &conditions.values,
        )
    }

    /// Whether `guards` retains `key`, and how many lock guards it holds.
    pub(crate) fn publication_guard_holds(
        guards: &super::TableDataPlanePublicationGuards,
        key: &(String, String),
    ) -> (bool, usize) {
        let state = guards.state.lock();
        (state.keys.contains(key), state.guards.len())
    }

    pub(crate) async fn authorize_request_with_location_constraint<T>(
        req: &mut S3Request<T>,
        action: Action,
        location_constraint: Option<String>,
    ) -> S3Result<()> {
        super::authorize_request_with_location_constraint(req, action, location_constraint).await
    }
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::{
        AMZ_WRITE_OFFSET_BYTES_HEADER, BucketGenerationGuard, BucketPolicyArgs, BucketPolicyExistingObjectTagHint,
        BucketPolicyRawLoadErrorKind, DenialContext, FS, InternalObjectAuthorization, ObjectTagConditions,
        PostObjectRequestMarker, ReqInfo, S3Access, StorageError, TABLE_DATA_PLANE_LIST_CURSOR_MAX_ENCODED_LEN,
        TableDataPlaneListAccess, TableDataPlaneListCursorPosition, TableDataPlaneListResource, TableDataPlanePublicationGuards,
        apply_bucket_generation_guard, apply_copy_source_bucket_generation_guard, authorization_conditions,
        bucket_policy_needs_existing_object_tag_from_hint, bucket_website_config_authorize_action,
        classify_bucket_policy_raw_load_error, complete_multipart_upload_authorize_action, delete_object_authorize_action,
        get_bucket_policy_authorize_action, has_write_offset_bytes_header, install_object_generation_test_hook,
        legal_hold_write_requested, list_parts_authorize_action, load_bucket_policy_existing_object_tag_hint,
        maybe_merge_object_tag_conditions, merge_request_object_tag_conditions, owner_can_bypass_policy_deny,
        post_object_authorize_action, put_bucket_policy_authorize_action, request_authz_facts, request_context_from_req,
        request_object_store, require_owned_reserved_table_object, retention_write_requested, table_data_plane_admin_action,
        table_data_plane_content_mutation, table_data_plane_resource_for_request, table_publication_guard_error,
        validate_post_object_success_controls, versioned_read_action, versioned_tagging_action,
    };
    use super::{BulkDenialAudit, audit_request_denial};
    use super::{
        PostObjectPolicyHeaders, RequestedCopySource, RequestedObjectLegalHold, RequestedObjectTags, remaining_retention_days,
        requested_bucket_location_constraint, set_post_object_policy_headers, set_requested_object_lock_retain_until,
        set_requested_object_lock_retention,
    };
    use crate::auth::OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION;
    use crate::auth::facts::{AuthzPrincipal, AuthzTarget, list_query_pairs, merge_list_query_conditions};
    use crate::error::ApiError;
    use crate::storage::denial_audit::{DenialAuditor, DenialRateLimiter};
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
    use std::time::Instant;
    use std::{collections::HashMap, sync::Arc};
    use time::OffsetDateTime;

    #[test]
    fn replication_head_checks_read_permissions_before_replication_permissions() {
        crate::app::gating_test_env::run_large_stack_test("replication-head-permissions", || async {
            let store = crate::app::gating_test_env::shared_gating_ecstore().await;
            let bucket = format!("replica-head-{}", uuid::Uuid::new_v4());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create bucket");
            let slot = ServerContextSlot::new();
            assert!(slot.install(Arc::new(AppContext::new(store.clone(), Arc::new(UnreadyIam), Arc::new(TestKms)))));
            let fs = FS::with_server_ctx(slot.clone());
            for (grant_read, deny_read) in [(false, false), (true, true), (true, false)] {
                let mut statements = vec![
                    serde_json::json!({"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:ReplicateObject","Resource":format!("arn:aws:s3:::{bucket}/*")}),
                ];
                for (enabled, effect) in [(grant_read, "Allow"), (deny_read, "Deny")] {
                    if enabled {
                        statements.push(serde_json::json!({"Effect":effect,"Principal":{"AWS":"*"},"Action":["s3:GetObject","s3:GetObjectVersion"],"Resource":format!("arn:aws:s3:::{bucket}/*")}));
                    }
                }
                store
                    .update_bucket_metadata_config(
                        &bucket,
                        crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                        serde_json::to_vec(&serde_json::json!({"Version":"2012-10-17","Statement":statements})).unwrap(),
                    )
                    .await
                    .expect("publish policy");
                for version in [None, Some("null"), Some("12345678-1234-1234-1234-123456789abc")] {
                    for header in ["x-rustfs-source-replication-check", "x-minio-source-replication-check"] {
                        let mut req = build_request(
                            HeadObjectInput {
                                bucket: bucket.clone(),
                                key: "object".to_owned(),
                                version_id: version.map(str::to_owned),
                                ..Default::default()
                            },
                            Method::HEAD,
                        );
                        ensure_req_info(&mut req);
                        req.extensions.insert(slot.clone());
                        req.headers.insert(header, HeaderValue::from_static("true"));
                        let result = fs.head_object(&mut req).await;
                        if grant_read && !deny_read {
                            result.expect("read and replication permissions allow metadata check");
                            assert!(super::replication_request_authorized(&req));
                        } else {
                            assert_eq!(
                                result.expect_err("replication cannot replace read permission").code(),
                                &S3ErrorCode::AccessDenied
                            );
                            assert!(!super::replication_request_authorized(&req));
                        }
                    }
                }
            }
        });
    }

    #[test]
    fn replication_write_hooks_authorize_the_destination_version_and_tags() {
        crate::app::gating_test_env::run_large_stack_test("replication-version-permissions", || async {
            let store = crate::app::gating_test_env::shared_gating_ecstore().await;
            let bucket = format!("replica-write-{}", uuid::Uuid::new_v4());
            store
                .make_bucket(
                    &bucket,
                    &MakeBucketOptions {
                        versioning_enabled: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("create versioned bucket");
            let slot = ServerContextSlot::new();
            assert!(slot.install(Arc::new(AppContext::new(store.clone(), Arc::new(UnreadyIam), Arc::new(TestKms)))));
            let fs = FS::with_server_ctx(slot.clone());
            let mut versions = Vec::new();
            for (body, tags) in [
                (b"secret".to_vec(), "classification=secret"),
                (b"public".to_vec(), "classification=public"),
            ] {
                let object = store
                    .put_object(
                        &bucket,
                        "target",
                        &mut crate::storage::PutObjReader::from_vec(body),
                        &crate::storage::ObjectOptions {
                            versioned: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("seed version");
                let version = object.version_id.expect("UUID version").to_string();
                store
                    .put_object_tags(
                        &bucket,
                        "target",
                        tags,
                        &crate::storage::ObjectOptions {
                            version_id: Some(version.clone()),
                            versioned: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("tag version");
                versions.push(version);
            }
            for condition in [
                serde_json::json!({"s3:versionid":versions[0]}),
                serde_json::json!({"s3:ExistingObjectTag/classification":"secret"}),
            ] {
                let policy = serde_json::json!({"Version":"2012-10-17","Statement":[
                    {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:*","Resource":format!("arn:aws:s3:::{bucket}/*")},
                    {"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:PutObject","Resource":format!("arn:aws:s3:::{bucket}/target"),"Condition":{"StringEquals":condition}}
                ]});
                store
                    .update_bucket_metadata_config(
                        &bucket,
                        crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                        serde_json::to_vec(&policy).unwrap(),
                    )
                    .await
                    .expect("publish version policy");
                for (index, version) in versions.iter().enumerate() {
                    for header in [None, Some("x-rustfs-source-version-id"), Some("x-minio-source-version-id")] {
                        for operation in ["put", "copy", "multipart"] {
                            let query_version = header.is_none().then(|| version.clone());
                            let mut base = build_request((), Method::PUT);
                            ensure_req_info(&mut base);
                            base.extensions.insert(slot.clone());
                            // A query version alone also requires replication authorization.
                            base.headers
                                .insert("x-rustfs-source-replication-request", HeaderValue::from_static("true"));
                            if let Some(header) = header {
                                base.headers.insert(header, HeaderValue::from_str(version).unwrap());
                            }
                            let (result, info) = match operation {
                                "put" => {
                                    let mut req = base.map_input(|_| PutObjectInput {
                                        bucket: bucket.clone(),
                                        key: "target".to_owned(),
                                        version_id: query_version,
                                        ..Default::default()
                                    });
                                    let result = fs.put_object(&mut req).await;
                                    (result, req.extensions.get::<ReqInfo>().unwrap().clone())
                                }
                                "copy" => {
                                    let mut req = base.map_input(|_| {
                                        CopyObjectInput::builder()
                                            .bucket(bucket.clone())
                                            .key("target".to_owned())
                                            .version_id(query_version)
                                            .copy_source(CopySource::parse(&format!("/{bucket}/source")).expect("copy source"))
                                            .build()
                                            .expect("copy input")
                                    });
                                    let result = fs.copy_object(&mut req).await;
                                    (result, req.extensions.get::<ReqInfo>().unwrap().clone())
                                }
                                _ => {
                                    let mut req = base.map_input(|_| CreateMultipartUploadInput {
                                        bucket: bucket.clone(),
                                        key: "target".to_owned(),
                                        version_id: query_version,
                                        ..Default::default()
                                    });
                                    let result = fs.create_multipart_upload(&mut req).await;
                                    (result, req.extensions.get::<ReqInfo>().unwrap().clone())
                                }
                            };
                            assert_eq!(info.version_id.as_deref(), Some(version.as_str()), "{operation}");
                            if index == 0 {
                                assert_eq!(
                                    result.expect_err("protected destination version must be denied").code(),
                                    &S3ErrorCode::AccessDenied,
                                    "{operation}"
                                );
                            } else {
                                result.expect("unprotected destination version remains authorized");
                            }
                        }
                    }
                }
            }
        });
    }

    #[test]
    fn replication_delete_hook_authorizes_the_header_selected_version() {
        crate::app::gating_test_env::run_large_stack_test("replication-delete-version-permissions", || async {
            let store = crate::app::gating_test_env::shared_gating_ecstore().await;
            let bucket = format!("replica-delete-{}", uuid::Uuid::new_v4());
            store
                .make_bucket(
                    &bucket,
                    &MakeBucketOptions {
                        versioning_enabled: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("create versioned bucket");
            let slot = ServerContextSlot::new();
            assert!(slot.install(Arc::new(AppContext::new(store.clone(), Arc::new(UnreadyIam), Arc::new(TestKms)))));
            let fs = FS::with_server_ctx(slot.clone());
            let mut versions = Vec::new();
            for body in [b"protected".to_vec(), b"disposable".to_vec()] {
                let object = store
                    .put_object(
                        &bucket,
                        "target",
                        &mut crate::storage::PutObjReader::from_vec(body),
                        &crate::storage::ObjectOptions {
                            versioned: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("seed version");
                versions.push(object.version_id.expect("UUID version").to_string());
            }
            let policy = serde_json::json!({"Version":"2012-10-17","Statement":[
                {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:*","Resource":format!("arn:aws:s3:::{bucket}/*")},
                {"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:DeleteObjectVersion","Resource":format!("arn:aws:s3:::{bucket}/target"),"Condition":{"StringEquals":{"s3:versionid":versions[0]}}}
            ]});
            store
                .update_bucket_metadata_config(
                    &bucket,
                    crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                    serde_json::to_vec(&policy).unwrap(),
                )
                .await
                .expect("publish version policy");
            // The version named only by the replication header must be the one
            // the policy sees: a plain `s3:DeleteObject` evaluation would let a
            // replica delete remove the protected version. A request without
            // the REPLICA status is authorized for the query version alone,
            // because storage ignores the header for it (a stray header must
            // not switch the evaluated action to `s3:DeleteObjectVersion`).
            for (header, version, replica, expect_denied) in [
                ("x-rustfs-source-version-id", &versions[0], true, true),
                ("x-minio-source-version-id", &versions[0], true, true),
                ("x-rustfs-source-version-id", &versions[0], false, false),
                ("x-rustfs-source-version-id", &versions[1], true, false),
            ] {
                let mut req = build_request(
                    DeleteObjectInput {
                        bucket: bucket.clone(),
                        key: "target".to_owned(),
                        ..Default::default()
                    },
                    Method::DELETE,
                );
                ensure_req_info(&mut req);
                req.extensions.insert(slot.clone());
                if replica {
                    req.headers
                        .insert("x-rustfs-source-replication-request", HeaderValue::from_static("true"));
                    req.headers.insert(
                        rustfs_utils::http::headers::AMZ_BUCKET_REPLICATION_STATUS,
                        HeaderValue::from_static("REPLICA"),
                    );
                }
                req.headers.insert(header, HeaderValue::from_str(version).unwrap());
                let result = fs.delete_object(&mut req).await;
                let info = req.extensions.get::<ReqInfo>().unwrap();
                let expected_version = replica.then_some(version.as_str());
                assert_eq!(info.version_id.as_deref(), expected_version, "{header} replica={replica}");
                if expect_denied {
                    assert_eq!(
                        result
                            .expect_err("protected version named by the header must be denied")
                            .code(),
                        &S3ErrorCode::AccessDenied,
                        "{header}"
                    );
                } else {
                    result.expect("a plain delete, or a replica delete of the unprotected version, succeeds");
                }
            }
            store
                .get_object_info(
                    &bucket,
                    "target",
                    &crate::storage::ObjectOptions {
                        version_id: Some(versions[0].clone()),
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("the protected version survives the denied replica deletes");
        });
    }

    #[test]
    fn terminal_iam_denial_is_access_denied_and_backend_failure_stays_internal() {
        let denied = s3s::S3Error::from(super::iam_authorization_error(rustfs_iam::error::Error::IAMActionNotAllowed));
        assert_eq!(denied.code(), &S3ErrorCode::AccessDenied);
        let unavailable = s3s::S3Error::from(super::iam_authorization_error(rustfs_iam::error::Error::IamSysNotInitialized));
        assert_eq!(unavailable.code(), &S3ErrorCode::InternalError);
    }

    pub(super) struct UnreadyIam;

    impl IamInterface for UnreadyIam {
        fn handle(&self) -> Arc<IamSys<ObjectStore>> {
            panic!("bucket generation guard tests do not resolve IAM")
        }

        fn is_ready(&self) -> bool {
            false
        }
    }

    pub(super) struct TestKms;

    impl KmsInterface for TestKms {
        fn handle(&self) -> Arc<KmsServiceManager> {
            Arc::new(KmsServiceManager::new())
        }
    }

    pub(super) fn build_request<T>(input: T, method: Method) -> S3Request<T> {
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

    pub(super) fn ensure_req_info<T>(req: &mut S3Request<T>) {
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

    fn table_list_access(allowed: bool) -> TableDataPlaneListAccess {
        TableDataPlaneListAccess {
            bucket: "analytics".to_string(),
            principal_fingerprint: "principal".to_string(),
            snapshot_fingerprint: "snapshot".to_string(),
            cursor_key: [0x42; 32],
            resources: vec![TableDataPlaneListResource {
                resource: crate::table_catalog::TableDataPlaneResource {
                    table_bucket: "analytics".to_string(),
                    namespace: "sales".to_string(),
                    table: "orders".to_string(),
                    table_id: "table-id".to_string(),
                    warehouse_object_prefix: "tables/table-id/".to_string(),
                },
                allowed,
            }],
        }
    }

    #[test]
    fn table_list_access_hides_unauthorized_table_objects_and_reserved_metadata() {
        let denied = table_list_access(false);
        assert!(!denied.allows_object("tables/table-id/data/file.parquet"));
        assert!(
            !denied.allows_object(".rustfs-table/warehouses/default/namespaces/sales/tables/orders/metadata/00001.metadata.json")
        );
        assert!(!denied.allows_object(".rustfs-table/private/catalog.json"));
        assert!(denied.allows_object("ordinary/file.txt"));

        let allowed = table_list_access(true);
        assert!(allowed.allows_object("tables/table-id/data/file.parquet"));
        assert!(
            !allowed
                .allows_object(".rustfs-table/warehouses/default/namespaces/sales/tables/orders/metadata/00001.metadata.json")
        );
        assert!(!allowed.allows_object(".rustfs-table/private/catalog.json"));
    }

    #[test]
    fn table_list_cursor_is_confidential_and_bound_to_the_request_snapshot() {
        let access = table_list_access(true);
        let marker = "tables/hidden/data/file.parquet";
        let token = access
            .encode_cursor(
                "ListObjectsV2",
                "tables/",
                Some("/"),
                1000,
                TableDataPlaneListCursorPosition {
                    marker: Some(marker.to_string()),
                    version_marker: None,
                    common_prefix: None,
                },
            )
            .expect("cursor should encode");
        assert!(!token.contains(marker));
        let decoded = access
            .decode_cursor("ListObjectsV2", "tables/", Some("/"), 1000, &token)
            .expect("cursor should decode");
        assert_eq!(decoded.marker(), Some(marker));

        let mut changed = access.clone();
        changed.snapshot_fingerprint = "changed".to_string();
        assert!(
            changed
                .decode_cursor("ListObjectsV2", "tables/", Some("/"), 1000, &token)
                .is_err()
        );
        assert!(
            access
                .decode_cursor("ListObjectsV2", "other/", Some("/"), 1000, &token)
                .is_err()
        );
        let mut rotated_key = access.clone();
        rotated_key.cursor_key[0] ^= 1;
        assert!(
            rotated_key
                .decode_cursor("ListObjectsV2", "tables/", Some("/"), 1000, &token)
                .is_err()
        );
        assert!(
            access
                .decode_cursor(
                    "ListObjectsV2",
                    "tables/",
                    Some("/"),
                    1000,
                    &"x".repeat(TABLE_DATA_PLANE_LIST_CURSOR_MAX_ENCODED_LEN + 1),
                )
                .is_err()
        );
    }

    #[test]
    fn reserved_table_metadata_requires_an_active_table_owner() {
        let error = require_owned_reserved_table_object::<()>(
            ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json",
            None,
        )
        .expect_err("unowned reserved metadata must fail closed");
        assert_eq!(error.code(), &S3ErrorCode::AccessDenied);

        assert!(
            require_owned_reserved_table_object("objects/data.parquet", None::<()>)
                .unwrap()
                .is_none()
        );
        assert!(
            require_owned_reserved_table_object(".rustfs-table/metadata.json", Some(()))
                .unwrap()
                .is_some()
        );
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
    fn table_publication_guard_unavailable_errors_are_retryable() {
        use crate::table_catalog::TableCatalogStoreError;

        for message in [
            "failed to acquire catalog migration lock: Quorum not reached: required 2, achieved 1",
            "failed to acquire catalog migration lock: lock acquisition timed out after 5s",
            "peer unavailable",
            "",
        ] {
            let error = TableCatalogStoreError::Unavailable(message.to_string());
            let expected_message = ApiError::error_code_to_message(&S3ErrorCode::ServiceUnavailable);
            let err = table_publication_guard_error(error);
            assert_eq!(err.code(), &S3ErrorCode::ServiceUnavailable, "{message}");
            assert_eq!(
                err.status_code().or_else(|| err.code().status_code()),
                Some(http::StatusCode::SERVICE_UNAVAILABLE)
            );
            assert_eq!(err.message(), Some(expected_message.as_str()));
            let response = err.to_http_response().expect("serialize catalog failure");
            assert!(!response.headers().contains_key(http::header::RETRY_AFTER));
        }
    }

    #[test]
    fn table_publication_guard_non_retryable_errors_stay_internal() {
        use crate::table_catalog::TableCatalogStoreError;

        let message = "temporarily unavailable: timeout: quorum not reached".to_string();
        for error in [
            TableCatalogStoreError::Internal(message.clone()),
            TableCatalogStoreError::Invalid(message.clone()),
            TableCatalogStoreError::Unsupported(message.clone()),
            TableCatalogStoreError::NotFound(message.clone()),
            TableCatalogStoreError::NamespaceNotFound(message.clone()),
            TableCatalogStoreError::TableNotFound(message.clone()),
            TableCatalogStoreError::AlreadyExists(message.clone()),
            TableCatalogStoreError::Conflict(message),
        ] {
            let expected_message = ApiError::error_code_to_message(&S3ErrorCode::InternalError);
            let err = table_publication_guard_error(error);
            assert_eq!(err.code(), &S3ErrorCode::InternalError);
            assert_eq!(
                err.status_code().or_else(|| err.code().status_code()),
                Some(http::StatusCode::INTERNAL_SERVER_ERROR)
            );
            assert_eq!(err.message(), Some(expected_message.as_str()));
            let response = err.to_http_response().expect("serialize catalog failure");
            assert!(!response.headers().contains_key(http::header::RETRY_AFTER));
        }
    }

    #[tokio::test]
    #[serial]
    async fn table_data_plane_cold_lookup_does_not_wait_for_backup_while_holding_publication() {
        use crate::table_catalog::{TableCatalogObjectBackend as _, TableCatalogStore as _};
        let (_temp_dir, _disk_paths, object_store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
        let bucket = "cold-authorization";
        object_store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create table bucket");
        let context = Arc::new(AppContext::new(object_store.clone(), Arc::new(UnreadyIam), Arc::new(TestKms)));
        let server_ctx = ServerContextSlot::new();
        assert!(server_ctx.install(context));
        let mut request = build_request((), Method::PUT);
        request.extensions.insert(server_ctx.clone());
        let backend = super::table_catalog_backend_for_data_plane(&request).expect("request backend");
        let catalog = crate::table_catalog::ConfiguredTableCatalogStore::new_for_test(
            backend.clone(),
            crate::table_catalog::TableCatalogBackingMode::ObjectBacked,
        );
        catalog
            .put_table_bucket(crate::table_catalog::TableBucketEntry {
                version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: bucket.to_string(),
                catalog_type: crate::table_catalog::TABLE_BUCKET_CATALOG_TYPE.to_string(),
                warehouse_root: format!("s3://{bucket}/"),
                state: crate::table_catalog::TableCatalogEntryState::Active,
                properties: Default::default(),
                active_rename_id: None,
                created_at: None,
                updated_at: None,
            })
            .await
            .expect("newly enabled catalog entry");
        super::retain_table_bucket_publication_guard(&mut request, bucket)
            .await
            .expect("S3 owns publication first");
        let backup_guard = backend
            .acquire_write_lock(
                crate::admin::storage_api::RUSTFS_META_BUCKET,
                &crate::table_catalog::TableCatalogObjectPaths::default().catalog_backup_fence_lock_path(bucket),
            )
            .await
            .expect("backup owns its catalog fence");
        let resource = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            table_data_plane_resource_for_request(&mut request, bucket, "ordinary-object", true),
        )
        .await
        .expect("cold S3 lookup must not wait for the backup catalog fence")
        .expect("resolve newly enabled table bucket");
        assert!(resource.is_none());
        assert!(
            request
                .extensions
                .get::<TableDataPlanePublicationGuards>()
                .expect("retained publication")
                .state
                .lock()
                .missing_resources
                .contains(&(bucket.to_string(), "ordinary-object".to_string()))
        );
        drop(backup_guard);
        drop(request);
        let mut read_request = build_request((), Method::GET);
        read_request.extensions.insert(server_ctx);
        table_data_plane_resource_for_request(&mut read_request, bucket, "ordinary-object", true)
            .await
            .expect("unfenced reads retain normal index initialization");
        assert!(
            backend
                .read_object(
                    crate::admin::storage_api::RUSTFS_META_BUCKET,
                    &crate::table_catalog::TableCatalogObjectPaths::default().warehouse_index_state_path(bucket),
                )
                .await
                .expect("read initialized index marker")
                .is_some()
        );
    }

    #[test]
    fn table_data_plane_mutations_fence_before_table_bucket_marker_lookup() {
        let source = include_str!("access.rs");
        for (function, end_marker) in [
            (
                "async fn authorize_table_data_plane_if_needed",
                "async fn deny_anonymous_table_data_plane_if_needed",
            ),
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
    async fn table_data_plane_request_reuses_reserved_warehouse_resource() {
        let resource = crate::table_catalog::TableDataPlaneResource {
            table_bucket: "warehouse".to_string(),
            namespace: "analytics".to_string(),
            table: "events".to_string(),
            table_id: "table-id".to_string(),
            warehouse_object_prefix: ".rustfs-table/custom-warehouse/events/".to_string(),
        };
        let retained = TableDataPlanePublicationGuards::default();
        retained.state.lock().resources.insert(
            (resource.table_bucket.clone(), resource.warehouse_object_prefix.clone()),
            resource.clone(),
        );
        let mut req = build_request((), Method::GET);
        req.extensions.insert(retained);

        let resolved = table_data_plane_resource_for_request(
            &mut req,
            "warehouse",
            ".rustfs-table/custom-warehouse/events/data/part-00001.parquet",
            true,
        )
        .await
        .expect("a reserved-prefix warehouse object should use ordinary table resolution");

        assert_eq!(resolved, Some(resource));
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
        merge_list_query_conditions(
            Action::S3Action(S3Action::ListBucketAction),
            &list_query_pairs(Some("prefix=photos%2F2024%2F&delimiter=%2F&max-keys=10&encoding-type=url")),
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
        merge_list_query_conditions(
            Action::S3Action(S3Action::ListBucketVersionsAction),
            &list_query_pairs(Some("prefix=&delimiter=%2F")),
            &mut conditions,
        );

        assert_eq!(conditions.get("prefix"), Some(&vec![String::new()]));
        assert_eq!(conditions.get("delimiter"), Some(&vec!["/".to_string()]));
    }

    #[test]
    fn test_merge_list_bucket_query_conditions_ignores_non_list_actions() {
        let mut conditions = HashMap::new();
        merge_list_query_conditions(
            Action::S3Action(S3Action::GetObjectAction),
            &list_query_pairs(Some("prefix=photos%2F2024%2F&delimiter=%2F&max-keys=10")),
            &mut conditions,
        );

        assert!(conditions.is_empty());
    }

    #[test]
    fn listing_conditions_reject_repeated_query_and_ignore_headers() {
        for action in [
            S3Action::ListBucketAction,
            S3Action::ListBucketVersionsAction,
            S3Action::ListBucketMultipartUploadsAction,
        ] {
            for key in ["prefix", "delimiter", "max-keys"] {
                let mut req = build_request((), Method::GET);
                req.headers.insert(key, HeaderValue::from_static("allowed"));
                let action = Action::S3Action(action);
                let credentials = rustfs_credentials::Credentials::default();
                let conditions = {
                    let principal = AuthzPrincipal::new(&credentials, "");
                    let facts = request_authz_facts(
                        &req,
                        AuthzTarget {
                            action,
                            bucket: "bucket",
                            object: "",
                            version_id: None,
                            location_constraint: None,
                        },
                    );
                    authorization_conditions(&req, &principal, &facts).unwrap()
                };
                assert!(!conditions.contains_key(key), "{key} must come from the applied query");
                req.uri = format!("/bucket?{key}=actual").parse().unwrap();
                let conditions = {
                    let principal = AuthzPrincipal::new(&credentials, "");
                    let facts = request_authz_facts(
                        &req,
                        AuthzTarget {
                            action,
                            bucket: "bucket",
                            object: "",
                            version_id: None,
                            location_constraint: None,
                        },
                    );
                    authorization_conditions(&req, &principal, &facts).unwrap()
                };
                assert_eq!(conditions.get(key), Some(&vec!["actual".to_string()]));
                req.uri = format!("/bucket?{key}=actual&{key}=allowed").parse().unwrap();
                let error = {
                    let principal = AuthzPrincipal::new(&credentials, "");
                    let facts = request_authz_facts(
                        &req,
                        AuthzTarget {
                            action,
                            bucket: "bucket",
                            object: "",
                            version_id: None,
                            location_constraint: None,
                        },
                    );
                    authorization_conditions(&req, &principal, &facts).unwrap_err()
                };
                assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
            }
        }
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
    fn access_facts_preserve_verified_client_context() {
        use crate::shared_types::RemoteAddr;
        use rustfs_trusted_proxies::{ClientInfo, ValidationMode};

        let trusted = ClientInfo::from_trusted_proxy(
            "203.0.113.9".parse().expect("client IP"),
            None,
            Some("https".to_owned()),
            "10.0.0.2".parse().expect("proxy IP"),
            1,
            ValidationMode::Lenient,
            Vec::new(),
        );
        for (client_info, expected_ip, expected_secure) in [(None, "192.0.2.10", "false"), (Some(trusted), "203.0.113.9", "true")]
        {
            let mut req = build_request((), Method::GET);
            req.headers
                .insert("x-forwarded-for", HeaderValue::from_static("198.51.100.1"));
            req.headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
            req.extensions
                .insert(Some(RemoteAddr("192.0.2.10:443".parse().expect("peer address"))));
            if let Some(client_info) = client_info {
                req.extensions.insert(client_info);
            }
            let conditions = remaining_retention_days_conditions(&req, Action::S3Action(S3Action::GetObjectAction));
            assert_eq!(conditions.get("SourceIp"), Some(&vec![expected_ip.to_owned()]));
            assert_eq!(conditions.get("SecureTransport"), Some(&vec![expected_secure.to_owned()]));
            assert!(req.extensions.get::<Option<RemoteAddr>>().is_some(), "facts must only borrow extensions");
        }
    }

    #[tokio::test]
    async fn versioned_tagging_requires_version_permissions() {
        use rustfs_policy::policy::{Args, Policy};
        let policy = Policy::parse_config(br#"{
            "Version":"2012-10-17",
            "Statement":[{"Effect":"Allow","Action":["s3:GetObjectTagging","s3:PutObjectTagging","s3:DeleteObjectTagging"],"Resource":"arn:aws:s3:::bucket/*"}]
        }"#).expect("current-object tagging policy should parse");
        let conditions = HashMap::new();
        let claims = HashMap::new();
        let groups = None;
        for (current, versioned) in [
            (S3Action::GetObjectTaggingAction, S3Action::GetObjectVersionTaggingAction),
            (S3Action::PutObjectTaggingAction, S3Action::PutObjectVersionTaggingAction),
            (S3Action::DeleteObjectTaggingAction, S3Action::DeleteObjectVersionTaggingAction),
        ] {
            for version in [None, Some("null"), Some("0194e0f1-0000-7000-8000-000000000000")] {
                let action = versioned_tagging_action(current, version);
                assert_eq!(action, Action::S3Action(if version.is_some() { versioned } else { current }));
                let args = Args {
                    account: "writer",
                    groups: &groups,
                    action,
                    bucket: "bucket",
                    conditions: &conditions,
                    is_owner: false,
                    object: "object",
                    claims: &claims,
                    deny_only: false,
                };
                assert_eq!(policy.is_allowed(&args).await, version.is_none(), "version selector {version:?}");
            }
        }
    }

    #[test]
    fn object_creation_operation_classifies_only_multipart_staging() {
        for (operation, staging) in [
            ("CreateMultipartUpload", true),
            ("UploadPart", true),
            ("UploadPartCopy", true),
            ("PutObject", false),
            ("CopyObject", false),
            ("CompleteMultipartUpload", false),
            ("PostObject", false),
            ("GetObject", false),
            ("InternalObjectWrite", false),
        ] {
            assert_eq!(
                super::MultipartStagingOperation::for_operation(operation).is_some(),
                staging,
                "{operation}"
            );
        }
    }

    #[test]
    fn object_creation_operation_ignores_request_and_claim_spoofing() {
        use crate::auth::{OBJECT_CREATION_OPERATION_CONDITION, get_condition_values};
        for forged in ["false", "true"] {
            for claim in [
                "ObjectCreationOperation",
                "objectcreationoperation",
                "OBJECTCREATIONOPERATION",
                "ldapObjectCreationOperation",
                "ldapldapObjectCreationOperation",
            ] {
                let credentials = rustfs_credentials::Credentials {
                    claims: Some(HashMap::from([(claim.to_string(), serde_json::json!(forged))])),
                    ..Default::default()
                };
                let mut req = build_request((), Method::GET);
                req.uri = format!("/bucket/key?ObjectCreationOperation={forged}&uploadId=fake&partNumber=1")
                    .parse()
                    .expect("spoofed query should parse");
                req.headers
                    .insert("objectcreationoperation", HeaderValue::from_str(forged).expect("header should parse"));
                assert!(
                    !get_condition_values(&req.headers, &credentials, None, None, None)
                        .keys()
                        .any(|key| key.eq_ignore_ascii_case(OBJECT_CREATION_OPERATION_CONDITION))
                );
                for staging in [false, true] {
                    if staging {
                        req.extensions.insert(super::MultipartStagingOperation);
                    }
                    for internal in [false, true] {
                        if internal {
                            req.extensions.insert(InternalObjectAuthorization);
                        } else {
                            req.extensions.remove::<InternalObjectAuthorization>();
                        }
                        for action in [
                            S3Action::PutObjectAction,
                            S3Action::GetObjectAction,
                            S3Action::GetObjectVersionAction,
                        ] {
                            let principal = AuthzPrincipal::new(&credentials, "");
                            let facts = request_authz_facts(
                                &req,
                                AuthzTarget {
                                    action: Action::S3Action(action),
                                    bucket: "",
                                    object: "",
                                    version_id: None,
                                    location_constraint: None,
                                },
                            );
                            let conditions = authorization_conditions(&req, &principal, &facts).expect("conditions should build");
                            let values = conditions
                                .iter()
                                .filter(|(key, _)| key.eq_ignore_ascii_case(OBJECT_CREATION_OPERATION_CONDITION))
                                .collect::<Vec<_>>();
                            if action == S3Action::PutObjectAction {
                                assert_eq!(values.len(), 1, "one canonical server-derived value");
                                assert_eq!(values[0].0, OBJECT_CREATION_OPERATION_CONDITION);
                                assert_eq!(
                                    values[0].1,
                                    &vec![(internal || !staging).to_string()],
                                    "{claim}, internal={internal}, staging={staging}"
                                );
                            } else {
                                assert!(values.is_empty(), "non-PutObject action {action:?} must have no creation condition");
                            }
                        }
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn object_creation_operation_controls_identity_and_bucket_policies() {
        use rustfs_policy::policy::{Args, Policy};
        for effect in ["Allow", "Deny"] {
            for expected in [false, true] {
                let mut statements = vec![serde_json::json!({
                    "Effect": effect, "Action": "s3:PutObject", "Resource": "arn:aws:s3:::bucket/*",
                    "Condition": { "Bool": { "s3:ObjectCreationOperation": expected.to_string() } }
                })];
                if effect == "Deny" {
                    statements.push(serde_json::json!({
                        "Effect": "Allow", "Action": "s3:PutObject", "Resource": "arn:aws:s3:::bucket/*"
                    }));
                }
                let identity: Policy = serde_json::from_str(
                    &serde_json::json!({
                        "Version": "2012-10-17", "Statement": statements
                    })
                    .to_string(),
                )
                .expect("identity policy should accept the AWS Bool key");
                for statement in &mut statements {
                    statement["Principal"] = serde_json::json!("*");
                }
                let bucket: BucketPolicy = serde_json::from_str(
                    &serde_json::json!({
                        "Version": "2012-10-17", "Statement": statements
                    })
                    .to_string(),
                )
                .expect("bucket policy should accept the AWS Bool key");
                for actual in [None, Some(false), Some(true)] {
                    let conditions = actual
                        .map(|value| HashMap::from([("ObjectCreationOperation".to_string(), vec![value.to_string()])]))
                        .unwrap_or_default();
                    let allowed = if effect == "Allow" {
                        actual == Some(expected)
                    } else {
                        actual != Some(expected)
                    };
                    assert_eq!(
                        identity
                            .is_allowed(&Args {
                                account: "writer",
                                groups: &None,
                                action: Action::S3Action(S3Action::PutObjectAction),
                                bucket: "bucket",
                                object: "object",
                                conditions: &conditions,
                                is_owner: false,
                                claims: &HashMap::new(),
                                deny_only: false,
                            })
                            .await,
                        allowed,
                        "identity {effect}, expected={expected}, actual={actual:?}"
                    );
                    assert_eq!(
                        bucket
                            .is_allowed(&BucketPolicyArgs {
                                account: "writer",
                                groups: &None,
                                action: Action::S3Action(S3Action::PutObjectAction),
                                bucket: "bucket",
                                object: "object",
                                conditions: &conditions,
                                is_owner: false,
                            })
                            .await,
                        allowed,
                        "bucket {effect}, expected={expected}, actual={actual:?}"
                    );
                }
            }
        }
    }

    fn remaining_retention_days_conditions<T>(req: &S3Request<T>, action: Action) -> HashMap<String, Vec<String>> {
        let credentials = rustfs_credentials::Credentials::default();
        let principal = AuthzPrincipal::new(&credentials, "");
        let facts = request_authz_facts(
            req,
            AuthzTarget {
                action,
                bucket: "",
                object: "",
                version_id: None,
                location_constraint: None,
            },
        );
        authorization_conditions(req, &principal, &facts).expect("conditions should build")
    }

    #[test]
    fn legal_hold_policy_checks_body_instead_of_conflicting_headers() {
        let mut req = build_request(
            PutObjectLegalHoldInput {
                legal_hold: Some(ObjectLockLegalHold {
                    status: Some(ObjectLockLegalHoldStatus::from_static(ObjectLockLegalHoldStatus::OFF)),
                }),
                ..Default::default()
            },
            Method::PUT,
        );
        req.headers
            .insert("x-amz-object-lock-legal-hold", HeaderValue::from_static("ON"));
        let action = Action::S3Action(S3Action::PutObjectLegalHoldAction);
        // Without the hook's extension the header leaks into the decision.
        let conditions = remaining_retention_days_conditions(&req, action);
        assert_eq!(conditions.get("object-lock-legal-hold"), Some(&vec!["ON".to_string()]));
        req.extensions
            .insert(RequestedObjectLegalHold(req.input.legal_hold.clone().unwrap_or_default()));
        let conditions = remaining_retention_days_conditions(&req, action);
        assert_eq!(conditions.get("object-lock-legal-hold"), Some(&vec!["OFF".to_string()]));
        // A body without a status stores no hold change; the header must not fill it in.
        req.extensions
            .insert(RequestedObjectLegalHold(ObjectLockLegalHold::default()));
        let conditions = remaining_retention_days_conditions(&req, action);
        assert!(!conditions.contains_key("object-lock-legal-hold"));
    }

    #[test]
    fn tagging_policy_checks_body_instead_of_conflicting_headers() {
        let mut req = build_request(
            PutObjectTaggingInput::builder()
                .bucket("bucket".to_string())
                .key("object".to_string())
                .tagging(Tagging {
                    tag_set: vec![Tag {
                        key: Some("classification".to_string()),
                        value: Some("confidential".to_string()),
                    }],
                })
                .build()
                .expect("valid tagging request DTO"),
            Method::PUT,
        );
        req.headers
            .insert("x-amz-tagging", HeaderValue::from_static("classification=public"));
        let action = Action::S3Action(S3Action::PutObjectTaggingAction);
        req.extensions.insert(RequestedObjectTags(req.input.tagging.clone()));
        let conditions = remaining_retention_days_conditions(&req, action);
        assert_eq!(conditions.get("RequestObjectTag/classification"), Some(&vec!["confidential".to_string()]));
        assert_eq!(conditions.get("RequestObjectTagKeys"), Some(&vec!["classification".to_string()]));
        req.extensions.insert(RequestedObjectTags(Tagging::default()));
        let conditions = remaining_retention_days_conditions(&req, action);
        assert!(!conditions.contains_key("RequestObjectTag/classification"));
        assert_eq!(conditions.get("RequestObjectTagKeys"), Some(&Vec::new()));
    }

    #[test]
    fn post_object_policy_checks_form_fields_instead_of_headers() {
        let mut req = build_request(
            PutObjectInput {
                bucket: "bucket".to_string(),
                key: "object".to_string(),
                object_lock_legal_hold_status: Some(ObjectLockLegalHoldStatus::from_static(ObjectLockLegalHoldStatus::ON)),
                object_lock_mode: Some(ObjectLockMode::from_static(ObjectLockMode::COMPLIANCE)),
                tagging: Some("classification=confidential".to_string()),
                ..Default::default()
            },
            Method::POST,
        );
        req.headers
            .insert("x-amz-object-lock-legal-hold", HeaderValue::from_static("OFF"));
        req.headers
            .insert("x-amz-object-lock-mode", HeaderValue::from_static("GOVERNANCE"));
        req.headers
            .insert("x-amz-object-lock-retain-until-date", HeaderValue::from_static("2029-01-01T00:00:00Z"));
        req.headers
            .insert("x-amz-tagging", HeaderValue::from_static("classification=public"));
        set_post_object_policy_headers(&mut req).expect("form fields project onto policy headers");
        let action = Action::S3Action(S3Action::PutObjectAction);
        let conditions = remaining_retention_days_conditions(&req, action);
        assert_eq!(conditions.get("object-lock-legal-hold"), Some(&vec!["ON".to_string()]));
        assert_eq!(conditions.get("object-lock-mode"), Some(&vec!["COMPLIANCE".to_string()]));
        // The form sets no retain-until date, so the header must not stand in for one.
        assert!(!conditions.contains_key("object-lock-retain-until-date"));
        assert_eq!(conditions.get("RequestObjectTag/classification"), Some(&vec!["confidential".to_string()]));
        // A PUT keeps reading its headers.
        let mut put = build_request(
            PutObjectInput {
                bucket: "bucket".to_string(),
                key: "object".to_string(),
                ..Default::default()
            },
            Method::PUT,
        );
        put.headers
            .insert("x-amz-object-lock-legal-hold", HeaderValue::from_static("ON"));
        set_post_object_policy_headers(&mut put).expect("PUT is untouched");
        let conditions = remaining_retention_days_conditions(&put, action);
        assert_eq!(conditions.get("object-lock-legal-hold"), Some(&vec!["ON".to_string()]));
    }

    #[tokio::test]
    async fn copy_source_conditions_use_the_decoded_source_for_both_copy_operations() {
        use rustfs_policy::policy::{Args, BucketPolicy, BucketPolicyArgs, Policy};
        let mut document = serde_json::json!({"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*"},
            {"Effect":"Deny","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*",
             "Condition":{"StringLike":{"s3:x-amz-copy-source":"secret/*"}}}
        ]});
        let identity =
            Policy::parse_config(&serde_json::to_vec(&document).expect("encode identity policy")).expect("parse policy");
        for statement in document["Statement"].as_array_mut().expect("statements") {
            statement["Principal"] = serde_json::json!("*");
        }
        let bucket: BucketPolicy =
            serde_json::from_slice(&serde_json::to_vec(&document).expect("encode bucket policy")).expect("parse bucket policy");
        for spelling in [
            "secret/report.txt",
            "%73ecret/report.txt",
            "secret%2Freport.txt",
            "/secret/report.txt",
            "%2Fsecret%2Freport.txt?versionId=00000000-0000-0000-0000-000000000000",
            "public/report.txt",
            "/public%2Freport.txt",
        ] {
            for multipart in [false, true] {
                let source = CopySource::parse(spelling).expect("parse source exactly as the request parser does");
                let mut req = if multipart {
                    let input = UploadPartCopyInput::builder()
                        .bucket("bucket".to_string())
                        .key("copy".to_string())
                        .copy_source(source)
                        .upload_id("unused".to_string())
                        .part_number(1)
                        .build()
                        .expect("part copy input");
                    let mut req = build_request(input, Method::PUT);
                    ensure_req_info(&mut req);
                    let _ = FS::new().upload_part_copy(&mut req).await;
                    req.map_input(|_| ())
                } else {
                    let input = CopyObjectInput::builder()
                        .bucket("bucket".to_string())
                        .key("copy".to_string())
                        .copy_source(source)
                        .build()
                        .expect("copy input");
                    let mut req = build_request(input, Method::PUT);
                    ensure_req_info(&mut req);
                    let _ = FS::new().copy_object(&mut req).await;
                    req.map_input(|_| ())
                };
                req.headers
                    .insert("x-amz-copy-source", HeaderValue::from_str(spelling).expect("source header"));
                let action = Action::S3Action(S3Action::PutObjectAction);
                let conditions = remaining_retention_days_conditions(&req, action);
                let canonical = &conditions["x-amz-copy-source"];
                assert_eq!(canonical.len(), 1, "one semantic source, independent of header spelling");
                let expected = if spelling.contains("public") {
                    "public/report.txt"
                } else if spelling.contains('?') {
                    "secret/report.txt?versionId=null"
                } else {
                    "secret/report.txt"
                };
                assert_eq!(canonical[0], expected, "{spelling}, multipart={multipart}");
                let allowed = expected.starts_with("public/");
                assert_eq!(
                    identity
                        .is_allowed(&Args {
                            account: "user",
                            groups: &None,
                            action,
                            bucket: "bucket",
                            object: "copy",
                            conditions: &conditions,
                            claims: &HashMap::new(),
                            is_owner: false,
                            deny_only: false
                        })
                        .await,
                    allowed
                );
                assert_eq!(
                    bucket
                        .is_allowed(&BucketPolicyArgs {
                            account: "user",
                            groups: &None,
                            action,
                            bucket: "bucket",
                            object: "copy",
                            conditions: &conditions,
                            is_owner: false
                        })
                        .await,
                    allowed
                );
            }
        }
    }

    #[tokio::test]
    async fn legal_hold_policy_checks_xml_body() {
        let action = Action::S3Action(S3Action::PutObjectLegalHoldAction);
        for header in [None, Some("ON")] {
            let mut req = build_request(
                PutObjectLegalHoldInput {
                    bucket: "bucket".to_string(),
                    key: "object".to_string(),
                    legal_hold: Some(ObjectLockLegalHold {
                        status: Some(ObjectLockLegalHoldStatus::from_static("OFF")),
                    }),
                    ..Default::default()
                },
                Method::PUT,
            );
            ensure_req_info(&mut req);
            if let Some(header) = header {
                req.headers
                    .insert("x-amz-object-lock-legal-hold", HeaderValue::from_static(header));
            }
            let _ = FS::new().put_object_legal_hold(&mut req).await;
            let conditions = remaining_retention_days_conditions(&req, action);
            assert_eq!(conditions.get("object-lock-legal-hold"), Some(&vec!["OFF".to_string()]));
            assert_requested_value_denied(action, "s3:object-lock-legal-hold", "OFF", &conditions).await;
        }
    }

    #[tokio::test]
    async fn tagging_policy_checks_xml_body() {
        for version in [None, Some("version")] {
            let mut req = build_request(
                PutObjectTaggingInput {
                    bucket: "bucket".to_string(),
                    key: "object".to_string(),
                    version_id: version.map(str::to_string),
                    tagging: Tagging {
                        tag_set: vec![Tag {
                            key: Some("classification".to_string()),
                            value: Some("public".to_string()),
                        }],
                    },
                    checksum_algorithm: None,
                    content_md5: None,
                    expected_bucket_owner: None,
                    request_payer: None,
                },
                Method::PUT,
            );
            ensure_req_info(&mut req);
            req.headers
                .insert("x-amz-tagging", HeaderValue::from_static("classification=secret&spoofed=yes"));
            let _ = FS::new().put_object_tagging(&mut req).await;
            let action = versioned_tagging_action(S3Action::PutObjectTaggingAction, version);
            let conditions = remaining_retention_days_conditions(&req, action);
            assert_eq!(conditions.get("RequestObjectTag/classification"), Some(&vec!["public".to_string()]));
            assert_eq!(conditions.get("RequestObjectTagKeys"), Some(&vec!["classification".to_string()]));
            assert!(!conditions.contains_key("RequestObjectTag/spoofed"));
            assert_requested_value_denied(action, "s3:RequestObjectTag/classification", "public", &conditions).await;
        }
    }

    async fn assert_requested_value_denied(action: Action, key: &str, value: &str, conditions: &HashMap<String, Vec<String>>) {
        let document = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": action, "Resource": "arn:aws:s3:::bucket/*"},
                {"Effect": "Deny", "Action": action, "Resource": "arn:aws:s3:::bucket/*",
                 "Condition": {"StringEquals": {key: value}}}
            ]
        });
        let policy = rustfs_policy::policy::Policy::parse_config(&serde_json::to_vec(&document).unwrap()).unwrap();
        let claims = HashMap::new();
        let args = rustfs_policy::policy::Args {
            account: "writer",
            groups: &None,
            action,
            bucket: "bucket",
            conditions,
            is_owner: false,
            object: "object",
            claims: &claims,
            deny_only: false,
        };
        assert!(!policy.is_allowed(&args).await, "the applied request value must trigger Deny");
    }

    #[tokio::test]
    async fn post_object_policy_checks_form_fields() {
        let mut req = build_request(
            PutObjectInput {
                bucket: "bucket".to_string(),
                key: "object".to_string(),
                object_lock_mode: Some(ObjectLockMode::from_static("COMPLIANCE")),
                object_lock_legal_hold_status: Some(ObjectLockLegalHoldStatus::from_static("ON")),
                object_lock_retain_until_date: Some(Timestamp::parse(TimestampFormat::DateTime, "2035-01-01T00:00:00Z").unwrap()),
                storage_class: Some(StorageClass::from_static("STANDARD_IA")),
                tagging: Some("classification=public".to_string()),
                ..Default::default()
            },
            Method::POST,
        );
        ensure_req_info(&mut req);
        for (key, value) in [
            ("x-amz-object-lock-mode", "GOVERNANCE"),
            ("x-amz-object-lock-legal-hold", "OFF"),
            ("x-amz-object-lock-retain-until-date", "2029-01-01T00:00:00Z"),
            ("x-amz-storage-class", "STANDARD"),
            ("x-amz-tagging", "classification=secret&spoofed=yes"),
            ("x-amz-acl", "public-read"),
        ] {
            req.headers.insert(key, HeaderValue::from_static(value));
        }
        let _ = FS::new().put_object(&mut req).await;
        let action = Action::S3Action(S3Action::PutObjectAction);
        let conditions = remaining_retention_days_conditions(&req, action);
        for (key, value) in [
            ("object-lock-mode", "COMPLIANCE"),
            ("object-lock-legal-hold", "ON"),
            ("object-lock-retain-until-date", "2035-01-01T00:00:00Z"),
            ("x-amz-storage-class", "STANDARD_IA"),
            ("RequestObjectTag/classification", "public"),
        ] {
            assert_eq!(conditions.get(key), Some(&vec![value.to_string()]));
        }
        assert!(!conditions.contains_key("x-amz-acl"));
        assert!(!conditions.contains_key("RequestObjectTag/spoofed"));
        assert_requested_value_denied(action, "s3:object-lock-mode", "COMPLIANCE", &conditions).await;
        assert_requested_value_denied(action, "s3:RequestObjectTag/classification", "public", &conditions).await;
        let post_req = req.map_input(|input| PostObjectInput {
            bucket: input.bucket,
            key: input.key,
            object_lock_mode: input.object_lock_mode,
            object_lock_legal_hold_status: input.object_lock_legal_hold_status,
            object_lock_retain_until_date: input.object_lock_retain_until_date,
            storage_class: input.storage_class,
            tagging: input.tagging,
            ..Default::default()
        });
        let post_conditions = remaining_retention_days_conditions(&post_req, action);
        assert_eq!(post_conditions.get("object-lock-mode"), conditions.get("object-lock-mode"));
        assert_eq!(
            post_conditions.get("RequestObjectTag/classification"),
            conditions.get("RequestObjectTag/classification")
        );
    }

    #[tokio::test]
    async fn retention_policy_checks_body_instead_of_conflicting_headers() {
        use rustfs_policy::policy::{Args, Policy};
        let mut req = build_request(
            PutObjectRetentionInput {
                retention: Some(ObjectLockRetention {
                    mode: Some(ObjectLockRetentionMode::from_static("COMPLIANCE")),
                    retain_until_date: Some(
                        Timestamp::parse(TimestampFormat::DateTime, "2035-01-02T00:00:00+23:59").expect("body date should parse"),
                    ),
                }),
                ..Default::default()
            },
            Method::PUT,
        );
        req.headers
            .insert("x-amz-object-lock-mode", HeaderValue::from_static("GOVERNANCE"));
        req.headers
            .insert("x-amz-object-lock-retain-until-date", HeaderValue::from_static("2029-01-01T00:00:00Z"));
        set_requested_object_lock_retention(&mut req);
        let action = Action::S3Action(S3Action::PutObjectRetentionAction);
        let conditions = remaining_retention_days_conditions(&req, action);
        assert_eq!(conditions.get("object-lock-mode"), Some(&vec!["COMPLIANCE".to_string()]));
        let applied = OffsetDateTime::from(
            req.input
                .retention
                .as_ref()
                .expect("body retention")
                .retain_until_date
                .clone()
                .expect("body date"),
        );
        let authorized = OffsetDateTime::parse(
            &conditions["object-lock-retain-until-date"][0],
            &time::format_description::well_known::Rfc3339,
        )
        .expect("condition date should parse");
        assert_eq!(authorized.unix_timestamp(), applied.unix_timestamp());
        let policy = Policy::parse_config(br#"{
            "Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObjectRetention","Resource":"arn:aws:s3:::bucket/*",
            "Condition":{"DateLessThan":{"s3:object-lock-retain-until-date":"2030-01-01T00:00:00Z"}}}]
        }"#).expect("bounded retention policy should parse");
        let args = Args {
            account: "writer",
            groups: &None,
            action,
            bucket: "bucket",
            conditions: &conditions,
            is_owner: false,
            object: "object",
            claims: &HashMap::new(),
            deny_only: false,
        };
        assert!(
            !policy.is_allowed(&args).await,
            "an in-range header cannot authorize an out-of-range body"
        );
        req.input.retention = None;
        set_requested_object_lock_retention(&mut req);
        let conditions = remaining_retention_days_conditions(&req, action);
        assert!(!conditions.contains_key("object-lock-mode"));
        assert!(!conditions.contains_key("object-lock-retain-until-date"));
    }

    fn request_retaining_for(retain_for: time::Duration) -> S3Request<()> {
        let mut req = build_request((), Method::PUT);
        let retain_until = OffsetDateTime::now_utc() + retain_for;
        set_requested_object_lock_retain_until(&mut req, Some(&retain_until.into()));
        req
    }

    /// AWS does not document how it rounds partial days. MinIO computes
    /// `ceil(hours / 24)` in `enforceRetentionBypassForPut`
    /// (cmd/bucket-object-lock.go), and RustFS keeps that rounding so a policy
    /// moved from MinIO evaluates the same way.
    #[test]
    fn remaining_retention_days_matches_s3_ceil_rounding() {
        let now = OffsetDateTime::now_utc();
        let cases = [
            (time::Duration::NANOSECOND, 1),
            (time::Duration::HOUR, 1),
            (time::Duration::DAY - time::Duration::SECOND, 1),
            (time::Duration::DAY, 1),
            (time::Duration::DAY + time::Duration::SECOND, 2),
            (time::Duration::days(29) + time::Duration::hours(12), 30),
            (time::Duration::days(30), 30),
            (time::Duration::days(30) + time::Duration::SECOND, 31),
        ];

        for (retain_for, expected) in cases {
            let minio_days = (retain_for.as_seconds_f64() / 3600.0 / 24.0).ceil() as i64;
            assert_eq!(minio_days, expected, "MinIO reference for {retain_for}");
            assert_eq!(remaining_retention_days(now + retain_for, now), expected, "retain for {retain_for}");
        }
    }

    #[test]
    fn remaining_retention_days_is_zero_for_dates_not_in_future() {
        let now = OffsetDateTime::now_utc();

        assert_eq!(remaining_retention_days(now, now), 0);
        assert_eq!(remaining_retention_days(now - time::Duration::NANOSECOND, now), 0);
        assert_eq!(remaining_retention_days(now - time::Duration::days(3), now), 0);
    }

    #[test]
    fn requested_retain_until_sets_remaining_retention_days_condition() {
        let req = request_retaining_for(time::Duration::days(10) - time::Duration::HOUR);

        let conditions = remaining_retention_days_conditions(&req, Action::S3Action(S3Action::PutObjectAction));

        assert_eq!(
            conditions.get(OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION),
            Some(&vec!["10".to_string()])
        );
    }

    #[test]
    fn request_without_retention_has_no_remaining_retention_days_condition() {
        let action = Action::S3Action(S3Action::PutObjectAction);
        let req = build_request((), Method::PUT);
        assert_eq!(
            remaining_retention_days_conditions(&req, action).get(OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION),
            None
        );

        let mut req = request_retaining_for(time::Duration::days(5));
        set_requested_object_lock_retain_until(&mut req, None);
        assert_eq!(
            remaining_retention_days_conditions(&req, action).get(OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION),
            None
        );
    }

    #[test]
    fn remaining_retention_days_condition_ignores_client_header() {
        let action = Action::S3Action(S3Action::PutObjectRetentionAction);
        let mut req = build_request((), Method::PUT);
        req.headers
            .insert(OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION, HeaderValue::from_static("1"));
        assert_eq!(
            remaining_retention_days_conditions(&req, action).get(OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION),
            None
        );

        let retain_until = OffsetDateTime::now_utc() + time::Duration::days(400);
        set_requested_object_lock_retain_until(&mut req, Some(&retain_until.into()));
        assert_eq!(
            remaining_retention_days_conditions(&req, action).get(OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION),
            Some(&vec!["400".to_string()])
        );
    }

    #[tokio::test]
    async fn bucket_policy_limits_retention_to_thirty_days() {
        let policy: BucketPolicy = serde_json::from_str(
            r#"{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Principal":{"AWS":"*"},
      "Action":["s3:PutObjectRetention"],
      "Resource":["arn:aws:s3:::bucket/*"]
    },
    {
      "Effect":"Deny",
      "Principal":{"AWS":"*"},
      "Action":["s3:PutObjectRetention"],
      "Resource":["arn:aws:s3:::bucket/*"],
      "Condition":{"NumericGreaterThan":{"s3:object-lock-remaining-retention-days":"30"}}
    }
  ]
}"#,
        )
        .expect("bucket policy should parse");
        let no_groups: Option<Vec<String>> = None;
        let action = Action::S3Action(S3Action::PutObjectRetentionAction);
        let spoofed_header = |mut req: S3Request<()>| {
            req.headers
                .insert(OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION, HeaderValue::from_static("1"));
            req
        };

        let cases = [
            ("29 days", request_retaining_for(time::Duration::days(29)), true),
            ("30 days", request_retaining_for(time::Duration::days(30)), true),
            ("31 days", request_retaining_for(time::Duration::days(31)), false),
            (
                "30 days and a minute",
                request_retaining_for(time::Duration::days(30) + time::Duration::MINUTE),
                false,
            ),
            ("no retention", build_request((), Method::PUT), true),
            (
                "31 days with spoofed header",
                spoofed_header(request_retaining_for(time::Duration::days(31))),
                false,
            ),
        ];

        for (name, req, allowed) in cases {
            let conditions = remaining_retention_days_conditions(&req, action);
            let args = BucketPolicyArgs {
                bucket: "bucket",
                action,
                is_owner: false,
                account: "",
                groups: &no_groups,
                conditions: &conditions,
                object: "obj",
            };

            assert_eq!(policy.is_allowed(&args).await, allowed, "{name}");
        }
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
        let principal = AuthzPrincipal::new(&credentials, "");
        let facts = request_authz_facts(
            &req,
            AuthzTarget {
                action: Action::S3Action(S3Action::PutObjectAction),
                bucket: "",
                object: "",
                version_id: None,
                location_constraint: None,
            },
        );
        let conditions =
            authorization_conditions(&req, &principal, &facts).expect("internal object authorization conditions should build");

        assert_eq!(conditions.get("authType"), Some(&vec!["REST-HEADER".to_string()]));
        assert_eq!(conditions.get("signatureversion"), Some(&vec!["AWS4-HMAC-SHA256".to_string()]));
        assert_eq!(conditions.get("UserAgent"), Some(&vec!["pyiceberg/test".to_string()]));
        assert!(!conditions.contains_key("authorization"));
        assert!(!conditions.contains_key("x-amz-server-side-encryption"));
        assert!(!conditions.contains_key("RequestObjectTag/classification"));
        assert!(!conditions.contains_key("RequestObjectTagKeys"));
    }

    #[test]
    fn replication_proxy_requires_authorization_independent_of_local_tags() {
        let mut extensions = http::Extensions::new();
        assert!(super::replication_proxy_allowed(&extensions));
        // A local miss produces an empty tag map, but still establishes that the
        // policy needs tags. Neither anonymous nor authenticated callers may proxy.
        extensions.insert(ObjectTagConditions::new("bucket", "missing", None, HashMap::new()));
        assert!(!super::replication_proxy_allowed(&extensions));
        extensions.insert(ObjectTagConditions::new(
            "bucket",
            "missing",
            Some("version"),
            HashMap::from([("ExistingObjectTag/protected".to_string(), vec!["yes".to_string()])]),
        ));
        assert!(!super::replication_proxy_allowed(&extensions));
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
        assert!(!super::replication_proxy_allowed(&request.extensions));
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

    #[tokio::test]
    #[serial]
    async fn existing_tag_hint_reuses_parsed_metadata_and_fails_closed_without_it() {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let bucket = format!("cached-policy-hint-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create hint bucket");
        let document = serde_json::json!({"Version":"2012-10-17","Statement":[{
            "Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{bucket}/*"),
            "Condition":{"StringEquals":{"s3:ExistingObjectTag/classification":"secret"}}
        }]});
        let mut metadata = (*crate::storage::get_bucket_metadata(&bucket).await.expect("bucket metadata")).clone();
        metadata.policy_config =
            Some(serde_json::from_slice(&serde_json::to_vec(&document).expect("policy JSON")).expect("parsed policy"));
        // A cached parsed snapshot is the authority used by actual policy
        // evaluation; the tag hint must consult that same snapshot.
        metadata.policy_config_json = b"invalid raw JSON".to_vec();
        crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata.clone())
            .await
            .expect("publish parsed snapshot");
        assert!(matches!(
            load_bucket_policy_existing_object_tag_hint(&store, &bucket, Action::S3Action(S3Action::GetObjectAction)).await,
            super::BucketPolicyExistingObjectTagHint::Parsed(_)
        ));
        metadata.policy_config = None;
        crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
            .await
            .expect("publish unreadable snapshot");
        assert!(matches!(
            load_bucket_policy_existing_object_tag_hint(&store, &bucket, Action::S3Action(S3Action::GetObjectAction)).await,
            super::BucketPolicyExistingObjectTagHint::ConservativeTagRequired
        ));
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

    type CapturedAudit = Arc<std::sync::Mutex<Vec<rustfs_audit::AuditEntry>>>;

    /// An auditor wired like the global one except that entries land in the returned vector.
    fn test_auditor(enabled: bool, authenticated_rate: u64, anonymous_rate: u64) -> (DenialAuditor, CapturedAudit) {
        let captured: CapturedAudit = Arc::default();
        let sink = Arc::clone(&captured);
        // A frozen clock: no wall-clock refill can change what the limiter admits.
        let t0 = std::time::Instant::now();
        let limiter = DenialRateLimiter::new(authenticated_rate, anonymous_rate, t0);
        let auditor =
            DenialAuditor::new(limiter, move || enabled, move |_, entry| sink.lock().unwrap().push(entry)).with_clock(move || t0);
        (auditor, captured)
    }

    fn denial_request(request_id: &str, authenticated: bool) -> S3Request<()> {
        let mut req = build_request((), Method::GET);
        req.uri = Uri::from_static("/audit-bucket/audit-object");
        req.extensions.insert(crate::storage::request_context::RequestContext {
            request_id: request_id.to_string(),
            x_amz_request_id: request_id.to_string(),
            trace_id: None,
            span_id: None,
            start_time: std::time::Instant::now(),
        });
        req.extensions.insert(ReqInfo {
            cred: authenticated.then(|| rustfs_credentials::Credentials {
                access_key: "denied-user".to_string(),
                ..Default::default()
            }),
            bucket: Some("audit-bucket".to_string()),
            object: Some("audit-object".to_string()),
            ..Default::default()
        });
        req
    }

    fn denial_context(auditor: &DenialAuditor, quiet: bool) -> DenialContext<'_> {
        DenialContext {
            quiet,
            suppress_audit: false,
            auditor,
            bucket: "audit-bucket",
            object: "audit-object",
            version_id: Some("version"),
            account: Some("denied-user"),
            is_owner: false,
        }
    }

    const GET_OBJECT: Action = Action::S3Action(S3Action::GetObjectAction);

    /// Issue #5740: the denial helper must keep the client-facing error identical
    /// to the historical bare response — the added diagnostics are log-only.
    #[test]
    fn test_access_denied_helper_keeps_generic_wire_response() {
        let (auditor, captured) = test_auditor(true, 10, 10);
        let req = denial_request("denial-wire-response", true);
        let err = denial_context(&auditor, false).deny(&req, "bucket_policy_explicit_deny", GET_OBJECT);
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
        assert_eq!(err.message(), Some("Access Denied"));
        assert_eq!(captured.lock().unwrap().len(), 1);
    }

    /// A real denial yields exactly one audit entry naming the account, bucket, object,
    /// denied action and internal reason.
    #[test]
    fn denial_is_audited_once_with_account_resource_action_and_reason() {
        let (auditor, captured) = test_auditor(true, 10, 10);
        let req = denial_request("denial-audited-once", true);
        let _ = denial_context(&auditor, false).deny(&req, "iam_explicit_deny", Action::S3Action(S3Action::PutObjectAction));

        let entries = captured.lock().unwrap();
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry.request_id.as_deref(), Some("denial-audited-once"));
        assert_eq!(entry.access_key.as_deref(), Some("denied-user"));
        assert_eq!(entry.api.name.as_deref(), Some("s3:PutObject"));
        assert_eq!(entry.api.bucket.as_deref(), Some("audit-bucket"));
        assert_eq!(entry.api.object.as_deref(), Some("audit-object"));
        assert_eq!(entry.api.status.as_deref(), Some("failure"));
        assert_eq!(entry.api.status_code, Some(403));
        assert_eq!(entry.error.as_deref(), Some("Access Denied"));
        let tags = entry.tags.as_ref().expect("denial entry carries its reason");
        assert_eq!(tags.get("denialReason").and_then(|value| value.as_str()), Some("iam_explicit_deny"));
        assert!(!tags.contains_key("suppressedDenials"));
    }

    /// Probe denials (per-bucket ListBuckets fallback, per-object metadata collection) are
    /// expected filter outcomes and must not flood the audit stream.
    #[test]
    fn quiet_probe_denial_is_not_audited() {
        let (auditor, captured) = test_auditor(true, 10, 10);
        let req = denial_request("denial-quiet-probe", true);
        let err =
            denial_context(&auditor, true).deny(&req, "no_policy_allows_action", Action::S3Action(S3Action::ListBucketAction));
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
        assert!(captured.lock().unwrap().is_empty());
    }

    #[test]
    fn denial_is_not_audited_when_the_audit_module_is_off() {
        let (auditor, captured) = test_auditor(false, 10, 10);
        let req = denial_request("denial-audit-off", true);
        let _ = denial_context(&auditor, false).deny(&req, "no_policy_allows_action", GET_OBJECT);
        assert!(captured.lock().unwrap().is_empty());
    }

    #[test]
    fn denial_audited_in_aggregate_by_the_caller_is_not_audited_per_key() {
        let (auditor, captured) = test_auditor(true, 10, 10);
        let req = denial_request("denial-per-key", true);
        let mut denial = denial_context(&auditor, false);
        denial.suppress_audit = true;
        let _ = denial.deny(&req, "iam_explicit_deny", GET_OBJECT);
        assert!(captured.lock().unwrap().is_empty());
    }

    /// A flood of denials emits a bounded number of entries; the rest are only counted.
    #[test]
    fn denial_flood_emits_bounded_entries() {
        let (auditor, captured) = test_auditor(true, 3, 3);
        let req = denial_request("denial-flood", true);
        for _ in 0..20 {
            let _ = denial_context(&auditor, false).deny(&req, "no_policy_allows_action", GET_OBJECT);
        }
        assert_eq!(captured.lock().unwrap().len(), 3);
    }

    /// The entry that follows a suppressed run reports how many denials were dropped.
    #[test]
    fn entry_after_a_flood_reports_the_suppressed_count() {
        use crate::storage::helper::DenialScope;
        let (auditor, captured) = test_auditor(true, 1, 1);
        let req = denial_request("denial-suppressed-count", true);
        let t0 = std::time::Instant::now();
        for _ in 0..5 {
            auditor.audit_at(&req, &GET_OBJECT, "no_policy_allows_action", DenialScope::Request, t0);
        }
        auditor.audit_at(
            &req,
            &GET_OBJECT,
            "no_policy_allows_action",
            DenialScope::Request,
            t0 + std::time::Duration::from_secs(2),
        );
        let entries = captured.lock().unwrap();
        assert_eq!(entries.len(), 2);
        let tag = |index: usize| {
            entries[index]
                .tags
                .as_ref()
                .unwrap()
                .get("suppressedDenials")
                .and_then(|v| v.as_u64())
        };
        assert_eq!(tag(0), None);
        assert_eq!(tag(1), Some(4));
    }

    /// Anonymous callers have their own, lower limit, so an unauthenticated scanner cannot
    /// use up the budget for authenticated callers.
    #[test]
    fn anonymous_denials_are_limited_separately_from_authenticated_ones() {
        let (auditor, captured) = test_auditor(true, 10, 1);
        let anonymous = denial_request("denial-anonymous", false);
        let authenticated = denial_request("denial-authenticated", true);
        for _ in 0..5 {
            let _ = denial_context(&auditor, false).deny(&anonymous, "no_policy_allows_action", GET_OBJECT);
        }
        for _ in 0..5 {
            let _ = denial_context(&auditor, false).deny(&authenticated, "no_policy_allows_action", GET_OBJECT);
        }
        let entries = captured.lock().unwrap();
        let count = |id: &str| entries.iter().filter(|entry| entry.request_id.as_deref() == Some(id)).count();
        assert_eq!(count("denial-anonymous"), 1);
        assert_eq!(count("denial-authenticated"), 5);
    }

    /// Exits that have no `DenialContext` of their own audit through the request's own state
    /// and honor the same probe flags.
    #[test]
    fn request_denial_exit_is_audited_and_honors_the_probe_flags() {
        let (auditor, captured) = test_auditor(true, 10, 10);
        let mut req = denial_request("denial-exit", true);
        audit_request_denial(&req, &auditor, "table_resource_policy_denied", GET_OBJECT);
        assert_eq!(captured.lock().unwrap().len(), 1);

        req.extensions.get_mut::<ReqInfo>().unwrap().suppress_denial_log = true;
        audit_request_denial(&req, &auditor, "table_resource_policy_denied", GET_OBJECT);
        assert_eq!(captured.lock().unwrap().len(), 1);

        req.extensions.get_mut::<ReqInfo>().unwrap().suppress_denial_log = false;
        req.extensions.get_mut::<ReqInfo>().unwrap().suppress_denial_audit = true;
        audit_request_denial(&req, &auditor, "table_resource_policy_denied", GET_OBJECT);
        assert_eq!(captured.lock().unwrap().len(), 1);
    }

    /// The table-policy, anonymous table and force-delete exits live deep in the authorization
    /// flow and need a table catalog to reach. This source-level guard fails if one stops
    /// auditing; it does not execute them.
    #[test]
    fn denial_exits_without_a_context_call_the_audit_function() {
        // Only the production code counts; the tests' own calls must not satisfy the guard.
        let source = include_str!("access.rs").split("\nmod tests {").next().unwrap();
        for reason in [
            "\"table_resource_policy_denied\"",
            "\"anonymous_table_data_plane_access\"",
            "\"recursive_force_delete_requires_authenticated_caller\"",
        ] {
            let call = source
                .match_indices("audit_request_denial(")
                .any(|(at, _)| source[at..].chars().take(260).collect::<String>().contains(reason));
            assert!(call, "{reason} exit does not call audit_request_denial");
        }
    }

    /// DeleteObjects audits its refused keys as one entry per denied action, carrying the count and
    /// no single object, and the per-key mark is cleared afterwards.
    #[test]
    fn bulk_denial_is_one_entry_per_denied_action_with_the_key_count() {
        let (auditor, captured) = test_auditor(true, 10, 10);
        let mut req = denial_request("denial-bulk", true);

        let nothing_denied = BulkDenialAudit::start(&mut req).unwrap();
        assert!(req.extensions.get::<ReqInfo>().unwrap().suppress_denial_audit);
        nothing_denied.finish_with(&mut req, &auditor).unwrap();
        assert!(!req.extensions.get::<ReqInfo>().unwrap().suppress_denial_audit);
        assert!(captured.lock().unwrap().is_empty());

        let mut bulk = BulkDenialAudit::start(&mut req).unwrap();
        let delete = Action::S3Action(S3Action::DeleteObjectAction);
        let bypass = Action::S3Action(S3Action::BypassGovernanceRetentionAction);
        for action in [delete, delete, bypass, delete] {
            bulk.denied(action);
        }
        bulk.finish_with(&mut req, &auditor).unwrap();

        let entries = captured.lock().unwrap();
        assert_eq!(entries.len(), 2);
        let summary: Vec<_> = entries
            .iter()
            .map(|entry| {
                assert_eq!(entry.api.object, None);
                (
                    entry.api.name.clone().unwrap(),
                    entry
                        .tags
                        .as_ref()
                        .unwrap()
                        .get("deniedKeys")
                        .and_then(|value| value.as_u64())
                        .unwrap(),
                )
            })
            .collect();
        assert_eq!(
            summary,
            vec![
                ("s3:DeleteObject".to_string(), 3),
                ("s3:BypassGovernanceRetention".to_string(), 1)
            ]
        );
    }

    #[test]
    fn delete_authorization_selects_the_addressed_version() {
        assert_eq!(delete_object_authorize_action(None), Action::S3Action(S3Action::DeleteObjectAction));
        for version_id in ["null", "8f418ad0-f9f4-4458-83b2-cc72bc6f1b70"] {
            assert_eq!(
                delete_object_authorize_action(Some(version_id)),
                Action::S3Action(S3Action::DeleteObjectVersionAction)
            );
        }
    }

    #[tokio::test]
    async fn test_anonymous_version_delete_uses_version_policy_and_tag_hint() {
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

        let needs_tag_current = bucket_policy_needs_existing_object_tag_from_hint(&hint, &args_delete).await;
        let needs_tag_version = bucket_policy_needs_existing_object_tag_from_hint(&hint, &args_delete_version).await;
        assert!(!needs_tag_current, "DeleteObject statement itself does not require ExistingObjectTag");
        assert!(
            needs_tag_version,
            "DeleteObjectVersion statement requires ExistingObjectTag when version delete is evaluated"
        );
        assert!(
            needs_tag_version,
            "the selected version action must require tag fetch for DeleteObject(versionId)"
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

    #[test]
    #[serial]
    fn odm_read_capture_preserves_access_and_parameter_error_order() {
        crate::app::gating_test_env::run_large_stack_test("odm-access-order", || async {
            let context = crate::app::gating_test_env::shared_gating_ambient().await;
            let server_ctx = ServerContextSlot::new();
            assert!(server_ctx.install(Arc::clone(&context)));
            let fs = FS::with_server_ctx(server_ctx);
            let sys = crate::on_demand_migration::OnDemandMigrationSys::get();
            let enabled_before = sys.is_module_enabled();
            let bucket = format!("odm-access-missing-{}", uuid::Uuid::new_v4());
            for enabled in [false, true] {
                sys.set_module_enabled(enabled);
                let mut get = build_request(
                    GetObjectInput {
                        bucket: bucket.clone(),
                        key: "key".into(),
                        part_number: Some(0),
                        ..Default::default()
                    },
                    Method::GET,
                );
                get.extensions.insert(ReqInfo {
                    cred: Some(rustfs_credentials::Credentials::default()),
                    is_owner: true,
                    ..Default::default()
                });
                get.extensions.insert(fs.server_ctx().clone());
                let mut head = get.clone().map_input(|_| HeadObjectInput {
                    bucket: bucket.clone(),
                    key: "key".into(),
                    part_number: Some(1),
                    range: Some(s3s::dto::Range::Int { first: 0, last: Some(1) }),
                    ..Default::default()
                });
                head.method = Method::HEAD;
                let mut list = get.clone().map_input(|_| ListObjectsV2Input {
                    bucket: bucket.clone(),
                    max_keys: Some(-1),
                    ..Default::default()
                });
                fs.get_object(&mut get)
                    .await
                    .expect("ordinary GET access must not require bucket identity");
                fs.head_object(&mut head)
                    .await
                    .expect("ordinary HEAD access must not require bucket identity");
                fs.list_objects_v2(&mut list)
                    .await
                    .expect("ordinary LIST access must not require bucket identity");
                assert!(matches!(
                    get.extensions.get::<super::OdmReadGenerationGuard>(),
                    Some(super::OdmReadGenerationGuard::Unavailable)
                ));
                assert!(matches!(
                    head.extensions.get::<super::OdmReadGenerationGuard>(),
                    Some(super::OdmReadGenerationGuard::Unavailable)
                ));
                assert!(matches!(
                    list.extensions.get::<super::OdmReadGenerationGuard>(),
                    Some(super::OdmReadGenerationGuard::Unavailable)
                ));
                let usecase = crate::app::object_usecase::DefaultObjectUsecase::with_context(Some(Arc::clone(&context)));
                assert_eq!(
                    crate::app::test_seams::get_object(&usecase, get)
                        .await
                        .expect_err("bad GET part number")
                        .code(),
                    &S3ErrorCode::InvalidArgument
                );
                assert_eq!(
                    crate::app::test_seams::head_object(&usecase, head)
                        .await
                        .expect_err("range and part number conflict")
                        .code(),
                    &S3ErrorCode::InvalidArgument
                );
                assert_eq!(
                    crate::app::test_seams::list_objects_v2(
                        &crate::app::bucket_usecase::DefaultBucketUsecase::with_context(Some(Arc::clone(&context))),
                        list
                    )
                    .await
                    .expect_err("negative max keys")
                    .code(),
                    &S3ErrorCode::InvalidArgument
                );
            }
            sys.set_module_enabled(enabled_before);
        });
    }

    #[test]
    #[serial]
    fn odm_read_capture_failure_is_deferred_until_source_miss() {
        crate::app::gating_test_env::run_large_stack_test("odm-access-failure", || async {
            let context = crate::app::gating_test_env::shared_gating_ambient().await;
            let store = context.object_store();
            let server_ctx = ServerContextSlot::new();
            assert!(server_ctx.install(Arc::clone(&context)));
            let fs = FS::with_server_ctx(server_ctx);
            let sys = crate::on_demand_migration::OnDemandMigrationSys::get();
            let enabled_before = sys.is_module_enabled();
            sys.set_module_enabled(true);
            let bucket = format!("odm-capture-failure-{}", uuid::Uuid::new_v4());
            let mut config: crate::on_demand_migration::OnDemandMigrationConfig = serde_json::from_str(r#"{"source":{"provider":"minio","endpoint":"https://source.example.com","region":"us-east-1","bucket":"source","credentials":{"access_key":"test","secret_key":"test"}}}"#).expect("source config");
            config.policy.list_through = true;
            sys.apply_for_incarnation(&bucket, uuid::Uuid::new_v4(), Some(&config)).await;
            let mut get = build_request(
                GetObjectInput {
                    bucket: bucket.clone(),
                    key: "local".into(),
                    ..Default::default()
                },
                Method::GET,
            );
            get.extensions.insert(ReqInfo {
                cred: Some(rustfs_credentials::Credentials::default()),
                is_owner: true,
                ..Default::default()
            });
            get.extensions.insert(fs.server_ctx().clone());
            let mut head = get.clone().map_input(|_| HeadObjectInput {
                bucket: bucket.clone(),
                key: "local".into(),
                ..Default::default()
            });
            head.method = Method::HEAD;
            let mut list = get.clone().map_input(|_| ListObjectsV2Input {
                bucket: bucket.clone(),
                ..Default::default()
            });
            fs.list_objects_v2(&mut list)
                .await
                .expect("capture failure must not preempt LIST authorization");
            fs.get_object(&mut get)
                .await
                .expect("capture failure must not preempt GET authorization");
            fs.head_object(&mut head)
                .await
                .expect("capture failure must not preempt HEAD authorization");
            assert!(matches!(
                get.extensions.get::<super::OdmReadGenerationGuard>(),
                Some(super::OdmReadGenerationGuard::Failed { .. })
            ));
            assert!(matches!(
                head.extensions.get::<super::OdmReadGenerationGuard>(),
                Some(super::OdmReadGenerationGuard::Failed { .. })
            ));
            store
                .make_bucket(
                    &bucket,
                    &MakeBucketOptions {
                        versioning_enabled: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("create bucket after capture");
            store
                .put_object(
                    &bucket,
                    "local",
                    &mut crate::storage::PutObjReader::from_vec(b"local".to_vec()),
                    &crate::storage::ObjectOptions::default(),
                )
                .await
                .expect("create local hit");
            // Publishing a usable source later must not repair a failed capture.
            let incarnation = store.bucket_incarnation_id(&bucket).await.expect("created identity");
            sys.apply_for_incarnation(&bucket, incarnation, Some(&config)).await;
            let usecase = crate::app::object_usecase::DefaultObjectUsecase::with_context(Some(Arc::clone(&context)));
            crate::app::test_seams::get_object(&usecase, get.clone())
                .await
                .expect("a local GET hit ignores source capture failure");
            crate::app::test_seams::head_object(&usecase, head.clone())
                .await
                .expect("a local HEAD hit ignores source capture failure");
            get.input.key = "missing".into();
            head.input.key = "missing".into();
            assert_eq!(
                crate::app::test_seams::get_object(&usecase, get.clone())
                    .await
                    .expect_err("failed capture cannot rebind on GET miss")
                    .code(),
                &S3ErrorCode::NoSuchBucket
            );
            assert_eq!(
                crate::app::test_seams::head_object(&usecase, head.clone())
                    .await
                    .expect_err("failed capture cannot rebind on HEAD miss")
                    .code(),
                &S3ErrorCode::NoSuchBucket
            );
            assert_eq!(
                crate::app::test_seams::list_objects_v2(
                    &crate::app::bucket_usecase::DefaultBucketUsecase::with_context(Some(Arc::clone(&context))),
                    list.clone()
                )
                .await
                .expect_err("failed capture cannot rebind on LIST")
                .code(),
                &S3ErrorCode::NoSuchBucket
            );
            config.filter.prefix = Some("remote/".into());
            sys.apply_for_incarnation(&bucket, incarnation, Some(&config)).await;
            assert_eq!(
                crate::app::test_seams::get_object(&usecase, get.clone())
                    .await
                    .expect_err("filtered GET remains local")
                    .code(),
                &S3ErrorCode::NoSuchKey
            );
            assert_eq!(
                crate::app::test_seams::head_object(&usecase, head.clone())
                    .await
                    .expect_err("filtered HEAD remains local")
                    .code(),
                &S3ErrorCode::NoSuchKey
            );
            list.input.prefix = Some("local/".into());
            let listing = crate::app::test_seams::list_objects_v2(
                &crate::app::bucket_usecase::DefaultBucketUsecase::with_context(Some(Arc::clone(&context))),
                list.clone(),
            )
            .await
            .expect("disjoint prefix needs no source identity");
            assert_eq!(listing.output.key_count, Some(0));
            list.input.prefix = Some("remote/".into());
            list.input.max_keys = Some(0);
            let listing = crate::app::test_seams::list_objects_v2(
                &crate::app::bucket_usecase::DefaultBucketUsecase::with_context(Some(Arc::clone(&context))),
                list,
            )
            .await
            .expect("an empty page needs no source identity");
            assert_eq!(listing.output.key_count, Some(0));

            config.filter.prefix = None;
            config.policy.head = crate::on_demand_migration::HeadPolicy::LocalOnly;
            sys.apply_for_incarnation(&bucket, incarnation, Some(&config)).await;
            assert_eq!(
                crate::app::test_seams::head_object(&usecase, head.clone())
                    .await
                    .expect_err("local-only HEAD needs no source identity")
                    .code(),
                &S3ErrorCode::NoSuchKey
            );
            config.policy.head = crate::on_demand_migration::HeadPolicy::Proxy;
            sys.apply_for_incarnation(&bucket, incarnation, Some(&config)).await;
            store
                .delete_object(
                    &bucket,
                    "missing",
                    crate::storage::ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("create local delete marker");
            assert_eq!(
                crate::app::test_seams::get_object(&usecase, get)
                    .await
                    .expect_err("GET respects the delete marker before capture failure")
                    .code(),
                &S3ErrorCode::NoSuchKey
            );
            assert_eq!(
                crate::app::test_seams::head_object(&usecase, head)
                    .await
                    .expect_err("HEAD respects the delete marker before capture failure")
                    .code(),
                &S3ErrorCode::NoSuchKey
            );
            sys.remove(&bucket);
            sys.set_module_enabled(enabled_before);
        });
    }

    #[tokio::test]
    #[serial]
    async fn put_read_quorum_access_keeps_authorization_precedence() {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let server_ctx = ServerContextSlot::new();
        assert!(server_ctx.install(Arc::new(AppContext::new(Arc::clone(&store), Arc::new(UnreadyIam), Arc::new(TestKms)))));
        let fs = FS::with_server_ctx(server_ctx);

        for effect in ["Allow", "Deny"] {
            let bucket = format!("put-quorum-access-{}", uuid::Uuid::new_v4());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("authorization precedence bucket must be created");
            let policy_json = format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"{effect}","Principal":{{"AWS":"*"}},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
            );
            let mut metadata = (*crate::storage::get_bucket_metadata(&bucket)
                .await
                .expect("test bucket metadata must be cached"))
            .clone();
            metadata.policy_config = Some(serde_json::from_str(&policy_json).expect("test policy must parse"));
            metadata.policy_config_json = policy_json.into_bytes();
            crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
                .await
                .expect("test policy must be installed");

            let input = PutObjectInput::builder()
                .bucket(bucket)
                .key("object".into())
                .build()
                .expect("access probe input must build");
            let mut req = build_request(input, Method::PUT);
            ensure_req_info(&mut req);
            req.extensions.insert(fs.server_ctx().clone());
            let err = super::PUT_GENERATION_READ_QUORUM_FAILURE
                .scope((), fs.put_object(&mut req))
                .await
                .expect_err("a generation failure must never admit a write");
            if effect == "Allow" {
                assert_eq!(err.code(), &S3ErrorCode::ServiceUnavailable);
                assert_eq!(err.status_code(), Some(http::StatusCode::SERVICE_UNAVAILABLE));
                assert!(matches!(
                    err.source().and_then(|source| source.downcast_ref::<StorageError>()),
                    Some(StorageError::ErasureReadQuorum)
                ));
            } else {
                assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
                assert_eq!(err.status_code(), Some(http::StatusCode::FORBIDDEN));
            }
            assert!(req.extensions.get::<BucketGenerationGuard>().is_none());
        }
    }

    #[tokio::test]
    #[serial]
    async fn tag_dependent_access_hooks_preserve_local_miss_for_replication_proxy() {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let server_ctx = ServerContextSlot::new();
        assert!(server_ctx.install(Arc::new(AppContext::new(Arc::clone(&store), Arc::new(UnreadyIam), Arc::new(TestKms)))));
        let fs = FS::with_server_ctx(server_ctx);
        let bucket = format!("tag-proxy-boundary-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket");
        let policy_json = format!(
            r#"{{"Version":"2012-10-17","Statement":[
            {{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:GetObject","s3:GetObjectTagging","s3:PutObjectTagging","s3:DeleteObjectTagging"],"Resource":["arn:aws:s3:::{bucket}/*"]}},
            {{"Effect":"Deny","Principal":{{"AWS":"*"}},"Action":["s3:GetObject","s3:GetObjectTagging","s3:PutObjectTagging","s3:DeleteObjectTagging"],"Resource":["arn:aws:s3:::{bucket}/*"],"Condition":{{"StringEquals":{{"s3:ExistingObjectTag/protected":"yes"}}}}}}
        ]}}"#
        );
        let policy_bytes = policy_json.into_bytes();
        store
            .update_bucket_metadata_config(
                &bucket,
                crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                policy_bytes,
            )
            .await
            .expect("publish policy");
        macro_rules! check_hook {
            ($input:expr, $method:expr, $hook:ident) => {{
                let mut req = build_request($input, $method);
                ensure_req_info(&mut req);
                req.extensions.insert(fs.server_ctx().clone());
                fs.$hook(&mut req)
                    .await
                    .expect("local miss does not match the tag deny");
                let evidence = req
                    .extensions
                    .get::<ObjectTagConditions>()
                    .expect("authorization must retain tag dependency");
                assert!(evidence.values.is_empty(), "missing local object has no tags");
                assert!(
                    !super::replication_proxy_allowed(&req.extensions),
                    "local absence cannot authorize a tagged replica"
                );
            }};
        }
        check_hook!(
            GetObjectInput::builder()
                .bucket(bucket.clone())
                .key("missing".to_string())
                .build()
                .expect("GET"),
            Method::GET,
            get_object
        );
        check_hook!(
            HeadObjectInput::builder()
                .bucket(bucket.clone())
                .key("missing".to_string())
                .build()
                .expect("HEAD"),
            Method::HEAD,
            head_object
        );
        check_hook!(
            GetObjectTaggingInput::builder()
                .bucket(bucket.clone())
                .key("missing".to_string())
                .build()
                .expect("get tags"),
            Method::GET,
            get_object_tagging
        );
        check_hook!(
            DeleteObjectTaggingInput::builder()
                .bucket(bucket.clone())
                .key("missing".to_string())
                .build()
                .expect("delete tags"),
            Method::DELETE,
            delete_object_tagging
        );
        check_hook!(
            PutObjectTaggingInput::builder()
                .bucket(bucket.clone())
                .key("missing".to_string())
                .tagging(s3s::dto::Tagging { tag_set: Vec::new() })
                .build()
                .expect("put tags"),
            Method::PUT,
            put_object_tagging
        );
    }

    #[tokio::test]
    #[serial]
    async fn conditional_writes_require_read_permission_only_for_specific_etags() {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let server_ctx = ServerContextSlot::new();
        assert!(server_ctx.install(Arc::new(AppContext::new(Arc::clone(&store), Arc::new(UnreadyIam), Arc::new(TestKms)))));
        let fs = FS::with_server_ctx(server_ctx);
        for readable in [false, true] {
            let bucket = format!("etag-oracle-{}", uuid::Uuid::new_v4());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create bucket");
            let read_effect = if readable { "Allow" } else { "Deny" };
            let policy_json = format!(
                r#"{{"Version":"2012-10-17","Statement":[
                {{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}},
                {{"Effect":"{read_effect}","Principal":{{"AWS":"*"}},"Action":["s3:GetObject","s3:GetObjectVersion"],"Resource":["arn:aws:s3:::{bucket}/*"]}}
            ]}}"#
            );
            let policy_bytes = policy_json.into_bytes();
            store
                .update_bucket_metadata_config(
                    &bucket,
                    crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                    policy_bytes,
                )
                .await
                .expect("publish policy");
            for condition in ["absent", "wildcard", "dto", "header"] {
                let specific = matches!(condition, "dto" | "header");
                let dto = match condition {
                    "dto" => Some(s3s::dto::ETagCondition::ETag(s3s::dto::ETag::Strong("guessed-etag".to_string()))),
                    "wildcard" => Some(s3s::dto::ETagCondition::Any),
                    _ => None,
                };
                let mut put_input = PutObjectInput::builder()
                    .bucket(bucket.clone())
                    .key("object".to_string())
                    .build()
                    .expect("put");
                put_input.if_match = dto.clone();
                let mut put = build_request(put_input, Method::PUT);
                ensure_req_info(&mut put);
                put.extensions.insert(fs.server_ctx().clone());
                let mut complete_input = CompleteMultipartUploadInput::builder()
                    .bucket(bucket.clone())
                    .key("object".to_string())
                    .upload_id("upload".to_string())
                    .build()
                    .expect("complete");
                complete_input.if_match = dto;
                let mut complete = build_request(complete_input, Method::POST);
                ensure_req_info(&mut complete);
                complete.extensions.insert(fs.server_ctx().clone());
                if condition == "header" {
                    for headers in [&mut put.headers, &mut complete.headers] {
                        headers.insert(http::header::IF_MATCH, HeaderValue::from_static("\"guessed-etag\""));
                    }
                }
                for result in [
                    fs.put_object(&mut put).await,
                    fs.complete_multipart_upload(&mut complete).await,
                ] {
                    if specific && !readable {
                        assert_eq!(
                            result
                                .expect_err("a write-only caller must not compare content fingerprints")
                                .code(),
                            &S3ErrorCode::AccessDenied
                        );
                    } else {
                        result.expect("readable or wildcard/ordinary writes remain authorized");
                    }
                }
            }
        }
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
    async fn include_deleted_listing_enforces_version_permissions() {
        use rustfs_iam::store::Store as _;
        use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
        struct ReadyIam(Arc<IamSys<ObjectStore>>);
        impl IamInterface for ReadyIam {
            fn handle(&self) -> Arc<IamSys<ObjectStore>> {
                self.0.clone()
            }
            fn is_ready(&self) -> bool {
                true
            }
        }
        let (_directory, _paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
        ObjectStore::new(store.clone())
            .save_iam_config(
                serde_json::json!({"version":1}),
                format!("{}/format.json", *rustfs_iam::store::object::IAM_CONFIG_PREFIX),
            )
            .await
            .expect("seed IAM format");
        let iam = rustfs_iam::build_iam_sys(store.clone()).await.expect("initialize IAM");
        iam.create_user(
            "list-reader",
            &AddOrUpdateUserReq {
                secret_key: "list-reader-secret".to_string(),
                policy: None,
                status: AccountStatus::Enabled,
            },
        )
        .await
        .expect("create reader");
        let user = iam.get_user("list-reader").await.expect("reader identity").credentials;
        iam.set_policy(
            "list-only",
            rustfs_policy::policy::Policy::parse_config(
                br#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::*"}]}"#,
            )
            .expect("list policy"),
        )
        .await
        .expect("store identity policy");
        iam.policy_db_set("list-reader", rustfs_iam::store::UserType::Reg, false, "list-only")
            .await
            .expect("attach list policy");
        let bucket = format!("deleted-list-auth-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket");
        let slot = ServerContextSlot::new();
        assert!(slot.install(Arc::new(AppContext::new(Arc::clone(&store), Arc::new(ReadyIam(iam)), Arc::new(TestKms)))));
        for (grant_versions, deny_versions) in [(false, false), (true, false), (true, true)] {
            let mut statements = vec![serde_json::json!({
                "Effect": "Allow", "Principal": {"AWS": "*"}, "Action": ["s3:ListBucket"], "Resource": format!("arn:aws:s3:::{bucket}")
            })];
            if grant_versions {
                statements.push(serde_json::json!({
                    "Effect": "Allow", "Principal": {"AWS": "*"}, "Action": ["s3:ListBucketVersions"], "Resource": format!("arn:aws:s3:::{bucket}")
                }));
            }
            if deny_versions {
                statements.push(serde_json::json!({
                    "Effect": "Deny", "Principal": {"AWS": "*"}, "Action": ["s3:ListBucketVersions"], "Resource": format!("arn:aws:s3:::{bucket}")
                }));
            }
            let policy = serde_json::json!({"Version": "2012-10-17", "Statement": statements});
            store
                .update_bucket_metadata_config(
                    &bucket,
                    crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                    serde_json::to_vec(&policy).unwrap(),
                )
                .await
                .expect("publish listing policy");
            for header in ["x-rustfs-include-deleted", "x-minio-include-deleted"] {
                for (value, authenticated) in [("true", true), ("false", true), ("true", false), ("false", false)] {
                    let mut req = build_request((), Method::GET);
                    req.extensions.insert(slot.clone());
                    req.extensions.insert(ReqInfo {
                        bucket: Some(bucket.clone()),
                        cred: authenticated.then(|| user.clone()),
                        ..Default::default()
                    });
                    req.headers.insert(header, HeaderValue::from_static(value));
                    let result = super::authorize_bucket_listing(&mut req, &bucket, S3Action::ListBucketAction).await;
                    if value == "true" && (deny_versions || (authenticated && !grant_versions)) {
                        assert_eq!(
                            result.expect_err("version visibility requires permission").code(),
                            &S3ErrorCode::AccessDenied
                        );
                    } else {
                        result.expect("authorized listings remain available");
                    }
                }
            }
        }
    }

    #[test]
    #[serial]
    fn delete_object_access_keeps_snapshot_across_recreation_before_admission() {
        crate::app::gating_test_env::run_large_stack_test(
            "delete-object-generation-guard",
            delete_object_access_keeps_snapshot_across_recreation_before_admission_inner,
        );
    }

    async fn delete_object_access_keeps_snapshot_across_recreation_before_admission_inner() {
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
        metadata.policy_config_json = policy_json.clone().into_bytes();
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

        let captured_incarnation_id = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("read the bucket generation before admission");
        let (loaded_tx, loaded_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
        install_object_generation_test_hook(bucket.clone(), loaded_tx, resume_rx);
        let access = tokio::spawn(async move {
            let result = fs.delete_object(&mut req).await;
            (result, req)
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), loaded_rx)
            .await
            .expect("DeleteObject should reach the generation snapshot hook")
            .expect("DeleteObject access should remain paused before admission");

        store
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("delete the authorized bucket generation");
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("recreate the same bucket name");
        let mut metadata = (*crate::storage::get_bucket_metadata(&bucket)
            .await
            .expect("recreated bucket metadata should be cached"))
        .clone();
        metadata.policy_config = Some(serde_json::from_str(&policy_json).expect("test policy should parse"));
        metadata.policy_config_json = policy_json.into_bytes();
        crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
            .await
            .expect("republish the same policy for admission");
        let mut reader = crate::storage::PutObjReader::from_vec(b"new generation".to_vec());
        store
            .put_object(&bucket, "object", &mut reader, &crate::storage::ObjectOptions::default())
            .await
            .expect("put the new-generation object");

        assert_ne!(
            captured_incarnation_id,
            store
                .bucket_incarnation_id(&bucket)
                .await
                .expect("read the recreated bucket generation")
        );
        resume_tx
            .send(())
            .expect("DeleteObject should still be paused before admission");
        let (result, req) = access.await.expect("DeleteObject access task should join");
        result.expect("the equivalent policy should authorize DeleteObject");
        let mut opts = crate::storage::ObjectOptions::default();
        apply_bucket_generation_guard(&req, &bucket, &mut opts).expect("request snapshot should apply to DeleteObject options");
        assert_eq!(opts.expected_bucket_incarnation_id, Some(captured_incarnation_id));

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

    #[test]
    fn copy_tags_authorize_source_and_effective_destination_tags() {
        crate::app::gating_test_env::run_large_stack_test("copy-tag-authorization", || async {
            let store = crate::app::gating_test_env::shared_gating_ecstore().await;
            let slot = ServerContextSlot::new();
            let context = Arc::new(AppContext::new(store.clone(), Arc::new(UnreadyIam), Arc::new(TestKms)));
            assert!(slot.install(context.clone()));
            let fs = FS::with_server_ctx(slot.clone());
            let bucket = format!("copy-tags-{}", uuid::Uuid::new_v4());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("bucket");
            let mut reader = crate::storage::PutObjReader::from_vec(b"confidential source".to_vec());
            let source = store
                .put_object(
                    &bucket,
                    "source",
                    &mut reader,
                    &crate::storage::ObjectOptions {
                        versioned: true,
                        user_defined: HashMap::from([(
                            rustfs_utils::http::AMZ_OBJECT_TAGGING.to_string(),
                            "visibility=private".to_string(),
                        )]),
                        ..Default::default()
                    },
                )
                .await
                .expect("tagged historical source");
            let historical = source.version_id.expect("versioned source").to_string();
            let mut reader = crate::storage::PutObjReader::from_vec(b"current source".to_vec());
            store
                .put_object(
                    &bucket,
                    "source",
                    &mut reader,
                    &crate::storage::ObjectOptions {
                        versioned: true,
                        user_defined: HashMap::from([(
                            rustfs_utils::http::AMZ_OBJECT_TAGGING.to_string(),
                            "visibility=private".to_string(),
                        )]),
                        ..Default::default()
                    },
                )
                .await
                .expect("current tagged source");
            for (key, tags) in [("source-null", "visibility=private"), ("source-empty", "")] {
                let mut reader = crate::storage::PutObjReader::from_vec(b"unversioned source".to_vec());
                store
                    .put_object(
                        &bucket,
                        key,
                        &mut reader,
                        &crate::storage::ObjectOptions {
                            user_defined: HashMap::from([(rustfs_utils::http::AMZ_OBJECT_TAGGING.to_string(), tags.to_string())]),
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("unversioned source");
            }
            for (index, (directive, replacement, explicit_version, tag_permission, deny_public, expected)) in [
                (None, None, None, "missing", false, false),
                (Some("COPY"), None, None, "deny", false, false),
                (None, None, None, "current", false, true),
                (Some("COPY"), None, Some("historical"), "current", false, false),
                (Some("COPY"), None, Some("historical"), "version", false, true),
                (Some("COPY"), None, Some("null"), "current", false, false),
                (Some("COPY"), None, Some("null"), "version", false, true),
                (None, None, None, "empty", false, true),
                (Some("REPLACE"), Some("visibility=private"), None, "deny", false, true),
                (Some("REPLACE"), Some(""), None, "deny", false, true),
                (Some("REPLACE"), Some("visibility=public"), None, "deny", true, false),
            ]
            .into_iter()
            .enumerate()
            {
                let mut statements = vec![
                    serde_json::json!({"Effect":"Allow","Principal":{"AWS":"*"},"Action":["s3:GetObject","s3:GetObjectVersion"],"Resource":format!("arn:aws:s3:::{bucket}/source*")}),
                    serde_json::json!({"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:PutObject","Resource":format!("arn:aws:s3:::{bucket}/copied-*")}),
                ];
                if !matches!(tag_permission, "missing" | "empty") {
                    let action = if tag_permission == "version" {
                        "s3:GetObjectVersionTagging"
                    } else {
                        "s3:GetObjectTagging"
                    };
                    statements.push(serde_json::json!({"Effect":if tag_permission == "deny" { "Deny" } else { "Allow" },"Principal":{"AWS":"*"},"Action":action,"Resource":format!("arn:aws:s3:::{bucket}/source*")}));
                }
                if deny_public {
                    statements.push(serde_json::json!({"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:PutObject","Resource":format!("arn:aws:s3:::{bucket}/*"),"Condition":{"StringEquals":{"s3:RequestObjectTag/visibility":"public"}}}));
                }
                let policy =
                    serde_json::to_vec(&serde_json::json!({"Version":"2012-10-17","Statement":statements})).expect("policy");
                store
                    .update_bucket_metadata_config(
                        &bucket,
                        crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                        policy,
                    )
                    .await
                    .expect("policy publication");
                let destination = format!("copied-{index}");
                let input = CopyObjectInput::builder()
                    .bucket(bucket.clone())
                    .key(destination.clone())
                    .copy_source(CopySource::Bucket {
                        bucket: bucket.clone().into(),
                        key: (if tag_permission == "empty" {
                            "source-empty"
                        } else if explicit_version == Some("null") {
                            "source-null"
                        } else {
                            "source"
                        })
                        .into(),
                        version_id: explicit_version.map(|version| {
                            if version == "historical" {
                                historical.clone().into()
                            } else {
                                "null".into()
                            }
                        }),
                    })
                    .tagging_directive(directive.map(|value| TaggingDirective::from(value.to_string())))
                    .tagging(replacement.map(str::to_string))
                    .build()
                    .expect("copy input");
                let mut request = build_request(input, Method::PUT);
                ensure_req_info(&mut request);
                request.extensions.insert(slot.clone());
                let access = fs.copy_object(&mut request).await;
                if !expected {
                    assert_eq!(
                        access.expect_err("copy must enforce tagging and destination policy").code(),
                        &S3ErrorCode::AccessDenied,
                        "case {index}"
                    );
                    assert!(
                        store
                            .get_object_info(&bucket, &destination, &crate::storage::ObjectOptions::default())
                            .await
                            .is_err()
                    );
                    continue;
                }
                access.expect("authorized copy");
                crate::app::object_usecase::DefaultObjectUsecase::with_context(Some(context.clone()))
                    .execute_copy_object(request)
                    .await
                    .expect("copy preserves allowed tags");
                assert_eq!(
                    store
                        .get_object_tags(&bucket, &destination, &crate::storage::ObjectOptions::default())
                        .await
                        .expect("destination tags"),
                    replacement.unwrap_or(if tag_permission == "empty" { "" } else { "visibility=private" })
                );
            }
        });
    }

    #[test]
    fn copy_tags_rechecks_actual_source_after_access_snapshot() {
        crate::app::gating_test_env::run_large_stack_test("copy-tag-snapshot", || async {
            let store = crate::app::gating_test_env::shared_gating_ecstore().await;
            let slot = ServerContextSlot::new();
            let context = Arc::new(AppContext::new(store.clone(), Arc::new(UnreadyIam), Arc::new(TestKms)));
            assert!(slot.install(context.clone()));
            let fs = FS::with_server_ctx(slot.clone());
            for mode in ["tag-change", "new-tags", "allowed-tags", "source-tag-change", "self-copy"] {
                let bucket = format!("copy-snapshot-{}", uuid::Uuid::new_v4());
                store
                    .make_bucket(&bucket, &MakeBucketOptions::default())
                    .await
                    .expect("bucket");
                let destination = if mode == "self-copy" { "source" } else { "destination" };
                let initial = if mode == "new-tags" { "" } else { "visibility=private" };
                let mut reader = crate::storage::PutObjReader::from_vec(b"source".to_vec());
                store
                    .put_object(
                        &bucket,
                        "source",
                        &mut reader,
                        &crate::storage::ObjectOptions {
                            user_defined: HashMap::from([(
                                rustfs_utils::http::AMZ_OBJECT_TAGGING.to_string(),
                                initial.to_string(),
                            )]),
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("source");
                let mut statements = vec![
                    serde_json::json!({"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{bucket}/source")}),
                    serde_json::json!({"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:PutObject","Resource":format!("arn:aws:s3:::{bucket}/{destination}")}),
                    serde_json::json!({"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:PutObject","Resource":format!("arn:aws:s3:::{bucket}/*"),"Condition":{"StringEquals":{"s3:RequestObjectTag/visibility":"public"}}}),
                ];
                if mode != "new-tags" {
                    statements.push(serde_json::json!({"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObjectTagging","Resource":format!("arn:aws:s3:::{bucket}/source")}));
                    // A conditional Allow must work at the preliminary hook as well.
                    statements[1]["Condition"] = serde_json::json!({"StringEquals":{"s3:RequestObjectTag/visibility":"private"}});
                    if mode == "source-tag-change" {
                        statements[1].as_object_mut().expect("statement").remove("Condition");
                        statements.remove(2);
                        statements.last_mut().expect("tag read statement")["Condition"] =
                            serde_json::json!({"StringEquals":{"s3:ExistingObjectTag/visibility":"private"}});
                    } else if mode == "self-copy" {
                        statements[1]["Condition"]["StringEquals"]["s3:ExistingObjectTag/visibility"] =
                            serde_json::json!("private");
                        statements.last_mut().expect("tag read statement")["Condition"] =
                            serde_json::json!({"StringEquals":{"s3:ExistingObjectTag/visibility":"private"}});
                    }
                }
                store
                    .update_bucket_metadata_config(
                        &bucket,
                        crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                        serde_json::to_vec(&serde_json::json!({"Version":"2012-10-17","Statement":statements})).expect("policy"),
                    )
                    .await
                    .expect("publish policy");
                let input = CopyObjectInput::builder()
                    .bucket(bucket.clone())
                    .key(destination.to_string())
                    .copy_source(CopySource::Bucket {
                        bucket: bucket.clone().into(),
                        key: "source".into(),
                        version_id: None,
                    })
                    .metadata_directive((mode == "self-copy").then(|| MetadataDirective::from_static(MetadataDirective::REPLACE)))
                    .build()
                    .expect("copy");
                let mut request = build_request(input, Method::PUT);
                ensure_req_info(&mut request);
                request.extensions.insert(slot.clone());
                fs.copy_object(&mut request).await.expect("preliminary snapshot permits copy");
                if !matches!(mode, "allowed-tags" | "self-copy") {
                    store
                        .put_object_tags(&bucket, "source", "visibility=public", &crate::storage::ObjectOptions::default())
                        .await
                        .expect("change source after access hook");
                }
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    crate::app::object_usecase::DefaultObjectUsecase::with_context(Some(context.clone()))
                        .execute_copy_object(request),
                )
                .await
                .expect("copy authorization must not reacquire its own object lock");
                if matches!(mode, "allowed-tags" | "self-copy") {
                    result.expect("unchanged allowed tags complete");
                    assert_eq!(
                        store
                            .get_object_tags(&bucket, destination, &crate::storage::ObjectOptions::default())
                            .await
                            .expect("copied tags"),
                        "visibility=private"
                    );
                } else {
                    assert_eq!(
                        result.expect_err("actual source tags require fresh authorization").code(),
                        &S3ErrorCode::AccessDenied
                    );
                    assert!(
                        store
                            .get_object_info(&bucket, "destination", &crate::storage::ObjectOptions::default())
                            .await
                            .is_err(),
                        "denial must precede destination publication"
                    );
                }
            }
        });
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

    #[test]
    #[serial]
    fn restore_object_access_keeps_snapshot_across_recreation_before_admission() {
        crate::app::gating_test_env::run_large_stack_test(
            "restore-object-generation-guard",
            restore_object_access_keeps_snapshot_across_recreation_before_admission_inner,
        );
    }

    async fn restore_object_access_keeps_snapshot_across_recreation_before_admission_inner() {
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
        metadata.policy_config_json = policy_json.clone().into_bytes();
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
        install_object_generation_test_hook(bucket.clone(), authorized_tx, resume_rx);

        let access = tokio::spawn(async move {
            let result = fs.restore_object(&mut req).await;
            (result, req)
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), authorized_rx)
            .await
            .expect("RestoreObject should reach the generation snapshot hook")
            .expect("RestoreObject access must not fail before reaching the test hook");

        store
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("delete the authorized bucket generation");
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("recreate the bucket with the same name");
        let mut metadata = (*crate::storage::get_bucket_metadata(&bucket)
            .await
            .expect("recreated bucket metadata should be cached"))
        .clone();
        metadata.policy_config = Some(serde_json::from_str(&policy_json).expect("test policy should parse"));
        metadata.policy_config_json = policy_json.into_bytes();
        crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
            .await
            .expect("republish the same policy for admission");
        let recreated_incarnation_id = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("read the recreated bucket incarnation");
        assert_ne!(authorized_incarnation_id, recreated_incarnation_id);
        resume_tx
            .send(())
            .expect("RestoreObject access should still be waiting at the test hook");

        let (result, req) = access.await.expect("RestoreObject access task should join");
        result.expect("the equivalent policy should authorize RestoreObject without refreshing its snapshot");
        let mut opts = crate::storage::ObjectOptions::default();
        apply_bucket_generation_guard(&req, &bucket, &mut opts).expect("apply the RestoreObject authorization guard");
        assert_eq!(opts.expected_bucket_incarnation_id, Some(authorized_incarnation_id));

        // The RestoreObject usecase future is large enough that heap-pinning a
        // single future was not enough on Linux: the whole scenario runs on a
        // dedicated large stack (see the `#[test]` wrapper above) so unrelated
        // growth in bucket metadata futures cannot tip the test over.
        let err = Box::pin(
            crate::app::object_usecase::DefaultObjectUsecase::with_context(Some(app_context)).execute_restore_object(req),
        )
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

#[cfg(test)]
mod signed_query_lock_permission_regression {
    use super::FS;
    use crate::runtime_sources::{AppContext, ServerContextSlot};
    use crate::storage::storage_api::contract::bucket::{BucketOperations, MakeBucketOptions};
    use crate::storage::storage_api::contract::object::ObjectOperations;
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::{Digest, Sha256};
    use std::sync::Arc;

    fn presigned_lock_put(bucket: &str, key: &str, hoisted: bool) -> http::Request<s3s::Body> {
        let now = time::OffsetDateTime::now_utc();
        let stamp = now
            .format(
                &time::format_description::parse_borrowed::<2>("[year][month][day]T[hour][minute][second]Z")
                    .expect("timestamp format"),
            )
            .expect("timestamp");
        let day = &stamp[..8];
        let scope = format!("{day}/us-east-1/s3/aws4_request");
        let signed_headers = if hoisted {
            "host"
        } else {
            "host;x-amz-object-lock-legal-hold"
        };
        let mut params = std::collections::BTreeMap::from([
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256".to_owned()),
            ("X-Amz-Credential", format!("review-local-access/{scope}")),
            ("X-Amz-Date", stamp.clone()),
            ("X-Amz-Expires", "300".to_owned()),
            ("X-Amz-SignedHeaders", signed_headers.to_owned()),
        ]);
        if hoisted {
            params.insert("x-amz-object-lock-legal-hold", "ON".to_owned());
            params.insert("x-amz-meta-user-id", "u1".to_owned());
        }
        let query = params
            .into_iter()
            .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(&v)))
            .collect::<Vec<_>>()
            .join("&");
        let path = format!("/{bucket}/{key}");
        let lock_header = if hoisted { "" } else { "x-amz-object-lock-legal-hold:ON\n" };
        let canonical = format!("PUT\n{path}\n{query}\nhost:localhost\n{lock_header}\n{signed_headers}\nUNSIGNED-PAYLOAD");
        let to_sign = format!(
            "AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{}",
            hex_simd::encode_to_string(Sha256::digest(canonical), hex_simd::AsciiCase::Lower)
        );
        let mut signing_key = b"AWS4review-local-secret".to_vec();
        for value in [day, "us-east-1", "s3", "aws4_request"] {
            let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(&signing_key).expect("signing key");
            mac.update(value.as_bytes());
            signing_key = mac.finalize().into_bytes().to_vec();
        }
        let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(&signing_key).expect("signing key");
        mac.update(to_sign.as_bytes());
        let signature = hex_simd::encode_to_string(mac.finalize().into_bytes(), hex_simd::AsciiCase::Lower);
        let mut request = http::Request::builder()
            .method("PUT")
            .uri(format!("http://localhost{path}?{query}&X-Amz-Signature={signature}"))
            .header("host", "localhost")
            .header("content-length", "4")
            .header("content-md5", "CY9rzUYh03PK3k6DJie09g==");
        if !hoisted {
            request = request.header("x-amz-object-lock-legal-hold", "ON");
        }
        request
            .body(s3s::Body::from(bytes::Bytes::from_static(b"test")))
            .expect("signed request")
    }

    #[tokio::test]
    async fn signed_query_metadata_cannot_add_legal_hold_without_permission() {
        use http_body_util::BodyExt;
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let iam = rustfs_iam::init_iam_sys(store.clone()).await.expect("test IAM");
        let context = Arc::new(AppContext::with_default_interfaces(
            store.clone(),
            iam,
            Arc::new(rustfs_kms::KmsServiceManager::new()),
        ));
        assert!(context.publish_action_credentials(rustfs_credentials::Credentials {
            access_key: "review-local-access".to_owned(),
            secret_key: "review-local-secret".to_owned(),
            status: "on".to_owned(),
            ..Default::default()
        }));
        let slot = ServerContextSlot::new();
        assert!(slot.install(context));
        let fs = FS::with_server_ctx(slot);
        let bucket = "review-lock-permission";
        store
            .make_bucket(
                bucket,
                &MakeBucketOptions {
                    lock_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("locked bucket");
        let policy = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}},{{"Effect":"Deny","Principal":{{"AWS":"*"}},"Action":["s3:PutObjectLegalHold","s3:PutObjectRetention"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
        );
        let mut metadata = (*crate::storage::get_bucket_metadata(bucket).await.expect("bucket metadata")).clone();
        metadata.policy_config = Some(serde_json::from_str(&policy).expect("bucket policy"));
        metadata.policy_config_json = policy.into_bytes();
        crate::storage::storage_api::set_bucket_metadata(bucket.to_owned(), metadata)
            .await
            .expect("set bucket policy");
        let mut builder = s3s::service::S3ServiceBuilder::new(fs.clone());
        builder.set_auth(crate::auth::IAMAuth::with_server_context(
            "review-local-access",
            "review-local-secret",
            fs.server_ctx().clone(),
        ));
        builder.set_access(fs);
        let mut config = s3s::config::S3Config::default();
        config.normalize_forward_slash_path = true;
        config.enable_sig_v2 = true;
        config.put_object_max_size = Some(rustfs_config::MAX_SINGLE_PUT_OBJECT_SIZE);
        config.sig_v4_allowed_services.push("s3tables".to_owned());
        builder.set_config(Arc::new(s3s::config::StaticConfigProvider::new(Arc::new(config))));
        let service = builder.build();
        let response = service
            .call(presigned_lock_put(bucket, "header-control", false))
            .await
            .expect("header request");
        let status = response.status();
        let body = response.into_body().collect().await.expect("header body").to_bytes();
        assert_eq!(status, http::StatusCode::FORBIDDEN, "header control: {}", String::from_utf8_lossy(&body));
        assert!(
            String::from_utf8_lossy(&body).contains("<Code>AccessDenied</Code>"),
            "signed header must reach the independent permission check: {}",
            String::from_utf8_lossy(&body)
        );
        let response = service
            .call(presigned_lock_put(bucket, "query-target", true))
            .await
            .expect("query request");
        let status = response.status();
        let body = response.into_body().collect().await.expect("query body").to_bytes();
        let stored = store
            .get_object_info(bucket, "query-target", &crate::storage::ObjectOptions::default())
            .await;
        assert_eq!(
            status,
            http::StatusCode::OK,
            "ordinary metadata PUT should succeed: {}",
            String::from_utf8_lossy(&body)
        );
        let stored = stored.expect("ordinary metadata PUT must publish an object");
        assert!(
            !stored.user_defined.contains_key("x-amz-object-lock-legal-hold"),
            "query metadata must not grant a separate Object Lock capability"
        );
        assert_eq!(stored.user_defined.get("user-id").map(String::as_str), Some("u1"));
    }
}
