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

//! Authorization of object requests that land in a table bucket's warehouse.
//!
//! Responsible for: mapping an object action to the table-resource admin action it needs, the
//! table publication read locks an authorized content mutation retains until its write
//! publishes ([`TableDataPlanePublicationGuards`]), resolving the table resource an object key
//! belongs to, and the sealed continuation cursor of a table-filtered listing
//! ([`TableDataPlaneListAccess`]).
//! Not responsible for: the bucket and IAM decision that runs first (`decision`), or building
//! a listing's table filter (`storage::access_owned::listing_multipart`).
//! Upstream: `decision::authorize_request_core` and the listing shapes. Downstream: the table
//! catalog store and its publication locks.

use super::{AuthorizationRequest, object_store_not_initialized_error, request_iam_store, request_object_store};
use crate::error::ApiError;
use crate::storage::access::{audit_request_denial, iam_authorization_error};
use crate::storage::denial_audit::DenialAuditor;
use crate::storage::storage_api::ecstore_bucket::lockless_cache::is_lockless_cache_bucket;
use crate::storage::storage_api::runtime_sources_consumer::runtime_sources;
use crate::storage::{ECStore, ObjectOptions, is_err_bucket_not_found};
use chacha20poly1305::{
    ChaCha20Poly1305, KeyInit,
    aead::{Aead, Payload},
};
use rand::Rng;
use rustfs_policy::policy::Args;
use rustfs_policy::policy::action::{Action, AdminAction, S3Action};
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

#[derive(Clone, Default)]
pub(crate) struct TableDataPlanePublicationGuards {
    pub(crate) state: Arc<parking_lot::Mutex<TableDataPlanePublicationState>>,
}

#[derive(Default)]
pub(crate) struct TableDataPlanePublicationState {
    pub(crate) keys: BTreeSet<(String, String)>,
    pub(crate) guards: Vec<Box<dyn Send>>,
    pub(crate) lock_lost_signals: Vec<Arc<rustfs_lock::distributed_lock::LockLostSignal>>,
    pub(crate) resources: HashMap<(String, String), crate::table_catalog::TableDataPlaneResource>,
    pub(crate) missing_resources: BTreeSet<(String, String)>,
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
pub(crate) const TABLE_DATA_PLANE_LIST_CURSOR_MAX_ENCODED_LEN: usize = 16 * 1024;

pub(crate) fn table_data_plane_list_internal_error(message: impl Into<String>) -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, message.into())
}

#[derive(Clone)]
pub(crate) struct TableDataPlaneListResource {
    pub(crate) resource: crate::table_catalog::TableDataPlaneResource,
    pub(crate) allowed: bool,
}

#[derive(Clone)]
pub(crate) struct TableDataPlaneListAccess {
    pub(crate) bucket: String,
    pub(crate) principal_fingerprint: String,
    pub(crate) snapshot_fingerprint: String,
    pub(crate) cursor_key: [u8; 32],
    pub(crate) resources: Vec<TableDataPlaneListResource>,
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

pub(crate) fn table_data_plane_admin_action(action: Action) -> Option<AdminAction> {
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

pub(crate) fn table_data_plane_content_mutation(action: Action) -> bool {
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
    !req.permission_probe() && !req.staged_multipart_part() && table_data_plane_content_mutation(action)
}

pub(crate) fn table_catalog_backend_for_data_plane<R: AuthorizationRequest + ?Sized>(
    req: &R,
) -> S3Result<crate::table_catalog::EcStoreTableCatalogObjectBackend<ECStore>> {
    let context = match req.server_context() {
        Some(server_ctx) => server_ctx.installed_app_context(),
        None => runtime_sources::current_app_context(),
    }
    .ok_or_else(object_store_not_initialized_error)?;
    Ok(crate::table_catalog::EcStoreTableCatalogObjectBackend::new(context.object_store()))
}

pub(crate) fn table_catalog_store_for_data_plane<R: AuthorizationRequest + ?Sized>(
    req: &R,
) -> S3Result<crate::table_catalog::EcStoreTableCatalogStore<ECStore>> {
    let backend = table_catalog_backend_for_data_plane(req)?;
    crate::table_catalog::ConfiguredTableCatalogStore::from_env(backend)
        .map_err(|err| s3_error!(InternalError, "failed to configure table catalog backing: {}", err))
}

pub(crate) fn table_publication_guard_error(err: crate::table_catalog::TableCatalogStoreError) -> S3Error {
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

pub(crate) async fn retain_table_bucket_publication_guard<R: AuthorizationRequest + ?Sized>(
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

pub(crate) async fn table_bucket_enabled_for_data_plane<R: AuthorizationRequest + ?Sized>(
    req: &R,
    bucket: &str,
) -> S3Result<bool> {
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

pub(crate) fn table_list_catalog_error(err: crate::table_catalog::TableCatalogStoreError) -> S3Error {
    if err.is_unavailable() {
        S3Error::from(ApiError::from(err))
    } else {
        S3Error::from(ApiError::access_denied())
    }
}

pub(crate) async fn table_data_plane_resource_for_request<R: AuthorizationRequest + ?Sized>(
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
    } else if publication_held || req.permission_probe() {
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

pub(crate) fn require_owned_reserved_table_object<T>(object: &str, resource: Option<T>) -> S3Result<Option<T>> {
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

/// Probe analysis retains trusted known-fact markers; real IAM and OPA evaluation use clean facts.
pub(crate) struct TablePermissionConditions<'a> {
    pub(crate) evaluation: &'a HashMap<String, Vec<String>>,
    pub(crate) probe: Option<&'a HashMap<String, Vec<String>>>,
}

pub(crate) async fn authorize_table_data_plane_if_needed<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    action: Action,
    bucket: &str,
    object: &str,
    cred: &rustfs_credentials::Credentials,
    is_owner: bool,
    conditions: TablePermissionConditions<'_>,
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
    let args = Args {
        account: &cred.access_key,
        groups: &cred.groups,
        action: Action::AdminAction(admin_action),
        bucket: resource.table_bucket.as_str(),
        conditions: conditions.evaluation,
        is_owner,
        object: resource_object.as_str(),
        claims,
        deny_only: false,
    };
    let allowed = if req.permission_probe() {
        let probe_args = Args {
            conditions: conditions.probe.unwrap_or(conditions.evaluation),
            ..args.clone()
        };
        let prepared = iam_store.prepare_auth(&probe_args).await;
        if prepared.has_unknown_probe_conditions(&probe_args).await {
            return Err(rustfs_s3_types::s3_error!(NotImplemented, "permission query lacks table operation facts"));
        }
        iam_store
            .try_eval_prepared(&prepared, &args)
            .await
            .map_err(iam_authorization_error)?
    } else {
        iam_store.is_allowed(&args).await
    };
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

pub(crate) async fn deny_anonymous_table_data_plane_if_needed<R: AuthorizationRequest + ?Sized>(
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
