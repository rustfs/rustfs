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

use super::access_owned::listing_multipart::{
    authorize_bucket_listing, authorize_list_multipart_uploads, authorize_list_objects_v2, authorize_list_parts,
};
use super::access_owned::object_misc::{authorize_delete_object, authorize_delete_objects};
// The legacy-edge files still import the owned shapes through this path.
#[cfg(test)]
use super::ECStore;
#[cfg(test)]
use super::ObjectOptions;
#[cfg(test)]
use super::StorageError;
pub(crate) use super::access_owned;
use super::ecfs::FS;
#[cfg(test)]
use crate::auth::facts::AuthzTarget;
#[cfg(test)]
use crate::auth::facts::{AuthzFacts, AuthzPrincipal, build_args, build_conditions};
use crate::auth::{get_request_auth_type_with_query, get_session_token, reject_unsigned_amz_headers_on_sigv4_request};
use crate::shared_types::RemoteAddr;
use crate::storage::authorization::ObjectTagConditions;
#[cfg(test)]
use crate::storage::authorization::TableDataPlaneListCursorPosition;
#[cfg(test)]
use crate::storage::authorization::capture_odm_read_generation;
#[cfg(test)]
use crate::storage::authorization::{AMZ_WRITE_OFFSET_BYTES_HEADER, replication_request_authorized, request_context_from_req};
use crate::storage::authorization::{AccessMarkers, AuthorizationRequest, VerifiedSignature, admit_operation, resolve_req_info};
use crate::storage::authorization::{
    BucketConfigMutationSnapshot, BucketGenerationGuard, CopySourceBucketGenerationGuard, OdmReadGenerationGuard,
    PendingDeleteBucketGenerationGuard,
};
#[cfg(test)]
use crate::storage::authorization::{
    BucketPolicyExistingObjectTagHint, BucketPolicyRawLoadErrorKind, classify_bucket_policy_raw_load_error,
    merge_request_object_tag_conditions, remaining_retention_days, resource_free_condition_values,
};
#[cfg(test)]
use crate::storage::authorization::{DenialContext, iam_authorization_error, owner_can_bypass_policy_deny};
#[cfg(test)]
use crate::storage::authorization::{PUT_GENERATION_READ_QUORUM_FAILURE, install_object_generation_test_hook};
#[cfg(test)]
use crate::storage::authorization::{
    TABLE_DATA_PLANE_LIST_CURSOR_MAX_ENCODED_LEN, require_owned_reserved_table_object, table_catalog_backend_for_data_plane,
    table_data_plane_admin_action, table_data_plane_content_mutation, table_data_plane_resource_for_request,
    table_publication_guard_error,
};
use crate::storage::authorization::{TableDataPlaneListAccess, TableDataPlanePublicationGuards};
#[cfg(test)]
use crate::storage::authorization::{TableDataPlaneListResource, retain_table_bucket_publication_guard};
#[cfg(test)]
use crate::storage::authorization::{
    apply_bucket_generation_guard, apply_copy_source_bucket_generation_guard, bucket_config_mutation_incarnation,
    odm_read_generation, retained_table_data_plane_publication_guards,
};
#[cfg(test)]
use crate::storage::authorization::{audit_request_denial, authorize_request_with_location_constraint};
#[cfg(test)]
use crate::storage::authorization::{authorization_conditions, request_authz_facts};
use crate::storage::authorization::{
    authorize_bucket_config_mutation, authorize_bucket_request, authorize_bucket_tag_deletion, authorize_object_request,
};
#[cfg(test)]
use crate::storage::authorization::{
    bucket_policy_needs_existing_object_tag_from_hint, load_bucket_policy_existing_object_tag_hint,
    maybe_merge_object_tag_conditions,
};
#[cfg(test)]
use crate::storage::authorization::{
    has_write_offset_bytes_header, request_object_store, set_requested_object_lock_retain_until,
};
#[cfg(test)]
pub(crate) use crate::storage::authorization_actions::{
    complete_multipart_upload_authorize_action, delete_object_authorize_action, list_parts_authorize_action,
    post_object_authorize_action, versioned_read_action, versioned_tagging_action,
};
use crate::storage::denial_audit::DenialAuditor;
use crate::storage::helper::DenialScope;
pub(crate) use crate::storage::req_info::ReqInfo;
use crate::storage::request_context::RequestContext;
pub(crate) use crate::storage::request_markers::{
    MultipartStagingOperation, PostObjectPolicyHeaders, PostObjectRequestMarker, RequestedCopySource, RequestedObjectLegalHold,
    RequestedObjectLockRetainUntil, RequestedObjectLockRetention, RequestedObjectTags, StagedMultipartPartAuthorization,
};
use crate::storage::storage_api::runtime_sources_consumer::ServerContextSlot;
use http::HeaderMap;
#[cfg(test)]
use rustfs_policy::policy::BucketPolicyArgs;
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_trusted_proxies::ClientInfo;
use s3s::access::{S3Access, S3AccessContext};
#[cfg(test)]
use s3s::{S3Error, S3ErrorCode};
use s3s::{S3Request, S3Result, dto::*, s3_error};
#[cfg(test)]
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;
use url::Url;

#[cfg(test)]
#[path = "access_owned/listing_multipart_test.rs"]
mod owned_listing_multipart_test;
#[cfg(test)]
#[path = "access_owned/object_commit_test.rs"]
mod owned_object_commit_test;
#[cfg(test)]
#[path = "access_owned/object_misc_test.rs"]
mod owned_object_misc_test;
#[cfg(test)]
#[path = "access_owned/object_read_test.rs"]
mod owned_object_read_test;
#[cfg(test)]
#[path = "access_permission_auth_test.rs"]
mod permission_auth_test;
#[cfg(test)]
#[path = "access_readers_test.rs"]
mod readers_test;

#[derive(Clone, Debug)]
struct InternalObjectAuthorization;

impl From<&ObjectLockRetention> for RequestedObjectLockRetention {
    fn from(retention: &ObjectLockRetention) -> Self {
        Self {
            mode: retention.mode.as_ref().map(|mode| mode.as_str().to_owned()),
            retain_until: retention.retain_until_date.clone().map(OffsetDateTime::from),
        }
    }
}

impl From<&ObjectLockLegalHold> for RequestedObjectLegalHold {
    fn from(legal_hold: &ObjectLockLegalHold) -> Self {
        Self(legal_hold.status.as_ref().map(|status| status.as_str().to_owned()))
    }
}

impl From<&Tagging> for RequestedObjectTags {
    fn from(tagging: &Tagging) -> Self {
        Self(
            tagging
                .tag_set
                .iter()
                .filter_map(|tag| Some((tag.key.clone()?, tag.value.clone()?)))
                .collect(),
        )
    }
}

/// The legacy decoder's copy source as owned parts; `None` for an access-point or Outposts ARN.
pub(crate) fn legacy_copy_source(source: &CopySource) -> Option<(String, String, Option<String>)> {
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
    fn set_staged_multipart_part(&mut self, staged: bool) {
        if staged {
            self.extensions.insert(StagedMultipartPartAuthorization);
        } else {
            self.extensions.remove::<StagedMultipartPartAuthorization>();
        }
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
    fn take_requested_object_tags(&mut self) -> Option<RequestedObjectTags> {
        self.extensions.remove::<RequestedObjectTags>()
    }
    fn install_requested_retention(&mut self, retention: RequestedObjectLockRetention) {
        self.extensions.insert(retention);
    }
    fn install_requested_retain_until(&mut self, retain_until: Option<RequestedObjectLockRetainUntil>) {
        match retain_until {
            Some(retain_until) => {
                self.extensions.insert(retain_until);
            }
            None => {
                self.extensions.remove::<RequestedObjectLockRetainUntil>();
            }
        }
    }
    fn install_requested_legal_hold(&mut self, legal_hold: RequestedObjectLegalHold) {
        self.extensions.insert(legal_hold);
    }
    fn install_requested_object_tags(&mut self, tags: RequestedObjectTags) {
        self.extensions.insert(tags);
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
    fn install_post_object_policy_headers(&mut self, headers: PostObjectPolicyHeaders) {
        self.extensions.insert(headers);
    }
    fn install_post_object_marker(&mut self) {
        self.extensions.insert(PostObjectRequestMarker);
    }
    fn object_tag_conditions(&self) -> Option<&ObjectTagConditions> {
        self.extensions.get::<ObjectTagConditions>()
    }
    fn take_object_tag_conditions(&mut self) -> Option<ObjectTagConditions> {
        self.extensions.remove::<ObjectTagConditions>()
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
    fn install_table_list_access(&mut self, access: TableDataPlaneListAccess) {
        self.extensions.insert(access);
    }
    fn audit_denial(&self, auditor: &DenialAuditor, action: &Action, reason: &'static str) {
        auditor.audit(self, action, reason, DenialScope::Request);
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

#[cfg(test)]
fn set_requested_object_lock_retention(req: &mut S3Request<PutObjectRetentionInput>) {
    let retention = req
        .input
        .retention
        .as_ref()
        .map(RequestedObjectLockRetention::from)
        .unwrap_or_default();
    set_requested_object_lock_retain_until(req, retention.retain_until);
    req.install_requested_retention(retention);
}

fn set_post_object_policy_headers(req: &mut S3Request<PutObjectInput>) -> S3Result<()> {
    if req.method != http::Method::POST {
        return Ok(());
    }
    use access_owned::object_write::{PostObjectFormPolicy, PostObjectPolicyFields, invalid_post_form_field};
    let input = &req.input;
    let field = |value: &ETagCondition| value.to_http_header().map_err(|_| invalid_post_form_field());
    PostObjectFormPolicy::new(PostObjectPolicyFields {
        lock_mode: input.object_lock_mode.as_ref().map(ObjectLockMode::as_str),
        legal_hold: input
            .object_lock_legal_hold_status
            .as_ref()
            .map(ObjectLockLegalHoldStatus::as_str),
        retain_until: input.object_lock_retain_until_date.clone().map(OffsetDateTime::from),
        tagging: input.tagging.as_deref(),
        acl: input.acl.as_ref().map(ObjectCannedACL::as_str),
        storage_class: input.storage_class.as_ref().map(StorageClass::as_str),
        encryption: input.server_side_encryption.as_ref().map(ServerSideEncryption::as_str),
        kms_key_id: input.ssekms_key_id.as_deref(),
        customer_algorithm: input.sse_customer_algorithm.as_deref(),
        grant_full_control: input.grant_full_control.as_deref(),
        grant_read: input.grant_read.as_deref(),
        grant_read_acp: input.grant_read_acp.as_deref(),
        grant_write_acp: input.grant_write_acp.as_deref(),
        if_match: input.if_match.as_ref().map(field).transpose()?,
        if_none_match: input.if_none_match.as_ref().map(field).transpose()?,
    })?
    .install(req);
    Ok(())
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

        admit_operation(cx.headers(), cx.uri().query(), cx.s3_op().name(), signature).map_err(Into::into)
    }

    /// Checks whether the CreateBucket request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn create_bucket(&self, req: &mut S3Request<CreateBucketInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let location_constraint = requested_bucket_location_constraint(&req.input);
        let object_lock_enabled = req.input.object_lock_enabled_for_bucket.is_some_and(|v| v);
        access_owned::bucket_core::authorize_create_bucket(req, &bucket, location_constraint, object_lock_enabled)
            .await
            .map_err(Into::into)
    }
    /// Checks whether the AbortMultipartUpload request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn abort_multipart_upload(&self, req: &mut S3Request<AbortMultipartUploadInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        access_owned::listing_multipart::authorize_abort_multipart_upload(self.server_ctx(), req, &bucket, &key)
            .await
            .map_err(Into::into)
    }

    /// Checks whether the CompleteMultipartUpload request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn complete_multipart_upload(&self, req: &mut S3Request<CompleteMultipartUploadInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let specific_etag = req.input.if_match.as_ref().is_some_and(|etag| !etag.is_any());
        access_owned::object_commit::authorize_complete_multipart_upload(req, self.server_ctx(), &bucket, &key, specific_etag)
            .await
            .map_err(Into::into)
    }

    /// Checks whether the CopyObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn copy_object(&self, req: &mut S3Request<CopyObjectInput>) -> S3Result<()> {
        use access_owned::object_commit::{CopyObjectAuthorization, authorize_copy_object};

        let input = &req.input;
        let bucket = input.bucket.clone();
        let key = input.key.clone();
        let source = legacy_copy_source(&input.copy_source);
        let source = source
            .as_ref()
            .map(|(bucket, key, version_id)| copy_source_object(bucket, key, version_id.as_deref()));
        let query_version = input.version_id.clone();
        let tagging = input.tagging.clone();
        let tagging_directive = input.tagging_directive.as_ref().map(|value| value.as_str().to_owned());
        let facts = CopyObjectAuthorization {
            bucket: &bucket,
            key: &key,
            source,
            query_version: query_version.as_deref(),
            tagging: tagging.as_deref(),
            tagging_directive: tagging_directive.as_deref(),
            retain_until: input.object_lock_retain_until_date.clone().map(OffsetDateTime::from),
            legal_hold: legal_hold_write_requested(input.object_lock_legal_hold_status.as_ref()),
            retention: retention_write_requested(input.object_lock_mode.as_ref(), input.object_lock_retain_until_date.as_ref()),
        };
        authorize_copy_object(req, self.server_ctx(), facts).await.map_err(Into::into)
    }

    /// Checks whether the CreateMultipartUpload request has accesses to the resources.
    async fn create_multipart_upload(&self, req: &mut S3Request<CreateMultipartUploadInput>) -> S3Result<()> {
        use access_owned::listing_multipart::{CreateMultipartAuthorization, authorize_create_multipart_upload};

        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let query_version = req.input.version_id.clone();
        let retain_until = req.input.object_lock_retain_until_date.clone().map(OffsetDateTime::from);
        let facts = CreateMultipartAuthorization {
            bucket: &bucket,
            key: &key,
            query_version: query_version.as_deref(),
            retain_until,
            legal_hold_present: req.input.object_lock_legal_hold_status.is_some(),
            retention_present: req.input.object_lock_mode.is_some() || retain_until.is_some(),
        };
        authorize_create_multipart_upload(self.server_ctx(), req, facts).await?;
        Ok(())
    }

    /// Checks whether the DeleteBucket request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket(&self, req: &mut S3Request<DeleteBucketInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        access_owned::bucket_core::authorize_delete_bucket(req, &bucket)
            .await
            .map_err(Into::into)
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
            .map_err(Into::into)
    }

    /// Checks whether the DeleteBucketEncryption request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_encryption(&self, req: &mut S3Request<DeleteBucketEncryptionInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::PutBucketEncryptionAction))
            .await
            .map_err(Into::into)
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
            .map_err(Into::into)
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
            .map_err(Into::into)
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
        .map_err(Into::into)
    }

    /// Checks whether the DeleteBucketTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_tagging(&self, req: &mut S3Request<DeleteBucketTaggingInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_tag_deletion(self.server_ctx(), req, &bucket)
            .await
            .map_err(Into::into)
    }

    /// Checks whether the DeleteBucketWebsite request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_website(&self, req: &mut S3Request<DeleteBucketWebsiteInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, bucket_website_config_authorize_action())
            .await
            .map_err(Into::into)
    }

    /// Checks whether the DeleteObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_object(&self, req: &mut S3Request<DeleteObjectInput>) -> S3Result<()> {
        let (bucket, key, version_id) = (req.input.bucket.clone(), req.input.key.clone(), req.input.version_id.clone());
        authorize_delete_object(self.server_ctx(), req, &bucket, &key, version_id.as_deref())
            .await
            .map_err(Into::into)
    }

    /// Checks whether the DeleteObjectTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_object_tagging(&self, req: &mut S3Request<DeleteObjectTaggingInput>) -> S3Result<()> {
        let (bucket, key, version_id) = (req.input.bucket.clone(), req.input.key.clone(), req.input.version_id.clone());
        access_owned::object_meta::authorize_object_tagging_request(
            req,
            &bucket,
            &key,
            version_id.as_deref(),
            S3Action::DeleteObjectTaggingAction,
        )
        .await
        .map_err(Into::into)
    }

    /// Checks whether the DeleteObjects request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_objects(&self, req: &mut S3Request<DeleteObjectsInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_delete_objects(req, &bucket).map_err(Into::into)
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
        .map_err(Into::into)
    }

    /// Checks whether the GetBucketAccelerateConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_accelerate_configuration(
        &self,
        req: &mut S3Request<GetBucketAccelerateConfigurationInput>,
    ) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_request(req, &bucket, Action::S3Action(S3Action::GetBucketPolicyAction))
            .await
            .map_err(Into::into)
    }

    /// Checks whether the GetBucketAcl request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_acl(&self, req: &mut S3Request<GetBucketAclInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_request(req, &bucket, Action::S3Action(S3Action::GetBucketAclAction))
            .await
            .map_err(Into::into)
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
        let bucket = req.input.bucket.clone();
        authorize_bucket_request(req, &bucket, Action::S3Action(S3Action::GetBucketCorsAction))
            .await
            .map_err(Into::into)
    }

    /// Checks whether the GetBucketEncryption request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_encryption(&self, req: &mut S3Request<GetBucketEncryptionInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_request(req, &bucket, Action::S3Action(S3Action::GetBucketEncryptionAction))
            .await
            .map_err(Into::into)
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
        let bucket = req.input.bucket.clone();
        authorize_bucket_request(req, &bucket, Action::S3Action(S3Action::GetBucketLifecycleAction))
            .await
            .map_err(Into::into)
    }

    /// Checks whether the GetBucketLocation request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_location(&self, req: &mut S3Request<GetBucketLocationInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_request(req, &bucket, Action::S3Action(S3Action::GetBucketLocationAction))
            .await
            .map_err(Into::into)
    }

    /// Checks whether the GetBucketLogging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_logging(&self, req: &mut S3Request<GetBucketLoggingInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_request(req, &bucket, Action::S3Action(S3Action::GetBucketLoggingAction))
            .await
            .map_err(Into::into)
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
        let bucket = req.input.bucket.clone();
        authorize_bucket_request(req, &bucket, Action::S3Action(S3Action::GetBucketNotificationAction))
            .await
            .map_err(Into::into)
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
        let bucket = req.input.bucket.clone();
        authorize_bucket_request(req, &bucket, get_bucket_policy_authorize_action())
            .await
            .map_err(Into::into)
    }

    /// Checks whether the GetBucketPolicyStatus request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_policy_status(&self, req: &mut S3Request<GetBucketPolicyStatusInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_request(req, &bucket, Action::S3Action(S3Action::GetBucketPolicyStatusAction))
            .await
            .map_err(Into::into)
    }

    /// Checks whether the GetBucketReplication request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_replication(&self, req: &mut S3Request<GetBucketReplicationInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_request(req, &bucket, Action::S3Action(S3Action::GetReplicationConfigurationAction))
            .await
            .map_err(Into::into)
    }

    /// Checks whether the GetBucketRequestPayment request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_request_payment(&self, req: &mut S3Request<GetBucketRequestPaymentInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_request(req, &bucket, Action::S3Action(S3Action::GetBucketPolicyAction))
            .await
            .map_err(Into::into)
    }

    /// Checks whether the GetBucketTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_tagging(&self, req: &mut S3Request<GetBucketTaggingInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_request(req, &bucket, Action::S3Action(S3Action::GetBucketTaggingAction))
            .await
            .map_err(Into::into)
    }

    /// Checks whether the GetBucketVersioning request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_versioning(&self, req: &mut S3Request<GetBucketVersioningInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_request(req, &bucket, Action::S3Action(S3Action::GetBucketVersioningAction))
            .await
            .map_err(Into::into)
    }

    /// Checks whether the GetBucketWebsite request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_website(&self, req: &mut S3Request<GetBucketWebsiteInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_request(req, &bucket, Action::S3Action(S3Action::GetBucketPolicyAction))
            .await
            .map_err(Into::into)
    }

    /// Checks whether the GetObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object(&self, req: &mut S3Request<GetObjectInput>) -> S3Result<()> {
        let (bucket, key, version) = (req.input.bucket.clone(), req.input.key.clone(), req.input.version_id.clone());
        access_owned::object_read::authorize_get_object(self.server_ctx(), req, &bucket, &key, version.as_deref())
            .await
            .map_err(Into::into)
    }

    /// Checks whether the GetObjectAcl request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_acl(&self, req: &mut S3Request<GetObjectAclInput>) -> S3Result<()> {
        let (bucket, key, version_id) = (req.input.bucket.clone(), req.input.key.clone(), req.input.version_id.clone());
        authorize_object_request(req, &bucket, &key, version_id.as_deref(), Action::S3Action(S3Action::GetObjectAclAction))
            .await
            .map_err(Into::into)
    }

    /// Checks whether the GetObjectAttributes request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_attributes(&self, req: &mut S3Request<GetObjectAttributesInput>) -> S3Result<()> {
        let (bucket, key, version_id) = (req.input.bucket.clone(), req.input.key.clone(), req.input.version_id.clone());
        access_owned::object_read::authorize_get_object_attributes(req, &bucket, &key, version_id.as_deref())
            .await
            .map_err(Into::into)
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
        .map_err(Into::into)
    }

    /// Checks whether the GetObjectLockConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_lock_configuration(&self, req: &mut S3Request<GetObjectLockConfigurationInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_request(req, &bucket, Action::S3Action(S3Action::GetBucketObjectLockConfigurationAction))
            .await
            .map_err(Into::into)
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
        .map_err(Into::into)
    }

    /// Checks whether the GetObjectTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_tagging(&self, req: &mut S3Request<GetObjectTaggingInput>) -> S3Result<()> {
        let (bucket, key, version_id) = (req.input.bucket.clone(), req.input.key.clone(), req.input.version_id.clone());
        access_owned::object_meta::authorize_object_tagging_request(
            req,
            &bucket,
            &key,
            version_id.as_deref(),
            S3Action::GetObjectTaggingAction,
        )
        .await
        .map_err(Into::into)
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
        let bucket = req.input.bucket.clone();
        authorize_bucket_request(req, &bucket, Action::S3Action(S3Action::GetBucketPublicAccessBlockAction))
            .await
            .map_err(Into::into)
    }

    /// Checks whether the HeadBucket request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn head_bucket(&self, req: &mut S3Request<HeadBucketInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_request(req, &bucket, Action::S3Action(S3Action::ListBucketAction))
            .await
            .map_err(Into::into)
    }

    /// Checks whether the HeadObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn head_object(&self, req: &mut S3Request<HeadObjectInput>) -> S3Result<()> {
        let input = req.input.clone();
        access_owned::object_read::authorize_head_object(
            self.server_ctx(),
            req,
            &input.bucket,
            &input.key,
            input.version_id.as_deref(),
        )
        .await
        .map_err(Into::into)
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
        Ok(authorize_list_multipart_uploads(self.server_ctx(), req, &bucket).await?)
    }

    /// Checks whether the `ListObjectVersions` request is authorized for the requested bucket.
    ///
    /// Returns `Ok(())` if the request is allowed, or an error if access is denied or another
    /// authorization-related issue occurs.
    async fn list_object_versions(&self, req: &mut S3Request<ListObjectVersionsInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        Ok(authorize_bucket_listing(req, &bucket, S3Action::ListBucketVersionsAction).await?)
    }

    /// Checks whether the ListObjects request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_objects(&self, req: &mut S3Request<ListObjectsInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        Ok(authorize_bucket_listing(req, &bucket, S3Action::ListBucketAction).await?)
    }

    /// Checks whether the ListObjectsV2 request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_objects_v2(&self, req: &mut S3Request<ListObjectsV2Input>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        Ok(authorize_list_objects_v2(self.server_ctx(), req, &bucket).await?)
    }

    /// Checks whether the ListParts request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_parts(&self, req: &mut S3Request<ListPartsInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        Ok(authorize_list_parts(self.server_ctx(), req, &bucket, &key).await?)
    }

    /// Checks whether the PostObject request has accesses to the resources.
    async fn post_object(&self, req: &mut S3Request<PostObjectInput>) -> S3Result<()> {
        validate_post_object_success_controls(&req.input)?;

        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let version = req.input.version_id.clone();
        access_owned::object_write::authorize_post_object(req, &bucket, &key, version.as_deref())
            .await
            .map_err(Into::into)
    }

    /// Checks whether the PutBucketAccelerateConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_accelerate_configuration(
        &self,
        req: &mut S3Request<PutBucketAccelerateConfigurationInput>,
    ) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::PutBucketPolicyAction))
            .await
            .map_err(Into::into)
    }

    /// Checks whether the PutBucketAcl request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_acl(&self, req: &mut S3Request<PutBucketAclInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_request(req, &bucket, Action::S3Action(S3Action::PutBucketAclAction))
            .await
            .map_err(Into::into)
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
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::PutBucketCorsAction))
            .await
            .map_err(Into::into)
    }

    /// Checks whether the PutBucketEncryption request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_encryption(&self, req: &mut S3Request<PutBucketEncryptionInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::PutBucketEncryptionAction))
            .await
            .map_err(Into::into)
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
            .map_err(Into::into)
    }

    async fn put_bucket_logging(&self, req: &mut S3Request<PutBucketLoggingInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::PutBucketLoggingAction))
            .await
            .map_err(Into::into)
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
            .map_err(Into::into)
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
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, put_bucket_policy_authorize_action())
            .await
            .map_err(Into::into)
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
        .map_err(Into::into)
    }

    /// Checks whether the PutBucketRequestPayment request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_request_payment(&self, req: &mut S3Request<PutBucketRequestPaymentInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::PutBucketPolicyAction))
            .await
            .map_err(Into::into)
    }

    /// Checks whether the PutBucketTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_tagging(&self, req: &mut S3Request<PutBucketTaggingInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::PutBucketTaggingAction))
            .await
            .map_err(Into::into)
    }

    /// Checks whether the PutBucketVersioning request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_versioning(&self, req: &mut S3Request<PutBucketVersioningInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, Action::S3Action(S3Action::PutBucketVersioningAction))
            .await
            .map_err(Into::into)
    }

    /// Checks whether the PutBucketWebsite request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_website(&self, req: &mut S3Request<PutBucketWebsiteInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        authorize_bucket_config_mutation(self.server_ctx(), req, &bucket, bucket_website_config_authorize_action())
            .await
            .map_err(Into::into)
    }

    /// Checks whether the PutObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object(&self, req: &mut S3Request<PutObjectInput>) -> S3Result<()> {
        crate::hp_guard!("S3Access::put_object");
        use access_owned::object_write::{PutObjectAuthorization, authorize_put_object};
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let version_id = req.input.version_id.clone();
        let input = PutObjectAuthorization {
            bucket: &bucket,
            key: &key,
            version_id: version_id.as_deref(),
            retain_until: req.input.object_lock_retain_until_date.clone().map(OffsetDateTime::from),
            specific_etag: req.input.if_match.as_ref().is_some_and(|etag| !etag.is_any()),
            legal_hold: legal_hold_write_requested(req.input.object_lock_legal_hold_status.as_ref()),
            retention: retention_write_requested(
                req.input.object_lock_mode.as_ref(),
                req.input.object_lock_retain_until_date.as_ref(),
            ),
            post: req.method == http::Method::POST,
        };
        authorize_put_object(req, self.server_ctx(), input, |req| {
            set_post_object_policy_headers(req).map_err(Into::into)
        })
        .await
        .map_err(Into::into)
    }

    /// Checks whether the PutObjectAcl request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_acl(&self, req: &mut S3Request<PutObjectAclInput>) -> S3Result<()> {
        let (bucket, key, version_id) = (req.input.bucket.clone(), req.input.key.clone(), req.input.version_id.clone());
        authorize_object_request(req, &bucket, &key, version_id.as_deref(), Action::S3Action(S3Action::PutObjectAclAction))
            .await
            .map_err(Into::into)
    }

    /// Checks whether the PutObjectLegalHold request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_legal_hold(&self, req: &mut S3Request<PutObjectLegalHoldInput>) -> S3Result<()> {
        let input = &req.input;
        let (bucket, key, version_id) = (input.bucket.clone(), input.key.clone(), input.version_id.clone());
        let legal_hold = input
            .legal_hold
            .as_ref()
            .map(RequestedObjectLegalHold::from)
            .unwrap_or_default();
        access_owned::object_meta::authorize_put_object_legal_hold(
            req,
            self.server_ctx(),
            &bucket,
            &key,
            version_id.as_deref(),
            legal_hold,
        )
        .await
        .map_err(Into::into)
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
        .map_err(Into::into)
    }

    /// Checks whether the PutObjectRetention request has accesses to the resources.
    async fn put_object_retention(&self, req: &mut S3Request<PutObjectRetentionInput>) -> S3Result<()> {
        let input = &req.input;
        let (bucket, key, version_id) = (input.bucket.clone(), input.key.clone(), input.version_id.clone());
        let retention = input
            .retention
            .as_ref()
            .map(RequestedObjectLockRetention::from)
            .unwrap_or_default();
        access_owned::object_meta::authorize_put_object_retention(
            req,
            self.server_ctx(),
            &bucket,
            &key,
            version_id.as_deref(),
            retention,
        )
        .await
        .map_err(Into::into)
    }

    /// Checks whether the PutObjectTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_tagging(&self, req: &mut S3Request<PutObjectTaggingInput>) -> S3Result<()> {
        let input = &req.input;
        let tags = RequestedObjectTags::from(&input.tagging);
        let (bucket, key, version_id) = (input.bucket.clone(), input.key.clone(), input.version_id.clone());
        access_owned::object_meta::authorize_put_object_tagging(req, &bucket, &key, version_id.as_deref(), tags)
            .await
            .map_err(Into::into)
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
        .map_err(Into::into)
    }

    /// Checks whether the RestoreObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn restore_object(&self, req: &mut S3Request<RestoreObjectInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let version_id = req.input.version_id.clone();
        access_owned::object_misc::authorize_restore_object(self.server_ctx(), req, &bucket, &key, version_id.as_deref())
            .await
            .map_err(Into::into)
    }

    /// Checks whether the SelectObjectContent request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn select_object_content(&self, req: &mut S3Request<SelectObjectContentInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        access_owned::object_misc::authorize_select_object_content(req, &bucket, &key)
            .await
            .map_err(Into::into)
    }

    /// Checks whether the UploadPart request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn upload_part(&self, req: &mut S3Request<UploadPartInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        access_owned::object_write::authorize_upload_part(req, self.server_ctx(), &bucket, &key)
            .await
            .map_err(Into::into)
    }

    /// Checks whether the UploadPartCopy request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn upload_part_copy(&self, req: &mut S3Request<UploadPartCopyInput>) -> S3Result<()> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let source = legacy_copy_source(&req.input.copy_source);
        let source = source
            .as_ref()
            .map(|(bucket, key, version_id)| copy_source_object(bucket, key, version_id.as_deref()));
        access_owned::object_commit::authorize_upload_part_copy(req, self.server_ctx(), &bucket, &key, source)
            .await
            .map_err(Into::into)
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
    pub(crate) use crate::storage::authorization::envelope_payloads::bucket_config_mutation;
    /// Whether `guards` retains `key`, and how many lock guards it holds.
    pub(crate) fn publication_guard_holds(
        guards: &super::TableDataPlanePublicationGuards,
        key: &(String, String),
    ) -> (bool, usize) {
        let state = guards.state.lock();
        (state.keys.contains(key), state.guards.len())
    }

    pub(crate) fn request_authz_facts<R: AuthorizationRequest + ?Sized>(req: &R, target: AuthzTarget<'_>) -> AuthzFacts {
        super::request_authz_facts(req, target)
    }

    pub(crate) fn authorization_conditions<R: AuthorizationRequest + ?Sized>(
        req: &R,
        principal: &AuthzPrincipal<'_>,
        facts: &AuthzFacts,
    ) -> S3Result<HashMap<String, Vec<String>>> {
        super::authorization_conditions(req, principal, facts).map_err(Into::into)
    }

    pub(crate) fn requested_bucket_location_constraint(input: &CreateBucketInput) -> Option<String> {
        super::requested_bucket_location_constraint(input)
    }

    pub(crate) fn request_object_store<R: AuthorizationRequest + ?Sized>(req: &R) -> S3Result<Arc<ECStore>> {
        super::request_object_store(req).map_err(Into::into)
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

    pub(crate) async fn authorize_request_with_location_constraint<T>(
        req: &mut S3Request<T>,
        action: Action,
        location_constraint: Option<String>,
    ) -> S3Result<()> {
        super::authorize_request_with_location_constraint(req, action, location_constraint)
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::audit_request_denial;
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
    use super::{
        PostObjectPolicyHeaders, RequestedCopySource, RequestedObjectLegalHold, RequestedObjectTags, remaining_retention_days,
        requested_bucket_location_constraint, set_post_object_policy_headers, set_requested_object_lock_retain_until,
        set_requested_object_lock_retention,
    };
    use crate::auth::OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION;
    use crate::auth::facts::{AuthzPrincipal, AuthzTarget, list_query_pairs, merge_list_query_conditions};
    use crate::error::ApiError;
    use crate::storage::denial_audit::BulkDenialAudit;
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

    /// The listing authorization hands its table snapshot to the use case through the request it
    /// was given, on either edge.
    #[test]
    fn table_list_access_reaches_the_use_case_from_either_edge() {
        use super::AuthorizationRequest;
        let access = TableDataPlaneListAccess::test_listing_fixture("bucket");

        let mut req = build_request((), Method::GET);
        let mut envelope = crate::app::RequestEnvelope::from_s3s(&req);
        assert!(req.extensions.get::<TableDataPlaneListAccess>().is_none());
        assert!(envelope.table_list_access().is_none());

        AuthorizationRequest::install_table_list_access(&mut req, access.clone());
        AuthorizationRequest::install_table_list_access(&mut envelope, access);

        assert!(req.extensions.get::<TableDataPlaneListAccess>().is_some());
        assert!(envelope.table_list_access().is_some());
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
        .map_err(s3s::S3Error::from)
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
            let err = s3s::S3Error::from(table_publication_guard_error(error));
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
            let err = s3s::S3Error::from(table_publication_guard_error(error));
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
                    authorization_conditions(&req, &principal, &facts)
                        .map_err(s3s::S3Error::from)
                        .unwrap_err()
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
        req.extensions.insert(
            req.input
                .legal_hold
                .as_ref()
                .map(RequestedObjectLegalHold::from)
                .unwrap_or_default(),
        );
        let conditions = remaining_retention_days_conditions(&req, action);
        assert_eq!(conditions.get("object-lock-legal-hold"), Some(&vec!["OFF".to_string()]));
        // A body without a status stores no hold change; the header must not fill it in.
        req.extensions.insert(RequestedObjectLegalHold::default());
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
        req.extensions.insert(RequestedObjectTags::from(&req.input.tagging));
        let conditions = remaining_retention_days_conditions(&req, action);
        assert_eq!(conditions.get("RequestObjectTag/classification"), Some(&vec!["confidential".to_string()]));
        assert_eq!(conditions.get("RequestObjectTagKeys"), Some(&vec!["classification".to_string()]));
        req.extensions.insert(RequestedObjectTags::default());
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

    #[test]
    fn gateway_post_policy_projects_effective_write_conditions() {
        use super::access_owned::object_write::{PostObjectFormPolicy, PostObjectPolicyFields};

        let mut req = build_request((), Method::POST);
        req.headers.append("if-none-match", HeaderValue::from_static("*"));
        req.headers.append("if-none-match", HeaderValue::from_static("\"second\""));
        req.headers
            .insert("x-amz-server-side-encryption", HeaderValue::from_static("wire"));
        req.headers
            .insert("x-amz-server-side-encryption-customer-algorithm", HeaderValue::from_static("AES256"));
        PostObjectFormPolicy::new(PostObjectPolicyFields {
            if_match: Some(HeaderValue::from_static("\"form-only\"")),
            if_none_match: Some(HeaderValue::from_static("\"form-only\"")),
            encryption: Some("form"),
            ..Default::default()
        })
        .expect("valid form policy")
        .install_gateway(&mut req);
        let projected = &req.extensions.get::<PostObjectPolicyHeaders>().expect("projected headers").0;
        assert!(!projected.contains_key("if-match"));
        assert_eq!(projected.get("if-none-match"), Some(&HeaderValue::from_static("*")));
        assert_eq!(projected.get("x-amz-server-side-encryption"), Some(&HeaderValue::from_static("form")));
        assert_eq!(
            projected.get("x-amz-server-side-encryption-customer-algorithm"),
            Some(&HeaderValue::from_static("AES256"))
        );
        assert_eq!(req.headers.get_all("if-none-match").iter().count(), 2);
        assert_eq!(req.headers.get("x-amz-server-side-encryption"), Some(&HeaderValue::from_static("wire")));
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
        set_requested_object_lock_retain_until(&mut req, Some(retain_until));
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
        set_requested_object_lock_retain_until(&mut req, Some(retain_until));
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

    fn tagging_proxy_allowed<T>(request: &S3Request<T>) -> bool {
        let envelope = crate::app::RequestEnvelope::from_s3s(request);
        crate::app::object_meta::tagging::proxy_allowed(&envelope)
    }

    #[test]
    fn replication_proxy_requires_authorization_independent_of_local_tags() {
        let mut request = build_request((), Method::PUT);
        assert!(tagging_proxy_allowed(&request));
        // A local miss produces an empty tag map, but still establishes that the
        // policy needs tags. Neither anonymous nor authenticated callers may proxy.
        request
            .extensions
            .insert(ObjectTagConditions::new("bucket", "missing", None, HashMap::new()));
        assert!(!tagging_proxy_allowed(&request));
        request.extensions.insert(ObjectTagConditions::new(
            "bucket",
            "missing",
            Some("version"),
            HashMap::from([("ExistingObjectTag/protected".to_string(), vec!["yes".to_string()])]),
        ));
        assert!(!tagging_proxy_allowed(&request));
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
        assert!(!tagging_proxy_allowed(&request));
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
        let err = s3s::S3Error::from(denial_context(&auditor, false).deny(&req, "bucket_policy_explicit_deny", GET_OBJECT));
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
        let err = s3s::S3Error::from(denial_context(&auditor, true).deny(
            &req,
            "no_policy_allows_action",
            Action::S3Action(S3Action::ListBucketAction),
        ));
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

    fn production_sources_audit_reason(sources: &[&str], reason: &str) -> bool {
        sources.iter().any(|source| {
            let production = source.split("\nmod tests {").next().expect("source has a production section");
            production
                .match_indices("audit_request_denial(")
                .any(|(at, _)| production[at..].chars().take(260).collect::<String>().contains(reason))
        })
    }

    /// Request-level denial exits span the legacy and owned authorization modules.
    /// Check each exit in its owning file alongside the audit behavior tests.
    #[test]
    fn denial_exits_without_a_context_call_the_audit_function() {
        let access = include_str!("authorization/table_data_plane.rs");
        let object_misc = include_str!("access_owned/object_misc.rs")
            .split("\nmod tests;")
            .next()
            .expect("owned source has a production section");
        for (source, reason) in [
            (access, "\"table_resource_policy_denied\""),
            (access, "\"anonymous_table_data_plane_access\""),
            (object_misc, "\"recursive_force_delete_requires_authenticated_caller\""),
        ] {
            assert!(
                production_sources_audit_reason(&[source], reason),
                "{reason} exit does not call audit_request_denial"
            );
        }
    }

    #[test]
    fn denial_source_guard_tracks_moved_calls_and_rejects_removed_calls() {
        let reason = "\"recursive_force_delete_requires_authenticated_caller\"";
        let original = "fn original_exit() {}";
        let moved = format!("fn moved_exit() {{ audit_request_denial(req, auditor, {reason}, action); }}");

        assert!(!production_sources_audit_reason(&[original], reason));
        assert!(production_sources_audit_reason(&[original, &moved], reason));

        let removed = moved.replace("audit_request_denial(", "unrecorded_denial(");
        assert!(!production_sources_audit_reason(&[original, &removed], reason));

        let test_only = format!("{original}\nmod tests {{\n{moved}\n}}");
        assert!(!production_sources_audit_reason(&[original, &test_only], reason));
    }

    /// DeleteObjects audits its refused keys as one entry per denied action, carrying the count and
    /// no single object, and the per-key mark is cleared afterwards.
    #[test]
    fn bulk_denial_is_one_entry_per_denied_action_with_the_key_count() {
        let (auditor, captured) = test_auditor(true, 10, 10);
        let req = denial_request("denial-bulk", true);
        let mut envelope = crate::app::RequestEnvelope::from_s3s(&req);

        let nothing_denied = BulkDenialAudit::start_owned(&mut envelope).unwrap();
        assert!(envelope.req_info().unwrap().suppress_denial_audit);
        nothing_denied
            .finish_owned_with(&mut envelope, crate::app::RequestEnvelope::audit_view, &auditor)
            .unwrap();
        assert!(!envelope.req_info().unwrap().suppress_denial_audit);
        assert!(captured.lock().unwrap().is_empty());

        let mut bulk = BulkDenialAudit::start_owned(&mut envelope).unwrap();
        let delete = Action::S3Action(S3Action::DeleteObjectAction);
        let bypass = Action::S3Action(S3Action::BypassGovernanceRetentionAction);
        for action in [delete, delete, bypass, delete] {
            bulk.denied(action);
        }
        bulk.finish_owned_with(&mut envelope, crate::app::RequestEnvelope::audit_view, &auditor)
            .unwrap();

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
                assert!(!tagging_proxy_allowed(&req), "local absence cannot authorize a tagged replica");
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
                    let mut envelope = crate::app::RequestEnvelope::from_s3s(&req);
                    let owned = super::authorize_bucket_listing(&mut envelope, &bucket, S3Action::ListBucketAction).await;
                    let result = super::authorize_bucket_listing(&mut req, &bucket, S3Action::ListBucketAction).await;
                    assert_eq!(
                        owned.as_ref().err().map(|err| err.code().as_str().to_owned()),
                        result.as_ref().err().map(|err| err.code().as_str().to_owned()),
                        "both edges decide alike: {header}={value}, authenticated={authenticated}"
                    );
                    if value == "true" && (deny_versions || (authenticated && !grant_versions)) {
                        assert_eq!(
                            result.expect_err("version visibility requires permission").code(),
                            &rustfs_s3_types::S3ErrorCode::AccessDenied
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
            .execute_delete_object_from_s3s(req)
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
                let probe = crate::storage::helper::MutationCompletionProbe::install(&bucket);
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    crate::app::object_usecase::DefaultObjectUsecase::with_context(Some(context.clone()))
                        .execute_copy_object(request),
                )
                .await
                .expect("copy authorization must not reacquire its own object lock");
                if matches!(mode, "allowed-tags" | "self-copy") {
                    result.expect("unchanged allowed tags complete");
                    {
                        let events = probe.events.lock().unwrap();
                        assert_eq!(events.len(), 1, "one committed copy event");
                        assert_eq!(events[0].event_name, rustfs_s3_types::EventName::ObjectCreatedCopy);
                        assert_eq!(events[0].object.name, destination);
                    }
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
                    assert!(probe.events.lock().unwrap().is_empty(), "refused copy cannot emit a success event");
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

    #[tokio::test]
    #[serial]
    async fn copy_object_rejects_recreated_destination_bucket_after_authorization() {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let slot = ServerContextSlot::new();
        let context = Arc::new(AppContext::new(store.clone(), Arc::new(UnreadyIam), Arc::new(TestKms)));
        assert!(slot.install(context.clone()));
        let fs = FS::with_server_ctx(slot.clone());
        let src = format!("copy-src-{}", uuid::Uuid::new_v4());
        let dst = format!("copy-dst-{}", uuid::Uuid::new_v4());
        let policy = |bucket: &str, action: &str| {
            serde_json::to_vec(&serde_json::json!({"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Principal":{"AWS":"*"},"Action":action,"Resource":format!("arn:aws:s3:::{bucket}/*")
            }]}))
            .unwrap()
        };
        for (bucket, action) in [(&src, "s3:GetObject"), (&dst, "s3:PutObject")] {
            store.make_bucket(bucket, &MakeBucketOptions::default()).await.unwrap();
            store
                .update_bucket_metadata_config(
                    bucket,
                    crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                    policy(bucket, action),
                )
                .await
                .unwrap();
        }
        let opts = crate::storage::ObjectOptions::default();
        let mut body = crate::storage::PutObjReader::from_vec(b"source contents".to_vec());
        store.put_object(&src, "source", &mut body, &opts).await.unwrap();
        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: src.into(),
                key: "source".into(),
                version_id: None,
            })
            .bucket(dst.clone())
            .key("copied".to_owned())
            .build()
            .unwrap();
        let mut request = build_request(input, Method::PUT);
        ensure_req_info(&mut request);
        request.extensions.insert(slot);
        let captured_incarnation = store.bucket_incarnation_id(&dst).await.unwrap();
        let (loaded_tx, loaded_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
        install_object_generation_test_hook(dst.clone(), loaded_tx, resume_rx);
        let access = tokio::spawn(async move { (fs.copy_object(&mut request).await, request) });
        tokio::time::timeout(std::time::Duration::from_secs(10), loaded_rx)
            .await
            .expect("destination generation capture must be reached")
            .expect("copy access ended before capture");
        store.delete_bucket(&dst, &DeleteBucketOptions::default()).await.unwrap();
        store.make_bucket(&dst, &MakeBucketOptions::default()).await.unwrap();
        store
            .update_bucket_metadata_config(
                &dst,
                crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                policy(&dst, "s3:PutObject"),
            )
            .await
            .unwrap();
        assert_ne!(captured_incarnation, store.bucket_incarnation_id(&dst).await.unwrap());
        resume_tx.send(()).expect("resume copy before admission");
        let (result, request) = access.await.unwrap();
        result.expect("the equivalent policy should admit the copy");
        let mut captured = crate::storage::ObjectOptions::default();
        apply_bucket_generation_guard(&request, &dst, &mut captured).expect("copy carries the destination snapshot");
        assert_eq!(captured.expected_bucket_incarnation_id, Some(captured_incarnation));

        let error = crate::app::object_usecase::DefaultObjectUsecase::with_context(Some(context))
            .execute_copy_object(request)
            .await
            .expect_err("must refuse before publishing to the recreated bucket");
        assert_eq!(error.code(), &S3ErrorCode::NoSuchBucket);
        let missing = store
            .get_object_info(&dst, "copied", &opts)
            .await
            .expect_err("stale copy wrote an object");
        assert!(crate::storage::is_err_object_not_found(&missing), "{missing}");
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
        let replacement = b"recreated bucket object";
        store
            .put_object(
                &bucket,
                "object",
                &mut crate::storage::PutObjReader::from_vec(replacement.to_vec()),
                &crate::storage::ObjectOptions::default(),
            )
            .await
            .expect("write the recreated bucket's object");
        resume_tx
            .send(())
            .expect("RestoreObject access should still be waiting at the test hook");

        let (result, req) = access.await.expect("RestoreObject access task should join");
        result.expect("the equivalent policy should authorize RestoreObject without refreshing its snapshot");
        let mut opts = crate::storage::ObjectOptions::default();
        apply_bucket_generation_guard(&req, &bucket, &mut opts).expect("apply the RestoreObject authorization guard");

        // The RestoreObject usecase future is large enough that heap-pinning a
        // single future was not enough on Linux: the whole scenario runs on a
        // dedicated large stack (see the `#[test]` wrapper above) so unrelated
        // growth in bucket metadata futures cannot tip the test over.
        let result = Box::pin(
            crate::app::object_usecase::DefaultObjectUsecase::with_context(Some(app_context)).execute_restore_object(req),
        )
        .await;
        use tokio::io::AsyncReadExt as _;
        let mut reader = store
            .get_object_reader(&bucket, "object", None, HeaderMap::new(), &crate::storage::ObjectOptions::default())
            .await
            .expect("read the recreated bucket's object");
        let mut bytes = Vec::new();
        reader.stream.read_to_end(&mut bytes).await.expect("read replacement bytes");
        assert_eq!(
            (
                opts.expected_bucket_incarnation_id,
                result.as_ref().err().map(|error| error.code()),
                bytes.as_slice(),
                reader.object_info.restore_ongoing
            ),
            (
                Some(authorized_incarnation_id),
                Some(&S3ErrorCode::NoSuchBucket),
                replacement.as_slice(),
                false
            ),
            "stale RestoreObject must refuse and preserve the recreated bucket's object"
        );
        drop(reader);
        store
            .delete_bucket(
                &bucket,
                &DeleteBucketOptions {
                    force: true,
                    ..Default::default()
                },
            )
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
            .map_err(s3s::S3Error::from)
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
            bucket: Some("previous-bucket".to_string()),
            object: Some("previous-key".to_string()),
            version_id: Some("previous-version".to_string()),
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
