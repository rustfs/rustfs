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

#[cfg(test)]
use super::ECStore;
use super::access_owned::listing_multipart::{
    authorize_bucket_listing, authorize_list_multipart_uploads, authorize_list_objects_v2, authorize_list_parts,
};
use super::access_owned::object_misc::{authorize_delete_object, authorize_delete_objects};
// The legacy-edge files still import the owned shapes through this path.
pub(crate) use super::access_owned;
use super::ecfs::FS;
#[cfg(test)]
use crate::auth::facts::{AuthzFacts, AuthzPrincipal, AuthzTarget};
use crate::auth::{get_request_auth_type_with_query, get_session_token, reject_unsigned_amz_headers_on_sigv4_request};
use crate::shared_types::RemoteAddr;
use crate::storage::authorization::ObjectTagConditions;
use crate::storage::authorization::{AccessMarkers, AuthorizationRequest, VerifiedSignature, admit_operation, resolve_req_info};
use crate::storage::authorization::{
    BucketConfigMutationSnapshot, BucketGenerationGuard, CopySourceBucketGenerationGuard, OdmReadGenerationGuard,
    PendingDeleteBucketGenerationGuard,
};
use crate::storage::authorization::{TableDataPlaneListAccess, TableDataPlanePublicationGuards};
#[cfg(test)]
use crate::storage::authorization::{
    authorization_conditions, authorize_request_with_location_constraint, request_authz_facts, request_object_store,
};
use crate::storage::authorization::{
    authorize_bucket_config_mutation, authorize_bucket_request, authorize_bucket_tag_deletion, authorize_object_request,
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
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_trusted_proxies::ClientInfo;
use s3s::access::{S3Access, S3AccessContext};
use s3s::{S3Request, S3Result, dto::*, s3_error};
#[cfg(test)]
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;
use url::Url;

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
        false
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
mod tests {
    // Pins on helpers only the legacy hooks call, for operations whose gateway-side choice no
    // test pins yet; they are deleted with the hooks.
    use super::{get_bucket_policy_authorize_action, put_bucket_policy_authorize_action, validate_post_object_success_controls};
    use rustfs_policy::policy::action::{Action, S3Action};
    use s3s::dto::PostObjectInput;

    #[test]
    fn get_bucket_policy_uses_get_bucket_policy_action() {
        assert_eq!(get_bucket_policy_authorize_action(), Action::S3Action(S3Action::GetBucketPolicyAction));
    }

    #[test]
    fn put_bucket_policy_uses_put_bucket_policy_action() {
        assert_eq!(put_bucket_policy_authorize_action(), Action::S3Action(S3Action::PutBucketPolicyAction));
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
    fn validate_post_object_success_controls_accepts_empty_redirect() {
        let input = PostObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .success_action_redirect(Some("".to_string()))
            .build()
            .expect("post object input should build");
        assert!(validate_post_object_success_controls(&input).is_ok());
    }
}
