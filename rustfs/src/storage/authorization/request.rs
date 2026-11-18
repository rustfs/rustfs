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

//! The request facts the shared authorization core reads, and the plain accessors over them.
//!
//! Responsible for: the [`AuthorizationRequest`] trait both request edges implement, the
//! `ReqInfo` and `RequestContext` accessors, the request-header predicates authorization
//! reads, and the object and IAM stores a request resolves to.
//! Not responsible for: any policy decision (`decision`), condition building (`conditions`),
//! or the trait implementations, which belong to the legacy S3 request adapter and the
//! app layer's owned `RequestEnvelope`.
//! Upstream: both edges' access phases. Downstream: every module of `storage::authorization`,
//! `storage::access_owned`, and the use cases that read authorization state.

use super::{
    BucketConfigMutationSnapshot, BucketGenerationGuard, CopySourceBucketGenerationGuard, OdmReadGenerationGuard,
    PendingDeleteBucketGenerationGuard,
};
use super::{TableDataPlaneListAccess, TableDataPlanePublicationGuards};
use crate::storage::access::ObjectTagConditions;
use crate::storage::denial_audit::DenialAuditor;
use crate::storage::req_info::ReqInfo;
use crate::storage::request_context::RequestContext;
use crate::storage::request_markers::{
    PermissionProbeRequestFact, PostObjectPolicyHeaders, RequestedCopySource, RequestedObjectLegalHold,
    RequestedObjectLockRetainUntil, RequestedObjectLockRetention, RequestedObjectTags,
};
use crate::storage::storage_api::runtime_sources_consumer::{ECStore, ServerContextSlot, runtime_sources};
use http::HeaderMap;
use rustfs_iam::{error::Error as IamError, store::object::ObjectStore, sys::IamSys};
use rustfs_policy::policy::action::Action;
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result, s3_error};
use rustfs_trusted_proxies::ClientInfo;
use rustfs_utils::http::{AMZ_OBJECT_LOCK_BYPASS_GOVERNANCE, force_delete_header};
use std::sync::Arc;
use time::OffsetDateTime;

pub(crate) fn replication_request_authorized<R: AuthorizationRequest + ?Sized>(req: &R) -> bool {
    req.req_info().is_some_and(|req_info| req_info.replication_request_authorized)
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

pub(crate) fn invalid_force_delete_header() -> S3Error {
    S3Error::with_message(S3ErrorCode::InvalidRequest, "Invalid force-delete header value")
}

pub(crate) fn missing_req_info_error() -> S3Error {
    s3_error!(InternalError, "ReqInfo not found in request extensions")
}

pub(crate) fn req_info_ref<R: AuthorizationRequest + ?Sized>(req: &R) -> S3Result<&ReqInfo> {
    req.req_info().ok_or_else(missing_req_info_error)
}

pub(crate) fn req_info_mut<R: AuthorizationRequest + ?Sized>(req: &mut R) -> S3Result<&mut ReqInfo> {
    req.req_info_mut().ok_or_else(missing_req_info_error)
}

/// Extract the canonical `RequestContext` from a request, checking both
/// the request extensions directly and the `ReqInfo.request_context` field.
pub(crate) fn request_context_from_req<R: AuthorizationRequest + ?Sized>(req: &R) -> Option<RequestContext> {
    req.request_context()
        .cloned()
        .or_else(|| req.req_info().and_then(|req_info| req_info.request_context.clone()))
}

pub(crate) const AMZ_WRITE_OFFSET_BYTES_HEADER: &str = "x-amz-write-offset-bytes";

pub(crate) fn has_write_offset_bytes_header(headers: &http::HeaderMap) -> bool {
    headers.contains_key(AMZ_WRITE_OFFSET_BYTES_HEADER)
}

/// The request facts the shared authorization core reads. Both edges feed the
/// same policy evaluation without constructing another request type; the
/// owned `RequestEnvelope` implements it in the app layer.
pub(crate) trait AuthorizationRequest {
    fn permission_probe_caller_authentication(&self) -> Option<crate::auth::facts::CallerAuthentication> {
        None
    }
    fn permission_probe_request_fact_known(&self, _fact: PermissionProbeRequestFact) -> bool {
        false
    }
    fn permission_probe_auth_type(&self) -> Option<crate::auth::facts::AuthTypeCondition> {
        None
    }
    fn permission_probe(&self) -> bool {
        false
    }
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
    fn set_staged_multipart_part(&mut self, staged: bool);
    fn requested_retention(&self) -> Option<&RequestedObjectLockRetention>;
    fn requested_retain_until(&self) -> Option<&RequestedObjectLockRetainUntil>;
    fn requested_legal_hold(&self) -> Option<&RequestedObjectLegalHold>;
    fn requested_object_tags(&self) -> Option<&RequestedObjectTags>;
    fn take_requested_object_tags(&mut self) -> Option<RequestedObjectTags>;
    fn install_requested_retention(&mut self, retention: RequestedObjectLockRetention);
    fn install_requested_retain_until(&mut self, retain_until: Option<RequestedObjectLockRetainUntil>);
    fn install_requested_legal_hold(&mut self, legal_hold: RequestedObjectLegalHold);
    fn install_requested_object_tags(&mut self, tags: RequestedObjectTags);
    fn requested_copy_source(&self) -> Option<&RequestedCopySource>;
    fn install_requested_copy_source(&mut self, source: RequestedCopySource);
    fn install_copy_source_bucket_generation(&mut self, guard: CopySourceBucketGenerationGuard);
    /// The headers that describe what the request stores: a POST upload's form
    /// fields when present, otherwise the request headers.
    fn policy_headers(&self) -> &HeaderMap;
    fn install_post_object_policy_headers(&mut self, headers: PostObjectPolicyHeaders);
    fn install_post_object_marker(&mut self);
    fn object_tag_conditions(&self) -> Option<&ObjectTagConditions>;
    fn take_object_tag_conditions(&mut self) -> Option<ObjectTagConditions>;
    fn install_object_tag_conditions(&mut self, conditions: ObjectTagConditions);
    fn publication_guards(&self) -> Option<&TableDataPlanePublicationGuards>;
    fn install_publication_guards(&mut self, guards: TableDataPlanePublicationGuards);
    fn install_table_list_access(&mut self, access: TableDataPlaneListAccess);
    fn audit_denial(&self, auditor: &DenialAuditor, action: &Action, reason: &'static str);
}

/// Records the retain-until date a request asks to set, or clears it when the
/// request sets none. Call it before `authorize_request` with the parsed date from
/// headers, form fields, the PutObjectRetention XML body, or an archive member.
pub(crate) fn set_requested_object_lock_retain_until<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    retain_until: Option<OffsetDateTime>,
) {
    req.install_requested_retain_until(retain_until.map(RequestedObjectLockRetainUntil));
}

pub(crate) fn request_object_store<R: AuthorizationRequest + ?Sized>(req: &R) -> S3Result<Arc<ECStore>> {
    match req.server_context() {
        Some(server_ctx) => server_ctx
            .installed_object_store()
            .ok_or_else(object_store_not_initialized_error),
        None => runtime_sources::current_object_store_handle().ok_or_else(object_store_not_initialized_error),
    }
}

pub(crate) fn object_store_not_initialized_error() -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, "object store is not initialized")
}

pub(crate) fn request_iam_store<R: AuthorizationRequest + ?Sized>(req: &R) -> S3Result<Arc<IamSys<ObjectStore>>> {
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

/// Check if the request has the x-amz-bypass-governance-retention header set to true
pub fn has_bypass_governance_header(headers: &http::HeaderMap) -> bool {
    headers
        .get(AMZ_OBJECT_LOCK_BYPASS_GOVERNANCE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}
