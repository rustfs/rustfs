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

//! The request facts the app layer reads, owned by RustFS rather than by the
//! HTTP framework that decoded the request (rustfs/backlog#2734 task T1.7).
//!
//! Every extension payload that app, storage and admin code reads from a
//! request is a named field of [`RequestEnvelope`]. An edge decodes the facts
//! it owns into a [`RequestHead`] and hands over the request's extensions;
//! [`RequestEnvelope::from_parts`] moves each named payload into its field and
//! keeps whatever is left in a residual bag. The bag exists only while call
//! sites migrate and must be empty once they have.
//!
//! This module names no HTTP-framework type. The legacy s3s edge that builds an
//! envelope lives in `storage/s3_compat/envelope.rs`.

use super::ServerContextSlot;
use crate::app::metadata_route::WebsiteRead;
use crate::app::object::request_body::BodyReadControl;
use crate::app::storage_api::context::OperationHelper;
use crate::app::storage_api::context::{
    AuditRequestView, AuthorizationRequest, DenialAuditor, DenialScope, authorize_request_core,
};
use crate::app::storage_api::context::{
    BucketConfigMutationSnapshot, BucketGenerationGuard, CopySourceBucketGenerationGuard, MultipartStagingOperation,
    ObjectTagConditions, OdmReadGenerationGuard, PendingDeleteBucketGenerationGuard, PostObjectPolicyHeaders,
    PostObjectRequestMarker, ReqInfo, RequestContext, RequestedCopySource, RequestedObjectLegalHold,
    RequestedObjectLockRetainUntil, RequestedObjectLockRetention, RequestedObjectTags, StagedMultipartPartAuthorization,
    TableDataPlaneListAccess, TableDataPlanePublicationGuards,
};
use crate::auth::{VerifiedPresignedRequest, VerifiedSigV4Request};
use crate::shared_types::{NativeTlsTransport, RemoteAddr};
use http::{Extensions, HeaderMap, Method, StatusCode, Uri};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_ops::S3Operation;
use rustfs_trusted_proxies::ClientInfo;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use zeroize::Zeroizing;

/// The identity a request was authenticated with.
///
/// The secret key and the session token are bearer material: the type has no
/// `PartialEq`, and its `Debug` output redacts both. An edge that keeps the
/// caller's secret (it verified the signature itself) builds the value with
/// [`Self::without_secret_key`]; the absence is then `None`, never a
/// substitute string a reader could compare, seal or derive with.
#[derive(Clone)]
pub struct RequestCredentials {
    access_key: String,
    secret_key: Option<Zeroizing<String>>,
    session_token: Option<Zeroizing<String>>,
}

/// An edge handed over an empty secret key. Absence is expressed with
/// [`RequestCredentials::without_secret_key`], so an empty secret is refused
/// rather than read as "absent" or used as a key.
#[derive(Debug)]
pub(crate) struct EmptySecretKey;

impl fmt::Display for EmptySecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("handed-over secret key is empty")
    }
}

impl std::error::Error for EmptySecretKey {}

impl RequestCredentials {
    /// Credentials whose secret the edge handed over.
    ///
    /// # Errors
    ///
    /// [`EmptySecretKey`] when `secret_key` is empty.
    pub(crate) fn new(access_key: String, secret_key: String, session_token: Option<String>) -> Result<Self, EmptySecretKey> {
        if secret_key.is_empty() {
            return Err(EmptySecretKey);
        }
        Ok(Self {
            access_key,
            secret_key: Some(Zeroizing::new(secret_key)),
            session_token: session_token.map(Zeroizing::new),
        })
    }

    /// Credentials whose secret the edge kept.
    pub(crate) fn without_secret_key(access_key: String, session_token: Option<String>) -> Self {
        Self {
            access_key,
            secret_key: None,
            session_token: session_token.map(Zeroizing::new),
        }
    }

    /// The same identity with `session_token` as its token.
    #[cfg(test)]
    pub(crate) fn with_session_token(mut self, session_token: Option<String>) -> Self {
        self.session_token = session_token.map(Zeroizing::new);
        self
    }

    pub fn access_key(&self) -> &str {
        &self.access_key
    }

    /// The caller's secret, or `None` when the edge did not hand it over.
    /// A reader must fail closed on `None`. Never log this value or compare it
    /// with `==`.
    pub fn secret_key(&self) -> Option<&str> {
        self.secret_key.as_deref().map(String::as_str)
    }

    pub fn session_token(&self) -> Option<&str> {
        self.session_token.as_deref().map(String::as_str)
    }

    /// The handed-over secret, or [`MissingSecretKey`] when the edge kept it.
    pub(crate) fn require_secret_key(&self) -> Result<&str, MissingSecretKey> {
        self.secret_key().ok_or(MissingSecretKey)
    }

    /// Whether the handed-over secret equals `stored`, compared in constant
    /// time. An absent secret matches nothing, not even an empty `stored`.
    pub(crate) fn secret_key_matches(&self, stored: &str) -> bool {
        self.secret_key()
            .is_some_and(|secret| crate::auth::constant_time_eq(secret, stored))
    }
}

/// A reader needed the caller's secret, but the edge did not hand it over.
#[derive(Debug)]
pub(crate) struct MissingSecretKey;

impl fmt::Display for MissingSecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the caller's secret key was not handed over for this request")
    }
}

impl std::error::Error for MissingSecretKey {}

impl fmt::Debug for RequestCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestCredentials")
            .field("access_key", &self.access_key)
            .field("secret_key", &self.secret_key.as_ref().map(|_| "<redacted>"))
            .field("session_token", &self.session_token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// The facts an edge decodes from the request itself, as opposed to the
/// payloads that middleware attached to it.
pub(crate) struct RequestHead {
    pub(crate) method: Method,
    pub(crate) uri: Uri,
    pub(crate) headers: HeaderMap,
    pub(crate) credentials: Option<RequestCredentials>,
    pub(crate) region: Option<String>,
    pub(crate) service: Option<String>,
}

/// Every request fact the app layer reads.
///
/// There is deliberately no generic extension lookup: a payload the app layer
/// needs gets a named field here, so an edge that builds an envelope can see
/// everything it has to supply.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "fields read only by the crate-private accessors stay unread until call sites move in T1.8 (rustfs/backlog#2749)"
    )
)]
pub struct RequestEnvelope {
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    credentials: Option<RequestCredentials>,
    region: Option<String>,
    service: Option<String>,
    // Attached by the connection and request layers in `server/`.
    remote_addr: Option<SocketAddr>,
    client_info: Option<ClientInfo>,
    native_tls_transport: Option<bool>,
    request_context: Option<RequestContext>,
    server_context: Option<Arc<ServerContextSlot>>,
    body_read_control: Option<BodyReadControl>,
    // Attached by routing and signature verification.
    verified_sigv4: Option<VerifiedSigV4Request>,
    verified_presigned: Option<VerifiedPresignedRequest>,
    website_read: Option<WebsiteRead>,
    post_object: Option<PostObjectRequestMarker>,
    post_object_policy_headers: Option<PostObjectPolicyHeaders>,
    // Attached by the authorization phase (`storage/access.rs`) for the use case.
    req_info: Option<ReqInfo>,
    bucket_generation: Option<BucketGenerationGuard>,
    pending_delete_bucket_generation: Option<PendingDeleteBucketGenerationGuard>,
    copy_source_bucket_generation: Option<CopySourceBucketGenerationGuard>,
    odm_read_generation: Option<OdmReadGenerationGuard>,
    bucket_config_mutation: Option<BucketConfigMutationSnapshot>,
    table_publication_guards: Option<TableDataPlanePublicationGuards>,
    table_list_access: Option<TableDataPlaneListAccess>,
    object_tag_conditions: Option<ObjectTagConditions>,
    multipart_staging_operation: Option<MultipartStagingOperation>,
    staged_multipart_part_authorization: Option<StagedMultipartPartAuthorization>,
    requested_object_lock_retention: Option<RequestedObjectLockRetention>,
    requested_object_lock_retain_until: Option<RequestedObjectLockRetainUntil>,
    requested_object_legal_hold: Option<RequestedObjectLegalHold>,
    requested_object_tags: Option<RequestedObjectTags>,
    requested_copy_source: Option<RequestedCopySource>,
    permission_probe: bool,
    permission_probe_auth_type: Option<crate::auth::facts::AuthTypeCondition>,
    permission_probe_known_request_facts: u8,
    residual_extensions: Extensions,
}

impl RequestEnvelope {
    /// Builds an envelope from what an edge decoded and the request's
    /// extensions. A payload the request does not carry leaves its field
    /// `None`; it is never replaced by a default.
    pub(crate) fn from_parts(head: RequestHead, mut extensions: Extensions) -> Self {
        // Each payload is removed under exactly the type its readers fetch, so
        // a named type never lingers in the residual bag. The connection layer
        // stores the peer as `Option<RemoteAddr>`; `None` there means the same
        // as no peer at all.
        Self {
            method: head.method,
            uri: head.uri,
            headers: head.headers,
            credentials: head.credentials,
            region: head.region,
            service: head.service,
            remote_addr: extensions.remove::<Option<RemoteAddr>>().flatten().map(|addr| addr.0),
            client_info: extensions.remove(),
            native_tls_transport: extensions.remove::<NativeTlsTransport>().map(|transport| transport.0),
            request_context: extensions.remove(),
            server_context: extensions.remove(),
            body_read_control: extensions.remove(),
            verified_sigv4: extensions.remove(),
            verified_presigned: extensions.remove(),
            website_read: extensions.remove(),
            post_object: extensions.remove(),
            post_object_policy_headers: extensions.remove(),
            req_info: extensions.remove(),
            bucket_generation: extensions.remove(),
            pending_delete_bucket_generation: extensions.remove(),
            copy_source_bucket_generation: extensions.remove(),
            odm_read_generation: extensions.remove(),
            bucket_config_mutation: extensions.remove(),
            table_publication_guards: extensions.remove(),
            table_list_access: extensions.remove(),
            object_tag_conditions: extensions.remove(),
            multipart_staging_operation: extensions.remove(),
            staged_multipart_part_authorization: extensions.remove(),
            requested_object_lock_retention: extensions.remove(),
            requested_object_lock_retain_until: extensions.remove(),
            requested_object_legal_hold: extensions.remove(),
            requested_object_tags: extensions.remove(),
            requested_copy_source: extensions.remove(),
            permission_probe: false,
            permission_probe_auth_type: None,
            permission_probe_known_request_facts: 0,
            residual_extensions: extensions,
        }
    }

    /// A copy for one more authorization of the same request, such as one member of an archive
    /// upload, which is authorized as its own object. It carries every fact the original carries,
    /// the access phase's output included: the same captured generations and the same body-read
    /// budget. The table publication guards are one shared set, so a lock the copy's
    /// authorization retains stays held for the original's write too. Anything else installed on
    /// the copy does not show on the original.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the PutObject archive member loop of batch D is its first caller")
    )]
    pub(crate) fn fork_for_authorization(&self) -> Self {
        Self {
            method: self.method.clone(),
            uri: self.uri.clone(),
            headers: self.headers.clone(),
            credentials: self.credentials.clone(),
            region: self.region.clone(),
            service: self.service.clone(),
            remote_addr: self.remote_addr,
            client_info: self.client_info.clone(),
            native_tls_transport: self.native_tls_transport,
            request_context: self.request_context.clone(),
            server_context: self.server_context.clone(),
            body_read_control: self.body_read_control.clone(),
            verified_sigv4: self.verified_sigv4,
            verified_presigned: self.verified_presigned,
            website_read: self.website_read.clone(),
            post_object: self.post_object.clone(),
            post_object_policy_headers: self.post_object_policy_headers.clone(),
            req_info: self.req_info.clone(),
            bucket_generation: self.bucket_generation.clone(),
            pending_delete_bucket_generation: self.pending_delete_bucket_generation.clone(),
            copy_source_bucket_generation: self.copy_source_bucket_generation.clone(),
            odm_read_generation: self.odm_read_generation.clone(),
            bucket_config_mutation: self.bucket_config_mutation.clone(),
            table_publication_guards: self.table_publication_guards.clone(),
            table_list_access: self.table_list_access.clone(),
            object_tag_conditions: self.object_tag_conditions.clone(),
            multipart_staging_operation: self.multipart_staging_operation,
            staged_multipart_part_authorization: self.staged_multipart_part_authorization.clone(),
            requested_object_lock_retention: self.requested_object_lock_retention.clone(),
            requested_object_lock_retain_until: self.requested_object_lock_retain_until.clone(),
            requested_object_legal_hold: self.requested_object_legal_hold.clone(),
            requested_object_tags: self.requested_object_tags.clone(),
            requested_copy_source: self.requested_copy_source.clone(),
            permission_probe: self.permission_probe,
            permission_probe_auth_type: self.permission_probe_auth_type,
            permission_probe_known_request_facts: self.permission_probe_known_request_facts,
            residual_extensions: self.residual_extensions.clone(),
        }
    }

    /// A read-only policy query preserves verified connection and identity facts, without
    /// operation guards, body metadata or internal authorization shortcuts.
    pub(crate) fn fork_for_permission_probe(&self, method: Method, uri: Uri, req_info: ReqInfo) -> Self {
        let mut extensions = Extensions::new();
        if let Some(addr) = self.remote_addr {
            extensions.insert(Some(RemoteAddr(addr)));
        }
        if let Some(client_info) = &self.client_info {
            extensions.insert(client_info.clone());
        }
        if let Some(transport) = self.native_tls_transport {
            extensions.insert(NativeTlsTransport(transport));
        }
        if let Some(context) = &self.server_context {
            extensions.insert(context.clone());
        }
        extensions.insert(req_info);
        let mut headers = HeaderMap::new();
        for name in ["authorization", "x-amz-date", "x-amz-security-token"] {
            if let Some(value) = self.headers.get(name) {
                headers.insert(http::header::HeaderName::from_static(name), value.clone());
            }
        }
        let mut probe = Self::from_parts(
            RequestHead {
                method,
                uri,
                headers,
                credentials: self.credentials.clone(),
                region: self.region.clone(),
                service: self.service.clone(),
            },
            extensions,
        );
        probe.permission_probe = true;
        probe
    }

    pub(crate) fn set_permission_probe_auth_type(&mut self, auth_type: crate::auth::facts::AuthTypeCondition) {
        if self.permission_probe {
            self.permission_probe_auth_type = Some(auth_type);
        }
    }

    pub(crate) fn mark_permission_probe_request_fact_known(&mut self, fact: crate::storage::access::PermissionProbeRequestFact) {
        if self.permission_probe {
            self.permission_probe_known_request_facts |= fact as u8;
        }
    }

    pub(crate) fn set_permission_probe_operation(&mut self, operation: &str) {
        if self.permission_probe {
            self.multipart_staging_operation = MultipartStagingOperation::for_operation(operation);
        }
    }

    pub fn method(&self) -> &Method {
        &self.method
    }

    pub fn uri(&self) -> &Uri {
        &self.uri
    }

    pub(crate) fn uri_mut(&mut self) -> &mut Uri {
        &mut self.uri
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// `None` means the request is anonymous.
    pub fn credentials(&self) -> Option<&RequestCredentials> {
        self.credentials.as_ref()
    }

    pub fn region(&self) -> Option<&str> {
        self.region.as_deref()
    }

    pub fn service(&self) -> Option<&str> {
        self.service.as_deref()
    }

    /// The TCP peer of the connection, which is a proxy when one is in front;
    /// [`Self::client_info`] carries the client address a trusted proxy reported.
    pub fn remote_addr(&self) -> Option<SocketAddr> {
        self.remote_addr
    }

    pub fn client_info(&self) -> Option<&ClientInfo> {
        self.client_info.as_ref()
    }

    /// The client address for policy evaluation: the verified proxy client
    /// when one is recorded, otherwise the connection peer (keeping its port).
    pub fn policy_source_addr(&self) -> Option<SocketAddr> {
        let peer = self.remote_addr;
        self.client_info
            .as_ref()
            .map(|client| SocketAddr::new(client.real_ip, peer.map_or(0, |addr| addr.port())))
            .or(peer)
    }

    pub(crate) fn native_tls_transport(&self) -> Option<bool> {
        self.native_tls_transport
    }

    pub fn request_context(&self) -> Option<&RequestContext> {
        self.request_context.as_ref()
    }

    pub fn server_context(&self) -> Option<&Arc<ServerContextSlot>> {
        self.server_context.as_ref()
    }

    /// Extension types no field names yet. Must be empty once call sites have
    /// migrated; nothing should read it in the meantime.
    pub fn residual_extensions(&self) -> &Extensions {
        &self.residual_extensions
    }
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "call sites move to the envelope in T1.8 (rustfs/backlog#2749); remove once every accessor has a caller"
    )
)]
impl RequestEnvelope {
    pub(crate) fn body_read_control(&self) -> Option<&BodyReadControl> {
        self.body_read_control.as_ref()
    }

    pub(crate) fn verified_sigv4(&self) -> Option<&VerifiedSigV4Request> {
        self.verified_sigv4.as_ref()
    }

    pub(crate) fn verified_presigned(&self) -> Option<&VerifiedPresignedRequest> {
        self.verified_presigned.as_ref()
    }

    pub(crate) fn website_read(&self) -> Option<&WebsiteRead> {
        self.website_read.as_ref()
    }

    pub(crate) fn post_object(&self) -> Option<&PostObjectRequestMarker> {
        self.post_object.as_ref()
    }

    pub(crate) fn req_info(&self) -> Option<&ReqInfo> {
        self.req_info.as_ref()
    }

    pub(crate) fn req_info_mut(&mut self) -> Option<&mut ReqInfo> {
        self.req_info.as_mut()
    }

    pub(crate) fn install_req_info(&mut self, req_info: ReqInfo) -> Option<ReqInfo> {
        self.req_info.replace(req_info)
    }

    pub(crate) fn bucket_generation(&self) -> Option<&BucketGenerationGuard> {
        self.bucket_generation.as_ref()
    }

    pub(crate) fn pending_delete_bucket_generation(&self) -> Option<&PendingDeleteBucketGenerationGuard> {
        self.pending_delete_bucket_generation.as_ref()
    }

    pub(crate) fn copy_source_bucket_generation(&self) -> Option<&CopySourceBucketGenerationGuard> {
        self.copy_source_bucket_generation.as_ref()
    }

    pub(crate) fn odm_read_generation(&self) -> Option<&OdmReadGenerationGuard> {
        self.odm_read_generation.as_ref()
    }

    pub(crate) fn bucket_config_mutation(&self) -> Option<&BucketConfigMutationSnapshot> {
        self.bucket_config_mutation.as_ref()
    }

    pub(crate) fn table_publication_guards(&self) -> Option<&TableDataPlanePublicationGuards> {
        self.table_publication_guards.as_ref()
    }

    pub(crate) fn install_table_publication_guards(&mut self, guards: TableDataPlanePublicationGuards) {
        self.table_publication_guards = Some(guards);
    }

    pub(crate) fn table_list_access(&self) -> Option<&TableDataPlaneListAccess> {
        self.table_list_access.as_ref()
    }

    pub(crate) fn object_tag_conditions(&self) -> Option<&ObjectTagConditions> {
        self.object_tag_conditions.as_ref()
    }

    pub(crate) fn install_object_tag_conditions(&mut self, conditions: ObjectTagConditions) {
        self.object_tag_conditions = Some(conditions);
    }

    pub(crate) fn multipart_staging_operation(&self) -> Option<&MultipartStagingOperation> {
        self.multipart_staging_operation.as_ref()
    }

    pub(crate) fn staged_multipart_part_authorization(&self) -> Option<&StagedMultipartPartAuthorization> {
        self.staged_multipart_part_authorization.as_ref()
    }

    pub(crate) fn requested_object_lock_retention(&self) -> Option<&RequestedObjectLockRetention> {
        self.requested_object_lock_retention.as_ref()
    }

    pub(crate) fn requested_object_lock_retain_until(&self) -> Option<&RequestedObjectLockRetainUntil> {
        self.requested_object_lock_retain_until.as_ref()
    }

    pub(crate) fn post_object_policy_headers(&self) -> Option<&PostObjectPolicyHeaders> {
        self.post_object_policy_headers.as_ref()
    }

    pub(crate) fn requested_object_legal_hold(&self) -> Option<&RequestedObjectLegalHold> {
        self.requested_object_legal_hold.as_ref()
    }

    pub(crate) fn requested_object_tags(&self) -> Option<&RequestedObjectTags> {
        self.requested_object_tags.as_ref()
    }

    pub(crate) fn requested_copy_source(&self) -> Option<&RequestedCopySource> {
        self.requested_copy_source.as_ref()
    }
}

/// The authorization core in `storage` reads requests only through
/// [`AuthorizationRequest`]; this impl hands it the envelope's named fields.
impl AuthorizationRequest for RequestEnvelope {
    fn permission_probe_request_fact_known(&self, fact: crate::storage::access::PermissionProbeRequestFact) -> bool {
        self.permission_probe_known_request_facts & (fact as u8) != 0
    }
    fn permission_probe(&self) -> bool {
        self.permission_probe
    }
    fn permission_probe_auth_type(&self) -> Option<crate::auth::facts::AuthTypeCondition> {
        self.permission_probe_auth_type
    }
    fn headers(&self) -> &HeaderMap {
        self.headers()
    }
    fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.headers
    }
    fn uri(&self) -> &http::Uri {
        self.uri()
    }
    fn remote_addr(&self) -> Option<std::net::SocketAddr> {
        self.remote_addr()
    }
    fn client_info(&self) -> Option<&ClientInfo> {
        self.client_info()
    }
    fn server_context(&self) -> Option<&Arc<ServerContextSlot>> {
        self.server_context()
    }
    fn req_info(&self) -> Option<&ReqInfo> {
        self.req_info()
    }
    fn req_info_mut(&mut self) -> Option<&mut ReqInfo> {
        self.req_info_mut()
    }
    fn request_context(&self) -> Option<&RequestContext> {
        self.request_context.as_ref()
    }
    fn bucket_generation(&self) -> Option<&BucketGenerationGuard> {
        self.bucket_generation.as_ref()
    }
    fn install_bucket_generation(&mut self, guard: BucketGenerationGuard) {
        self.bucket_generation = Some(guard);
    }
    fn pending_delete_bucket_generation(&self) -> Option<&PendingDeleteBucketGenerationGuard> {
        self.pending_delete_bucket_generation.as_ref()
    }
    fn install_pending_delete_bucket_generation(&mut self, guard: PendingDeleteBucketGenerationGuard) {
        self.pending_delete_bucket_generation = Some(guard);
    }
    fn copy_source_bucket_generation(&self) -> Option<&CopySourceBucketGenerationGuard> {
        self.copy_source_bucket_generation.as_ref()
    }
    fn odm_read_generation(&self) -> Option<&OdmReadGenerationGuard> {
        self.odm_read_generation.as_ref()
    }
    fn install_odm_read_generation(&mut self, guard: OdmReadGenerationGuard) {
        self.odm_read_generation = Some(guard);
    }
    fn bucket_config_mutation(&self) -> Option<&BucketConfigMutationSnapshot> {
        self.bucket_config_mutation.as_ref()
    }
    fn install_bucket_config_mutation(&mut self, snapshot: BucketConfigMutationSnapshot) {
        self.bucket_config_mutation = Some(snapshot);
    }
    #[cfg(test)]
    fn test_extensions(&self) -> &Extensions {
        &self.residual_extensions
    }
    fn internal_object_authorization(&self) -> bool {
        false
    }
    fn multipart_staging(&self) -> bool {
        self.multipart_staging_operation().is_some()
    }
    fn staged_multipart_part(&self) -> bool {
        self.staged_multipart_part_authorization().is_some()
    }
    fn set_staged_multipart_part(&mut self, staged: bool) {
        self.staged_multipart_part_authorization = staged.then_some(StagedMultipartPartAuthorization);
    }
    fn requested_retention(&self) -> Option<&RequestedObjectLockRetention> {
        self.requested_object_lock_retention()
    }
    fn requested_retain_until(&self) -> Option<&RequestedObjectLockRetainUntil> {
        self.requested_object_lock_retain_until()
    }
    fn requested_legal_hold(&self) -> Option<&RequestedObjectLegalHold> {
        self.requested_object_legal_hold()
    }
    fn requested_object_tags(&self) -> Option<&RequestedObjectTags> {
        self.requested_object_tags()
    }
    fn install_requested_retention(&mut self, retention: RequestedObjectLockRetention) {
        self.requested_object_lock_retention = Some(retention);
    }
    fn install_requested_retain_until(&mut self, retain_until: Option<RequestedObjectLockRetainUntil>) {
        self.requested_object_lock_retain_until = retain_until;
    }
    fn install_requested_legal_hold(&mut self, legal_hold: RequestedObjectLegalHold) {
        self.requested_object_legal_hold = Some(legal_hold);
    }
    fn install_requested_object_tags(&mut self, tags: RequestedObjectTags) {
        self.requested_object_tags = Some(tags);
    }
    fn requested_copy_source(&self) -> Option<&RequestedCopySource> {
        self.requested_copy_source()
    }
    fn install_requested_copy_source(&mut self, source: RequestedCopySource) {
        self.requested_copy_source = Some(source);
    }
    fn install_copy_source_bucket_generation(&mut self, guard: CopySourceBucketGenerationGuard) {
        self.copy_source_bucket_generation = Some(guard);
    }
    fn policy_headers(&self) -> &HeaderMap {
        self.post_object_policy_headers().map_or(self.headers(), |headers| &headers.0)
    }
    fn object_tag_conditions(&self) -> Option<&ObjectTagConditions> {
        self.object_tag_conditions()
    }
    fn install_object_tag_conditions(&mut self, conditions: ObjectTagConditions) {
        self.install_object_tag_conditions(conditions);
    }
    fn publication_guards(&self) -> Option<&TableDataPlanePublicationGuards> {
        self.table_publication_guards()
    }
    fn install_publication_guards(&mut self, guards: TableDataPlanePublicationGuards) {
        self.install_table_publication_guards(guards);
    }
    fn install_table_list_access(&mut self, access: TableDataPlaneListAccess) {
        self.table_list_access = Some(access);
    }
    fn audit_denial(&self, auditor: &DenialAuditor, action: &Action, reason: &'static str) {
        auditor.audit_owned(&self.audit_view(), action, reason, DenialScope::Request);
    }
}

/// Authorizes an owned request through the same IAM, bucket and table policies as the legacy edge.
///
/// Like the legacy `authorize_request`, it first restores the signed `x-amz-meta-*`
/// query parameters into headers, so policy conditions and duplicate rejection see
/// the same request shape on both edges.
///
/// This is only the policy decision. It does **not** perform the composite checks
/// the legacy `S3Access` hooks run around their decision, such as the
/// `s3:BypassGovernanceRetention` authorization for a governance-bypass header or
/// the copy-source read authorization; a caller that needs them runs them itself.
///
/// # Errors
///
/// `InvalidArgument` when the signed query repeats a user metadata key;
/// `AccessDenied` when the decision for `action` depends on a request-body input
/// the envelope does not carry (see [`require_body_policy_inputs`]) or when the
/// policies deny the request.
pub(crate) async fn authorize_owned_request(
    req: &mut RequestEnvelope,
    action: Action,
    location_constraint: Option<String>,
) -> rustfs_s3_types::S3Result<()> {
    prepare_owned_authorization(req, action)?;
    authorize_request_core(req, action, location_constraint, false)
        .await
        .map_err(Into::into)
}

/// Authorizes one exact object key after its enclosing table-catalog operation has been authorized.
///
/// It applies the same preparation and has the same limits as [`authorize_owned_request`].
pub(crate) async fn authorize_owned_internal_object_request(
    req: &mut RequestEnvelope,
    action: Action,
) -> rustfs_s3_types::S3Result<()> {
    prepare_owned_authorization(req, action)?;
    authorize_request_core(req, action, None, true).await.map_err(Into::into)
}

fn prepare_owned_authorization(req: &mut RequestEnvelope, action: Action) -> rustfs_s3_types::S3Result<()> {
    crate::auth::hoist_signed_query_params(&mut req.headers, req.uri.query()).map_err(rustfs_s3_types::S3Error::from)?;
    require_body_policy_inputs(req, action)
}

/// Fails closed when the decision for `action` depends on a value the request
/// body supplies but the envelope does not carry it. Policy conditions would
/// otherwise fall back to client headers, which the stored value need not match
/// (header `OFF` with body `ON` would pass a legal-hold Deny).
fn require_body_policy_inputs(req: &RequestEnvelope, action: Action) -> rustfs_s3_types::S3Result<()> {
    let missing = match action {
        Action::S3Action(S3Action::PutObjectLegalHoldAction) => req.requested_object_legal_hold.is_none(),
        Action::S3Action(S3Action::PutObjectTaggingAction | S3Action::PutObjectVersionTaggingAction) => {
            req.requested_object_tags.is_none()
        }
        Action::S3Action(S3Action::PutObjectRetentionAction) => req.requested_object_lock_retention.is_none(),
        _ => false,
    } || (req.post_object.is_some() && req.post_object_policy_headers.is_none())
        || (req.headers.contains_key(COPY_SOURCE_HEADER) && req.requested_copy_source.is_none());
    if missing {
        return Err(rustfs_s3_types::s3_error!(
            AccessDenied,
            "request body policy input is missing for this action"
        ));
    }
    Ok(())
}

const COPY_SOURCE_HEADER: &str = "x-amz-copy-source";

impl RequestEnvelope {
    /// The request fields an audit entry for this request is built from.
    pub(crate) fn audit_view(&self) -> AuditRequestView<'_> {
        AuditRequestView::from_facts(
            self.headers(),
            self.uri(),
            self.req_info(),
            self.client_info(),
            self.remote_addr(),
            self.request_context(),
        )
    }
}

impl RequestEnvelope {
    /// Runs `handler` on this request and records its outcome as an audit-only
    /// entry for `op`, the owned form of the legacy edge's `audit_only_operation`.
    ///
    /// The entry is started before the request is handed over; a success is
    /// recorded with `success_status` and no added response headers. An operation
    /// that adds response headers builds its helper with
    /// [`OperationHelper::audit_only_from_view`] and calls
    /// [`OperationHelper::complete_owned`] itself.
    pub(crate) async fn audit_only_operation<O, F, Fut>(
        req: Self,
        op: S3Operation,
        success_status: StatusCode,
        handler: F,
    ) -> rustfs_s3_types::S3Result<O>
    where
        F: FnOnce(Self) -> Fut,
        Fut: Future<Output = rustfs_s3_types::S3Result<O>>,
    {
        let helper = OperationHelper::audit_only_from_view(&req.audit_view(), op);
        let result = handler(req).await;
        let _helper = helper.complete_owned(&result, success_status, &HeaderMap::new());
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authorization_payloads_move_from_extensions_into_named_fields() {
        let mut extensions = Extensions::new();
        extensions.insert(MultipartStagingOperation);
        extensions.insert(StagedMultipartPartAuthorization);
        extensions.insert(RequestedObjectLockRetention {
            mode: Some("COMPLIANCE".to_owned()),
            retain_until: None,
        });
        let retain_until = time::OffsetDateTime::from_unix_timestamp(2_000_000_000).expect("valid timestamp");
        extensions.insert(RequestedObjectLockRetainUntil(retain_until));
        let head = || RequestHead {
            method: Method::PUT,
            uri: "/bucket/key".parse().expect("valid URI"),
            headers: HeaderMap::new(),
            credentials: None,
            region: None,
            service: None,
        };

        let envelope = RequestEnvelope::from_parts(head(), extensions);
        let empty = RequestEnvelope::from_parts(head(), Extensions::new());

        assert_eq!(
            (
                envelope.multipart_staging_operation().is_some(),
                envelope.staged_multipart_part_authorization().is_some(),
                envelope
                    .requested_object_lock_retention()
                    .and_then(|value| value.mode.as_deref()),
                envelope.requested_object_lock_retain_until().map(|value| value.0),
                envelope.residual_extensions().is_empty(),
                empty.multipart_staging_operation().is_none(),
                empty.staged_multipart_part_authorization().is_none(),
                empty.requested_object_lock_retention().is_none(),
                empty.requested_object_lock_retain_until().is_none(),
            ),
            (true, true, Some("COMPLIANCE"), Some(retain_until), true, true, true, true, true),
        );
    }

    #[test]
    fn request_body_policy_payloads_move_from_extensions_into_named_fields() {
        let mut form_headers = HeaderMap::new();
        form_headers.insert("x-amz-tagging", http::HeaderValue::from_static("from=form"));
        let legal_hold = RequestedObjectLegalHold(Some("ON".to_owned()));
        let tags = RequestedObjectTags(vec![("body".to_owned(), "xml".to_owned())]);
        let mut extensions = Extensions::new();
        extensions.insert(PostObjectPolicyHeaders(form_headers));
        extensions.insert(legal_hold);
        extensions.insert(tags);
        extensions.insert(RequestedCopySource("source-bucket/source-key".to_owned()));
        let head = || RequestHead {
            method: Method::PUT,
            uri: "/bucket/key".parse().expect("valid URI"),
            headers: HeaderMap::new(),
            credentials: None,
            region: None,
            service: None,
        };

        let envelope = RequestEnvelope::from_parts(head(), extensions);
        let empty = RequestEnvelope::from_parts(head(), Extensions::new());

        assert_eq!(
            (
                envelope
                    .post_object_policy_headers()
                    .and_then(|headers| headers.0.get("x-amz-tagging"))
                    .and_then(|value| value.to_str().ok()),
                envelope.requested_object_legal_hold().and_then(|hold| hold.0.as_deref()),
                envelope
                    .requested_object_tags()
                    .and_then(|tags| tags.0.first())
                    .map(|(key, _)| key.as_str()),
                envelope.requested_copy_source().map(|source| source.0.as_str()),
                envelope.residual_extensions().is_empty(),
                empty.post_object_policy_headers().is_none(),
                empty.requested_object_legal_hold().is_none(),
                empty.requested_object_tags().is_none(),
                empty.requested_copy_source().is_none(),
            ),
            (
                Some("from=form"),
                Some("ON"),
                Some("body"),
                Some("source-bucket/source-key"),
                true,
                true,
                true,
                true,
                true
            ),
        );
    }

    #[test]
    fn an_empty_handed_over_secret_is_refused() {
        assert!(RequestCredentials::new("AKIAENVELOPE".to_owned(), String::new(), None).is_err());
    }

    #[test]
    fn an_absent_secret_matches_no_stored_secret() {
        let kept = RequestCredentials::without_secret_key("AKIAENVELOPE".to_owned(), None);
        assert_eq!(kept.secret_key(), None);
        assert!(!kept.secret_key_matches(""), "absent must not equal an empty stored secret");
        assert!(!kept.secret_key_matches("stored-secret"));
        let handed = RequestCredentials::new("AKIAENVELOPE".to_owned(), "stored-secret".to_owned(), None).expect("secret");
        assert!(!handed.secret_key_matches(""));
        assert!(!handed.secret_key_matches("other-secret"));
        assert!(handed.secret_key_matches("stored-secret"));
    }

    #[test]
    fn a_reader_of_an_absent_secret_fails_closed() {
        let kept = RequestCredentials::without_secret_key("AKIAENVELOPE".to_owned(), Some("token".to_owned()));
        assert!(kept.require_secret_key().is_err());
        let handed = RequestCredentials::new("AKIAENVELOPE".to_owned(), "s".to_owned(), None).expect("secret");
        assert_eq!(handed.require_secret_key().ok(), Some("s"));
    }

    #[test]
    fn kept_secret_debug_prints_neither_secret_nor_token() {
        let kept = RequestCredentials::without_secret_key("AKIAENVELOPE".to_owned(), Some("envelope-session-token".to_owned()));
        let debug = format!("{kept:?}");
        assert!(debug.contains("AKIAENVELOPE"), "{debug}");
        assert!(!debug.contains("envelope-session-token"), "{debug}");
    }

    #[test]
    fn request_credentials_debug_redacts_secret_and_session_token() {
        let credentials = RequestCredentials::new(
            "AKIAENVELOPE".to_owned(),
            "envelope-secret-value".to_owned(),
            Some("envelope-session-token".to_owned()),
        )
        .expect("non-empty secret");

        let debug = format!("{credentials:?}");

        assert!(debug.contains("AKIAENVELOPE"), "{debug}");
        assert!(!debug.contains("envelope-secret-value"), "{debug}");
        assert!(!debug.contains("envelope-session-token"), "{debug}");
    }
}
