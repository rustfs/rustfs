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
use crate::app::storage_api::context::{
    BucketConfigMutationSnapshot, BucketGenerationGuard, CopySourceBucketGenerationGuard, ObjectTagConditions,
    OdmReadGenerationGuard, PendingDeleteBucketGenerationGuard, PostObjectRequestMarker, ReqInfo, RequestContext,
    TableDataPlaneListAccess, TableDataPlanePublicationGuards,
};
use crate::auth::{VerifiedPresignedRequest, VerifiedSigV4Request};
use crate::shared_types::RemoteAddr;
use http::{Extensions, HeaderMap, Method, Uri};
use rustfs_trusted_proxies::ClientInfo;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use zeroize::Zeroizing;

/// The identity a request was authenticated with.
///
/// The secret key and the session token are bearer material: the type has no
/// `PartialEq`, and its `Debug` output redacts both.
#[derive(Clone)]
pub struct RequestCredentials {
    access_key: String,
    secret_key: Zeroizing<String>,
    session_token: Option<Zeroizing<String>>,
}

impl RequestCredentials {
    pub(crate) fn new(access_key: String, secret_key: String, session_token: Option<String>) -> Self {
        Self {
            access_key,
            secret_key: Zeroizing::new(secret_key),
            session_token: session_token.map(Zeroizing::new),
        }
    }

    pub fn access_key(&self) -> &str {
        &self.access_key
    }

    /// Never log this value or compare it with `==`.
    pub fn secret_key(&self) -> &str {
        &self.secret_key
    }

    pub fn session_token(&self) -> Option<&str> {
        self.session_token.as_deref().map(String::as_str)
    }
}

impl fmt::Debug for RequestCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestCredentials")
            .field("access_key", &self.access_key)
            .field("secret_key", &"<redacted>")
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
    request_context: Option<RequestContext>,
    server_context: Option<Arc<ServerContextSlot>>,
    body_read_control: Option<BodyReadControl>,
    // Attached by routing and signature verification.
    verified_sigv4: Option<VerifiedSigV4Request>,
    verified_presigned: Option<VerifiedPresignedRequest>,
    website_read: Option<WebsiteRead>,
    post_object: Option<PostObjectRequestMarker>,
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
            request_context: extensions.remove(),
            server_context: extensions.remove(),
            body_read_control: extensions.remove(),
            verified_sigv4: extensions.remove(),
            verified_presigned: extensions.remove(),
            website_read: extensions.remove(),
            post_object: extensions.remove(),
            req_info: extensions.remove(),
            bucket_generation: extensions.remove(),
            pending_delete_bucket_generation: extensions.remove(),
            copy_source_bucket_generation: extensions.remove(),
            odm_read_generation: extensions.remove(),
            bucket_config_mutation: extensions.remove(),
            table_publication_guards: extensions.remove(),
            table_list_access: extensions.remove(),
            object_tag_conditions: extensions.remove(),
            residual_extensions: extensions,
        }
    }

    pub fn method(&self) -> &Method {
        &self.method
    }

    pub fn uri(&self) -> &Uri {
        &self.uri
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

    pub(crate) fn table_list_access(&self) -> Option<&TableDataPlaneListAccess> {
        self.table_list_access.as_ref()
    }

    pub(crate) fn object_tag_conditions(&self) -> Option<&ObjectTagConditions> {
        self.object_tag_conditions.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_credentials_debug_redacts_secret_and_session_token() {
        let credentials = RequestCredentials::new(
            "AKIAENVELOPE".to_owned(),
            "envelope-secret-value".to_owned(),
            Some("envelope-session-token".to_owned()),
        );

        let debug = format!("{credentials:?}");

        assert!(debug.contains("AKIAENVELOPE"), "{debug}");
        assert!(!debug.contains("envelope-secret-value"), "{debug}");
        assert!(!debug.contains("envelope-session-token"), "{debug}");
    }
}
