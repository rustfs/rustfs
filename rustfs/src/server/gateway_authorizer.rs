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

//! The gateway stack's early authorizer (rustfs/backlog#2761).
//!
//! Responsible for: deciding a routed operation with the same authorization core the handler's
//! legacy hook calls, early enough for the gateway to refuse it before the handler runs.
//! Not responsible for: replacing that hook. The handler still runs it. Body-derived conditions,
//! the table publication fence, and any guard the hook installs stay in the handler.
//! Upstream: [`super::gateway_stack::early_gate`]. Downstream: `storage::authorization` and the
//! owned hooks in `storage::access_owned` that do not install a guard.
//!
//! A decision of [`Decision::Allow`] is a real core allow, an explicit "the legacy hook does not
//! check" row, or a remainder the handler still decides. A backend failure is indeterminate.
//! An operation the table serves with no row is denied.

use crate::app::{RequestCredentials, RequestEnvelope, RequestHead};
use crate::auth::{get_session_token, keystone_is_owner};
use crate::runtime_sources::ServerContextSlot;
use crate::server::gateway_handlers::{OperationTable, metadata_listing, serves};
use crate::server::gateway_stack::OwnedKeystoneIdentity;
use crate::shared_types::{NativeTlsTransport, RemoteAddr};
use crate::storage::access_owned::bucket_core::authorize_delete_bucket;
use crate::storage::access_owned::object_misc::{authorize_select_object_content, decide_delete_object};
use crate::storage::access_owned::object_read::authorize_get_object_attributes;
use crate::storage::authorization::{
    AccessMarkers, AuthorizationRequest, BucketConfigMutationSnapshot, BucketGenerationGuard, CopySourceBucketGenerationGuard,
    ObjectTagConditions, OdmReadGenerationGuard, PendingDeleteBucketGenerationGuard, TableDataPlaneListAccess,
    TableDataPlanePublicationGuards, VerifiedSignature, authorize_bucket_request, authorize_object_request,
    authorize_replication_only_put_headers, authorize_request, authorize_staged_multipart_part, authorize_write_etag_condition,
    has_write_offset_bytes_header, load_bucket_generation, missing_req_info_error, resolve_req_info,
    set_requested_object_lock_retain_until, table_data_plane_content_mutation,
};
use crate::storage::authorization_actions::{versioned_read_action, versioned_tagging_action};
use crate::storage::denial_audit::DenialAuditor;
use crate::storage::options::destination_version_id;
use crate::storage::req_info::ReqInfo;
use crate::storage::request_context::RequestContext;
use crate::storage::request_markers::{
    PostObjectPolicyHeaders, RequestedCopySource, RequestedObjectLegalHold, RequestedObjectLockRetainUntil,
    RequestedObjectLockRetention, RequestedObjectTags,
};
use http::{Extensions, HeaderMap, Method, Uri};
use rustfs_gateway::RequestContext as GatewayRequestContext;
use rustfs_gateway::dto;
use rustfs_gateway::{
    AuthSchemeRef, Authorizer, AuthzRequest, BoxFuture, Decision, IdentityOrigin, InputAuthzRequest, InputDecisions, Operation,
};
use rustfs_gateway_dialect_rustfs_admin::ops::list_object_versions_with_metadata::NAME as LIST_VERSIONS_WITH_METADATA;
use rustfs_gateway_dialect_rustfs_admin::ops::list_objects_v2_with_metadata::NAME as LIST_OBJECTS_WITH_METADATA;
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_types::{S3ErrorCode, S3Result};
use rustfs_trusted_proxies::ClientInfo;
use rustfs_utils::http::{
    AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
    SUFFIX_INCLUDE_DELETED, SUFFIX_SOURCE_REPLICATION_CHECK, get_header,
};
use std::sync::Arc;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// The early gate over `table` and the two metadata listings.
pub(crate) fn early_gate(table: OperationTable, server_ctx: Arc<ServerContextSlot>) -> EarlyAuthorizer {
    EarlyAuthorizer { table, server_ctx }
}

/// Decides routed operations by the legacy hook's core. See the module docs.
pub(crate) struct EarlyAuthorizer {
    table: OperationTable,
    server_ctx: Arc<ServerContextSlot>,
}

impl Authorizer for EarlyAuthorizer {
    fn authorize_route<'a>(
        &'a self,
        context: &'a GatewayRequestContext<'a>,
        request: &'a AuthzRequest<'a>,
    ) -> BoxFuture<'a, Decision> {
        Box::pin(self.decide_route(context, request))
    }

    fn authorize_copy_source<'a>(
        &'a self,
        _context: &'a GatewayRequestContext<'a>,
        _route: &'a AuthzRequest<'a>,
        _source: &'a AuthzRequest<'a>,
    ) -> BoxFuture<'a, Decision> {
        // The handler hook still authorizes the copy source. The default indeterminate answer
        // would refuse every copy when source-first mode is on.
        Box::pin(async { Decision::Allow })
    }

    fn authorize_input<'a>(
        &'a self,
        _context: &'a GatewayRequestContext<'a>,
        request: &'a InputAuthzRequest<'a>,
    ) -> BoxFuture<'a, InputDecisions> {
        // Derived resources, including each DeleteObjects key, stay with the handler hook.
        // Allowing the visibility question keeps a missing GetObject as NoSuchKey.
        let decisions = request.decide_all(Decision::Allow, |_| Decision::Allow);
        Box::pin(async move { decisions })
    }
}

impl EarlyAuthorizer {
    async fn decide_route(&self, context: &GatewayRequestContext<'_>, request: &AuthzRequest<'_>) -> Decision {
        let operation = request.operation;
        let served = serves(self.table, operation) || metadata_listing::NAMES.contains(&operation);
        if let Some(decision) = decision_before_evaluation(served, operation) {
            return decision;
        }
        let (Some(header_view), Some(raw_query)) = (context.headers(), context.raw_query()) else {
            return Decision::Indeterminate;
        };
        let Some(eval) = evaluation(operation) else {
            return Decision::Indeterminate;
        };
        let Some(bucket) = request.bucket.map(|bucket| bucket.as_str()) else {
            return Decision::Indeterminate;
        };
        let key = match (eval.needs_key(), request.key.map(|key| key.as_str())) {
            (false, _) => "",
            (true, Some(key)) => key,
            (true, None) => return Decision::Indeterminate,
        };
        if eval.uses_version() && version_id_count(raw_query) > 1 {
            return Decision::Allow;
        }
        let version = first_version_id(raw_query);
        let mut prepared = match prepare_request(context, request, header_view, raw_query, &self.server_ctx) {
            Ok(prepared) => prepared,
            Err(decision) => return decision,
        };
        if let Err(decision) = install_caller(&mut prepared, &self.server_ctx).await {
            return decision;
        }
        let mut route = RouteRequest {
            inner: &mut prepared.envelope,
            evaluate_table: true,
        };
        decision_of(evaluate(&mut route, eval, bucket, key, version.as_deref()).await)
    }
}

/// Who the gate resolves, classified the way [`super::gateway_handlers::access::admit`] does.
enum Caller {
    Anonymous,
    Signed {
        access_key: String,
    },
    Keystone {
        credentials: Box<rustfs_credentials::Credentials>,
        had_session_token: bool,
    },
}

struct PreparedRequest {
    envelope: RequestEnvelope,
    caller: Caller,
    session_token: Option<String>,
}

fn prepare_request(
    context: &GatewayRequestContext<'_>,
    request: &AuthzRequest<'_>,
    header_view: rustfs_gateway_http::HeaderView<'_>,
    raw_query: &str,
    server_ctx: &Arc<ServerContextSlot>,
) -> Result<PreparedRequest, Decision> {
    let mut headers = HeaderMap::new();
    for (name, value) in header_view.iter_raw() {
        headers.append(name.clone(), value.clone());
    }
    let uri = request_uri(raw_query).map_err(|()| Decision::Indeterminate)?;
    let session_token = get_session_token(&uri, &headers).map(str::to_owned);
    let caller = classify_caller(context, request)?;
    let credentials = match &caller {
        Caller::Anonymous => None,
        Caller::Signed { access_key } => Some(RequestCredentials::without_secret_key(access_key.clone(), session_token.clone())),
        Caller::Keystone { credentials, .. } => {
            Some(RequestCredentials::without_secret_key(credentials.access_key.clone(), None))
        }
    };
    let mut extensions = Extensions::new();
    copy_extension::<Option<RemoteAddr>>(context, &mut extensions);
    copy_extension::<ClientInfo>(context, &mut extensions);
    copy_extension::<NativeTlsTransport>(context, &mut extensions);
    copy_extension::<RequestContext>(context, &mut extensions);
    extensions.insert(Arc::clone(server_ctx));
    AccessMarkers::new(request.operation, verified_signature(context.auth_scheme())).install(&mut extensions);
    let scope = context.verified_scope();
    let head = RequestHead {
        method: Method::GET,
        uri,
        headers,
        credentials,
        region: scope.map(|scope| scope.region().to_owned()),
        service: scope.map(|scope| scope.service().to_owned()),
    };
    Ok(PreparedRequest {
        envelope: RequestEnvelope::from_parts(head, extensions),
        caller,
        session_token,
    })
}

fn classify_caller(context: &GatewayRequestContext<'_>, request: &AuthzRequest<'_>) -> Result<Caller, Decision> {
    let host_token = context.auth_scheme() == AuthSchemeRef::HeaderToken
        || request
            .identity
            .is_some_and(|identity| identity.origin() == IdentityOrigin::HostToken);
    if host_token {
        let Some(identity) = request.identity else {
            return Err(Decision::Deny);
        };
        let Some(owned) = context.server_extensions().get::<OwnedKeystoneIdentity>() else {
            return Err(Decision::Deny);
        };
        if identity.origin() != IdentityOrigin::HostToken || identity.access_key_id() != owned.credentials().access_key {
            return Err(Decision::Deny);
        }
        return Ok(Caller::Keystone {
            credentials: Box::new(owned.credentials().clone()),
            had_session_token: owned.had_session_token(),
        });
    }
    match request.identity {
        Some(identity) => Ok(Caller::Signed {
            access_key: identity.access_key_id().to_owned(),
        }),
        None => Ok(Caller::Anonymous),
    }
}

async fn install_caller(prepared: &mut PreparedRequest, server_ctx: &Arc<ServerContextSlot>) -> Result<(), Decision> {
    let request_context = prepared.envelope.request_context().cloned();
    let resolved = match &prepared.caller {
        Caller::Signed { access_key } => {
            resolve_req_info(
                server_ctx,
                Some(access_key.as_str()),
                prepared.session_token.as_deref().unwrap_or(""),
                request_context,
            )
            .await
        }
        Caller::Anonymous | Caller::Keystone { .. } => resolve_req_info(server_ctx, None, "", request_context).await,
    };
    let mut info = match resolved {
        Ok(info) => info,
        Err(error) => return Err(decision_of(Err(error))),
    };
    if let Caller::Keystone {
        credentials,
        had_session_token,
    } = &prepared.caller
    {
        info.is_owner = keystone_is_owner(credentials);
        info.cred = Some(credentials.as_ref().clone());
        info.keystone_session_present = Some(*had_session_token);
    }
    prepared.envelope.install_req_info(info);
    Ok(())
}

fn verified_signature(scheme: AuthSchemeRef) -> VerifiedSignature {
    match scheme {
        AuthSchemeRef::SigV4Header => VerifiedSignature {
            sigv4: true,
            presigned: false,
        },
        AuthSchemeRef::SigV4Presigned => VerifiedSignature {
            sigv4: true,
            presigned: true,
        },
        _ => VerifiedSignature::default(),
    }
}

fn request_uri(raw_query: &str) -> Result<Uri, ()> {
    let target = if raw_query.is_empty() {
        "/".to_owned()
    } else {
        format!("/?{raw_query}")
    };
    target.parse().map_err(|_| ())
}

fn copy_extension<T>(context: &GatewayRequestContext<'_>, extensions: &mut Extensions)
where
    T: Clone + Send + Sync + 'static,
{
    if let Some(value) = context.server_extensions().get::<T>() {
        extensions.insert(value.clone());
    }
}

fn arm(route: &mut RouteRequest<'_>, action: Action) {
    route.evaluate_table = !table_data_plane_content_mutation(action);
}

async fn evaluate(route: &mut RouteRequest<'_>, eval: Eval, bucket: &str, key: &str, version: Option<&str>) -> S3Result<()> {
    match eval {
        Eval::Bucket(action) => {
            let action = Action::S3Action(action);
            arm(route, action);
            authorize_bucket_request(route, bucket, action).await
        }
        Eval::DeleteBucket => {
            route.evaluate_table = true;
            authorize_delete_bucket(route, bucket).await
        }
        Eval::DeleteBucketTagging => {
            let action = Action::S3Action(S3Action::PutBucketTaggingAction);
            arm(route, action);
            rustfs_iam::sys::with_bucket_tag_deletion(authorize_bucket_request(route, bucket, action)).await
        }
        Eval::Listing(action) => evaluate_listing(route, bucket, action).await,
        Eval::PutObject => evaluate_put_object(route, bucket, key, version).await,
        Eval::CreateMultipartUpload => evaluate_create_multipart(route, bucket, key, version).await,
        Eval::CompleteMultipartUpload => evaluate_complete_multipart(route, bucket, key).await,
        Eval::DeleteObject => evaluate_delete_object(route, bucket, key, version).await,
        Eval::RestoreObject => {
            let action = Action::S3Action(S3Action::RestoreObjectAction);
            arm(route, action);
            authorize_object_request(route, bucket, key, version, action).await
        }
        Eval::HeadObject => evaluate_head_object(route, bucket, key, version).await,
        Eval::GetObject => authorize_versioned_read(route, bucket, key, version).await,
        Eval::GetObjectAttributes => {
            route.evaluate_table = true;
            authorize_get_object_attributes(route, bucket, key, version).await
        }
        Eval::SelectObjectContent => {
            route.evaluate_table = true;
            authorize_select_object_content(route, bucket, key).await
        }
        Eval::FixedObject(action) => {
            let action = Action::S3Action(action);
            arm(route, action);
            authorize_object_request(route, bucket, key, version, action).await
        }
        Eval::Tagging(action) => {
            let action = versioned_tagging_action(action, version);
            arm(route, action);
            authorize_object_request(route, bucket, key, version, action).await
        }
        Eval::AbortMultipartUpload => {
            let action = Action::S3Action(S3Action::AbortMultipartUploadAction);
            arm(route, action);
            authorize_object_request(route, bucket, key, None, action).await
        }
        Eval::UploadPart => {
            bind_target(route, bucket, Some(key), None)?;
            route.evaluate_table = true;
            authorize_staged_multipart_part(route).await
        }
        Eval::ListParts => {
            let action = Action::S3Action(S3Action::ListMultipartUploadPartsAction);
            arm(route, action);
            authorize_object_request(route, bucket, key, None, action).await
        }
    }
}

async fn evaluate_listing(route: &mut RouteRequest<'_>, bucket: &str, action: S3Action) -> S3Result<()> {
    let primary = Action::S3Action(action);
    arm(route, primary);
    authorize_bucket_request(route, bucket, primary).await?;
    if action == S3Action::ListBucketAction && get_header(route.headers(), SUFFIX_INCLUDE_DELETED).as_deref() == Some("true") {
        let versions = Action::S3Action(S3Action::ListBucketVersionsAction);
        arm(route, versions);
        authorize_request(route, versions).await?;
    }
    Ok(())
}

async fn evaluate_put_object(route: &mut RouteRequest<'_>, bucket: &str, key: &str, version: Option<&str>) -> S3Result<()> {
    let retain = parsed_retain_until(route.headers());
    // The handler reports these before it asks policy. Deciding here would turn them into a deny.
    if matches!(retain, RetainUntil::Invalid) || has_write_offset_bytes_header(route.headers()) {
        return Ok(());
    }
    let version = destination_version_id(version, route.headers());
    bind_target(route, bucket, Some(key), version.as_deref())?;
    set_requested_object_lock_retain_until(route, retain.date());
    let put = Action::S3Action(S3Action::PutObjectAction);
    arm(route, put);
    authorize_request(route, put).await?;
    route.evaluate_table = true;
    authorize_write_etag_condition(route, false).await?;
    route.evaluate_table = false;
    authorize_replication_only_put_headers(route).await?;
    if header_present(route.headers(), AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER) {
        let action = Action::S3Action(S3Action::PutObjectLegalHoldAction);
        arm(route, action);
        authorize_request(route, action).await?;
    }
    if header_present(route.headers(), AMZ_OBJECT_LOCK_MODE_LOWER) || retain.date().is_some() {
        let action = Action::S3Action(S3Action::PutObjectRetentionAction);
        arm(route, action);
        authorize_request(route, action).await?;
    }
    Ok(())
}

async fn evaluate_create_multipart(route: &mut RouteRequest<'_>, bucket: &str, key: &str, version: Option<&str>) -> S3Result<()> {
    let retain = parsed_retain_until(route.headers());
    if matches!(retain, RetainUntil::Invalid) {
        return Ok(());
    }
    let version = destination_version_id(version, route.headers());
    bind_target(route, bucket, Some(key), version.as_deref())?;
    set_requested_object_lock_retain_until(route, retain.date());
    let put = Action::S3Action(S3Action::PutObjectAction);
    arm(route, put);
    authorize_request(route, put).await?;
    route.evaluate_table = false;
    authorize_replication_only_put_headers(route).await?;
    // Legal-hold and retention on this operation are decided with the decoded input, in the handler.
    Ok(())
}

async fn evaluate_complete_multipart(route: &mut RouteRequest<'_>, bucket: &str, key: &str) -> S3Result<()> {
    bind_target(route, bucket, Some(key), None)?;
    let put = Action::S3Action(S3Action::PutObjectAction);
    arm(route, put);
    authorize_request(route, put).await?;
    route.evaluate_table = true;
    authorize_write_etag_condition(route, false).await?;
    route.evaluate_table = false;
    authorize_replication_only_put_headers(route).await
}

async fn evaluate_delete_object(route: &mut RouteRequest<'_>, bucket: &str, key: &str, version: Option<&str>) -> S3Result<()> {
    // One flag covers every action this helper asks. Content-mutation checks would take the
    // publication fence, so the whole call leaves the table plane to the handler.
    route.evaluate_table = false;
    let server_ctx = route.server_context().cloned().ok_or_else(missing_req_info_error)?;
    let generation = load_bucket_generation(server_ctx.as_ref(), route, bucket).await;
    match decide_delete_object(route, bucket, key, version, generation).await {
        Ok(_guard) => Ok(()),
        Err(error) => Err(error),
    }
}

async fn evaluate_head_object(route: &mut RouteRequest<'_>, bucket: &str, key: &str, version: Option<&str>) -> S3Result<()> {
    authorize_versioned_read(route, bucket, key, version).await?;
    if get_header(route.headers(), SUFFIX_SOURCE_REPLICATION_CHECK).as_deref() == Some("true") {
        let action = Action::S3Action(S3Action::ReplicateObjectAction);
        arm(route, action);
        authorize_request(route, action).await?;
    }
    Ok(())
}

async fn authorize_versioned_read(route: &mut RouteRequest<'_>, bucket: &str, key: &str, version: Option<&str>) -> S3Result<()> {
    let action = versioned_read_action(version);
    arm(route, action);
    authorize_object_request(route, bucket, key, version, action).await
}

fn bind_target(route: &mut RouteRequest<'_>, bucket: &str, key: Option<&str>, version: Option<&str>) -> S3Result<()> {
    let info = route.req_info_mut().ok_or_else(missing_req_info_error)?;
    info.bucket = Some(bucket.to_owned());
    info.object = key.map(str::to_owned);
    info.version_id = version.map(str::to_owned);
    Ok(())
}

#[derive(Clone, Copy)]
enum RetainUntil {
    Absent,
    Invalid,
    Parsed(OffsetDateTime),
}

impl RetainUntil {
    fn date(self) -> Option<OffsetDateTime> {
        match self {
            Self::Parsed(date) => Some(date),
            Self::Absent | Self::Invalid => None,
        }
    }
}

fn parsed_retain_until(headers: &HeaderMap) -> RetainUntil {
    let Some(value) = headers.get(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER) else {
        return RetainUntil::Absent;
    };
    let Ok(text) = value.to_str() else {
        return RetainUntil::Invalid;
    };
    let text = text.trim();
    if text.is_empty() {
        return RetainUntil::Absent;
    }
    match OffsetDateTime::parse(text, &Rfc3339) {
        Ok(date) => RetainUntil::Parsed(date),
        Err(_) => RetainUntil::Invalid,
    }
}

fn header_present(headers: &HeaderMap, name: &str) -> bool {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| !value.trim().is_empty())
}

/// A temporary envelope whose table-plane bit the gate sets per action.
struct RouteRequest<'a> {
    inner: &'a mut RequestEnvelope,
    evaluate_table: bool,
}

impl AuthorizationRequest for RouteRequest<'_> {
    fn headers(&self) -> &HeaderMap {
        AuthorizationRequest::headers(self.inner)
    }
    fn headers_mut(&mut self) -> &mut HeaderMap {
        AuthorizationRequest::headers_mut(self.inner)
    }
    fn uri(&self) -> &http::Uri {
        AuthorizationRequest::uri(self.inner)
    }
    fn remote_addr(&self) -> Option<std::net::SocketAddr> {
        AuthorizationRequest::remote_addr(self.inner)
    }
    fn client_info(&self) -> Option<&ClientInfo> {
        AuthorizationRequest::client_info(self.inner)
    }
    fn server_context(&self) -> Option<&Arc<ServerContextSlot>> {
        AuthorizationRequest::server_context(self.inner)
    }
    fn req_info(&self) -> Option<&ReqInfo> {
        AuthorizationRequest::req_info(self.inner)
    }
    fn req_info_mut(&mut self) -> Option<&mut ReqInfo> {
        AuthorizationRequest::req_info_mut(self.inner)
    }
    fn request_context(&self) -> Option<&RequestContext> {
        AuthorizationRequest::request_context(self.inner)
    }
    fn bucket_generation(&self) -> Option<&BucketGenerationGuard> {
        AuthorizationRequest::bucket_generation(self.inner)
    }
    fn install_bucket_generation(&mut self, guard: BucketGenerationGuard) {
        AuthorizationRequest::install_bucket_generation(self.inner, guard);
    }
    fn pending_delete_bucket_generation(&self) -> Option<&PendingDeleteBucketGenerationGuard> {
        AuthorizationRequest::pending_delete_bucket_generation(self.inner)
    }
    fn install_pending_delete_bucket_generation(&mut self, guard: PendingDeleteBucketGenerationGuard) {
        AuthorizationRequest::install_pending_delete_bucket_generation(self.inner, guard);
    }
    fn copy_source_bucket_generation(&self) -> Option<&CopySourceBucketGenerationGuard> {
        AuthorizationRequest::copy_source_bucket_generation(self.inner)
    }
    fn odm_read_generation(&self) -> Option<&OdmReadGenerationGuard> {
        AuthorizationRequest::odm_read_generation(self.inner)
    }
    fn install_odm_read_generation(&mut self, guard: OdmReadGenerationGuard) {
        AuthorizationRequest::install_odm_read_generation(self.inner, guard);
    }
    fn bucket_config_mutation(&self) -> Option<&BucketConfigMutationSnapshot> {
        AuthorizationRequest::bucket_config_mutation(self.inner)
    }
    fn install_bucket_config_mutation(&mut self, snapshot: BucketConfigMutationSnapshot) {
        AuthorizationRequest::install_bucket_config_mutation(self.inner, snapshot);
    }
    #[cfg(test)]
    fn test_extensions(&self) -> &http::Extensions {
        AuthorizationRequest::test_extensions(self.inner)
    }
    fn internal_object_authorization(&self) -> bool {
        AuthorizationRequest::internal_object_authorization(self.inner)
    }
    fn multipart_staging(&self) -> bool {
        AuthorizationRequest::multipart_staging(self.inner)
    }
    fn staged_multipart_part(&self) -> bool {
        AuthorizationRequest::staged_multipart_part(self.inner)
    }
    fn set_staged_multipart_part(&mut self, staged: bool) {
        AuthorizationRequest::set_staged_multipart_part(self.inner, staged);
    }
    fn requested_retention(&self) -> Option<&RequestedObjectLockRetention> {
        AuthorizationRequest::requested_retention(self.inner)
    }
    fn requested_retain_until(&self) -> Option<&RequestedObjectLockRetainUntil> {
        AuthorizationRequest::requested_retain_until(self.inner)
    }
    fn requested_legal_hold(&self) -> Option<&RequestedObjectLegalHold> {
        AuthorizationRequest::requested_legal_hold(self.inner)
    }
    fn requested_object_tags(&self) -> Option<&RequestedObjectTags> {
        AuthorizationRequest::requested_object_tags(self.inner)
    }
    fn take_requested_object_tags(&mut self) -> Option<RequestedObjectTags> {
        AuthorizationRequest::take_requested_object_tags(self.inner)
    }
    fn install_requested_retention(&mut self, retention: RequestedObjectLockRetention) {
        AuthorizationRequest::install_requested_retention(self.inner, retention);
    }
    fn install_requested_retain_until(&mut self, retain_until: Option<RequestedObjectLockRetainUntil>) {
        AuthorizationRequest::install_requested_retain_until(self.inner, retain_until);
    }
    fn install_requested_legal_hold(&mut self, legal_hold: RequestedObjectLegalHold) {
        AuthorizationRequest::install_requested_legal_hold(self.inner, legal_hold);
    }
    fn install_requested_object_tags(&mut self, tags: RequestedObjectTags) {
        AuthorizationRequest::install_requested_object_tags(self.inner, tags);
    }
    fn requested_copy_source(&self) -> Option<&RequestedCopySource> {
        AuthorizationRequest::requested_copy_source(self.inner)
    }
    fn install_requested_copy_source(&mut self, source: RequestedCopySource) {
        AuthorizationRequest::install_requested_copy_source(self.inner, source);
    }
    fn install_copy_source_bucket_generation(&mut self, guard: CopySourceBucketGenerationGuard) {
        AuthorizationRequest::install_copy_source_bucket_generation(self.inner, guard);
    }
    fn policy_headers(&self) -> &HeaderMap {
        AuthorizationRequest::policy_headers(self.inner)
    }
    fn install_post_object_policy_headers(&mut self, headers: PostObjectPolicyHeaders) {
        AuthorizationRequest::install_post_object_policy_headers(self.inner, headers);
    }
    fn install_post_object_marker(&mut self) {
        AuthorizationRequest::install_post_object_marker(self.inner);
    }
    fn object_tag_conditions(&self) -> Option<&ObjectTagConditions> {
        AuthorizationRequest::object_tag_conditions(self.inner)
    }
    fn take_object_tag_conditions(&mut self) -> Option<ObjectTagConditions> {
        AuthorizationRequest::take_object_tag_conditions(self.inner)
    }
    fn install_object_tag_conditions(&mut self, conditions: ObjectTagConditions) {
        AuthorizationRequest::install_object_tag_conditions(self.inner, conditions);
    }
    fn publication_guards(&self) -> Option<&TableDataPlanePublicationGuards> {
        AuthorizationRequest::publication_guards(self.inner)
    }
    fn install_publication_guards(&mut self, guards: TableDataPlanePublicationGuards) {
        AuthorizationRequest::install_publication_guards(self.inner, guards);
    }
    fn install_table_list_access(&mut self, access: TableDataPlaneListAccess) {
        AuthorizationRequest::install_table_list_access(self.inner, access);
    }
    fn evaluates_table_data_plane(&self) -> bool {
        self.evaluate_table
    }
    fn audit_denial(&self, auditor: &DenialAuditor, action: &Action, reason: &'static str) {
        AuthorizationRequest::audit_denial(self.inner, auditor, action, reason);
    }
}

/// How much of the legacy hook this gate runs.
///
/// `Unchecked` is an empty hook. `Remainder` leaves the whole hook to the handler.
/// `Partial` runs the body-independent prefix and leaves the fenced or body-derived part.
/// `Full` runs the same decision the hook runs, without installing the hook's guard.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg(test)]
enum Class {
    Unchecked,
    Partial,
    Full,
}

#[derive(Clone, Copy)]
enum Row {
    Unchecked,
    Remainder,
    Partial(Eval),
    Full(Eval),
}

/// The check the gate can make from the route. A content mutation is marked so the publication
/// fence stays in the handler; that check cannot run on a temporary request without a false deny.
#[derive(Clone, Copy)]
enum Eval {
    Bucket(S3Action),
    DeleteBucket,
    DeleteBucketTagging,
    Listing(S3Action),
    PutObject,
    CreateMultipartUpload,
    CompleteMultipartUpload,
    DeleteObject,
    RestoreObject,
    HeadObject,
    GetObject,
    GetObjectAttributes,
    SelectObjectContent,
    FixedObject(S3Action),
    Tagging(S3Action),
    AbortMultipartUpload,
    UploadPart,
    ListParts,
}

impl Eval {
    fn needs_key(self) -> bool {
        matches!(
            self,
            Self::PutObject
                | Self::CreateMultipartUpload
                | Self::CompleteMultipartUpload
                | Self::DeleteObject
                | Self::RestoreObject
                | Self::HeadObject
                | Self::GetObject
                | Self::GetObjectAttributes
                | Self::SelectObjectContent
                | Self::FixedObject(_)
                | Self::Tagging(_)
                | Self::AbortMultipartUpload
                | Self::UploadPart
                | Self::ListParts
        )
    }

    fn uses_version(self) -> bool {
        matches!(
            self,
            Self::PutObject
                | Self::CreateMultipartUpload
                | Self::DeleteObject
                | Self::RestoreObject
                | Self::HeadObject
                | Self::GetObject
                | Self::GetObjectAttributes
                | Self::FixedObject(_)
                | Self::Tagging(_)
        )
    }
}

fn authorization_row(operation: &str) -> Option<Row> {
    Some(match operation {
        dto::ListBuckets::NAME | dto::GetObjectTorrent::NAME => Row::Unchecked,
        dto::CreateBucket::NAME
        | dto::DeleteObjects::NAME
        | dto::PostObject::NAME
        | dto::PutObjectTagging::NAME
        | dto::PutObjectLegalHold::NAME
        | dto::PutObjectRetention::NAME
        | dto::CopyObject::NAME
        | dto::UploadPartCopy::NAME => Row::Remainder,
        dto::PutObject::NAME => Row::Partial(Eval::PutObject),
        dto::DeleteObject::NAME => Row::Partial(Eval::DeleteObject),
        dto::RestoreObject::NAME => Row::Partial(Eval::RestoreObject),
        dto::CreateMultipartUpload::NAME => Row::Partial(Eval::CreateMultipartUpload),
        dto::CompleteMultipartUpload::NAME => Row::Partial(Eval::CompleteMultipartUpload),
        dto::HeadObject::NAME => Row::Partial(Eval::HeadObject),
        dto::ListObjects::NAME | dto::ListObjectsV2::NAME | LIST_OBJECTS_WITH_METADATA => {
            Row::Partial(Eval::Listing(S3Action::ListBucketAction))
        }
        dto::ListObjectVersions::NAME | LIST_VERSIONS_WITH_METADATA => {
            Row::Partial(Eval::Listing(S3Action::ListBucketVersionsAction))
        }
        dto::ListMultipartUploads::NAME => Row::Partial(Eval::Listing(S3Action::ListBucketMultipartUploadsAction)),
        dto::HeadBucket::NAME => Row::Full(Eval::Bucket(S3Action::ListBucketAction)),
        dto::GetBucketLocation::NAME => Row::Full(Eval::Bucket(S3Action::GetBucketLocationAction)),
        dto::GetBucketAccelerateConfiguration::NAME | dto::GetBucketRequestPayment::NAME | dto::GetBucketWebsite::NAME => {
            Row::Full(Eval::Bucket(S3Action::GetBucketPolicyAction))
        }
        dto::GetBucketLogging::NAME => Row::Full(Eval::Bucket(S3Action::GetBucketLoggingAction)),
        dto::PutBucketLogging::NAME => Row::Full(Eval::Bucket(S3Action::PutBucketLoggingAction)),
        dto::PutBucketAccelerateConfiguration::NAME
        | dto::PutBucketRequestPayment::NAME
        | dto::PutBucketWebsite::NAME
        | dto::DeleteBucketWebsite::NAME => Row::Full(Eval::Bucket(S3Action::PutBucketPolicyAction)),
        dto::DeleteBucket::NAME => Row::Full(Eval::DeleteBucket),
        dto::GetBucketVersioning::NAME => Row::Full(Eval::Bucket(S3Action::GetBucketVersioningAction)),
        dto::PutBucketVersioning::NAME => Row::Full(Eval::Bucket(S3Action::PutBucketVersioningAction)),
        dto::GetBucketPolicy::NAME => Row::Full(Eval::Bucket(S3Action::GetBucketPolicyAction)),
        dto::PutBucketPolicy::NAME => Row::Full(Eval::Bucket(S3Action::PutBucketPolicyAction)),
        dto::DeleteBucketPolicy::NAME => Row::Full(Eval::Bucket(S3Action::DeleteBucketPolicyAction)),
        dto::GetBucketPolicyStatus::NAME => Row::Full(Eval::Bucket(S3Action::GetBucketPolicyStatusAction)),
        dto::GetBucketAcl::NAME => Row::Full(Eval::Bucket(S3Action::GetBucketAclAction)),
        dto::PutBucketAcl::NAME => Row::Full(Eval::Bucket(S3Action::PutBucketAclAction)),
        dto::GetBucketNotificationConfiguration::NAME => Row::Full(Eval::Bucket(S3Action::GetBucketNotificationAction)),
        dto::PutBucketNotificationConfiguration::NAME => Row::Full(Eval::Bucket(S3Action::PutBucketNotificationAction)),
        dto::GetBucketCors::NAME => Row::Full(Eval::Bucket(S3Action::GetBucketCorsAction)),
        dto::PutBucketCors::NAME => Row::Full(Eval::Bucket(S3Action::PutBucketCorsAction)),
        dto::DeleteBucketCors::NAME => Row::Full(Eval::Bucket(S3Action::DeleteBucketCorsAction)),
        dto::GetBucketEncryption::NAME => Row::Full(Eval::Bucket(S3Action::GetBucketEncryptionAction)),
        dto::PutBucketEncryption::NAME | dto::DeleteBucketEncryption::NAME => {
            Row::Full(Eval::Bucket(S3Action::PutBucketEncryptionAction))
        }
        dto::GetBucketTagging::NAME => Row::Full(Eval::Bucket(S3Action::GetBucketTaggingAction)),
        dto::PutBucketTagging::NAME => Row::Full(Eval::Bucket(S3Action::PutBucketTaggingAction)),
        dto::DeleteBucketTagging::NAME => Row::Full(Eval::DeleteBucketTagging),
        dto::GetPublicAccessBlock::NAME => Row::Full(Eval::Bucket(S3Action::GetBucketPublicAccessBlockAction)),
        dto::PutPublicAccessBlock::NAME => Row::Full(Eval::Bucket(S3Action::PutBucketPublicAccessBlockAction)),
        dto::DeletePublicAccessBlock::NAME => Row::Full(Eval::Bucket(S3Action::DeleteBucketPublicAccessBlockAction)),
        dto::GetBucketLifecycleConfiguration::NAME => Row::Full(Eval::Bucket(S3Action::GetBucketLifecycleAction)),
        dto::PutBucketLifecycleConfiguration::NAME | dto::DeleteBucketLifecycle::NAME => {
            Row::Full(Eval::Bucket(S3Action::PutBucketLifecycleAction))
        }
        dto::GetBucketReplication::NAME => Row::Full(Eval::Bucket(S3Action::GetReplicationConfigurationAction)),
        dto::PutBucketReplication::NAME | dto::DeleteBucketReplication::NAME => {
            Row::Full(Eval::Bucket(S3Action::PutReplicationConfigurationAction))
        }
        dto::GetObjectLockConfiguration::NAME => Row::Full(Eval::Bucket(S3Action::GetBucketObjectLockConfigurationAction)),
        dto::PutObjectLockConfiguration::NAME => Row::Full(Eval::Bucket(S3Action::PutBucketObjectLockConfigurationAction)),
        dto::GetObject::NAME => Row::Full(Eval::GetObject),
        dto::GetObjectAttributes::NAME => Row::Full(Eval::GetObjectAttributes),
        dto::SelectObjectContent::NAME => Row::Full(Eval::SelectObjectContent),
        dto::GetObjectAcl::NAME => Row::Full(Eval::FixedObject(S3Action::GetObjectAclAction)),
        dto::PutObjectAcl::NAME => Row::Full(Eval::FixedObject(S3Action::PutObjectAclAction)),
        dto::GetObjectLegalHold::NAME => Row::Full(Eval::FixedObject(S3Action::GetObjectLegalHoldAction)),
        dto::GetObjectRetention::NAME => Row::Full(Eval::FixedObject(S3Action::GetObjectRetentionAction)),
        dto::GetObjectTagging::NAME => Row::Full(Eval::Tagging(S3Action::GetObjectTaggingAction)),
        dto::DeleteObjectTagging::NAME => Row::Full(Eval::Tagging(S3Action::DeleteObjectTaggingAction)),
        dto::AbortMultipartUpload::NAME => Row::Full(Eval::AbortMultipartUpload),
        dto::UploadPart::NAME => Row::Full(Eval::UploadPart),
        dto::ListParts::NAME => Row::Full(Eval::ListParts),
        _ => return None,
    })
}

fn evaluation(operation: &str) -> Option<Eval> {
    match authorization_row(operation) {
        Some(Row::Partial(eval) | Row::Full(eval)) => Some(eval),
        Some(Row::Unchecked | Row::Remainder) | None => None,
    }
}

#[cfg(test)]
fn route_class(operation: &str) -> Option<Class> {
    match authorization_row(operation) {
        Some(Row::Unchecked) => Some(Class::Unchecked),
        Some(Row::Remainder | Row::Partial(_)) => Some(Class::Partial),
        Some(Row::Full(_)) => Some(Class::Full),
        None => None,
    }
}

/// What the gate answers before it reads headers or calls the core.
fn decision_before_evaluation(served: bool, operation: &str) -> Option<Decision> {
    if !served {
        return Some(Decision::Deny);
    }
    match authorization_row(operation) {
        None => Some(Decision::Deny),
        Some(Row::Unchecked | Row::Remainder) => Some(Decision::Allow),
        Some(Row::Partial(_) | Row::Full(_)) => None,
    }
}

fn decision_of(result: S3Result<()>) -> Decision {
    match result {
        Ok(()) => Decision::Allow,
        Err(error) if error.code() == &S3ErrorCode::AccessDenied => Decision::Deny,
        Err(error) if error.code() == &S3ErrorCode::InternalError => Decision::Indeterminate,
        Err(_) => Decision::Allow,
    }
}

fn version_id_count(query: &str) -> usize {
    url::form_urlencoded::parse(query.as_bytes())
        .filter(|(name, _)| name == "versionId")
        .count()
}

fn first_version_id(query: &str) -> Option<String> {
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(name, _)| name == "versionId")
        .map(|(_, value)| value.into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::gateway_handlers::{OPERATIONS, rows};
    use rustfs_s3_types::S3Error;
    use std::collections::BTreeSet;

    const UNCHECKED: &[&str] = &[dto::GetObjectTorrent::NAME, dto::ListBuckets::NAME];

    fn partial_names() -> BTreeSet<&'static str> {
        BTreeSet::from([
            dto::CreateBucket::NAME,
            dto::DeleteObjects::NAME,
            dto::PostObject::NAME,
            dto::PutObjectTagging::NAME,
            dto::PutObjectLegalHold::NAME,
            dto::PutObjectRetention::NAME,
            dto::CopyObject::NAME,
            dto::UploadPartCopy::NAME,
            dto::PutObject::NAME,
            dto::DeleteObject::NAME,
            dto::RestoreObject::NAME,
            dto::CreateMultipartUpload::NAME,
            dto::CompleteMultipartUpload::NAME,
            dto::HeadObject::NAME,
            dto::ListObjects::NAME,
            dto::ListObjectsV2::NAME,
            dto::ListObjectVersions::NAME,
            dto::ListMultipartUploads::NAME,
            LIST_OBJECTS_WITH_METADATA,
            LIST_VERSIONS_WITH_METADATA,
        ])
    }

    #[test]
    fn every_served_operation_has_the_pinned_authorization_class() {
        let mut served: Vec<&str> = rows(OPERATIONS).map(|row| row.name).collect();
        served.extend(metadata_listing::NAMES);
        let served_set: BTreeSet<&str> = served.iter().copied().collect();
        assert_eq!(served.len(), served_set.len(), "an operation is served twice");

        let mut unchecked = BTreeSet::new();
        let mut partial = BTreeSet::new();
        let mut full = BTreeSet::new();
        for name in &served_set {
            match route_class(name).unwrap_or_else(|| panic!("{name} has no authorization row")) {
                Class::Unchecked => unchecked.insert(*name),
                Class::Partial => partial.insert(*name),
                Class::Full => full.insert(*name),
            };
        }
        assert_eq!(unchecked, BTreeSet::from_iter(UNCHECKED.iter().copied()));
        assert_eq!(partial, partial_names());
        assert_eq!(full.len() + partial.len() + unchecked.len(), served_set.len());
        for name in UNCHECKED.iter().copied().chain(partial_names()) {
            assert!(served_set.contains(name), "{name} is not served");
        }
    }

    #[test]
    fn the_gate_denies_before_evaluation_only_for_an_unserved_or_unmapped_operation() {
        assert_eq!(decision_before_evaluation(true, "NotARow"), Some(Decision::Deny));
        assert_eq!(decision_before_evaluation(false, dto::ListBuckets::NAME), Some(Decision::Deny));
        assert_eq!(decision_before_evaluation(true, dto::ListBuckets::NAME), Some(Decision::Allow));
        assert_eq!(decision_before_evaluation(true, dto::GetObjectTorrent::NAME), Some(Decision::Allow));
        assert_eq!(decision_before_evaluation(true, dto::CopyObject::NAME), Some(Decision::Allow));
        assert_eq!(decision_before_evaluation(true, dto::HeadBucket::NAME), None);
    }

    #[test]
    fn only_access_denied_and_internal_error_change_the_gateway_decision() {
        assert_eq!(decision_of(Ok(())), Decision::Allow);
        assert_eq!(decision_of(Err(S3Error::new(S3ErrorCode::AccessDenied))), Decision::Deny);
        assert_eq!(decision_of(Err(S3Error::new(S3ErrorCode::InternalError))), Decision::Indeterminate);
        assert_eq!(decision_of(Err(S3Error::new(S3ErrorCode::InvalidRequest))), Decision::Allow);
        assert_eq!(decision_of(Err(S3Error::new(S3ErrorCode::NotImplemented))), Decision::Allow);
        assert_eq!(decision_of(Err(S3Error::new(S3ErrorCode::InvalidArgument))), Decision::Allow);
        assert_eq!(decision_of(Err(S3Error::new(S3ErrorCode::NoSuchBucket))), Decision::Allow);
    }

    #[test]
    fn a_repeated_version_id_is_counted_and_the_first_value_is_kept() {
        assert_eq!(version_id_count(""), 0);
        assert_eq!(version_id_count("location"), 0);
        assert_eq!(first_version_id("location"), None);
        assert_eq!(version_id_count("versionId="), 1);
        assert_eq!(first_version_id("versionId=").as_deref(), Some(""));
        assert_eq!(version_id_count("versionId=a&versionId=b"), 2);
        assert_eq!(first_version_id("versionId=a&versionId=b").as_deref(), Some("a"));
    }
}
