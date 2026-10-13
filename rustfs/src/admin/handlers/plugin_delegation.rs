// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Restriction registration only. These endpoints do not vend credentials.
use crate::admin::{
    auth::{AdminResourceScope, validate_admin_action_with_bucket_object_for_iam},
    router::{AdminOperation, Operation, S3Router},
    runtime_sources,
};
use crate::{
    auth::{check_key_valid_with_context, get_session_token},
    server::{ADMIN_PREFIX, RemoteAddr},
};
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use matchit::Params;
use rustfs_iam::delegation::{DelegationError, Restrictions};
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn register_plugin_delegation_route(router: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    router.insert(Method::POST, &format!("{ADMIN_PREFIX}/v3/plugin-delegations"), AdminOperation(&Create))?;
    router.insert(Method::GET, &format!("{ADMIN_PREFIX}/v3/plugin-delegations/{{id}}"), AdminOperation(&Get))?;
    router.insert(
        Method::POST,
        &format!("{ADMIN_PREFIX}/v3/plugin-delegations/{{id}}/revoke"),
        AdminOperation(&Revoke),
    )?;
    Ok(())
}

pub(super) fn storage_error(error: DelegationError) -> s3s::S3Error {
    match error {
        DelegationError::Invalid => s3_error!(InvalidRequest, "invalid delegation restriction"),
        DelegationError::NotFound | DelegationError::OwnerRejected => s3_error!(NoSuchKey, "delegation not found"),
        DelegationError::Unavailable => s3_error!(InternalError, "delegation unavailable"),
        _ => s3_error!(AccessDenied, "delegation rejected"),
    }
}

pub(super) async fn owner(
    req: &S3Request<Body>,
    action: AdminAction,
) -> S3Result<(std::sync::Arc<rustfs_iam::sys::IamSys<rustfs_iam::store::object::ObjectStore>>, String)> {
    let signed = req
        .credentials
        .as_ref()
        .ok_or_else(|| s3_error!(AccessDenied, "authentication required"))?;
    if get_session_token(&req.uri, &req.headers).is_some() {
        return Err(s3_error!(AccessDenied, "original user required"));
    }
    let context =
        runtime_sources::app_context_from_req(req).ok_or_else(|| s3_error!(InternalError, "application context unavailable"))?;
    let (credentials, root) = check_key_valid_with_context("", &signed.access_key, Some(context.as_ref())).await?;
    if root || credentials.is_temp() || credentials.is_service_account() {
        return Err(s3_error!(AccessDenied, "original user required"));
    }
    if !context.iam().is_ready() {
        return Err(s3_error!(InternalError, "iam unavailable"));
    }
    let iam = context.iam().handle();
    validate_admin_action_with_bucket_object_for_iam(
        iam.clone(),
        &req.headers,
        &credentials,
        false,
        Action::AdminAction(action),
        req.extensions.get::<Option<RemoteAddr>>().and_then(|a| a.map(|a| a.0)),
        AdminResourceScope::bucket_object("", ""),
    )
    .await?;
    Ok((iam, credentials.access_key))
}

pub(super) fn response<T: serde::Serialize>(value: &T) -> S3Result<S3Response<(StatusCode, Body)>> {
    let bytes = serde_json::to_vec(value).map_err(|_| s3_error!(InternalError, "delegation response unavailable"))?;
    let mut headers = HeaderMap::new();
    headers.insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(http::header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(S3Response::with_headers((StatusCode::OK, Body::from(bytes)), headers))
}

struct Create;
#[async_trait::async_trait]
impl Operation for Create {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let (iam, owner) = owner(&req, AdminAction::CreatePluginDelegationAction).await?;
        let bytes = tokio::time::timeout(Duration::from_secs(10), req.input.store_all_limited(16384))
            .await
            .map_err(|_| s3_error!(InvalidRequest, "delegation body unavailable"))?
            .map_err(|_| s3_error!(InvalidRequest, "delegation body unavailable"))?;
        let restriction: Restrictions =
            serde_json::from_slice(&bytes).map_err(|_| s3_error!(InvalidRequest, "invalid delegation restriction"))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| s3_error!(InternalError, "clock unavailable"))?
            .as_secs();
        let record = iam
            .create_plugin_delegation(&owner, restriction, now)
            .await
            .map_err(storage_error)?;
        response(&record)
    }
}

struct Get;
#[async_trait::async_trait]
impl Operation for Get {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let (iam, owner) = owner(&req, AdminAction::GetPluginDelegationAction).await?;
        let id = params
            .get("id")
            .ok_or_else(|| s3_error!(InvalidRequest, "delegation id required"))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| s3_error!(InternalError, "clock unavailable"))?
            .as_secs();
        let record = iam
            .read_active_plugin_delegation(&owner, id, now)
            .await
            .map_err(storage_error)?;
        response(&record)
    }
}

struct Revoke;
#[async_trait::async_trait]
impl Operation for Revoke {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let (iam, owner) = owner(&req, AdminAction::RevokePluginDelegationAction).await?;
        let id = params
            .get("id")
            .ok_or_else(|| s3_error!(InvalidRequest, "delegation id required"))?;
        iam.revoke_plugin_delegation(&owner, id).await.map_err(storage_error)?;
        response(&serde_json::json!({"revoked":true,"id":id}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn delegation_mutations_require_authenticated_original_user() {
        for operation in 0..3 {
            let req = S3Request {
                input: Body::from("{}".to_owned()),
                method: Method::POST,
                uri: http::Uri::from_static("/rustfs/admin/v3/plugin-delegations"),
                headers: HeaderMap::new(),
                extensions: http::Extensions::new(),
                credentials: None,
                region: None,
                service: None,
                trailing_headers: None,
            };
            let result = if operation == 0 {
                Get.call(req, Params::new()).await
            } else if operation == 1 {
                Revoke.call(req, Params::new()).await
            } else {
                Create.call(req, Params::new()).await
            };
            let error = result.expect_err("unsigned mutations must not reach persistence");
            assert_eq!(error.code(), &s3s::S3ErrorCode::AccessDenied);
        }
    }
}
