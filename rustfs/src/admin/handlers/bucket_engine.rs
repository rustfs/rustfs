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

//! Bucket engine binding admin handlers.
//!
//! `PUT /rustfs/admin/v3/bucket-engine/{bucket}` binds an empty bucket to a
//! built-in engine (or changes the config of its current engine), `GET`
//! returns the binding and this node's route for the bucket, and `DELETE`
//! returns a bucket whose engine holds no data to the object engine. The
//! binding is stored as the `engine.json` bucket metadata entry and reaches
//! every node's engine registry through bucket metadata reloads. See
//! docs/architecture/bucket-engines.md.

use crate::admin::auth::validate_admin_request;
use crate::admin::operation::{AdminRequest, AdminResponse, Operation};
use crate::admin::router::{AdminOperation, S3Router};
use crate::admin::runtime_sources::{current_notification_system, current_object_store_handle};
use crate::auth::{check_key_valid, get_session_token};
use crate::bucket_engine::{EngineRegistry, EngineRoute, binding};
use crate::server::ADMIN_PREFIX;
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_bucket_engine::{BucketEngineBinding, MAX_BINDING_BYTES};
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_s3_types::{Body, S3Result, s3_error};
use serde::Serialize;
use tracing::{info, warn};

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_BUCKET_ENGINE: &str = "bucket_engine";
const EVENT_ADMIN_BUCKET_ENGINE: &str = "admin_bucket_engine_state";

#[derive(Debug, Serialize)]
struct BucketEngineResponse {
    bucket: String,
    /// The bucket's binding, or `null` for the object engine.
    binding: Option<BucketEngineBinding>,
    /// This node's route for the bucket: `object`, `engine` or `unavailable`.
    route: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    /// This node's engine status snapshot for the bucket, when attached.
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<serde_json::Value>,
}

pub struct SetBucketEngineHandler;
pub struct GetBucketEngineHandler;
pub struct DeleteBucketEngineHandler;

pub fn register_bucket_engine_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    let path = format!("{}{}", ADMIN_PREFIX, "/v3/bucket-engine/{bucket}");
    r.insert(Method::PUT, path.as_str(), AdminOperation(&SetBucketEngineHandler {}))?;
    r.insert(Method::GET, path.as_str(), AdminOperation(&GetBucketEngineHandler {}))?;
    r.insert(Method::DELETE, path.as_str(), AdminOperation(&DeleteBucketEngineHandler {}))?;
    Ok(())
}

/// Kick peers to reload the bucket's metadata so their registries see the
/// binding without waiting for the periodic refresh loop. Failures are
/// logged, not surfaced: the refresh loop converges peers eventually.
fn notify_peers_reload(bucket: String, operation: &'static str) {
    tokio::spawn(async move {
        if let Some(notification_sys) = current_notification_system()
            && let Err(err) = notification_sys.load_bucket_metadata(&bucket).await
        {
            warn!(
                event = EVENT_ADMIN_BUCKET_ENGINE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_BUCKET_ENGINE,
                bucket = %bucket,
                error = %err,
                "failed to notify peers after {operation}"
            );
        }
    });
}

async fn authenticate_admin(req: &AdminRequest) -> S3Result<()> {
    let Some(cred) = req.credentials() else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };

    let (cred, owner) =
        check_key_valid(get_session_token(req.uri(), req.headers()).unwrap_or_default(), cred.access_key()).await?;

    let remote_addr = req.policy_source_addr();
    validate_admin_request(
        req.headers(),
        &cred,
        owner,
        false,
        vec![Action::AdminAction(AdminAction::ConfigUpdateAdminAction)],
        remote_addr,
    )
    .await?;

    Ok(())
}

fn bucket_from_params(params: &Params<'_, '_>) -> S3Result<String> {
    let bucket = params.get("bucket").unwrap_or("").to_string();
    if bucket.is_empty() {
        return Err(s3_error!(InvalidRequest, "bucket name is required"));
    }
    Ok(bucket)
}

fn route_summary(route: EngineRoute) -> (&'static str, Option<String>) {
    match route {
        EngineRoute::Object => ("object", None),
        EngineRoute::Engine(_) => ("engine", None),
        EngineRoute::Unavailable { reason, .. } => ("unavailable", Some(reason)),
    }
}

async fn engine_response(bucket: String, binding: Option<BucketEngineBinding>) -> S3Result<AdminResponse> {
    let route = EngineRegistry::global().route(&bucket);
    let status = match &route {
        EngineRoute::Engine(bound) => Some(match bound.engine.status(&bound.context).await {
            Ok(status) => status,
            Err(err) => serde_json::json!({ "error": err.to_string() }),
        }),
        _ => None,
    };
    let (route, reason) = route_summary(route);
    let response = BucketEngineResponse {
        bucket,
        binding,
        route,
        reason,
        status,
    };
    let json = serde_json::to_string(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
    Ok(AdminResponse::new(StatusCode::OK, Body::from(json)))
}

#[async_trait::async_trait]
impl Operation for SetBucketEngineHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        authenticate_admin(&req).await?;

        let bucket = bucket_from_params(&params)?;
        let body = req
            .take_body()
            .store_all_limited(MAX_BINDING_BYTES)
            .await
            .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;
        if body.is_empty() {
            return Err(s3_error!(
                InvalidRequest,
                "request body is required, e.g. {{\"engine\":\"kv-cache\",\"config_version\":1,\"config\":{{}}}}"
            ));
        }
        let requested = binding::requested_binding(EngineRegistry::global(), &body)?;
        let stored = requested.binding.clone();
        let store = current_object_store_handle().ok_or_else(|| s3_error!(InternalError, "object store not initialized"))?;
        if binding::set_binding(store, &bucket, requested).await? {
            info!(
                event = EVENT_ADMIN_BUCKET_ENGINE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_BUCKET_ENGINE,
                bucket = %bucket,
                engine = %stored.engine.as_str(),
                config_version = stored.config_version,
                "bucket engine binding set"
            );
            notify_peers_reload(bucket.clone(), "set bucket engine binding");
        }

        engine_response(bucket, Some(stored)).await
    }
}

#[async_trait::async_trait]
impl Operation for GetBucketEngineHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        authenticate_admin(&req).await?;

        let bucket = bucket_from_params(&params)?;
        let state = binding::bucket_state(&bucket).await?;
        engine_response(bucket, state.current).await
    }
}

#[async_trait::async_trait]
impl Operation for DeleteBucketEngineHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        authenticate_admin(&req).await?;

        let bucket = bucket_from_params(&params)?;
        if let Some(removed) = binding::clear_binding(EngineRegistry::global(), &bucket).await? {
            info!(
                event = EVENT_ADMIN_BUCKET_ENGINE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_BUCKET_ENGINE,
                bucket = %bucket,
                engine = %removed.engine.as_str(),
                "bucket engine binding cleared"
            );
            notify_peers_reload(bucket.clone(), "clear bucket engine binding");
        }

        engine_response(bucket, None).await
    }
}
