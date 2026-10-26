// Copyright 2024 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Administrator-directed integrity inspection and bounded, recoverable jobs.
//! These routes do not change the MinIO batch-job compatibility endpoints.

use crate::admin::auth::authorize_admin_envelope;
use crate::admin::operation::{AdminRequest, AdminResponse, Operation};
use crate::admin::router::{AdminOperation, S3Router};
use crate::admin::runtime_sources::object_store_from_envelope;
use crate::admin::storage_api::integrity as service;
use crate::admin::storage_api::s3::{S3Error, S3ErrorCode, S3Result, error as admin_s3_error};
use crate::admin::utils::{extract_query_params, read_compatible_admin_body};
use crate::server::ADMIN_PREFIX;
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_s3_types::Body;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy)]
enum Route {
    Readiness,
    Inventory,
    Create,
    Status,
    Control,
}

struct Handler(Route);

fn permission(route: Route) -> AdminAction {
    match route {
        Route::Readiness => AdminAction::ServerInfoAdminAction,
        Route::Inventory => AdminAction::InspectDataAction,
        Route::Status => AdminAction::DescribeBatchJobAction,
        Route::Create | Route::Control => AdminAction::StartBatchJobAction,
    }
}

pub fn register_integrity_routes(router: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    for (method, path, handler) in [
        (Method::GET, "/v3/integrity/readiness", &Handler(Route::Readiness)),
        (Method::GET, "/v3/integrity/{bucket}/inventory", &Handler(Route::Inventory)),
        (Method::POST, "/v3/integrity/{bucket}/jobs", &Handler(Route::Create)),
        (Method::GET, "/v3/integrity/{bucket}/jobs/{job_id}", &Handler(Route::Status)),
        (Method::POST, "/v3/integrity/{bucket}/jobs/{job_id}/control", &Handler(Route::Control)),
    ] {
        router.insert(method, &format!("{ADMIN_PREFIX}{path}"), AdminOperation(handler))?;
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlRequest {
    operation: Control,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Control {
    Pause,
    Resume,
    Cancel,
}

fn map_error(error: service::IntegrityError) -> S3Error {
    match error {
        service::IntegrityError::Invalid(message) => admin_s3_error(S3ErrorCode::InvalidArgument, message),
        service::IntegrityError::Conflict => admin_s3_error(S3ErrorCode::PreconditionFailed, "integrity job or target changed"),
        service::IntegrityError::Busy => {
            admin_s3_error(S3ErrorCode::SlowDown, "an integrity worker is already active; retry resume later")
        }
        service::IntegrityError::NotFound => admin_s3_error(S3ErrorCode::NoSuchKey, "integrity job not found"),
        service::IntegrityError::NotActivated => admin_s3_error(
            S3ErrorCode::InvalidRequest,
            "protected writes must be explicitly activated before migration",
        ),
        service::IntegrityError::UnsupportedBucket => admin_s3_error(
            S3ErrorCode::InvalidRequest,
            "migration requires a plain unversioned bucket without configured automation or retention",
        ),
        _ => admin_s3_error(
            S3ErrorCode::InternalError,
            "integrity operation failed; inspect the durable job before retrying",
        ),
    }
}

/// Owned-boundary form of `crate::admin::utils::json_response`, which still
/// answers with the s3s response type.
fn json_response<T: Serialize>(status: StatusCode, value: &T) -> S3Result<AdminResponse> {
    let data = serde_json::to_vec(value)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("failed to serialize response: {e}")))?;
    let mut headers = http::HeaderMap::new();
    headers.insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("application/json"));
    Ok(AdminResponse::with_headers(status, Body::from(data), headers))
}

#[async_trait::async_trait]
impl Operation for Handler {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let credentials = authorize_admin_envelope(req.envelope(), vec![Action::AdminAction(permission(self.0))]).await?;
        if matches!(self.0, Route::Readiness) {
            return json_response(StatusCode::OK, &service::readiness());
        }
        let store = object_store_from_envelope(req.envelope())
            .ok_or_else(|| admin_s3_error(S3ErrorCode::InternalError, "object store is not initialized"))?;
        let bucket = params
            .get("bucket")
            .ok_or_else(|| admin_s3_error(S3ErrorCode::InvalidArgument, "bucket is required"))?;
        match self.0 {
            Route::Inventory => {
                let query = extract_query_params(req.uri());
                if query
                    .keys()
                    .any(|key| !["prefix", "key-marker", "version-marker", "limit"].contains(&key.as_str()))
                {
                    return Err(admin_s3_error(S3ErrorCode::InvalidArgument, "unknown inventory parameter"));
                }
                let limit = query
                    .get("limit")
                    .map_or(Ok(100), |v| v.parse::<i32>())
                    .map_err(|_| admin_s3_error(S3ErrorCode::InvalidArgument, "invalid limit"))?;
                let page = service::inventory(
                    store,
                    bucket,
                    query.get("prefix").map_or("", String::as_str),
                    query.get("key-marker").cloned(),
                    query.get("version-marker").cloned(),
                    limit,
                )
                .await
                .map_err(map_error)?;
                json_response(StatusCode::OK, &page)
            }
            Route::Create => {
                let body =
                    read_compatible_admin_body(req.take_body(), 128 * 1024, req.uri().path(), &credentials.secret_key).await?;
                let request: service::JobRequest = serde_json::from_slice(&body)
                    .map_err(|_| admin_s3_error(S3ErrorCode::InvalidArgument, "invalid integrity job request"))?;
                let job = service::create_job(store, bucket, request).await.map_err(map_error)?;
                json_response(StatusCode::CREATED, &job)
            }
            Route::Status | Route::Control => {
                let id = params
                    .get("job_id")
                    .and_then(|s| Uuid::parse_str(s).ok())
                    .filter(|id| !id.is_nil())
                    .ok_or_else(|| admin_s3_error(S3ErrorCode::InvalidArgument, "invalid job id"))?;
                let job = if matches!(self.0, Route::Status) {
                    service::get_job(store, bucket, id).await.map_err(map_error)?
                } else {
                    let body =
                        read_compatible_admin_body(req.take_body(), 1024, req.uri().path(), &credentials.secret_key).await?;
                    let request: ControlRequest = serde_json::from_slice(&body)
                        .map_err(|_| admin_s3_error(S3ErrorCode::InvalidArgument, "expected pause, resume or cancel"))?;
                    match request.operation {
                        Control::Resume => service::resume_job(store, bucket, id).await,
                        Control::Pause => service::control_job(store, bucket, id, false).await,
                        Control::Cancel => service::control_job(store, bucket, id, true).await,
                    }
                    .map_err(map_error)?
                };
                json_response(StatusCode::OK, &job)
            }
            Route::Readiness => json_response(StatusCode::OK, &service::readiness()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn integrity_read_permissions_cannot_start_or_resume_payload_writes() {
        assert_eq!(permission(Route::Create), AdminAction::StartBatchJobAction);
        assert_eq!(permission(Route::Control), AdminAction::StartBatchJobAction);
        assert_eq!(permission(Route::Inventory), AdminAction::InspectDataAction);
        assert_eq!(permission(Route::Status), AdminAction::DescribeBatchJobAction);
    }
    #[test]
    fn control_rejects_unknown_fields_and_operations() {
        assert!(serde_json::from_str::<ControlRequest>(r#"{"operation":"resume","force":true}"#).is_err());
        assert!(serde_json::from_str::<ControlRequest>(r#"{"operation":"overwrite"}"#).is_err());
    }
}

#[cfg(test)]
#[tokio::test]
async fn integrity_routes_reject_unauthenticated_requests_before_storage_access() {
    let mut router = matchit::Router::new();
    router.insert("/{bucket}", ()).expect("test route");
    for route in [
        Route::Readiness,
        Route::Inventory,
        Route::Create,
        Route::Status,
        Route::Control,
    ] {
        let req =
            crate::admin::operation::test_support::TestRequest::new(Method::POST, "/rustfs/admin/v3/integrity/example/jobs")
                .headers(http::HeaderMap::new())
                .extensions(http::Extensions::new())
                .body(Body::from(String::new()))
                .build();
        let error = Handler(route)
            .call(req, router.at("/example").expect("params").params)
            .await
            .err()
            .expect("credentials required");
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(error.message(), Some("get cred failed"));
    }
}
