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

//! Per-bucket durability tier admin handlers (HP-5 phase 2, rustfs/backlog#938).
//!
//! A bucket can override the process-wide durability mode
//! (`RUSTFS_DURABILITY_MODE`) with its own `strict` | `relaxed` | `none`
//! tier. The override is stored in the bucket metadata (`durability.json`
//! entry) and consumed by the disk layer at commit points. System buckets
//! can never carry an override. See docs/operations/durability-modes.md.

use crate::admin::auth::validate_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::current_notification_system;
use crate::admin::storage_api::bucket::durability::BucketDurabilityConfig;
use crate::admin::storage_api::bucket::metadata::BUCKET_DURABILITY_CONFIG;
use crate::admin::storage_api::bucket::metadata_sys;
use crate::auth::{check_key_valid, get_session_token};
use crate::server::ADMIN_PREFIX;
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_DURABILITY: &str = "bucket_durability";
const EVENT_ADMIN_BUCKET_DURABILITY: &str = "admin_bucket_durability_state";

#[derive(Debug, Deserialize)]
struct SetBucketDurabilityRequest {
    mode: String,
}

#[derive(Debug, Serialize)]
struct BucketDurabilityResponse {
    bucket: String,
    /// The bucket's own override, or `null` when the bucket inherits the
    /// process-wide durability mode.
    mode: Option<String>,
}

pub struct SetBucketDurabilityHandler;
pub struct GetBucketDurabilityHandler;
pub struct DeleteBucketDurabilityHandler;

pub fn register_durability_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/bucket-durability/{bucket}").as_str(),
        AdminOperation(&SetBucketDurabilityHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/bucket-durability/{bucket}").as_str(),
        AdminOperation(&GetBucketDurabilityHandler {}),
    )?;

    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/bucket-durability/{bucket}").as_str(),
        AdminOperation(&DeleteBucketDurabilityHandler {}),
    )?;

    Ok(())
}

fn parse_set_bucket_durability_request(body: &[u8]) -> Result<SetBucketDurabilityRequest, s3s::S3Error> {
    if body.is_empty() {
        return Err(s3_error!(InvalidRequest, "request body is required, e.g. {{\"mode\":\"relaxed\"}}"));
    }
    serde_json::from_slice(body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))
}

/// Validates and canonicalizes the requested tier name.
fn normalize_mode(mode: &str) -> Result<String, s3s::S3Error> {
    let normalized = mode.trim().to_ascii_lowercase();
    if BucketDurabilityConfig::is_valid_mode(&normalized) {
        Ok(normalized)
    } else {
        Err(s3_error!(
            InvalidArgument,
            "invalid durability mode {:?}: expected strict|relaxed|none",
            mode
        ))
    }
}

/// Kick peers to reload the bucket's metadata so the override takes effect
/// cluster-wide without waiting for the periodic refresh loop. Failures are
/// logged, not surfaced: the refresh loop converges peers eventually.
fn notify_peers_reload(bucket: String, operation: &'static str) {
    tokio::spawn(async move {
        if let Some(notification_sys) = current_notification_system()
            && let Err(err) = notification_sys.load_bucket_metadata(&bucket).await
        {
            warn!(
                event = EVENT_ADMIN_BUCKET_DURABILITY,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_DURABILITY,
                bucket = %bucket,
                error = %err,
                "failed to notify peers after {operation}"
            );
        }
    });
}

async fn authenticate_admin(req: &S3Request<Body>) -> S3Result<()> {
    let Some(ref cred) = req.credentials else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };

    let (cred, owner) = check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

    validate_admin_request(
        &req.headers,
        &cred,
        owner,
        false,
        vec![Action::AdminAction(AdminAction::ConfigUpdateAdminAction)],
        None,
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

fn durability_response(bucket: String, mode: Option<String>) -> S3Result<S3Response<(StatusCode, Body)>> {
    let response = BucketDurabilityResponse { bucket, mode };
    let json = serde_json::to_string(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
    Ok(S3Response::new((StatusCode::OK, Body::from(json))))
}

#[async_trait::async_trait]
impl Operation for SetBucketDurabilityHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authenticate_admin(&req).await?;

        let bucket = bucket_from_params(&params)?;

        let body = req
            .input
            .store_all_limited(rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;

        let request = parse_set_bucket_durability_request(&body)?;
        let mode = normalize_mode(&request.mode)?;

        let config = BucketDurabilityConfig::new(&mode);
        let json = serde_json::to_vec(&config).map_err(|e| s3_error!(InternalError, "failed to encode config: {}", e))?;

        // System buckets are rejected by the metadata layer (and pinned to
        // strict by the disk layer regardless).
        metadata_sys::update(&bucket, BUCKET_DURABILITY_CONFIG, json)
            .await
            .map_err(|e| s3_error!(InternalError, "failed to set bucket durability: {}", e))?;

        info!(
            event = EVENT_ADMIN_BUCKET_DURABILITY,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_DURABILITY,
            bucket = %bucket,
            mode = %mode,
            "bucket durability override set"
        );

        notify_peers_reload(bucket.clone(), "set bucket durability");

        durability_response(bucket, Some(mode))
    }
}

#[async_trait::async_trait]
impl Operation for GetBucketDurabilityHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authenticate_admin(&req).await?;

        let bucket = bucket_from_params(&params)?;

        let (config, _updated_at) = metadata_sys::get_durability_config(&bucket)
            .await
            .map_err(|e| s3_error!(NoSuchBucket, "failed to read bucket durability: {}", e))?;

        let mode = config.and_then(|c| c.normalized_mode());
        durability_response(bucket, mode)
    }
}

#[async_trait::async_trait]
impl Operation for DeleteBucketDurabilityHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authenticate_admin(&req).await?;

        let bucket = bucket_from_params(&params)?;

        metadata_sys::delete(&bucket, BUCKET_DURABILITY_CONFIG)
            .await
            .map_err(|e| s3_error!(InternalError, "failed to clear bucket durability: {}", e))?;

        info!(
            event = EVENT_ADMIN_BUCKET_DURABILITY,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_DURABILITY,
            bucket = %bucket,
            "bucket durability override cleared"
        );

        notify_peers_reload(bucket.clone(), "clear bucket durability");

        durability_response(bucket, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_set_request_requires_body_and_valid_json() {
        assert!(parse_set_bucket_durability_request(b"").is_err());
        assert!(parse_set_bucket_durability_request(b"not-json").is_err());

        let req = parse_set_bucket_durability_request(br#"{"mode":"relaxed"}"#).expect("parse");
        assert_eq!(req.mode, "relaxed");
    }

    #[test]
    fn normalize_mode_accepts_tiers_and_rejects_everything_else() {
        assert_eq!(normalize_mode("strict").unwrap(), "strict");
        assert_eq!(normalize_mode(" RELAXED ").unwrap(), "relaxed");
        assert_eq!(normalize_mode("none").unwrap(), "none");
        assert!(normalize_mode("").is_err());
        assert!(normalize_mode("bogus").is_err());
        // The legacy full-off switch is process-wide only.
        assert!(normalize_mode("legacy-off").is_err());
    }

    #[test]
    fn response_serializes_inherit_as_null() {
        let json = serde_json::to_string(&BucketDurabilityResponse {
            bucket: "b".to_string(),
            mode: None,
        })
        .expect("serialize");
        assert_eq!(json, r#"{"bucket":"b","mode":null}"#);
    }
}
