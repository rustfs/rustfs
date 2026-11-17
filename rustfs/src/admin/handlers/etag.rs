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

//! Per-bucket ETag algorithm admin handlers.
//!
//! A bucket can derive the ETags of objects written without Content-MD5 from
//! XXH3-128 (`xxh3`) instead of MD5 (`md5`, the default), for clients that
//! never treat the ETag as an MD5, such as an LLM KV cache. The choice is
//! stored in the bucket metadata (`etag.json` entry) and consulted by the
//! PutObject and UploadPart write paths; a multipart upload keeps the
//! algorithm it was created with. See docs/operations/nixl-s3-over-rdma.md.

use crate::admin::auth::validate_admin_request;
use crate::admin::operation::{AdminRequest, AdminResponse, Operation};
use crate::admin::router::{AdminOperation, S3Router};
use crate::admin::runtime_sources::current_notification_system;
use crate::admin::storage_api::bucket::etag::BucketEtagConfig;
use crate::admin::storage_api::bucket::metadata::BUCKET_ETAG_CONFIG;
use crate::admin::storage_api::bucket::metadata_sys;
use crate::auth::{check_key_valid, get_session_token};
use crate::server::ADMIN_PREFIX;
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_s3_types::{Body, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_ETAG: &str = "bucket_etag";
const EVENT_ADMIN_BUCKET_ETAG: &str = "admin_bucket_etag_state";

#[derive(Debug, Deserialize)]
struct SetBucketEtagRequest {
    algorithm: String,
}

#[derive(Debug, Serialize)]
struct BucketEtagResponse {
    bucket: String,
    /// The bucket's ETag algorithm, or `null` when the bucket has no setting
    /// (MD5 ETags).
    algorithm: Option<String>,
}

pub struct SetBucketEtagHandler;
pub struct GetBucketEtagHandler;
pub struct DeleteBucketEtagHandler;

pub fn register_etag_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/bucket-etag/{bucket}").as_str(),
        AdminOperation(&SetBucketEtagHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/bucket-etag/{bucket}").as_str(),
        AdminOperation(&GetBucketEtagHandler {}),
    )?;

    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/bucket-etag/{bucket}").as_str(),
        AdminOperation(&DeleteBucketEtagHandler {}),
    )?;

    Ok(())
}

fn parse_set_bucket_etag_request(body: &[u8]) -> Result<SetBucketEtagRequest, rustfs_s3_types::S3Error> {
    if body.is_empty() {
        return Err(s3_error!(InvalidRequest, "request body is required, e.g. {{\"algorithm\":\"xxh3\"}}"));
    }
    serde_json::from_slice(body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))
}

/// Validates and canonicalizes the requested algorithm name.
fn normalize_algorithm(algorithm: &str) -> Result<String, rustfs_s3_types::S3Error> {
    let normalized = algorithm.trim().to_ascii_lowercase();
    if BucketEtagConfig::is_valid_algorithm(&normalized) {
        Ok(normalized)
    } else {
        Err(s3_error!(InvalidArgument, "invalid ETag algorithm {:?}: expected md5|xxh3", algorithm))
    }
}

/// Kick peers to reload the bucket's metadata so the setting takes effect
/// cluster-wide without waiting for the periodic refresh loop. Failures are
/// logged, not surfaced: the refresh loop converges peers eventually.
fn notify_peers_reload(bucket: String, operation: &'static str) {
    tokio::spawn(async move {
        if let Some(notification_sys) = current_notification_system()
            && let Err(err) = notification_sys.load_bucket_metadata(&bucket).await
        {
            warn!(
                event = EVENT_ADMIN_BUCKET_ETAG,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_ETAG,
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

fn etag_response(bucket: String, algorithm: Option<String>) -> S3Result<AdminResponse> {
    let response = BucketEtagResponse { bucket, algorithm };
    let json = serde_json::to_string(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
    Ok(AdminResponse::new(StatusCode::OK, Body::from(json)))
}

#[async_trait::async_trait]
impl Operation for SetBucketEtagHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        authenticate_admin(&req).await?;

        let bucket = bucket_from_params(&params)?;
        let expected_incarnation_id = metadata_sys::capture_bucket_metadata_incarnation(&bucket)
            .await
            .map_err(|e| s3_error!(InternalError, "failed to capture bucket incarnation: {}", e))?;

        let body = req
            .take_body()
            .store_all_limited(rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;

        let request = parse_set_bucket_etag_request(&body)?;
        let algorithm = normalize_algorithm(&request.algorithm)?;

        let config = BucketEtagConfig::new(&algorithm);
        let json = serde_json::to_vec(&config).map_err(|e| s3_error!(InternalError, "failed to encode config: {}", e))?;

        // System buckets are rejected by the metadata layer.
        metadata_sys::update_if_incarnation(&bucket, BUCKET_ETAG_CONFIG, json, expected_incarnation_id)
            .await
            .map_err(|e| s3_error!(InternalError, "failed to set bucket ETag algorithm: {}", e))?;

        info!(
            event = EVENT_ADMIN_BUCKET_ETAG,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_ETAG,
            bucket = %bucket,
            algorithm = %algorithm,
            "bucket ETag algorithm set"
        );

        notify_peers_reload(bucket.clone(), "set bucket ETag algorithm");

        etag_response(bucket, Some(algorithm))
    }
}

#[async_trait::async_trait]
impl Operation for GetBucketEtagHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        authenticate_admin(&req).await?;

        let bucket = bucket_from_params(&params)?;

        let (config, _updated_at) = metadata_sys::get_etag_config(&bucket)
            .await
            .map_err(|e| s3_error!(NoSuchBucket, "failed to read bucket ETag algorithm: {}", e))?;

        let algorithm = config.and_then(|c| c.normalized_algorithm());
        etag_response(bucket, algorithm)
    }
}

#[async_trait::async_trait]
impl Operation for DeleteBucketEtagHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        authenticate_admin(&req).await?;

        let bucket = bucket_from_params(&params)?;
        let expected_incarnation_id = metadata_sys::capture_bucket_metadata_incarnation(&bucket)
            .await
            .map_err(|e| s3_error!(InternalError, "failed to capture bucket incarnation: {}", e))?;

        metadata_sys::delete_if_incarnation(&bucket, BUCKET_ETAG_CONFIG, expected_incarnation_id)
            .await
            .map_err(|e| s3_error!(InternalError, "failed to clear bucket ETag algorithm: {}", e))?;

        info!(
            event = EVENT_ADMIN_BUCKET_ETAG,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_ETAG,
            bucket = %bucket,
            "bucket ETag algorithm cleared"
        );

        notify_peers_reload(bucket.clone(), "clear bucket ETag algorithm");

        etag_response(bucket, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_set_request_requires_body_and_valid_json() {
        assert!(parse_set_bucket_etag_request(b"").is_err());
        assert!(parse_set_bucket_etag_request(b"not-json").is_err());

        let req = parse_set_bucket_etag_request(br#"{"algorithm":"xxh3"}"#).expect("parse");
        assert_eq!(req.algorithm, "xxh3");
    }

    #[test]
    fn normalize_algorithm_accepts_md5_and_xxh3_only() {
        assert_eq!(normalize_algorithm("md5").unwrap(), "md5");
        assert_eq!(normalize_algorithm(" XXH3 ").unwrap(), "xxh3");
        assert!(normalize_algorithm("").is_err());
        assert!(normalize_algorithm("sha256").is_err());
    }

    #[test]
    fn response_serializes_no_setting_as_null() {
        let json = serde_json::to_string(&BucketEtagResponse {
            bucket: "b".to_string(),
            algorithm: None,
        })
        .expect("serialize");
        assert_eq!(json, r#"{"bucket":"b","algorithm":null}"#);
    }
}
