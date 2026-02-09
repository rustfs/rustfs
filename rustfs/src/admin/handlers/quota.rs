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

//! Quota admin handlers for HTTP API

use super::Operation;
use crate::admin::auth::validate_admin_request;
use crate::auth::{check_key_valid, get_session_token};
use hyper::StatusCode;
use matchit::Params;
use rustfs_ecstore::bucket::quota::checker::QuotaChecker;
use rustfs_ecstore::bucket::quota::{BucketQuota, QuotaError, QuotaOperation};
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use serde_json;
use tracing::{debug, info, warn};

#[derive(Debug, Deserialize)]
pub struct SetBucketQuotaRequest {
    pub quota: Option<u64>,
    #[serde(default = "default_quota_type")]
    pub quota_type: String,
}

fn default_quota_type() -> String {
    rustfs_config::QUOTA_TYPE_HARD.to_string()
}

#[derive(Debug, Serialize)]
pub struct BucketQuotaResponse {
    pub bucket: String,
    pub quota: Option<u64>,
    pub size: u64,
    /// Current usage size in bytes
    pub quota_type: String,
}

#[derive(Debug, Serialize)]
pub struct BucketQuotaStats {
    pub bucket: String,
    pub quota_limit: Option<u64>,
    pub current_usage: u64,
    pub remaining_quota: Option<u64>,
    pub usage_percentage: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct CheckQuotaRequest {
    pub operation_type: String,
    pub operation_size: u64,
}

#[derive(Debug, Serialize)]
pub struct CheckQuotaResponse {
    pub bucket: String,
    pub operation_type: String,
    pub operation_size: u64,
    pub allowed: bool,
    pub current_usage: u64,
    pub quota_limit: Option<u64>,
    pub remaining_quota: Option<u64>,
}

/// Quota management handlers
pub struct SetBucketQuotaHandler;
pub struct GetBucketQuotaHandler;
pub struct ClearBucketQuotaHandler;
pub struct GetBucketQuotaStatsHandler;
pub struct CheckBucketQuotaHandler;

#[async_trait::async_trait]
impl Operation for SetBucketQuotaHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle SetBucketQuota");

        let Some(ref cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::SetBucketQuotaAdminAction)],
            None,
        )
        .await?;

        let bucket = params.get("bucket").unwrap_or("").to_string();
        if bucket.is_empty() {
            return Err(s3_error!(InvalidRequest, "bucket name is required"));
        }

        let body = req
            .input
            .store_all_limited(rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;

        let request: SetBucketQuotaRequest = if body.is_empty() {
            SetBucketQuotaRequest {
                quota: None,
                quota_type: default_quota_type(),
            }
        } else {
            serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))?
        };

        if request.quota_type.to_uppercase() != rustfs_config::QUOTA_TYPE_HARD {
            return Err(s3_error!(InvalidArgument, "{}", rustfs_config::QUOTA_INVALID_TYPE_ERROR_MSG));
        }

        let quota = BucketQuota::new(request.quota);

        let metadata_sys_lock = rustfs_ecstore::bucket::metadata_sys::GLOBAL_BucketMetadataSys
            .get()
            .ok_or_else(|| s3_error!(InternalError, "{}", rustfs_config::QUOTA_METADATA_SYSTEM_ERROR_MSG))?;
        let mut quota_checker = QuotaChecker::new(metadata_sys_lock.clone());

        quota_checker
            .set_quota_config(&bucket, quota.clone())
            .await
            .map_err(|e| s3_error!(InternalError, "Failed to set quota: {}", e))?;

        // Get real-time usage from data usage system
        let current_usage = if let Some(store) = rustfs_ecstore::global::GLOBAL_OBJECT_API.get() {
            match rustfs_ecstore::data_usage::load_data_usage_from_backend(store.clone()).await {
                Ok(data_usage_info) => data_usage_info
                    .buckets_usage
                    .get(&bucket)
                    .map(|bucket_usage| bucket_usage.size)
                    .unwrap_or(0),
                Err(_) => 0,
            }
        } else {
            0
        };

        let response = BucketQuotaResponse {
            bucket,
            quota: quota.quota,
            size: current_usage,
            quota_type: rustfs_config::QUOTA_TYPE_HARD.to_string(),
        };

        let json =
            serde_json::to_string(&response).map_err(|e| s3_error!(InternalError, "Failed to serialize response: {}", e))?;

        Ok(S3Response::new((StatusCode::OK, Body::from(json))))
    }
}

#[async_trait::async_trait]
impl Operation for GetBucketQuotaHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle GetBucketQuota");

        let Some(ref cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::GetBucketQuotaAdminAction)],
            None,
        )
        .await?;

        let bucket = params.get("bucket").unwrap_or("").to_string();
        if bucket.is_empty() {
            return Err(s3_error!(InvalidRequest, "bucket name is required"));
        }

        let metadata_sys_lock = rustfs_ecstore::bucket::metadata_sys::GLOBAL_BucketMetadataSys
            .get()
            .ok_or_else(|| s3_error!(InternalError, "Bucket metadata system not initialized"))?;

        let quota_checker = QuotaChecker::new(metadata_sys_lock.clone());

        let (quota, current_usage) = quota_checker.get_quota_stats(&bucket).await.map_err(|e| match e {
            QuotaError::ConfigNotFound { .. } => {
                s3_error!(NoSuchBucket, "Bucket not found: {}", bucket)
            }
            _ => s3_error!(InternalError, "Failed to get quota: {}", e),
        })?;

        let response = BucketQuotaResponse {
            bucket,
            quota: quota.quota,
            size: current_usage.unwrap_or(0),
            quota_type: rustfs_config::QUOTA_TYPE_HARD.to_string(),
        };

        let json =
            serde_json::to_string(&response).map_err(|e| s3_error!(InternalError, "Failed to serialize response: {}", e))?;

        Ok(S3Response::new((StatusCode::OK, Body::from(json))))
    }
}

#[async_trait::async_trait]
impl Operation for ClearBucketQuotaHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle ClearBucketQuota");

        let Some(ref cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::SetBucketQuotaAdminAction)],
            None,
        )
        .await?;

        let bucket = params.get("bucket").unwrap_or("").to_string();
        if bucket.is_empty() {
            return Err(s3_error!(InvalidRequest, "bucket name is required"));
        }

        info!("Clearing quota for bucket: {}", bucket);

        let metadata_sys_lock = rustfs_ecstore::bucket::metadata_sys::GLOBAL_BucketMetadataSys
            .get()
            .ok_or_else(|| s3_error!(InternalError, "Bucket metadata system not initialized"))?;

        let mut quota_checker = QuotaChecker::new(metadata_sys_lock.clone());

        // Clear quota (set to None)
        let quota = BucketQuota::new(None);
        quota_checker
            .set_quota_config(&bucket, quota.clone())
            .await
            .map_err(|e| s3_error!(InternalError, "Failed to clear quota: {}", e))?;

        info!("Successfully cleared quota for bucket: {}", bucket);

        // Get real-time usage from data usage system
        let current_usage = if let Some(store) = rustfs_ecstore::global::GLOBAL_OBJECT_API.get() {
            match rustfs_ecstore::data_usage::load_data_usage_from_backend(store.clone()).await {
                Ok(data_usage_info) => data_usage_info
                    .buckets_usage
                    .get(&bucket)
                    .map(|bucket_usage| bucket_usage.size)
                    .unwrap_or(0),
                Err(_) => 0,
            }
        } else {
            0
        };

        let response = BucketQuotaResponse {
            bucket,
            quota: None,
            size: current_usage,
            quota_type: rustfs_config::QUOTA_TYPE_HARD.to_string(),
        };

        let json =
            serde_json::to_string(&response).map_err(|e| s3_error!(InternalError, "Failed to serialize response: {}", e))?;

        Ok(S3Response::new((StatusCode::OK, Body::from(json))))
    }
}

#[async_trait::async_trait]
impl Operation for GetBucketQuotaStatsHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle GetBucketQuotaStats");

        let Some(ref cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::GetBucketQuotaAdminAction)],
            None,
        )
        .await?;

        let bucket = params.get("bucket").unwrap_or("").to_string();
        if bucket.is_empty() {
            return Err(s3_error!(InvalidRequest, "bucket name is required"));
        }

        let metadata_sys_lock = rustfs_ecstore::bucket::metadata_sys::GLOBAL_BucketMetadataSys
            .get()
            .ok_or_else(|| s3_error!(InternalError, "Bucket metadata system not initialized"))?;

        let quota_checker = QuotaChecker::new(metadata_sys_lock.clone());

        let (quota, current_usage_opt) = quota_checker.get_quota_stats(&bucket).await.map_err(|e| match e {
            QuotaError::ConfigNotFound { .. } => {
                s3_error!(NoSuchBucket, "Bucket not found: {}", bucket)
            }
            _ => s3_error!(InternalError, "Failed to get quota stats: {}", e),
        })?;

        let current_usage = current_usage_opt.unwrap_or(0);
        let usage_percentage = quota.quota.and_then(|limit| {
            if limit == 0 {
                None
            } else {
                Some((current_usage as f64 / limit as f64) * 100.0)
            }
        });

        let remaining_quota = quota.get_remaining_quota(current_usage);

        let response = BucketQuotaStats {
            bucket,
            quota_limit: quota.quota,
            current_usage,
            remaining_quota,
            usage_percentage,
        };

        let json =
            serde_json::to_string(&response).map_err(|e| s3_error!(InternalError, "Failed to serialize response: {}", e))?;

        Ok(S3Response::new((StatusCode::OK, Body::from(json))))
    }
}

#[async_trait::async_trait]
impl Operation for CheckBucketQuotaHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle CheckBucketQuota");

        let Some(ref cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::GetBucketQuotaAdminAction)],
            None,
        )
        .await?;

        let bucket = params.get("bucket").unwrap_or("").to_string();
        if bucket.is_empty() {
            return Err(s3_error!(InvalidRequest, "bucket name is required"));
        }

        let body = req
            .input
            .store_all_limited(rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;

        let request: CheckQuotaRequest = if body.is_empty() {
            return Err(s3_error!(InvalidRequest, "request body cannot be empty"));
        } else {
            serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))?
        };

        debug!(
            "Checking quota for bucket: {}, operation: {}, size: {}",
            bucket, request.operation_type, request.operation_size
        );

        let metadata_sys_lock = rustfs_ecstore::bucket::metadata_sys::GLOBAL_BucketMetadataSys
            .get()
            .ok_or_else(|| s3_error!(InternalError, "Bucket metadata system not initialized"))?;

        let quota_checker = QuotaChecker::new(metadata_sys_lock.clone());

        let operation: QuotaOperation = match request.operation_type.to_uppercase().as_str() {
            "PUT" | "PUTOBJECT" => QuotaOperation::PutObject,
            "POST" | "POSTOBJECT" => QuotaOperation::PostObject,
            "COPY" | "COPYOBJECT" => QuotaOperation::CopyObject,
            "DELETE" | "DELETEOBJECT" => QuotaOperation::DeleteObject,
            _ => QuotaOperation::PutObject, // Default to PUT operation
        };

        let result = quota_checker
            .check_quota_with_usage_reporting(&bucket, operation, request.operation_size, true)
            .await
            .map_err(|e| s3_error!(InternalError, "Failed to check quota: {}", e))?;

        let response = CheckQuotaResponse {
            bucket,
            operation_type: request.operation_type,
            operation_size: request.operation_size,
            allowed: result.allowed,
            current_usage: result.current_usage.unwrap_or(0),
            quota_limit: result.quota_limit,
            remaining_quota: result.remaining,
        };

        let json =
            serde_json::to_string(&response).map_err(|e| s3_error!(InternalError, "Failed to serialize response: {}", e))?;

        Ok(S3Response::new((StatusCode::OK, Body::from(json))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_quota_type() {
        assert_eq!(default_quota_type(), "HARD");
    }

    #[test]
    fn test_quota_operation_parsing() {
        let parse_operation = |operation: &str| match operation.to_uppercase().as_str() {
            "PUT" | "PUTOBJECT" => QuotaOperation::PutObject,
            "POST" | "POSTOBJECT" => QuotaOperation::PostObject,
            "COPY" | "COPYOBJECT" => QuotaOperation::CopyObject,
            "DELETE" | "DELETEOBJECT" => QuotaOperation::DeleteObject,
            _ => QuotaOperation::PutObject,
        };

        assert!(matches!(parse_operation("put"), QuotaOperation::PutObject));
        assert!(matches!(parse_operation("PUT"), QuotaOperation::PutObject));
        assert!(matches!(parse_operation("PutObject"), QuotaOperation::PutObject));
        assert!(matches!(parse_operation("post"), QuotaOperation::PostObject));
        assert!(matches!(parse_operation("POST"), QuotaOperation::PostObject));
        assert!(matches!(parse_operation("PostObject"), QuotaOperation::PostObject));
        assert!(matches!(parse_operation("copy"), QuotaOperation::CopyObject));
        assert!(matches!(parse_operation("DELETE"), QuotaOperation::DeleteObject));
        assert!(matches!(parse_operation("unknown"), QuotaOperation::PutObject));
    }

    #[tokio::test]
    async fn test_quota_response_serialization() {
        let response = BucketQuotaResponse {
            bucket: "test-bucket".to_string(),
            quota: Some(2147483648),
            size: 1073741824,
            quota_type: rustfs_config::QUOTA_TYPE_HARD.to_string(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("test-bucket"));
        assert!(json.contains("2147483648"));
        assert!(json.contains("HARD"));
    }
}
