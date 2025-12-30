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

//! KMS admin handlers for HTTP API

use super::Operation;
use crate::admin::auth::validate_admin_request;
use crate::auth::{check_key_valid, get_session_token};
use crate::server::RemoteAddr;
use base64::Engine;
use hyper::{HeaderMap, StatusCode};
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_kms::{get_global_encryption_service, types::*};
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use tracing::{error, info, warn};

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateKeyApiRequest {
    pub key_usage: Option<KeyUsage>,
    pub description: Option<String>,
    pub tags: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateKeyApiResponse {
    pub key_id: String,
    pub key_metadata: KeyMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DescribeKeyApiResponse {
    pub key_metadata: KeyMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListKeysApiResponse {
    pub keys: Vec<KeyInfo>,
    pub truncated: bool,
    pub next_marker: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GenerateDataKeyApiRequest {
    pub key_id: String,
    pub key_spec: KeySpec,
    pub encryption_context: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GenerateDataKeyApiResponse {
    pub key_id: String,
    pub plaintext_key: String,   // Base64 encoded
    pub ciphertext_blob: String, // Base64 encoded
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KmsStatusResponse {
    pub backend_type: String,
    pub backend_status: String,
    pub cache_enabled: bool,
    pub cache_stats: Option<CacheStatsResponse>,
    pub default_key_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CacheStatsResponse {
    pub hit_count: u64,
    pub miss_count: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KmsConfigResponse {
    pub backend: String,
    pub cache_enabled: bool,
    pub cache_max_keys: usize,
    pub cache_ttl_seconds: u64,
    pub default_key_id: Option<String>,
}

fn extract_query_params(uri: &hyper::Uri) -> HashMap<String, String> {
    let mut params = HashMap::new();
    if let Some(query) = uri.query() {
        query.split('&').for_each(|pair| {
            if let Some((key, value)) = pair.split_once('=') {
                params.insert(
                    urlencoding::decode(key).unwrap_or_default().into_owned(),
                    urlencoding::decode(value).unwrap_or_default().into_owned(),
                );
            }
        });
    }
    params
}

/// Create a new KMS master key
pub struct CreateKeyHandler {}

#[async_trait::async_trait]
impl Operation for CreateKeyHandler {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::ServerInfoAdminAction)], // TODO: Add specific KMS action
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
        )
        .await?;

        let body = req
            .input
            .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;

        let request: CreateKeyApiRequest = if body.is_empty() {
            CreateKeyApiRequest {
                key_usage: Some(KeyUsage::EncryptDecrypt),
                description: None,
                tags: None,
            }
        } else {
            serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))?
        };

        let Some(service) = get_global_encryption_service().await else {
            return Err(s3_error!(InternalError, "KMS service not initialized"));
        };

        // Extract key name from tags if provided
        let tags = request.tags.unwrap_or_default();
        let key_name = tags.get("name").cloned();

        let kms_request = CreateKeyRequest {
            key_name,
            key_usage: request.key_usage.unwrap_or(KeyUsage::EncryptDecrypt),
            description: request.description,
            tags,
            origin: Some("AWS_KMS".to_string()),
            policy: None,
        };

        match service.create_key(kms_request).await {
            Ok(response) => {
                let api_response = CreateKeyApiResponse {
                    key_id: response.key_id,
                    key_metadata: response.key_metadata,
                };

                let data = serde_json::to_vec(&api_response)
                    .map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

                Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
            }
            Err(e) => {
                error!("Failed to create KMS key: {}", e);
                Err(s3_error!(InternalError, "failed to create key: {}", e))
            }
        }
    }
}

/// Describe a KMS key
pub struct DescribeKeyHandler {}

#[async_trait::async_trait]
impl Operation for DescribeKeyHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::ServerInfoAdminAction)],
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
        )
        .await?;

        let query_params = extract_query_params(&req.uri);
        let Some(key_id) = query_params.get("keyId") else {
            return Err(s3_error!(InvalidRequest, "missing keyId parameter"));
        };

        let Some(service) = get_global_encryption_service().await else {
            return Err(s3_error!(InternalError, "KMS service not initialized"));
        };

        let request = DescribeKeyRequest { key_id: key_id.clone() };

        match service.describe_key(request).await {
            Ok(response) => {
                let api_response = DescribeKeyApiResponse {
                    key_metadata: response.key_metadata,
                };

                let data = serde_json::to_vec(&api_response)
                    .map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

                Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
            }
            Err(e) => {
                error!("Failed to describe KMS key {}: {}", key_id, e);
                Err(s3_error!(InternalError, "failed to describe key: {}", e))
            }
        }
    }
}

/// List KMS keys
pub struct ListKeysHandler {}

#[async_trait::async_trait]
impl Operation for ListKeysHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::ServerInfoAdminAction)],
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
        )
        .await?;

        let query_params = extract_query_params(&req.uri);
        let limit = query_params.get("limit").and_then(|s| s.parse::<u32>().ok()).unwrap_or(100);
        let marker = query_params.get("marker").cloned();

        let Some(service) = get_global_encryption_service().await else {
            return Err(s3_error!(InternalError, "KMS service not initialized"));
        };

        let request = ListKeysRequest {
            limit: Some(limit),
            marker,
            status_filter: None,
            usage_filter: None,
        };

        match service.list_keys(request).await {
            Ok(response) => {
                let api_response = ListKeysApiResponse {
                    keys: response.keys,
                    truncated: response.truncated,
                    next_marker: response.next_marker,
                };

                let data = serde_json::to_vec(&api_response)
                    .map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

                Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
            }
            Err(e) => {
                error!("Failed to list KMS keys: {}", e);
                Err(s3_error!(InternalError, "failed to list keys: {}", e))
            }
        }
    }
}

/// Generate data encryption key
pub struct GenerateDataKeyHandler {}

#[async_trait::async_trait]
impl Operation for GenerateDataKeyHandler {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::ServerInfoAdminAction)],
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
        )
        .await?;

        let body = req
            .input
            .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;

        let request: GenerateDataKeyApiRequest =
            serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))?;

        let Some(service) = get_global_encryption_service().await else {
            return Err(s3_error!(InternalError, "KMS service not initialized"));
        };

        let kms_request = GenerateDataKeyRequest {
            key_id: request.key_id,
            key_spec: request.key_spec,
            encryption_context: request.encryption_context.unwrap_or_default(),
        };

        match service.generate_data_key(kms_request).await {
            Ok(response) => {
                let api_response = GenerateDataKeyApiResponse {
                    key_id: response.key_id,
                    plaintext_key: base64::prelude::BASE64_STANDARD.encode(&response.plaintext_key),
                    ciphertext_blob: base64::prelude::BASE64_STANDARD.encode(&response.ciphertext_blob),
                };

                let data = serde_json::to_vec(&api_response)
                    .map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

                Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
            }
            Err(e) => {
                error!("Failed to generate data key: {}", e);
                Err(s3_error!(InternalError, "failed to generate data key: {}", e))
            }
        }
    }
}

/// Get KMS service status
pub struct KmsStatusHandler {}

#[async_trait::async_trait]
impl Operation for KmsStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::ServerInfoAdminAction)],
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
        )
        .await?;

        let Some(service) = get_global_encryption_service().await else {
            return Err(s3_error!(InternalError, "KMS service not initialized"));
        };

        let backend_status = match service.health_check().await {
            Ok(true) => "healthy".to_string(),
            Ok(false) => "unhealthy".to_string(),
            Err(e) => {
                warn!("KMS health check failed: {}", e);
                "error".to_string()
            }
        };

        let cache_stats = service.cache_stats().await.map(|(hits, misses)| CacheStatsResponse {
            hit_count: hits,
            miss_count: misses,
        });

        let response = KmsStatusResponse {
            backend_type: "vault".to_string(), // TODO: Get from config
            backend_status,
            cache_enabled: cache_stats.is_some(),
            cache_stats,
            default_key_id: service.get_default_key_id().cloned(),
        };

        let data = serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
    }
}

/// Get KMS configuration
pub struct KmsConfigHandler {}

#[async_trait::async_trait]
impl Operation for KmsConfigHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::ServerInfoAdminAction)],
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
        )
        .await?;

        let Some(service) = get_global_encryption_service().await else {
            return Err(s3_error!(InternalError, "KMS service not initialized"));
        };

        // TODO: Get actual config from service
        let response = KmsConfigResponse {
            backend: "vault".to_string(),
            cache_enabled: true,
            cache_max_keys: 1000,
            cache_ttl_seconds: 300,
            default_key_id: service.get_default_key_id().cloned(),
        };

        let data = serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
    }
}

/// Clear KMS cache
pub struct KmsClearCacheHandler {}

#[async_trait::async_trait]
impl Operation for KmsClearCacheHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::ServerInfoAdminAction)],
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
        )
        .await?;

        let Some(service) = get_global_encryption_service().await else {
            return Err(s3_error!(InternalError, "KMS service not initialized"));
        };

        match service.clear_cache().await {
            Ok(()) => {
                info!("KMS cache cleared successfully");
                let response = serde_json::json!({
                    "status": "success",
                    "message": "cache cleared successfully"
                });

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

                Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
            }
            Err(e) => {
                error!("Failed to clear KMS cache: {}", e);
                Err(s3_error!(InternalError, "failed to clear cache: {}", e))
            }
        }
    }
}
