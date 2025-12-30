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

//! KMS key management admin API handlers

use super::Operation;
use crate::admin::auth::validate_admin_request;
use crate::auth::{check_key_valid, get_session_token};
use crate::server::RemoteAddr;
use hyper::{HeaderMap, StatusCode};
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_kms::{KmsError, get_global_kms_service_manager, types::*};
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use tracing::{error, info};
use urlencoding;

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateKmsKeyRequest {
    pub key_usage: Option<KeyUsage>,
    pub description: Option<String>,
    pub tags: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateKmsKeyResponse {
    pub success: bool,
    pub message: String,
    pub key_id: String,
    pub key_metadata: Option<KeyMetadata>,
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

/// Create a new KMS key
pub struct CreateKmsKeyHandler;

#[async_trait::async_trait]
impl Operation for CreateKmsKeyHandler {
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

        let request: CreateKmsKeyRequest = if body.is_empty() {
            CreateKmsKeyRequest {
                key_usage: Some(KeyUsage::EncryptDecrypt),
                description: None,
                tags: None,
            }
        } else {
            serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))?
        };

        let Some(service_manager) = get_global_kms_service_manager() else {
            let response = CreateKmsKeyResponse {
                success: false,
                message: "KMS service manager not initialized".to_string(),
                key_id: "".to_string(),
                key_metadata: None,
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
            return Ok(S3Response::with_headers((StatusCode::SERVICE_UNAVAILABLE, Body::from(data)), headers));
        };

        let Some(manager) = service_manager.get_manager().await else {
            let response = CreateKmsKeyResponse {
                success: false,
                message: "KMS service not running".to_string(),
                key_id: "".to_string(),
                key_metadata: None,
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
            return Ok(S3Response::with_headers((StatusCode::SERVICE_UNAVAILABLE, Body::from(data)), headers));
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

        match manager.create_key(kms_request).await {
            Ok(kms_response) => {
                info!("Created KMS key: {}", kms_response.key_id);
                let response = CreateKmsKeyResponse {
                    success: true,
                    message: "Key created successfully".to_string(),
                    key_id: kms_response.key_id,
                    key_metadata: Some(kms_response.key_metadata),
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

                Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
            }
            Err(e) => {
                error!("Failed to create KMS key: {}", e);
                let response = CreateKmsKeyResponse {
                    success: false,
                    message: format!("Failed to create key: {e}"),
                    key_id: "".to_string(),
                    key_metadata: None,
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

                Ok(S3Response::with_headers((StatusCode::INTERNAL_SERVER_ERROR, Body::from(data)), headers))
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteKmsKeyRequest {
    pub key_id: String,
    pub pending_window_in_days: Option<u32>,
    pub force_immediate: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteKmsKeyResponse {
    pub success: bool,
    pub message: String,
    pub key_id: String,
    pub deletion_date: Option<String>,
}

/// Delete a KMS key
pub struct DeleteKmsKeyHandler;

#[async_trait::async_trait]
impl Operation for DeleteKmsKeyHandler {
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

        let request: DeleteKmsKeyRequest = if body.is_empty() {
            let query_params = extract_query_params(&req.uri);
            let Some(key_id) = query_params.get("keyId") else {
                let response = DeleteKmsKeyResponse {
                    success: false,
                    message: "missing keyId parameter".to_string(),
                    key_id: "".to_string(),
                    deletion_date: None,
                };
                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
                return Ok(S3Response::with_headers((StatusCode::BAD_REQUEST, Body::from(data)), headers));
            };

            // Extract pending_window_in_days and force_immediate from query parameters
            let pending_window_in_days = query_params.get("pending_window_in_days").and_then(|s| s.parse::<u32>().ok());
            let force_immediate = query_params.get("force_immediate").and_then(|s| s.parse::<bool>().ok());

            DeleteKmsKeyRequest {
                key_id: key_id.clone(),
                pending_window_in_days,
                force_immediate,
            }
        } else {
            serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))?
        };

        let Some(service_manager) = get_global_kms_service_manager() else {
            let response = DeleteKmsKeyResponse {
                success: false,
                message: "KMS service manager not initialized".to_string(),
                key_id: request.key_id,
                deletion_date: None,
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
            return Ok(S3Response::with_headers((StatusCode::SERVICE_UNAVAILABLE, Body::from(data)), headers));
        };

        let Some(manager) = service_manager.get_manager().await else {
            let response = DeleteKmsKeyResponse {
                success: false,
                message: "KMS service not running".to_string(),
                key_id: request.key_id,
                deletion_date: None,
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
            return Ok(S3Response::with_headers((StatusCode::SERVICE_UNAVAILABLE, Body::from(data)), headers));
        };

        let kms_request = DeleteKeyRequest {
            key_id: request.key_id.clone(),
            pending_window_in_days: request.pending_window_in_days,
            force_immediate: request.force_immediate,
        };

        match manager.delete_key(kms_request).await {
            Ok(kms_response) => {
                info!("Successfully deleted KMS key: {}", kms_response.key_id);
                let response = DeleteKmsKeyResponse {
                    success: true,
                    message: "Key deleted successfully".to_string(),
                    key_id: kms_response.key_id,
                    deletion_date: kms_response.deletion_date,
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

                Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
            }
            Err(e) => {
                error!("Failed to delete KMS key {}: {}", request.key_id, e);
                let status = match &e {
                    KmsError::KeyNotFound { .. } => StatusCode::NOT_FOUND,
                    KmsError::InvalidOperation { .. } | KmsError::ValidationError { .. } => StatusCode::BAD_REQUEST,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                let response = DeleteKmsKeyResponse {
                    success: false,
                    message: format!("Failed to delete key: {e}"),
                    key_id: request.key_id,
                    deletion_date: None,
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

                Ok(S3Response::with_headers((status, Body::from(data)), headers))
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CancelKmsKeyDeletionRequest {
    pub key_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CancelKmsKeyDeletionResponse {
    pub success: bool,
    pub message: String,
    pub key_id: String,
    pub key_metadata: Option<KeyMetadata>,
}

/// Cancel KMS key deletion
pub struct CancelKmsKeyDeletionHandler;

#[async_trait::async_trait]
impl Operation for CancelKmsKeyDeletionHandler {
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

        let request: CancelKmsKeyDeletionRequest = if body.is_empty() {
            let query_params = extract_query_params(&req.uri);
            let Some(key_id) = query_params.get("keyId") else {
                let response = CancelKmsKeyDeletionResponse {
                    success: false,
                    message: "missing keyId parameter".to_string(),
                    key_id: "".to_string(),
                    key_metadata: None,
                };
                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
                return Ok(S3Response::with_headers((StatusCode::BAD_REQUEST, Body::from(data)), headers));
            };
            CancelKmsKeyDeletionRequest { key_id: key_id.clone() }
        } else {
            serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))?
        };

        let Some(service_manager) = get_global_kms_service_manager() else {
            let response = CancelKmsKeyDeletionResponse {
                success: false,
                message: "KMS service manager not initialized".to_string(),
                key_id: request.key_id,
                key_metadata: None,
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
            return Ok(S3Response::with_headers((StatusCode::SERVICE_UNAVAILABLE, Body::from(data)), headers));
        };

        let Some(manager) = service_manager.get_manager().await else {
            let response = CancelKmsKeyDeletionResponse {
                success: false,
                message: "KMS service not running".to_string(),
                key_id: request.key_id,
                key_metadata: None,
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
            return Ok(S3Response::with_headers((StatusCode::SERVICE_UNAVAILABLE, Body::from(data)), headers));
        };

        let kms_request = CancelKeyDeletionRequest {
            key_id: request.key_id.clone(),
        };

        match manager.cancel_key_deletion(kms_request).await {
            Ok(kms_response) => {
                info!("Cancelled deletion for KMS key: {}", kms_response.key_id);
                let response = CancelKmsKeyDeletionResponse {
                    success: true,
                    message: "Key deletion cancelled successfully".to_string(),
                    key_id: kms_response.key_id,
                    key_metadata: Some(kms_response.key_metadata),
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

                Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
            }
            Err(e) => {
                error!("Failed to cancel deletion for KMS key {}: {}", request.key_id, e);
                let response = CancelKmsKeyDeletionResponse {
                    success: false,
                    message: format!("Failed to cancel key deletion: {e}"),
                    key_id: request.key_id,
                    key_metadata: None,
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

                Ok(S3Response::with_headers((StatusCode::INTERNAL_SERVER_ERROR, Body::from(data)), headers))
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListKmsKeysResponse {
    pub success: bool,
    pub message: String,
    pub keys: Vec<KeyInfo>,
    pub truncated: bool,
    pub next_marker: Option<String>,
}

/// List KMS keys
pub struct ListKmsKeysHandler;

#[async_trait::async_trait]
impl Operation for ListKmsKeysHandler {
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

        let Some(service_manager) = get_global_kms_service_manager() else {
            let response = ListKmsKeysResponse {
                success: false,
                message: "KMS service manager not initialized".to_string(),
                keys: vec![],
                truncated: false,
                next_marker: None,
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
            return Ok(S3Response::with_headers((StatusCode::SERVICE_UNAVAILABLE, Body::from(data)), headers));
        };

        let Some(manager) = service_manager.get_manager().await else {
            let response = ListKmsKeysResponse {
                success: false,
                message: "KMS service not running".to_string(),
                keys: vec![],
                truncated: false,
                next_marker: None,
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
            return Ok(S3Response::with_headers((StatusCode::SERVICE_UNAVAILABLE, Body::from(data)), headers));
        };

        let kms_request = ListKeysRequest {
            limit: Some(limit),
            marker,
            status_filter: None,
            usage_filter: None,
        };

        match manager.list_keys(kms_request).await {
            Ok(kms_response) => {
                info!("Listed {} KMS keys", kms_response.keys.len());
                let response = ListKmsKeysResponse {
                    success: true,
                    message: "Keys listed successfully".to_string(),
                    keys: kms_response.keys,
                    truncated: kms_response.truncated,
                    next_marker: kms_response.next_marker,
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

                Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
            }
            Err(e) => {
                error!("Failed to list KMS keys: {}", e);
                let response = ListKmsKeysResponse {
                    success: false,
                    message: format!("Failed to list keys: {e}"),
                    keys: vec![],
                    truncated: false,
                    next_marker: None,
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

                Ok(S3Response::with_headers((StatusCode::INTERNAL_SERVER_ERROR, Body::from(data)), headers))
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DescribeKmsKeyResponse {
    pub success: bool,
    pub message: String,
    pub key_metadata: Option<KeyMetadata>,
}

/// Describe a KMS key
pub struct DescribeKmsKeyHandler;

#[async_trait::async_trait]
impl Operation for DescribeKmsKeyHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
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

        let Some(key_id) = params.get("key_id") else {
            let response = DescribeKmsKeyResponse {
                success: false,
                message: "missing keyId parameter".to_string(),
                key_metadata: None,
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
            return Ok(S3Response::with_headers((StatusCode::BAD_REQUEST, Body::from(data)), headers));
        };

        let Some(service_manager) = get_global_kms_service_manager() else {
            let response = DescribeKmsKeyResponse {
                success: false,
                message: "KMS service manager not initialized".to_string(),
                key_metadata: None,
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
            return Ok(S3Response::with_headers((StatusCode::SERVICE_UNAVAILABLE, Body::from(data)), headers));
        };

        let Some(manager) = service_manager.get_manager().await else {
            let response = DescribeKmsKeyResponse {
                success: false,
                message: "KMS service not running".to_string(),
                key_metadata: None,
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
            return Ok(S3Response::with_headers((StatusCode::SERVICE_UNAVAILABLE, Body::from(data)), headers));
        };

        let kms_request = DescribeKeyRequest {
            key_id: key_id.to_string(),
        };

        match manager.describe_key(kms_request).await {
            Ok(kms_response) => {
                info!("Described KMS key: {}", key_id);
                let response = DescribeKmsKeyResponse {
                    success: true,
                    message: "Key described successfully".to_string(),
                    key_metadata: Some(kms_response.key_metadata),
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

                Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
            }
            Err(e) => {
                error!("Failed to describe KMS key {}: {}", key_id, e);
                let status = match &e {
                    KmsError::KeyNotFound { .. } => StatusCode::NOT_FOUND,
                    KmsError::InvalidOperation { .. } => StatusCode::BAD_REQUEST,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };

                let response = DescribeKmsKeyResponse {
                    success: false,
                    message: format!("Failed to describe key: {e}"),
                    key_metadata: None,
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

                Ok(S3Response::with_headers((status, Body::from(data)), headers))
            }
        }
    }
}
