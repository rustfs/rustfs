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

//! KMS management route registration.

use super::kms_keys::{CreateKeyHandler, DescribeKeyHandler, GenerateDataKeyHandler, ListKeysHandler};
use crate::admin::auth::validate_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::app::context::resolve_kms_runtime_service_manager;
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use hyper::{HeaderMap, Method, StatusCode};
use matchit::Params;
use rustfs_kms::init_global_kms_service_manager;
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

async fn kms_encryption_service_from_context() -> Option<std::sync::Arc<rustfs_kms::ObjectEncryptionService>> {
    let manager = match resolve_kms_runtime_service_manager() {
        Some(manager) => manager,
        None => {
            warn!("KMS service manager not initialized, initializing now as fallback");
            init_global_kms_service_manager()
        }
    };
    manager.get_encryption_service().await
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

pub fn register_kms_management_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/create-key").as_str(),
        AdminOperation(&CreateKeyHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/describe-key").as_str(),
        AdminOperation(&DescribeKeyHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/list-keys").as_str(),
        AdminOperation(&ListKeysHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/generate-data-key").as_str(),
        AdminOperation(&GenerateDataKeyHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/status").as_str(),
        AdminOperation(&KmsStatusHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/config").as_str(),
        AdminOperation(&KmsConfigHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/clear-cache").as_str(),
        AdminOperation(&KmsClearCacheHandler {}),
    )?;

    Ok(())
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
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let Some(service) = kms_encryption_service_from_context().await else {
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
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let Some(service) = kms_encryption_service_from_context().await else {
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
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let Some(service) = kms_encryption_service_from_context().await else {
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
