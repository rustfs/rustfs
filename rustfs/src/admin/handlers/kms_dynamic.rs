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

//! KMS dynamic configuration admin API handlers

use super::Operation;
use crate::admin::auth::validate_admin_request;
use crate::auth::{check_key_valid, get_session_token};
use crate::server::RemoteAddr;
use hyper::StatusCode;
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_ecstore::config::com::{read_config, save_config};
use rustfs_ecstore::new_object_layer_fn;
use rustfs_kms::{
    ConfigureKmsRequest, ConfigureKmsResponse, KmsConfig, KmsConfigSummary, KmsServiceStatus, KmsStatusResponse, StartKmsRequest,
    StartKmsResponse, StopKmsResponse, get_global_kms_service_manager,
};
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use tracing::{error, info, warn};

/// Path to store KMS configuration in the cluster metadata
const KMS_CONFIG_PATH: &str = "config/kms_config.json";

/// Save KMS configuration to cluster storage
async fn save_kms_config(config: &KmsConfig) -> Result<(), String> {
    let Some(store) = new_object_layer_fn() else {
        return Err("Storage layer not initialized".to_string());
    };

    let data = serde_json::to_vec(config).map_err(|e| format!("Failed to serialize KMS config: {e}"))?;

    save_config(store, KMS_CONFIG_PATH, data)
        .await
        .map_err(|e| format!("Failed to save KMS config to storage: {e}"))?;

    info!("KMS configuration persisted to cluster storage at {}", KMS_CONFIG_PATH);
    Ok(())
}

/// Load KMS configuration from cluster storage
pub async fn load_kms_config() -> Option<KmsConfig> {
    let Some(store) = new_object_layer_fn() else {
        warn!("Storage layer not initialized, cannot load KMS config");
        return None;
    };

    match read_config(store, KMS_CONFIG_PATH).await {
        Ok(data) => match serde_json::from_slice::<KmsConfig>(&data) {
            Ok(config) => {
                info!("Loaded KMS configuration from cluster storage");
                Some(config)
            }
            Err(e) => {
                error!("Failed to deserialize KMS config: {}", e);
                None
            }
        },
        Err(e) => {
            // Config not found is normal on first run
            if e.to_string().contains("ConfigNotFound") || e.to_string().contains("not found") {
                info!("No persisted KMS configuration found (first run or not configured yet)");
            } else {
                warn!("Failed to load KMS config from storage: {}", e);
            }
            None
        }
    }
}

/// Configure KMS service handler
pub struct ConfigureKmsHandler;

#[async_trait::async_trait]
impl Operation for ConfigureKmsHandler {
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

        let configure_request: ConfigureKmsRequest = if body.is_empty() {
            return Ok(S3Response::new((
                StatusCode::BAD_REQUEST,
                Body::from("Request body is required".to_string()),
            )));
        } else {
            match serde_json::from_slice(&body) {
                Ok(req) => req,
                Err(e) => {
                    error!("Invalid JSON in configure request: {}", e);
                    return Ok(S3Response::new((StatusCode::BAD_REQUEST, Body::from(format!("Invalid JSON: {e}")))));
                }
            }
        };

        info!("Configuring KMS with request: {:?}", configure_request);

        let service_manager = get_global_kms_service_manager().unwrap_or_else(|| {
            warn!("KMS service manager not initialized, initializing now as fallback");
            // Initialize the service manager as a fallback
            rustfs_kms::init_global_kms_service_manager()
        });

        // Convert request to KmsConfig
        let kms_config = configure_request.to_kms_config();

        // Configure the service
        let (success, message, status) = match service_manager.configure(kms_config.clone()).await {
            Ok(()) => {
                // Persist the configuration to cluster storage
                if let Err(e) = save_kms_config(&kms_config).await {
                    let error_msg = format!("KMS configured in memory but failed to persist: {e}");
                    error!("{}", error_msg);
                    let status = service_manager.get_status().await;
                    (false, error_msg, status)
                } else {
                    let status = service_manager.get_status().await;
                    info!("KMS configured successfully and persisted with status: {:?}", status);
                    (true, "KMS configured successfully".to_string(), status)
                }
            }
            Err(e) => {
                let error_msg = format!("Failed to configure KMS: {e}");
                error!("{}", error_msg);
                let status = service_manager.get_status().await;
                (false, error_msg, status)
            }
        };

        let response = ConfigureKmsResponse {
            success,
            message,
            status,
        };

        let json_response = match serde_json::to_string(&response) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to serialize response: {}", e);
                return Ok(S3Response::new((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Body::from("Serialization error".to_string()),
                )));
            }
        };

        Ok(S3Response::new((StatusCode::OK, Body::from(json_response))))
    }
}

/// Start KMS service handler
pub struct StartKmsHandler;

#[async_trait::async_trait]
impl Operation for StartKmsHandler {
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

        let start_request: StartKmsRequest = if body.is_empty() {
            StartKmsRequest { force: None }
        } else {
            match serde_json::from_slice(&body) {
                Ok(req) => req,
                Err(e) => {
                    error!("Invalid JSON in start request: {}", e);
                    return Ok(S3Response::new((StatusCode::BAD_REQUEST, Body::from(format!("Invalid JSON: {e}")))));
                }
            }
        };

        info!("Starting KMS service with force: {:?}", start_request.force);

        let service_manager = get_global_kms_service_manager().unwrap_or_else(|| {
            warn!("KMS service manager not initialized, initializing now as fallback");
            // Initialize the service manager as a fallback
            rustfs_kms::init_global_kms_service_manager()
        });

        // Check if already running and force flag
        let current_status = service_manager.get_status().await;
        if matches!(current_status, KmsServiceStatus::Running) && !start_request.force.unwrap_or(false) {
            warn!("KMS service is already running");
            let response = StartKmsResponse {
                success: false,
                message: "KMS service is already running. Use force=true to restart.".to_string(),
                status: current_status,
            };
            let json_response = match serde_json::to_string(&response) {
                Ok(json) => json,
                Err(e) => {
                    error!("Failed to serialize response: {}", e);
                    return Ok(S3Response::new((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Body::from("Serialization error".to_string()),
                    )));
                }
            };
            return Ok(S3Response::new((StatusCode::OK, Body::from(json_response))));
        }

        // Start the service (or restart if force=true)
        let (success, message, status) =
            if start_request.force.unwrap_or(false) && matches!(current_status, KmsServiceStatus::Running) {
                // Force restart
                match service_manager.stop().await {
                    Ok(()) => match service_manager.start().await {
                        Ok(()) => {
                            let status = service_manager.get_status().await;
                            info!("KMS service restarted successfully");
                            (true, "KMS service restarted successfully".to_string(), status)
                        }
                        Err(e) => {
                            let error_msg = format!("Failed to restart KMS service: {e}");
                            error!("{}", error_msg);
                            let status = service_manager.get_status().await;
                            (false, error_msg, status)
                        }
                    },
                    Err(e) => {
                        let error_msg = format!("Failed to stop KMS service for restart: {e}");
                        error!("{}", error_msg);
                        let status = service_manager.get_status().await;
                        (false, error_msg, status)
                    }
                }
            } else {
                // Normal start
                match service_manager.start().await {
                    Ok(()) => {
                        let status = service_manager.get_status().await;
                        info!("KMS service started successfully");
                        (true, "KMS service started successfully".to_string(), status)
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to start KMS service: {e}");
                        error!("{}", error_msg);
                        let status = service_manager.get_status().await;
                        (false, error_msg, status)
                    }
                }
            };

        let response = StartKmsResponse {
            success,
            message,
            status,
        };

        let json_response = match serde_json::to_string(&response) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to serialize response: {}", e);
                return Ok(S3Response::new((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Body::from("Serialization error".to_string()),
                )));
            }
        };

        Ok(S3Response::new((StatusCode::OK, Body::from(json_response))))
    }
}

/// Stop KMS service handler
pub struct StopKmsHandler;

#[async_trait::async_trait]
impl Operation for StopKmsHandler {
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

        info!("Stopping KMS service");

        let service_manager = get_global_kms_service_manager().unwrap_or_else(|| {
            warn!("KMS service manager not initialized, initializing now as fallback");
            // Initialize the service manager as a fallback
            rustfs_kms::init_global_kms_service_manager()
        });

        let (success, message, status) = match service_manager.stop().await {
            Ok(()) => {
                let status = service_manager.get_status().await;
                info!("KMS service stopped successfully");
                (true, "KMS service stopped successfully".to_string(), status)
            }
            Err(e) => {
                let error_msg = format!("Failed to stop KMS service: {e}");
                error!("{}", error_msg);
                let status = service_manager.get_status().await;
                (false, error_msg, status)
            }
        };

        let response = StopKmsResponse {
            success,
            message,
            status,
        };

        let json_response = match serde_json::to_string(&response) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to serialize response: {}", e);
                return Ok(S3Response::new((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Body::from("Serialization error".to_string()),
                )));
            }
        };

        Ok(S3Response::new((StatusCode::OK, Body::from(json_response))))
    }
}

/// Get KMS status handler
pub struct GetKmsStatusHandler;

#[async_trait::async_trait]
impl Operation for GetKmsStatusHandler {
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

        info!("Getting KMS service status");

        let service_manager = get_global_kms_service_manager().unwrap_or_else(|| {
            warn!("KMS service manager not initialized, initializing now as fallback");
            // Initialize the service manager as a fallback
            rustfs_kms::init_global_kms_service_manager()
        });

        let status = service_manager.get_status().await;
        let config = service_manager.get_config().await;

        // Get backend type and health status
        let backend_type = config.as_ref().map(|c| c.backend.clone());
        let healthy = if matches!(status, KmsServiceStatus::Running) {
            match service_manager.health_check().await {
                Ok(healthy) => Some(healthy),
                Err(_) => Some(false),
            }
        } else {
            None
        };

        // Create config summary (without sensitive data)
        let config_summary = config.as_ref().map(KmsConfigSummary::from);

        let response = KmsStatusResponse {
            status,
            backend_type,
            healthy,
            config_summary,
        };

        info!("KMS status: {:?}", response);

        let json_response = match serde_json::to_string(&response) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to serialize response: {}", e);
                return Ok(S3Response::new((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Body::from("Serialization error".to_string()),
                )));
            }
        };

        Ok(S3Response::new((StatusCode::OK, Body::from(json_response))))
    }
}

/// Reconfigure KMS service handler
pub struct ReconfigureKmsHandler;

#[async_trait::async_trait]
impl Operation for ReconfigureKmsHandler {
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

        let configure_request: ConfigureKmsRequest = if body.is_empty() {
            return Ok(S3Response::new((
                StatusCode::BAD_REQUEST,
                Body::from("Request body is required".to_string()),
            )));
        } else {
            match serde_json::from_slice(&body) {
                Ok(req) => req,
                Err(e) => {
                    error!("Invalid JSON in reconfigure request: {}", e);
                    return Ok(S3Response::new((StatusCode::BAD_REQUEST, Body::from(format!("Invalid JSON: {e}")))));
                }
            }
        };

        info!("Reconfiguring KMS with request: {:?}", configure_request);

        let service_manager = get_global_kms_service_manager().unwrap_or_else(|| {
            warn!("KMS service manager not initialized, initializing now as fallback");
            // Initialize the service manager as a fallback
            rustfs_kms::init_global_kms_service_manager()
        });

        // Convert request to KmsConfig
        let kms_config = configure_request.to_kms_config();

        // Reconfigure the service (stops, reconfigures, and starts)
        let (success, message, status) = match service_manager.reconfigure(kms_config.clone()).await {
            Ok(()) => {
                // Persist the configuration to cluster storage
                if let Err(e) = save_kms_config(&kms_config).await {
                    let error_msg = format!("KMS reconfigured in memory but failed to persist: {e}");
                    error!("{}", error_msg);
                    let status = service_manager.get_status().await;
                    (false, error_msg, status)
                } else {
                    let status = service_manager.get_status().await;
                    info!("KMS reconfigured successfully and persisted with status: {:?}", status);
                    (true, "KMS reconfigured and restarted successfully".to_string(), status)
                }
            }
            Err(e) => {
                let error_msg = format!("Failed to reconfigure KMS: {e}");
                error!("{}", error_msg);
                let status = service_manager.get_status().await;
                (false, error_msg, status)
            }
        };

        let response = ConfigureKmsResponse {
            success,
            message,
            status,
        };

        let json_response = match serde_json::to_string(&response) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to serialize response: {}", e);
                return Ok(S3Response::new((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Body::from("Serialization error".to_string()),
                )));
            }
        };

        Ok(S3Response::new((StatusCode::OK, Body::from(json_response))))
    }
}
