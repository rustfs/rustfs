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

use super::storage_compat::{read_admin_config, save_admin_config};
use crate::admin::auth::validate_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::app::context::{resolve_kms_runtime_service_manager, resolve_object_store_handle};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_kms::{
    ConfigureKmsRequest, ConfigureKmsResponse, KmsConfig, KmsConfigSummary, KmsServiceStatus, KmsStatusResponse, StartKmsRequest,
    StartKmsResponse, StopKmsResponse,
};
use rustfs_policy::policy::action::{Action, KmsAction};
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use tracing::{error, info, instrument, warn};

/// Path to store KMS configuration in the cluster metadata
const KMS_CONFIG_PATH: &str = "config/kms_config.json";
const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_KMS: &str = "kms";
const EVENT_ADMIN_KMS_DYNAMIC_STATE: &str = "admin_kms_dynamic_state";

fn kms_service_manager_from_context() -> std::sync::Arc<rustfs_kms::KmsServiceManager> {
    resolve_kms_runtime_service_manager().unwrap_or_else(|| {
        warn!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_KMS,
            event = "kms_service_manager_fallback",
            result = "service_manager_fallback_initialized",
            "admin kms dynamic state"
        );
        rustfs_kms::init_global_kms_service_manager()
    })
}

fn token_is_blank(auth_method: &rustfs_kms::config::VaultAuthMethod) -> bool {
    matches!(
        auth_method,
        rustfs_kms::config::VaultAuthMethod::Token { token } if token.trim().is_empty()
    )
}

fn existing_vault_auth(config: &KmsConfig) -> Option<rustfs_kms::config::VaultAuthMethod> {
    match &config.backend_config {
        rustfs_kms::config::BackendConfig::VaultKv2(vault) => Some(vault.auth_method.clone()),
        rustfs_kms::config::BackendConfig::VaultTransit(vault) => Some(vault.auth_method.clone()),
        rustfs_kms::config::BackendConfig::Local(_) => None,
    }
}

fn kms_configure_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::ConfigureAction)]
}

fn kms_service_control_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::ServiceControlAction)]
}

fn normalize_configure_request_auth(
    request: &mut ConfigureKmsRequest,
    existing_config: Option<&KmsConfig>,
) -> Result<(), String> {
    let needs_existing_auth = match request {
        ConfigureKmsRequest::VaultKv2(req) => token_is_blank(&req.auth_method),
        ConfigureKmsRequest::VaultTransit(req) => token_is_blank(&req.auth_method),
        ConfigureKmsRequest::Local(_) => false,
    };

    if !needs_existing_auth {
        return Ok(());
    }

    let existing_auth = existing_config
        .and_then(existing_vault_auth)
        .ok_or_else(|| "Vault token is required when no existing KMS credentials are available".to_string())?;

    match request {
        ConfigureKmsRequest::VaultKv2(req) => req.auth_method = existing_auth,
        ConfigureKmsRequest::VaultTransit(req) => req.auth_method = existing_auth,
        ConfigureKmsRequest::Local(_) => {}
    }

    Ok(())
}

/// Save KMS configuration to cluster storage
#[instrument(skip(config))]
async fn save_kms_config(config: &KmsConfig) -> Result<(), String> {
    let Some(store) = resolve_object_store_handle() else {
        return Err("Storage layer not initialized".to_string());
    };

    let data = serde_json::to_vec(config).map_err(|e| format!("Failed to serialize KMS config: {e}"))?;

    save_admin_config(store, KMS_CONFIG_PATH, data)
        .await
        .map_err(|e| format!("Failed to save KMS config to storage: {e}"))?;

    info!(
        component = LOG_COMPONENT_ADMIN,
        subsystem = LOG_SUBSYSTEM_KMS,
        event = "kms_config_persisted",
        storage_path = KMS_CONFIG_PATH,
        state = "config_persisted",
        "admin kms dynamic state"
    );
    Ok(())
}

/// Load KMS configuration from cluster storage
#[instrument]
pub async fn load_kms_config() -> Option<KmsConfig> {
    let Some(store) = resolve_object_store_handle() else {
        warn!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_KMS,
            event = "kms_config_load_skipped",
            reason = "storage_uninitialized",
            result = "config_load_skipped",
            "admin kms dynamic state"
        );
        return None;
    };

    match read_admin_config(store, KMS_CONFIG_PATH).await {
        Ok(data) => match serde_json::from_slice::<KmsConfig>(&data) {
            Ok(config) => {
                info!(
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS,
                    event = "kms_config_loaded",
                    storage_path = KMS_CONFIG_PATH,
                    state = "config_loaded",
                    "admin kms dynamic state"
                );
                Some(config)
            }
            Err(e) => {
                error!(
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS,
                    event = "kms_config_deserialize_failed",
                    storage_path = KMS_CONFIG_PATH,
                    result = "config_deserialize_failed",
                    error = %e,
                    "admin kms dynamic state"
                );
                None
            }
        },
        Err(e) => {
            // Config not found is normal on first run
            if e.to_string().contains("ConfigNotFound") || e.to_string().contains("not found") {
                info!(
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS,
                    event = "kms_config_loaded",
                    state = "not_found",
                    storage_path = KMS_CONFIG_PATH,
                    "admin kms dynamic state"
                );
            } else {
                warn!(
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS,
                    event = "kms_config_load_failed",
                    storage_path = KMS_CONFIG_PATH,
                    result = "config_load_failed",
                    error = %e,
                    "admin kms dynamic state"
                );
            }
            None
        }
    }
}

pub fn register_kms_dynamic_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/configure").as_str(),
        AdminOperation(&ConfigureKmsHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/start").as_str(),
        AdminOperation(&StartKmsHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/stop").as_str(),
        AdminOperation(&StopKmsHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/service-status").as_str(),
        AdminOperation(&GetKmsStatusHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/reconfigure").as_str(),
        AdminOperation(&ReconfigureKmsHandler {}),
    )?;

    Ok(())
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
            kms_configure_actions(),
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let body = req
            .input
            .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;

        let mut configure_request: ConfigureKmsRequest = if body.is_empty() {
            return Ok(S3Response::new((
                StatusCode::BAD_REQUEST,
                Body::from("Request body is required".to_string()),
            )));
        } else {
            match serde_json::from_slice(&body) {
                Ok(req) => req,
                Err(e) => {
                    error!(
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_KMS,
                        event = "kms_request_decode_failed",
                        operation = "configure",
                        result = "request_decode_failed",
                        error = %e,
                        "admin kms dynamic state"
                    );
                    return Ok(S3Response::new((StatusCode::BAD_REQUEST, Body::from(format!("Invalid JSON: {e}")))));
                }
            }
        };

        info!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_KMS,
            event = "kms_service_state",
            operation = "configure",
            state = "requested",
            "admin kms dynamic state"
        );

        let service_manager = kms_service_manager_from_context();
        let existing_config = service_manager.get_config().await;

        if let Err(e) = normalize_configure_request_auth(&mut configure_request, existing_config.as_ref()) {
            return Ok(S3Response::new((StatusCode::BAD_REQUEST, Body::from(e))));
        }

        // Convert request to KmsConfig
        let kms_config = configure_request.to_kms_config();

        // Configure the service
        let (success, message, status) = match service_manager.configure(kms_config.clone()).await {
            Ok(()) => {
                // Persist the configuration to cluster storage
                if let Err(e) = save_kms_config(&kms_config).await {
                    let error_msg = format!("KMS configured in memory but failed to persist: {e}");
                    error!(
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_KMS,
                        event = "kms_service_state",
                        operation = "configure",
                        state = "persist_failed",
                        error = %e,
                        "admin kms dynamic state"
                    );
                    let status = service_manager.get_status().await;
                    (false, error_msg, status)
                } else {
                    let status = service_manager.get_status().await;
                    info!(
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_KMS,
                        event = "kms_service_state",
                        operation = "configure",
                        state = "configured",
                        status = ?status,
                        "admin kms dynamic state"
                    );
                    (true, "KMS configured successfully".to_string(), status)
                }
            }
            Err(e) => {
                let error_msg = format!("Failed to configure KMS: {e}");
                error!(
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS,
                    event = "kms_service_state",
                    operation = "configure",
                    state = "configure_failed",
                    error = %e,
                    "admin kms dynamic state"
                );
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
                error!(
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS,
                    event = EVENT_ADMIN_KMS_DYNAMIC_STATE,
                    operation = "configure",
                    result = "response_serialize_failed",
                    error = %e,
                    "admin kms dynamic state"
                );
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
            kms_service_control_actions(),
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
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
                    error!(
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_KMS,
                        event = "kms_request_decode_failed",
                        operation = "start",
                        result = "request_decode_failed",
                        error = %e,
                        "admin kms dynamic state"
                    );
                    return Ok(S3Response::new((StatusCode::BAD_REQUEST, Body::from(format!("Invalid JSON: {e}")))));
                }
            }
        };

        info!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_KMS,
            event = "kms_service_state",
            operation = "start",
            state = "requested",
            force = start_request.force.unwrap_or(false),
            "admin kms dynamic state"
        );

        let service_manager = kms_service_manager_from_context();

        // Check if already running and force flag
        let current_status = service_manager.get_status().await;
        if matches!(current_status, KmsServiceStatus::Running) && !start_request.force.unwrap_or(false) {
            warn!(
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_KMS,
                event = "kms_service_state",
                operation = "start",
                state = "already_running",
                "admin kms dynamic state"
            );
            let response = StartKmsResponse {
                success: false,
                message: "KMS service is already running. Use force=true to restart.".to_string(),
                status: current_status,
            };
            let json_response = match serde_json::to_string(&response) {
                Ok(json) => json,
                Err(e) => {
                    error!(
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_KMS,
                        event = EVENT_ADMIN_KMS_DYNAMIC_STATE,
                        operation = "start",
                        result = "response_serialize_failed",
                        error = %e,
                        "admin kms dynamic state"
                    );
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
                            info!(
                                component = LOG_COMPONENT_ADMIN,
                                subsystem = LOG_SUBSYSTEM_KMS,
                                event = "kms_service_state",
                                operation = "restart",
                                state = "running",
                                status = ?status,
                                "admin kms dynamic state"
                            );
                            (true, "KMS service restarted successfully".to_string(), status)
                        }
                        Err(e) => {
                            let error_msg = format!("Failed to restart KMS service: {e}");
                            error!(
                                component = LOG_COMPONENT_ADMIN,
                                subsystem = LOG_SUBSYSTEM_KMS,
                                event = "kms_service_state",
                                operation = "restart",
                                state = "start_failed",
                                error = %e,
                                "admin kms dynamic state"
                            );
                            let status = service_manager.get_status().await;
                            (false, error_msg, status)
                        }
                    },
                    Err(e) => {
                        let error_msg = format!("Failed to stop KMS service for restart: {e}");
                        error!(
                            component = LOG_COMPONENT_ADMIN,
                            subsystem = LOG_SUBSYSTEM_KMS,
                            event = "kms_service_state",
                            operation = "restart",
                            state = "stop_failed",
                            error = %e,
                            "admin kms dynamic state"
                        );
                        let status = service_manager.get_status().await;
                        (false, error_msg, status)
                    }
                }
            } else {
                // Normal start
                match service_manager.start().await {
                    Ok(()) => {
                        let status = service_manager.get_status().await;
                        info!(
                            component = LOG_COMPONENT_ADMIN,
                            subsystem = LOG_SUBSYSTEM_KMS,
                            event = "kms_service_state",
                            operation = "start",
                            state = "running",
                            status = ?status,
                            "admin kms dynamic state"
                        );
                        (true, "KMS service started successfully".to_string(), status)
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to start KMS service: {e}");
                        error!(
                            component = LOG_COMPONENT_ADMIN,
                            subsystem = LOG_SUBSYSTEM_KMS,
                            event = "kms_service_state",
                            operation = "start",
                            state = "start_failed",
                            error = %e,
                            "admin kms dynamic state"
                        );
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
                error!(
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS,
                    event = EVENT_ADMIN_KMS_DYNAMIC_STATE,
                    operation = "start",
                    result = "response_serialize_failed",
                    error = %e,
                    "admin kms dynamic state"
                );
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
            kms_service_control_actions(),
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        info!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_KMS,
            event = "kms_service_state",
            operation = "stop",
            state = "requested",
            "admin kms dynamic state"
        );

        let service_manager = kms_service_manager_from_context();

        let (success, message, status) = match service_manager.stop().await {
            Ok(()) => {
                let status = service_manager.get_status().await;
                info!(
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS,
                    event = "kms_service_state",
                    operation = "stop",
                    state = "stopped",
                    status = ?status,
                    "admin kms dynamic state"
                );
                (true, "KMS service stopped successfully".to_string(), status)
            }
            Err(e) => {
                let error_msg = format!("Failed to stop KMS service: {e}");
                error!(
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS,
                    event = "kms_service_state",
                    operation = "stop",
                    state = "stop_failed",
                    error = %e,
                    "admin kms dynamic state"
                );
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
                error!(
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS,
                    event = EVENT_ADMIN_KMS_DYNAMIC_STATE,
                    operation = "stop",
                    result = "response_serialize_failed",
                    error = %e,
                    "admin kms dynamic state"
                );
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
            kms_service_control_actions(),
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        info!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_KMS,
            event = "kms_status_requested",
            state = "status_requested",
            "admin kms dynamic state"
        );

        let service_manager = kms_service_manager_from_context();

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

        info!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_KMS,
            event = "kms_status_resolved",
            status = ?response.status,
            backend_type = ?response.backend_type,
            healthy = response.healthy,
            has_config_summary = response.config_summary.is_some(),
            state = "status_resolved",
            "admin kms dynamic state"
        );

        let json_response = match serde_json::to_string(&response) {
            Ok(json) => json,
            Err(e) => {
                error!(
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS,
                    event = EVENT_ADMIN_KMS_DYNAMIC_STATE,
                    operation = "status",
                    result = "response_serialize_failed",
                    error = %e,
                    "admin kms dynamic state"
                );
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
            kms_configure_actions(),
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let body = req
            .input
            .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;

        let mut configure_request: ConfigureKmsRequest = if body.is_empty() {
            return Ok(S3Response::new((
                StatusCode::BAD_REQUEST,
                Body::from("Request body is required".to_string()),
            )));
        } else {
            match serde_json::from_slice(&body) {
                Ok(req) => req,
                Err(e) => {
                    error!(
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_KMS,
                        event = "kms_request_decode_failed",
                        operation = "reconfigure",
                        result = "request_decode_failed",
                        error = %e,
                        "admin kms dynamic state"
                    );
                    return Ok(S3Response::new((StatusCode::BAD_REQUEST, Body::from(format!("Invalid JSON: {e}")))));
                }
            }
        };

        info!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_KMS,
            event = "kms_service_state",
            operation = "reconfigure",
            state = "requested",
            "admin kms dynamic state"
        );

        let service_manager = kms_service_manager_from_context();
        let existing_config = service_manager.get_config().await;

        if let Err(e) = normalize_configure_request_auth(&mut configure_request, existing_config.as_ref()) {
            return Ok(S3Response::new((StatusCode::BAD_REQUEST, Body::from(e))));
        }

        // Convert request to KmsConfig
        let kms_config = configure_request.to_kms_config();

        // Reconfigure the service (stops, reconfigures, and starts)
        let (success, message, status) = match service_manager.reconfigure(kms_config.clone()).await {
            Ok(()) => {
                // Persist the configuration to cluster storage
                if let Err(e) = save_kms_config(&kms_config).await {
                    let error_msg = format!("KMS reconfigured in memory but failed to persist: {e}");
                    error!(
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_KMS,
                        event = "kms_service_state",
                        operation = "reconfigure",
                        state = "persist_failed",
                        error = %e,
                        "admin kms dynamic state"
                    );
                    let status = service_manager.get_status().await;
                    (false, error_msg, status)
                } else {
                    let status = service_manager.get_status().await;
                    info!(
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_KMS,
                        event = "kms_service_state",
                        operation = "reconfigure",
                        state = "reconfigured",
                        status = ?status,
                        "admin kms dynamic state"
                    );
                    (true, "KMS reconfigured and restarted successfully".to_string(), status)
                }
            }
            Err(e) => {
                let error_msg = format!("Failed to reconfigure KMS: {e}");
                error!(
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS,
                    event = "kms_service_state",
                    operation = "reconfigure",
                    state = "reconfigure_failed",
                    error = %e,
                    "admin kms dynamic state"
                );
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
                error!(
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS,
                    event = EVENT_ADMIN_KMS_DYNAMIC_STATE,
                    operation = "reconfigure",
                    result = "response_serialize_failed",
                    error = %e,
                    "admin kms dynamic state"
                );
                return Ok(S3Response::new((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Body::from("Serialization error".to_string()),
                )));
            }
        };

        Ok(S3Response::new((StatusCode::OK, Body::from(json_response))))
    }
}

#[cfg(test)]
mod tests {
    use super::{kms_configure_actions, kms_service_control_actions};
    use rustfs_policy::policy::action::{Action, AdminAction, KmsAction};

    fn assert_has_action(actions: &[Action], action: Action) {
        assert!(actions.contains(&action), "expected action list to contain {action:?}");
    }

    fn assert_lacks_action(actions: &[Action], action: Action) {
        assert!(!actions.contains(&action), "expected action list not to contain {action:?}");
    }

    #[test]
    fn kms_dynamic_auth_actions_use_dedicated_kms_actions() {
        assert_has_action(&kms_configure_actions(), Action::KmsAction(KmsAction::ConfigureAction));
        assert_has_action(&kms_service_control_actions(), Action::KmsAction(KmsAction::ServiceControlAction));
    }

    #[test]
    fn kms_dynamic_actions_reject_server_info_fallback() {
        assert_lacks_action(&kms_configure_actions(), Action::AdminAction(AdminAction::ServerInfoAdminAction));
        assert_lacks_action(&kms_service_control_actions(), Action::AdminAction(AdminAction::ServerInfoAdminAction));
    }
}
