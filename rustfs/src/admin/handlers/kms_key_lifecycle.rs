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

//! KMS key lifecycle admin API handlers: enable, disable and rotate.

use super::kms_audit::KmsAdminAudit;
use super::kms_keys::scoped_key_id;
use crate::admin::auth::validate_admin_request_with_kms_key;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::current_kms_runtime_service_manager;
use crate::admin::utils::{extract_query_params, json_response};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_kms::{
    KmsAuditOperation, KmsError, KmsManager,
    types::{DescribeKeyRequest, KeyMetadata, OperationContext},
};
use rustfs_policy::policy::action::{Action, KmsAction};
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info, warn};

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_KMS_KEYS: &str = "kms_key_lifecycle";
const EVENT_ADMIN_KMS_KEYS_STATE: &str = "admin_kms_keys_state";

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KmsKeyLifecycleRequest {
    pub key_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KmsKeyLifecycleResponse {
    pub success: bool,
    pub message: String,
    pub key_id: String,
    pub key_metadata: Option<KeyMetadata>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecycleOperation {
    Enable,
    Disable,
    Rotate,
}

impl LifecycleOperation {
    /// The KMS audit operation this endpoint is recorded as.
    fn audit_operation(self) -> KmsAuditOperation {
        match self {
            Self::Enable => KmsAuditOperation::EnableKey,
            Self::Disable => KmsAuditOperation::DisableKey,
            Self::Rotate => KmsAuditOperation::RotateKey,
        }
    }

    fn action(self) -> &'static str {
        match self {
            Self::Enable => "enable_key",
            Self::Disable => "disable_key",
            Self::Rotate => "rotate_key",
        }
    }

    fn verb(self) -> &'static str {
        match self {
            Self::Enable => "enable",
            Self::Disable => "disable",
            Self::Rotate => "rotate",
        }
    }

    fn past_tense(self) -> &'static str {
        match self {
            Self::Enable => "enabled",
            Self::Disable => "disabled",
            Self::Rotate => "rotated",
        }
    }
}

fn kms_enable_key_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::EnableKeyAction)]
}

fn kms_disable_key_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::DisableKeyAction)]
}

fn kms_rotate_key_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::RotateKeyAction)]
}

pub fn register_kms_key_lifecycle_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/keys/enable").as_str(),
        AdminOperation(&EnableKmsKeyHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/keys/disable").as_str(),
        AdminOperation(&DisableKmsKeyHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/keys/rotate").as_str(),
        AdminOperation(&RotateKmsKeyHandler {}),
    )?;

    Ok(())
}

/// Map a lifecycle failure to an HTTP status.
///
/// `UnsupportedCapability` is a permanent gap of the configured backend, not a
/// missing resource: it must surface as 501, never 404. State-machine
/// rejections (`InvalidOperation`) keep the 400 mapping used by the other
/// `/v3/kms/keys` handlers.
fn lifecycle_error_status(error: &KmsError) -> StatusCode {
    match error {
        KmsError::KeyNotFound { .. } => StatusCode::NOT_FOUND,
        KmsError::UnsupportedCapability { .. } => StatusCode::NOT_IMPLEMENTED,
        KmsError::InvalidOperation { .. } | KmsError::ValidationError { .. } => StatusCode::BAD_REQUEST,
        // Damaged or missing key material is an integrity fault of an existing
        // key: it must surface as a server error, never as NOT_FOUND (the key
        // exists) and never as a retryable backend outage.
        KmsError::MaterialMissing { .. }
        | KmsError::MaterialCorrupt { .. }
        | KmsError::MaterialAuthenticationFailed { .. }
        | KmsError::UnsupportedFormatVersion { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Run one lifecycle operation against the manager and build the HTTP reply.
/// Split from the handlers so the status/response contract is unit-testable
/// without the admin auth stack.
async fn execute_lifecycle(
    manager: &KmsManager,
    key_id: &str,
    operation: LifecycleOperation,
    context: &OperationContext,
) -> (StatusCode, KmsKeyLifecycleResponse) {
    let result = match operation {
        LifecycleOperation::Enable => manager.enable_key_with_context(key_id, context).await,
        LifecycleOperation::Disable => manager.disable_key_with_context(key_id, context).await,
        LifecycleOperation::Rotate => manager.rotate_key_with_context(key_id, context).await,
    };

    match result {
        Ok(()) => {
            info!(
                event = EVENT_ADMIN_KMS_KEYS_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_KMS_KEYS,
                action = operation.action(),
                key_id = %key_id,
                state = "completed",
                "admin kms keys state"
            );
            // Best effort: the state change already succeeded, so a failing
            // describe only drops the metadata echo from the response.
            let key_metadata = match manager
                .describe_key(DescribeKeyRequest {
                    key_id: key_id.to_string(),
                })
                .await
            {
                Ok(response) => Some(response.key_metadata),
                Err(error) => {
                    warn!(
                        event = EVENT_ADMIN_KMS_KEYS_STATE,
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_KMS_KEYS,
                        action = operation.action(),
                        key_id = %key_id,
                        error = %error,
                        "key lifecycle change succeeded but describe failed"
                    );
                    None
                }
            };
            (
                StatusCode::OK,
                KmsKeyLifecycleResponse {
                    success: true,
                    message: format!("key {} successfully", operation.past_tense()),
                    key_id: key_id.to_string(),
                    key_metadata,
                },
            )
        }
        Err(error) => {
            error!(
                event = EVENT_ADMIN_KMS_KEYS_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_KMS_KEYS,
                action = operation.action(),
                key_id = %key_id,
                result = "failed",
                error = %error,
                "admin kms keys state"
            );
            (
                lifecycle_error_status(&error),
                KmsKeyLifecycleResponse {
                    success: false,
                    message: format!("Failed to {} key: {error}", operation.verb()),
                    key_id: key_id.to_string(),
                    key_metadata: None,
                },
            )
        }
    }
}

fn unavailable_response(message: &str, key_id: String) -> S3Result<S3Response<(StatusCode, Body)>> {
    json_response(
        StatusCode::SERVICE_UNAVAILABLE,
        &KmsKeyLifecycleResponse {
            success: false,
            message: message.to_string(),
            key_id,
            key_metadata: None,
        },
    )
}

async fn handle_lifecycle_request(
    mut req: S3Request<Body>,
    actions: Vec<Action>,
    operation: LifecycleOperation,
) -> S3Result<S3Response<(StatusCode, Body)>> {
    let Some(cred) = req.credentials else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };

    let (cred, owner) = check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

    // The body (or the `keyId` query parameter) names the key this operation acts
    // on, so it has to be resolved before the gate runs. A read failure is
    // surfaced only afterwards so an unauthorized caller still sees AccessDenied.
    let body = req.input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await;
    let scoped = body.as_ref().ok().and_then(|body| scoped_key_id(body, &req.uri));
    let audit = KmsAdminAudit::from_request(&req.extensions, &req.headers, &cred);

    audit.gate(
        validate_admin_request_with_kms_key(
            &req.headers,
            &cred,
            owner,
            false,
            actions,
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
            scoped.as_deref().unwrap_or_default(),
        )
        .await,
        operation.audit_operation(),
        scoped.as_deref(),
    )?;

    let body = body.map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;

    let request: KmsKeyLifecycleRequest = if body.is_empty() {
        let query_params = extract_query_params(&req.uri);
        let Some(key_id) = query_params.get("keyId") else {
            return json_response(
                StatusCode::BAD_REQUEST,
                &KmsKeyLifecycleResponse {
                    success: false,
                    message: "missing required parameter: 'keyId'".to_string(),
                    key_id: "".to_string(),
                    key_metadata: None,
                },
            );
        };
        KmsKeyLifecycleRequest { key_id: key_id.clone() }
    } else {
        serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))?
    };

    let Some(service_manager) = kms_service_manager_from_context() else {
        return unavailable_response("kms service manager is not initialized", request.key_id);
    };

    let Some(manager) = service_manager.get_manager().await else {
        return unavailable_response("kms service is not running", request.key_id);
    };

    let (status, response) = execute_lifecycle(&manager, &request.key_id, operation, audit.context()).await;
    json_response(status, &response)
}

fn kms_service_manager_from_context() -> Option<Arc<rustfs_kms::KmsServiceManager>> {
    current_kms_runtime_service_manager()
}

/// Enable a disabled KMS key
pub struct EnableKmsKeyHandler;

#[async_trait::async_trait]
impl Operation for EnableKmsKeyHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        handle_lifecycle_request(req, kms_enable_key_actions(), LifecycleOperation::Enable).await
    }
}

/// Disable a KMS key; existing data remains decryptable
pub struct DisableKmsKeyHandler;

#[async_trait::async_trait]
impl Operation for DisableKmsKeyHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        handle_lifecycle_request(req, kms_disable_key_actions(), LifecycleOperation::Disable).await
    }
}

/// Rotate a KMS key to a new version
pub struct RotateKmsKeyHandler;

#[async_trait::async_trait]
impl Operation for RotateKmsKeyHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        handle_lifecycle_request(req, kms_rotate_key_actions(), LifecycleOperation::Rotate).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Uri;
    use rustfs_kms::backends::local::LocalKmsBackend;
    use rustfs_kms::config::KmsConfig;
    use rustfs_kms::types::{CreateKeyRequest, KeyState};
    use rustfs_policy::policy::action::AdminAction;

    async fn local_manager(temp_dir: &tempfile::TempDir) -> KmsManager {
        let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();
        let backend = Arc::new(
            LocalKmsBackend::new(config.clone())
                .await
                .expect("local backend should build"),
        );
        KmsManager::new(backend, config)
    }

    async fn create_key(manager: &KmsManager, key_name: &str) -> String {
        manager
            .create_key(CreateKeyRequest {
                key_name: Some(key_name.to_string()),
                ..Default::default()
            })
            .await
            .expect("key should be created")
            .key_id
    }

    fn key_state(response: &KmsKeyLifecycleResponse) -> KeyState {
        response
            .key_metadata
            .as_ref()
            .expect("lifecycle response should echo key metadata")
            .key_state
            .clone()
    }

    #[tokio::test]
    async fn enable_disable_enable_round_trip() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let manager = local_manager(&temp_dir).await;
        let key_id = create_key(&manager, "lifecycle-round-trip").await;

        let (status, response) =
            execute_lifecycle(&manager, &key_id, LifecycleOperation::Disable, &OperationContext::internal()).await;
        assert_eq!(status, StatusCode::OK);
        assert!(response.success);
        assert_eq!(key_state(&response), KeyState::Disabled);

        let (status, response) =
            execute_lifecycle(&manager, &key_id, LifecycleOperation::Enable, &OperationContext::internal()).await;
        assert_eq!(status, StatusCode::OK);
        assert!(response.success);
        assert_eq!(key_state(&response), KeyState::Enabled);

        // Enabling an already enabled key stays idempotent.
        let (status, response) =
            execute_lifecycle(&manager, &key_id, LifecycleOperation::Enable, &OperationContext::internal()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(key_state(&response), KeyState::Enabled);
    }

    #[tokio::test]
    async fn local_rotate_is_unsupported_capability_not_missing_key() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let manager = local_manager(&temp_dir).await;
        let key_id = create_key(&manager, "rotate-unsupported").await;

        let (status, response) =
            execute_lifecycle(&manager, &key_id, LifecycleOperation::Rotate, &OperationContext::internal()).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert_ne!(status, StatusCode::NOT_FOUND);
        assert!(!response.success);
        assert!(
            response.message.contains("not supported"),
            "message should name the capability gap: {}",
            response.message
        );

        // A disabled key must not change the picture: rotation stays rejected.
        let (status, _) = execute_lifecycle(&manager, &key_id, LifecycleOperation::Disable, &OperationContext::internal()).await;
        assert_eq!(status, StatusCode::OK);
        let (status, response) =
            execute_lifecycle(&manager, &key_id, LifecycleOperation::Rotate, &OperationContext::internal()).await;
        assert_ne!(status, StatusCode::OK);
        assert!(!response.success);
    }

    #[tokio::test]
    async fn lifecycle_on_missing_key_maps_to_not_found() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let manager = local_manager(&temp_dir).await;

        let (status, response) =
            execute_lifecycle(&manager, "no-such-key", LifecycleOperation::Enable, &OperationContext::internal()).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(!response.success);
    }

    #[test]
    fn lifecycle_error_status_contract() {
        let unsupported = KmsError::unsupported_capability("local", "rotate_key");
        assert_eq!(lifecycle_error_status(&unsupported), StatusCode::NOT_IMPLEMENTED);
        assert_ne!(lifecycle_error_status(&unsupported), StatusCode::NOT_FOUND);

        assert_eq!(lifecycle_error_status(&KmsError::invalid_key_state("disabled")), StatusCode::BAD_REQUEST);
        assert_eq!(lifecycle_error_status(&KmsError::key_not_found("gone")), StatusCode::NOT_FOUND);
    }

    #[test]
    fn lifecycle_auth_actions_use_dedicated_kms_actions() {
        assert_eq!(kms_enable_key_actions(), vec![Action::KmsAction(KmsAction::EnableKeyAction)]);
        assert_eq!(kms_disable_key_actions(), vec![Action::KmsAction(KmsAction::DisableKeyAction)]);
        assert_eq!(kms_rotate_key_actions(), vec![Action::KmsAction(KmsAction::RotateKeyAction)]);

        for actions in [kms_enable_key_actions(), kms_disable_key_actions(), kms_rotate_key_actions()] {
            assert!(!actions.contains(&Action::AdminAction(AdminAction::ServerInfoAdminAction)));
        }
    }

    /// The lifecycle gate must be scoped to the key the request names, and the
    /// key must be resolved the same way for authorization and for execution.
    #[test]
    fn lifecycle_requests_authorize_against_the_key_they_operate_on() {
        let src = include_str!("kms_key_lifecycle.rs");
        let handler = src
            .split_once("async fn handle_lifecycle_request(")
            .expect("lifecycle entry point should exist")
            .1;
        let handler = &handler[..handler.find("\nfn kms_service_manager_from_context").unwrap_or(handler.len())];

        assert!(
            handler.contains("validate_admin_request_with_kms_key("),
            "lifecycle requests must scope authorization to the requested key"
        );
        assert!(
            handler.contains("scoped_key_id(body, &req.uri)"),
            "authorization must resolve the key exactly as execution does"
        );

        let uri: Uri = "/rustfs/admin/v3/kms/keys/disable?keyId=query-key"
            .parse()
            .expect("uri should parse");
        let body = br#"{"key_id":"body-key"}"#;

        assert_eq!(scoped_key_id(body, &uri).as_deref(), Some("body-key"));
        assert_eq!(
            scoped_key_id(body, &uri).as_deref(),
            Some(
                serde_json::from_slice::<KmsKeyLifecycleRequest>(body)
                    .expect("lifecycle body should parse")
                    .key_id
                    .as_str()
            )
        );
        assert_eq!(
            scoped_key_id(b"", &uri).as_deref(),
            extract_query_params(&uri).get("keyId").map(String::as_str)
        );
    }

    #[test]
    fn lifecycle_request_rejects_unknown_fields() {
        let error = serde_json::from_str::<KmsKeyLifecycleRequest>(r#"{"key_id":"key","unexpected_field":true}"#)
            .expect_err("unknown lifecycle request field should fail");
        assert!(error.to_string().contains("unknown field"));
    }
}
