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

use super::kms_audit::{KmsAdminAudit, KmsAdminOperation};
use crate::admin::auth::validate_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::{
    current_app_context, current_kms_runtime_service_manager, current_notification_system_for_context,
    current_object_store_handle_for_context, current_or_init_kms_runtime_service_manager,
};
use crate::admin::storage_api::config::{read_admin_config, save_admin_config};
use crate::admin::storage_api::error::StorageError;
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
use sha2::{Digest, Sha256};
use tracing::{error, info, instrument, warn};

/// Path to store KMS configuration in the cluster metadata
const KMS_CONFIG_PATH: &str = "config/kms_config.json";
const STATIC_KMS_LOCAL_CONFIG_REQUIRED: &str =
    "Static KMS must be configured through RUSTFS_KMS_STATIC_SECRET_KEY or RUSTFS_KMS_STATIC_SECRET_KEY_FILE";
const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_KMS: &str = "kms";
const EVENT_ADMIN_KMS_DYNAMIC_STATE: &str = "admin_kms_dynamic_state";
/// Substituted for every secret value before a configuration is fingerprinted,
/// so the digest depends on the shape of the configuration and never on key
/// material an operator could recover by replaying candidate secrets.
const REDACTED_CONFIG_VALUE: &str = "[redacted]";
/// Configuration field names carrying secrets. Matched at any depth so a
/// backend that nests its credentials cannot leak them into the fingerprint.
const REDACTED_CONFIG_FIELDS: [&str; 6] = ["token", "secret_key", "secret_id", "master_key", "password", "client_secret"];

fn kms_service_manager_from_context() -> std::sync::Arc<rustfs_kms::KmsServiceManager> {
    current_kms_runtime_service_manager().unwrap_or_else(|| {
        warn!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_KMS,
            event = "kms_service_manager_fallback",
            result = "service_manager_fallback_initialized",
            "admin kms dynamic state"
        );
        current_or_init_kms_runtime_service_manager()
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
        rustfs_kms::config::BackendConfig::Static(_) => None,
        // AWS credentials come from the aws-config chain, not from KMS config.
        rustfs_kms::config::BackendConfig::Aws(_) => None,
    }
}

fn kms_configure_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::ConfigureAction)]
}

fn kms_service_control_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::ServiceControlAction)]
}

fn normalize_configure_request_secrets(
    request: &mut ConfigureKmsRequest,
    existing_config: Option<&KmsConfig>,
) -> Result<(), String> {
    if existing_config.is_some_and(|config| matches!(&config.backend_config, rustfs_kms::BackendConfig::Local(_)))
        && !matches!(request, ConfigureKmsRequest::Local(_))
    {
        return Err("Changing from the Local KMS backend is not supported".to_string());
    }

    if let ConfigureKmsRequest::Local(request) = request
        && let Some(KmsConfig {
            backend_config: rustfs_kms::BackendConfig::Local(existing),
            allow_insecure_dev_defaults,
            ..
        }) = existing_config
    {
        if request.key_dir != existing.key_dir {
            return Err("Changing the Local KMS key directory is not supported".to_string());
        }
        match request.file_permissions {
            Some(permissions) if Some(permissions) != existing.file_permissions => {
                return Err("Changing Local KMS file permissions is not supported".to_string());
            }
            None => request.file_permissions = existing.file_permissions,
            Some(_) => {}
        }
        if request.master_key.as_deref().is_some_and(|master_key| !master_key.is_empty()) {
            return Err("Changing the Local KMS master key is not supported".to_string());
        }
        request.master_key.clone_from(&existing.master_key);
        request.allow_insecure_dev_defaults = Some(*allow_insecure_dev_defaults);
    }

    let needs_existing_auth = match request {
        ConfigureKmsRequest::VaultKv2(req) => token_is_blank(&req.auth_method),
        ConfigureKmsRequest::VaultTransit(req) => token_is_blank(&req.auth_method),
        ConfigureKmsRequest::Local(_) => false,
        ConfigureKmsRequest::Static(_) => false,
        // AWS credentials come from the aws-config chain, so there is nothing
        // to carry over from the existing configuration.
        ConfigureKmsRequest::Aws(_) => false,
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
        ConfigureKmsRequest::Static(_) => {}
        ConfigureKmsRequest::Aws(_) => {}
    }

    Ok(())
}

fn ensure_kms_config_persistable(config: &KmsConfig) -> Result<(), String> {
    if matches!(&config.backend_config, rustfs_kms::BackendConfig::Static(_)) {
        return Err(STATIC_KMS_LOCAL_CONFIG_REQUIRED.to_string());
    }
    Ok(())
}

fn ensure_kms_request_persistable(request: &ConfigureKmsRequest) -> Result<(), String> {
    if matches!(request, ConfigureKmsRequest::Static(_)) {
        return Err(STATIC_KMS_LOCAL_CONFIG_REQUIRED.to_string());
    }
    Ok(())
}

/// Save KMS configuration to cluster storage
#[instrument(skip(config))]
async fn save_kms_config(config: &KmsConfig) -> Result<(), String> {
    ensure_kms_config_persistable(config)?;

    let context = current_app_context();
    let Some(store) = current_object_store_handle_for_context(context.as_deref()) else {
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

fn decode_persisted_kms_config(data: &[u8]) -> serde_json::Result<(KmsConfig, bool)> {
    // The observing loader warns about fields this build ignores, per the
    // repository unknown-field rule for compatibility-bound formats.
    let mut config: KmsConfig = rustfs_kms::config::kms_config_from_persisted_json(data)?;
    // The immediate-deletion gate is per-server operator state, never stored,
    // so a config loaded from cluster storage still has to pick it up here.
    config.allow_immediate_deletion = rustfs_kms::config::allow_immediate_deletion_from_env();
    let value: serde_json::Value = serde_json::from_slice(data)?;
    let is_missing_development_flag = value
        .as_object()
        .is_some_and(|object| !object.contains_key("allow_insecure_dev_defaults"));
    let mut uses_legacy_local_defaults = false;

    if is_missing_development_flag
        && matches!(&config.backend_config, rustfs_kms::BackendConfig::Local(_))
        && config.validate().is_err()
    {
        // RUSTFS_COMPAT_TODO(rustfs-5063): Remove after pre-beta.9 configurations are rewritten with this field.
        // Pre-beta.9 persisted Local KMS configurations predate the explicit
        // development-default flag.
        config.allow_insecure_dev_defaults = true;
        if config.validate().is_ok() {
            uses_legacy_local_defaults = true;
        } else {
            config.allow_insecure_dev_defaults = false;
        }
    }

    Ok((config, uses_legacy_local_defaults))
}

/// Load KMS configuration from cluster storage
#[instrument]
pub async fn load_kms_config() -> Option<KmsConfig> {
    let context = current_app_context();
    let Some(store) = current_object_store_handle_for_context(context.as_deref()) else {
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
        Ok(data) => match decode_persisted_kms_config(&data) {
            Ok((config, is_legacy_local)) => {
                if is_legacy_local {
                    warn!(
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_KMS,
                        event = "kms_legacy_local_config_loaded",
                        storage_path = KMS_CONFIG_PATH,
                        state = "legacy_config_accepted",
                        "admin kms dynamic state"
                    );
                }
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
            // Config not found is normal on first run: `read_config` maps a missing or
            // empty config object to `ConfigNotFound`, so that variant is the only
            // "absent" signal reaching here. Every other not-found variant (disk,
            // volume, bucket) means degraded storage and must stay a warning.
            if matches!(e, StorageError::ConfigNotFound) {
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

fn redact_config_secrets(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            for (key, field) in object.iter_mut() {
                if REDACTED_CONFIG_FIELDS.contains(&key.as_str()) {
                    *field = serde_json::Value::String(REDACTED_CONFIG_VALUE.to_string());
                } else {
                    redact_config_secrets(field);
                }
            }
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(redact_config_secrets),
        _ => {}
    }
}

/// Serialize with object keys ordered so two nodes holding the same
/// configuration always hash the same bytes, whatever their map iteration order.
fn write_canonical_json(value: &serde_json::Value, out: &mut String) {
    match value {
        serde_json::Value::Object(object) => {
            let mut keys = object.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            out.push('{');
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::Value::String(key.clone()).to_string());
                out.push(':');
                write_canonical_json(&object[key], out);
            }
            out.push('}');
        }
        serde_json::Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_canonical_json(item, out);
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

fn redacted_canonical_config(config: &KmsConfig) -> Option<String> {
    let mut value = serde_json::to_value(config).ok()?;
    redact_config_secrets(&mut value);
    let mut canonical = String::new();
    write_canonical_json(&value, &mut canonical);
    Some(canonical)
}

/// Fingerprint of a KMS configuration in its persisted form, with every secret
/// replaced before hashing.
///
/// Comparing fingerprints across nodes makes a configuration split visible
/// without exposing what any node is configured with. Redaction is deliberate:
/// two nodes holding the same backend with different credentials fingerprint
/// alike, which is the price of a digest that is safe to report.
pub fn kms_config_fingerprint(config: &KmsConfig) -> Option<String> {
    let canonical = redacted_canonical_config(config)?;
    Some(hex_simd::encode_to_string(
        Sha256::digest(canonical.as_bytes()),
        hex_simd::AsciiCase::Lower,
    ))
}

/// Fingerprint of the configuration this node is currently running.
///
/// `None` when KMS has never been configured on this node, which is itself a
/// divergence from a cluster that has.
pub async fn current_kms_config_fingerprint() -> Option<String> {
    kms_config_fingerprint(&kms_service_manager_from_context().get_config().await?)
}

fn kms_config_is_unchanged(current: &KmsConfig, candidate: &KmsConfig) -> bool {
    match (serde_json::to_vec(current), serde_json::to_vec(candidate)) {
        (Ok(current), Ok(candidate)) => current == candidate,
        _ => false,
    }
}

/// Re-read the cluster-persisted KMS configuration and switch this node to it.
///
/// Invoked on peers by the reload signal that a configure or reconfigure
/// request broadcasts, so that a runtime change reaches every node instead of
/// only the one that served the admin request.
pub async fn reload_persisted_kms_config() -> Result<(), String> {
    let Some(config) = load_kms_config().await else {
        return Err("no persisted KMS configuration is available".to_string());
    };

    let service_manager = kms_service_manager_from_context();
    if service_manager
        .get_config()
        .await
        .is_some_and(|current| kms_config_is_unchanged(&current, &config))
    {
        info!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_KMS,
            event = "kms_service_state",
            operation = "peer_reload",
            state = "already_current",
            "admin kms dynamic state"
        );
        return Ok(());
    }

    service_manager.reconfigure(config).await.map_err(|err| {
        error!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_KMS,
            event = "kms_service_state",
            operation = "peer_reload",
            state = "reload_failed",
            error = %err,
            "admin kms dynamic state"
        );
        format!("failed to apply persisted KMS configuration: {err}")
    })?;

    info!(
        component = LOG_COMPONENT_ADMIN,
        subsystem = LOG_SUBSYSTEM_KMS,
        event = "kms_service_state",
        operation = "peer_reload",
        state = "reconfigured",
        "admin kms dynamic state"
    );
    Ok(())
}

/// Ask every peer to adopt the configuration that was just persisted.
///
/// Returns the hosts that did not converge. Reporting is best effort by
/// design: this node has already switched and KMS configuration has no quorum
/// or authoritative holder to roll back to, so refusing the local change would
/// trade a bounded divergence window for an outage.
async fn broadcast_kms_config_reload() -> Vec<String> {
    let context = current_app_context();
    let Some(notification_sys) = current_notification_system_for_context(context.as_deref()) else {
        return Vec::new();
    };

    let mut unconverged = Vec::new();
    for outcome in notification_sys.reload_kms_config().await {
        let Some(err) = outcome.err else {
            continue;
        };
        let host = if outcome.host.is_empty() {
            "<unknown>".to_string()
        } else {
            outcome.host
        };
        warn!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_KMS,
            event = "kms_peer_config_reload_failed",
            peer = %host,
            result = "peer_reload_failed",
            error = %err,
            "admin kms dynamic state"
        );
        unconverged.push(host);
    }
    unconverged
}

/// Fold a best-effort peer reload result into the local outcome.
///
/// The local outcome stays successful whatever the peers reported: this node
/// has already switched, and a failed peer is named in the message so the
/// operator can act on the divergence instead of being told nothing happened.
fn local_success_with_peer_report(message: &str, unconverged: &[String]) -> (bool, String) {
    if unconverged.is_empty() {
        return (true, message.to_string());
    }
    (
        true,
        format!(
            "{message}. {} peer(s) did not reload the new configuration and keep serving their previous KMS configuration until they do: {}",
            unconverged.len(),
            unconverged.join(", ")
        ),
    )
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

        let audit = KmsAdminAudit::from_request(&req.extensions, &req.headers, &cred);

        audit.gate_admin(
            validate_admin_request(
                &req.headers,
                &cred,
                owner,
                false,
                kms_configure_actions(),
                req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
            )
            .await,
            KmsAdminOperation::Configure,
            None,
        )?;

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

        if let Err(e) = normalize_configure_request_secrets(&mut configure_request, existing_config.as_ref()) {
            return Ok(S3Response::new((StatusCode::BAD_REQUEST, Body::from(e))));
        }

        if let Err(e) = ensure_kms_request_persistable(&configure_request) {
            return Ok(S3Response::new((StatusCode::BAD_REQUEST, Body::from(e))));
        }

        // Convert request to KmsConfig
        let kms_config = configure_request.to_kms_config();

        let persisted_config = kms_config.clone();
        let (success, message, status) = match service_manager
            .configure_with_persistence(kms_config, || async move {
                save_kms_config(&persisted_config)
                    .await
                    .map_err(|error| rustfs_kms::KmsError::backend_error(format!("Failed to persist KMS configuration: {error}")))
            })
            .await
        {
            Ok(()) => {
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
                let unconverged = broadcast_kms_config_reload().await;
                let (success, message) = local_success_with_peer_report("KMS configured successfully", &unconverged);
                audit.finish(KmsAdminOperation::Configure, None, None);
                (success, message, status)
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
                audit.finish(KmsAdminOperation::Configure, None, Some(&e));
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

        let audit = KmsAdminAudit::from_request(&req.extensions, &req.headers, &cred);

        audit.gate_admin(
            validate_admin_request(
                &req.headers,
                &cred,
                owner,
                false,
                kms_service_control_actions(),
                req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
            )
            .await,
            KmsAdminOperation::Start,
            None,
        )?;

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
        let force = start_request.force.unwrap_or(false);
        let (success, message, status) = match service_manager.start_or_restart(force).await {
            Ok(rustfs_kms::KmsStartOutcome::Started) => {
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
                audit.finish(KmsAdminOperation::Start, None, None);
                (true, "KMS service started successfully".to_string(), status)
            }
            Ok(rustfs_kms::KmsStartOutcome::Restarted) => {
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
                audit.finish(KmsAdminOperation::Start, None, None);
                (true, "KMS service restarted successfully".to_string(), status)
            }
            Ok(rustfs_kms::KmsStartOutcome::AlreadyRunning) => {
                let status = service_manager.get_status().await;
                warn!(
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS,
                    event = "kms_service_state",
                    operation = "start",
                    state = "already_running",
                    "admin kms dynamic state"
                );
                // A refusal, not an outage: recorded as a failed attempt so the
                // trail shows the request without inventing a KMS error for it.
                audit.finish_with_class(KmsAdminOperation::Start, Some("invalid_operation"));
                (false, "KMS service is already running. Use force=true to restart.".to_string(), status)
            }
            Err(e) => {
                let error_msg = format!("Failed to start or restart KMS service: {e}");
                error!(
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS,
                    event = "kms_service_state",
                    operation = "start",
                    state = "start_failed",
                    error = %e,
                    "admin kms dynamic state"
                );
                audit.finish(KmsAdminOperation::Start, None, Some(&e));
                let status = service_manager.get_status().await;
                (false, error_msg, status)
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

        let audit = KmsAdminAudit::from_request(&req.extensions, &req.headers, &cred);

        audit.gate_admin(
            validate_admin_request(
                &req.headers,
                &cred,
                owner,
                false,
                kms_service_control_actions(),
                req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
            )
            .await,
            KmsAdminOperation::Stop,
            None,
        )?;

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
                audit.finish(KmsAdminOperation::Stop, None, None);
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
                audit.finish(KmsAdminOperation::Stop, None, Some(&e));
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

        let (status, config) = service_manager.get_redacted_state().await;

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

        let audit = KmsAdminAudit::from_request(&req.extensions, &req.headers, &cred);

        audit.gate_admin(
            validate_admin_request(
                &req.headers,
                &cred,
                owner,
                false,
                kms_configure_actions(),
                req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
            )
            .await,
            KmsAdminOperation::Reconfigure,
            None,
        )?;

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

        if let Err(e) = normalize_configure_request_secrets(&mut configure_request, existing_config.as_ref()) {
            return Ok(S3Response::new((StatusCode::BAD_REQUEST, Body::from(e))));
        }

        if let Err(e) = ensure_kms_request_persistable(&configure_request) {
            return Ok(S3Response::new((StatusCode::BAD_REQUEST, Body::from(e))));
        }

        // Convert request to KmsConfig
        let kms_config = configure_request.to_kms_config();

        let persisted_config = kms_config.clone();
        let (success, message, status) = match service_manager
            .reconfigure_with_persistence(kms_config, || async move {
                save_kms_config(&persisted_config)
                    .await
                    .map_err(|error| rustfs_kms::KmsError::backend_error(format!("Failed to persist KMS configuration: {error}")))
            })
            .await
        {
            Ok(()) => {
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
                let unconverged = broadcast_kms_config_reload().await;
                let (success, message) =
                    local_success_with_peer_report("KMS reconfigured and restarted successfully", &unconverged);
                audit.finish(KmsAdminOperation::Reconfigure, None, None);
                (success, message, status)
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
                audit.finish(KmsAdminOperation::Reconfigure, None, Some(&e));
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
    use super::{
        decode_persisted_kms_config, ensure_kms_config_persistable, ensure_kms_request_persistable, kms_config_fingerprint,
        kms_config_is_unchanged, kms_configure_actions, kms_service_control_actions, local_success_with_peer_report,
        normalize_configure_request_secrets, redacted_canonical_config,
    };
    use rustfs_policy::policy::action::{Action, AdminAction, KmsAction};
    use std::path::PathBuf;
    use tempfile::TempDir;

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

    #[test]
    fn persisted_beta5_local_config_retains_legacy_development_mode() {
        let temp_dir = TempDir::new().expect("create legacy local KMS directory");
        let config = rustfs_kms::KmsConfig::local(temp_dir.path().to_path_buf());
        let mut value = serde_json::to_value(config).expect("serialize local KMS config");
        value
            .as_object_mut()
            .expect("KMS config is a JSON object")
            .remove("allow_insecure_dev_defaults");

        let (config, migrated) = decode_persisted_kms_config(&serde_json::to_vec(&value).expect("serialize beta.5 config"))
            .expect("decode beta.5 persisted config");
        assert!(migrated);
        assert!(config.allow_insecure_dev_defaults);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn persisted_local_config_with_explicit_secure_mode_stays_secure() {
        let temp_dir = TempDir::new().expect("create secure local KMS directory");
        let config = rustfs_kms::KmsConfig::local(temp_dir.path().to_path_buf());

        let (config, migrated) = decode_persisted_kms_config(&serde_json::to_vec(&config).expect("serialize current config"))
            .expect("decode current persisted config");
        assert!(!migrated);
        assert!(!config.allow_insecure_dev_defaults);
        assert!(config.validate().is_err());
    }

    #[test]
    fn persisted_config_rejects_duplicate_security_field() {
        let temp_dir = TempDir::new().expect("create local KMS directory");
        let config = rustfs_kms::KmsConfig::local(temp_dir.path().to_path_buf());
        let serialized = serde_json::to_string(&config).expect("serialize current config");
        let duplicate = serialized.replacen('{', r#"{"allow_insecure_dev_defaults":true,"#, 1);

        assert!(decode_persisted_kms_config(duplicate.as_bytes()).is_err());
    }

    #[test]
    fn persisted_secure_local_config_without_legacy_field_stays_secure() {
        #[cfg(unix)]
        let key_dir = std::path::PathBuf::from("/var/lib/rustfs/kms");
        #[cfg(windows)]
        let key_dir = std::path::PathBuf::from(r"C:\rustfs-kms");
        let mut config = rustfs_kms::KmsConfig::local(key_dir);
        let rustfs_kms::BackendConfig::Local(local) = &mut config.backend_config else {
            panic!("local constructor must create local backend config");
        };
        local.master_key = Some("configured-master-key".to_string());
        let mut value = serde_json::to_value(config).expect("serialize secure local KMS config");
        value
            .as_object_mut()
            .expect("KMS config is a JSON object")
            .remove("allow_insecure_dev_defaults");

        let (config, migrated) = decode_persisted_kms_config(&serde_json::to_vec(&value).expect("serialize old config"))
            .expect("decode secure persisted config");
        assert!(!migrated);
        assert!(!config.allow_insecure_dev_defaults);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn static_kms_config_is_not_persisted_with_cluster_configuration() {
        use base64::Engine as _;

        let config = rustfs_kms::KmsConfig::static_kms(
            "static-key".to_string(),
            base64::engine::general_purpose::STANDARD.encode([0x5au8; 32]),
        );

        assert!(ensure_kms_config_persistable(&config).is_err());
    }

    #[test]
    fn local_reconfigure_preserves_hidden_master_key_for_same_directory() {
        let key_dir = PathBuf::from("/var/lib/rustfs/kms");
        let mut existing = rustfs_kms::KmsConfig::local(key_dir.clone());
        let rustfs_kms::BackendConfig::Local(existing_local) = &mut existing.backend_config else {
            panic!("local constructor must create local backend config");
        };
        existing_local.master_key = Some("stored-master-key".to_string());

        let mut request = rustfs_kms::ConfigureKmsRequest::Local(rustfs_kms::ConfigureLocalKmsRequest {
            key_dir,
            master_key: None,
            file_permissions: Some(0o600),
            default_key_id: Some("experience-key".to_string()),
            timeout_seconds: Some(30),
            retry_attempts: Some(3),
            enable_cache: Some(true),
            max_cached_keys: Some(1000),
            cache_ttl_seconds: Some(3600),
            allow_insecure_dev_defaults: Some(false),
        });

        normalize_configure_request_secrets(&mut request, Some(&existing)).expect("normalize local request");
        let rustfs_kms::ConfigureKmsRequest::Local(request) = request else {
            panic!("request must remain local");
        };
        assert_eq!(request.master_key.as_deref(), Some("stored-master-key"));
    }

    #[test]
    fn local_reconfigure_preserves_unspecified_legacy_file_permissions() {
        let key_dir = PathBuf::from("/var/lib/rustfs/kms");
        let mut existing = rustfs_kms::KmsConfig::local(key_dir.clone());
        let rustfs_kms::BackendConfig::Local(existing_local) = &mut existing.backend_config else {
            panic!("local constructor must create local backend config");
        };
        existing_local.master_key = Some("stored-master-key".to_string());
        existing_local.file_permissions = None;

        let mut request = rustfs_kms::ConfigureKmsRequest::Local(rustfs_kms::ConfigureLocalKmsRequest {
            key_dir,
            master_key: None,
            file_permissions: None,
            default_key_id: Some("experience-key".to_string()),
            timeout_seconds: None,
            retry_attempts: None,
            enable_cache: None,
            max_cached_keys: None,
            cache_ttl_seconds: None,
            allow_insecure_dev_defaults: Some(false),
        });

        normalize_configure_request_secrets(&mut request, Some(&existing)).expect("normalize legacy local request");
        let rustfs_kms::ConfigureKmsRequest::Local(request) = request else {
            panic!("request must remain local");
        };
        assert!(request.file_permissions.is_none());
    }

    #[test]
    fn local_reconfigure_does_not_reuse_master_key_for_different_directory() {
        let mut existing = rustfs_kms::KmsConfig::local(PathBuf::from("/var/lib/rustfs/kms"));
        let rustfs_kms::BackendConfig::Local(existing_local) = &mut existing.backend_config else {
            panic!("local constructor must create local backend config");
        };
        existing_local.master_key = Some("stored-master-key".to_string());

        let mut request = rustfs_kms::ConfigureKmsRequest::Local(rustfs_kms::ConfigureLocalKmsRequest {
            key_dir: PathBuf::from("/var/lib/rustfs/other-kms"),
            master_key: None,
            file_permissions: Some(0o600),
            default_key_id: None,
            timeout_seconds: None,
            retry_attempts: None,
            enable_cache: None,
            max_cached_keys: None,
            cache_ttl_seconds: None,
            allow_insecure_dev_defaults: Some(false),
        });

        let error = normalize_configure_request_secrets(&mut request, Some(&existing))
            .expect_err("changing the local key directory must be rejected");
        assert_eq!(error, "Changing the Local KMS key directory is not supported");
    }

    #[test]
    fn local_reconfigure_rejects_file_permission_and_master_key_changes() {
        let key_dir = PathBuf::from("/var/lib/rustfs/kms");
        let mut existing = rustfs_kms::KmsConfig::local(key_dir.clone());
        let rustfs_kms::BackendConfig::Local(existing_local) = &mut existing.backend_config else {
            panic!("local constructor must create local backend config");
        };
        existing_local.master_key = Some("stored-master-key".to_string());
        existing_local.file_permissions = Some(0o600);

        let request = |master_key, file_permissions| {
            rustfs_kms::ConfigureKmsRequest::Local(rustfs_kms::ConfigureLocalKmsRequest {
                key_dir: key_dir.clone(),
                master_key,
                file_permissions,
                default_key_id: None,
                timeout_seconds: None,
                retry_attempts: None,
                enable_cache: None,
                max_cached_keys: None,
                cache_ttl_seconds: None,
                allow_insecure_dev_defaults: Some(false),
            })
        };

        let mut permissions_change = request(None, Some(0o666));
        let permissions_error = normalize_configure_request_secrets(&mut permissions_change, Some(&existing))
            .expect_err("changing file permissions must be rejected");
        assert_eq!(permissions_error, "Changing Local KMS file permissions is not supported");

        let mut master_key_change = request(Some("replacement-master-key".to_string()), Some(0o600));
        let master_key_error = normalize_configure_request_secrets(&mut master_key_change, Some(&existing))
            .expect_err("changing the master key must be rejected");
        assert_eq!(master_key_error, "Changing the Local KMS master key is not supported");

        let mut backend_change = rustfs_kms::ConfigureKmsRequest::Static(rustfs_kms::ConfigureStaticKmsRequest {
            key_id: "static-key".to_string(),
            secret_key: "not-used-by-normalization".to_string(),
            default_key_id: None,
            timeout_seconds: None,
            retry_attempts: None,
            enable_cache: None,
            max_cached_keys: None,
            cache_ttl_seconds: None,
            allow_insecure_dev_defaults: None,
        });
        let backend_error = normalize_configure_request_secrets(&mut backend_change, Some(&existing))
            .expect_err("changing from the local backend must be rejected");
        assert_eq!(backend_error, "Changing from the Local KMS backend is not supported");
    }

    fn aws_configure_request(region: &str) -> rustfs_kms::ConfigureKmsRequest {
        rustfs_kms::ConfigureKmsRequest::Aws(rustfs_kms::ConfigureAwsKmsRequest {
            region: region.to_string(),
            endpoint_url: None,
            default_key_id: Some("arn:aws:kms:us-east-1:111122223333:key/1234abcd".to_string()),
            timeout_seconds: None,
            retry_attempts: None,
            enable_cache: None,
            max_cached_keys: None,
            cache_ttl_seconds: None,
            allow_insecure_dev_defaults: None,
        })
    }

    /// The AWS backend holds no credential material of its own, so its
    /// configuration is safe to persist cluster-wide and needs nothing carried
    /// over from a previous configuration.
    #[test]
    fn aws_configure_request_is_persistable_and_needs_no_existing_credentials() {
        let mut request = aws_configure_request("us-east-1");
        normalize_configure_request_secrets(&mut request, None).expect("aws request needs no existing credentials");
        assert!(ensure_kms_request_persistable(&request).is_ok());

        let config = request.to_kms_config();
        assert!(ensure_kms_config_persistable(&config).is_ok());

        let canonical = redacted_canonical_config(&config).expect("aws configuration should serialize");
        assert!(canonical.contains("us-east-1"), "the pinned region must drive the fingerprint");
        for credential_field in ["access_key", "secret_access_key", "session_token"] {
            assert!(
                !canonical.contains(credential_field),
                "aws configuration must carry no credential material: {canonical}"
            );
        }
    }

    /// Two nodes cannot be allowed to read the same AWS configuration as
    /// different regions, so the pinned region has to be part of what a
    /// fingerprint comparison would flag as a split.
    #[test]
    fn aws_config_fingerprint_tracks_the_pinned_region() {
        let first = kms_config_fingerprint(&aws_configure_request("us-east-1").to_kms_config())
            .expect("fingerprint should be computable");
        let same = kms_config_fingerprint(&aws_configure_request("us-east-1").to_kms_config())
            .expect("fingerprint should be computable");
        let other = kms_config_fingerprint(&aws_configure_request("eu-central-1").to_kms_config())
            .expect("fingerprint should be computable");

        assert_eq!(first, same);
        assert_ne!(first, other);
    }

    #[test]
    fn local_backend_cannot_be_switched_to_aws() {
        let mut existing = rustfs_kms::KmsConfig::local(PathBuf::from("/var/lib/rustfs/kms"));
        let rustfs_kms::BackendConfig::Local(existing_local) = &mut existing.backend_config else {
            panic!("local constructor must create local backend config");
        };
        existing_local.master_key = Some("stored-master-key".to_string());

        let mut request = aws_configure_request("us-east-1");
        let error = normalize_configure_request_secrets(&mut request, Some(&existing))
            .expect_err("switching away from the local backend must be rejected");
        assert_eq!(error, "Changing from the Local KMS backend is not supported");
    }

    const VAULT_TOKEN: &str = "hvs-super-secret-token";

    fn vault_config(address: &str, token: &str) -> rustfs_kms::KmsConfig {
        let vault = rustfs_kms::config::VaultConfig {
            address: address.to_string(),
            auth_method: rustfs_kms::config::VaultAuthMethod::Token {
                token: token.to_string(),
            },
            ..Default::default()
        };
        rustfs_kms::KmsConfig {
            backend: rustfs_kms::KmsBackend::VaultKv2,
            backend_config: rustfs_kms::BackendConfig::VaultKv2(Box::new(vault)),
            ..Default::default()
        }
    }

    #[test]
    fn config_fingerprint_input_carries_no_credentials() {
        let canonical = redacted_canonical_config(&vault_config("https://vault.internal:8200", VAULT_TOKEN))
            .expect("vault configuration should serialize");
        assert!(!canonical.contains(VAULT_TOKEN), "fingerprint input must not carry the vault token");
        assert!(canonical.contains(super::REDACTED_CONFIG_VALUE));
        assert!(
            canonical.contains("vault.internal"),
            "non-secret configuration must still drive the fingerprint"
        );
    }

    #[test]
    fn config_fingerprint_agrees_across_nodes_holding_the_same_configuration() {
        let first = kms_config_fingerprint(&vault_config("https://vault.internal:8200", VAULT_TOKEN))
            .expect("fingerprint should be computable");
        let second = kms_config_fingerprint(&vault_config("https://vault.internal:8200", VAULT_TOKEN))
            .expect("fingerprint should be computable");
        assert_eq!(first, second);

        let other_backend = kms_config_fingerprint(&vault_config("https://vault.other:8200", VAULT_TOKEN))
            .expect("fingerprint should be computable");
        assert_ne!(first, other_backend, "a different backend address must be visible as a split");

        // Credentials are redacted before hashing, so a rotated token is not a
        // split. Documented here so the trade-off is not silently regressed.
        let rotated_token = kms_config_fingerprint(&vault_config("https://vault.internal:8200", "hvs-rotated-token"))
            .expect("fingerprint should be computable");
        assert_eq!(first, rotated_token);
    }

    #[test]
    fn peer_reload_replays_only_when_the_persisted_configuration_moved() {
        let current = vault_config("https://vault.internal:8200", VAULT_TOKEN);
        assert!(kms_config_is_unchanged(
            &current,
            &vault_config("https://vault.internal:8200", VAULT_TOKEN)
        ));
        assert!(!kms_config_is_unchanged(&current, &vault_config("https://vault.other:8200", VAULT_TOKEN)));
        assert!(
            !kms_config_is_unchanged(&current, &vault_config("https://vault.internal:8200", "hvs-rotated-token")),
            "a credential-only change must still be adopted, unlike the redacted fingerprint"
        );
    }

    #[test]
    fn peer_reload_failures_are_reported_without_failing_the_local_change() {
        let (success, message) = local_success_with_peer_report("KMS configured successfully", &[]);
        assert!(success);
        assert_eq!(message, "KMS configured successfully");

        let (success, message) = local_success_with_peer_report(
            "KMS configured successfully",
            &["10.0.0.2:9000".to_string(), "10.0.0.3:9000".to_string()],
        );
        assert!(success, "a peer that failed to reload must not fail the node that already switched");
        assert!(message.contains("2 peer(s)"));
        assert!(message.contains("10.0.0.2:9000"));
        assert!(message.contains("10.0.0.3:9000"));
    }
}
