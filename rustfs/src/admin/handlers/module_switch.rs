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

use crate::admin::{
    auth::validate_admin_request,
    router::{AdminOperation, Operation, S3Router},
};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{
    ADMIN_PREFIX, ModuleSwitchSnapshot, ModuleSwitchSource, PersistedModuleSwitches, RemoteAddr, current_module_switch_snapshot,
    init_event_notifier, refresh_audit_module_enabled, refresh_notify_module_enabled,
    refresh_persisted_module_switches_from_store, save_persisted_module_switches_to_store, shutdown_event_notifier,
    start_audit_system, stop_audit_system, validate_module_switch_update,
};
use http::{HeaderMap, StatusCode};
use hyper::Method;
use matchit::Params;
use rustfs_audit::AuditError;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::{Body, S3Request, S3Response, S3Result, header::CONTENT_TYPE, s3_error};
use serde::{Deserialize, Serialize};

pub fn register_module_switch_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/module-switches").as_str(),
        AdminOperation(&GetModuleSwitchesHandler {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/module-switches").as_str(),
        AdminOperation(&UpdateModuleSwitchesHandler {}),
    )?;

    Ok(())
}

#[derive(Debug, Deserialize)]
struct UpdateModuleSwitchesRequest {
    notify_enabled: bool,
    audit_enabled: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
struct ModuleSwitchesResponse {
    notify_enabled: bool,
    audit_enabled: bool,
    persisted_notify_enabled: bool,
    persisted_audit_enabled: bool,
    notify_source: ModuleSwitchSource,
    audit_source: ModuleSwitchSource,
}

impl From<ModuleSwitchSnapshot> for ModuleSwitchesResponse {
    fn from(value: ModuleSwitchSnapshot) -> Self {
        Self {
            notify_enabled: value.notify_enabled,
            audit_enabled: value.audit_enabled,
            persisted_notify_enabled: value.persisted_notify_enabled,
            persisted_audit_enabled: value.persisted_audit_enabled,
            notify_source: value.notify_source,
            audit_source: value.audit_source,
        }
    }
}

fn build_response<T: Serialize>(
    status: StatusCode,
    body: &T,
    request_id: Option<&http::HeaderValue>,
) -> S3Result<S3Response<(StatusCode, Body)>> {
    let data = serde_json::to_vec(body).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
    let mut header = HeaderMap::new();
    header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
    if let Some(v) = request_id {
        header.insert("x-request-id", v.clone());
    }
    Ok(S3Response::with_headers((status, Body::from(data)), header))
}

async fn authorize_module_switch_request(req: &S3Request<Body>, action: AdminAction) -> S3Result<()> {
    let Some(input_cred) = &req.credentials else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };

    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    validate_admin_request(
        &req.headers,
        &cred,
        owner,
        false,
        vec![Action::AdminAction(action)],
        req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
    )
    .await
}

async fn refresh_module_switch_snapshot() -> S3Result<ModuleSwitchSnapshot> {
    // Re-read persisted values before every console read/write so the current
    // node reflects the latest cluster-wide state instead of stale atomics.
    refresh_persisted_module_switches_from_store()
        .await
        .map_err(|e| s3_error!(InternalError, "failed to reload persisted module switches: {}", e))?;
    refresh_notify_module_enabled();
    refresh_audit_module_enabled();
    Ok(current_module_switch_snapshot())
}

pub struct GetModuleSwitchesHandler {}

#[async_trait::async_trait]
impl Operation for GetModuleSwitchesHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_module_switch_request(&req, AdminAction::ServerInfoAdminAction).await?;
        let snapshot = refresh_module_switch_snapshot().await?;
        build_response(StatusCode::OK, &ModuleSwitchesResponse::from(snapshot), req.headers.get("x-request-id"))
    }
}

pub struct UpdateModuleSwitchesHandler {}

#[async_trait::async_trait]
impl Operation for UpdateModuleSwitchesHandler {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_module_switch_request(&req, AdminAction::ConfigUpdateAdminAction).await?;
        refresh_persisted_module_switches_from_store()
            .await
            .map_err(|e| s3_error!(InternalError, "failed to reload persisted module switches: {}", e))?;

        let body = req
            .input
            .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;
        if body.is_empty() {
            return Err(s3_error!(InvalidRequest, "request body is required"));
        }

        let request: UpdateModuleSwitchesRequest =
            serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))?;
        let switches = PersistedModuleSwitches {
            notify_enabled: request.notify_enabled,
            audit_enabled: request.audit_enabled,
        };

        // Reject conflicting writes early so operators do not persist a console
        // value that still cannot win over an explicit env override.
        if let Err(err) = validate_module_switch_update(switches) {
            let _ = refresh_module_switch_snapshot().await;
            return Err(s3_error!(InvalidRequest, "{err}"));
        }

        save_persisted_module_switches_to_store(switches)
            .await
            .map_err(|e| s3_error!(InternalError, "failed to save module switches: {}", e))?;

        // Apply the new effective values immediately on this node so the console
        // response reflects the runtime state after to write completes.
        if refresh_notify_module_enabled() {
            init_event_notifier().await;
        } else {
            shutdown_event_notifier().await;
        }

        if refresh_audit_module_enabled() {
            match start_audit_system().await {
                Ok(()) | Err(AuditError::AlreadyInitialized) => {}
                Err(e) => return Err(s3_error!(InternalError, "failed to apply audit module switch: {}", e)),
            }
        } else {
            stop_audit_system()
                .await
                .map_err(|e| s3_error!(InternalError, "failed to stop audit module after switch update: {}", e))?;
        }

        let snapshot = current_module_switch_snapshot();
        build_response(StatusCode::OK, &ModuleSwitchesResponse::from(snapshot), req.headers.get("x-request-id"))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn module_switch_handlers_require_admin_authorization_contract() {
        let src = include_str!("module_switch.rs");
        let get_block = extract_block_between_markers(
            src,
            "impl Operation for GetModuleSwitchesHandler",
            "pub struct UpdateModuleSwitchesHandler",
        );
        let put_block = extract_block_between_markers(src, "impl Operation for UpdateModuleSwitchesHandler", "#[cfg(test)]");

        assert!(
            get_block.contains("authorize_module_switch_request(&req, AdminAction::ServerInfoAdminAction).await?;"),
            "module switch GET should require ServerInfoAdminAction"
        );
        assert!(
            put_block.contains("authorize_module_switch_request(&req, AdminAction::ConfigUpdateAdminAction).await?;"),
            "module switch PUT should require ConfigUpdateAdminAction"
        );
    }

    fn extract_block_between_markers<'a>(src: &'a str, start_marker: &str, end_marker: &str) -> &'a str {
        let start = src
            .find(start_marker)
            .unwrap_or_else(|| panic!("Expected marker `{start_marker}` in source"));
        let after_start = &src[start..];
        let end = after_start
            .find(end_marker)
            .unwrap_or_else(|| panic!("Expected end marker `{end_marker}` in source"));
        &after_start[..end]
    }
}
