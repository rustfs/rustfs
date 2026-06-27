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

use crate::admin::auth::{authenticate_request, validate_admin_request};
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::current_object_store_handle;
use crate::admin::storage_api::access::spawn_traced;
use crate::admin::storage_api::bucket::is_reserved_or_invalid_bucket;
use crate::admin::storage_api::bucket::utils::is_valid_object_prefix;
use crate::admin::storage_api::contract::heal::HealOperations as _;
use crate::server::ADMIN_PREFIX;
use crate::server::RemoteAddr;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Uri};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_common::heal_channel::{HealChannelPriority, HealChannelRequest, HealOpts, HealRequestSource};
use rustfs_config::MAX_HEAL_REQUEST_SIZE;
use rustfs_heal::heal::utils::format_set_disk_id;
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_scanner::scanner::{BackgroundHealInfo, read_background_heal_info};
use rustfs_utils::path::path_join;
use s3s::header::{CONTENT_LENGTH, CONTENT_TYPE};
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::mpsc;
use tracing::{info, warn};

const LOG_COMPONENT_ADMIN_API: &str = "admin_api";
const LOG_SUBSYSTEM_HEAL_ADMIN: &str = "heal_admin";
const EVENT_ADMIN_REQUEST_REJECTED: &str = "admin_request_rejected";
const EVENT_ADMIN_REQUEST_FAILED: &str = "admin_request_failed";
const EVENT_ADMIN_RESPONSE_EMITTED: &str = "admin_response_emitted";

#[derive(Debug, Default, Serialize, Deserialize)]
struct HealInitParams {
    bucket: String,
    obj_prefix: String,
    hs: HealOpts,
    client_token: String,
    force_start: bool,
    force_stop: bool,
}

fn extract_heal_init_params(body: &Bytes, uri: &Uri, params: Params<'_, '_>) -> S3Result<HealInitParams> {
    let mut hip = HealInitParams {
        bucket: params.get("bucket").map(|s| s.to_string()).unwrap_or_default(),
        obj_prefix: params.get("prefix").map(|s| s.to_string()).unwrap_or_default(),
        ..Default::default()
    };
    validate_heal_target(&hip.bucket, &hip.obj_prefix)?;

    if let Some(query) = uri.query() {
        let mut seen = HashSet::with_capacity(3);
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            match key.as_ref() {
                "clientToken" => {
                    if !seen.insert("clientToken") {
                        return Err(s3_error!(InvalidArgument, "duplicate heal query parameter"));
                    }
                    hip.client_token = value.into_owned();
                }
                "forceStart" => {
                    if !seen.insert("forceStart") {
                        return Err(s3_error!(InvalidArgument, "duplicate heal query parameter"));
                    }
                    hip.force_start = parse_heal_query_bool(value.as_ref())?;
                }
                "forceStop" => {
                    if !seen.insert("forceStop") {
                        return Err(s3_error!(InvalidArgument, "duplicate heal query parameter"));
                    }
                    hip.force_stop = parse_heal_query_bool(value.as_ref())?;
                }
                _ => return Err(s3_error!(InvalidArgument, "unknown heal query parameter")),
            }
        }
    }

    if hip.force_start && (hip.force_stop || !hip.client_token.is_empty()) {
        return Err(s3_error!(
            InvalidRequest,
            "invalid combination of clientToken, forceStart, and forceStop parameters"
        ));
    }

    if hip.client_token.is_empty() {
        hip.hs = serde_json::from_slice(body).map_err(|e| {
            warn!(
                event = EVENT_ADMIN_REQUEST_REJECTED,
                component = LOG_COMPONENT_ADMIN_API,
                subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
                operation = "heal_request_init",
                result = "rejected",
                reason = "request_body_parse_failed",
                error = ?e,
                "admin request rejected"
            );
            s3_error!(InvalidRequest, "err request body parse")
        })?;
    }

    Ok(hip)
}

fn parse_heal_query_bool(value: &str) -> S3Result<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(s3_error!(InvalidArgument, "invalid heal query boolean")),
    }
}

fn validate_heal_target(bucket: &str, obj_prefix: &str) -> S3Result<()> {
    if bucket.is_empty() && !obj_prefix.is_empty() {
        return Err(s3_error!(InvalidRequest, "invalid bucket name"));
    }
    if !bucket.is_empty() && is_reserved_or_invalid_bucket(bucket, false) {
        return Err(s3_error!(InvalidRequest, "invalid bucket name"));
    }
    if !is_valid_object_prefix(obj_prefix) {
        return Err(s3_error!(InvalidRequest, "invalid object name"));
    }

    Ok(())
}

pub fn register_heal_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    // Some APIs are only available in EC mode
    // if is_dist_erasure().await || is_erasure().await {
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/heal/").as_str(),
        AdminOperation(&HealHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/heal/{bucket}").as_str(),
        AdminOperation(&HealHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/heal/{bucket}/{prefix}").as_str(),
        AdminOperation(&HealHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/background-heal/status").as_str(),
        AdminOperation(&BackgroundHealStatusHandler {}),
    )?;

    Ok(())
}

#[derive(Default)]
struct HealResp {
    resp_bytes: Vec<u8>,
    api_err: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HealStartSuccess {
    client_token: String,
    client_address: String,
    start_time: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HealTaskStatus {
    summary: String,
    #[serde(rename = "detail")]
    failure_detail: String,
    start_time: String,
    #[serde(rename = "settings")]
    heal_settings: HealOpts,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    items: Vec<rustfs_madmin::heal_commands::HealResultItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    progress: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BackgroundHealStatus<'a> {
    #[serde(flatten)]
    info: &'a BackgroundHealInfo,
    heal_queue_length: u64,
    heal_active_tasks: u64,
    heal_operations: rustfs_heal::HealOperationsSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    progress: Option<BackgroundHealProgress>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BackgroundHealProgress {
    objects_scanned: u64,
    objects_healed: u64,
    objects_failed: u64,
    bytes_processed: u64,
}

impl From<rustfs_heal::HealProgress> for BackgroundHealProgress {
    fn from(progress: rustfs_heal::HealProgress) -> Self {
        Self {
            objects_scanned: progress.objects_scanned,
            objects_healed: progress.objects_healed,
            objects_failed: progress.objects_failed,
            bytes_processed: progress.bytes_processed,
        }
    }
}

#[derive(Debug, Deserialize)]
struct HealTaskStatusPayload {
    summary: String,
    #[serde(default)]
    items: Vec<rustfs_madmin::heal_commands::HealResultItem>,
    #[serde(default)]
    progress: Option<serde_json::Value>,
}

fn map_heal_response(result: Option<HealResp>) -> S3Result<(StatusCode, Vec<u8>)> {
    match result {
        Some(result) => {
            if let Some(err) = result.api_err {
                return Err(s3_error!(InternalError, "{err}"));
            }

            if result.resp_bytes.is_empty() {
                return Err(s3_error!(InternalError, "heal response body is empty"));
            }

            Ok((StatusCode::OK, result.resp_bytes))
        }
        None => Err(s3_error!(InternalError, "heal channel closed unexpectedly")),
    }
}

fn current_rfc3339_time() -> S3Result<String> {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .map_err(|e| s3_error!(InternalError, "failed to format heal timestamp: {e}"))
}

fn encode_json<T: Serialize>(value: &T) -> S3Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| s3_error!(InternalError, "failed to serialize heal response: {e}"))
}

fn encode_heal_start_success(client_token: String, client_address: String) -> S3Result<Vec<u8>> {
    encode_json(&HealStartSuccess {
        client_token,
        client_address,
        start_time: current_rfc3339_time()?,
    })
}

fn encode_heal_task_status(
    summary: String,
    failure_detail: String,
    heal_settings: HealOpts,
    items: Vec<rustfs_madmin::heal_commands::HealResultItem>,
    progress: Option<serde_json::Value>,
) -> S3Result<Vec<u8>> {
    encode_json(&HealTaskStatus {
        summary,
        failure_detail,
        start_time: current_rfc3339_time()?,
        heal_settings,
        items,
        progress,
    })
}

fn build_heal_channel_request(hip: &HealInitParams) -> HealChannelRequest {
    let root_erasure_set_target =
        hip.bucket.is_empty() && hip.obj_prefix.is_empty() && matches!((hip.hs.pool, hip.hs.set), (Some(_), Some(_)));
    let root_cluster_target = hip.bucket.is_empty() && hip.obj_prefix.is_empty() && hip.hs.pool.is_none() && hip.hs.set.is_none();
    let recursive = if (!hip.bucket.is_empty() && hip.obj_prefix.is_empty()) || root_erasure_set_target {
        true
    } else {
        hip.hs.recursive
    };
    let mut heal_request = rustfs_common::heal_channel::create_heal_request(
        hip.bucket.clone(),
        if hip.obj_prefix.is_empty() {
            None
        } else {
            Some(hip.obj_prefix.clone())
        },
        hip.force_start,
        Some(HealChannelPriority::High),
    );

    heal_request.pool_index = hip.hs.pool;
    heal_request.set_index = hip.hs.set;
    if root_erasure_set_target && let (Some(pool), Some(set)) = (hip.hs.pool, hip.hs.set) {
        heal_request.disk = Some(format_set_disk_id(pool, set));
    }
    heal_request.scan_mode = Some(hip.hs.scan_mode);
    heal_request.remove_corrupted = Some(hip.hs.remove);
    heal_request.recreate_missing = Some(hip.hs.recreate);
    heal_request.update_parity = Some(hip.hs.update_parity);
    heal_request.recursive = Some(recursive || root_cluster_target);
    heal_request.dry_run = Some(hip.hs.dry_run);
    heal_request.source = HealRequestSource::Admin;
    heal_request
}

fn heal_channel_response_status(
    response: &rustfs_common::heal_channel::HealChannelResponse,
) -> (String, Vec<rustfs_madmin::heal_commands::HealResultItem>, Option<serde_json::Value>) {
    let Some(data) = response.data.as_deref() else {
        return ("running".to_string(), Vec::new(), None);
    };

    if let Ok(payload) = serde_json::from_slice::<HealTaskStatusPayload>(data)
        && !payload.summary.is_empty()
    {
        return (payload.summary, payload.items, payload.progress);
    }

    let summary = std::str::from_utf8(data)
        .ok()
        .filter(|summary| !summary.is_empty())
        .unwrap_or("running")
        .to_string();
    (summary, Vec::new(), None)
}

#[cfg(test)]
fn heal_channel_response_summary(response: &rustfs_common::heal_channel::HealChannelResponse) -> String {
    heal_channel_response_status(response).0
}

#[cfg(test)]
fn heal_channel_response_items(
    response: &rustfs_common::heal_channel::HealChannelResponse,
) -> Vec<rustfs_madmin::heal_commands::HealResultItem> {
    heal_channel_response_status(response).1
}

#[cfg(test)]
fn heal_channel_response_progress(response: &rustfs_common::heal_channel::HealChannelResponse) -> Option<serde_json::Value> {
    heal_channel_response_status(response).2
}

fn encode_background_heal_status(
    info: &BackgroundHealInfo,
    heal_operations: rustfs_heal::HealOperationsSnapshot,
    progress: Option<BackgroundHealProgress>,
) -> S3Result<Vec<u8>> {
    let status = BackgroundHealStatus {
        info,
        heal_queue_length: heal_operations.queue_length,
        heal_active_tasks: heal_operations.active_tasks,
        heal_operations,
        progress,
    };
    serde_json::to_vec(&status).map_err(|e| {
        warn!(
            event = EVENT_ADMIN_REQUEST_FAILED,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
            operation = "background_heal_status",
            result = "failed",
            reason = "serialize_background_heal_status_failed",
            error = %e,
            "admin request failed"
        );
        s3_error!(InternalError, "failed to serialize background heal status: {e}")
    })
}

fn validate_heal_request_mode(hip: &HealInitParams) -> S3Result<()> {
    if hip.bucket.is_empty() && hip.client_token.is_empty() && !hip.force_stop {
        return match (hip.hs.pool, hip.hs.set) {
            (Some(_), Some(_)) => Ok(()),
            (Some(_), None) | (None, Some(_)) => {
                Err(s3_error!(InvalidRequest, "root heal erasure-set target requires both pool and set"))
            }
            (None, None) if hip.hs.recursive => Ok(()),
            (None, None) => Err(s3_error!(InvalidRequest, "root heal requires recursive=true or a bucket target")),
        };
    }

    Ok(())
}

fn should_handle_root_heal_directly(_hip: &HealInitParams) -> bool {
    false
}

fn map_root_heal_status(heal_err: Option<crate::admin::storage_api::error::Error>) -> S3Result<()> {
    match heal_err {
        None => Ok(()),
        Some(crate::admin::storage_api::error::StorageError::NoHealRequired) => {
            info!(
                event = EVENT_ADMIN_RESPONSE_EMITTED,
                component = LOG_COMPONENT_ADMIN_API,
                subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
                operation = "root_heal",
                result = "success",
                state = "no_heal_required",
                "admin response emitted"
            );
            Ok(())
        }
        Some(err) => {
            warn!(
                event = EVENT_ADMIN_REQUEST_FAILED,
                component = LOG_COMPONENT_ADMIN_API,
                subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
                operation = "root_heal",
                result = "failed",
                reason = "root_heal_failed",
                error = %err,
                "admin request failed"
            );
            Err(s3_error!(InternalError, "root heal failed: {err}"))
        }
    }
}

fn json_response(status: StatusCode, body: Vec<u8>) -> S3Response<(StatusCode, Body)> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Ok(value) = HeaderValue::from_str(&body.len().to_string()) {
        headers.insert(CONTENT_LENGTH, value);
    }
    S3Response::with_headers((status, Body::from(body)), headers)
}

async fn validate_heal_admin_request(req: &S3Request<Body>) -> S3Result<()> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };

    let (cred, owner) = authenticate_request(&req.headers, &req.uri, input_cred).await?;

    validate_admin_request(
        &req.headers,
        &cred,
        owner,
        false,
        vec![Action::AdminAction(AdminAction::HealAdminAction)],
        req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
    )
    .await
}

pub struct HealHandler {}

#[async_trait::async_trait]
impl Operation for HealHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_heal_admin_request(&req).await?;
        let client_address = req
            .extensions
            .get::<Option<RemoteAddr>>()
            .and_then(|opt| opt.map(|addr| addr.0.to_string()))
            .unwrap_or_default();
        let mut input = req.input;
        let bytes = match input.store_all_limited(MAX_HEAL_REQUEST_SIZE).await {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    event = EVENT_ADMIN_REQUEST_REJECTED,
                    component = LOG_COMPONENT_ADMIN_API,
                    subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
                    operation = "heal_request_body",
                    result = "rejected",
                    reason = "request_body_read_failed",
                    error = ?e,
                    "admin request rejected"
                );
                return Err(s3_error!(InvalidRequest, "heal request body too large or failed to read"));
            }
        };
        let hip = extract_heal_init_params(&bytes, &req.uri, params)?;
        // The heal channel currently models bucket/object work. Root heal reuses the
        // existing format-heal path directly so `/v3/heal/` is accepted intentionally.
        if should_handle_root_heal_directly(&hip) {
            let Some(store) = current_object_store_handle() else {
                warn!(
                    event = EVENT_ADMIN_REQUEST_FAILED,
                    component = LOG_COMPONENT_ADMIN_API,
                    subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
                    operation = "root_heal",
                    result = "failed",
                    reason = "server_not_initialized",
                    "admin request failed"
                );
                return Err(s3_error!(InternalError, "server not initialized"));
            };

            let (_, heal_err) = store.heal_format(hip.hs.dry_run).await.map_err(|e| {
                warn!(
                    event = EVENT_ADMIN_REQUEST_FAILED,
                    component = LOG_COMPONENT_ADMIN_API,
                    subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
                    operation = "root_heal",
                    result = "failed",
                    reason = "heal_format_failed",
                    error = %e,
                    "admin request failed"
                );
                s3_error!(InternalError, "root heal failed: {e}")
            })?;

            map_root_heal_status(heal_err)?;
            let body = encode_heal_start_success("root-heal".to_string(), client_address)?;
            info!(
                event = EVENT_ADMIN_RESPONSE_EMITTED,
                component = LOG_COMPONENT_ADMIN_API,
                subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
                operation = "root_heal",
                result = "success",
                state = "started",
                "admin response emitted"
            );

            return Ok(json_response(StatusCode::OK, body));
        }
        validate_heal_request_mode(&hip)?;
        let response_operation = if hip.force_stop {
            "cancel_heal"
        } else if !hip.client_token.is_empty() && !hip.force_start {
            "query_heal_status"
        } else {
            "start_heal"
        };

        let heal_path = path_join(&[PathBuf::from(hip.bucket.clone()), PathBuf::from(hip.obj_prefix.clone())]);
        let (tx, mut rx) = mpsc::channel(1);

        if !hip.client_token.is_empty() && !hip.force_start && !hip.force_stop {
            // Query heal status
            let tx_clone = tx.clone();
            let heal_path_str = heal_path.to_str().unwrap_or_default().to_string();
            let client_token = hip.client_token.clone();
            spawn_traced(async move {
                match rustfs_common::heal_channel::query_heal_status(heal_path_str, client_token).await {
                    Ok(response) if response.success => {
                        let (summary, items, progress) = heal_channel_response_status(&response);
                        let resp_bytes = encode_heal_task_status(
                            summary,
                            response.error.unwrap_or_default(),
                            HealOpts::default(),
                            items,
                            progress,
                        );
                        match resp_bytes {
                            Ok(resp_bytes) => {
                                let _ = tx_clone
                                    .send(HealResp {
                                        resp_bytes,
                                        ..Default::default()
                                    })
                                    .await;
                            }
                            Err(e) => {
                                let _ = tx_clone
                                    .send(HealResp {
                                        api_err: Some(e.to_string()),
                                        ..Default::default()
                                    })
                                    .await;
                            }
                        }
                    }
                    Ok(response) => {
                        let _ = tx_clone
                            .send(HealResp {
                                api_err: Some(response.error.unwrap_or_else(|| "query heal status failed".to_string())),
                                ..Default::default()
                            })
                            .await;
                    }
                    Err(e) => {
                        let _ = tx_clone
                            .send(HealResp {
                                api_err: Some(format!("query heal status failed: {e}")),
                                ..Default::default()
                            })
                            .await;
                    }
                }
            });
        } else if hip.force_stop {
            // Cancel heal task
            let tx_clone = tx.clone();
            let heal_path_str = heal_path.to_str().unwrap_or_default().to_string();
            let client_token = hip.client_token.clone();
            let client_address = client_address.clone();
            let heal_settings = hip.hs;
            spawn_traced(async move {
                match rustfs_common::heal_channel::cancel_heal_task(heal_path_str, client_token.clone()).await {
                    Ok(response) if response.success => {
                        let resp_bytes = if client_token.is_empty() {
                            encode_heal_start_success(response.request_id, client_address)
                        } else {
                            let (summary, items, progress) = heal_channel_response_status(&response);
                            encode_heal_task_status(summary, response.error.unwrap_or_default(), heal_settings, items, progress)
                        };
                        match resp_bytes {
                            Ok(resp_bytes) => {
                                let _ = tx_clone
                                    .send(HealResp {
                                        resp_bytes,
                                        ..Default::default()
                                    })
                                    .await;
                            }
                            Err(e) => {
                                let _ = tx_clone
                                    .send(HealResp {
                                        api_err: Some(e.to_string()),
                                        ..Default::default()
                                    })
                                    .await;
                            }
                        }
                    }
                    Ok(response) => {
                        let _ = tx_clone
                            .send(HealResp {
                                api_err: Some(response.error.unwrap_or_else(|| "cancel heal task failed".to_string())),
                                ..Default::default()
                            })
                            .await;
                    }
                    Err(e) => {
                        let _ = tx_clone
                            .send(HealResp {
                                api_err: Some(format!("cancel heal task failed: {e}")),
                                ..Default::default()
                            })
                            .await;
                    }
                }
            });
        } else if hip.client_token.is_empty() {
            // Use new heal channel mechanism
            let tx_clone = tx.clone();
            let client_address = client_address.clone();
            spawn_traced(async move {
                // Create heal request through channel
                let heal_request = build_heal_channel_request(&hip);
                let client_token = heal_request.id.clone();

                match rustfs_common::heal_channel::send_heal_request_with_admission(heal_request).await {
                    Ok(admission) if admission.is_admitted() => {
                        let resp_bytes = encode_heal_start_success(client_token, client_address);
                        match resp_bytes {
                            Ok(resp_bytes) => {
                                let _ = tx_clone
                                    .send(HealResp {
                                        resp_bytes,
                                        ..Default::default()
                                    })
                                    .await;
                            }
                            Err(e) => {
                                let _ = tx_clone
                                    .send(HealResp {
                                        api_err: Some(e.to_string()),
                                        ..Default::default()
                                    })
                                    .await;
                            }
                        }
                    }
                    Ok(admission) => {
                        let _ = tx_clone
                            .send(HealResp {
                                api_err: Some(format!(
                                    "heal request not admitted: admission={}, reason={}",
                                    admission.result_label(),
                                    admission.reason_label()
                                )),
                                ..Default::default()
                            })
                            .await;
                    }
                    Err(e) => {
                        // Error - send error response
                        let _ = tx_clone
                            .send(HealResp {
                                api_err: Some(format!("send heal request failed: {e}")),
                                ..Default::default()
                            })
                            .await;
                    }
                }
            });
        }

        let (status, body) = map_heal_response(rx.recv().await)?;
        info!(
            event = EVENT_ADMIN_RESPONSE_EMITTED,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
            operation = response_operation,
            result = "success",
            status_code = status.as_u16(),
            "admin response emitted"
        );
        Ok(json_response(status, body))
    }
}

pub struct BackgroundHealStatusHandler {}

#[async_trait::async_trait]
impl Operation for BackgroundHealStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_heal_admin_request(&req).await?;

        let Some(store) = current_object_store_handle() else {
            warn!(
                event = EVENT_ADMIN_REQUEST_FAILED,
                component = LOG_COMPONENT_ADMIN_API,
                subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
                operation = "background_heal_status",
                result = "failed",
                reason = "server_not_initialized",
                "admin request failed"
            );
            return Err(s3_error!(InternalError, "server not initialized"));
        };

        let info = read_background_heal_info(store).await;
        let heal_operations = rustfs_heal::current_heal_operations_snapshot().await;
        let progress = rustfs_heal::current_heal_progress_snapshot()
            .await
            .map(BackgroundHealProgress::from);
        let body = encode_background_heal_status(&info, heal_operations, progress)?;
        info!(
            event = EVENT_ADMIN_RESPONSE_EMITTED,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
            operation = "background_heal_status",
            result = "success",
            "admin response emitted"
        );

        Ok(json_response(StatusCode::OK, body))
    }
}

#[cfg(test)]
mod tests {
    use super::extract_heal_init_params;
    use super::{
        BackgroundHealProgress, HealInitParams, HealResp, build_heal_channel_request, encode_background_heal_status,
        encode_heal_start_success, encode_heal_task_status, heal_channel_response_items, heal_channel_response_progress,
        heal_channel_response_summary, json_response, map_heal_response, map_root_heal_status, should_handle_root_heal_directly,
        validate_heal_request_mode, validate_heal_target,
    };
    use crate::admin::storage_api::error::StorageError;
    use bytes::Bytes;
    use http::StatusCode;
    use http::Uri;
    use matchit::Router;
    use rustfs_common::heal_channel::{HealChannelPriority, HealOpts, HealRequestSource, HealScanMode};
    use rustfs_scanner::scanner::BackgroundHealInfo;
    use s3s::{
        S3ErrorCode,
        header::{CONTENT_LENGTH, CONTENT_TYPE},
    };
    use serde_json::json;
    use time::{OffsetDateTime, format_description::well_known::Rfc3339};
    use tokio::sync::mpsc;
    use tracing::debug;

    #[test]
    fn test_heal_opts_serialization() {
        // Test that HealOpts can be properly deserialized
        let heal_opts_json = json!({
            "recursive": true,
            "dryRun": false,
            "remove": true,
            "recreate": false,
            "scanMode": 2,
            "updateParity": true,
            "nolock": false
        });

        let json_str = serde_json::to_string(&heal_opts_json).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed["recursive"], true);
        assert_eq!(parsed["scanMode"], 2);
    }

    #[test]
    fn test_heal_opts_url_encoding() {
        // Test URL encoding/decoding of HealOpts
        let opts = HealOpts {
            recursive: true,
            dry_run: false,
            remove: true,
            recreate: false,
            scan_mode: rustfs_common::heal_channel::HealScanMode::Normal,
            update_parity: false,
            no_lock: true,
            pool: Some(1),
            set: Some(0),
        };

        let encoded = serde_urlencoded::to_string(opts).unwrap();
        assert!(encoded.contains("recursive=true"));
        assert!(encoded.contains("remove=true"));

        // Test round-trip
        let decoded: HealOpts = serde_urlencoded::from_str(&encoded).unwrap();
        assert_eq!(decoded.recursive, opts.recursive);
        assert_eq!(decoded.scan_mode, opts.scan_mode);
    }

    #[test]
    fn test_extract_heal_init_params_invalid_control_combination_returns_descriptive_error() {
        let uri: Uri = "/rustfs/admin/v3/heal/test-bucket?clientToken=token&forceStart=true"
            .parse()
            .expect("uri should parse");

        let mut router = Router::new();
        router
            .insert("/rustfs/admin/v3/heal/{bucket}", ())
            .expect("route should insert");
        let matched = router.at("/rustfs/admin/v3/heal/test-bucket").expect("route should match");

        let err = extract_heal_init_params(&Bytes::new(), &uri, matched.params).expect_err("must reject invalid combo");
        assert!(
            err.to_string()
                .contains("invalid combination of clientToken, forceStart, and forceStop parameters")
        );
    }

    #[test]
    fn test_extract_heal_init_params_allows_client_token_force_stop() {
        let uri: Uri = "/rustfs/admin/v3/heal/test-bucket?clientToken=token&forceStop=true"
            .parse()
            .expect("uri should parse");

        let mut router = Router::new();
        router
            .insert("/rustfs/admin/v3/heal/{bucket}", ())
            .expect("route should insert");
        let matched = router.at("/rustfs/admin/v3/heal/test-bucket").expect("route should match");

        let parsed = extract_heal_init_params(&Bytes::new(), &uri, matched.params).expect("client-token stop should be accepted");
        assert_eq!(parsed.client_token, "token");
        assert!(parsed.force_stop);
    }

    #[test]
    fn test_extract_heal_init_params_rejects_unknown_duplicate_and_invalid_bool() {
        let mut router = Router::new();
        router
            .insert("/rustfs/admin/v3/heal/{bucket}", ())
            .expect("route should insert");

        for query in [
            "forceStart=yes",
            "forceStart=true&forceStart=false",
            "clientToken=token&unexpected=true",
        ] {
            let uri: Uri = format!("/rustfs/admin/v3/heal/test-bucket?{query}")
                .parse()
                .expect("uri should parse");
            let matched = router.at("/rustfs/admin/v3/heal/test-bucket").expect("route should match");
            let err = extract_heal_init_params(&Bytes::new(), &uri, matched.params)
                .expect_err("strict heal query should reject malformed input");
            assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        }
    }

    #[test]
    fn test_heal_channel_request_preserves_admin_heal_options() {
        let hip = HealInitParams {
            bucket: "bucket".to_string(),
            obj_prefix: "prefix".to_string(),
            hs: HealOpts {
                recursive: true,
                dry_run: true,
                remove: true,
                recreate: true,
                scan_mode: HealScanMode::Deep,
                update_parity: true,
                pool: Some(1),
                set: Some(2),
                ..Default::default()
            },
            force_start: true,
            ..Default::default()
        };

        let request = build_heal_channel_request(&hip);

        assert_eq!(request.bucket, "bucket");
        assert_eq!(request.object_prefix.as_deref(), Some("prefix"));
        assert_eq!(request.priority, HealChannelPriority::High);
        assert!(request.force_start);
        assert_eq!(request.scan_mode, Some(HealScanMode::Deep));
        assert_eq!(request.recursive, Some(true));
        assert_eq!(request.dry_run, Some(true));
        assert_eq!(request.remove_corrupted, Some(true));
        assert_eq!(request.recreate_missing, Some(true));
        assert_eq!(request.update_parity, Some(true));
        assert_eq!(request.pool_index, Some(1));
        assert_eq!(request.set_index, Some(2));
    }

    #[test]
    fn test_root_heal_channel_request_with_pool_set_targets_erasure_set() {
        let hip = HealInitParams {
            hs: HealOpts {
                scan_mode: HealScanMode::Deep,
                recreate: true,
                pool: Some(1),
                set: Some(2),
                ..Default::default()
            },
            force_start: true,
            ..Default::default()
        };

        let request = build_heal_channel_request(&hip);

        assert_eq!(request.bucket, "");
        assert_eq!(request.disk.as_deref(), Some("pool_1_set_2"));
        assert_eq!(request.priority, HealChannelPriority::High);
        assert_eq!(request.source, HealRequestSource::Admin);
        assert_eq!(request.pool_index, Some(1));
        assert_eq!(request.set_index, Some(2));
        assert_eq!(request.scan_mode, Some(HealScanMode::Deep));
        assert_eq!(request.recursive, Some(true));
        assert_eq!(request.recreate_missing, Some(true));
    }

    #[test]
    fn test_root_recursive_heal_channel_request_targets_cluster() {
        let hip = HealInitParams {
            hs: HealOpts {
                recursive: true,
                scan_mode: HealScanMode::Deep,
                recreate: true,
                ..Default::default()
            },
            force_start: true,
            ..Default::default()
        };

        let request = build_heal_channel_request(&hip);

        assert_eq!(request.bucket, "");
        assert_eq!(request.disk, None);
        assert_eq!(request.object_prefix, None);
        assert_eq!(request.priority, HealChannelPriority::High);
        assert_eq!(request.source, HealRequestSource::Admin);
        assert_eq!(request.scan_mode, Some(HealScanMode::Deep));
        assert_eq!(request.recursive, Some(true));
        assert_eq!(request.recreate_missing, Some(true));
    }

    #[test]
    fn test_bucket_heal_channel_request_defaults_to_recursive() {
        let hip = HealInitParams {
            bucket: "bucket".to_string(),
            obj_prefix: String::new(),
            hs: HealOpts {
                recursive: false,
                scan_mode: HealScanMode::Deep,
                recreate: true,
                ..Default::default()
            },
            ..Default::default()
        };

        let request = build_heal_channel_request(&hip);

        assert_eq!(request.bucket, "bucket");
        assert_eq!(request.object_prefix, None);
        assert_eq!(request.recursive, Some(true));
    }

    #[test]
    fn test_extract_heal_init_params_allows_root_heal_target() {
        let uri: Uri = "/rustfs/admin/v3/heal/".parse().expect("uri should parse");
        let heal_opts = json!({
            "recursive": false,
            "dryRun": false,
            "remove": false,
            "recreate": false,
            "scanMode": 1,
            "updateParity": false,
            "nolock": false
        });

        let mut router = Router::new();
        router.insert("/rustfs/admin/v3/heal/", ()).expect("route should insert");
        let matched = router.at("/rustfs/admin/v3/heal/").expect("route should match");

        let parsed = extract_heal_init_params(
            &Bytes::from(serde_json::to_vec(&heal_opts).expect("json should serialize")),
            &uri,
            matched.params,
        )
        .expect("root heal target should be accepted");

        assert!(parsed.bucket.is_empty());
        assert!(parsed.obj_prefix.is_empty());
    }

    #[test]
    fn test_validate_heal_request_mode_rejects_root_heal_start() {
        let err = validate_heal_request_mode(&HealInitParams::default()).expect_err("must reject root heal start");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(
            err.to_string()
                .contains("root heal requires recursive=true or a bucket target")
        );
    }

    #[test]
    fn test_validate_heal_request_mode_allows_root_recursive_heal_start() {
        validate_heal_request_mode(&HealInitParams {
            hs: HealOpts {
                recursive: true,
                ..Default::default()
            },
            ..Default::default()
        })
        .expect("root recursive heal should start tracked cluster heal");
    }

    #[test]
    fn test_validate_heal_request_mode_allows_root_erasure_set_target() {
        validate_heal_request_mode(&HealInitParams {
            hs: HealOpts {
                pool: Some(1),
                set: Some(2),
                ..Default::default()
            },
            ..Default::default()
        })
        .expect("root heal with pool and set should start tracked erasure-set rebuild");
    }

    #[test]
    fn test_validate_heal_request_mode_rejects_partial_root_erasure_set_target() {
        let err = validate_heal_request_mode(&HealInitParams {
            hs: HealOpts {
                pool: Some(1),
                ..Default::default()
            },
            ..Default::default()
        })
        .expect_err("root heal must provide both pool and set");

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(
            err.to_string()
                .contains("root heal erasure-set target requires both pool and set")
        );
    }

    #[test]
    fn test_should_handle_root_heal_directly_is_disabled_for_root_start_modes() {
        assert!(!should_handle_root_heal_directly(&HealInitParams::default()));
        assert!(!should_handle_root_heal_directly(&HealInitParams {
            force_start: true,
            ..Default::default()
        }));
    }

    #[test]
    fn test_should_handle_root_heal_directly_skips_query_cancel_and_bucket_targets() {
        assert!(!should_handle_root_heal_directly(&HealInitParams {
            client_token: "heal-token".to_string(),
            ..Default::default()
        }));
        assert!(!should_handle_root_heal_directly(&HealInitParams {
            force_stop: true,
            ..Default::default()
        }));
        assert!(!should_handle_root_heal_directly(&HealInitParams {
            bucket: "bucket".to_string(),
            ..Default::default()
        }));
        assert!(!should_handle_root_heal_directly(&HealInitParams {
            hs: HealOpts {
                pool: Some(1),
                set: Some(2),
                ..Default::default()
            },
            ..Default::default()
        }));
    }

    #[test]
    fn test_map_root_heal_status_allows_no_heal_required() {
        map_root_heal_status(Some(StorageError::NoHealRequired)).expect("NoHealRequired should stay non-fatal");
    }

    #[test]
    fn test_map_root_heal_status_rejects_fatal_errors() {
        let err = map_root_heal_status(Some(StorageError::Unexpected)).expect_err("fatal status must fail");
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert!(err.to_string().contains("root heal failed: Unexpected error"));
    }

    #[test]
    fn test_validate_heal_request_mode_allows_root_query_and_cancel() {
        validate_heal_request_mode(&HealInitParams {
            client_token: "heal-token".to_string(),
            ..Default::default()
        })
        .expect("root heal status query should be accepted");

        validate_heal_request_mode(&HealInitParams {
            force_stop: true,
            ..Default::default()
        })
        .expect("root heal cancel should be accepted");
    }

    #[test]
    fn test_extract_heal_init_params_rejects_prefix_without_bucket() {
        let err = validate_heal_target("", "prefix").expect_err("must reject empty bucket");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(err.to_string().contains("invalid bucket name"));
    }

    #[ignore] // FIXME: failed in github actions - keeping original test
    #[test]
    fn test_decode() {
        let b = b"{\"recursive\":false,\"dryRun\":false,\"remove\":false,\"recreate\":false,\"scanMode\":1,\"updateParity\":false,\"nolock\":false}";
        let s: HealOpts = serde_urlencoded::from_bytes(b).unwrap();
        debug!("Parsed HealOpts: {:?}", s);
    }

    #[tokio::test]
    async fn test_map_heal_response_propagates_errors() {
        let (tx, mut rx) = mpsc::channel(1);
        drop(tx);
        let result = map_heal_response(rx.recv().await);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), &S3ErrorCode::InternalError);

        let (tx, mut rx) = mpsc::channel(1);
        tx.send(HealResp {
            resp_bytes: vec![],
            api_err: Some("channel failed".to_string()),
        })
        .await
        .unwrap();
        let result = map_heal_response(rx.recv().await);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), &S3ErrorCode::InternalError);

        let (tx, mut rx) = mpsc::channel(1);
        tx.send(HealResp {
            resp_bytes: vec![],
            api_err: None,
        })
        .await
        .unwrap();
        let result = map_heal_response(rx.recv().await);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), &S3ErrorCode::InternalError);

        let (tx, mut rx) = mpsc::channel(1);
        let _ = tx
            .send(HealResp {
                resp_bytes: vec![1, 2, 3],
                api_err: None,
            })
            .await;
        let result = map_heal_response(rx.recv().await).expect("heal response should be successful");
        assert_eq!(result.0, StatusCode::OK);
        assert_eq!(result.1, vec![1, 2, 3]);
    }

    #[test]
    fn test_encode_background_heal_status_uses_expected_shape() {
        let info = BackgroundHealInfo {
            bitrot_start_time: None,
            bitrot_start_cycle: 42,
            current_scan_mode: HealScanMode::Deep,
        };

        let operations = rustfs_heal::HealOperationsSnapshot {
            queue_length: 3,
            active_tasks: 2,
            ..Default::default()
        };

        let encoded = encode_background_heal_status(&info, operations, None).expect("background heal info should serialize");
        let json: serde_json::Value = serde_json::from_slice(&encoded).expect("json should deserialize");

        assert_eq!(json["bitrotStartCycle"], 42);
        assert_eq!(json["currentScanMode"], 2);
        assert!(json["bitrotStartTime"].is_null());
        assert!(json["healQueueLength"].is_u64());
        assert!(json["healActiveTasks"].is_u64());
        assert_eq!(json["healOperations"]["queueLength"], json["healQueueLength"]);
        assert_eq!(json["healOperations"]["activeTasks"], json["healActiveTasks"]);
        assert!(json["healOperations"]["queuedBySource"]["scanner"].is_u64());
        assert!(json["healOperations"]["queuedBySource"]["admin"].is_u64());
        assert!(json["healOperations"]["queuedByPriority"]["low"].is_u64());
        assert!(json["healOperations"]["queuedByPriority"]["high"].is_u64());
        assert!(json["progress"].is_null());
    }

    #[test]
    fn test_encode_background_heal_status_includes_progress() {
        let info = BackgroundHealInfo {
            bitrot_start_time: None,
            bitrot_start_cycle: 42,
            current_scan_mode: HealScanMode::Deep,
        };

        let progress = BackgroundHealProgress {
            objects_scanned: 7,
            objects_healed: 3,
            objects_failed: 1,
            bytes_processed: 4096,
        };

        let encoded = encode_background_heal_status(&info, rustfs_heal::HealOperationsSnapshot::default(), Some(progress))
            .expect("background heal info should serialize");
        let json: serde_json::Value = serde_json::from_slice(&encoded).expect("json should deserialize");

        assert_eq!(json["progress"]["objectsScanned"], 7);
        assert_eq!(json["progress"]["objectsHealed"], 3);
        assert_eq!(json["progress"]["objectsFailed"], 1);
        assert_eq!(json["progress"]["bytesProcessed"], 4096);
    }

    #[test]
    fn test_encode_heal_start_success_uses_client_wire_shape() {
        let encoded = encode_heal_start_success("token-1".to_string(), "127.0.0.1:9000".to_string())
            .expect("start response should serialize");
        let json: serde_json::Value = serde_json::from_slice(&encoded).expect("json should deserialize");

        assert_eq!(json["clientToken"], "token-1");
        assert_eq!(json["clientAddress"], "127.0.0.1:9000");
        let start_time = json["startTime"].as_str().expect("startTime should be a string");
        OffsetDateTime::parse(start_time, &Rfc3339).expect("startTime should be RFC3339");
    }

    #[test]
    fn test_encode_heal_task_status_uses_client_wire_shape() {
        let encoded = encode_heal_task_status(
            "Heal status query accepted".to_string(),
            String::new(),
            HealOpts::default(),
            Vec::new(),
            None,
        )
        .expect("status response should serialize");
        let json: serde_json::Value = serde_json::from_slice(&encoded).expect("json should deserialize");

        assert_eq!(json["summary"], "Heal status query accepted");
        assert_eq!(json["detail"], "");
        assert!(json["items"].is_null());
        assert!(json["settings"].is_object());
        let start_time = json["startTime"].as_str().expect("startTime should be a string");
        OffsetDateTime::parse(start_time, &Rfc3339).expect("startTime should be RFC3339");
    }

    #[test]
    fn test_encode_heal_task_status_preserves_progress() {
        let progress = json!({
            "objectsScanned": 7,
            "objectsHealed": 3,
            "currentObject": "bucket-a/object-a"
        });
        let encoded = encode_heal_task_status(
            "running".to_string(),
            String::new(),
            HealOpts::default(),
            Vec::new(),
            Some(progress.clone()),
        )
        .expect("status response should serialize");
        let json: serde_json::Value = serde_json::from_slice(&encoded).expect("json should deserialize");

        assert_eq!(json["progress"], progress);
    }

    #[test]
    fn test_build_heal_channel_request_preserves_client_options() {
        let hip = HealInitParams {
            bucket: "bucket-a".to_string(),
            obj_prefix: "prefix-a".to_string(),
            force_start: true,
            hs: HealOpts {
                recursive: true,
                dry_run: true,
                remove: true,
                recreate: false,
                scan_mode: HealScanMode::Deep,
                update_parity: false,
                no_lock: true,
                pool: Some(1),
                set: Some(2),
            },
            ..Default::default()
        };

        let request = build_heal_channel_request(&hip);

        assert_eq!(request.bucket, "bucket-a");
        assert_eq!(request.object_prefix.as_deref(), Some("prefix-a"));
        assert_eq!(request.priority, HealChannelPriority::High);
        assert_eq!(request.source, HealRequestSource::Admin);
        assert!(request.force_start);
        assert_eq!(request.pool_index, Some(1));
        assert_eq!(request.set_index, Some(2));
        assert_eq!(request.scan_mode, Some(HealScanMode::Deep));
        assert_eq!(request.remove_corrupted, Some(true));
        assert_eq!(request.recreate_missing, Some(false));
        assert_eq!(request.update_parity, Some(false));
        assert_eq!(request.recursive, Some(true));
        assert_eq!(request.dry_run, Some(true));
    }

    #[test]
    fn test_heal_channel_response_summary_defaults_to_running() {
        let response = rustfs_common::heal_channel::create_heal_response("token".to_string(), true, None, None);
        assert_eq!(heal_channel_response_summary(&response), "running");

        let response =
            rustfs_common::heal_channel::create_heal_response("token".to_string(), true, Some(b"finished".to_vec()), None);
        assert_eq!(heal_channel_response_summary(&response), "finished");
    }

    #[test]
    fn test_heal_channel_response_status_preserves_items() {
        let payload = serde_json::json!({
            "summary": "finished",
            "items": [{
                "resultId": 0,
                "type": "object",
                "bucket": "bucket-a",
                "object": "object-a",
                "versionId": "",
                "detail": "",
                "parityBlocks": 2,
                "dataBlocks": 2,
                "diskCount": 4,
                "setCount": 1,
                "before": { "drives": [] },
                "after": { "drives": [] },
                "objectSize": 1024
            }]
        });
        let response = rustfs_common::heal_channel::create_heal_response(
            "token".to_string(),
            true,
            Some(serde_json::to_vec(&payload).expect("payload should serialize")),
            None,
        );

        assert_eq!(heal_channel_response_summary(&response), "finished");
        let items = heal_channel_response_items(&response);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].bucket, "bucket-a");
        assert_eq!(items[0].object, "object-a");
        assert_eq!(items[0].object_size, 1024);
    }

    #[test]
    fn test_heal_channel_response_status_preserves_progress() {
        let progress = serde_json::json!({
            "objectsScanned": 4,
            "objectsHealed": 2,
            "currentObject": "bucket-a/object-a"
        });
        let payload = serde_json::json!({
            "summary": "running",
            "items": [],
            "progress": progress
        });
        let response = rustfs_common::heal_channel::create_heal_response(
            "token".to_string(),
            true,
            Some(serde_json::to_vec(&payload).expect("payload should serialize")),
            None,
        );

        assert_eq!(heal_channel_response_summary(&response), "running");
        assert_eq!(heal_channel_response_progress(&response), Some(progress));
    }

    #[test]
    fn test_json_response_sets_application_json_content_type() {
        let response = json_response(StatusCode::OK, b"{}".to_vec());
        let content_type = response.headers.get(CONTENT_TYPE).and_then(|value| value.to_str().ok());
        assert_eq!(content_type, Some("application/json"),);
        let content_length = response.headers.get(CONTENT_LENGTH).and_then(|value| value.to_str().ok());
        assert_eq!(content_length, Some("2"),);
    }
}
