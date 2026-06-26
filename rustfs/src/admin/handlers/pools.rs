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

use http::{HeaderMap, HeaderValue, StatusCode, Uri};
use matchit::Params;
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_utils::{
    MaskedAccessKey,
    http::{AMZ_REQUEST_ID, REQUEST_ID_HEADER},
};
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, header::CONTENT_TYPE, s3_error};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    admin::runtime_sources::{
        AdminPoolStatus, QueryPoolStatusRequest, default_admin_usecase, resolve_endpoints_handle, resolve_notification_system,
        resolve_object_store_handle,
    },
    admin::{
        auth::validate_admin_request,
        router::{AdminOperation, Operation, S3Router},
        storage_api::runtime::{EndpointServerPools, PeerRestClient},
    },
    auth::{check_key_valid, get_session_token},
    error::ApiError,
    server::{ADMIN_PREFIX, RemoteAddr},
};
use hyper::Method;

use std::collections::HashSet;

#[derive(Debug, Clone, serde::Serialize)]
struct PoolAdminDiscovery {
    #[serde(rename = "runtimeCapabilities")]
    runtime_capabilities: String,
    #[serde(rename = "clusterSnapshot")]
    cluster_snapshot: String,
    #[serde(rename = "extensionsCatalog")]
    extensions_catalog: String,
}

#[derive(Debug, Clone, serde::Serialize)]
struct PoolStatusResponse {
    pool: AdminPoolStatus,
    admin_discovery: PoolAdminDiscovery,
}

const LOG_COMPONENT_ADMIN_API: &str = "admin_api";
const LOG_SUBSYSTEM_POOL_ADMIN: &str = "pool_admin";
const EVENT_ADMIN_REQUEST_STATE: &str = "admin_request_state";
const EVENT_ADMIN_REQUEST_REJECTED: &str = "admin_request_rejected";
const EVENT_ADMIN_REQUEST_FAILED: &str = "admin_request_failed";
const EVENT_ADMIN_RESPONSE_EMITTED: &str = "admin_response_emitted";

fn admin_request_id(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(REQUEST_ID_HEADER)
        .or_else(|| headers.get(AMZ_REQUEST_ID))
        .and_then(|value| value.to_str().ok())
}

fn admin_remote_addr(req: &S3Request<Body>) -> Option<String> {
    req.extensions
        .get::<Option<RemoteAddr>>()
        .and_then(|opt| opt.map(|addr| addr.0.to_string()))
}

fn log_pool_request_rejected_with_context(operation: &str, reason: &str, request_id: &str, actor: &str, remote_addr: &str) {
    warn!(
        event = EVENT_ADMIN_REQUEST_REJECTED,
        component = LOG_COMPONENT_ADMIN_API,
        subsystem = LOG_SUBSYSTEM_POOL_ADMIN,
        operation,
        action = operation,
        result = "rejected",
        reason,
        request_id = %request_id,
        actor = %actor,
        remote_addr = %remote_addr,
        "admin request rejected"
    );
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PoolAuditContext<'a> {
    request_id: &'a str,
    actor: &'a str,
    remote_addr: &'a str,
}

impl<'a> PoolAuditContext<'a> {
    fn new(request_id: &'a str, actor: &'a str, remote_addr: &'a str) -> Self {
        Self {
            request_id,
            actor,
            remote_addr,
        }
    }
}

fn log_pool_request_rejected_with_audit(operation: &str, reason: &str, audit: PoolAuditContext<'_>) {
    warn!(
        event = EVENT_ADMIN_REQUEST_REJECTED,
        component = LOG_COMPONENT_ADMIN_API,
        subsystem = LOG_SUBSYSTEM_POOL_ADMIN,
        operation,
        action = operation,
        result = "rejected",
        reason,
        request_id = %audit.request_id,
        actor = %audit.actor,
        remote_addr = %audit.remote_addr,
        "admin request rejected"
    );
}

fn log_pool_request_rejected_with_pool_audit(operation: &str, reason: &str, pool: &str, audit: PoolAuditContext<'_>) {
    warn!(
        event = EVENT_ADMIN_REQUEST_REJECTED,
        component = LOG_COMPONENT_ADMIN_API,
        subsystem = LOG_SUBSYSTEM_POOL_ADMIN,
        operation,
        action = operation,
        result = "rejected",
        reason,
        request_id = %audit.request_id,
        actor = %audit.actor,
        remote_addr = %audit.remote_addr,
        pool,
        "admin request rejected"
    );
}

fn log_pool_request_rejected_with_index_audit(
    operation: &str,
    reason: &str,
    idx: usize,
    pool_count: usize,
    audit: PoolAuditContext<'_>,
) {
    warn!(
        event = EVENT_ADMIN_REQUEST_REJECTED,
        component = LOG_COMPONENT_ADMIN_API,
        subsystem = LOG_SUBSYSTEM_POOL_ADMIN,
        operation,
        action = operation,
        result = "rejected",
        reason,
        request_id = %audit.request_id,
        actor = %audit.actor,
        remote_addr = %audit.remote_addr,
        pool_index = idx,
        pool_count,
        "admin request rejected"
    );
}

macro_rules! log_pool_request_rejected {
    ($operation:expr, $reason:expr) => {
        warn!(
            event = EVENT_ADMIN_REQUEST_REJECTED,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_POOL_ADMIN,
            operation = $operation,
            action = $operation,
            result = "rejected",
            reason = $reason,
            "admin request rejected"
        );
    };
}

macro_rules! log_pool_request_failed {
    ($operation:expr, $reason:expr, $err:expr) => {
        error!(
            event = EVENT_ADMIN_REQUEST_FAILED,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_POOL_ADMIN,
            operation = $operation,
            action = $operation,
            result = "failed",
            reason = $reason,
            error = %$err,
            "admin request failed"
        );
    };
}

macro_rules! log_pool_response_emitted {
    ($operation:expr) => {
        info!(
            event = EVENT_ADMIN_REQUEST_STATE,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_POOL_ADMIN,
            operation = $operation,
            action = $operation,
            result = "success",
            "admin response emitted"
        );
    };
}

fn endpoints_from_context() -> Option<crate::admin::storage_api::runtime::EndpointServerPools> {
    resolve_endpoints_handle()
}

fn validate_start_decommission_guards(decommission_running: bool, rebalance_running: bool) -> s3s::S3Result<()> {
    if decommission_running {
        return Err(s3_error!(InvalidRequest, "DecommissionAlreadyRunning"));
    }

    if rebalance_running {
        return Err(S3Error::with_message(
            S3ErrorCode::OperationAborted,
            "Decommission cannot be started, rebalance is already in progress".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
fn validate_pool_mutation_leader(
    endpoints: &EndpointServerPools,
    idx: usize,
    operation: &str,
    audit: PoolAuditContext<'_>,
) -> s3s::S3Result<()> {
    let endpoint = endpoints
        .as_ref()
        .get(idx)
        .and_then(|pool| pool.endpoints.as_ref().first())
        .ok_or_else(|| pool_admin_pool_index_error_with_audit(operation, idx, endpoints.as_ref().len(), audit))?;

    if !endpoint.is_local {
        log_pool_request_rejected_with_index_audit(
            operation_to_event(operation),
            "not_pool_leader",
            idx,
            endpoints.as_ref().len(),
            audit,
        );
        return Err(S3Error::with_message(
            S3ErrorCode::OperationAborted,
            format!("Failed to {operation}: pool {idx} must be handled by its first endpoint {endpoint}"),
        ));
    }

    Ok(())
}

fn decommission_peer_target(
    endpoints: &EndpointServerPools,
    idx: usize,
    operation: &str,
    audit: PoolAuditContext<'_>,
) -> s3s::S3Result<Option<PeerRestClient>> {
    let endpoint = endpoints
        .as_ref()
        .get(idx)
        .and_then(|pool| pool.endpoints.as_ref().first())
        .ok_or_else(|| pool_admin_pool_index_error_with_audit(operation, idx, endpoints.as_ref().len(), audit))?;

    if endpoint.is_local {
        return Ok(None);
    }

    let grid_host = endpoint.grid_host();
    let Some(notification_sys) = resolve_notification_system() else {
        log_pool_request_rejected_with_index_audit(
            operation_to_event(operation),
            "notification_sys_not_initialized",
            idx,
            endpoints.as_ref().len(),
            audit,
        );
        return Err(S3Error::with_message(
            S3ErrorCode::OperationAborted,
            format!("Failed to {operation}: target pool first endpoint is not reachable"),
        ));
    };

    let Some(client) = notification_sys.peer_client_for_grid_host(&grid_host) else {
        log_pool_request_rejected_with_index_audit(
            operation_to_event(operation),
            "target_peer_not_found",
            idx,
            endpoints.as_ref().len(),
            audit,
        );
        return Err(S3Error::with_message(
            S3ErrorCode::OperationAborted,
            format!("Failed to {operation}: target pool first endpoint is not reachable"),
        ));
    };

    Ok(Some(client))
}

fn contextualize_admin_pool_api_error(
    err: crate::error::ApiError,
    operation: &str,
    pool_context: impl std::fmt::Display,
) -> crate::error::ApiError {
    crate::error::ApiError {
        code: err.code,
        message: format!("admin {operation} failed for {pool_context}: {}", err.message),
        source: err.source,
    }
}

fn decommission_admin_not_initialized_error_with_audit(operation: &str, audit: PoolAuditContext<'_>) -> S3Error {
    error!(
        event = EVENT_ADMIN_REQUEST_FAILED,
        component = LOG_COMPONENT_ADMIN_API,
        subsystem = LOG_SUBSYSTEM_POOL_ADMIN,
        operation = operation_to_event(operation),
        action = operation_to_event(operation),
        result = "failed",
        reason = "object_layer_not_initialized",
        request_id = %audit.request_id,
        actor = %audit.actor,
        remote_addr = %audit.remote_addr,
        error = "object layer not initialized",
        "admin request failed"
    );
    S3Error::with_message(S3ErrorCode::InternalError, format!("Failed to {operation}: object layer not initialized"))
}

fn pool_admin_missing_credentials_error(operation: &str) -> S3Error {
    log_pool_request_rejected!(operation_to_event(operation), "missing_credentials");
    S3Error::with_message(S3ErrorCode::InvalidRequest, format!("Failed to {operation}: missing credentials"))
}

fn pool_admin_missing_credentials_error_with_request(operation: &str, request_id: &str, remote_addr: &str) -> S3Error {
    warn!(
        event = EVENT_ADMIN_REQUEST_REJECTED,
        component = LOG_COMPONENT_ADMIN_API,
        subsystem = LOG_SUBSYSTEM_POOL_ADMIN,
        operation = operation_to_event(operation),
        action = operation_to_event(operation),
        result = "rejected",
        reason = "missing_credentials",
        request_id = %request_id,
        remote_addr = %remote_addr,
        "admin request rejected"
    );
    S3Error::with_message(S3ErrorCode::InvalidRequest, format!("Failed to {operation}: missing credentials"))
}

fn pool_admin_query_parse_error(operation: &str) -> S3Error {
    log_pool_request_rejected!(operation_to_event(operation), "invalid_query_parameters");
    S3Error::with_message(S3ErrorCode::InvalidArgument, format!("Failed to {operation}: invalid query parameters"))
}

fn pool_admin_query_parse_error_with_audit(operation: &str, audit: PoolAuditContext<'_>) -> S3Error {
    log_pool_request_rejected_with_audit(operation_to_event(operation), "invalid_query_parameters", audit);
    S3Error::with_message(S3ErrorCode::InvalidArgument, format!("Failed to {operation}: invalid query parameters"))
}

fn pool_admin_pool_parse_error_with_audit(operation: &str, pool: &str, audit: PoolAuditContext<'_>) -> S3Error {
    log_pool_request_rejected_with_pool_audit(operation_to_event(operation), "invalid_pool", pool, audit);
    S3Error::with_message(S3ErrorCode::InvalidArgument, format!("Failed to {operation}: invalid pool `{pool}`"))
}

fn pool_admin_pool_not_found_error_with_audit(operation: &str, pool: &str, audit: PoolAuditContext<'_>) -> S3Error {
    log_pool_request_rejected_with_pool_audit(operation_to_event(operation), "pool_not_found", pool, audit);
    S3Error::with_message(
        S3ErrorCode::InvalidArgument,
        format!("Failed to {operation}: pool `{pool}` was not found"),
    )
}

fn pool_admin_pool_index_error_with_audit(
    operation: &str,
    idx: usize,
    pool_count: usize,
    audit: PoolAuditContext<'_>,
) -> S3Error {
    log_pool_request_rejected_with_index_audit(operation_to_event(operation), "pool_index_out_of_range", idx, pool_count, audit);
    S3Error::with_message(
        S3ErrorCode::InvalidArgument,
        format!("Failed to {operation}: pool index {idx} is out of range for {pool_count} pools"),
    )
}

fn operation_to_event(operation: &str) -> &'static str {
    match operation {
        "list pools" => "list_pools",
        "load pool status" => "query_pool_status",
        "load decommission status" => "query_decommission_status",
        "start decommission" => "start_decommission",
        "cancel decommission" => "cancel_decommission",
        "clear decommission" => "clear_decommission",
        _ => "pool_admin",
    }
}

fn parse_pool_idx_by_id(pool: &str, endpoint_count: usize) -> Option<usize> {
    let idx = pool.parse::<usize>().ok()?;
    (idx < endpoint_count).then_some(idx)
}

fn pool_admin_discovery() -> PoolAdminDiscovery {
    let usecase = default_admin_usecase();
    PoolAdminDiscovery {
        runtime_capabilities: usecase.runtime_capabilities_route().to_string(),
        cluster_snapshot: usecase.cluster_snapshot_route().to_string(),
        extensions_catalog: usecase.extensions_catalog_route().to_string(),
    }
}

fn has_duplicate_indices(indices: &[usize]) -> bool {
    let mut seen = HashSet::with_capacity(indices.len());
    for idx in indices {
        if !seen.insert(*idx) {
            return true;
        }
    }
    false
}

pub fn register_pool_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/list").as_str(),
        AdminOperation(&ListPools {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/status").as_str(),
        AdminOperation(&StatusPool {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/decommission/status").as_str(),
        AdminOperation(&StatusDecommission {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/decommission").as_str(),
        AdminOperation(&StartDecommission {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/cancel").as_str(),
        AdminOperation(&CancelDecommission {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/clear").as_str(),
        AdminOperation(&ClearDecommission {}),
    )?;

    Ok(())
}

pub struct ListPools {}

#[async_trait::async_trait]
impl Operation for ListPools {
    // GET <endpoint>/<admin-API>/pools/list
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(input_cred) = req.credentials else {
            return Err(pool_admin_missing_credentials_error("list pools"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![
                Action::AdminAction(AdminAction::ServerInfoAdminAction),
                Action::AdminAction(AdminAction::DecommissionAdminAction),
            ],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let usecase = default_admin_usecase();
        let pool_items = usecase.execute_list_pools().await.map_err(S3Error::from)?;

        let data = serde_json::to_vec(&pool_items).map_err(|e| {
            log_pool_request_failed!("list_pools", "serialize_pools_list_failed", e);
            S3Error::with_message(S3ErrorCode::InternalError, "serialize pools list failed")
        })?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        log_pool_response_emitted!("list_pools");

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
pub struct StatusPoolQuery {
    pub pool: String,
    #[serde(rename = "by-id")]
    pub by_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PoolQueryMode {
    Status,
    Mutation,
}

fn parse_status_pool_query(uri: &Uri) -> Result<StatusPoolQuery, ()> {
    parse_pool_query(uri, PoolQueryMode::Status)
}

fn parse_mutation_pool_query(uri: &Uri) -> Result<StatusPoolQuery, ()> {
    parse_pool_query(uri, PoolQueryMode::Mutation)
}

fn parse_pool_query(uri: &Uri, mode: PoolQueryMode) -> Result<StatusPoolQuery, ()> {
    let mut parsed = StatusPoolQuery::default();
    let mut seen = HashSet::with_capacity(2);
    let Some(query) = uri.query() else {
        return Ok(parsed);
    };

    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        match key.as_ref() {
            "pool" => {
                if !seen.insert("pool") {
                    return Err(());
                }
                parsed.pool = value.into_owned();
            }
            "by-id" => {
                if !seen.insert("by-id") {
                    return Err(());
                }
                match value.as_ref() {
                    "true" | "false" => parsed.by_id = value.into_owned(),
                    _ => return Err(()),
                }
            }
            _ if mode == PoolQueryMode::Status => {}
            _ => return Err(()),
        }
    }

    Ok(parsed)
}

pub struct StatusPool {}

#[async_trait::async_trait]
impl Operation for StatusPool {
    // GET <endpoint>/<admin-API>/pools/status?pool=http://server{1...4}/disk{1...4}
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(input_cred) = req.credentials else {
            return Err(pool_admin_missing_credentials_error("load pool status"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![
                Action::AdminAction(AdminAction::ServerInfoAdminAction),
                Action::AdminAction(AdminAction::DecommissionAdminAction),
            ],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let query = parse_status_pool_query(&req.uri).map_err(|_| pool_admin_query_parse_error("load pool status"))?;

        let usecase = default_admin_usecase();
        let pools_status = usecase
            .execute_query_pool_status(QueryPoolStatusRequest {
                pool: query.pool,
                by_id: query.by_id.as_str() == "true",
            })
            .await
            .map_err(S3Error::from)?;
        let response = PoolStatusResponse {
            pool: pools_status,
            admin_discovery: pool_admin_discovery(),
        };

        let data = serde_json::to_vec(&response).map_err(|e| {
            log_pool_request_failed!("query_pool_status", "serialize_pool_status_failed", e);
            S3Error::with_message(S3ErrorCode::InternalError, "parse accountInfo failed")
        })?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        log_pool_response_emitted!("query_pool_status");

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

pub struct StatusDecommission {}

#[async_trait::async_trait]
impl Operation for StatusDecommission {
    // GET <endpoint>/<admin-API>/decommission/status[?pool=http://server{1...4}/disk{1...4}]
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(input_cred) = req.credentials else {
            return Err(pool_admin_missing_credentials_error("load decommission status"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![
                Action::AdminAction(AdminAction::ServerInfoAdminAction),
                Action::AdminAction(AdminAction::DecommissionAdminAction),
            ],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let query = parse_status_pool_query(&req.uri).map_err(|_| pool_admin_query_parse_error("load decommission status"))?;

        let usecase = default_admin_usecase();
        let data = if query.pool.is_empty() {
            let status = usecase.execute_list_decommission_status().await.map_err(S3Error::from)?;
            serde_json::to_vec(&status)
        } else {
            let status = usecase
                .execute_query_decommission_status(QueryPoolStatusRequest {
                    pool: query.pool,
                    by_id: query.by_id.as_str() == "true",
                })
                .await
                .map_err(S3Error::from)?;
            serde_json::to_vec(&status)
        }
        .map_err(|e| {
            log_pool_request_failed!("query_decommission_status", "serialize_decommission_status_failed", e);
            S3Error::with_message(S3ErrorCode::InternalError, "serialize decommission status failed")
        })?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        log_pool_response_emitted!("query_decommission_status");

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

pub struct StartDecommission {}

#[async_trait::async_trait]
impl Operation for StartDecommission {
    // POST <endpoint>/<admin-API>/pools/decommission?pool=http://server{1...4}/disk{1...4}
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let request_id = admin_request_id(&req.headers).unwrap_or_default().to_string();
        let remote_addr = admin_remote_addr(&req).unwrap_or_default();
        info!(
            event = EVENT_ADMIN_REQUEST_STATE,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_POOL_ADMIN,
            operation = "start_decommission",
            action = "start_decommission",
            state = "requested",
            request_id = %request_id,
            remote_addr = %remote_addr,
            "admin pool request state"
        );

        let Some(input_cred) = req.credentials else {
            return Err(pool_admin_missing_credentials_error_with_request(
                "start decommission",
                &request_id,
                &remote_addr,
            ));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
        let actor = MaskedAccessKey(&input_cred.access_key).to_string();

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::DecommissionAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;
        let audit = PoolAuditContext::new(&request_id, &actor, &remote_addr);

        let Some(endpoints) = endpoints_from_context() else {
            log_pool_request_rejected_with_context("start_decommission", "not_implemented", &request_id, &actor, &remote_addr);
            return Err(s3_error!(NotImplemented));
        };

        if endpoints.legacy() {
            log_pool_request_rejected_with_context(
                "start_decommission",
                "legacy_endpoints_not_supported",
                &request_id,
                &actor,
                &remote_addr,
            );
            return Err(s3_error!(NotImplemented));
        }

        let Some(store) = resolve_object_store_handle() else {
            return Err(decommission_admin_not_initialized_error_with_audit("start decommission", audit));
        };

        let query = parse_mutation_pool_query(&req.uri)
            .map_err(|_| pool_admin_query_parse_error_with_audit("start decommission", audit))?;
        let is_byid = query.by_id.as_str() == "true";

        let pools: Vec<&str> = query.pool.split(",").collect();
        let mut parsed_indices = Vec::with_capacity(pools.len());

        let ctx = CancellationToken::new();

        for pool in pools.iter() {
            let idx = {
                if is_byid {
                    parse_pool_idx_by_id(pool, endpoints.as_ref().len())
                        .ok_or_else(|| pool_admin_pool_parse_error_with_audit("start decommission", pool, audit))?
                } else {
                    let Some(idx) = endpoints.get_pool_idx(pool) else {
                        return Err(pool_admin_pool_parse_error_with_audit("start decommission", pool, audit));
                    };
                    idx
                }
            };

            if idx >= store.pools.len() {
                return Err(pool_admin_pool_index_error_with_audit(
                    "start decommission",
                    idx,
                    store.pools.len(),
                    audit,
                ));
            }

            parsed_indices.push(idx);
        }
        if has_duplicate_indices(&parsed_indices) {
            return Err(pool_admin_query_parse_error_with_audit("start decommission", audit));
        }
        let pools_indices = parsed_indices;

        if let Some(first_idx) = pools_indices.first().copied()
            && let Some(client) = decommission_peer_target(&endpoints, first_idx, "start decommission", audit)?
        {
            let pool_context = format!("pools {:?}", &pools_indices);
            client
                .start_decommission(pools_indices.clone())
                .await
                .map_err(ApiError::from)
                .map_err(|err| contextualize_admin_pool_api_error(err, "start decommission", &pool_context))?;
        } else {
            store
                .load_rebalance_meta()
                .await
                .map_err(|e| s3_error!(InternalError, "failed to refresh rebalance metadata before decommission start: {}", e))?;
            let decommission_running = store.is_decommission_running().await;
            let rebalance_running = store.is_rebalance_started().await;
            if decommission_running {
                log_pool_request_rejected_with_context(
                    "start_decommission",
                    "decommission_already_running",
                    &request_id,
                    &actor,
                    &remote_addr,
                );
            } else if rebalance_running {
                log_pool_request_rejected_with_context(
                    "start_decommission",
                    "rebalance_in_progress",
                    &request_id,
                    &actor,
                    &remote_addr,
                );
            }
            validate_start_decommission_guards(decommission_running, rebalance_running)?;

            if !pools_indices.is_empty() {
                let pool_context = format!("pools {:?}", &pools_indices);
                store
                    .decommission(ctx.clone(), pools_indices.clone())
                    .await
                    .map_err(ApiError::from)
                    .map_err(|err| contextualize_admin_pool_api_error(err, "start decommission", &pool_context))?;
            }
        }

        info!(
            event = EVENT_ADMIN_RESPONSE_EMITTED,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_POOL_ADMIN,
            operation = "start_decommission",
            action = "start_decommission",
            result = "success",
            request_id = %request_id,
            actor = %actor,
            remote_addr = %remote_addr,
            pool_indices = ?pools_indices,
            "admin response emitted"
        );
        Ok(S3Response::new((StatusCode::OK, Body::default())))
    }
}

pub struct CancelDecommission {}

#[async_trait::async_trait]
impl Operation for CancelDecommission {
    // POST <endpoint>/<admin-API>/pools/cancel?pool=http://server{1...4}/disk{1...4}
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let request_id = admin_request_id(&req.headers).unwrap_or_default().to_string();
        let remote_addr = admin_remote_addr(&req).unwrap_or_default();
        info!(
            event = EVENT_ADMIN_REQUEST_STATE,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_POOL_ADMIN,
            operation = "cancel_decommission",
            action = "cancel_decommission",
            state = "requested",
            request_id = %request_id,
            remote_addr = %remote_addr,
            "admin pool request state"
        );

        let Some(input_cred) = req.credentials else {
            return Err(pool_admin_missing_credentials_error_with_request(
                "cancel decommission",
                &request_id,
                &remote_addr,
            ));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
        let actor = MaskedAccessKey(&input_cred.access_key).to_string();

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::DecommissionAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;
        let audit = PoolAuditContext::new(&request_id, &actor, &remote_addr);

        let Some(endpoints) = endpoints_from_context() else {
            log_pool_request_rejected_with_context("cancel_decommission", "not_implemented", &request_id, &actor, &remote_addr);
            return Err(s3_error!(NotImplemented));
        };

        if endpoints.legacy() {
            log_pool_request_rejected_with_context(
                "cancel_decommission",
                "legacy_endpoints_not_supported",
                &request_id,
                &actor,
                &remote_addr,
            );
            return Err(s3_error!(NotImplemented));
        }

        let query = parse_mutation_pool_query(&req.uri)
            .map_err(|_| pool_admin_query_parse_error_with_audit("cancel decommission", audit))?;

        let is_byid = query.by_id.as_str() == "true";

        let has_idx = {
            if is_byid {
                parse_pool_idx_by_id(&query.pool, endpoints.as_ref().len())
            } else {
                endpoints.get_pool_idx(&query.pool)
            }
        };

        let Some(idx) = has_idx else {
            return Err(pool_admin_pool_not_found_error_with_audit("cancel decommission", &query.pool, audit));
        };

        if let Some(client) = decommission_peer_target(&endpoints, idx, "cancel decommission", audit)? {
            client
                .decommission_cancel(idx)
                .await
                .map_err(ApiError::from)
                .map_err(|err| contextualize_admin_pool_api_error(err, "cancel decommission", format!("pool {idx}")))?;
        } else {
            let Some(store) = resolve_object_store_handle() else {
                return Err(decommission_admin_not_initialized_error_with_audit("cancel decommission", audit));
            };

            store
                .decommission_cancel(idx)
                .await
                .map_err(ApiError::from)
                .map_err(|err| contextualize_admin_pool_api_error(err, "cancel decommission", format!("pool {idx}")))?;
        }

        info!(
            event = EVENT_ADMIN_RESPONSE_EMITTED,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_POOL_ADMIN,
            operation = "cancel_decommission",
            action = "cancel_decommission",
            result = "success",
            request_id = %request_id,
            actor = %actor,
            remote_addr = %remote_addr,
            pool_index = idx,
            "admin response emitted"
        );
        Ok(S3Response::new((StatusCode::OK, Body::default())))
    }
}

pub struct ClearDecommission {}

#[async_trait::async_trait]
impl Operation for ClearDecommission {
    // POST <endpoint>/<admin-API>/pools/clear?pool=http://server{1...4}/disk{1...4}
    // Clears failed/canceled decommission metadata only; already moved data is not rolled back.
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let request_id = admin_request_id(&req.headers).unwrap_or_default().to_string();
        let remote_addr = admin_remote_addr(&req).unwrap_or_default();
        info!(
            event = EVENT_ADMIN_REQUEST_STATE,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_POOL_ADMIN,
            operation = "clear_decommission",
            action = "clear_decommission",
            state = "requested",
            request_id = %request_id,
            remote_addr = %remote_addr,
            "admin pool request state"
        );

        let Some(input_cred) = req.credentials else {
            return Err(pool_admin_missing_credentials_error_with_request(
                "clear decommission",
                &request_id,
                &remote_addr,
            ));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
        let actor = MaskedAccessKey(&input_cred.access_key).to_string();

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::DecommissionAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;
        let audit = PoolAuditContext::new(&request_id, &actor, &remote_addr);

        let Some(endpoints) = endpoints_from_context() else {
            log_pool_request_rejected_with_context("clear_decommission", "not_implemented", &request_id, &actor, &remote_addr);
            return Err(s3_error!(NotImplemented));
        };

        if endpoints.legacy() {
            log_pool_request_rejected_with_context(
                "clear_decommission",
                "legacy_endpoints_not_supported",
                &request_id,
                &actor,
                &remote_addr,
            );
            return Err(s3_error!(NotImplemented));
        }

        let query = parse_mutation_pool_query(&req.uri)
            .map_err(|_| pool_admin_query_parse_error_with_audit("clear decommission", audit))?;

        let is_byid = query.by_id.as_str() == "true";

        let has_idx = {
            if is_byid {
                parse_pool_idx_by_id(&query.pool, endpoints.as_ref().len())
            } else {
                endpoints.get_pool_idx(&query.pool)
            }
        };

        let Some(idx) = has_idx else {
            return Err(pool_admin_pool_not_found_error_with_audit("clear decommission", &query.pool, audit));
        };

        if let Some(client) = decommission_peer_target(&endpoints, idx, "clear decommission", audit)? {
            client
                .clear_decommission(idx)
                .await
                .map_err(ApiError::from)
                .map_err(|err| contextualize_admin_pool_api_error(err, "clear decommission", format!("pool {idx}")))?;
        } else {
            let Some(store) = resolve_object_store_handle() else {
                return Err(decommission_admin_not_initialized_error_with_audit("clear decommission", audit));
            };

            store
                .clear_decommission(idx)
                .await
                .map_err(ApiError::from)
                .map_err(|err| contextualize_admin_pool_api_error(err, "clear decommission", format!("pool {idx}")))?;
        }

        info!(
            event = EVENT_ADMIN_RESPONSE_EMITTED,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_POOL_ADMIN,
            operation = "clear_decommission",
            action = "clear_decommission",
            result = "success",
            request_id = %request_id,
            actor = %actor,
            remote_addr = %remote_addr,
            pool_index = idx,
            "admin response emitted"
        );
        Ok(S3Response::new((StatusCode::OK, Body::default())))
    }
}

#[cfg(test)]
mod pools_handler_tests {
    use super::{
        AdminPoolStatus, PoolAuditContext, contextualize_admin_pool_api_error,
        decommission_admin_not_initialized_error_with_audit, decommission_peer_target, has_duplicate_indices,
        parse_mutation_pool_query, parse_pool_idx_by_id, parse_status_pool_query, pool_admin_missing_credentials_error,
        pool_admin_missing_credentials_error_with_request, pool_admin_pool_index_error_with_audit,
        pool_admin_pool_not_found_error_with_audit, pool_admin_pool_parse_error_with_audit, pool_admin_query_parse_error,
        pool_admin_query_parse_error_with_audit, validate_pool_mutation_leader, validate_start_decommission_guards,
    };
    use crate::admin::storage_api::runtime::{Endpoint, EndpointServerPools, Endpoints, PoolEndpoints};

    fn test_pool_endpoints(is_local: bool) -> EndpointServerPools {
        let mut endpoint = Endpoint::try_from("http://127.0.0.1:9000/disk").expect("test endpoint should parse");
        endpoint.is_local = is_local;
        EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 1,
            cmd_line: "http://127.0.0.1:9000/disk".to_string(),
            endpoints: Endpoints::from(vec![endpoint]),
            platform: String::new(),
        }])
    }

    #[test]
    fn test_parse_pool_idx_by_id_rejects_non_numeric() {
        assert_eq!(parse_pool_idx_by_id("invalid", 4), None);
    }

    #[test]
    fn test_parse_pool_idx_by_id_rejects_out_of_range() {
        assert_eq!(parse_pool_idx_by_id("4", 4), None);
    }

    #[test]
    fn test_parse_status_pool_query_ignores_unknown_but_rejects_duplicate_and_invalid_bool() {
        let unknown = "/rustfs/admin/v3/pools/status?pool=0&force=true"
            .parse()
            .expect("uri should parse");
        let query = parse_status_pool_query(&unknown).expect("status query should ignore unknown keys");
        assert_eq!(query.pool, "0");

        let duplicate = "/rustfs/admin/v3/pools/status?pool=0&pool=1"
            .parse()
            .expect("uri should parse");
        assert!(parse_status_pool_query(&duplicate).is_err());

        let invalid_bool = "/rustfs/admin/v3/pools/status?by-id=yes".parse().expect("uri should parse");
        assert!(parse_status_pool_query(&invalid_bool).is_err());
    }

    #[test]
    fn test_parse_mutation_pool_query_rejects_unknown_duplicate_and_invalid_bool() {
        let unknown = "/rustfs/admin/v3/pools/decommission?pool=0&force=true"
            .parse()
            .expect("uri should parse");
        assert!(parse_mutation_pool_query(&unknown).is_err());

        let duplicate = "/rustfs/admin/v3/pools/decommission?pool=0&pool=1"
            .parse()
            .expect("uri should parse");
        assert!(parse_mutation_pool_query(&duplicate).is_err());

        let invalid_bool = "/rustfs/admin/v3/pools/decommission?by-id=yes"
            .parse()
            .expect("uri should parse");
        assert!(parse_mutation_pool_query(&invalid_bool).is_err());
    }

    #[test]
    fn test_parse_status_pool_query_accepts_expected_keys() {
        let uri = "/rustfs/admin/v3/pools/status?pool=pool-a&by-id=true"
            .parse()
            .expect("uri should parse");
        let query = parse_status_pool_query(&uri).expect("valid query should parse");

        assert_eq!(query.pool, "pool-a");
        assert_eq!(query.by_id, "true");
    }

    #[test]
    fn test_parse_pool_idx_by_id_rejects_empty_pool_count() {
        assert_eq!(parse_pool_idx_by_id("0", 0), None);
    }

    #[test]
    fn test_parse_pool_idx_by_id_accepts_valid_index() {
        assert_eq!(parse_pool_idx_by_id("2", 4), Some(2));
    }

    #[test]
    fn test_validate_start_decommission_guards_rejects_decommission_running() {
        let err = validate_start_decommission_guards(true, false).expect_err("decommission running should be rejected");
        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some("DecommissionAlreadyRunning"));
    }

    #[test]
    fn test_validate_start_decommission_guards_rejects_rebalance_running() {
        let err = validate_start_decommission_guards(false, true).expect_err("rebalance running should be rejected");
        assert_eq!(err.code(), &s3s::S3ErrorCode::OperationAborted);
        assert_eq!(err.message(), Some("Decommission cannot be started, rebalance is already in progress"));
    }

    #[test]
    fn test_validate_start_decommission_guards_prefers_decommission_over_rebalance() {
        let err = validate_start_decommission_guards(true, true).expect_err("decommission should be checked before rebalance");
        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some("DecommissionAlreadyRunning"));
    }

    #[test]
    fn test_validate_start_decommission_guards_allows_when_idle() {
        assert!(validate_start_decommission_guards(false, false).is_ok());
    }

    #[test]
    fn test_validate_pool_mutation_leader_allows_local_first_endpoint() {
        let endpoints = test_pool_endpoints(true);
        let audit = PoolAuditContext::new("request", "actor", "remote");

        assert!(validate_pool_mutation_leader(&endpoints, 0, "cancel decommission", audit).is_ok());
    }

    #[test]
    fn test_decommission_peer_target_returns_none_for_local_first_endpoint() {
        let endpoints = test_pool_endpoints(true);
        let audit = PoolAuditContext::new("request", "actor", "remote");

        let target = decommission_peer_target(&endpoints, 0, "start decommission", audit)
            .expect("local first endpoint should resolve without peer lookup");

        assert!(target.is_none());
    }

    #[test]
    fn test_validate_pool_mutation_leader_rejects_remote_first_endpoint() {
        let endpoints = test_pool_endpoints(false);
        let audit = PoolAuditContext::new("request", "actor", "remote");

        let err = validate_pool_mutation_leader(&endpoints, 0, "cancel decommission", audit)
            .expect_err("remote first endpoint should reject mutation");

        assert_eq!(err.code(), &s3s::S3ErrorCode::OperationAborted);
        assert!(
            err.message()
                .expect("rejection should include message")
                .contains("must be handled by its first endpoint")
        );
    }

    #[test]
    fn test_contextualize_admin_pool_api_error_preserves_code_and_adds_pool_context() {
        let err = crate::error::ApiError {
            code: s3s::S3ErrorCode::InvalidRequest,
            message: "decommission already running".to_string(),
            source: None,
        };

        let err = contextualize_admin_pool_api_error(err, "start decommission", "pools [1, 3]");

        assert_eq!(err.code, s3s::S3ErrorCode::InvalidRequest);
        assert_eq!(
            err.message,
            "admin start decommission failed for pools [1, 3]: decommission already running"
        );
    }

    #[test]
    fn test_contextualize_admin_pool_api_error_preserves_source() {
        let err = contextualize_admin_pool_api_error(
            crate::error::ApiError::other(std::io::Error::other("boom")),
            "cancel decommission",
            "pool 2",
        );

        assert!(err.message.contains("admin cancel decommission failed for pool 2"));
        assert!(err.source.is_some());
    }

    #[test]
    fn test_decommission_admin_not_initialized_error_with_audit_preserves_response_contract() {
        let audit = PoolAuditContext::new("req-1", "access-key", "127.0.0.1:9000");
        let err = decommission_admin_not_initialized_error_with_audit("start decommission", audit);

        assert_eq!(err.code(), &s3s::S3ErrorCode::InternalError);
        assert_eq!(err.message(), Some("Failed to start decommission: object layer not initialized"));
    }

    #[test]
    fn test_pool_admin_missing_credentials_error_formats_list_context() {
        let err = pool_admin_missing_credentials_error("list pools");

        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some("Failed to list pools: missing credentials"));
    }

    #[test]
    fn test_pool_admin_missing_credentials_error_formats_decommission_context() {
        let err = pool_admin_missing_credentials_error("start decommission");

        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some("Failed to start decommission: missing credentials"));
    }

    #[test]
    fn test_pool_admin_missing_credentials_error_with_request_preserves_response_contract() {
        let err = pool_admin_missing_credentials_error_with_request("cancel decommission", "req-1", "127.0.0.1:9000");

        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some("Failed to cancel decommission: missing credentials"));
    }

    #[test]
    fn test_pool_admin_query_parse_error_formats_status_context() {
        let err = pool_admin_query_parse_error("load pool status");

        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("Failed to load pool status: invalid query parameters"));
    }

    #[test]
    fn test_pool_audit_context_keeps_request_actor_and_remote_addr() {
        let audit = PoolAuditContext::new("req-1", "access-key", "127.0.0.1:9000");

        assert_eq!(audit.request_id, "req-1");
        assert_eq!(audit.actor, "access-key");
        assert_eq!(audit.remote_addr, "127.0.0.1:9000");
    }

    #[test]
    fn test_pool_admin_query_parse_error_with_audit_preserves_response_contract() {
        let audit = PoolAuditContext::new("req-1", "access-key", "127.0.0.1:9000");
        let err = pool_admin_query_parse_error_with_audit("start decommission", audit);

        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("Failed to start decommission: invalid query parameters"));
    }

    #[test]
    fn test_pool_admin_pool_parse_error_with_audit_preserves_response_contract() {
        let audit = PoolAuditContext::new("req-1", "access-key", "127.0.0.1:9000");
        let err = pool_admin_pool_parse_error_with_audit("start decommission", "pool-x", audit);

        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("Failed to start decommission: invalid pool `pool-x`"));
    }

    #[test]
    fn test_pool_admin_pool_index_error_with_audit_preserves_response_contract() {
        let audit = PoolAuditContext::new("req-1", "access-key", "127.0.0.1:9000");
        let err = pool_admin_pool_index_error_with_audit("start decommission", 4, 2, audit);

        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidArgument);
        assert_eq!(
            err.message(),
            Some("Failed to start decommission: pool index 4 is out of range for 2 pools")
        );
    }

    #[test]
    fn test_pool_admin_pool_not_found_error_with_audit_preserves_response_contract() {
        let audit = PoolAuditContext::new("req-1", "access-key", "127.0.0.1:9000");
        let err = pool_admin_pool_not_found_error_with_audit("cancel decommission", "pool-x", audit);

        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("Failed to cancel decommission: pool `pool-x` was not found"));
    }

    #[test]
    fn test_has_duplicate_indices_detects_duplicate_indices() {
        assert!(has_duplicate_indices(&[0, 2, 1, 2, 3]));
    }

    #[test]
    fn test_has_duplicate_indices_allows_unique_and_empty_input() {
        let empty: Vec<usize> = Vec::new();
        assert!(!has_duplicate_indices(&empty));
        assert!(!has_duplicate_indices(&[0, 2, 1, 3]));
    }

    #[test]
    fn test_pool_status_response_exposes_admin_discovery_paths() {
        let response = super::PoolStatusResponse {
            pool: AdminPoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: time::OffsetDateTime::UNIX_EPOCH,
                total_size: 0,
                current_size: 0,
                used_size: 0,
                used: 0.0,
                status: "active".to_string(),
                decommission_status: "none".to_string(),
                rebalance_status: "none".to_string(),
                decommission: None,
            },
            admin_discovery: super::PoolAdminDiscovery {
                runtime_capabilities: "/rustfs/admin/v4/runtime/capabilities".to_string(),
                cluster_snapshot: "/rustfs/admin/v4/cluster/snapshot".to_string(),
                extensions_catalog: "/rustfs/admin/v4/extensions/catalog".to_string(),
            },
        };

        let value = serde_json::to_value(response).expect("pool status response should serialize");
        assert_eq!(value["admin_discovery"]["runtimeCapabilities"], "/rustfs/admin/v4/runtime/capabilities");
        assert_eq!(value["admin_discovery"]["clusterSnapshot"], "/rustfs/admin/v4/cluster/snapshot");
        assert_eq!(value["admin_discovery"]["extensionsCatalog"], "/rustfs/admin/v4/extensions/catalog");
    }
}
