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

use http::{HeaderMap, StatusCode};
use matchit::Params;
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, header::CONTENT_TYPE, s3_error};
use serde::Deserialize;
use serde_urlencoded::from_bytes;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    admin::{
        auth::validate_admin_request,
        router::{AdminOperation, Operation, S3Router},
    },
    app::admin_usecase::{DefaultAdminUsecase, QueryPoolStatusRequest},
    app::context::resolve_endpoints_handle,
    auth::{check_key_valid, get_session_token},
    error::ApiError,
    server::{ADMIN_PREFIX, RemoteAddr},
};
use hyper::Method;
use rustfs_ecstore::new_object_layer_fn;

use std::collections::HashSet;

fn endpoints_from_context() -> Option<rustfs_ecstore::endpoints::EndpointServerPools> {
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

fn decommission_admin_not_initialized_error(operation: &str) -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, format!("Failed to {operation}: object layer not initialized"))
}

fn pool_admin_missing_credentials_error(operation: &str) -> S3Error {
    S3Error::with_message(S3ErrorCode::InvalidRequest, format!("Failed to {operation}: missing credentials"))
}

fn pool_admin_query_parse_error(operation: &str) -> S3Error {
    S3Error::with_message(S3ErrorCode::InvalidArgument, format!("Failed to {operation}: invalid query parameters"))
}

fn pool_admin_pool_parse_error(operation: &str, pool: &str) -> S3Error {
    S3Error::with_message(S3ErrorCode::InvalidArgument, format!("Failed to {operation}: invalid pool `{pool}`"))
}

fn pool_admin_pool_not_found_error(operation: &str, pool: &str) -> S3Error {
    S3Error::with_message(
        S3ErrorCode::InvalidArgument,
        format!("Failed to {operation}: pool `{pool}` was not found"),
    )
}

fn pool_admin_pool_index_error(operation: &str, idx: usize, pool_count: usize) -> S3Error {
    S3Error::with_message(
        S3ErrorCode::InvalidArgument,
        format!("Failed to {operation}: pool index {idx} is out of range for {pool_count} pools"),
    )
}

fn parse_pool_idx_by_id(pool: &str, endpoint_count: usize) -> Option<usize> {
    let idx = pool.parse::<usize>().ok()?;
    (idx < endpoint_count).then_some(idx)
}

fn dedup_indices(indices: &[usize]) -> Vec<usize> {
    let mut seen = HashSet::with_capacity(indices.len());
    let mut output = Vec::with_capacity(indices.len());
    for idx in indices {
        if seen.insert(*idx) {
            output.push(*idx);
        }
    }
    output
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
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/decommission").as_str(),
        AdminOperation(&StartDecommission {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/cancel").as_str(),
        AdminOperation(&CancelDecommission {}),
    )?;

    Ok(())
}

pub struct ListPools {}

#[async_trait::async_trait]
impl Operation for ListPools {
    // GET <endpoint>/<admin-API>/pools/list
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle ListPools");

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

        let usecase = DefaultAdminUsecase::from_global();
        let pool_items = usecase.execute_list_pools().await.map_err(S3Error::from)?;

        let data = serde_json::to_vec(&pool_items)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "serialize pools list failed"))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

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

pub struct StatusPool {}

#[async_trait::async_trait]
impl Operation for StatusPool {
    // GET <endpoint>/<admin-API>/pools/status?pool=http://server{1...4}/disk{1...4}
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle StatusPool");

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

        let query = {
            if let Some(query) = req.uri.query() {
                let input: StatusPoolQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| pool_admin_query_parse_error("load pool status"))?;
                input
            } else {
                StatusPoolQuery::default()
            }
        };

        let usecase = DefaultAdminUsecase::from_global();
        let pools_status = usecase
            .execute_query_pool_status(QueryPoolStatusRequest {
                pool: query.pool,
                by_id: query.by_id.as_str() == "true",
            })
            .await
            .map_err(S3Error::from)?;

        let data = serde_json::to_vec(&pools_status)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse accountInfo failed"))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

pub struct StartDecommission {}

#[async_trait::async_trait]
impl Operation for StartDecommission {
    // POST <endpoint>/<admin-API>/pools/decommission?pool=http://server{1...4}/disk{1...4}
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle StartDecommission");

        let Some(input_cred) = req.credentials else {
            return Err(pool_admin_missing_credentials_error("start decommission"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::DecommissionAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let Some(endpoints) = endpoints_from_context() else {
            return Err(s3_error!(NotImplemented));
        };

        if endpoints.legacy() {
            return Err(s3_error!(NotImplemented));
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(decommission_admin_not_initialized_error("start decommission"));
        };

        validate_start_decommission_guards(store.is_decommission_running().await, store.is_rebalance_started().await)?;

        let query = {
            if let Some(query) = req.uri.query() {
                let input: StatusPoolQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| pool_admin_query_parse_error("start decommission"))?;
                input
            } else {
                StatusPoolQuery::default()
            }
        };
        let is_byid = query.by_id.as_str() == "true";

        let pools: Vec<&str> = query.pool.split(",").collect();
        let mut parsed_indices = Vec::with_capacity(pools.len());

        let ctx = CancellationToken::new();

        for pool in pools.iter() {
            let idx = {
                if is_byid {
                    parse_pool_idx_by_id(pool, endpoints.as_ref().len())
                        .ok_or_else(|| pool_admin_pool_parse_error("start decommission", pool))?
                } else {
                    let Some(idx) = endpoints.get_pool_idx(pool) else {
                        return Err(pool_admin_pool_parse_error("start decommission", pool));
                    };
                    idx
                }
            };

            if idx >= store.pools.len() {
                return Err(pool_admin_pool_index_error("start decommission", idx, store.pools.len()));
            }

            parsed_indices.push(idx);
        }
        let pools_indices = dedup_indices(&parsed_indices);

        if !pools_indices.is_empty() {
            let pool_context = format!("pools {:?}", &pools_indices);
            store
                .decommission(ctx.clone(), pools_indices)
                .await
                .map_err(ApiError::from)
                .map_err(|err| contextualize_admin_pool_api_error(err, "start decommission", &pool_context))?;
        }

        Ok(S3Response::new((StatusCode::OK, Body::default())))
    }
}

pub struct CancelDecommission {}

#[async_trait::async_trait]
impl Operation for CancelDecommission {
    // POST <endpoint>/<admin-API>/pools/cancel?pool=http://server{1...4}/disk{1...4}
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle CancelDecommission");

        let Some(input_cred) = req.credentials else {
            return Err(pool_admin_missing_credentials_error("cancel decommission"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::DecommissionAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let Some(endpoints) = endpoints_from_context() else {
            return Err(s3_error!(NotImplemented));
        };

        if endpoints.legacy() {
            return Err(s3_error!(NotImplemented));
        }

        let query = {
            if let Some(query) = req.uri.query() {
                let input: StatusPoolQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| pool_admin_query_parse_error("cancel decommission"))?;
                input
            } else {
                StatusPoolQuery::default()
            }
        };

        let is_byid = query.by_id.as_str() == "true";

        let has_idx = {
            if is_byid {
                parse_pool_idx_by_id(&query.pool, endpoints.as_ref().len())
            } else {
                endpoints.get_pool_idx(&query.pool)
            }
        };

        let Some(idx) = has_idx else {
            warn!("specified pool {} not found, please specify a valid pool", &query.pool);
            return Err(pool_admin_pool_not_found_error("cancel decommission", &query.pool));
        };

        let Some(store) = new_object_layer_fn() else {
            return Err(decommission_admin_not_initialized_error("cancel decommission"));
        };

        store
            .decommission_cancel(idx)
            .await
            .map_err(ApiError::from)
            .map_err(|err| contextualize_admin_pool_api_error(err, "cancel decommission", format!("pool {idx}")))?;

        Ok(S3Response::new((StatusCode::OK, Body::default())))
    }
}

#[cfg(test)]
mod pools_handler_tests {
    use super::{
        contextualize_admin_pool_api_error, decommission_admin_not_initialized_error, dedup_indices, parse_pool_idx_by_id,
        pool_admin_missing_credentials_error, pool_admin_pool_index_error, pool_admin_pool_not_found_error,
        pool_admin_pool_parse_error, pool_admin_query_parse_error, validate_start_decommission_guards,
    };

    #[test]
    fn test_parse_pool_idx_by_id_rejects_non_numeric() {
        assert_eq!(parse_pool_idx_by_id("invalid", 4), None);
    }

    #[test]
    fn test_parse_pool_idx_by_id_rejects_out_of_range() {
        assert_eq!(parse_pool_idx_by_id("4", 4), None);
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
    fn test_decommission_admin_not_initialized_error_formats_start_context() {
        let err = decommission_admin_not_initialized_error("start decommission");

        assert_eq!(err.code(), &s3s::S3ErrorCode::InternalError);
        assert_eq!(err.message(), Some("Failed to start decommission: object layer not initialized"));
    }

    #[test]
    fn test_decommission_admin_not_initialized_error_formats_cancel_context() {
        let err = decommission_admin_not_initialized_error("cancel decommission");

        assert_eq!(err.code(), &s3s::S3ErrorCode::InternalError);
        assert_eq!(err.message(), Some("Failed to cancel decommission: object layer not initialized"));
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
    fn test_pool_admin_query_parse_error_formats_status_context() {
        let err = pool_admin_query_parse_error("load pool status");

        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("Failed to load pool status: invalid query parameters"));
    }

    #[test]
    fn test_pool_admin_pool_parse_error_formats_pool_context() {
        let err = pool_admin_pool_parse_error("start decommission", "pool-x");

        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("Failed to start decommission: invalid pool `pool-x`"));
    }

    #[test]
    fn test_pool_admin_pool_index_error_formats_range_context() {
        let err = pool_admin_pool_index_error("start decommission", 4, 2);

        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidArgument);
        assert_eq!(
            err.message(),
            Some("Failed to start decommission: pool index 4 is out of range for 2 pools")
        );
    }

    #[test]
    fn test_pool_admin_pool_not_found_error_formats_cancel_context() {
        let err = pool_admin_pool_not_found_error("cancel decommission", "pool-x");

        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("Failed to cancel decommission: pool `pool-x` was not found"));
    }

    #[test]
    fn test_dedup_indices_removes_duplicates_preserving_order() {
        assert_eq!(dedup_indices(&[0, 2, 1, 2, 3, 0]), vec![0, 2, 1, 3]);
    }

    #[test]
    fn test_dedup_indices_handles_empty_input() {
        let empty: Vec<usize> = Vec::new();
        assert!(dedup_indices(&empty).is_empty());
    }
}
