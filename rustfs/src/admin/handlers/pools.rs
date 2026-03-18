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

fn validate_start_decommission_guards(
    decommission_running: bool,
    rebalance_running: bool,
) -> s3s::S3Result<()> {
    if decommission_running {
        return Err(s3_error!(InvalidRequest, "DecommissionAlreadyRunning"));
    }

    if rebalance_running {
        return Err(s3_error!(InvalidRequest, "RebalanceAlreadyRunning"));
    }

    Ok(())
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
            return Err(s3_error!(InvalidRequest, "get cred failed"));
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
        let pools_status = usecase.execute_list_pool_statuses().await.map_err(S3Error::from)?;

        let data = serde_json::to_vec(&pools_status)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse accountInfo failed"))?;

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
            return Err(s3_error!(InvalidRequest, "get cred failed"));
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
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed"))?;
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
            return Err(s3_error!(InvalidRequest, "get cred failed"));
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
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        validate_start_decommission_guards(store.is_decommission_running().await, store.is_rebalance_started().await)?;

        let query = {
            if let Some(query) = req.uri.query() {
                let input: StatusPoolQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed"))?;
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
                        .ok_or_else(|| s3_error!(InvalidArgument, "pool parse failed"))?
                } else {
                    let Some(idx) = endpoints.get_pool_idx(pool) else {
                        return Err(s3_error!(InvalidArgument, "pool parse failed"));
                    };
                    idx
                }
            };

            if idx >= store.pools.len() {
                return Err(s3_error!(InvalidArgument));
            }

            parsed_indices.push(idx);
        }
        let pools_indices = dedup_indices(&parsed_indices);

        if !pools_indices.is_empty() {
            store.decommission(ctx.clone(), pools_indices).await.map_err(ApiError::from)?;
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
            return Err(s3_error!(InvalidRequest, "get cred failed"));
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
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed"))?;
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
            return Err(s3_error!(InvalidArgument));
        };

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store.decommission_cancel(idx).await.map_err(ApiError::from)?;

        Ok(S3Response::new((StatusCode::OK, Body::default())))
    }
}

#[cfg(test)]
mod pools_handler_tests {
    use super::{dedup_indices, parse_pool_idx_by_id, validate_start_decommission_guards};

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
        assert!(err.to_string().contains("DecommissionAlreadyRunning"));
    }

    #[test]
    fn test_validate_start_decommission_guards_rejects_rebalance_running() {
        let err = validate_start_decommission_guards(false, true).expect_err("rebalance running should be rejected");
        assert!(err.to_string().contains("RebalanceAlreadyRunning"));
    }

    #[test]
    fn test_validate_start_decommission_guards_allows_when_idle() {
        assert!(validate_start_decommission_guards(false, false).is_ok());
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
