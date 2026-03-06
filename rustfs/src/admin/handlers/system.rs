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

use super::metrics;
use crate::admin::auth::validate_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::app::admin_usecase::{DefaultAdminUsecase, QueryServerInfoRequest};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use http::{HeaderMap, HeaderValue};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_policy::policy::action::{Action, AdminAction, S3Action};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use tracing::warn;

pub fn register_system_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/service").as_str(),
        AdminOperation(&ServiceHandle {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/info").as_str(),
        AdminOperation(&ServerInfoHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/inspect-data").as_str(),
        AdminOperation(&InspectDataHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/inspect-data").as_str(),
        AdminOperation(&InspectDataHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/storageinfo").as_str(),
        AdminOperation(&StorageInfoHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/datausageinfo").as_str(),
        AdminOperation(&DataUsageInfoHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/metrics").as_str(),
        AdminOperation(&metrics::MetricsHandler {}),
    )?;

    Ok(())
}

pub struct ServiceHandle {}

#[async_trait::async_trait]
impl Operation for ServiceHandle {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle ServiceHandle");

        Err(s3_error!(NotImplemented))
    }
}

pub struct ServerInfoHandler {}

#[async_trait::async_trait]
impl Operation for ServerInfoHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::ServerInfoAdminAction)],
            remote_addr,
        )
        .await?;

        let usecase = DefaultAdminUsecase::from_global();
        let info = usecase
            .execute_query_server_info(QueryServerInfoRequest { include_pools: true })
            .await
            .map_err(S3Error::from)?
            .info;

        let data = serde_json::to_vec(&info)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse serverInfo failed"))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

pub struct InspectDataHandler {}

#[async_trait::async_trait]
impl Operation for InspectDataHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle InspectDataHandler");

        Err(s3_error!(NotImplemented))
    }
}

pub struct StorageInfoHandler {}

#[async_trait::async_trait]
impl Operation for StorageInfoHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle StorageInfoHandler");

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::StorageInfoAdminAction)],
            remote_addr,
        )
        .await?;

        let usecase = DefaultAdminUsecase::from_global();
        let info = usecase.execute_query_storage_info().await.map_err(S3Error::from)?;

        let data = serde_json::to_vec(&info)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "failed to serialize storage info"))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

pub struct DataUsageInfoHandler {}

#[async_trait::async_trait]
impl Operation for DataUsageInfoHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle DataUsageInfoHandler");

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![
                Action::AdminAction(AdminAction::DataUsageInfoAdminAction),
                Action::S3Action(S3Action::ListBucketAction),
            ],
            remote_addr,
        )
        .await?;

        let usecase = DefaultAdminUsecase::from_global();
        let info = usecase.execute_query_data_usage_info().await.map_err(S3Error::from)?;

        let data = serde_json::to_vec(&info)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse DataUsageInfo failed"))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}
