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
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use http::{HeaderMap, HeaderValue};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_ecstore::admin_server_info::get_server_info;
use rustfs_ecstore::data_usage::load_data_usage_from_backend;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::pools::{get_total_usable_capacity, get_total_usable_capacity_free};
use rustfs_ecstore::store_api::StorageAPI;
use rustfs_policy::policy::action::{Action, AdminAction, S3Action};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use tracing::{debug, error, info, warn};

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

        let info = get_server_info(true).await;

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

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        // TODO:getAggregatedBackgroundHealState
        let info = store.storage_info().await;

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

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let mut info = load_data_usage_from_backend(store.clone()).await.map_err(|e| {
            error!("load_data_usage_from_backend failed {:?}", e);
            s3_error!(InternalError, "load_data_usage_from_backend failed")
        })?;

        let sinfo = store.storage_info().await;

        // Use the fixed capacity calculation function (built-in deduplication)
        let raw_total = get_total_usable_capacity(&sinfo.disks, &sinfo);
        let raw_free = get_total_usable_capacity_free(&sinfo.disks, &sinfo);

        // Add a plausibility check (extra layer of protection)
        const MAX_REASONABLE_CAPACITY: u64 = 100_000 * 1024 * 1024 * 1024 * 1024; // 100 PiB
        const MIN_REASONABLE_CAPACITY: u64 = 1024 * 1024 * 1024; // 1 GiB

        let total_u64 = raw_total as u64;
        let free_u64 = raw_free as u64;

        // Detect outliers
        if total_u64 > MAX_REASONABLE_CAPACITY {
            error!(
                "Abnormal total capacity detected: {} bytes ({:.2} TiB), capping to physical capacity",
                total_u64,
                total_u64 as f64 / (1024.0_f64.powi(4))
            );

            let disk_count = sinfo.disks.len();
            if disk_count > 0 {
                use std::collections::HashSet;
                let unique_disks: HashSet<String> = sinfo
                    .disks
                    .iter()
                    .map(|d| format!("{}|{}", d.endpoint, d.drive_path))
                    .collect();

                let actual_disk_count = unique_disks.len();

                if let Some(first_disk) = sinfo.disks.first() {
                    info.total_capacity = first_disk.total_space * actual_disk_count as u64;
                    info.total_free_capacity = first_disk.available_space * actual_disk_count as u64;

                    info!(
                        "Applied capacity correction: {} unique disks, capacity per disk: {} bytes",
                        actual_disk_count, first_disk.total_space
                    );
                } else {
                    info.total_capacity = 0;
                    info.total_free_capacity = 0;
                }
            } else {
                info.total_capacity = 0;
                info.total_free_capacity = 0;
            }
        } else if total_u64 < MIN_REASONABLE_CAPACITY && total_u64 > 0 {
            warn!(
                "Unusually small total capacity: {} bytes ({:.2} GiB)",
                total_u64,
                total_u64 as f64 / (1024.0_f64.powi(3))
            );
            info.total_capacity = total_u64;
            info.total_free_capacity = free_u64;
        } else {
            info.total_capacity = total_u64;
            info.total_free_capacity = free_u64;
        }

        info.total_used_capacity = info.total_capacity.saturating_sub(info.total_free_capacity);

        debug!(
            "Capacity statistics: total={:.2} TiB, free={:.2} TiB, used={:.2} TiB",
            info.total_capacity as f64 / (1024.0_f64.powi(4)),
            info.total_free_capacity as f64 / (1024.0_f64.powi(4)),
            info.total_used_capacity as f64 / (1024.0_f64.powi(4))
        );

        let data = serde_json::to_vec(&info)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse DataUsageInfo failed"))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}
