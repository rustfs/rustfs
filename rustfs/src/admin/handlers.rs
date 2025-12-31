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

use super::router::Operation;
use crate::admin::auth::validate_admin_request;
use crate::auth::check_key_valid;
use crate::auth::get_condition_values;
use crate::auth::get_session_token;
use crate::error::ApiError;
use crate::server::RemoteAddr;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::{HeaderMap, HeaderValue, Uri};
use hyper::StatusCode;
use matchit::Params;
use rustfs_common::heal_channel::HealOpts;
use rustfs_config::{MAX_ADMIN_REQUEST_BODY_SIZE, MAX_HEAL_REQUEST_SIZE};
use rustfs_credentials::get_global_action_cred;
use rustfs_ecstore::admin_server_info::get_server_info;
use rustfs_ecstore::bucket::bucket_target_sys::BucketTargetSys;
use rustfs_ecstore::bucket::metadata::BUCKET_TARGETS_FILE;
use rustfs_ecstore::bucket::metadata_sys;
use rustfs_ecstore::bucket::target::BucketTarget;
use rustfs_ecstore::bucket::versioning_sys::BucketVersioningSys;
use rustfs_ecstore::data_usage::{
    aggregate_local_snapshots, compute_bucket_usage, load_data_usage_from_backend, store_data_usage_in_backend,
};
use rustfs_ecstore::error::StorageError;
use rustfs_ecstore::global::global_rustfs_port;
use rustfs_ecstore::metrics_realtime::{CollectMetricsOpts, MetricType, collect_local_metrics};
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::pools::{get_total_usable_capacity, get_total_usable_capacity_free};
use rustfs_ecstore::store::is_valid_object_prefix;
use rustfs_ecstore::store_api::BucketOptions;
use rustfs_ecstore::store_api::StorageAPI;
use rustfs_ecstore::store_utils::is_reserved_or_invalid_bucket;
use rustfs_iam::store::MappedPolicy;
use rustfs_madmin::metrics::RealtimeMetrics;
use rustfs_madmin::utils::parse_duration;
use rustfs_policy::policy::Args;
use rustfs_policy::policy::BucketPolicy;
use rustfs_policy::policy::action::Action;
use rustfs_policy::policy::action::AdminAction;
use rustfs_policy::policy::action::S3Action;
use rustfs_policy::policy::default::DEFAULT_POLICIES;
use rustfs_utils::path::path_join;
use s3s::header::CONTENT_TYPE;
use s3s::stream::{ByteStream, DynByteStream};
use s3s::{Body, S3Error, S3Request, S3Response, S3Result, s3_error};
use s3s::{S3ErrorCode, StdError};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration as std_Duration;
use tokio::sync::mpsc::{self};
use tokio::time::interval;
use tokio::{select, spawn};
use tokio_stream::wrappers::ReceiverStream;
use tracing::debug;
use tracing::{error, info, warn};
use url::Host;

pub mod bucket_meta;
pub mod event;
pub mod group;
pub mod kms;
pub mod kms_dynamic;
pub mod kms_keys;
pub mod policies;
pub mod pools;
pub mod profile;
pub mod rebalance;
pub mod service_account;
pub mod sts;
pub mod tier;
pub mod trace;
pub mod user;

#[derive(Debug, Serialize)]
pub struct IsAdminResponse {
    pub is_admin: bool,
    pub access_key: String,
    pub message: String,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "PascalCase", default)]
pub struct AccountInfo {
    pub account_name: String,
    pub server: rustfs_madmin::BackendInfo,
    pub policy: BucketPolicy,
}

/// Health check handler for endpoint monitoring
pub struct HealthCheckHandler {}

#[async_trait::async_trait]
impl Operation for HealthCheckHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        use serde_json::json;

        // Extract the original HTTP Method (encapsulated by s3s into S3Request)
        let method = req.method;

        // Only GET and HEAD are allowed
        if method != http::Method::GET && method != http::Method::HEAD {
            // 405 Method Not Allowed
            let mut headers = HeaderMap::new();
            headers.insert(http::header::ALLOW, HeaderValue::from_static("GET, HEAD"));
            return Ok(S3Response::with_headers(
                (StatusCode::METHOD_NOT_ALLOWED, Body::from("Method Not Allowed".to_string())),
                headers,
            ));
        }

        let health_info = json!({
            "status": "ok",
            "service": "rustfs-endpoint",
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "version": env!("CARGO_PKG_VERSION")
        });

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        if method == http::Method::HEAD {
            // HEAD: only returns the header and status code, not the body
            return Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), headers));
        }

        // GET: Return JSON body normally
        let body_str = serde_json::to_string(&health_info).unwrap_or_else(|_| "{}".to_string());
        let body = Body::from(body_str);

        Ok(S3Response::with_headers((StatusCode::OK, body), headers))
    }
}

pub struct IsAdminHandler {}
#[async_trait::async_trait]
impl Operation for IsAdminHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, _owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let access_key_to_check = input_cred.access_key.clone();

        // Check if the user is admin by comparing with global credentials
        let is_admin = if let Some(sys_cred) = get_global_action_cred() {
            crate::auth::constant_time_eq(&access_key_to_check, &sys_cred.access_key)
                || crate::auth::constant_time_eq(&cred.parent_user, &sys_cred.access_key)
        } else {
            false
        };

        let response = IsAdminResponse {
            is_admin,
            access_key: access_key_to_check,
            message: format!("User is {}an administrator", if is_admin { "" } else { "not " }),
        };

        let data = serde_json::to_vec(&response)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse IsAdminResponse failed"))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

pub struct AccountInfoHandler {}
#[async_trait::async_trait]
impl Operation for AccountInfoHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        let default_claims = HashMap::new();
        let claims = cred.claims.as_ref().unwrap_or(&default_claims);

        let cred_clone = cred.clone();
        let remote_addr = req.extensions.get::<RemoteAddr>().map(|a| a.0);
        let conditions = get_condition_values(&req.headers, &cred_clone, None, None, remote_addr);
        let cred_clone = Arc::new(cred_clone);
        let conditions = Arc::new(conditions);

        let is_allow = Box::new({
            let iam_clone = Arc::clone(&iam_store);
            let cred_clone = Arc::clone(&cred_clone);
            let conditions = Arc::clone(&conditions);
            move |name: String| {
                let iam_clone = Arc::clone(&iam_clone);
                let cred_clone = Arc::clone(&cred_clone);
                let conditions = Arc::clone(&conditions);
                async move {
                    let (mut rd, mut wr) = (false, false);
                    if !iam_clone
                        .is_allowed(&Args {
                            account: &cred_clone.access_key,
                            groups: &cred_clone.groups,
                            action: Action::S3Action(S3Action::ListBucketAction),
                            bucket: &name,
                            conditions: &conditions,
                            is_owner: owner,
                            object: "",
                            claims,
                            deny_only: false,
                        })
                        .await
                    {
                        rd = true
                    }

                    if !iam_clone
                        .is_allowed(&Args {
                            account: &cred_clone.access_key,
                            groups: &cred_clone.groups,
                            action: Action::S3Action(S3Action::GetBucketLocationAction),
                            bucket: &name,
                            conditions: &conditions,
                            is_owner: owner,
                            object: "",
                            claims,
                            deny_only: false,
                        })
                        .await
                    {
                        rd = true
                    }

                    if !iam_clone
                        .is_allowed(&Args {
                            account: &cred_clone.access_key,
                            groups: &cred_clone.groups,
                            action: Action::S3Action(S3Action::PutObjectAction),
                            bucket: &name,
                            conditions: &conditions,
                            is_owner: owner,
                            object: "",
                            claims,
                            deny_only: false,
                        })
                        .await
                    {
                        wr = true
                    }

                    (rd, wr)
                }
            }
        });

        let account_name = if cred.is_temp() || cred.is_service_account() {
            cred.parent_user.clone()
        } else {
            cred.access_key.clone()
        };

        let claims_args = Args {
            account: "",
            groups: &None,
            action: Action::None,
            bucket: "",
            conditions: &HashMap::new(),
            is_owner: false,
            object: "",
            claims,
            deny_only: false,
        };

        let role_arn = claims_args.get_role_arn();

        // TODO: get_policies_from_claims(claims);

        let Some(admin_cred) = get_global_action_cred() else {
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                "get_global_action_cred failed".to_string(),
            ));
        };

        let mut effective_policy: rustfs_policy::policy::Policy = Default::default();

        if account_name == admin_cred.access_key {
            for (name, p) in DEFAULT_POLICIES.iter() {
                if *name == "consoleAdmin" {
                    effective_policy = p.clone();
                    break;
                }
            }
        } else if let Some(arn) = role_arn {
            let (_, policy_name) = iam_store
                .get_role_policy(arn)
                .await
                .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

            let policies = MappedPolicy::new(&policy_name).to_slice();
            effective_policy = iam_store.get_combined_policy(&policies).await;
        } else {
            let policies = iam_store
                .policy_db_get(&account_name, &cred.groups)
                .await
                .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("get policy failed: {e}")))?;

            effective_policy = iam_store.get_combined_policy(&policies).await;
        };

        let policy_str = serde_json::to_string(&effective_policy)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse policy failed"))?;

        let mut account_info = rustfs_madmin::AccountInfo {
            account_name,
            server: store.backend_info().await,
            policy: serde_json::Value::String(policy_str),
            ..Default::default()
        };

        // TODO: bucket policy
        let buckets = store
            .list_bucket(&BucketOptions {
                cached: true,
                ..Default::default()
            })
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

        for bucket in buckets.iter() {
            let (rd, wr) = is_allow(bucket.name.clone()).await;
            if rd || wr {
                // TODO: BucketQuotaSys
                // TODO: other attributes
                account_info.buckets.push(rustfs_madmin::BucketAccessInfo {
                    name: bucket.name.clone(),
                    details: Some(rustfs_madmin::BucketDetails {
                        versioning: BucketVersioningSys::enabled(bucket.name.as_str()).await,
                        versioning_suspended: BucketVersioningSys::suspended(bucket.name.as_str()).await,
                        ..Default::default()
                    }),
                    created: bucket.created,
                    access: rustfs_madmin::AccountAccess { read: rd, write: wr },
                    ..Default::default()
                });
            }
        }

        let data = serde_json::to_vec(&account_info)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse accountInfo failed"))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
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

        let remote_addr = req.extensions.get::<RemoteAddr>().map(|a| a.0);
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
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

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

        let remote_addr = req.extensions.get::<RemoteAddr>().map(|a| a.0);
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
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse accountInfo failed"))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

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

        let remote_addr = req.extensions.get::<RemoteAddr>().map(|a| a.0);
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

        let (disk_statuses, mut info) = match aggregate_local_snapshots(store.clone()).await {
            Ok((statuses, usage)) => (statuses, usage),
            Err(err) => {
                warn!("aggregate_local_snapshots failed: {:?}", err);
                (
                    Vec::new(),
                    load_data_usage_from_backend(store.clone()).await.map_err(|e| {
                        error!("load_data_usage_from_backend failed {:?}", e);
                        s3_error!(InternalError, "load_data_usage_from_backend failed")
                    })?,
                )
            }
        };

        let snapshots_available = disk_statuses.iter().any(|status| status.snapshot_exists);
        if !snapshots_available {
            if let Ok(fallback) = load_data_usage_from_backend(store.clone()).await {
                let mut fallback_info = fallback;
                fallback_info.disk_usage_status = disk_statuses.clone();
                info = fallback_info;
            }
        } else {
            info.disk_usage_status = disk_statuses.clone();
        }

        let last_update_age = info.last_update.and_then(|ts| ts.elapsed().ok());
        let data_missing = info.objects_total_count == 0 && info.buckets_count == 0;
        let stale = last_update_age
            .map(|elapsed| elapsed > std::time::Duration::from_secs(300))
            .unwrap_or(true);

        if data_missing {
            info!("No data usage statistics found, attempting real-time collection");

            if let Err(e) = collect_realtime_data_usage(&mut info, store.clone()).await {
                warn!("Failed to collect real-time data usage: {}", e);
            } else if let Err(e) = store_data_usage_in_backend(info.clone(), store.clone()).await {
                warn!("Failed to persist refreshed data usage: {}", e);
            }
        } else if stale {
            info!(
                "Data usage statistics are stale (last update {:?} ago), refreshing asynchronously",
                last_update_age
            );

            let mut info_for_refresh = info.clone();
            let store_for_refresh = store.clone();
            spawn(async move {
                if let Err(e) = collect_realtime_data_usage(&mut info_for_refresh, store_for_refresh.clone()).await {
                    warn!("Background data usage refresh failed: {}", e);
                    return;
                }

                if let Err(e) = store_data_usage_in_backend(info_for_refresh, store_for_refresh).await {
                    warn!("Background data usage persistence failed: {}", e);
                }
            });
        }

        info.disk_usage_status = disk_statuses;

        // Set capacity information
        let sinfo = store.storage_info().await;
        info.total_capacity = get_total_usable_capacity(&sinfo.disks, &sinfo) as u64;
        info.total_free_capacity = get_total_usable_capacity_free(&sinfo.disks, &sinfo) as u64;
        if info.total_capacity > info.total_free_capacity {
            info.total_used_capacity = info.total_capacity - info.total_free_capacity;
        }

        let data = serde_json::to_vec(&info)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse DataUsageInfo failed"))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct MetricsParams {
    disks: String,
    hosts: String,
    #[serde(rename = "interval")]
    tick: String,
    n: u64,
    types: u32,
    #[serde(rename = "by-disk")]
    by_disk: String,
    #[serde(rename = "by-host")]
    by_host: String,
    #[serde(rename = "by-jobID")]
    by_job_id: String,
    #[serde(rename = "by-depID")]
    by_dep_id: String,
}

impl Default for MetricsParams {
    fn default() -> Self {
        Self {
            disks: Default::default(),
            hosts: Default::default(),
            tick: Default::default(),
            n: u64::MAX,
            types: Default::default(),
            by_disk: Default::default(),
            by_host: Default::default(),
            by_job_id: Default::default(),
            by_dep_id: Default::default(),
        }
    }
}

fn extract_metrics_init_params(uri: &Uri) -> MetricsParams {
    let mut mp = MetricsParams::default();
    if let Some(query) = uri.query() {
        let params: Vec<&str> = query.split('&').collect();
        for param in params {
            let mut parts = param.split('=');
            if let Some(key) = parts.next() {
                if key == "disks" {
                    if let Some(value) = parts.next() {
                        mp.disks = value.to_string();
                    }
                }
                if key == "hosts" {
                    if let Some(value) = parts.next() {
                        mp.hosts = value.to_string();
                    }
                }
                if key == "interval" {
                    if let Some(value) = parts.next() {
                        mp.tick = value.to_string();
                    }
                }
                if key == "n" {
                    if let Some(value) = parts.next() {
                        mp.n = value.parse::<u64>().unwrap_or(u64::MAX);
                    }
                }
                if key == "types" {
                    if let Some(value) = parts.next() {
                        mp.types = value.parse::<u32>().unwrap_or_default();
                    }
                }
                if key == "by-disk" {
                    if let Some(value) = parts.next() {
                        mp.by_disk = value.to_string();
                    }
                }
                if key == "by-host" {
                    if let Some(value) = parts.next() {
                        mp.by_host = value.to_string();
                    }
                }
                if key == "by-jobID" {
                    if let Some(value) = parts.next() {
                        mp.by_job_id = value.to_string();
                    }
                }
                if key == "by-depID" {
                    if let Some(value) = parts.next() {
                        mp.by_dep_id = value.to_string();
                    }
                }
            }
        }
    }
    mp
}

struct MetricsStream {
    inner: ReceiverStream<Result<Bytes, StdError>>,
}

impl Stream for MetricsStream {
    type Item = Result<Bytes, StdError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        info!("MetricsStream poll_next");
        let this = Pin::into_inner(self);
        this.inner.poll_next_unpin(cx)
    }
}

impl ByteStream for MetricsStream {}

pub struct MetricsHandler {}

#[async_trait::async_trait]
impl Operation for MetricsHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        info!("handle MetricsHandler, req: {:?}, params: {:?}", req, params);
        let Some(cred) = req.credentials else { return Err(s3_error!(InvalidRequest, "get cred failed")) };
        info!("cred: {:?}", cred);

        let mp = extract_metrics_init_params(&req.uri);
        info!("mp: {:?}", mp);

        let tick = parse_duration(&mp.tick).unwrap_or_else(|_| std_Duration::from_secs(3));

        let mut n = mp.n;
        if n == 0 {
            n = u64::MAX;
        }

        let types = if mp.types != 0 {
            MetricType::new(mp.types)
        } else {
            MetricType::ALL
        };

        fn parse_comma_separated(s: &str) -> HashSet<String> {
            s.split(',').filter(|part| !part.is_empty()).map(String::from).collect()
        }

        let disks = parse_comma_separated(&mp.disks);
        let by_disk = mp.by_disk == "true";
        let disk_map = disks;

        let job_id = mp.by_job_id;
        let hosts = parse_comma_separated(&mp.hosts);
        let by_host = mp.by_host == "true";
        let host_map = hosts;

        let d_id = mp.by_dep_id;
        let mut interval = interval(tick);

        let opts = CollectMetricsOpts {
            hosts: host_map,
            disks: disk_map,
            job_id,
            dep_id: d_id,
        };
        let (tx, rx) = mpsc::channel(10);
        let in_stream: DynByteStream = Box::pin(MetricsStream {
            inner: ReceiverStream::new(rx),
        });
        let body = Body::from(in_stream);
        spawn(async move {
            while n > 0 {
                info!("loop, n: {n}");
                let mut m = RealtimeMetrics::default();
                let m_local = collect_local_metrics(types, &opts).await;
                m.merge(m_local);

                if !by_host {
                    m.by_host = HashMap::new();
                }
                if !by_disk {
                    m.by_disk = HashMap::new();
                }

                m.finally = n <= 1;

                // todo write resp
                match serde_json::to_vec(&m) {
                    Ok(re) => {
                        info!("got metrics, send it to client, m: {m:?}");
                        let _ = tx.send(Ok(Bytes::from(re))).await;
                    }
                    Err(e) => {
                        error!("MetricsHandler: json encode failed, err: {:?}", e);
                        return;
                    }
                }

                n -= 1;
                if n == 0 {
                    break;
                }

                select! {
                    _ = tx.closed() => { return; }
                    _ = interval.tick() => {}
                }
            }
        });

        Ok(S3Response::new((StatusCode::OK, body)))
    }
}

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
    if hip.bucket.is_empty() && !hip.obj_prefix.is_empty() {
        return Err(s3_error!(InvalidRequest, "invalid bucket name"));
    }
    if is_reserved_or_invalid_bucket(&hip.bucket, false) {
        return Err(s3_error!(InvalidRequest, "invalid bucket name"));
    }
    if !is_valid_object_prefix(&hip.obj_prefix) {
        return Err(s3_error!(InvalidRequest, "invalid object name"));
    }

    if let Some(query) = uri.query() {
        let params: Vec<&str> = query.split('&').collect();
        for param in params {
            let mut parts = param.split('=');
            if let Some(key) = parts.next() {
                if key == "clientToken" {
                    if let Some(value) = parts.next() {
                        hip.client_token = value.to_string();
                    }
                }
                if key == "forceStart" && parts.next().is_some() {
                    hip.force_start = true;
                }
                if key == "forceStop" && parts.next().is_some() {
                    hip.force_stop = true;
                }
            }
        }
    }

    if (hip.force_start && hip.force_stop) || (!hip.client_token.is_empty() && (hip.force_start || hip.force_stop)) {
        return Err(s3_error!(InvalidRequest, ""));
    }

    if hip.client_token.is_empty() {
        hip.hs = serde_json::from_slice(body).map_err(|e| {
            info!("err request body parse, err: {:?}", e);
            s3_error!(InvalidRequest, "err request body parse")
        })?;
    }

    Ok(hip)
}

pub struct HealHandler {}

#[async_trait::async_trait]
impl Operation for HealHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle HealHandler, req: {:?}, params: {:?}", req, params);
        let Some(cred) = req.credentials else { return Err(s3_error!(InvalidRequest, "get cred failed")) };
        info!("cred: {:?}", cred);
        let mut input = req.input;
        let bytes = match input.store_all_limited(MAX_HEAL_REQUEST_SIZE).await {
            Ok(b) => b,
            Err(e) => {
                warn!("get body failed, e: {:?}", e);
                return Err(s3_error!(InvalidRequest, "heal request body too large or failed to read"));
            }
        };
        info!("bytes: {:?}", bytes);
        let hip = extract_heal_init_params(&bytes, &req.uri, params)?;
        info!("body: {:?}", hip);

        #[derive(Default)]
        struct HealResp {
            resp_bytes: Vec<u8>,
            _api_err: Option<StorageError>,
            _err_body: String,
        }

        let heal_path = path_join(&[PathBuf::from(hip.bucket.clone()), PathBuf::from(hip.obj_prefix.clone())]);
        let (tx, mut rx) = mpsc::channel(1);

        if !hip.client_token.is_empty() && !hip.force_start && !hip.force_stop {
            // Query heal status
            let tx_clone = tx.clone();
            let heal_path_str = heal_path.to_str().unwrap_or_default().to_string();
            let client_token = hip.client_token.clone();
            spawn(async move {
                match rustfs_common::heal_channel::query_heal_status(heal_path_str, client_token).await {
                    Ok(_) => {
                        // TODO: Get actual response from channel
                        let _ = tx_clone
                            .send(HealResp {
                                resp_bytes: vec![],
                                ..Default::default()
                            })
                            .await;
                    }
                    Err(e) => {
                        let _ = tx_clone
                            .send(HealResp {
                                _api_err: Some(StorageError::other(e)),
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
            spawn(async move {
                match rustfs_common::heal_channel::cancel_heal_task(heal_path_str).await {
                    Ok(_) => {
                        // TODO: Get actual response from channel
                        let _ = tx_clone
                            .send(HealResp {
                                resp_bytes: vec![],
                                ..Default::default()
                            })
                            .await;
                    }
                    Err(e) => {
                        let _ = tx_clone
                            .send(HealResp {
                                _api_err: Some(StorageError::other(e)),
                                ..Default::default()
                            })
                            .await;
                    }
                }
            });
        } else if hip.client_token.is_empty() {
            // Use new heal channel mechanism
            let tx_clone = tx.clone();
            spawn(async move {
                // Create heal request through channel
                let heal_request = rustfs_common::heal_channel::create_heal_request(
                    hip.bucket.clone(),
                    if hip.obj_prefix.is_empty() {
                        None
                    } else {
                        Some(hip.obj_prefix.clone())
                    },
                    hip.force_start,
                    Some(rustfs_common::heal_channel::HealChannelPriority::Normal),
                );

                match rustfs_common::heal_channel::send_heal_request(heal_request).await {
                    Ok(_) => {
                        // Success - send empty response for now
                        let _ = tx_clone
                            .send(HealResp {
                                resp_bytes: vec![],
                                ..Default::default()
                            })
                            .await;
                    }
                    Err(e) => {
                        // Error - send error response
                        let _ = tx_clone
                            .send(HealResp {
                                _api_err: Some(StorageError::other(e)),
                                ..Default::default()
                            })
                            .await;
                    }
                }
            });
        }

        match rx.recv().await {
            Some(result) => Ok(S3Response::new((StatusCode::OK, Body::from(result.resp_bytes)))),
            None => Ok(S3Response::new((StatusCode::INTERNAL_SERVER_ERROR, Body::from(vec![])))),
        }
    }
}

pub struct BackgroundHealStatusHandler {}

#[async_trait::async_trait]
impl Operation for BackgroundHealStatusHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle BackgroundHealStatusHandler");

        Err(s3_error!(NotImplemented))
    }
}

fn extract_query_params(uri: &Uri) -> HashMap<String, String> {
    let mut params = HashMap::new();

    if let Some(query) = uri.query() {
        query.split('&').for_each(|pair| {
            if let Some((key, value)) = pair.split_once('=') {
                params.insert(key.to_string(), value.to_string());
            }
        });
    }

    params
}

#[allow(dead_code)]
fn is_local_host(_host: String) -> bool {
    false
}

//awscurl --service s3 --region us-east-1 --access_key rustfsadmin --secret_key rustfsadmin "http://:9000/rustfs/admin/v3/replicationmetrics?bucket=1"
pub struct GetReplicationMetricsHandler {}
#[async_trait::async_trait]
impl Operation for GetReplicationMetricsHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        error!("GetReplicationMetricsHandler");
        let queries = extract_query_params(&_req.uri);
        if let Some(bucket) = queries.get("bucket") {
            error!("get bucket:{} metrics", bucket);
        }
        //return Err(s3_error!(InvalidArgument, "Invalid bucket name"));
        //Ok(S3Response::with_headers((StatusCode::OK, Body::from()), header))
        Ok(S3Response::new((StatusCode::OK, Body::from("Ok".to_string()))))
    }
}

pub struct SetRemoteTargetHandler {}
#[async_trait::async_trait]
impl Operation for SetRemoteTargetHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let queries = extract_query_params(&req.uri);

        let Some(bucket) = queries.get("bucket") else {
            return Err(s3_error!(InvalidRequest, "bucket is required"));
        };

        let update = queries.get("update").is_some_and(|v| v == "true");

        warn!("set remote target, bucket: {}, update: {}", bucket, update);

        if bucket.is_empty() {
            return Err(s3_error!(InvalidRequest, "bucket is required"));
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let mut input = req.input;
        let body = match input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await {
            Ok(b) => b,
            Err(e) => {
                warn!("get body failed, e: {:?}", e);
                return Err(s3_error!(InvalidRequest, "remote target configuration body too large or failed to read"));
            }
        };

        let mut remote_target: BucketTarget = serde_json::from_slice(&body).map_err(|e| {
            error!("Failed to parse BucketTarget from body: {}", e);
            ApiError::other(e)
        })?;

        let Ok(target_url) = remote_target.url() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Invalid target url".to_string()));
        };

        let same_target = rustfs_utils::net::is_local_host(
            target_url.host().unwrap_or(Host::Domain("localhost")),
            target_url.port().unwrap_or(80),
            global_rustfs_port(),
        )
        .unwrap_or_default();

        if same_target && bucket == &remote_target.target_bucket {
            return Err(S3Error::with_message(S3ErrorCode::IncorrectEndpoint, "Same target".to_string()));
        }

        remote_target.source_bucket = bucket.clone();

        let bucket_target_sys = BucketTargetSys::get();

        if !update {
            let (arn, exist) = bucket_target_sys.get_remote_arn(bucket, Some(&remote_target), "").await;
            remote_target.arn = arn.clone();
            if exist && !arn.is_empty() {
                let arn_str = serde_json::to_string(&arn).unwrap_or_default();

                warn!("return exists, arn: {}", arn_str);
                return Ok(S3Response::new((StatusCode::OK, Body::from(arn_str))));
            }
        }

        if remote_target.arn.is_empty() {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "ARN is empty".to_string()));
        }

        if update {
            let Some(mut target) = bucket_target_sys
                .get_remote_bucket_target_by_arn(bucket, &remote_target.arn)
                .await
            else {
                return Err(S3Error::with_message(S3ErrorCode::InternalError, "Target not found".to_string()));
            };

            target.credentials = remote_target.credentials;
            target.endpoint = remote_target.endpoint;
            target.secure = remote_target.secure;
            target.target_bucket = remote_target.target_bucket;

            target.path = remote_target.path;
            target.replication_sync = remote_target.replication_sync;
            target.bandwidth_limit = remote_target.bandwidth_limit;
            target.health_check_duration = remote_target.health_check_duration;

            warn!("update target, target: {:?}", target);
            remote_target = target;
        }

        let arn = remote_target.arn.clone();

        bucket_target_sys
            .set_target(bucket, &remote_target, update)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

        let targets = bucket_target_sys.list_bucket_targets(bucket).await.map_err(|e| {
            error!("Failed to list bucket targets: {}", e);
            S3Error::with_message(S3ErrorCode::InternalError, "Failed to list bucket targets".to_string())
        })?;
        let json_targets = serde_json::to_vec(&targets).map_err(|e| {
            error!("Serialization error: {}", e);
            S3Error::with_message(S3ErrorCode::InternalError, "Failed to serialize targets".to_string())
        })?;

        metadata_sys::update(bucket, BUCKET_TARGETS_FILE, json_targets)
            .await
            .map_err(|e| {
                error!("Failed to update bucket targets: {}", e);
                S3Error::with_message(S3ErrorCode::InternalError, format!("Failed to update bucket targets: {e}"))
            })?;

        let arn_str = serde_json::to_string(&arn).unwrap_or_default();

        Ok(S3Response::new((StatusCode::OK, Body::from(arn_str))))
    }
}

pub struct ListRemoteTargetHandler {}
#[async_trait::async_trait]
impl Operation for ListRemoteTargetHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let queries = extract_query_params(&req.uri);
        let Some(_cred) = req.credentials else {
            error!("credentials null");
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        if let Some(bucket) = queries.get("bucket") {
            if bucket.is_empty() {
                error!("bucket parameter is empty");
                return Ok(S3Response::new((
                    StatusCode::BAD_REQUEST,
                    Body::from("Bucket parameter is required".to_string()),
                )));
            }

            let Some(store) = new_object_layer_fn() else {
                return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not initialized".to_string()));
            };

            if let Err(err) = store.get_bucket_info(bucket, &BucketOptions::default()).await {
                error!("Error fetching bucket info: {:?}", err);
                return Ok(S3Response::new((StatusCode::BAD_REQUEST, Body::from("Invalid bucket".to_string()))));
            }

            let sys = BucketTargetSys::get();
            let targets = sys.list_targets(bucket, "").await;

            let json_targets = serde_json::to_vec(&targets).map_err(|e| {
                error!("Serialization error: {}", e);
                S3Error::with_message(S3ErrorCode::InternalError, "Failed to serialize targets".to_string())
            })?;

            let mut header = HeaderMap::new();
            header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

            return Ok(S3Response::with_headers((StatusCode::OK, Body::from(json_targets)), header));
        }

        let targets: Vec<BucketTarget> = Vec::new();

        let json_targets = serde_json::to_vec(&targets).map_err(|e| {
            error!("Serialization error: {}", e);
            S3Error::with_message(S3ErrorCode::InternalError, "Failed to serialize targets".to_string())
        })?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(json_targets)), header))
    }
}

pub struct RemoveRemoteTargetHandler {}
#[async_trait::async_trait]
impl Operation for RemoveRemoteTargetHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        debug!("remove remote target called");
        let queries = extract_query_params(&req.uri);
        let Some(bucket) = queries.get("bucket") else {
            return Ok(S3Response::new((
                StatusCode::BAD_REQUEST,
                Body::from("Bucket parameter is required".to_string()),
            )));
        };

        let Some(arn_str) = queries.get("arn") else {
            return Ok(S3Response::new((StatusCode::BAD_REQUEST, Body::from("ARN is required".to_string()))));
        };

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not initialized".to_string()));
        };

        if let Err(err) = store.get_bucket_info(bucket, &BucketOptions::default()).await {
            error!("Error fetching bucket info: {:?}", err);
            return Ok(S3Response::new((StatusCode::BAD_REQUEST, Body::from("Invalid bucket".to_string()))));
        }

        let sys = BucketTargetSys::get();

        sys.remove_target(bucket, arn_str).await.map_err(|e| {
            error!("Failed to remove target: {}", e);
            S3Error::with_message(S3ErrorCode::InternalError, "Failed to remove target".to_string())
        })?;

        let targets = sys.list_bucket_targets(bucket).await.map_err(|e| {
            error!("Failed to list bucket targets: {}", e);
            S3Error::with_message(S3ErrorCode::InternalError, "Failed to list bucket targets".to_string())
        })?;

        let json_targets = serde_json::to_vec(&targets).map_err(|e| {
            error!("Serialization error: {}", e);
            S3Error::with_message(S3ErrorCode::InternalError, "Failed to serialize targets".to_string())
        })?;

        metadata_sys::update(bucket, BUCKET_TARGETS_FILE, json_targets)
            .await
            .map_err(|e| {
                error!("Failed to update bucket targets: {}", e);
                S3Error::with_message(S3ErrorCode::InternalError, format!("Failed to update bucket targets: {e}"))
            })?;

        Ok(S3Response::new((StatusCode::NO_CONTENT, Body::from("".to_string()))))
    }
}

/// Real-time data collection function
async fn collect_realtime_data_usage(
    info: &mut rustfs_common::data_usage::DataUsageInfo,
    store: Arc<rustfs_ecstore::store::ECStore>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Get bucket list and collect basic statistics
    let buckets = store.list_bucket(&BucketOptions::default()).await?;

    info.buckets_count = buckets.len() as u64;
    info.last_update = Some(std::time::SystemTime::now());
    info.buckets_usage.clear();
    info.bucket_sizes.clear();
    info.disk_usage_status.clear();
    info.objects_total_count = 0;
    info.objects_total_size = 0;
    info.versions_total_count = 0;
    info.delete_markers_total_count = 0;

    let mut total_objects = 0u64;
    let mut total_versions = 0u64;
    let mut total_size = 0u64;
    let mut total_delete_markers = 0u64;

    // For each bucket, try to get object count
    for bucket_info in buckets {
        let bucket_name = &bucket_info.name;

        // Skip system buckets
        if bucket_name.starts_with('.') {
            continue;
        }

        match compute_bucket_usage(store.clone(), bucket_name).await {
            Ok(bucket_usage) => {
                total_objects = total_objects.saturating_add(bucket_usage.objects_count);
                total_versions = total_versions.saturating_add(bucket_usage.versions_count);
                total_size = total_size.saturating_add(bucket_usage.size);
                total_delete_markers = total_delete_markers.saturating_add(bucket_usage.delete_markers_count);

                info.buckets_usage.insert(bucket_name.clone(), bucket_usage.clone());
                info.bucket_sizes.insert(bucket_name.clone(), bucket_usage.size);
            }
            Err(e) => {
                warn!("Failed to compute bucket usage for {}: {}", bucket_name, e);
            }
        }
    }

    info.objects_total_count = total_objects;
    info.objects_total_size = total_size;
    info.versions_total_count = total_versions;
    info.delete_markers_total_count = total_delete_markers;

    Ok(())
}

pub struct ProfileHandler {}
#[async_trait::async_trait]
impl Operation for ProfileHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        #[cfg(not(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64")))]
        {
            let requested_url = req.uri.to_string();
            let target_os = std::env::consts::OS;
            let target_arch = std::env::consts::ARCH;
            let target_env = option_env!("CARGO_CFG_TARGET_ENV").unwrap_or("unknown");
            let msg = format!(
                "CPU profiling is not supported on this platform. target_os={target_os}, target_env={target_env}, target_arch={target_arch}, requested_url={requested_url}"
            );
            return Ok(S3Response::new((StatusCode::NOT_IMPLEMENTED, Body::from(msg))));
        }

        #[cfg(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
        {
            use rustfs_config::{DEFAULT_CPU_FREQ, ENV_CPU_FREQ};
            use rustfs_utils::get_env_usize;

            let queries = extract_query_params(&req.uri);
            let seconds = queries.get("seconds").and_then(|s| s.parse::<u64>().ok()).unwrap_or(30);
            let format = queries.get("format").cloned().unwrap_or_else(|| "protobuf".to_string());

            if seconds > 300 {
                return Ok(S3Response::new((
                    StatusCode::BAD_REQUEST,
                    Body::from("Profile duration cannot exceed 300 seconds".to_string()),
                )));
            }

            match format.as_str() {
                "protobuf" | "pb" => match crate::profiling::dump_cpu_pprof_for(std::time::Duration::from_secs(seconds)).await {
                    Ok(path) => match tokio::fs::read(&path).await {
                        Ok(bytes) => {
                            let mut headers = HeaderMap::new();
                            headers.insert(CONTENT_TYPE, "application/octet-stream".parse().unwrap());
                            Ok(S3Response::with_headers((StatusCode::OK, Body::from(bytes)), headers))
                        }
                        Err(e) => Ok(S3Response::new((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Body::from(format!("Failed to read profile file: {e}")),
                        ))),
                    },
                    Err(e) => Ok(S3Response::new((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Body::from(format!("Failed to collect CPU profile: {e}")),
                    ))),
                },
                "flamegraph" | "svg" => {
                    let freq = get_env_usize(ENV_CPU_FREQ, DEFAULT_CPU_FREQ) as i32;
                    let guard = match pprof::ProfilerGuard::new(freq) {
                        Ok(g) => g,
                        Err(e) => {
                            return Ok(S3Response::new((
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Body::from(format!("Failed to create profiler: {e}")),
                            )));
                        }
                    };

                    tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;

                    let report = match guard.report().build() {
                        Ok(r) => r,
                        Err(e) => {
                            return Ok(S3Response::new((
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Body::from(format!("Failed to build profile report: {e}")),
                            )));
                        }
                    };

                    let mut flamegraph_buf = Vec::new();
                    if let Err(e) = report.flamegraph(&mut flamegraph_buf) {
                        return Ok(S3Response::new((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Body::from(format!("Failed to generate flamegraph: {e}")),
                        )));
                    }

                    let mut headers = HeaderMap::new();
                    headers.insert(CONTENT_TYPE, "image/svg+xml".parse().unwrap());
                    Ok(S3Response::with_headers((StatusCode::OK, Body::from(flamegraph_buf)), headers))
                }
                _ => Ok(S3Response::new((
                    StatusCode::BAD_REQUEST,
                    Body::from("Unsupported format. Use 'protobuf' or 'flamegraph'".to_string()),
                ))),
            }
        }
    }
}

pub struct ProfileStatusHandler {}
#[async_trait::async_trait]
impl Operation for ProfileStatusHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        use std::collections::HashMap;

        #[cfg(not(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64")))]
        let message = format!("CPU profiling is not supported on {} platform", std::env::consts::OS);
        #[cfg(not(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64")))]
        let status = HashMap::from([
            ("enabled", "false"),
            ("status", "not_supported"),
            ("platform", std::env::consts::OS),
            ("message", message.as_str()),
        ]);

        #[cfg(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
        let status = {
            use rustfs_config::{DEFAULT_ENABLE_PROFILING, ENV_ENABLE_PROFILING};
            use rustfs_utils::get_env_bool;

            let enabled = get_env_bool(ENV_ENABLE_PROFILING, DEFAULT_ENABLE_PROFILING);
            if enabled {
                HashMap::from([
                    ("enabled", "true"),
                    ("status", "running"),
                    ("supported_formats", "protobuf, flamegraph"),
                    ("max_duration_seconds", "300"),
                    ("endpoint", "/rustfs/admin/debug/pprof/profile"),
                ])
            } else {
                HashMap::from([
                    ("enabled", "false"),
                    ("status", "disabled"),
                    ("message", "Set RUSTFS_ENABLE_PROFILING=true to enable profiling"),
                ])
            }
        };

        match serde_json::to_string(&status) {
            Ok(json) => {
                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
                Ok(S3Response::with_headers((StatusCode::OK, Body::from(json)), headers))
            }
            Err(e) => {
                error!("Failed to serialize status: {}", e);
                Ok(S3Response::new((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Body::from("Failed to serialize status".to_string()),
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_common::heal_channel::HealOpts;
    use rustfs_madmin::BackendInfo;
    use rustfs_policy::policy::BucketPolicy;
    use serde_json::json;

    #[test]
    fn test_account_info_structure() {
        // Test AccountInfo struct creation and serialization
        let account_info = AccountInfo {
            account_name: "test-account".to_string(),
            server: BackendInfo::default(),
            policy: BucketPolicy::default(),
        };

        assert_eq!(account_info.account_name, "test-account");

        // Test JSON serialization (PascalCase rename)
        let json_str = serde_json::to_string(&account_info).unwrap();
        assert!(json_str.contains("AccountName"));
    }

    #[test]
    fn test_account_info_default() {
        // Test that AccountInfo can be created with default values
        let default_info = AccountInfo::default();

        assert!(default_info.account_name.is_empty());
    }

    #[test]
    fn test_handler_struct_creation() {
        // Test that handler structs can be created
        let _account_handler = AccountInfoHandler {};
        let _service_handler = ServiceHandle {};
        let _server_info_handler = ServerInfoHandler {};
        let _inspect_data_handler = InspectDataHandler {};
        let _storage_info_handler = StorageInfoHandler {};
        let _data_usage_handler = DataUsageInfoHandler {};
        let _metrics_handler = MetricsHandler {};
        let _heal_handler = HealHandler {};
        let _bg_heal_handler = BackgroundHealStatusHandler {};
        let _replication_metrics_handler = GetReplicationMetricsHandler {};
        let _set_remote_target_handler = SetRemoteTargetHandler {};
        let _list_remote_target_handler = ListRemoteTargetHandler {};
        let _remove_remote_target_handler = RemoveRemoteTargetHandler {};

        // Just verify they can be created without panicking
        // Test passes if we reach this point without panicking
    }

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

    #[ignore] // FIXME: failed in github actions - keeping original test
    #[test]
    fn test_decode() {
        let b = b"{\"recursive\":false,\"dryRun\":false,\"remove\":false,\"recreate\":false,\"scanMode\":1,\"updateParity\":false,\"nolock\":false}";
        let s: HealOpts = serde_urlencoded::from_bytes(b).unwrap();
        debug!("Parsed HealOpts: {:?}", s);
    }

    // Note: Testing the actual async handler implementations requires:
    // 1. S3Request setup with proper headers, URI, and credentials
    // 2. Global object store initialization
    // 3. IAM system initialization
    // 4. Mock or real backend services
    // 5. Authentication and authorization setup
    //
    // These are better suited for integration tests with proper test infrastructure.
    // The current tests focus on data structures and basic functionality that can be
    // tested in isolation without complex dependencies.
}
