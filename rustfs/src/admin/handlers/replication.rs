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

use crate::admin::auth::validate_admin_request;
use crate::admin::handlers::site_replication::site_replication_peer_deployment_id_for_endpoint;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::{
    AppContext, app_context_from_req, current_notification_system_for_context, current_replication_stats_handle_for_context,
    current_runtime_port, object_store_from_req,
};
use crate::admin::storage_api::bucket::metadata::BUCKET_TARGETS_FILE;
use crate::admin::storage_api::bucket::metadata_sys;
use crate::admin::storage_api::bucket::metadata_sys::get_replication_config;
use crate::admin::storage_api::bucket::replication::{BucketStats, ReplicationStatusType};
use crate::admin::storage_api::bucket::target::{BucketTarget, BucketTargetType, Credentials as TargetCredentials, LatencyStat};
use crate::admin::storage_api::bucket::target_sys::{BucketTargetError, BucketTargetSys};
use crate::admin::storage_api::contract::bucket::{BucketOperations, BucketOptions};
use crate::admin::storage_api::contract::list::ListOperations as _;
use crate::admin::storage_api::error::StorageError;
use crate::admin::storage_api::runtime::PeerRestClient;
use crate::admin::utils::read_compatible_admin_body;
use crate::auth::{check_key_valid, get_session_token};
use crate::error::ApiError;
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use crate::storage::storage_api::lock_bucket_targets_metadata;
use http::{HeaderMap, HeaderValue, Uri};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_credentials::Credentials;
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use tracing::{debug, error, info, warn};
use url::Host;

fn extract_query_params(uri: &Uri) -> HashMap<String, String> {
    let mut params = HashMap::new();

    if let Some(query) = uri.query() {
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            params.insert(key.into_owned(), value.into_owned());
        }
    }

    params
}

fn map_bucket_target_error(err: BucketTargetError) -> S3Error {
    match err {
        BucketTargetError::BucketRemoteTargetNotFound { .. }
        | BucketTargetError::BucketRemoteArnTypeInvalid { .. }
        | BucketTargetError::BucketRemoteAlreadyExists { .. }
        | BucketTargetError::BucketRemoteArnInvalid { .. }
        | BucketTargetError::RemoteTargetConnectionErr { .. }
        | BucketTargetError::BucketReplicationSourceNotVersioned { .. }
        | BucketTargetError::BucketRemoteTargetNotVersioned { .. }
        | BucketTargetError::BucketRemoteRemoveDisallowed { .. } => {
            S3Error::with_message(S3ErrorCode::InvalidRequest, err.to_string())
        }
        BucketTargetError::Io(io_err) => S3Error::with_message(S3ErrorCode::InternalError, io_err.to_string()),
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteTargetCredentialsRequest {
    #[serde(rename = "accessKey")]
    access_key: String,
    #[serde(rename = "secretKey")]
    secret_key: String,
    session_token: Option<String>,
    expiration: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<RemoteTargetCredentialsRequest> for TargetCredentials {
    fn from(value: RemoteTargetCredentialsRequest) -> Self {
        Self {
            access_key: value.access_key,
            secret_key: value.secret_key,
            session_token: value.session_token,
            expiration: value.expiration,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteTargetRequest {
    #[serde(rename = "sourcebucket", default)]
    source_bucket: String,
    endpoint: String,
    credentials: RemoteTargetCredentialsRequest,
    #[serde(rename = "targetbucket")]
    target_bucket: String,
    #[serde(default)]
    secure: bool,
    #[serde(default)]
    path: String,
    #[serde(default)]
    api: String,
    #[serde(default)]
    arn: String,
    #[serde(rename = "type")]
    target_type: BucketTargetType,
    #[serde(default)]
    region: String,
    #[serde(alias = "bandwidth", default)]
    bandwidth_limit: i64,
    #[serde(rename = "replicationSync", default)]
    replication_sync: bool,
    #[serde(default)]
    storage_class: String,
    #[serde(rename = "skipTlsVerify", default)]
    skip_tls_verify: bool,
    #[serde(rename = "caCertPem", default)]
    ca_cert_pem: String,
    #[serde(rename = "healthCheckDuration", default)]
    health_check_duration: u64,
    #[serde(rename = "disableProxy", default)]
    disable_proxy: bool,
    #[serde(rename = "resetBeforeDate", with = "time::serde::rfc3339::option", default)]
    reset_before_date: Option<OffsetDateTime>,
    #[serde(default)]
    reset_id: String,
    #[serde(rename = "totalDowntime", default)]
    total_downtime: u64,
    #[serde(rename = "lastOnline", with = "time::serde::rfc3339::option", default)]
    last_online: Option<OffsetDateTime>,
    #[serde(rename = "isOnline", default)]
    online: bool,
    #[serde(default)]
    latency: LatencyStat,
    #[serde(default)]
    deployment_id: String,
    #[serde(default)]
    edge: bool,
    #[serde(rename = "edgeSyncBeforeExpiry", default)]
    edge_sync_before_expiry: bool,
    #[serde(rename = "offlineCount", default)]
    offline_count: u64,
}

impl RemoteTargetRequest {
    fn into_bucket_target(self) -> S3Result<BucketTarget> {
        if self.endpoint.trim().is_empty() {
            return Err(s3_error!(InvalidRequest, "endpoint is required"));
        }

        if self.target_bucket.trim().is_empty() {
            return Err(s3_error!(InvalidRequest, "targetbucket is required"));
        }

        if !self.target_type.is_valid() {
            return Err(s3_error!(InvalidRequest, "type is invalid"));
        }

        if self.credentials.access_key.trim().is_empty() {
            return Err(s3_error!(InvalidRequest, "credentials.accessKey is required"));
        }

        if self.credentials.secret_key.trim().is_empty() {
            return Err(s3_error!(InvalidRequest, "credentials.secretKey is required"));
        }

        Ok(BucketTarget {
            source_bucket: self.source_bucket,
            endpoint: self.endpoint,
            credentials: Some(self.credentials.into()),
            target_bucket: self.target_bucket,
            secure: self.secure,
            path: self.path,
            api: self.api,
            arn: self.arn,
            target_type: self.target_type,
            region: self.region,
            bandwidth_limit: self.bandwidth_limit,
            replication_sync: self.replication_sync,
            storage_class: self.storage_class,
            skip_tls_verify: self.skip_tls_verify,
            ca_cert_pem: self.ca_cert_pem,
            health_check_duration: Duration::from_secs(self.health_check_duration),
            disable_proxy: self.disable_proxy,
            reset_before_date: self.reset_before_date,
            reset_id: self.reset_id,
            total_downtime: Duration::from_secs(self.total_downtime),
            last_online: self.last_online,
            online: self.online,
            latency: self.latency,
            deployment_id: self.deployment_id,
            edge: self.edge,
            edge_sync_before_expiry: self.edge_sync_before_expiry,
            offline_count: self.offline_count,
        })
    }
}

fn validate_remote_target_tls_settings(remote_target: &BucketTarget) -> S3Result<()> {
    let has_custom_ca = !remote_target.ca_cert_pem.trim().is_empty();

    if !remote_target.secure && remote_target.skip_tls_verify {
        return Err(s3_error!(InvalidRequest, "skipTlsVerify requires an HTTPS remote target"));
    }

    if !remote_target.secure && has_custom_ca {
        return Err(s3_error!(InvalidRequest, "caCertPem requires an HTTPS remote target"));
    }

    if remote_target.skip_tls_verify && has_custom_ca {
        return Err(s3_error!(InvalidRequest, "skipTlsVerify and caCertPem cannot be enabled together"));
    }

    Ok(())
}

pub fn register_replication_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/list-remote-targets").as_str(),
        AdminOperation(&ListRemoteTargetHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/replicationmetrics").as_str(),
        AdminOperation(&GetReplicationMetricsHandler {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/set-remote-target").as_str(),
        AdminOperation(&SetRemoteTargetHandler {}),
    )?;

    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/remove-remote-target").as_str(),
        AdminOperation(&RemoveRemoteTargetHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/replication/diff").as_str(),
        AdminOperation(&ReplicationDiffHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/replication/mrf").as_str(),
        AdminOperation(&ReplicationMrfHandler {}),
    )?;

    Ok(())
}

async fn validate_replication_admin_request(req: &S3Request<Body>, action: AdminAction) -> S3Result<Credentials> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(InvalidRequest, "get cred failed"));
    };

    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
    validate_admin_request(&req.headers, &cred, owner, false, vec![Action::AdminAction(action)], remote_addr).await?;

    Ok(cred)
}

#[allow(dead_code)]
fn is_local_host(_host: String) -> bool {
    false
}

pub(crate) async fn cluster_replication_stats(bucket: &str, context: Option<Arc<AppContext>>) -> BucketStats {
    let Some(stats) = current_replication_stats_handle_for_context(context.clone()) else {
        return BucketStats::default();
    };

    let local = stats.get_latest_replication_stats(bucket).await;
    let Some(notification_system) = current_notification_system_for_context(context.as_deref()) else {
        return local;
    };

    let (peers, expected_node_count) = unique_replication_peers(&notification_system.peer_clients);
    let peer_results = futures_util::future::join_all(peers.into_iter().map(|peer| peer.get_bucket_stats(bucket))).await;
    let mut snapshots = Vec::with_capacity(peer_results.len().saturating_add(1));
    snapshots.push(local);
    snapshots.extend(peer_results.into_iter().filter_map(Result::ok));

    stats
        .aggregate_bucket_replication_stats(bucket, snapshots, expected_node_count)
        .await
}

fn unique_replication_peers(peer_clients: &[Option<PeerRestClient>]) -> (Vec<&PeerRestClient>, u32) {
    let mut seen_grid_hosts = HashSet::new();
    let peers: Vec<_> = peer_clients
        .iter()
        .filter_map(|peer| peer.as_ref())
        .filter(|peer| seen_grid_hosts.insert(peer.grid_host.clone()))
        .collect();
    let unavailable_slots = peer_clients.iter().filter(|peer| peer.is_none()).count();
    let expected_node_count = u32::try_from(peers.len().saturating_add(unavailable_slots).saturating_add(1)).unwrap_or(u32::MAX);
    (peers, expected_node_count)
}

//awscurl --service s3 --region us-east-1 --access_key rustfsadmin --secret_key rustfsadmin "http://:9000/rustfs/admin/v3/replicationmetrics?bucket=1"
pub struct GetReplicationMetricsHandler {}

#[async_trait::async_trait]
impl Operation for GetReplicationMetricsHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_replication_admin_request(&req, AdminAction::GetReplicationMetricsAction).await?;

        let queries = extract_query_params(&req.uri);

        let Some(bucket) = queries.get("bucket") else {
            return Err(s3_error!(InvalidRequest, "bucket is required"));
        };

        if bucket.is_empty() {
            return Err(s3_error!(InvalidRequest, "bucket is required"));
        }

        let Some(store) = object_store_from_req(&req) else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        if let Err(err) = get_replication_config(bucket).await {
            if err == StorageError::ConfigNotFound {
                info!("replication configuration not found for bucket '{}'", bucket);
                return Err(S3Error::with_message(
                    S3ErrorCode::ReplicationConfigurationNotFoundError,
                    "replication not found".to_string(),
                ));
            }
            error!("get_replication_config unexpected error: {:?}", err);
            return Err(ApiError::from(err).into());
        }

        let bucket_stats = cluster_replication_stats(bucket, app_context_from_req(&req)).await;

        let data = serde_json::to_vec(&bucket_stats.replication_stats)
            .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "serialize failed"))?;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
    }
}

pub struct SetRemoteTargetHandler {}

#[async_trait::async_trait]
impl Operation for SetRemoteTargetHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_replication_admin_request(&req, AdminAction::SetBucketTargetAction).await?;

        let queries = extract_query_params(&req.uri);

        let Some(bucket) = queries.get("bucket") else {
            return Err(s3_error!(InvalidRequest, "bucket is required"));
        };

        let update = queries.get("update").is_some_and(|v| v == "true");

        warn!("set remote target, bucket: {}, update: {}", bucket, update);

        if bucket.is_empty() {
            return Err(s3_error!(InvalidRequest, "bucket is required"));
        }

        let Some(store) = object_store_from_req(&req) else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let body =
            match read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, req.uri.path(), &cred.secret_key).await {
                Ok(body) => body,
                Err(e) => {
                    warn!("get body failed, e: {:?}", e);
                    return Err(e);
                }
            };

        let mut remote_target = serde_json::from_slice::<RemoteTargetRequest>(&body)
            .map_err(|e| {
                error!("Failed to parse remote target request body: {}", e);
                S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid remote target request: {e}"))
            })?
            .into_bucket_target()?;
        validate_remote_target_tls_settings(&remote_target)?;

        let Ok(target_url) = remote_target.url() else {
            return Err(s3_error!(InvalidRequest, "invalid target url"));
        };

        let same_target = rustfs_utils::net::is_local_host(
            target_url.host().unwrap_or(Host::Domain("localhost")),
            target_url.port().unwrap_or(80),
            current_runtime_port(),
        )
        .unwrap_or_default();

        if same_target && bucket == &remote_target.target_bucket {
            return Err(S3Error::with_message(S3ErrorCode::IncorrectEndpoint, "Same target".to_string()));
        }

        remote_target.source_bucket = bucket.clone();
        let site_endpoint = if remote_target.endpoint.starts_with("http://") || remote_target.endpoint.starts_with("https://") {
            remote_target.endpoint.clone()
        } else if remote_target.secure {
            format!("https://{}", remote_target.endpoint)
        } else {
            format!("http://{}", remote_target.endpoint)
        };
        if let Some(deployment_id) = site_replication_peer_deployment_id_for_endpoint(&site_endpoint).await {
            remote_target.deployment_id = deployment_id;
        }

        let bucket_target_sys = BucketTargetSys::get();

        if !update {
            let (arn, exist) = bucket_target_sys
                .get_remote_arn(bucket, Some(&remote_target), remote_target.deployment_id.as_str())
                .await;
            remote_target.arn = arn.clone();
            if exist && !arn.is_empty() {
                let arn_str = serde_json::to_string(&arn).unwrap_or_default();

                warn!("return exists, arn: {}", arn_str);
                // MinIO-compatible clients encrypt the request payload for this endpoint,
                // but they parse the success response directly as plain JSON string ARN.
                return Ok(S3Response::new((StatusCode::OK, Body::from(arn_str))));
            }
        }

        if remote_target.arn.is_empty() {
            return Err(S3Error::with_message(S3ErrorCode::InvalidRequest, "ARN is empty".to_string()));
        }
        let _targets_guard = lock_bucket_targets_metadata(bucket).await;

        if update {
            let Some(mut target) = bucket_target_sys
                .get_remote_bucket_target_by_arn(bucket, &remote_target.arn)
                .await
            else {
                return Err(S3Error::with_message(S3ErrorCode::InvalidRequest, "Target not found".to_string()));
            };

            target.credentials = remote_target.credentials;
            target.endpoint = remote_target.endpoint;
            target.secure = remote_target.secure;
            target.target_bucket = remote_target.target_bucket;

            target.path = remote_target.path;
            target.replication_sync = remote_target.replication_sync;
            target.bandwidth_limit = remote_target.bandwidth_limit;
            target.skip_tls_verify = remote_target.skip_tls_verify;
            target.ca_cert_pem = remote_target.ca_cert_pem;
            target.health_check_duration = remote_target.health_check_duration;

            warn!(
                bucket = %bucket,
                arn = %target.arn,
                endpoint = %target.endpoint,
                secure = target.secure,
                skip_tls_verify = target.skip_tls_verify,
                has_custom_ca = !target.ca_cert_pem.trim().is_empty(),
                "update remote target"
            );
            remote_target = target;
        }

        let arn = remote_target.arn.clone();

        let targets = bucket_target_sys
            .set_target(bucket, &remote_target, update)
            .await
            .map_err(map_bucket_target_error)?;
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
        bucket_target_sys.update_all_targets(bucket, Some(&targets)).await;

        let arn_str = serde_json::to_string(&arn).unwrap_or_default();

        // MinIO-compatible clients encrypt the request payload for this endpoint,
        // but they parse the success response directly as plain JSON string ARN.
        Ok(S3Response::new((StatusCode::OK, Body::from(arn_str))))
    }
}

pub struct ListRemoteTargetHandler {}

#[async_trait::async_trait]
impl Operation for ListRemoteTargetHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_replication_admin_request(&req, AdminAction::GetBucketTargetAction).await?;

        let queries = extract_query_params(&req.uri);

        if let Some(bucket) = queries.get("bucket") {
            if bucket.is_empty() {
                error!("bucket parameter is empty");
                return Err(s3_error!(InvalidRequest, "bucket is required"));
            }

            let Some(store) = object_store_from_req(&req) else {
                return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not initialized".to_string()));
            };

            store
                .get_bucket_info(bucket, &BucketOptions::default())
                .await
                .map_err(ApiError::from)?;

            let sys = BucketTargetSys::get();
            let targets = sys.list_targets(bucket, "").await;

            let targets: Vec<_> = targets.iter().map(|target| target.redacted_credentials()).collect();
            let json_targets = serde_json::to_vec(&targets).map_err(|e| {
                error!("Serialization error: {}", e);
                S3Error::with_message(S3ErrorCode::InternalError, "Failed to serialize targets".to_string())
            })?;

            let mut header = HeaderMap::new();
            header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

            return Ok(S3Response::with_headers((StatusCode::OK, Body::from(json_targets)), header));
        }

        let targets: Vec<BucketTarget> = Vec::new();

        let json_targets = serde_json::to_vec(&targets).map_err(|e| {
            error!("Serialization error: {}", e);
            S3Error::with_message(S3ErrorCode::InternalError, "Failed to serialize targets".to_string())
        })?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(json_targets)), header))
    }
}

pub struct RemoveRemoteTargetHandler {}

#[async_trait::async_trait]
impl Operation for RemoveRemoteTargetHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_replication_admin_request(&req, AdminAction::SetBucketTargetAction).await?;

        debug!("remove remote target called");
        let queries = extract_query_params(&req.uri);
        let Some(bucket) = queries.get("bucket") else {
            return Err(s3_error!(InvalidRequest, "bucket is required"));
        };
        if bucket.is_empty() {
            return Err(s3_error!(InvalidRequest, "bucket is required"));
        }

        let Some(arn_str) = queries.get("arn") else {
            return Err(s3_error!(InvalidRequest, "arn is required"));
        };
        if arn_str.is_empty() {
            return Err(s3_error!(InvalidRequest, "arn is required"));
        };

        let Some(store) = object_store_from_req(&req) else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not initialized".to_string()));
        };

        store
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let sys = BucketTargetSys::get();
        let _targets_guard = lock_bucket_targets_metadata(bucket).await;

        let targets = sys.remove_target(bucket, arn_str).await.map_err(map_bucket_target_error)?;

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
        sys.update_all_targets(bucket, Some(&targets)).await;

        Ok(S3Response::new((StatusCode::NO_CONTENT, Body::from("".to_string()))))
    }
}

/// Upper bound on the number of object versions scanned per `POST
/// /v3/replication/diff` request. RustFS has no persisted per-object
/// replication-diff index, so the diff is computed by scanning object versions
/// on demand. Cap the work so a single admin call cannot walk an arbitrarily
/// large bucket. When the scan is truncated, `is_truncated` is set on the
/// response so clients know the diff is partial.
const REPLICATION_DIFF_MAX_SCAN: usize = 10_000;

/// Number of object versions requested per `list_object_versions` page while
/// computing a replication diff.
const REPLICATION_DIFF_PAGE_SIZE: i32 = 1_000;

/// A single object version whose replication is not yet complete, reported by
/// `POST /v3/replication/diff`. Field names mirror MinIO's `madmin.DiffInfo`
/// so MinIO-compatible admin clients can parse the response.
#[derive(Debug, Serialize)]
struct ReplicationDiffEntry {
    #[serde(rename = "Object")]
    object: String,
    #[serde(rename = "VersionID", skip_serializing_if = "Option::is_none")]
    version_id: Option<String>,
    #[serde(rename = "Size")]
    size: i64,
    #[serde(rename = "IsDeleteMarker")]
    is_delete_marker: bool,
    #[serde(rename = "ReplicationStatus")]
    replication_status: String,
    #[serde(rename = "LastModified", skip_serializing_if = "Option::is_none")]
    last_modified: Option<String>,
}

/// Response body for `POST /v3/replication/diff`.
///
/// `entries` lists object versions with a `PENDING` or `FAILED` replication
/// status. `is_truncated` indicates the on-demand scan hit
/// [`REPLICATION_DIFF_MAX_SCAN`] before reaching the end of the bucket, so the
/// diff is partial and should be re-run with a narrower prefix.
#[derive(Debug, Serialize)]
struct ReplicationDiffResponse {
    #[serde(rename = "Entries")]
    entries: Vec<ReplicationDiffEntry>,
    #[serde(rename = "IsTruncated")]
    is_truncated: bool,
    #[serde(rename = "ScannedVersions")]
    scanned_versions: usize,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplicationDiffRequest {
    #[serde(default)]
    prefix: String,
}

/// `POST /v3/replication/diff`
///
/// Computes, on demand, the set of object versions in a bucket whose replication
/// is still `PENDING` or has `FAILED`. RustFS stores the replication status on
/// each object version (`x-amz-replication-status`) but has no pre-built diff
/// index, so this handler scans object versions (bounded by
/// [`REPLICATION_DIFF_MAX_SCAN`]) and returns the not-yet-replicated versions.
///
/// The bucket must exist and have a replication configuration, matching MinIO's
/// behavior of returning `ReplicationConfigurationNotFoundError` otherwise.
pub struct ReplicationDiffHandler {}

#[async_trait::async_trait]
impl Operation for ReplicationDiffHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_replication_admin_request(&req, AdminAction::ReplicationDiff).await?;

        let queries = extract_query_params(&req.uri);
        let Some(bucket) = queries.get("bucket").filter(|b| !b.is_empty()).cloned() else {
            return Err(s3_error!(InvalidRequest, "bucket is required"));
        };

        let Some(store) = object_store_from_req(&req) else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        // A replication diff is only meaningful for a bucket that is configured
        // for replication; mirror MinIO's not-found semantics otherwise.
        if let Err(err) = get_replication_config(&bucket).await {
            if err == StorageError::ConfigNotFound {
                return Err(S3Error::with_message(
                    S3ErrorCode::ReplicationConfigurationNotFoundError,
                    "replication configuration not found".to_string(),
                ));
            }
            return Err(ApiError::from(err).into());
        }

        // Optional prefix can be supplied either as a query parameter (MinIO
        // clients) or, for RustFS clients, as a small JSON body.
        let mut prefix = queries.get("prefix").cloned().unwrap_or_default();
        let body = read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, req.uri.path(), &cred.secret_key)
            .await
            .unwrap_or_default();
        if prefix.is_empty() && !body.trim_ascii().is_empty() {
            match serde_json::from_slice::<ReplicationDiffRequest>(&body) {
                Ok(parsed) => prefix = parsed.prefix,
                Err(e) => return Err(s3_error!(InvalidRequest, "invalid replication diff request body: {e}")),
            }
        }

        let mut entries: Vec<ReplicationDiffEntry> = Vec::new();
        let mut scanned_versions: usize = 0;
        let mut marker: Option<String> = None;
        let mut version_marker: Option<String> = None;
        let mut is_truncated = false;

        'scan: loop {
            let listing = store
                .clone()
                .list_object_versions(&bucket, &prefix, marker.clone(), version_marker.clone(), None, REPLICATION_DIFF_PAGE_SIZE)
                .await
                .map_err(ApiError::from)?;

            for object in &listing.objects {
                scanned_versions += 1;

                if matches!(object.replication_status, ReplicationStatusType::Pending | ReplicationStatusType::Failed) {
                    entries.push(ReplicationDiffEntry {
                        object: object.name.clone(),
                        version_id: object.version_id.map(|v| v.to_string()),
                        size: object.size,
                        is_delete_marker: object.delete_marker,
                        replication_status: object.replication_status.as_str().to_string(),
                        last_modified: object
                            .mod_time
                            .and_then(|t| t.format(&time::format_description::well_known::Rfc3339).ok()),
                    });
                }

                if scanned_versions >= REPLICATION_DIFF_MAX_SCAN {
                    // We stopped early; the diff is partial.
                    is_truncated =
                        listing.is_truncated || listing.next_marker.is_some() || listing.next_version_idmarker.is_some();
                    break 'scan;
                }
            }

            if !listing.is_truncated {
                break;
            }
            marker = listing.next_marker;
            version_marker = listing.next_version_idmarker;
            if marker.is_none() && version_marker.is_none() {
                break;
            }
        }

        debug!(
            bucket = %bucket,
            prefix = %prefix,
            scanned = scanned_versions,
            pending_or_failed = entries.len(),
            truncated = is_truncated,
            "computed replication diff"
        );

        let response = ReplicationDiffResponse {
            entries,
            is_truncated,
            scanned_versions,
        };
        let data = serde_json::to_vec(&response)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize failed: {e}")))?;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
    }
}

/// Failed-replication totals for one remote target (ARN), summarised from the
/// runtime replication statistics.
#[derive(Debug, Serialize)]
struct MrfTargetBacklog {
    #[serde(rename = "ARN")]
    arn: String,
    #[serde(rename = "FailedCount")]
    failed_count: i64,
    #[serde(rename = "FailedSize")]
    failed_size: i64,
    #[serde(rename = "ObservationScope")]
    observation_scope: &'static str,
}

/// Response body for `GET /v3/replication/mrf`.
///
/// Runtime failed/queued totals and the durable MRF recovery backlog are kept
/// separate because persisted MRF entries do not contain a target ARN.
#[derive(Debug, Serialize)]
struct MrfResponse {
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Targets")]
    targets: Vec<MrfTargetBacklog>,
    #[serde(rename = "TotalFailedCount")]
    total_failed_count: i64,
    #[serde(rename = "TotalFailedSize")]
    total_failed_size: i64,
    #[serde(rename = "QueuedCount")]
    queued_count: i64,
    #[serde(rename = "QueuedSize")]
    queued_size: i64,
    #[serde(rename = "PerObjectEntriesAvailable")]
    per_object_entries_available: bool,
    #[serde(rename = "RuntimeStatsAvailable")]
    runtime_stats_available: bool,
    #[serde(rename = "ClusterComplete")]
    cluster_complete: bool,
    #[serde(rename = "ObservedNodeCount")]
    observed_node_count: u32,
    #[serde(rename = "ExpectedNodeCount")]
    expected_node_count: u32,
    #[serde(rename = "DurableBacklogAvailable")]
    durable_backlog_available: bool,
    #[serde(rename = "DurableCount")]
    durable_count: i64,
    #[serde(rename = "DurableSize")]
    durable_size: i64,
    #[serde(rename = "PerTargetDurableEntriesAvailable")]
    per_target_durable_entries_available: bool,
}

fn build_mrf_response(
    bucket: String,
    bucket_stats: &BucketStats,
    durable: crate::admin::storage_api::replication::DurableMrfBacklog,
) -> MrfResponse {
    let observation_scope = if bucket_stats.replication_stats.cluster_complete {
        "cluster_aggregated"
    } else {
        "partial_cluster"
    };
    let mut targets: Vec<MrfTargetBacklog> = Vec::with_capacity(bucket_stats.replication_stats.stats.len());
    let mut total_failed_count: i64 = 0;
    let mut total_failed_size: i64 = 0;
    for (arn, stat) in &bucket_stats.replication_stats.stats {
        total_failed_count = total_failed_count.saturating_add(stat.failed.count);
        total_failed_size = total_failed_size.saturating_add(stat.failed.size);
        targets.push(MrfTargetBacklog {
            arn: arn.clone(),
            failed_count: stat.failed.count,
            failed_size: stat.failed.size,
            observation_scope,
        });
    }
    targets.sort_by(|a, b| a.arn.cmp(&b.arn));

    let queued = &bucket_stats.replication_stats.q_stat.curr;
    let (durable_count, durable_size) = if durable.available {
        durable
            .entries
            .iter()
            .filter(|entry| entry.bucket == bucket)
            .fold((0i64, 0i64), |(count, size), entry| {
                (count.saturating_add(1), size.saturating_add(entry.size))
            })
    } else {
        (0, 0)
    };

    MrfResponse {
        bucket,
        targets,
        total_failed_count,
        total_failed_size,
        queued_count: queued.count,
        queued_size: queued.bytes,
        per_object_entries_available: false,
        runtime_stats_available: bucket_stats.replication_stats.provider_available,
        cluster_complete: bucket_stats.replication_stats.cluster_complete,
        observed_node_count: bucket_stats.replication_stats.observed_node_count,
        expected_node_count: bucket_stats.replication_stats.expected_node_count,
        durable_backlog_available: durable.available,
        durable_count,
        durable_size,
        per_target_durable_entries_available: false,
    }
}

/// `GET /v3/replication/mrf`
///
/// Reports the failed-replication backlog (MinIO's MRF concept) for a bucket.
///
/// Compatibility note: MinIO returns a stream of individual MRF entries. RustFS
/// deliberately returns aggregate runtime and durable counters instead.
/// `PerObjectEntriesAvailable` and `PerTargetDurableEntriesAvailable` remain
/// false until an enumerable API and target-bearing durable format exist.
pub struct ReplicationMrfHandler {}

#[async_trait::async_trait]
impl Operation for ReplicationMrfHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_replication_admin_request(&req, AdminAction::GetReplicationMetricsAction).await?;

        let queries = extract_query_params(&req.uri);
        let Some(bucket) = queries.get("bucket").filter(|b| !b.is_empty()).cloned() else {
            return Err(s3_error!(InvalidRequest, "bucket is required"));
        };

        let Some(store) = object_store_from_req(&req) else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        if let Err(err) = get_replication_config(&bucket).await {
            if err == StorageError::ConfigNotFound {
                return Err(S3Error::with_message(
                    S3ErrorCode::ReplicationConfigurationNotFoundError,
                    "replication configuration not found".to_string(),
                ));
            }
            return Err(ApiError::from(err).into());
        }

        let durable = crate::admin::storage_api::replication::read_durable_mrf_backlog(store).await;
        let bucket_stats = cluster_replication_stats(&bucket, app_context_from_req(&req)).await;
        let response = build_mrf_response(bucket, &bucket_stats, durable);

        let data = serde_json::to_vec(&response)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize failed: {e}")))?;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        RemoteTargetRequest, build_mrf_response, extract_query_params, unique_replication_peers,
        validate_remote_target_tls_settings,
    };
    use crate::admin::storage_api::bucket::target::BucketTarget;
    use crate::admin::storage_api::replication::{BucketStats, DurableMrfBacklog, MrfOpKind, MrfReplicateEntry};
    use http::Uri;

    fn valid_remote_target_request() -> serde_json::Value {
        serde_json::json!({
            "endpoint": "192.168.1.10:9000",
            "credentials": {
                "accessKey": "access",
                "secretKey": "secret"
            },
            "targetbucket": "target",
            "secure": true,
            "type": "replication"
        })
    }

    #[test]
    fn cluster_peer_plan_deduplicates_nodes_and_counts_unavailable_slots() {
        let peer = crate::admin::storage_api::runtime::PeerRestClient::new(
            rustfs_utils::XHost::try_from("127.0.0.1:9000".to_string()).expect("peer host should parse"),
            "node-a.example.com:9001".to_string(),
        );
        let slots = vec![Some(peer.clone()), Some(peer), None];

        let (peers, expected_node_count) = unique_replication_peers(&slots);

        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].grid_host, "node-a.example.com:9001");
        assert_eq!(expected_node_count, 3);
    }

    #[test]
    fn mrf_response_keeps_runtime_and_durable_truth_separate() {
        let mut stats = BucketStats::default();
        stats.replication_stats.provider_available = true;
        stats.replication_stats.cluster_complete = false;
        stats.replication_stats.observed_node_count = 2;
        stats.replication_stats.expected_node_count = 3;
        let target = stats.replication_stats.stats.entry("arn-a".to_string()).or_default();
        target.failed.count = 3;
        target.failed.size = 900;
        stats
            .replication_stats
            .q_stat
            .curr
            .now_count
            .store(4, std::sync::atomic::Ordering::Relaxed);
        stats
            .replication_stats
            .q_stat
            .curr
            .now_bytes
            .store(1200, std::sync::atomic::Ordering::Relaxed);
        stats.replication_stats.q_stat = stats.replication_stats.q_stat.snapshot();
        let durable = DurableMrfBacklog {
            available: true,
            entries: vec![
                MrfReplicateEntry {
                    bucket: "bucket-a".to_string(),
                    object: "object-a".to_string(),
                    version_id: None,
                    retry_count: 0,
                    size: 250,
                    op: MrfOpKind::Object,
                    delete_marker_version_id: None,
                    delete_marker: false,
                    delete_marker_mtime: None,
                },
                MrfReplicateEntry {
                    bucket: "other-bucket".to_string(),
                    object: "object-b".to_string(),
                    version_id: None,
                    retry_count: 0,
                    size: 999,
                    op: MrfOpKind::Object,
                    delete_marker_version_id: None,
                    delete_marker: false,
                    delete_marker_mtime: None,
                },
            ],
        };

        let response = build_mrf_response("bucket-a".to_string(), &stats, durable);
        let json = serde_json::to_value(response).expect("MRF response should serialize");

        assert_eq!(json["TotalFailedCount"], 3);
        assert_eq!(json["TotalFailedSize"], 900);
        assert_eq!(json["QueuedCount"], 4);
        assert_eq!(json["QueuedSize"], 1200);
        assert_eq!(json["DurableCount"], 1);
        assert_eq!(json["DurableSize"], 250);
        assert_eq!(json["RuntimeStatsAvailable"], true);
        assert_eq!(json["ClusterComplete"], false);
        assert_eq!(json["Targets"][0]["ObservationScope"], "partial_cluster");
        assert_eq!(json["PerObjectEntriesAvailable"], false);
        assert_eq!(json["PerTargetDurableEntriesAvailable"], false);
    }

    #[test]
    fn mrf_response_distinguishes_unavailable_sources_from_valid_zero() {
        let unavailable = build_mrf_response("bucket-a".to_string(), &BucketStats::default(), DurableMrfBacklog::default());
        let unavailable_json = serde_json::to_value(unavailable).expect("unavailable response should serialize");
        assert_eq!(unavailable_json["RuntimeStatsAvailable"], false);
        assert_eq!(unavailable_json["DurableBacklogAvailable"], false);

        let mut valid_empty_stats = BucketStats::default();
        valid_empty_stats.replication_stats.provider_available = true;
        valid_empty_stats.replication_stats.cluster_complete = true;
        valid_empty_stats.replication_stats.observed_node_count = 1;
        valid_empty_stats.replication_stats.expected_node_count = 1;
        let valid_empty = build_mrf_response(
            "bucket-a".to_string(),
            &valid_empty_stats,
            DurableMrfBacklog {
                available: true,
                entries: Vec::new(),
            },
        );
        let valid_empty_json = serde_json::to_value(valid_empty).expect("valid empty response should serialize");
        assert_eq!(valid_empty_json["RuntimeStatsAvailable"], true);
        assert_eq!(valid_empty_json["DurableBacklogAvailable"], true);
        assert_eq!(valid_empty_json["TotalFailedCount"], 0);
        assert_eq!(valid_empty_json["DurableCount"], 0);
    }

    #[test]
    fn test_extract_query_params_decodes_percent_encoded_values() {
        let uri: Uri = "/rustfs/admin/v3/list-remote-targets?bucket=foo%2Fbar&flag=a+b"
            .parse()
            .expect("uri should parse");
        let params = extract_query_params(&uri);

        assert_eq!(params.get("bucket"), Some(&"foo/bar".to_string()));
        assert_eq!(params.get("flag"), Some(&"a b".to_string()));
    }

    #[test]
    fn validate_remote_target_tls_settings_rejects_insecure_tls_for_http_targets() {
        let err = validate_remote_target_tls_settings(&BucketTarget {
            secure: false,
            skip_tls_verify: true,
            ..Default::default()
        })
        .expect_err("HTTP targets must reject skipTlsVerify");

        assert!(err.to_string().contains("skipTlsVerify requires an HTTPS remote target"));
    }

    #[test]
    fn validate_remote_target_tls_settings_rejects_custom_ca_for_http_targets() {
        let err = validate_remote_target_tls_settings(&BucketTarget {
            secure: false,
            ca_cert_pem: "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n".to_string(),
            ..Default::default()
        })
        .expect_err("HTTP targets must reject custom CA PEM");

        assert!(err.to_string().contains("caCertPem requires an HTTPS remote target"));
    }

    #[test]
    fn validate_remote_target_tls_settings_rejects_insecure_and_custom_ca_combination() {
        let err = validate_remote_target_tls_settings(&BucketTarget {
            secure: true,
            skip_tls_verify: true,
            ca_cert_pem: "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n".to_string(),
            ..Default::default()
        })
        .expect_err("custom CA and insecure TLS must be mutually exclusive");

        assert!(
            err.to_string()
                .contains("skipTlsVerify and caCertPem cannot be enabled together")
        );
    }

    #[test]
    fn validate_remote_target_tls_settings_allows_https_insecure_without_custom_ca() {
        validate_remote_target_tls_settings(&BucketTarget {
            secure: true,
            skip_tls_verify: true,
            ..Default::default()
        })
        .expect("HTTPS targets should allow skipTlsVerify when no custom CA is configured");
    }

    #[test]
    fn remote_target_request_rejects_unknown_fields() {
        let mut request = valid_remote_target_request();
        request["unexpected"] = serde_json::json!(true);

        let err = serde_json::from_value::<RemoteTargetRequest>(request)
            .expect_err("remote target request should reject unknown fields");

        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn remote_target_request_rejects_missing_credentials() {
        let mut request = valid_remote_target_request();
        request
            .as_object_mut()
            .expect("request should be an object")
            .remove("credentials");

        let err =
            serde_json::from_value::<RemoteTargetRequest>(request).expect_err("remote target request should require credentials");

        assert!(err.to_string().contains("missing field"));
    }

    #[test]
    fn remote_target_request_rejects_empty_secret_key() {
        let mut request = valid_remote_target_request();
        request["credentials"]["secretKey"] = serde_json::json!("");
        let request: RemoteTargetRequest =
            serde_json::from_value(request).expect("request should deserialize before semantic validation");

        let err = match request.into_bucket_target() {
            Ok(_) => panic!("empty secret key should fail semantic validation"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("credentials.secretKey is required"));
    }

    #[test]
    fn remote_target_request_converts_to_bucket_target() {
        let target = serde_json::from_value::<RemoteTargetRequest>(valid_remote_target_request())
            .expect("request should deserialize")
            .into_bucket_target()
            .expect("request should pass semantic validation");

        assert_eq!(target.endpoint, "192.168.1.10:9000");
        assert_eq!(target.target_bucket, "target");
        assert!(target.secure);
        assert_eq!(target.credentials.expect("credentials should be present").access_key, "access");
    }
}
