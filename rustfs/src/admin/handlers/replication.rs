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

use crate::admin::auth::authorize_admin_request;
use crate::admin::handlers::site_replication::site_replication_peer_deployment_id_for_endpoint;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::{
    AppContext, app_context_from_req, current_notification_system_for_context, current_replication_stats_handle_for_context,
    current_runtime_port, object_store_from_req,
};
use crate::admin::storage_api::bucket::metadata::BUCKET_TARGETS_FILE;
use crate::admin::storage_api::bucket::metadata_sys;
use crate::admin::storage_api::bucket::metadata_sys::get_replication_config;
use crate::admin::storage_api::bucket::replication::REMOTE_TARGET_UNSUPPORTED_FIELDS;
#[cfg(test)]
use crate::admin::storage_api::bucket::replication::REMOTE_TARGET_WRITABLE_FIELDS;
use crate::admin::storage_api::bucket::replication::{BucketStats, ReplicationStatusType};
use crate::admin::storage_api::bucket::target::{
    BucketTarget, BucketTargetType, Credentials as TargetCredentials, LatencyStat, duration_from_secs_or_nanos,
};
use crate::admin::storage_api::bucket::target_sys::{BucketTargetError, BucketTargetSys};
use crate::admin::storage_api::contract::bucket::{BucketOperations, BucketOptions};
use crate::admin::storage_api::contract::list::ListOperations as _;
use crate::admin::storage_api::error::StorageError;
use crate::admin::storage_api::runtime::PeerRestClient;
use crate::admin::utils::read_compatible_admin_body;
use crate::error::ApiError;
use crate::server::ADMIN_PREFIX;
use crate::storage::storage_api::lock_bucket_targets_metadata;
use http::{HeaderMap, HeaderValue, Uri};
use hyper::{Method, StatusCode};
use jiff::Timestamp;
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

const SUPPORTED_REMOTE_TARGET_API: &str = "s3v4";

/// Go encodes the zero `time.Time` as the year-1 instant
/// (`0001-01-01T00:00:00Z`, possibly re-encoded with an offset); no real
/// credential expiry lives in year 1, so any such timestamp means "unset".
fn is_go_zero_time(timestamp: Timestamp) -> bool {
    timestamp.to_zoned(jiff::tz::TimeZone::UTC).year() == 1
}

/// Field groups a `set-remote-target?update=true` request may modify, mirroring
/// MinIO's `TargetUpdateType` / `GetTargetUpdateOps` query contract: the update
/// overlays only the requested groups onto the stored target, so a client can
/// e.g. flip sync mode without knowing or re-sending the target credentials.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TargetUpdateOp {
    /// Connection group: credentials plus endpoint, target bucket, and TLS settings.
    Credentials,
    Sync,
    /// Per-target read-proxy opt-out (`disableProxy`).
    Proxy,
    Bandwidth,
    Path,
}

fn parse_remote_target_update_ops(queries: &HashMap<String, String>) -> S3Result<Vec<TargetUpdateOp>> {
    const SUPPORTED_OPS: &[(&str, TargetUpdateOp)] = &[
        ("creds", TargetUpdateOp::Credentials),
        ("sync", TargetUpdateOp::Sync),
        ("proxy", TargetUpdateOp::Proxy),
        ("bandwidth", TargetUpdateOp::Bandwidth),
        ("path", TargetUpdateOp::Path),
    ];
    // Present in the MinIO wire contract, but they drive target fields this
    // version rejects as unsupported — fail loudly instead of silently ignoring.
    const UNSUPPORTED_OPS: &[&str] = &["healthcheck", "edge", "edgeSyncBeforeExpiry"];

    for key in UNSUPPORTED_OPS {
        if queries.get(*key).is_some_and(|value| value == "true") {
            return Err(s3_error!(
                InvalidRequest,
                "remote target update op {key} is not supported by this RustFS version"
            ));
        }
    }
    Ok(SUPPORTED_OPS
        .iter()
        .filter(|(key, _)| queries.get(*key).is_some_and(|value| value == "true"))
        .map(|(_, op)| *op)
        .collect())
}

fn site_endpoint_for(endpoint: &str, secure: bool) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else if secure {
        format!("https://{endpoint}")
    } else {
        format!("http://{endpoint}")
    }
}

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

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RemoteTargetCredentialsRequest {
    #[serde(rename = "accessKey", default)]
    access_key: String,
    // madmin's BucketTarget::Clone() strips the secret before mc round-trips a
    // target, so a non-creds `mc replicate update` body carries accessKey
    // without secretKey; validate_connection_fields still rejects that shape
    // for create and creds updates.
    #[serde(rename = "secretKey", default)]
    secret_key: String,
    #[serde(alias = "sessionToken", default)]
    session_token: Option<String>,
    #[serde(default)]
    expiration: Option<Timestamp>,
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteTargetRequest {
    #[serde(rename = "sourcebucket", default)]
    source_bucket: String,
    #[serde(default)]
    endpoint: String,
    // Defaulted so a partial `update=true` body can omit credentials (or, as mc
    // does, send accessKey without the secret); the create path and creds
    // updates still reject incomplete ones.
    #[serde(default)]
    credentials: RemoteTargetCredentialsRequest,
    #[serde(rename = "targetbucket", default)]
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
    // The extra aliases accept madmin's JSON tags so mc bodies deserialize.
    #[serde(alias = "bandwidth", alias = "bandwidthlimit", default)]
    bandwidth_limit: i64,
    #[serde(rename = "replicationSync", default)]
    replication_sync: bool,
    #[serde(alias = "storageclass", default)]
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
    #[serde(alias = "resetID", default)]
    reset_id: String,
    #[serde(rename = "totalDowntime", default)]
    total_downtime: u64,
    #[serde(rename = "lastOnline", with = "time::serde::rfc3339::option", default)]
    last_online: Option<OffsetDateTime>,
    #[serde(rename = "isOnline", default)]
    online: bool,
    // Accepted so mc's round-tripped runtime latency still deserializes under
    // deny_unknown_fields, but deliberately unread: it is never persisted.
    #[serde(rename = "latency", default)]
    _latency: LatencyStat,
    #[serde(alias = "deploymentID", default)]
    deployment_id: String,
    #[serde(default)]
    edge: bool,
    #[serde(rename = "edgeSyncBeforeExpiry", default)]
    edge_sync_before_expiry: bool,
    #[serde(rename = "offlineCount", default)]
    offline_count: u64,
}

impl RemoteTargetRequest {
    /// Connection-group requirements: enforced on create and on `creds` updates,
    /// where the endpoint/credentials in the body replace the stored ones.
    fn validate_connection_fields(&self) -> S3Result<()> {
        if self.endpoint.trim().is_empty() {
            return Err(s3_error!(InvalidRequest, "endpoint is required"));
        }

        if self.target_bucket.trim().is_empty() {
            return Err(s3_error!(InvalidRequest, "targetbucket is required"));
        }

        if self.credentials.access_key.trim().is_empty() {
            return Err(s3_error!(InvalidRequest, "credentials.accessKey is required"));
        }

        if self.credentials.secret_key.trim().is_empty() {
            return Err(s3_error!(InvalidRequest, "credentials.secretKey is required"));
        }

        Ok(())
    }

    fn into_bucket_target(self) -> S3Result<BucketTarget> {
        self.validate_connection_fields()?;
        self.into_bucket_target_common()
    }

    /// Partial-update parse: only the field groups named by `ops` are validated;
    /// everything else may be absent from the body (MinIO clients omit it).
    fn into_update_bucket_target(self, ops: &[TargetUpdateOp]) -> S3Result<BucketTarget> {
        if self.arn.trim().is_empty() {
            return Err(s3_error!(InvalidRequest, "arn is required for update"));
        }
        if ops.contains(&TargetUpdateOp::Credentials) {
            self.validate_connection_fields()?;
        }
        self.into_bucket_target_common()
    }

    fn into_bucket_target_common(self) -> S3Result<BucketTarget> {
        if !self.target_type.is_valid() {
            return Err(s3_error!(InvalidRequest, "type is invalid"));
        }

        if self
            .credentials
            .session_token
            .as_deref()
            .is_some_and(|token| !token.trim().is_empty())
        {
            return Err(s3_error!(
                InvalidRequest,
                "remote target field credentials.session_token is not supported by this RustFS version"
            ));
        }

        // Go's `omitempty` never elides a zero `time.Time`, so every madmin
        // marshal carries `"expiration":"0001-01-01T00:00:00Z"`; only a real
        // (non-year-1) expiry means the client wants expiring credentials.
        if self
            .credentials
            .expiration
            .is_some_and(|expiration| !is_go_zero_time(expiration))
        {
            return Err(s3_error!(
                InvalidRequest,
                "remote target field credentials.expiration is not supported by this RustFS version"
            ));
        }

        if !self.api.is_empty() && self.api != SUPPORTED_REMOTE_TARGET_API {
            return Err(s3_error!(
                InvalidRequest,
                "remote target field api value is not supported by this RustFS version"
            ));
        }

        for (unsupported, configured) in REMOTE_TARGET_UNSUPPORTED_FIELDS
            .iter()
            .copied()
            .zip([self.edge, self.edge_sync_before_expiry])
        {
            if configured {
                return Err(s3_error!(
                    InvalidRequest,
                    "remote target field {unsupported} is not supported by this RustFS version"
                ));
            }
        }

        let mut credentials = TargetCredentials::from(self.credentials);
        // Past the check above the expiration can only be the zero-value
        // sentinel, i.e. "no expiration" — never persist it.
        credentials.expiration = None;

        Ok(BucketTarget {
            source_bucket: self.source_bucket,
            endpoint: self.endpoint,
            credentials: Some(credentials),
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
            // madmin/mc encode these Go `time.Duration` fields as nanoseconds;
            // legacy RustFS clients sent seconds. Accepted for mc compatibility;
            // the per-target health-check interval is not yet applied — the
            // heartbeat keeps its global env-configured interval.
            health_check_duration: duration_from_secs_or_nanos(self.health_check_duration),
            disable_proxy: self.disable_proxy,
            reset_before_date: self.reset_before_date,
            reset_id: self.reset_id,
            total_downtime: duration_from_secs_or_nanos(self.total_downtime),
            last_online: self.last_online,
            online: self.online,
            // Latency is a server-measured runtime stat that mc echoes back
            // from list-remote-targets (Go time.Duration nanoseconds), while
            // the persisted format is milliseconds — never store the client
            // value.
            latency: LatencyStat::default(),
            deployment_id: self.deployment_id,
            edge: self.edge,
            edge_sync_before_expiry: self.edge_sync_before_expiry,
            offline_count: self.offline_count,
        })
    }
}

/// Admin-response encoding of a remote target: the persisted bucket-targets
/// format keeps `healthCheckDuration`/`totalDowntime` in seconds and the
/// `latency` stats in milliseconds, but madmin decodes all of them as Go
/// `time.Duration` (nanoseconds) — re-encode just those fields without
/// touching the persistence wire format.
fn remote_target_admin_json(target: &BucketTarget) -> Result<serde_json::Value, serde_json::Error> {
    fn go_duration_nanos(duration: Duration) -> serde_json::Value {
        // Saturate instead of truncating: >u64::MAX nanoseconds (~584 years)
        // is unrepresentable for a Go time.Duration reader anyway.
        u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX).into()
    }

    let mut value = serde_json::to_value(target)?;
    value["healthCheckDuration"] = go_duration_nanos(target.health_check_duration);
    value["totalDowntime"] = go_duration_nanos(target.total_downtime);
    value["latency"] = serde_json::json!({
        "curr": go_duration_nanos(target.latency.curr),
        "avg": go_duration_nanos(target.latency.avg),
        "max": go_duration_nanos(target.latency.max),
    });
    Ok(value)
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
    authorize_admin_request(req, vec![Action::AdminAction(action)]).await
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

        // Same minio-go `replication.Metrics` wire shape as
        // `?replication-metrics` — the internal snake_case stats are the peer
        // RPC wire format and must not leak here.
        let data = serde_json::to_vec(&crate::admin::replication_metrics_wire::MetricsWire::from(
            &bucket_stats.replication_stats,
        ))
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

        let request = serde_json::from_slice::<RemoteTargetRequest>(&body).map_err(|e| {
            error!("Failed to parse remote target request body: {}", e);
            S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid remote target request: {e}"))
        })?;

        let update_ops = if update {
            parse_remote_target_update_ops(&queries)?
        } else {
            Vec::new()
        };
        let replacing_connection = !update || update_ops.contains(&TargetUpdateOp::Credentials);

        let mut remote_target = if update {
            request.into_update_bucket_target(&update_ops)?
        } else {
            request.into_bucket_target()?
        };

        // Endpoint, TLS, and credential fields from the body only take effect on
        // create or a `creds` update; a partial update body may omit them.
        if replacing_connection {
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

            if update {
                // Never trust a body-supplied deployment id on update: it is a
                // peer-identity anchor, so derive it from the new endpoint's peer
                // lookup below (empty when the endpoint is not a peer).
                remote_target.deployment_id = String::new();
            }
            let site_endpoint = site_endpoint_for(&remote_target.endpoint, remote_target.secure);
            if let Some(deployment_id) = site_replication_peer_deployment_id_for_endpoint(&site_endpoint).await {
                remote_target.deployment_id = deployment_id;
            }
        }
        remote_target.source_bucket = bucket.clone();

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

            // Overlay only the requested field groups onto the stored target
            // (MinIO `TargetUpdateType` semantics); everything else — including
            // credentials — stays as persisted.
            for op in &update_ops {
                match op {
                    TargetUpdateOp::Credentials => {
                        // Mirror MinIO: an operator never knows the site
                        // replicator's credentials, so overwriting a peer-owned
                        // target's connection settings would silently break sync.
                        // Peer ownership is probed three ways — either scheme
                        // derivation of the stored endpoint (a stale `secure` flag
                        // must not bypass the guard via a default-port mismatch)
                        // and the stored deployment id (covers peers registered
                        // under an alternate NAT/rewritten address).
                        let https_endpoint = site_endpoint_for(&target.endpoint, true);
                        let http_endpoint = site_endpoint_for(&target.endpoint, false);
                        let peer_owned = !target.deployment_id.trim().is_empty()
                            || site_replication_peer_deployment_id_for_endpoint(&https_endpoint)
                                .await
                                .is_some()
                            || site_replication_peer_deployment_id_for_endpoint(&http_endpoint)
                                .await
                                .is_some();
                        if peer_owned {
                            warn!(
                                bucket = %bucket,
                                arn = %target.arn,
                                "skip credentials update for site-replication peer target"
                            );
                            continue;
                        }
                        target.credentials = remote_target.credentials.clone();
                        target.endpoint = remote_target.endpoint.clone();
                        target.secure = remote_target.secure;
                        target.target_bucket = remote_target.target_bucket.clone();
                        target.skip_tls_verify = remote_target.skip_tls_verify;
                        target.ca_cert_pem = remote_target.ca_cert_pem.clone();
                        target.deployment_id = remote_target.deployment_id.clone();
                    }
                    TargetUpdateOp::Sync => target.replication_sync = remote_target.replication_sync,
                    TargetUpdateOp::Proxy => target.disable_proxy = remote_target.disable_proxy,
                    TargetUpdateOp::Bandwidth => target.bandwidth_limit = remote_target.bandwidth_limit,
                    TargetUpdateOp::Path => target.path = remote_target.path.clone(),
                }
            }

            warn!(
                bucket = %bucket,
                arn = %target.arn,
                endpoint = %target.endpoint,
                secure = target.secure,
                skip_tls_verify = target.skip_tls_verify,
                has_custom_ca = !target.ca_cert_pem.trim().is_empty(),
                ops = ?update_ops,
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

            let targets: Vec<_> = targets
                .iter()
                .map(|target| remote_target_admin_json(&target.redacted_credentials()))
                .collect::<Result<_, _>>()
                .map_err(|e| {
                    error!("Serialization error: {}", e);
                    S3Error::with_message(S3ErrorCode::InternalError, "Failed to serialize targets".to_string())
                })?;
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
/// `POST /v3/replication/diff`. Field names are the exact json tags of
/// madmin-go `DiffInfo` (replication-api.go), which `mc replicate diff`
/// decodes one JSON document at a time from the response body. `Size` is a
/// RustFS extension key with no madmin counterpart; Go decoders ignore
/// unknown keys.
#[derive(Debug, Serialize)]
struct ReplicationDiffEntry {
    #[serde(rename = "object")]
    object: String,
    #[serde(rename = "versionId", skip_serializing_if = "Option::is_none")]
    version_id: Option<String>,
    #[serde(rename = "Size")]
    size: i64,
    #[serde(rename = "deletemarker")]
    is_delete_marker: bool,
    #[serde(rename = "rStatus")]
    replication_status: String,
    #[serde(rename = "lastModified", skip_serializing_if = "Option::is_none")]
    last_modified: Option<String>,
}

/// Aggregate response body for `POST /v3/replication/diff?aggregate=true`.
///
/// This shell is a deliberate RustFS extension: madmin streams bare
/// `DiffInfo` documents with no envelope (see [`render_replication_diff`]),
/// so the scan-coverage metadata (`is_truncated`, `scanned_versions`) is only
/// representable in this opt-in aggregate shape. `entries` lists object
/// versions with a `PENDING` or `FAILED` replication status. `is_truncated`
/// indicates the on-demand scan hit [`REPLICATION_DIFF_MAX_SCAN`] before
/// reaching the end of the bucket, so the diff is partial and should be
/// re-run with a narrower prefix.
#[derive(Debug, Serialize)]
struct ReplicationDiffResponse {
    #[serde(rename = "Entries")]
    entries: Vec<ReplicationDiffEntry>,
    #[serde(rename = "IsTruncated")]
    is_truncated: bool,
    #[serde(rename = "ScannedVersions")]
    scanned_versions: usize,
}

/// Render the diff scan result as a response body.
///
/// Default (madmin-compatible) mode emits one `DiffInfo` JSON document per
/// line with no envelope — madmin's `BucketReplicationDiff` reads the body
/// with a `json.Decoder` loop, so any envelope object would decode as a
/// single entry with an empty `object` (a phantom row in `mc replicate
/// diff`). Scan-truncation info is not representable in that stream and is
/// reported via the tracing event in the handler instead.
///
/// `aggregate=true` (RustFS extension) keeps the enveloped shape including
/// `IsTruncated`/`ScannedVersions`.
fn render_replication_diff(
    entries: Vec<ReplicationDiffEntry>,
    is_truncated: bool,
    scanned_versions: usize,
    aggregate: bool,
) -> Result<Vec<u8>, serde_json::Error> {
    if aggregate {
        return serde_json::to_vec(&ReplicationDiffResponse {
            entries,
            is_truncated,
            scanned_versions,
        });
    }

    let mut data = Vec::new();
    for entry in &entries {
        serde_json::to_writer(&mut data, entry)?;
        data.push(b'\n');
    }
    Ok(data)
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
        let body = read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, req.uri.path(), &cred.secret_key).await?;
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
        let aggregate = queries.get("aggregate").map(String::as_str) == Some("true");
        if is_truncated && !aggregate {
            // The madmin stream has no envelope to carry truncation info, so
            // surface the partial-scan condition here instead.
            tracing::warn!(
                bucket = %bucket,
                prefix = %prefix,
                scanned = scanned_versions,
                max_scan = REPLICATION_DIFF_MAX_SCAN,
                "replication diff scan truncated; stream response is partial — re-run with a narrower prefix or use aggregate=true"
            );
        }

        let data = render_replication_diff(entries, is_truncated, scanned_versions, aggregate)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize failed: {e}")))?;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        // The madmin stream has no envelope to carry truncation info; a
        // truncated scan would otherwise be indistinguishable from a complete
        // one (an empty diff on a >MAX_SCAN bucket reads as "healthy"). Signal
        // it out-of-band so RustFS-aware clients can detect the partial scan;
        // madmin/mc ignore unknown headers.
        if is_truncated {
            headers.insert("x-rustfs-replication-diff-truncated", HeaderValue::from_static("true"));
        }
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
    #[serde(rename = "DurableCount")]
    durable_count: i64,
    #[serde(rename = "DurableSize")]
    durable_size: i64,
    #[serde(rename = "ObservationScope")]
    observation_scope: &'static str,
}

/// Response body for `GET /v3/replication/mrf`.
///
/// Runtime failed/queued totals and the durable MRF recovery backlog are kept
/// separate because older persisted MRF entries do not contain a target ARN.
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
    durable: &crate::admin::storage_api::replication::DurableMrfBacklog,
) -> MrfResponse {
    let observation_scope = if bucket_stats.replication_stats.cluster_complete {
        "cluster_aggregated"
    } else {
        "partial_cluster"
    };
    let mut targets: Vec<MrfTargetBacklog> = Vec::with_capacity(bucket_stats.replication_stats.stats.len());
    let mut targets_by_arn: HashMap<String, usize> = HashMap::with_capacity(bucket_stats.replication_stats.stats.len());
    let mut total_failed_count: i64 = 0;
    let mut total_failed_size: i64 = 0;
    for (arn, stat) in &bucket_stats.replication_stats.stats {
        total_failed_count = total_failed_count.saturating_add(stat.failed.count);
        total_failed_size = total_failed_size.saturating_add(stat.failed.size);
        targets_by_arn.insert(arn.clone(), targets.len());
        targets.push(MrfTargetBacklog {
            arn: arn.clone(),
            failed_count: stat.failed.count,
            failed_size: stat.failed.size,
            durable_count: 0,
            durable_size: 0,
            observation_scope,
        });
    }

    let queued = &bucket_stats.replication_stats.q_stat.curr;
    let (durable_count, durable_size, missing_target_arns) = if durable.available {
        durable.entries.iter().filter(|entry| entry.bucket == bucket).fold(
            (0i64, 0i64, false),
            |(count, size, missing_target_arns), entry| {
                let mut missing_target_arns = missing_target_arns;
                for target_arn in &entry.target_arns {
                    if target_arn.is_empty() {
                        continue;
                    }
                    let index = if let Some(index) = targets_by_arn.get(target_arn).copied() {
                        index
                    } else {
                        targets_by_arn.insert(target_arn.clone(), targets.len());
                        targets.push(MrfTargetBacklog {
                            arn: target_arn.clone(),
                            failed_count: 0,
                            failed_size: 0,
                            durable_count: 0,
                            durable_size: 0,
                            observation_scope,
                        });
                        targets.len() - 1
                    };
                    targets[index].durable_count = targets[index].durable_count.saturating_add(1);
                    targets[index].durable_size = targets[index].durable_size.saturating_add(entry.size);
                }
                if entry.target_arns.is_empty() {
                    missing_target_arns = true;
                }
                (count.saturating_add(1), size.saturating_add(entry.size), missing_target_arns)
            },
        )
    } else {
        (0, 0, false)
    };
    targets.sort_by(|a, b| a.arn.cmp(&b.arn));

    MrfResponse {
        bucket,
        targets,
        total_failed_count,
        total_failed_size,
        queued_count: queued.count,
        queued_size: queued.bytes,
        // The default (non-aggregate) response mode streams the durable
        // backlog per object, so the enumerable API exists whenever the
        // backlog is readable.
        per_object_entries_available: durable.available,
        runtime_stats_available: bucket_stats.replication_stats.provider_available,
        cluster_complete: bucket_stats.replication_stats.cluster_complete,
        observed_node_count: bucket_stats.replication_stats.observed_node_count,
        expected_node_count: bucket_stats.replication_stats.expected_node_count,
        durable_backlog_available: durable.available,
        durable_count,
        durable_size,
        per_target_durable_entries_available: durable.available && !missing_target_arns,
    }
}

/// One durable MRF backlog entry rendered for the default (madmin-compatible)
/// stream. Field names are the exact json tags of madmin-go `ReplicationMRF`
/// (replication-api.go), which `mc replicate backlog` decodes one JSON
/// document at a time. `Size` and `TargetARNs` are RustFS extension keys with
/// no madmin counterpart; Go decoders ignore unknown keys.
#[derive(Debug, Serialize)]
struct MrfEntryDocument {
    /// The durable backlog is a cluster-shared ledger with no per-node
    /// attribution, so the madmin `nodeName` tag is always empty.
    #[serde(rename = "nodeName")]
    node_name: String,
    #[serde(rename = "bucket")]
    bucket: String,
    #[serde(rename = "object")]
    object: String,
    #[serde(rename = "versionId")]
    version_id: String,
    #[serde(rename = "retryCount")]
    retry_count: i32,
    #[serde(rename = "Size")]
    size: i64,
    #[serde(rename = "TargetARNs", skip_serializing_if = "Vec::is_empty")]
    target_arns: Vec<String>,
}

/// Upper bound on the number of documents one stream response emits. The
/// durable ledger is not bounded by the in-memory pending cap (recovery can
/// persist far larger generations), and the body is buffered before send, so
/// an unbounded read could stage hundreds of MB per request. The handler
/// rejects a response beyond this bound instead of returning a partial 200.
const REPLICATION_MRF_MAX_STREAM_ENTRIES: usize = 10_000;

/// Project the durable backlog into madmin `ReplicationMRF` documents,
/// scoped to `bucket` when it is non-empty (madmin allows an empty bucket to
/// mean "across all buckets"), bounded by
/// [`REPLICATION_MRF_MAX_STREAM_ENTRIES`]. Returns the documents and whether
/// the backlog was truncated.
fn mrf_entry_documents(
    bucket: &str,
    durable: &crate::admin::storage_api::replication::DurableMrfBacklog,
) -> (Vec<MrfEntryDocument>, bool) {
    let mut documents = Vec::new();
    let mut truncated = false;
    for entry in durable
        .entries
        .iter()
        .filter(|entry| bucket.is_empty() || entry.bucket == bucket)
    {
        if documents.len() >= REPLICATION_MRF_MAX_STREAM_ENTRIES {
            truncated = true;
            break;
        }
        documents.push(MrfEntryDocument {
            node_name: String::new(),
            bucket: entry.bucket.clone(),
            object: entry.object.clone(),
            // Delete-marker purge entries track the marker version separately;
            // fall back to it so those rows still carry a version identity.
            // The nil UUID is RustFS's in-memory null-version sentinel and
            // must leave as the S3 wire token, not a zero UUID.
            version_id: entry
                .version_id
                .or(entry.delete_marker_version_id)
                .map(|v| {
                    if v.is_nil() {
                        rustfs_filemeta::NULL_VERSION_ID.to_string()
                    } else {
                        v.to_string()
                    }
                })
                .unwrap_or_default(),
            retry_count: entry.retry_count,
            size: entry.size,
            target_arns: entry.target_arns.clone(),
        });
    }
    (documents, truncated)
}

/// Render the MRF backlog as a response body.
///
/// Default (madmin-compatible) mode emits one `ReplicationMRF` JSON document
/// per line with no envelope — madmin's `BucketReplicationMRF` reads the body
/// with a `json.Decoder` loop, so an envelope object would decode as a single
/// entry whose `"Bucket"` key case-insensitively matches
/// `ReplicationMRF.Bucket` (a phantom row in `mc replicate backlog`), and an
/// empty backlog must render an empty body so the loop ends on io.EOF with
/// zero rows.
///
/// `aggregate=true` (RustFS extension) keeps the enveloped counter shape;
/// backlog-source health (`RuntimeStatsAvailable`/`DurableBacklogAvailable`)
/// is only representable there — an unreadable ledger fails the stream
/// request outright in the handler (madmin only decodes the body of a 200,
/// so an empty stream would read as a healthy zero-row backlog).
fn render_mrf_backlog(
    response: &MrfResponse,
    durable: &crate::admin::storage_api::replication::DurableMrfBacklog,
    aggregate: bool,
) -> Result<(Vec<u8>, bool), serde_json::Error> {
    if aggregate {
        return Ok((serde_json::to_vec(response)?, false));
    }

    let (documents, truncated) = mrf_entry_documents(&response.bucket, durable);
    let mut data = Vec::new();
    for entry in documents {
        serde_json::to_writer(&mut data, &entry)?;
        data.push(b'\n');
    }
    Ok((data, truncated))
}

fn ensure_complete_mrf_stream(truncated: bool) -> S3Result<()> {
    if truncated {
        return Err(S3Error::with_message(
            S3ErrorCode::ServiceUnavailable,
            "durable MRF backlog exceeds the stream limit; narrow the bucket scope or drain the backlog".to_string(),
        ));
    }
    Ok(())
}

/// `GET /v3/replication/mrf`
///
/// Reports the failed-replication backlog (MinIO's MRF concept) for a bucket.
///
/// The default response is a madmin-compatible stream of `ReplicationMRF`
/// documents built from the durable backlog ledger (in-memory failures that
/// have not been flushed yet — the persister runs every few seconds — are not
/// visible). `?aggregate=true` (RustFS extension) returns the enveloped
/// runtime + durable counter shape instead; `PerTargetDurableEntriesAvailable`
/// is false there when the durable backlog includes older entries that cannot
/// be attributed to a target.
///
/// The madmin `node` parameter is accepted but has no filtering effect: the
/// durable ledger is cluster-shared with no per-node attribution, so every
/// node serves the same (complete) backlog.
///
/// Authorization: the stream requires `admin:ReplicationDiff` (it enumerates
/// object names and version ids, MinIO parity); `?aggregate=true` carries no
/// object identities and requires only `admin:GetReplicationMetrics`.
pub struct ReplicationMrfHandler {}

#[async_trait::async_trait]
impl Operation for ReplicationMrfHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let queries = extract_query_params(&req.uri);
        let aggregate = queries.get("aggregate").map(String::as_str) == Some("true");
        // The default stream enumerates object names and version ids, which
        // a metrics-only principal must not see; gate it on the same action
        // MinIO uses for this endpoint. The aggregate counters carry no
        // object identities and keep the metrics action.
        let action = if aggregate {
            AdminAction::GetReplicationMetricsAction
        } else {
            AdminAction::ReplicationDiff
        };
        validate_replication_admin_request(&req, action).await?;

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

        if let Some(node) = queries.get("node").filter(|node| !node.is_empty() && node.as_str() != "all") {
            // The durable backlog ledger is cluster-shared with no per-node
            // attribution, so a node-scoped request still sees the complete
            // (superset) backlog.
            debug!(node = %node, "replication mrf node filter has no effect on the cluster-shared backlog");
        }

        let durable = crate::admin::storage_api::replication::read_durable_mrf_backlog(store).await;
        let bucket_stats = cluster_replication_stats(&bucket, app_context_from_req(&req)).await;
        let response = build_mrf_response(bucket, &bucket_stats, &durable);

        if !durable.available && !aggregate {
            // The madmin stream has no envelope to carry source health, and
            // madmin only decodes the body of a 200 — an empty stream would
            // read as a clean, healthy zero-row backlog. Fail loudly instead;
            // aggregate mode still reports the availability fields.
            tracing::warn!(
                bucket = %response.bucket,
                "durable MRF backlog is unreadable; failing the stream request — use aggregate=true to see source health"
            );
            return Err(S3Error::with_message(
                S3ErrorCode::ServiceUnavailable,
                "durable MRF backlog is unreadable; retry, or use aggregate=true for source health".to_string(),
            ));
        }

        let (data, truncated) = render_mrf_backlog(&response, &durable, aggregate)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize failed: {e}")))?;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if truncated {
            tracing::warn!(
                event = "replication_mrf_stream_rejected",
                component = "admin",
                subsystem = "replication",
                result = "rejected",
                bucket = %response.bucket,
                max_entries = REPLICATION_MRF_MAX_STREAM_ENTRIES,
                "replication mrf stream exceeds the response limit"
            );
        }
        ensure_complete_mrf_stream(truncated)?;
        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        REMOTE_TARGET_UNSUPPORTED_FIELDS, REMOTE_TARGET_WRITABLE_FIELDS, RemoteTargetCredentialsRequest, RemoteTargetRequest,
        ReplicationDiffEntry, SUPPORTED_REMOTE_TARGET_API, TargetUpdateOp, build_mrf_response, extract_query_params,
        parse_remote_target_update_ops, render_mrf_backlog, render_replication_diff, unique_replication_peers,
        validate_remote_target_tls_settings,
    };
    use crate::admin::storage_api::bucket::target::{BucketTarget, LatencyStat};
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

    /// Verbatim `encoding/json` marshal of the madmin-go v3.0.109 `BucketTarget`
    /// that `mc replicate add` builds (path=auto, api=s3v4, 60s healthcheck).
    /// Field presence follows Go `omitempty` semantics against the v3.0.109 tags:
    /// zero strings (`sessionToken`, `arn`, `region`, `storageclass`, `resetID`,
    /// `deploymentID`) and the zero `bandwidthlimit` int are elided; untagged
    /// fields (`sourcebucket`, `secure`, `type`, `replicationSync`,
    /// `disableProxy`, `totalDowntime`, `lastOnline`, `isOnline`, `latency`,
    /// `edge`, `edgeSyncBeforeExpiry`, `offlineCount`) always appear; and
    /// `omitempty` never elides a struct, so the zero `time.Time` fields
    /// (`credentials.expiration`, `resetBeforeDate`) still appear as
    /// `0001-01-01T00:00:00Z`.
    const MADMIN_REPLICATE_ADD_BODY: &str = r#"{"sourcebucket":"","endpoint":"192.168.1.10:9000","credentials":{"accessKey":"access","secretKey":"secret","expiration":"0001-01-01T00:00:00Z"},"targetbucket":"target","secure":false,"path":"auto","api":"s3v4","type":"replication","replicationSync":false,"healthCheckDuration":60000000000,"disableProxy":false,"resetBeforeDate":"0001-01-01T00:00:00Z","totalDowntime":0,"lastOnline":"0001-01-01T00:00:00Z","isOnline":false,"latency":{"curr":0,"avg":0,"max":0},"edge":false,"edgeSyncBeforeExpiry":false,"offlineCount":0}"#;

    /// Verbatim marshal of the same target after `mc replicate update --sync`:
    /// mc lists remote targets (latency reported in Go `time.Duration`
    /// nanoseconds), `BucketTarget::Clone()` strips only the secret key, and the
    /// mutated clone round-trips every other stored field — including the
    /// nanosecond latency echo — back to set-remote-target.
    const MADMIN_REPLICATE_UPDATE_BODY: &str = r#"{"sourcebucket":"src","endpoint":"192.168.1.10:9000","credentials":{"accessKey":"access","expiration":"0001-01-01T00:00:00Z"},"targetbucket":"target","secure":false,"path":"auto","api":"s3v4","arn":"arn:rustfs:replication:us-east-1:dep:target","type":"replication","replicationSync":true,"healthCheckDuration":60000000000,"disableProxy":false,"resetBeforeDate":"0001-01-01T00:00:00Z","totalDowntime":0,"lastOnline":"0001-01-01T00:00:00Z","isOnline":true,"latency":{"curr":60000000000,"avg":45000000000,"max":90000000000},"edge":false,"edgeSyncBeforeExpiry":false,"offlineCount":0}"#;

    fn query_map(pairs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn update_ops_parse_minio_query_contract() {
        let ops = parse_remote_target_update_ops(&query_map(&[
            ("update", "true"),
            ("creds", "true"),
            ("sync", "true"),
            ("proxy", "true"),
            ("bandwidth", "true"),
            ("path", "true"),
        ]))
        .expect("supported ops should parse");
        assert_eq!(
            ops,
            vec![
                TargetUpdateOp::Credentials,
                TargetUpdateOp::Sync,
                TargetUpdateOp::Proxy,
                TargetUpdateOp::Bandwidth,
                TargetUpdateOp::Path
            ]
        );

        assert!(
            parse_remote_target_update_ops(&query_map(&[("update", "true")]))
                .expect("no ops should parse")
                .is_empty()
        );

        let err = parse_remote_target_update_ops(&query_map(&[("update", "true"), ("healthcheck", "true")]))
            .expect_err("unsupported op must be rejected");
        assert!(err.to_string().contains("not supported"), "unexpected error: {err}");
    }

    #[test]
    fn update_body_without_creds_op_may_omit_credentials() {
        let body = serde_json::json!({
            "arn": "arn:rustfs:replication:us-east-1:dep:target",
            "type": "replication",
            "replicationSync": true
        });
        let request: RemoteTargetRequest = serde_json::from_value(body).expect("partial update body should deserialize");
        let target = request
            .into_update_bucket_target(&[TargetUpdateOp::Sync])
            .expect("sync-only update must not require credentials");
        assert!(target.replication_sync);

        // The same partial body must stay rejected when it claims a creds update.
        let body = serde_json::json!({
            "arn": "arn:rustfs:replication:us-east-1:dep:target",
            "type": "replication"
        });
        let request: RemoteTargetRequest = serde_json::from_value(body).expect("body should deserialize");
        let err = request
            .into_update_bucket_target(&[TargetUpdateOp::Credentials])
            .expect_err("creds update requires connection fields");
        assert!(err.to_string().contains("endpoint is required"), "unexpected error: {err}");
    }

    #[test]
    fn update_body_accepts_mc_wire_shape() {
        // What `mc replicate update --sync` actually sends: a madmin
        // BucketTarget round-trip with the secret stripped by Clone() and
        // madmin's own JSON tags (bandwidthlimit, deploymentID, ...).
        let body = serde_json::json!({
            "sourcebucket": "src",
            "endpoint": "192.168.1.10:9000",
            "credentials": { "accessKey": "access" },
            "targetbucket": "target",
            "secure": false,
            "path": "auto",
            "api": "s3v4",
            "arn": "arn:rustfs:replication:us-east-1:dep:target",
            "type": "replication",
            "bandwidthlimit": 1073741824i64,
            "replicationSync": true,
            "totalDowntime": 0,
            "lastOnline": "2024-01-01T00:00:00Z",
            "isOnline": true,
            "latency": { "curr": 0, "avg": 0, "max": 0 },
            "deploymentID": "dep",
            "edge": false,
            "edgeSyncBeforeExpiry": false,
            "offlineCount": 0
        });
        let request: RemoteTargetRequest = serde_json::from_value(body).expect("mc-shaped body should deserialize");
        let target = request
            .into_update_bucket_target(&[TargetUpdateOp::Sync, TargetUpdateOp::Bandwidth])
            .expect("non-creds update must not require the stripped secret");
        assert!(target.replication_sync);
        assert_eq!(target.bandwidth_limit, 1073741824);
    }

    #[test]
    fn update_body_requires_arn() {
        let body = serde_json::json!({
            "type": "replication",
            "replicationSync": true
        });
        let request: RemoteTargetRequest = serde_json::from_value(body).expect("body should deserialize");
        let err = request
            .into_update_bucket_target(&[TargetUpdateOp::Sync])
            .expect_err("update without arn must fail");
        assert!(err.to_string().contains("arn is required"), "unexpected error: {err}");
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
                    force_delete: false,
                    delete_marker_version_id: None,
                    delete_marker: false,
                    delete_marker_mtime: None,
                    target_arns: vec!["arn-a".to_string(), "arn-durable-only".to_string()],
                    ..Default::default()
                },
                MrfReplicateEntry {
                    bucket: "other-bucket".to_string(),
                    object: "object-b".to_string(),
                    version_id: None,
                    retry_count: 0,
                    size: 999,
                    op: MrfOpKind::Object,
                    force_delete: false,
                    delete_marker_version_id: None,
                    delete_marker: false,
                    delete_marker_mtime: None,
                    target_arns: Vec::new(),
                    ..Default::default()
                },
            ],
        };

        let response = build_mrf_response("bucket-a".to_string(), &stats, &durable);
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
        // The bare stream enumerates the durable backlog per object, so a
        // readable backlog advertises the enumerable API.
        assert_eq!(json["PerObjectEntriesAvailable"], true);
        assert_eq!(json["PerTargetDurableEntriesAvailable"], true);

        let targets = json["Targets"].as_array().expect("targets should serialize as an array");
        let target_a = targets
            .iter()
            .find(|target| target["ARN"] == "arn-a")
            .expect("runtime target should remain present");
        assert_eq!(target_a["FailedCount"], 3);
        assert_eq!(target_a["FailedSize"], 900);
        assert_eq!(target_a["DurableCount"], 1);
        assert_eq!(target_a["DurableSize"], 250);
        let target_durable_only = targets
            .iter()
            .find(|target| target["ARN"] == "arn-durable-only")
            .expect("durable-only target should be surfaced");
        assert_eq!(target_durable_only["FailedCount"], 0);
        assert_eq!(target_durable_only["FailedSize"], 0);
        assert_eq!(target_durable_only["DurableCount"], 1);
        assert_eq!(target_durable_only["DurableSize"], 250);
    }

    #[test]
    fn mrf_response_keeps_legacy_durable_entries_bucket_only() {
        let mut stats = BucketStats::default();
        stats.replication_stats.provider_available = true;
        stats.replication_stats.cluster_complete = true;
        stats.replication_stats.observed_node_count = 1;
        stats.replication_stats.expected_node_count = 1;
        let durable = DurableMrfBacklog {
            available: true,
            entries: vec![MrfReplicateEntry {
                bucket: "bucket-a".to_string(),
                object: "legacy-object".to_string(),
                version_id: None,
                retry_count: 0,
                size: 250,
                op: MrfOpKind::Object,
                force_delete: false,
                delete_marker_version_id: None,
                delete_marker: false,
                delete_marker_mtime: None,
                target_arns: Vec::new(),
                ..Default::default()
            }],
        };

        let response = build_mrf_response("bucket-a".to_string(), &stats, &durable);
        let json = serde_json::to_value(response).expect("MRF response should serialize");

        assert_eq!(json["DurableBacklogAvailable"], true);
        assert_eq!(json["DurableCount"], 1);
        assert_eq!(json["DurableSize"], 250);
        assert_eq!(json["PerTargetDurableEntriesAvailable"], false);
        assert_eq!(
            json["Targets"]
                .as_array()
                .expect("targets should serialize as an array")
                .len(),
            0
        );
    }

    #[test]
    fn mrf_response_distinguishes_unavailable_sources_from_valid_zero() {
        let unavailable = build_mrf_response("bucket-a".to_string(), &BucketStats::default(), &DurableMrfBacklog::default());
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
            &DurableMrfBacklog {
                available: true,
                entries: Vec::new(),
            },
        );
        let valid_empty_json = serde_json::to_value(valid_empty).expect("valid empty response should serialize");
        assert_eq!(valid_empty_json["RuntimeStatsAvailable"], true);
        assert_eq!(valid_empty_json["DurableBacklogAvailable"], true);
        assert_eq!(valid_empty_json["TotalFailedCount"], 0);
        assert_eq!(valid_empty_json["DurableCount"], 0);
        assert_eq!(valid_empty_json["PerTargetDurableEntriesAvailable"], true);
    }

    fn sample_durable_backlog() -> DurableMrfBacklog {
        DurableMrfBacklog {
            available: true,
            entries: vec![
                MrfReplicateEntry {
                    bucket: "bucket-a".to_string(),
                    object: "object-a".to_string(),
                    version_id: Some(uuid::Uuid::from_u128(7)),
                    retry_count: 2,
                    size: 250,
                    op: MrfOpKind::Object,
                    target_arns: vec!["arn-a".to_string()],
                    ..Default::default()
                },
                MrfReplicateEntry {
                    bucket: "other-bucket".to_string(),
                    object: "object-b".to_string(),
                    version_id: None,
                    retry_count: 0,
                    size: 999,
                    op: MrfOpKind::Object,
                    target_arns: Vec::new(),
                    ..Default::default()
                },
            ],
        }
    }

    /// madmin's `BucketReplicationMRF` decodes the body one `ReplicationMRF`
    /// JSON document at a time; the default response must therefore be a bare
    /// document stream with madmin's exact json tags, not an envelope.
    #[test]
    fn mrf_stream_renders_bare_madmin_documents() {
        let durable = sample_durable_backlog();
        let response = build_mrf_response("bucket-a".to_string(), &BucketStats::default(), &durable);

        let (body, _) = render_mrf_backlog(&response, &durable, false).expect("stream body should serialize");
        let text = String::from_utf8(body).expect("body should be utf-8");
        let lines: Vec<&str> = text.lines().filter(|line| !line.trim().is_empty()).collect();

        // Only the entry matching the requested bucket is streamed.
        assert_eq!(lines.len(), 1, "expected one MRF document, got: {text}");
        let doc: serde_json::Value = serde_json::from_str(lines[0]).expect("each line should be a JSON document");
        assert_eq!(doc["bucket"], "bucket-a");
        assert_eq!(doc["object"], "object-a");
        assert_eq!(doc["versionId"], uuid::Uuid::from_u128(7).to_string());
        assert_eq!(doc["retryCount"], 2);
        // madmin `ReplicationMRF` has a `nodeName` tag; the durable backlog is
        // cluster-shared, so RustFS reports an empty node name.
        assert_eq!(doc["nodeName"], "");
        // The envelope keys must not leak into the stream: a `"Bucket"` key
        // would case-insensitively populate `ReplicationMRF.Bucket` and render
        // a phantom row in `mc replicate backlog`.
        assert!(doc.get("Bucket").is_none());
        assert!(doc.get("Targets").is_none());
    }

    /// An empty backlog must produce an empty body: madmin's decoder loop then
    /// terminates on io.EOF with zero rows instead of one phantom row.
    #[test]
    fn mrf_stream_renders_empty_body_for_no_entries() {
        let durable = DurableMrfBacklog {
            available: true,
            entries: Vec::new(),
        };
        let response = build_mrf_response("bucket-a".to_string(), &BucketStats::default(), &durable);

        let (body, _) = render_mrf_backlog(&response, &durable, false).expect("stream body should serialize");
        assert!(
            body.is_empty(),
            "empty backlog must serialize to an empty body, got: {}",
            String::from_utf8_lossy(&body)
        );
    }

    /// `?aggregate=true` (RustFS extension) keeps the enveloped counter shape.
    #[test]
    fn mrf_aggregate_envelope_retains_counters() {
        let durable = sample_durable_backlog();
        let response = build_mrf_response("bucket-a".to_string(), &BucketStats::default(), &durable);

        let (body, _) = render_mrf_backlog(&response, &durable, true).expect("aggregate body should serialize");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("aggregate body should be one JSON object");
        assert_eq!(json["Bucket"], "bucket-a");
        assert_eq!(json["DurableCount"], 1);
        assert_eq!(json["DurableBacklogAvailable"], true);
        // The bare stream is an enumerable per-object API, so the aggregate
        // shell now truthfully advertises it whenever the backlog is readable.
        assert_eq!(json["PerObjectEntriesAvailable"], true);
    }

    /// The nil UUID is RustFS's in-memory null-version sentinel; the wire
    /// token is `null`, never the zero UUID (second review round).
    #[test]
    fn mrf_stream_maps_nil_version_to_null_token() {
        let durable = DurableMrfBacklog {
            available: true,
            entries: vec![MrfReplicateEntry {
                bucket: "bucket-a".to_string(),
                object: "null-version-object".to_string(),
                version_id: Some(uuid::Uuid::nil()),
                retry_count: 1,
                size: 10,
                op: MrfOpKind::Object,
                ..Default::default()
            }],
        };
        let response = build_mrf_response("bucket-a".to_string(), &BucketStats::default(), &durable);

        let (body, truncated) = render_mrf_backlog(&response, &durable, false).expect("stream body should serialize");
        assert!(!truncated);
        let doc: serde_json::Value =
            serde_json::from_str(String::from_utf8(body).expect("utf-8").lines().next().expect("one line"))
                .expect("line should be a JSON document");
        assert_eq!(doc["versionId"], "null");
    }

    /// The durable ledger is not bounded by the in-memory pending cap; the
    /// stream must stop at the documented bound and signal truncation
    /// (second review round).
    #[test]
    fn mrf_stream_truncates_at_the_documented_bound() {
        let entries = (0..super::REPLICATION_MRF_MAX_STREAM_ENTRIES + 1)
            .map(|index| MrfReplicateEntry {
                bucket: "bucket-a".to_string(),
                object: format!("object-{index}"),
                retry_count: 1,
                op: MrfOpKind::Object,
                ..Default::default()
            })
            .collect();
        let durable = DurableMrfBacklog {
            available: true,
            entries,
        };
        let response = build_mrf_response("bucket-a".to_string(), &BucketStats::default(), &durable);

        let (body, truncated) = render_mrf_backlog(&response, &durable, false).expect("stream body should serialize");
        assert!(truncated, "one entry past the bound must signal truncation");
        assert_eq!(
            String::from_utf8(body).expect("utf-8").lines().count(),
            super::REPLICATION_MRF_MAX_STREAM_ENTRIES
        );
        let error = super::ensure_complete_mrf_stream(truncated).expect_err("partial streams must not return 200");
        assert_eq!(error.code(), &s3s::S3ErrorCode::ServiceUnavailable);
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

        let err = match serde_json::from_value::<RemoteTargetRequest>(request) {
            Ok(_) => panic!("remote target request should reject unknown fields"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn remote_target_request_rejects_missing_credentials() {
        // Credentials may be absent at the serde layer (partial update bodies omit
        // them), but the create path must still reject their absence.
        let mut request = valid_remote_target_request();
        request
            .as_object_mut()
            .expect("request should be an object")
            .remove("credentials");

        let request: RemoteTargetRequest = serde_json::from_value(request).expect("body without credentials should deserialize");
        let err = request
            .into_bucket_target()
            .expect_err("create without credentials must fail");
        assert!(err.to_string().contains("credentials.accessKey is required"), "unexpected error: {err}");
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
    fn remote_target_credentials_expiration_json_remains_rfc3339() {
        let credentials: RemoteTargetCredentialsRequest = serde_json::from_value(serde_json::json!({
            "accessKey": "access",
            "secretKey": "secret",
            "expiration": "2026-01-01T00:00:00Z"
        }))
        .expect("credentials expiration should deserialize from RFC3339 JSON");
        let credentials = crate::admin::storage_api::bucket::target::Credentials::from(credentials);
        let expiration = credentials.expiration.expect("expiration should be preserved");

        assert_eq!(
            serde_json::to_value(expiration).expect("expiration should serialize to JSON"),
            serde_json::json!("2026-01-01T00:00:00Z")
        );
    }

    #[test]
    fn remote_target_request_rejects_unimplemented_fields() {
        for (field, value) in [
            ("credentials.session_token", serde_json::json!("session-token")),
            ("credentials.expiration", serde_json::json!("2026-01-01T00:00:00Z")),
            ("api", serde_json::json!("s3v2")),
            ("edge", serde_json::json!(true)),
            ("edgeSyncBeforeExpiry", serde_json::json!(true)),
        ] {
            let mut request = valid_remote_target_request();
            if let Some((credential_field, credential_name)) = field.split_once('.') {
                request[credential_field][credential_name] = value;
            } else {
                request[field] = value;
            }
            let request: RemoteTargetRequest =
                serde_json::from_value(request).expect("unsupported field should still deserialize");
            let err = request
                .into_bucket_target()
                .expect_err("unimplemented remote target fields must not be persisted");

            assert!(err.to_string().contains(field));
            assert!(err.to_string().contains("not supported by this RustFS version"));
        }
    }

    #[test]
    fn remote_target_request_accepts_real_madmin_add_marshal() {
        let request: RemoteTargetRequest =
            serde_json::from_str(MADMIN_REPLICATE_ADD_BODY).expect("real mc replicate add body should deserialize");
        let target = request
            .into_bucket_target()
            .expect("real mc replicate add body must be accepted");

        assert_eq!(target.endpoint, "192.168.1.10:9000");
        assert_eq!(target.target_bucket, "target");
        assert_eq!(target.health_check_duration, std::time::Duration::from_secs(60));
        let credentials = target.credentials.expect("credentials should be present");
        assert_eq!(credentials.access_key, "access");
        assert_eq!(credentials.secret_key, "secret");
        assert!(
            credentials.expiration.is_none(),
            "Go zero-value expiration is an unset sentinel and must not be persisted"
        );
    }

    #[test]
    fn remote_target_request_accepts_go_zero_time_expiration_offset_forms() {
        // Go marshals the zero time.Time as 0001-01-01T00:00:00Z, but a body
        // that re-encoded the same instant with an offset is equally "unset".
        for expiration in ["0001-01-01T00:00:00Z", "0001-01-01T08:00:00+08:00"] {
            let mut request = valid_remote_target_request();
            request["credentials"]["expiration"] = serde_json::json!(expiration);
            let request: RemoteTargetRequest = serde_json::from_value(request).expect("request should deserialize");
            request
                .into_bucket_target()
                .unwrap_or_else(|err| panic!("zero-value expiration {expiration} must be accepted: {err}"));
        }
    }

    #[test]
    fn remote_target_update_accepts_madmin_clone_round_trip() {
        let request: RemoteTargetRequest =
            serde_json::from_str(MADMIN_REPLICATE_UPDATE_BODY).expect("mc replicate update body should deserialize");
        let target = request
            .into_update_bucket_target(&[TargetUpdateOp::Sync])
            .expect("mc replicate update round-trip body must be accepted");

        assert!(target.replication_sync);
        assert_eq!(
            target.latency.curr,
            std::time::Duration::ZERO,
            "nanosecond latency echoed back by mc must not be persisted as milliseconds"
        );
    }

    #[test]
    fn remote_target_request_ignores_client_supplied_latency() {
        // Latency is a server-measured runtime stat; mc echoes the
        // list-remote-targets nanosecond values back on update, while the
        // request parser historically read them as milliseconds (1e6 blow-up).
        let mut request = valid_remote_target_request();
        request["latency"] = serde_json::json!({
            "curr": 60_000_000_000u64,
            "avg": 45_000_000_000u64,
            "max": 90_000_000_000u64
        });

        let target = serde_json::from_value::<RemoteTargetRequest>(request)
            .expect("request should deserialize")
            .into_bucket_target()
            .expect("request should pass semantic validation");

        assert_eq!(target.latency.curr, std::time::Duration::ZERO);
        assert_eq!(target.latency.avg, std::time::Duration::ZERO);
        assert_eq!(target.latency.max, std::time::Duration::ZERO);
    }

    #[test]
    fn remote_target_request_accepts_static_credentials_and_supported_api() {
        let mut request = valid_remote_target_request();
        request["api"] = serde_json::json!("s3v4");
        request["secure"] = serde_json::json!(true);
        request["caCertPem"] = serde_json::json!("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n");
        request["credentials"]["session_token"] = serde_json::json!("");

        let target = serde_json::from_value::<RemoteTargetRequest>(request)
            .expect("supported remote target request should deserialize")
            .into_bucket_target()
            .expect("static credentials, SigV4 and custom CA should remain supported");

        assert_eq!(target.api, SUPPORTED_REMOTE_TARGET_API);
        assert_eq!(
            target
                .credentials
                .as_ref()
                .and_then(|credentials| credentials.session_token.as_deref()),
            Some("")
        );
        assert!(!target.ca_cert_pem.is_empty());
    }

    #[test]
    fn remote_target_request_validation_does_not_echo_credential_values() {
        let mut request = valid_remote_target_request();
        request["credentials"]["session_token"] = serde_json::json!("session-token-must-not-leak");

        let request: RemoteTargetRequest = serde_json::from_value(request).expect("request should deserialize");
        let err = request
            .into_bucket_target()
            .expect_err("session tokens must be rejected before persistence");
        let message = err.to_string();

        assert!(message.contains("credentials.session_token"));
        assert!(!message.contains("session-token-must-not-leak"));
        assert!(!message.contains("secret"));
    }

    #[test]
    fn remote_target_request_accepts_go_duration_wire_values() {
        // `mc replicate add` defaults `--healthcheck-seconds` to 60; madmin
        // serializes Go `time.Duration` fields as nanosecond integers.
        let mut request = valid_remote_target_request();
        request["healthCheckDuration"] = serde_json::json!(60_000_000_000u64);
        request["totalDowntime"] = serde_json::json!(90_000_000_000u64);

        let target = serde_json::from_value::<RemoteTargetRequest>(request)
            .expect("madmin-shaped request should deserialize")
            .into_bucket_target()
            .expect("mc default healthCheckDuration must be accepted");

        assert_eq!(target.health_check_duration, std::time::Duration::from_secs(60));
        assert_eq!(target.total_downtime, std::time::Duration::from_secs(90));
    }

    #[test]
    fn remote_target_request_accepts_legacy_seconds_health_check() {
        // Older RustFS clients sent these duration fields as plain seconds.
        let mut request = valid_remote_target_request();
        request["healthCheckDuration"] = serde_json::json!(60);

        let target = serde_json::from_value::<RemoteTargetRequest>(request)
            .expect("request should deserialize")
            .into_bucket_target()
            .expect("legacy seconds healthCheckDuration must be accepted");

        assert_eq!(target.health_check_duration, std::time::Duration::from_secs(60));
    }

    #[test]
    fn list_remote_targets_response_encodes_go_durations_as_nanoseconds() {
        // madmin (and therefore mc) decode healthCheckDuration/totalDowntime
        // as Go time.Duration nanoseconds; the persisted format stays seconds.
        let target = BucketTarget {
            endpoint: "192.168.1.10:9000".to_string(),
            target_bucket: "target".to_string(),
            health_check_duration: std::time::Duration::from_secs(60),
            total_downtime: std::time::Duration::from_secs(90),
            latency: LatencyStat {
                curr: std::time::Duration::from_millis(12),
                avg: std::time::Duration::from_millis(34),
                max: std::time::Duration::from_millis(250),
            },
            ..Default::default()
        };

        let value = super::remote_target_admin_json(&target).expect("admin response should serialize");

        assert_eq!(value["healthCheckDuration"], 60_000_000_000u64);
        assert_eq!(value["totalDowntime"], 90_000_000_000u64);
        assert_eq!(value["latency"]["curr"], 12_000_000u64);
        assert_eq!(value["latency"]["avg"], 34_000_000u64);
        assert_eq!(value["latency"]["max"], 250_000_000u64);
        // Persistence keeps seconds/milliseconds: the response path must not
        // leak into it.
        let persisted = serde_json::to_value(&target).expect("persisted form should serialize");
        assert_eq!(persisted["healthCheckDuration"], 60);
        assert_eq!(persisted["totalDowntime"], 90);
        assert_eq!(persisted["latency"]["curr"], 12);
        assert_eq!(persisted["latency"]["avg"], 34);
        assert_eq!(persisted["latency"]["max"], 250);
    }

    #[test]
    fn remote_target_admin_json_latency_round_trips_through_go_duration() {
        // Round trip: a madmin reader decodes the latency values as Go
        // time.Duration nanoseconds — decoding must recover the exact
        // durations the server reported.
        let target = BucketTarget {
            latency: LatencyStat {
                curr: std::time::Duration::from_micros(1_500),
                avg: std::time::Duration::from_millis(87),
                max: std::time::Duration::from_secs(2),
            },
            ..Default::default()
        };

        let value = super::remote_target_admin_json(&target).expect("admin response should serialize");

        for (field, expected) in [
            ("curr", target.latency.curr),
            ("avg", target.latency.avg),
            ("max", target.latency.max),
        ] {
            let nanos = value["latency"][field].as_u64().expect("latency field must be a u64");
            assert_eq!(std::time::Duration::from_nanos(nanos), expected, "latency.{field} must round-trip");
        }
    }

    #[test]
    fn remote_target_health_check_duration_is_declared_writable() {
        assert!(REMOTE_TARGET_WRITABLE_FIELDS.contains(&"healthCheckDuration"));
        assert!(!REMOTE_TARGET_UNSUPPORTED_FIELDS.contains(&"healthCheckDuration"));
    }

    #[test]
    fn remote_target_disable_proxy_is_declared_writable_edge_stays_unsupported() {
        assert!(REMOTE_TARGET_WRITABLE_FIELDS.contains(&"disableProxy"));
        assert!(!REMOTE_TARGET_UNSUPPORTED_FIELDS.contains(&"disableProxy"));
        // edge sync has no implementation behind it — it must stay rejected.
        assert!(REMOTE_TARGET_UNSUPPORTED_FIELDS.contains(&"edge"));
        assert!(REMOTE_TARGET_UNSUPPORTED_FIELDS.contains(&"edgeSyncBeforeExpiry"));
    }

    #[test]
    fn remote_target_create_accepts_disable_proxy() {
        let mut request = valid_remote_target_request();
        request["disableProxy"] = serde_json::json!(true);

        let target = serde_json::from_value::<RemoteTargetRequest>(request)
            .expect("request should deserialize")
            .into_bucket_target()
            .expect("disableProxy is a supported per-target read-proxy opt-out");

        assert!(target.disable_proxy);
    }

    #[test]
    fn update_body_with_proxy_op_toggles_disable_proxy_without_credentials() {
        // Mirrors the other partial-update groups: a proxy-only update body may
        // omit the connection fields entirely.
        let body = serde_json::json!({
            "arn": "arn:rustfs:replication:us-east-1:dep:target",
            "type": "replication",
            "disableProxy": true
        });
        let request: RemoteTargetRequest = serde_json::from_value(body).expect("partial update body should deserialize");
        let target = request
            .into_update_bucket_target(&[TargetUpdateOp::Proxy])
            .expect("proxy-only update must not require credentials");
        assert!(target.disable_proxy);
    }

    #[test]
    fn remote_target_capability_fields_do_not_overlap() {
        for field in REMOTE_TARGET_UNSUPPORTED_FIELDS {
            assert!(
                !REMOTE_TARGET_WRITABLE_FIELDS.contains(field),
                "remote target field {field} cannot be both writable and unsupported"
            );
        }
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

    fn sample_diff_entries() -> Vec<ReplicationDiffEntry> {
        vec![
            ReplicationDiffEntry {
                object: "a.txt".to_string(),
                version_id: Some("v1".to_string()),
                size: 42,
                is_delete_marker: false,
                replication_status: "PENDING".to_string(),
                last_modified: Some("2026-01-01T00:00:00Z".to_string()),
            },
            ReplicationDiffEntry {
                object: "b.txt".to_string(),
                version_id: None,
                size: 0,
                is_delete_marker: true,
                replication_status: "FAILED".to_string(),
                last_modified: None,
            },
        ]
    }

    #[test]
    fn replication_diff_stream_emits_bare_madmin_diff_info_documents() {
        let data = render_replication_diff(sample_diff_entries(), true, 2, false).expect("stream must render");
        let text = std::str::from_utf8(&data).expect("stream must be utf-8");

        // madmin decodes the body with a json.Decoder loop over DiffInfo — one
        // bare document per entry, no envelope keys anywhere in the stream.
        let lines: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).expect("each line must be a JSON document"))
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["object"], "a.txt");
        assert_eq!(lines[0]["versionId"], "v1");
        assert_eq!(lines[0]["rStatus"], "PENDING");
        assert_eq!(lines[0]["deletemarker"], false);
        assert_eq!(lines[0]["lastModified"], "2026-01-01T00:00:00Z");
        assert_eq!(lines[1]["object"], "b.txt");
        assert_eq!(lines[1]["deletemarker"], true);
        assert_eq!(lines[1]["rStatus"], "FAILED");
        for line in &lines {
            assert!(line.get("Entries").is_none());
            assert!(line.get("IsTruncated").is_none());
            assert!(line.get("ScannedVersions").is_none());
        }
    }

    #[test]
    fn replication_diff_stream_renders_empty_body_for_no_entries() {
        let data = render_replication_diff(Vec::new(), false, 0, false).expect("stream must render");
        // An empty body makes madmin's json.Decoder loop end immediately with
        // io.EOF — zero rows, not one phantom empty row.
        assert!(data.is_empty());
    }

    #[test]
    fn replication_diff_aggregate_keeps_enveloped_extension_shape() {
        let data = render_replication_diff(sample_diff_entries(), true, 7, true).expect("aggregate must render");
        let payload: serde_json::Value = serde_json::from_slice(&data).expect("aggregate must be one JSON object");

        assert_eq!(payload["Entries"].as_array().map(Vec::len), Some(2));
        assert_eq!(payload["Entries"][0]["object"], "a.txt");
        assert_eq!(payload["IsTruncated"], true);
        assert_eq!(payload["ScannedVersions"], 7);
    }
}
