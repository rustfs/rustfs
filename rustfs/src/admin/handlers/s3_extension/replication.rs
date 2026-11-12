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

//! The replication extension routes: `GET /{bucket}?replication-metrics[=2]`,
//! `GET /{bucket}?replication-check`, `GET /{bucket}?replication-reset-status`
//! and `PUT /{bucket}?replication-reset`.
//!
//! Responsible for: each route's handler and the metrics, resync and status
//! answers. The active target validation behind `replication-check` lives in
//! `check`.
//!
//! Not responsible for: recognising or admitting the request (`super`), nor the
//! admin-API replication endpoints under `/rustfs/admin/v3` (`handlers::replication`).

mod check;
#[cfg(test)]
mod tests;

use crate::admin::handlers::replication::cluster_replication_stats;
use crate::admin::operation::{AdminRequest, AdminResponse, Operation};
use crate::admin::replication_metrics_wire::{MetricsV2Wire, MetricsWire};
use crate::admin::runtime_sources::{
    AppContext, app_context_from_envelope, current_boot_time, current_bucket_monitor_handle, current_object_store_handle,
    current_replication_pool_handle,
};
use crate::admin::storage_api::bucket::bandwidth::monitor::BandwidthDetails;
use crate::admin::storage_api::bucket::metadata_sys;
use crate::admin::storage_api::bucket::replication::{self, BucketReplicationResyncStatus, BucketStats};
use crate::admin::storage_api::bucket::target_sys::BucketTargetSys;
use crate::admin::storage_api::bucket::versioning_sys::BucketVersioningSys;
use crate::admin::storage_api::bucket::{AdminReplicationConfigExt as _, AdminVersioningConfigExt as _};
use crate::admin::storage_api::contract::bucket::{BucketOperations, BucketOptions};
use crate::admin::storage_api::error::StorageError;
use crate::admin::utils::{owned_json_response, query_value_exact};
use crate::error::ApiError;
use crate::storage::storage_api::lock_bucket_targets_metadata;
use check::run_replication_check;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue, Method, StatusCode, Uri};
use matchit::Params;
use rustfs_gateway_types::dto::Status;
use rustfs_gateway_types::persistence::PersistedReplicationConfiguration;
use rustfs_madmin::utils::parse_duration;
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_types::{Body, S3Error, S3ErrorCode, S3Result, s3_error};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;
use time::OffsetDateTime;
use uuid::Uuid;

/// The registered replication extension handlers, one per route.
pub(super) static REPLICATION_EXTENSIONS: [ReplicationExtension; 5] = [
    ReplicationExtension(ReplicationExtRoute::ResetStart),
    ReplicationExtension(ReplicationExtRoute::ResetStatus),
    ReplicationExtension(ReplicationExtRoute::MetricsV2),
    ReplicationExtension(ReplicationExtRoute::MetricsV1),
    ReplicationExtension(ReplicationExtRoute::Check),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReplicationExtRoute {
    MetricsV1,
    MetricsV2,
    Check,
    ResetStart,
    ResetStatus,
}

impl ReplicationExtRoute {
    pub(super) fn method(self) -> Method {
        match self {
            Self::ResetStart => Method::PUT,
            Self::MetricsV1 | Self::MetricsV2 | Self::Check | Self::ResetStatus => Method::GET,
        }
    }

    /// The query sub-resource that names the route; both metrics versions share
    /// `replication-metrics` and are told apart by its value.
    pub(super) fn sub_resource(self) -> &'static str {
        match self {
            Self::MetricsV1 => "replication-metrics",
            Self::MetricsV2 => "replication-metrics=2",
            Self::Check => "replication-check",
            Self::ResetStart => "replication-reset",
            Self::ResetStatus => "replication-reset-status",
        }
    }

    /// The route a bucket-level request names, judged on the first occurrence of
    /// each sub-resource in the order below.
    pub(super) fn classify(method: &Method, uri: &Uri) -> Option<Self> {
        let flag = |key| query_value_exact(uri, key).as_deref() == Some("");
        if method == Method::PUT {
            return flag("replication-reset").then_some(Self::ResetStart);
        }
        if method != Method::GET {
            return None;
        }
        if flag("replication-reset-status") {
            return Some(Self::ResetStatus);
        }
        match query_value_exact(uri, "replication-metrics").as_deref() {
            Some("2") => return Some(Self::MetricsV2),
            Some("") => return Some(Self::MetricsV1),
            _ => {}
        }
        flag("replication-check").then_some(Self::Check)
    }
}

/// One replication extension route.
pub(super) struct ReplicationExtension(ReplicationExtRoute);

impl ReplicationExtension {
    pub(super) fn route(&self) -> ReplicationExtRoute {
        self.0
    }
}

#[async_trait::async_trait]
impl Operation for ReplicationExtension {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let route = self.0;
        let bucket = params
            .get("bucket")
            .ok_or_else(|| s3_error!(InternalError, "replication extension route has no bucket"))?;
        super::authorize(&mut req, Some(bucket), replication_extension_policy_action(route)).await?;
        ensure_replication_bucket_exists(bucket).await?;

        match route {
            ReplicationExtRoute::MetricsV1 | ReplicationExtRoute::MetricsV2 => {
                ensure_replication_config_exists(bucket).await?;
                build_replication_metrics_response(bucket, route, app_context_from_envelope(req.envelope())).await
            }
            ReplicationExtRoute::Check => {
                let (versioning, _) = metadata_sys::get_versioning_config(bucket).await.map_err(ApiError::from)?;
                if !versioning.enabled() && !BucketVersioningSys::enabled(bucket).await {
                    return Err(s3_error!(
                        InvalidRequest,
                        "replication validation requires bucket versioning to be enabled"
                    ));
                }
                ensure_replication_config_exists(bucket).await?;
                run_replication_check(bucket).await
            }
            ReplicationExtRoute::ResetStatus => {
                ensure_replication_config_exists(bucket).await?;
                let status_req = parse_reset_status_target(req.uri());
                let status = load_replication_resync_status(bucket).await?;
                build_replication_reset_status_response(status, status_req.arn.as_deref())
            }
            ReplicationExtRoute::ResetStart => {
                ensure_replication_config_exists(bucket).await?;
                let target = parse_reset_start_target(req.uri())?;
                let target = start_replication_resync(bucket, &target).await?;
                owned_json_response(StatusCode::OK, &ReplicationResetResponse { targets: vec![target] })
            }
        }
    }
}

// Wire shape mirrors madmin-go `ResyncTargetsInfo`/`ResyncTarget` json tags so
// `mc replicate resync` can decode the response (Go json decoding is
// case-insensitive per field, but the `target` shell key must match exactly).
#[derive(Debug, Clone, serde::Serialize, Default)]
struct ReplicationResetResponse {
    #[serde(rename = "target")]
    targets: Vec<ReplicationResetTarget>,
}

#[derive(Debug, Clone, serde::Serialize, Default)]
struct ReplicationResetTarget {
    #[serde(rename = "arn")]
    arn: String,
    #[serde(rename = "resetid")]
    reset_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplicationResetStartRequest {
    arn: String,
    reset_id: String,
    reset_before: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct ReplicationResetStatusRequest {
    arn: Option<String>,
}

// Wire shape mirrors madmin-go `ResyncTargetsInfo`/`ResyncTarget` json tags
// (see `ReplicationResetResponse`). `ResetBeforeDate` and `Error` are RustFS
// extension keys with no madmin counterpart; Go decoders ignore unknown keys,
// so they coexist with madmin/mc clients at zero cost.
#[derive(Debug, Clone, serde::Serialize, Default)]
struct ReplicationResetStatusResponse {
    #[serde(rename = "target")]
    targets: Vec<ReplicationResetStatusTarget>,
}

#[derive(Debug, Clone, serde::Serialize, Default)]
struct ReplicationResetStatusTarget {
    #[serde(rename = "arn")]
    arn: String,
    #[serde(rename = "resetid")]
    reset_id: String,
    #[serde(
        rename = "ResetBeforeDate",
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    reset_before_date: Option<OffsetDateTime>,
    #[serde(
        rename = "startTime",
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    start_time: Option<OffsetDateTime>,
    #[serde(
        rename = "endTime",
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    end_time: Option<OffsetDateTime>,
    #[serde(rename = "resyncStatus")]
    status: String,
    #[serde(rename = "replicationCount")]
    replicated_count: i64,
    #[serde(rename = "completedReplicationSize")]
    replicated_size: i64,
    #[serde(rename = "failedReplicationCount")]
    failed_count: i64,
    #[serde(rename = "failedReplicationSize")]
    failed_size: i64,
    #[serde(rename = "bucket", skip_serializing_if = "String::is_empty")]
    bucket: String,
    #[serde(rename = "object", skip_serializing_if = "String::is_empty")]
    object: String,
    #[serde(rename = "Error", skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

async fn ensure_replication_bucket_exists(bucket: &str) -> S3Result<()> {
    let Some(store) = current_object_store_handle() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init"));
    };

    store
        .get_bucket_info(bucket, &BucketOptions::default())
        .await
        .map_err(ApiError::from)?;

    Ok(())
}

async fn ensure_replication_config_exists(bucket: &str) -> S3Result<()> {
    match metadata_sys::get_replication_config(bucket).await {
        Ok(_) => Ok(()),
        Err(StorageError::ConfigNotFound) => Err(S3Error::new(S3ErrorCode::ReplicationConfigurationNotFoundError)),
        Err(err) => Err(ApiError::from(err).into()),
    }
}

async fn build_replication_metrics_response(
    bucket: &str,
    route: ReplicationExtRoute,
    context: Option<Arc<AppContext>>,
) -> S3Result<AdminResponse> {
    let bucket_stats = cluster_replication_stats(bucket, context).await;
    let bucket_stats = apply_replication_metrics_bandwidth_report(bucket_stats, collect_replication_metrics_bandwidth(bucket));
    let bucket_stats = apply_replication_metrics_runtime_fields(bucket_stats, route, replication_metrics_uptime_seconds());

    let node_name = crate::runtime_sources::current_local_node_name().await.unwrap_or_default();
    let body = serialize_replication_metrics_body(&bucket_stats, route, &node_name)?;

    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Ok(AdminResponse::with_headers(StatusCode::OK, Body::from(body), headers))
}

fn replication_metrics_uptime_seconds() -> i64 {
    current_boot_time()
        .and_then(|boot_time| SystemTime::now().duration_since(boot_time).ok())
        .map(|uptime| uptime.as_secs() as i64)
        .unwrap_or_default()
}

fn collect_replication_metrics_bandwidth(bucket: &str) -> HashMap<String, BandwidthDetails> {
    current_bucket_monitor_handle()
        .map(|monitor| {
            monitor
                .get_report(|name| name == bucket)
                .bucket_stats
                .into_iter()
                .filter_map(|(opts, details)| {
                    if opts.replication_arn.is_empty() {
                        None
                    } else {
                        Some((opts.replication_arn, details))
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

fn apply_replication_metrics_bandwidth_report(
    mut bucket_stats: BucketStats,
    bandwidth_report: HashMap<String, BandwidthDetails>,
) -> BucketStats {
    if bucket_stats.replication_stats.expected_node_count > 1 {
        return bucket_stats;
    }
    for (arn, details) in bandwidth_report {
        let stat = bucket_stats.replication_stats.stats.entry(arn).or_default();
        stat.set_node_local_bandwidth(details.limit_bytes_per_sec, details.current_bandwidth_bytes_per_sec);
    }

    bucket_stats
}

fn apply_replication_metrics_runtime_fields(
    mut bucket_stats: BucketStats,
    route: ReplicationExtRoute,
    uptime_seconds: i64,
) -> BucketStats {
    if route == ReplicationExtRoute::MetricsV2 {
        bucket_stats.uptime = uptime_seconds;
    }
    bucket_stats
}

/// Serialize the metrics body in the minio-go wire shapes
/// (`replication.Metrics` for v1, `replication.MetricsV2` for v2). The
/// internal `BucketStats` serde names are the intra-cluster peer-RPC wire
/// format and must never appear here — see
/// `crate::admin::replication_metrics_wire`.
fn serialize_replication_metrics_body(
    bucket_stats: &BucketStats,
    route: ReplicationExtRoute,
    node_name: &str,
) -> S3Result<Vec<u8>> {
    match route {
        ReplicationExtRoute::MetricsV1 => serde_json::to_vec(&MetricsWire::from(&bucket_stats.replication_stats))
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("{e}"))),
        ReplicationExtRoute::MetricsV2 => serde_json::to_vec(&MetricsV2Wire::from_stats(bucket_stats, node_name))
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("{e}"))),
        ReplicationExtRoute::Check | ReplicationExtRoute::ResetStart | ReplicationExtRoute::ResetStatus => {
            Err(S3Error::with_message(S3ErrorCode::InternalError, "invalid route for metrics response"))
        }
    }
}

fn replication_extension_policy_action(route: ReplicationExtRoute) -> Action {
    match route {
        ReplicationExtRoute::MetricsV1 | ReplicationExtRoute::MetricsV2 => {
            Action::S3Action(S3Action::GetReplicationConfigurationAction)
        }
        ReplicationExtRoute::Check => Action::S3Action(S3Action::PutReplicationConfigurationAction),
        ReplicationExtRoute::ResetStart | ReplicationExtRoute::ResetStatus => {
            Action::S3Action(S3Action::ResetBucketReplicationStateAction)
        }
    }
}

fn parse_reset_start_target(uri: &Uri) -> S3Result<ReplicationResetStartRequest> {
    let arn = query_value_exact(uri, "arn").filter(|v| !v.is_empty()).unwrap_or_default();

    let now = OffsetDateTime::now_utc();
    let reset_before = match query_value_exact(uri, "older-than").filter(|v| !v.is_empty()) {
        Some(older_than) => {
            let duration = parse_duration(&older_than).map_err(|err| {
                S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid older-than query parameter: {err}"))
            })?;
            let duration = time::Duration::try_from(duration).map_err(|err| {
                S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid older-than query parameter: {err}"))
            })?;
            Some(now - duration)
        }
        None => Some(now),
    };

    let reset_id = query_value_exact(uri, "reset-id")
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    Ok(ReplicationResetStartRequest {
        arn,
        reset_id,
        reset_before,
    })
}

fn collect_resettable_replication_target_arns(config: &PersistedReplicationConfiguration) -> Vec<String> {
    let mut arns = Vec::new();
    let mut seen = HashSet::new();

    for rule in &config.rules {
        if rule.status == Status::DISABLED.as_str() {
            continue;
        }

        let existing_object_enabled = rule
            .existing_object_replication
            .as_ref()
            .is_some_and(|status| status.status == Status::ENABLED.as_str());
        if !existing_object_enabled {
            continue;
        }

        let arn = if config.role.trim().is_empty() {
            rule.destination.bucket.trim().to_string()
        } else {
            config.role.trim().to_string()
        };

        if seen.insert(arn.clone()) {
            arns.push(arn);
        }

        if !config.role.trim().is_empty() {
            break;
        }
    }

    arns
}

fn resolve_replication_reset_target_arn(config: &PersistedReplicationConfiguration, requested_arn: &str) -> S3Result<String> {
    let resettable_arns = collect_resettable_replication_target_arns(config);

    if requested_arn.is_empty() {
        return match resettable_arns.as_slice() {
            [] => Err(s3_error!(
                InvalidRequest,
                "replication reset requires a target with existing object replication enabled"
            )),
            [arn] => Ok(arn.clone()),
            _ => Err(s3_error!(
                InvalidRequest,
                "arn query parameter is required when multiple replication targets are configured"
            )),
        };
    }

    let (has_arn, existing_object_enabled) = config.has_existing_object_replication(requested_arn);
    if !has_arn {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            "replication reset arn is not configured for this bucket",
        ));
    }
    if !existing_object_enabled {
        return Err(s3_error!(
            InvalidRequest,
            "replication reset requires existing object replication to be enabled for the target"
        ));
    }

    Ok(requested_arn.to_string())
}

fn map_replication_resync_start_error(error: StorageError) -> S3Error {
    match replication::resync_start_conflict_id(&error) {
        Some(active_resync_id) => replication_resync_active_conflict_error(active_resync_id),
        None => S3Error::with_message(S3ErrorCode::InternalError, format!("{error}")),
    }
}

fn replication_resync_active_conflict_error(active_resync_id: &str) -> S3Error {
    s3_error!(
        OperationAborted,
        "replication resync {active_resync_id} is already active for this target"
    )
}

fn parse_reset_status_target(uri: &Uri) -> ReplicationResetStatusRequest {
    ReplicationResetStatusRequest {
        arn: query_value_exact(uri, "arn").filter(|v| !v.is_empty()),
    }
}

fn build_replication_reset_status_targets(
    status: &BucketReplicationResyncStatus,
    arn_filter: Option<&str>,
) -> Vec<ReplicationResetStatusTarget> {
    let mut targets = status
        .targets_map
        .iter()
        .filter(|(arn, _)| arn_filter.is_none_or(|filter| *arn == filter))
        .map(|(arn, target)| ReplicationResetStatusTarget {
            arn: arn.clone(),
            reset_id: target.resync_id.clone(),
            reset_before_date: target.resync_before_date,
            start_time: target.start_time,
            end_time: target.last_update,
            status: target.resync_status.to_string(),
            replicated_count: target.replicated_count,
            replicated_size: target.replicated_size,
            failed_count: target.failed_count,
            failed_size: target.failed_size,
            bucket: target.bucket.clone(),
            object: target.object.clone(),
            error: target.error.clone(),
        })
        .collect::<Vec<_>>();
    targets.sort_by(|left, right| left.arn.cmp(&right.arn));
    targets
}

fn build_replication_reset_status_response(
    status: BucketReplicationResyncStatus,
    arn_filter: Option<&str>,
) -> S3Result<AdminResponse> {
    owned_json_response(
        StatusCode::OK,
        &ReplicationResetStatusResponse {
            targets: build_replication_reset_status_targets(&status, arn_filter),
        },
    )
}

async fn start_replication_resync(bucket: &str, reset: &ReplicationResetStartRequest) -> S3Result<ReplicationResetTarget> {
    let Some(pool) = current_replication_pool_handle() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "replication pool is not initialized"));
    };

    let _targets_guard = lock_bucket_targets_metadata(bucket).await;
    let transaction_guard = metadata_sys::acquire_bucket_metadata_transaction_lock(bucket)
        .await
        .map_err(ApiError::from)?;
    let (config, _) = metadata_sys::get_replication_config(bucket).await.map_err(ApiError::from)?;
    let resolved_arn = resolve_replication_reset_target_arn(&config, &reset.arn)?;
    let targets = metadata_sys::list_bucket_targets_from_disk(bucket)
        .await
        .map_err(ApiError::from)?;
    let opts = replication::resync_opts(bucket, resolved_arn.clone(), &reset.reset_id, reset.reset_before);
    let admission_pool = pool.clone();
    let activation_pool = pool.clone();
    let committed_targets = replication::commit_resync_target(
        targets,
        opts,
        move |opts| async move { admission_pool.admit_bucket_resync(opts).await },
        move |encoded| async move {
            metadata_sys::update_bucket_targets_under_transaction_lock(&transaction_guard, bucket, encoded)
                .await
                .map(|_| ())
                .map_err(|_| {
                    StorageError::other(
                        "replication resync was accepted but target metadata commit failed; retry the same reset ID to reconcile",
                    )
                })
        },
        move |opts, recovering| async move { activation_pool.activate_bucket_resync(opts, recovering).await },
    )
    .await
    .map_err(map_replication_resync_start_error)?;
    BucketTargetSys::get()
        .update_all_targets(bucket, Some(&committed_targets))
        .await;

    Ok(ReplicationResetTarget {
        arn: resolved_arn,
        reset_id: reset.reset_id.clone(),
    })
}

async fn load_replication_resync_status(bucket: &str) -> S3Result<BucketReplicationResyncStatus> {
    let Some(pool) = current_replication_pool_handle() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "replication pool is not initialized"));
    };

    pool.get_bucket_resync_status(bucket)
        .await
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("{e}")))
}
