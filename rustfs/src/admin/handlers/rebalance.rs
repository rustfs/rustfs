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

use crate::admin::runtime_sources::object_store_from_extensions;
use crate::admin::storage_api::contract::admin::StorageAdminApi;
use crate::admin::storage_api::contract::bucket::{BucketOperations, BucketOptions};
use crate::admin::storage_api::error::StorageError;
use crate::admin::storage_api::rebalance::{
    DiskStat, RebalSaveOpt, RebalanceCleanupWarnings, RebalanceMeta, RebalanceStopPropagationRecord,
    decode_rebalance_stop_propagation_record,
};
use crate::admin::storage_api::runtime::{ECStore, NotificationSys};
use crate::{
    admin::runtime_sources::current_notification_system,
    admin::{
        auth::authorize_admin_request,
        router::{AdminOperation, Operation, S3Router},
    },
    server::{ADMIN_PREFIX, RemoteAddr},
};
use http::{HeaderMap, HeaderValue, StatusCode, Uri};
use hyper::Method;
use matchit::Params;
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_utils::{
    MaskedAccessKey,
    http::{AMZ_REQUEST_ID, REQUEST_ID_HEADER},
};
use s3s::{
    Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    header::{CONTENT_LENGTH, CONTENT_TYPE},
    s3_error,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use time::OffsetDateTime;
use tracing::{error, info, warn};

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_REBALANCE: &str = "rebalance";
const EVENT_ADMIN_REBALANCE_STATE: &str = "admin_rebalance_state";

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

fn log_rebalance_request_rejected(action: &str, reason: &str, request_id: &str, actor: &str, remote_addr: &str) {
    warn!(
        event = EVENT_ADMIN_REBALANCE_STATE,
        component = LOG_COMPONENT_ADMIN,
        subsystem = LOG_SUBSYSTEM_REBALANCE,
        action,
        result = "rejected",
        reason,
        request_id = %request_id,
        actor = %actor,
        remote_addr = %remote_addr,
        "admin rebalance state"
    );
}

fn rebalance_query_present(uri: &Uri) -> bool {
    uri.query().is_some_and(|query| !query.is_empty())
}

fn rollback_result_label(result: &Result<(), String>) -> &'static str {
    match result {
        Ok(_) => "rollback_success",
        Err(err) if err.contains("peer") => "rollback_partial",
        Err(_) => "rollback_failed",
    }
}

fn rebalance_start_rollback_error(start_err: &str, rollback_result: &Result<(), String>) -> String {
    match rollback_result {
        Ok(_) => format!("failed to propagate rebalance start: {start_err}; rollback result: rollback_success"),
        Err(err) => format!(
            "failed to propagate rebalance start: {start_err}; rollback result: {}; rollback error: {err}",
            rollback_result_label(rollback_result)
        ),
    }
}

fn rebalance_internal_error(message: impl Into<String>) -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, message.into())
}

fn rebalance_rollback_stop_failure_message(rebalance_id: &str, failures: &[String]) -> String {
    format!("cluster stop_rebalance rollback for {rebalance_id} partial: {}", failures.join("; "))
}

fn rebalance_rollback_terminal_reload_failure_message(rebalance_id: &str, failures: &[String]) -> String {
    format!(
        "cluster terminal rebalance reload rollback for {rebalance_id} partial: {}",
        failures.join("; ")
    )
}

fn rebalance_rollback_failure_message(
    rebalance_id: &str,
    stop_failures: &[String],
    terminal_reload_failures: &[String],
) -> String {
    let mut failures = Vec::new();
    if !stop_failures.is_empty() {
        failures.push(rebalance_rollback_stop_failure_message(rebalance_id, stop_failures));
    }
    if !terminal_reload_failures.is_empty() {
        failures.push(rebalance_rollback_terminal_reload_failure_message(rebalance_id, terminal_reload_failures));
    }
    failures.join("; ")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RebalanceStartStep {
    PropagateFence,
    StartLocal,
    PropagateWorkers,
}

const DISTRIBUTED_REBALANCE_START_STEPS: [RebalanceStartStep; 3] = [
    RebalanceStartStep::PropagateFence,
    RebalanceStartStep::StartLocal,
    RebalanceStartStep::PropagateWorkers,
];
const LOCAL_REBALANCE_START_STEPS: [RebalanceStartStep; 1] = [RebalanceStartStep::StartLocal];

fn rebalance_start_steps(has_notification_sys: bool) -> &'static [RebalanceStartStep] {
    if has_notification_sys {
        &DISTRIBUTED_REBALANCE_START_STEPS
    } else {
        &LOCAL_REBALANCE_START_STEPS
    }
}

async fn rollback_cluster_rebalance_start(
    store: &Arc<ECStore>,
    notification_sys: Option<&NotificationSys>,
    rebalance_id: &str,
) -> Result<(), String> {
    let stop_attempt_at = OffsetDateTime::now_utc();
    if let Some(notification_sys) = notification_sys {
        let stop_failures = notification_sys
            .stop_rebalance_failures(Some(rebalance_id))
            .await
            .map_err(|err| format!("cluster stop_rebalance rollback for {rebalance_id} failed: {err}"))?;
        let terminal_reload_attempt_at = OffsetDateTime::now_utc();
        let terminal_reload_failures = match notification_sys.load_rebalance_meta_failures(false).await {
            Ok(failures) => failures,
            Err(err) => vec![format!("terminal rebalance reload rollback for {rebalance_id} failed: {err}")],
        };
        if !stop_failures.is_empty() || !terminal_reload_failures.is_empty() {
            let record = RebalanceStopPropagationRecord {
                stop_attempt_at: Some(stop_attempt_at),
                stop_failures: stop_failures.clone(),
                terminal_reload_attempt_at: Some(terminal_reload_attempt_at),
                terminal_reload_failures: terminal_reload_failures.clone(),
            };
            store
                .record_rebalance_stop_propagation(rebalance_id, record)
                .await
                .map_err(|err| {
                    format!(
                        "cluster rebalance rollback for {rebalance_id} partial; failed to persist stop propagation: {err}; {}",
                        rebalance_rollback_failure_message(rebalance_id, &stop_failures, &terminal_reload_failures)
                    )
                })?;
            return Err(rebalance_rollback_failure_message(
                rebalance_id,
                &stop_failures,
                &terminal_reload_failures,
            ));
        }
        return Ok(());
    }

    store
        .stop_rebalance_for_id(Some(rebalance_id))
        .await
        .map_err(|err| format!("local stop_rebalance rollback for {rebalance_id} failed: {err}"))?;
    store
        .save_rebalance_stats_for_id(usize::MAX, RebalSaveOpt::StoppedAt, rebalance_id)
        .await
        .map_err(|err| format!("local rollback stop metadata save for {rebalance_id} failed: {err}"))?;
    Ok(())
}

async fn rollback_rebalance_start_for_admin(
    store: &Arc<ECStore>,
    notification_sys: Option<&NotificationSys>,
    rebalance_id: &str,
    start_err: &str,
    request_id: &str,
    actor: &str,
    remote_addr: &str,
) -> S3Result<()> {
    let rollback_result = rollback_cluster_rebalance_start(store, notification_sys, rebalance_id).await;
    let rollback_label = rollback_result_label(&rollback_result);
    match &rollback_result {
        Ok(_) => info!(
            event = EVENT_ADMIN_REBALANCE_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            action = "start",
            result = rollback_label,
            request_id = %request_id,
            actor = %actor,
            remote_addr = %remote_addr,
            rebalance_id = %rebalance_id,
            propagation_error = %start_err,
            "admin rebalance state"
        ),
        Err(rollback_err) => error!(
            event = EVENT_ADMIN_REBALANCE_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            action = "start",
            result = rollback_label,
            request_id = %request_id,
            actor = %actor,
            remote_addr = %remote_addr,
            rebalance_id = %rebalance_id,
            propagation_error = %start_err,
            rollback_error = %rollback_err,
            "admin rebalance state"
        ),
    }

    Err(rebalance_internal_error(rebalance_start_rollback_error(start_err, &rollback_result)))
}

pub fn register_rebalance_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/rebalance/start").as_str(),
        AdminOperation(&RebalanceStart {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/rebalance/status").as_str(),
        AdminOperation(&RebalanceStatus {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/rebalance/stop").as_str(),
        AdminOperation(&RebalanceStop {}),
    )?;

    Ok(())
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RebalanceResp {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RebalPoolProgress {
    #[serde(rename = "objects")]
    pub num_objects: u64,
    #[serde(rename = "versions")]
    pub num_versions: u64,
    #[serde(rename = "bytes")]
    pub bytes: u64,
    #[serde(rename = "remainingBuckets")]
    pub remaining_buckets: usize,
    #[serde(rename = "bucket")]
    pub bucket: String,
    #[serde(rename = "object")]
    pub object: String,
    #[serde(rename = "elapsed")]
    pub elapsed: u64,
    #[serde(rename = "eta")]
    pub eta: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RebalancePoolStatus {
    #[serde(rename = "id")]
    pub id: usize, // Pool index (zero-based)
    #[serde(rename = "status")]
    pub status: String, // Active if rebalance is running, empty otherwise
    #[serde(rename = "stopping")]
    pub stopping: bool, // Stop requested but worker terminal acknowledgement not yet persisted
    #[serde(rename = "used")]
    pub used: f64, // Fraction of used space in range 0.0..=1.0
    #[serde(rename = "lastError")]
    pub last_error: Option<String>, // Last rebalance error message for this pool
    #[serde(rename = "cleanupWarnings")]
    pub cleanup_warnings: RebalanceCleanupWarnings,
    #[serde(rename = "progress")]
    pub progress: Option<RebalPoolProgress>, // None when rebalance is not running
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RebalanceStopPropagationStatus {
    #[serde(rename = "lastAttemptAt", with = "offsetdatetime_rfc3339")]
    pub last_attempt_at: Option<OffsetDateTime>,
    #[serde(rename = "failedPeers")]
    pub failed_peers: Vec<String>,
    #[serde(rename = "terminalReloadAttemptAt", with = "offsetdatetime_rfc3339")]
    pub terminal_reload_attempt_at: Option<OffsetDateTime>,
    #[serde(rename = "terminalReloadFailedPeers")]
    pub terminal_reload_failed_peers: Vec<String>,
    #[serde(rename = "pendingTerminalReload")]
    pub pending_terminal_reload: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RebalanceAdminStatus {
    pub id: String, // Identifies the ongoing rebalance operation by a UUID
    #[serde(rename = "pools")]
    pub pools: Vec<RebalancePoolStatus>, // Contains all pools, including inactive
    #[serde(rename = "stoppedAt", with = "offsetdatetime_rfc3339")]
    pub stopped_at: Option<OffsetDateTime>, // Optional timestamp when rebalance was stopped
    #[serde(rename = "stopPropagation")]
    pub stop_propagation: RebalanceStopPropagationStatus,
}

fn calculate_rebalance_progress(
    now: OffsetDateTime,
    start_time: Option<OffsetDateTime>,
    terminal_time: Option<OffsetDateTime>,
    bytes: u64,
    target_bytes: f64,
) -> Option<(u64, u64)> {
    let start = start_time?;
    let reference = terminal_time.unwrap_or(now);
    let elapsed_secs = (reference - start).whole_seconds().max(0) as u64;

    if terminal_time.is_some() {
        return Some((elapsed_secs, 0));
    }

    if !target_bytes.is_finite() || bytes == 0 || target_bytes <= bytes as f64 {
        return Some((elapsed_secs, 0));
    }

    let remaining = target_bytes - bytes as f64;
    if remaining <= 0.0 {
        return Some((elapsed_secs, 0));
    }

    let eta_secs_f64 = remaining * elapsed_secs as f64 / bytes as f64;
    let eta_secs = Duration::try_from_secs_f64(eta_secs_f64).map_or(0, |duration| duration.as_secs());
    Some((elapsed_secs, eta_secs))
}

fn build_rebalance_pool_progress(
    now: OffsetDateTime,
    stop_time: Option<OffsetDateTime>,
    percent_free_goal: f64,
    ps: &crate::admin::storage_api::rebalance::RebalanceStats,
) -> Option<RebalPoolProgress> {
    let total_bytes_to_rebal = ps.init_capacity as f64 * percent_free_goal - ps.init_free_space as f64;
    let terminal_time = ps.info.end_time.or(stop_time);
    let (elapsed, eta) = calculate_rebalance_progress(now, ps.info.start_time, terminal_time, ps.bytes, total_bytes_to_rebal)?;

    Some(RebalPoolProgress {
        num_objects: ps.num_objects,
        num_versions: ps.num_versions,
        bytes: ps.bytes,
        remaining_buckets: rebalance_remaining_buckets(ps.buckets.len(), ps.rebalanced_buckets.len()),
        bucket: ps.bucket.clone(),
        object: ps.object.clone(),
        elapsed,
        eta,
    })
}

fn rebalance_used_pct(total: u64, available: u64) -> f64 {
    if total == 0 {
        return 0.0;
    }

    let bounded_available = available.min(total);
    (total - bounded_available) as f64 / total as f64
}

fn rebalance_remaining_buckets(buckets: usize, _rebalanced_buckets: usize) -> usize {
    buckets
}

fn rebalance_pool_used(disk_stats: &[DiskStat], idx: usize) -> f64 {
    let (total_space, available_space) = disk_stats
        .get(idx)
        .map(|stat| (stat.total_space, stat.available_space))
        .unwrap_or((0, 0));
    rebalance_used_pct(total_space, available_space)
}

fn build_rebalance_pool_statuses(
    now: OffsetDateTime,
    stop_time: Option<OffsetDateTime>,
    percent_free_goal: f64,
    pool_stats: &[crate::admin::storage_api::rebalance::RebalanceStats],
    disk_stats: &[DiskStat],
) -> Vec<RebalancePoolStatus> {
    pool_stats
        .iter()
        .enumerate()
        .map(|(i, ps)| {
            let mut status = RebalancePoolStatus {
                id: i,
                status: ps.info.status.to_string(),
                stopping: ps.info.stopping,
                used: rebalance_pool_used(disk_stats, i),
                last_error: ps.info.last_error.clone(),
                cleanup_warnings: ps.cleanup_warnings.clone(),
                progress: None,
            };

            if ps.participating {
                status.progress = build_rebalance_pool_progress(now, stop_time, percent_free_goal, ps);
            }

            status
        })
        .collect()
}

fn build_rebalance_stop_propagation_status(meta: &RebalanceMeta) -> RebalanceStopPropagationStatus {
    let record = meta
        .pool_stats
        .iter()
        .filter_map(|pool_stat| pool_stat.info.last_error.as_deref())
        .find_map(decode_rebalance_stop_propagation_record);

    if let Some(record) = record {
        let last_attempt_at = record.stop_attempt_at.or(meta.stopped_at);
        let terminal_reload_attempt_at = record.terminal_reload_attempt_at;
        return RebalanceStopPropagationStatus {
            pending_terminal_reload: last_attempt_at.is_some() && terminal_reload_attempt_at.is_none(),
            last_attempt_at,
            failed_peers: record.stop_failures,
            terminal_reload_attempt_at,
            terminal_reload_failed_peers: record.terminal_reload_failures,
        };
    }

    RebalanceStopPropagationStatus {
        last_attempt_at: meta.stopped_at,
        pending_terminal_reload: false,
        ..Default::default()
    }
}

fn build_rebalance_admin_status(now: OffsetDateTime, disk_stats: &[DiskStat], meta: &RebalanceMeta) -> RebalanceAdminStatus {
    let stop_time = meta.stopped_at;
    RebalanceAdminStatus {
        id: meta.id.clone(),
        stopped_at: meta.stopped_at,
        pools: build_rebalance_pool_statuses(now, stop_time, meta.percent_free_goal, &meta.pool_stats, disk_stats),
        stop_propagation: build_rebalance_stop_propagation_status(meta),
    }
}

pub struct RebalanceStart {}

#[async_trait::async_trait]
impl Operation for RebalanceStart {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let request_id = admin_request_id(&req.headers).unwrap_or_default().to_string();
        let remote_addr = admin_remote_addr(&req).unwrap_or_default();
        info!(
            event = EVENT_ADMIN_REBALANCE_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            action = "start",
            state = "requested",
            request_id = %request_id,
            remote_addr = %remote_addr,
            "admin rebalance state"
        );

        let Some(input_cred) = req.credentials.as_ref() else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };
        let actor = MaskedAccessKey(&input_cred.access_key).to_string();

        authorize_admin_request(&req, vec![Action::AdminAction(AdminAction::RebalanceAdminAction)]).await?;

        if rebalance_query_present(&req.uri) {
            log_rebalance_request_rejected("start", "invalid_query_parameters", &request_id, &actor, &remote_addr);
            return Err(s3_error!(InvalidArgument, "rebalance start does not accept query parameters"));
        }

        let Some(store) = object_store_from_extensions(&req.extensions) else {
            return Err(s3_error!(InternalError, "object layer is not initialized"));
        };

        if store.pools.len() == 1 {
            log_rebalance_request_rejected("start", "single_pool_not_supported", &request_id, &actor, &remote_addr);
            return Err(s3_error!(NotImplemented));
        }

        if store.is_decommission_running().await {
            log_rebalance_request_rejected("start", "decommission_in_progress", &request_id, &actor, &remote_addr);
            return Err(s3_error!(InvalidRequest, "cannot start rebalance while decommission is in progress"));
        }

        if store.is_rebalance_conflicting_with_decommission().await {
            log_rebalance_request_rejected("start", "rebalance_already_in_progress", &request_id, &actor, &remote_addr);
            return Err(s3_error!(OperationAborted, "rebalance is already in progress"));
        }

        let bucket_infos = store
            .list_bucket(&BucketOptions::default())
            .await
            .map_err(|e| s3_error!(InternalError, "failed to list buckets for rebalance: {}", e))?;

        let buckets: Vec<String> = bucket_infos.into_iter().map(|bucket| bucket.name).collect();

        let id = match store.init_rebalance_start(buckets).await {
            Ok(id) => id,
            Err(StorageError::DecommissionAlreadyRunning) => {
                log_rebalance_request_rejected("start", "decommission_in_progress", &request_id, &actor, &remote_addr);
                return Err(s3_error!(InvalidRequest, "cannot start rebalance while decommission is in progress"));
            }
            Err(StorageError::RebalanceAlreadyRunning) => {
                log_rebalance_request_rejected("start", "rebalance_already_in_progress", &request_id, &actor, &remote_addr);
                return Err(s3_error!(OperationAborted, "rebalance is already in progress"));
            }
            Err(e) => {
                return Err(s3_error!(InternalError, "failed to initialize rebalance: {}", e));
            }
        };

        info!(
            event = EVENT_ADMIN_REBALANCE_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            action = "start",
            state = "metadata_initialized",
            request_id = %request_id,
            actor = %actor,
            remote_addr = %remote_addr,
            rebalance_id = %id,
            "admin rebalance state"
        );
        let notification_sys = current_notification_system();
        for step in rebalance_start_steps(notification_sys.is_some()) {
            match step {
                RebalanceStartStep::PropagateFence => {
                    if let Some(notification_sys) = notification_sys.as_ref() {
                        info!(
                            event = EVENT_ADMIN_REBALANCE_STATE,
                            component = LOG_COMPONENT_ADMIN,
                            subsystem = LOG_SUBSYSTEM_REBALANCE,
                            action = "start",
                            state = "fence_propagation_started",
                            request_id = %request_id,
                            actor = %actor,
                            remote_addr = %remote_addr,
                            rebalance_id = %id,
                            "admin rebalance state"
                        );
                        if let Err(err) = notification_sys.load_rebalance_meta(false).await {
                            error!(
                                event = EVENT_ADMIN_REBALANCE_STATE,
                                component = LOG_COMPONENT_ADMIN,
                                subsystem = LOG_SUBSYSTEM_REBALANCE,
                                action = "start",
                                result = "fence_propagation_failed",
                                request_id = %request_id,
                                actor = %actor,
                                remote_addr = %remote_addr,
                                rebalance_id = %id,
                                error = %err,
                                "admin rebalance state"
                            );

                            let start_err = err.to_string();
                            rollback_rebalance_start_for_admin(
                                &store,
                                Some(notification_sys),
                                &id,
                                &start_err,
                                &request_id,
                                &actor,
                                &remote_addr,
                            )
                            .await?;
                        }
                    }
                }
                RebalanceStartStep::StartLocal => {
                    if let Err(err) = store.start_rebalance_for_id(&id).await {
                        error!(
                            event = EVENT_ADMIN_REBALANCE_STATE,
                            component = LOG_COMPONENT_ADMIN,
                            subsystem = LOG_SUBSYSTEM_REBALANCE,
                            action = "start",
                            result = "local_start_failed",
                            request_id = %request_id,
                            actor = %actor,
                            remote_addr = %remote_addr,
                            rebalance_id = %id,
                            error = %err,
                            "admin rebalance state"
                        );

                        let start_err = err.to_string();
                        if let Err(rollback_err) = store.rollback_rebalance_start_for_id(Some(&id), start_err.clone()).await {
                            error!(
                                event = EVENT_ADMIN_REBALANCE_STATE,
                                component = LOG_COMPONENT_ADMIN,
                                subsystem = LOG_SUBSYSTEM_REBALANCE,
                                action = "start",
                                result = "local_start_rollback_failed",
                                request_id = %request_id,
                                actor = %actor,
                                remote_addr = %remote_addr,
                                rebalance_id = %id,
                                start_error = %start_err,
                                rollback_error = %rollback_err,
                                "admin rebalance state"
                            );
                            return Err(rebalance_internal_error(format!(
                                "failed to start rebalance after metadata initialized for {id}; rollback failed: {rollback_err}"
                            )));
                        }

                        info!(
                            event = EVENT_ADMIN_REBALANCE_STATE,
                            component = LOG_COMPONENT_ADMIN,
                            subsystem = LOG_SUBSYSTEM_REBALANCE,
                            action = "start",
                            result = "local_start_rollback_success",
                            request_id = %request_id,
                            actor = %actor,
                            remote_addr = %remote_addr,
                            rebalance_id = %id,
                            start_error = %start_err,
                            "admin rebalance state"
                        );
                        if let Some(notification_sys) = notification_sys.as_ref() {
                            let terminal_reload_attempt_at = OffsetDateTime::now_utc();
                            let terminal_reload_failures = match notification_sys.load_rebalance_meta_failures(false).await {
                                Ok(failures) => failures,
                                Err(err) => vec![format!("terminal rebalance reload rollback for {id} failed: {err}")],
                            };
                            if !terminal_reload_failures.is_empty() {
                                let record = RebalanceStopPropagationRecord {
                                    stop_attempt_at: None,
                                    stop_failures: Vec::new(),
                                    terminal_reload_attempt_at: Some(terminal_reload_attempt_at),
                                    terminal_reload_failures: terminal_reload_failures.clone(),
                                };
                                store.record_rebalance_stop_propagation(&id, record).await.map_err(|err| {
                                    rebalance_internal_error(format!(
                                        "failed to persist rebalance local-start rollback propagation metadata: {err}"
                                    ))
                                })?;
                                return Err(rebalance_internal_error(format!(
                                    "failed to start rebalance after metadata initialized for {}; local metadata was finalized as failed, but terminal peer reload was incomplete: {}",
                                    id,
                                    rebalance_rollback_terminal_reload_failure_message(&id, &terminal_reload_failures)
                                )));
                            }
                        }
                        return Err(rebalance_internal_error(format!(
                            "failed to start rebalance after metadata initialized for {id}; local metadata was finalized as failed: {start_err}"
                        )));
                    }
                }
                RebalanceStartStep::PropagateWorkers => {
                    if let Some(notification_sys) = notification_sys.as_ref() {
                        info!(
                            event = EVENT_ADMIN_REBALANCE_STATE,
                            component = LOG_COMPONENT_ADMIN,
                            subsystem = LOG_SUBSYSTEM_REBALANCE,
                            action = "start",
                            state = "worker_propagation_started",
                            request_id = %request_id,
                            actor = %actor,
                            remote_addr = %remote_addr,
                            rebalance_id = %id,
                            "admin rebalance state"
                        );
                        if let Err(err) = notification_sys.load_rebalance_meta(true).await {
                            error!(
                                event = EVENT_ADMIN_REBALANCE_STATE,
                                component = LOG_COMPONENT_ADMIN,
                                subsystem = LOG_SUBSYSTEM_REBALANCE,
                                action = "start",
                                result = "worker_propagation_failed",
                                request_id = %request_id,
                                actor = %actor,
                                remote_addr = %remote_addr,
                                rebalance_id = %id,
                                error = %err,
                                "admin rebalance state"
                            );

                            let start_err = err.to_string();
                            rollback_rebalance_start_for_admin(
                                &store,
                                Some(notification_sys),
                                &id,
                                &start_err,
                                &request_id,
                                &actor,
                                &remote_addr,
                            )
                            .await?;
                        }
                    }
                }
            }
        }

        if notification_sys.is_some() {
            info!(
                event = EVENT_ADMIN_REBALANCE_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_REBALANCE,
                action = "start",
                state = "propagation_completed",
                result = "success",
                request_id = %request_id,
                actor = %actor,
                remote_addr = %remote_addr,
                rebalance_id = %id,
                "admin rebalance state"
            );
        }

        let resp = RebalanceResp { id };
        let data = serde_json::to_string(&resp)
            .map_err(|e| s3_error!(InternalError, "failed to serialize rebalance start response: {}", e))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

// RebalanceStatus
pub struct RebalanceStatus {}

#[async_trait::async_trait]
impl Operation for RebalanceStatus {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let request_id = admin_request_id(&req.headers).unwrap_or_default().to_string();
        let remote_addr = admin_remote_addr(&req).unwrap_or_default();
        info!(
            event = EVENT_ADMIN_REBALANCE_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            action = "status",
            state = "requested",
            request_id = %request_id,
            remote_addr = %remote_addr,
            "admin rebalance state"
        );

        let Some(input_cred) = req.credentials.as_ref() else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };
        let actor = MaskedAccessKey(&input_cred.access_key).to_string();

        authorize_admin_request(&req, vec![Action::AdminAction(AdminAction::RebalanceAdminAction)]).await?;

        let Some(store) = object_store_from_extensions(&req.extensions) else {
            return Err(s3_error!(InternalError, "object layer is not initialized"));
        };

        if store.pools.is_empty() {
            return Err(s3_error!(InternalError, "no storage pools are available"));
        }

        let first_pool = store
            .pools
            .first()
            .cloned()
            .ok_or_else(|| s3_error!(InternalError, "no storage pools are available"))?;

        let mut meta = RebalanceMeta::new();
        if let Err(err) = meta.load(first_pool).await {
            if err == StorageError::ConfigNotFound {
                log_rebalance_request_rejected("status", "rebalance_not_started", &request_id, &actor, &remote_addr);
                return Err(s3_error!(NoSuchResource, "pool rebalance is not started"));
            }

            return Err(s3_error!(InternalError, "failed to load rebalance metadata from pool 0: {}", err));
        }

        // Compute disk usage percentage
        let si = StorageAdminApi::storage_info(store.as_ref()).await;
        let mut disk_stats = vec![DiskStat::default(); store.pools.len()];

        for disk in si.disks.iter() {
            if disk.pool_index < 0 || disk_stats.len() <= disk.pool_index as usize {
                continue;
            }
            disk_stats[disk.pool_index as usize].available_space += disk.available_space;
            disk_stats[disk.pool_index as usize].total_space += disk.total_space;
        }

        let now = OffsetDateTime::now_utc();
        let admin_status = build_rebalance_admin_status(now, &disk_stats, &meta);

        let data = serde_json::to_string(&admin_status)
            .map_err(|e| s3_error!(InternalError, "failed to serialize rebalance status response: {}", e))?;
        info!(
            event = EVENT_ADMIN_REBALANCE_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            action = "status",
            result = "success",
            request_id = %request_id,
            actor = %actor,
            remote_addr = %remote_addr,
            rebalance_id = %admin_status.id,
            pool_count = admin_status.pools.len(),
            cleanup_warning_count = admin_status.pools.iter().map(|pool| pool.cleanup_warnings.count).sum::<u64>(),
            "admin rebalance state"
        );
        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

async fn rebalance_stop_target_id(store: &Arc<ECStore>) -> S3Result<Option<String>> {
    store
        .prepare_rebalance_stop()
        .await
        .map_err(|e| s3_error!(InternalError, "failed to prepare rebalance metadata for stop: {}", e))
}

async fn stop_rebalance_admission_first(
    store: &Arc<ECStore>,
    notification_sys: Option<&NotificationSys>,
    expected_rebalance_id: &str,
) -> S3Result<Vec<String>> {
    // prepare_rebalance_stop already closed admission for this exact run.

    if let Some(notification_sys) = notification_sys {
        return notification_sys
            .stop_rebalance_failures(Some(expected_rebalance_id))
            .await
            .map_err(|e| s3_error!(InternalError, "failed to stop rebalance via notification system: {}", e));
    }

    store
        .stop_rebalance_for_id(Some(expected_rebalance_id))
        .await
        .map_err(|e| s3_error!(InternalError, "failed to stop rebalance: {}", e))?;
    store
        .save_rebalance_stats_for_id(usize::MAX, RebalSaveOpt::StoppedAt, expected_rebalance_id)
        .await
        .map_err(|e| s3_error!(InternalError, "failed to persist rebalance stop metadata: {}", e))?;
    Ok(Vec::new())
}

// RebalanceStop
pub struct RebalanceStop {}

#[async_trait::async_trait]
impl Operation for RebalanceStop {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let request_id = admin_request_id(&req.headers).unwrap_or_default().to_string();
        let remote_addr = admin_remote_addr(&req).unwrap_or_default();
        info!(
            event = EVENT_ADMIN_REBALANCE_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            action = "stop",
            state = "requested",
            request_id = %request_id,
            remote_addr = %remote_addr,
            "admin rebalance state"
        );

        let Some(input_cred) = req.credentials.as_ref() else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };
        let actor = MaskedAccessKey(&input_cred.access_key).to_string();

        authorize_admin_request(&req, vec![Action::AdminAction(AdminAction::RebalanceAdminAction)]).await?;

        if rebalance_query_present(&req.uri) {
            log_rebalance_request_rejected("stop", "invalid_query_parameters", &request_id, &actor, &remote_addr);
            return Err(s3_error!(InvalidArgument, "rebalance stop does not accept query parameters"));
        }

        let Some(store) = object_store_from_extensions(&req.extensions) else {
            return Err(s3_error!(InternalError, "object layer is not initialized"));
        };

        let Some(expected_rebalance_id) = rebalance_stop_target_id(&store).await? else {
            log_rebalance_request_rejected("stop", "rebalance_not_started", &request_id, &actor, &remote_addr);
            return Err(s3_error!(NoSuchResource, "pool rebalance is not started"));
        };

        let notification_sys = current_notification_system();
        let stop_attempt_at = OffsetDateTime::now_utc();
        let stop_failures =
            stop_rebalance_admission_first(&store, notification_sys.as_deref(), expected_rebalance_id.as_str()).await?;

        info!(
            event = EVENT_ADMIN_REBALANCE_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            action = "stop",
            state = "local_stop_persisted",
            result = "success",
            request_id = %request_id,
            actor = %actor,
            remote_addr = %remote_addr,
            "admin rebalance state"
        );

        let mut terminal_reload_attempt_at = None;
        let mut terminal_reload_failures = Vec::new();
        if let Some(notification_sys) = notification_sys {
            info!(
                event = EVENT_ADMIN_REBALANCE_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_REBALANCE,
                action = "stop",
                state = "propagation_started",
                request_id = %request_id,
                actor = %actor,
                remote_addr = %remote_addr,
                "admin rebalance state"
            );
            terminal_reload_attempt_at = Some(OffsetDateTime::now_utc());
            match notification_sys.load_rebalance_meta_failures(false).await {
                Ok(failures) => {
                    terminal_reload_failures = failures;
                    if terminal_reload_failures.is_empty() {
                        info!(
                            event = EVENT_ADMIN_REBALANCE_STATE,
                            component = LOG_COMPONENT_ADMIN,
                            subsystem = LOG_SUBSYSTEM_REBALANCE,
                            action = "stop",
                            state = "propagation_completed",
                            result = "success",
                            request_id = %request_id,
                            actor = %actor,
                            remote_addr = %remote_addr,
                            "admin rebalance state"
                        );
                    }
                }
                Err(err) => {
                    terminal_reload_failures.push(format!("terminal rebalance reload propagation failed: {err}"));
                }
            }
        }

        if !stop_failures.is_empty() || !terminal_reload_failures.is_empty() {
            let record = RebalanceStopPropagationRecord {
                stop_attempt_at: Some(stop_attempt_at),
                stop_failures: stop_failures.clone(),
                terminal_reload_attempt_at,
                terminal_reload_failures: terminal_reload_failures.clone(),
            };
            store
                .record_rebalance_stop_propagation(expected_rebalance_id.as_str(), record)
                .await
                .map_err(|e| s3_error!(InternalError, "failed to persist rebalance stop propagation metadata: {}", e))?;

            error!(
                event = EVENT_ADMIN_REBALANCE_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_REBALANCE,
                action = "stop",
                result = "propagation_failed",
                request_id = %request_id,
                actor = %actor,
                remote_addr = %remote_addr,
                stop_failure_count = stop_failures.len(),
                terminal_reload_failure_count = terminal_reload_failures.len(),
                "admin rebalance state"
            );
            let mut failures = Vec::new();
            failures.extend(stop_failures);
            failures.extend(terminal_reload_failures);
            return Err(s3_error!(
                InternalError,
                "rebalance stop propagation incomplete after local stop was persisted: {}",
                failures.join("; ")
            ));
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        header.insert(CONTENT_LENGTH, HeaderValue::from_static("0"));
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

mod offsetdatetime_rfc3339 {
    use serde::{self, Deserialize, Deserializer, Serializer};
    use time::{OffsetDateTime, format_description::well_known::Rfc3339};

    pub fn serialize<S>(dt: &Option<OffsetDateTime>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match dt {
            Some(dt) => {
                let s = dt.format(&Rfc3339).map_err(serde::ser::Error::custom)?;
                serializer.serialize_some(&s)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<OffsetDateTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt = Option::<String>::deserialize(deserializer)?;
        match opt {
            Some(s) => {
                let dt = OffsetDateTime::parse(&s, &Rfc3339).map_err(serde::de::Error::custom)?;
                Ok(Some(dt))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod rebalance_handler_tests {
    use super::build_rebalance_pool_progress;
    use super::calculate_rebalance_progress;
    use super::{
        Body, HeaderMap, Method, Operation, Params, RebalPoolProgress, RebalanceAdminStatus, RebalancePoolStatus, RebalanceStart,
        RebalanceStartStep, RebalanceStatus, RebalanceStop, RebalanceStopPropagationStatus, S3ErrorCode, S3Request, Uri,
        build_rebalance_admin_status, build_rebalance_pool_statuses, build_rebalance_stop_propagation_status,
        rebalance_pool_used, rebalance_query_present, rebalance_remaining_buckets, rebalance_rollback_failure_message,
        rebalance_rollback_stop_failure_message, rebalance_start_rollback_error, rebalance_start_steps, rebalance_stop_target_id,
        rebalance_used_pct, rollback_result_label, stop_rebalance_admission_first,
    };
    use crate::admin::storage_api::rebalance::{
        DiskStat, RebalSaveOpt, RebalStatus, RebalanceCleanupWarningEntry, RebalanceCleanupWarnings, RebalanceInfo,
        RebalanceMeta, RebalanceStats, RebalanceStopPropagationRecord, encode_rebalance_stop_propagation_record,
    };
    use time::OffsetDateTime;

    fn credential_less_request(method: Method, uri: &'static str) -> S3Request<Body> {
        S3Request {
            input: Body::empty(),
            method,
            uri: Uri::from_static(uri),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    async fn assert_missing_credentials(operation: &dyn Operation, method: Method, uri: &'static str) {
        let err = operation
            .call(credential_less_request(method, uri), Params::new())
            .await
            .expect_err("a rebalance admin request without credentials must fail");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some("authentication required"));
    }

    fn started_rebalance_meta(id: &str) -> RebalanceMeta {
        RebalanceMeta {
            id: id.to_string(),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    start_time: Some(OffsetDateTime::now_utc()),
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn real_admin_stop_cancels_paused_entry_before_waiting_for_activation_gate() {
        const REBALANCE_ID: &str = "admin-stop-paused-entry";
        let mut fixture =
            crate::admin::storage_api::ecstore_rebalance::test_util::PausedRebalanceEntryTestFixture::new(REBALANCE_ID).await;
        fixture.wait_until_entry_paused().await;

        let stop_store = fixture.store();
        let mut stop_task = tokio::spawn(async move {
            let expected_rebalance_id = rebalance_stop_target_id(&stop_store)
                .await
                .expect("admin stop target resolution should succeed")
                .expect("the active rebalance should remain stoppable");
            stop_rebalance_admission_first(&stop_store, None, expected_rebalance_id.as_str()).await
        });
        fixture.wait_until_admission_cancelled().await;
        fixture.wait_until_stop_waiting_for_entry().await;
        assert!(!stop_task.is_finished(), "admin stop must wait for the paused entry guard to drain");

        fixture.release_entry();
        fixture.assert_entry_cancelled().await;
        let stop_failures = tokio::time::timeout(std::time::Duration::from_secs(5), &mut stop_task)
            .await
            .expect("admin stop should finish after the entry guard drains")
            .expect("admin stop task should not panic")
            .expect("admin stop should persist the terminal state");
        assert!(stop_failures.is_empty());
        assert!(!fixture.store().is_rebalance_conflicting_with_decommission().await);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn real_admin_stop_accepts_same_run_terminalization_after_prepare() {
        const REBALANCE_ID: &str = "admin-stop-terminal-after-prepare";
        const REPLACEMENT_ID: &str = "admin-stop-replacement";
        let (_temp_dirs, store) =
            crate::admin::storage_api::ecstore_rebalance::test_util::test_store_with_persisted_rebalance_meta(
                started_rebalance_meta(REBALANCE_ID),
            )
            .await;
        let terminal_barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let worker_barrier = std::sync::Arc::clone(&terminal_barrier);
        let worker_store = std::sync::Arc::clone(&store);
        let terminal_task = tokio::spawn(async move {
            worker_barrier.wait().await;
            {
                let mut rebalance_meta = worker_store.rebalance_meta.write().await;
                let meta = rebalance_meta
                    .as_mut()
                    .expect("the prepared rebalance metadata should remain installed");
                assert_eq!(meta.id, REBALANCE_ID);
                let pool = meta
                    .pool_stats
                    .first_mut()
                    .expect("the prepared rebalance should have a pool");
                pool.info.status = RebalStatus::Stopped;
                pool.info.end_time = Some(OffsetDateTime::now_utc());
            }
            worker_store
                .save_rebalance_stats_for_id(0, RebalSaveOpt::Stats, REBALANCE_ID)
                .await
        });

        let expected_rebalance_id = rebalance_stop_target_id(&store)
            .await
            .expect("admin stop target resolution should succeed")
            .expect("the active rebalance should remain stoppable");
        assert_eq!(expected_rebalance_id, REBALANCE_ID);
        let cancel = store
            .rebalance_meta
            .read()
            .await
            .as_ref()
            .and_then(|meta| meta.cancel.clone())
            .expect("prepare should install the admission cancellation token");
        assert!(cancel.is_cancelled());

        terminal_barrier.wait().await;
        tokio::time::timeout(std::time::Duration::from_secs(30), terminal_task)
            .await
            .expect("worker terminalization should finish after the barrier opens")
            .expect("worker terminalization task should not panic")
            .expect("worker terminalization should persist");
        assert!(!store.is_rebalance_conflicting_with_decommission().await);

        *store.rebalance_meta.write().await = None;
        store
            .load_rebalance_meta()
            .await
            .expect("the worker terminal state should reload before the final stop");
        {
            let terminal = store.rebalance_meta.read().await;
            let terminal = terminal.as_ref().expect("the worker terminal state should remain persisted");
            assert_eq!(terminal.id, REBALANCE_ID);
            assert_eq!(terminal.pool_stats[0].info.status, RebalStatus::Stopped);
        }

        let stop_failures = stop_rebalance_admission_first(&store, None, expected_rebalance_id.as_str())
            .await
            .expect("same-run terminalization after prepare should be a successful stop");
        assert!(stop_failures.is_empty());

        *store.rebalance_meta.write().await = Some(started_rebalance_meta(REPLACEMENT_ID));
        let error = stop_rebalance_admission_first(&store, None, expected_rebalance_id.as_str())
            .await
            .expect_err("the prepared stop must not mutate a replacement run");
        assert!(error.to_string().contains(REBALANCE_ID));
        let replacement = store.rebalance_meta.read().await;
        let replacement = replacement.as_ref().expect("the replacement run should remain installed");
        assert_eq!(replacement.id, REPLACEMENT_ID);
        assert_eq!(replacement.pool_stats[0].info.status, RebalStatus::Started);
        assert!(replacement.cancel.is_none());
        assert!(replacement.stopped_at.is_none());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn real_admin_stop_loads_persisted_active_rebalance_from_cold_memory() {
        const REBALANCE_ID: &str = "admin-stop-cold-memory";
        let (_temp_dirs, store) =
            crate::admin::storage_api::ecstore_rebalance::test_util::test_store_with_persisted_rebalance_meta(
                started_rebalance_meta(REBALANCE_ID),
            )
            .await;
        *store.rebalance_meta.write().await = None;
        assert!(store.current_rebalance_id().await.is_none());

        let expected_rebalance_id = rebalance_stop_target_id(&store)
            .await
            .expect("admin stop should load persisted rebalance metadata")
            .expect("persisted active rebalance should be stoppable");
        assert_eq!(expected_rebalance_id, REBALANCE_ID);

        let stop_failures = stop_rebalance_admission_first(&store, None, expected_rebalance_id.as_str())
            .await
            .expect("admin stop should persist the terminal state after a cold load");
        assert!(stop_failures.is_empty());
        assert!(!store.is_rebalance_conflicting_with_decommission().await);

        *store.rebalance_meta.write().await = None;
        store
            .load_rebalance_meta()
            .await
            .expect("the persisted terminal rebalance metadata should remain readable");
        assert_eq!(store.current_rebalance_id().await.as_deref(), Some(REBALANCE_ID));
        assert!(!store.is_rebalance_conflicting_with_decommission().await);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn real_admin_stop_refreshes_persisted_active_over_stale_inactive_memory() {
        const PERSISTED_REBALANCE_ID: &str = "admin-stop-persisted-active";
        const STALE_REBALANCE_ID: &str = "admin-stop-stale-terminal";
        let (_temp_dirs, store) =
            crate::admin::storage_api::ecstore_rebalance::test_util::test_store_with_persisted_rebalance_meta(
                started_rebalance_meta(PERSISTED_REBALANCE_ID),
            )
            .await;
        *store.rebalance_meta.write().await = Some(RebalanceMeta {
            id: STALE_REBALANCE_ID.to_string(),
            stopped_at: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        });
        assert_eq!(store.current_rebalance_id().await.as_deref(), Some(STALE_REBALANCE_ID));
        assert!(!store.is_rebalance_conflicting_with_decommission().await);

        let expected_rebalance_id = rebalance_stop_target_id(&store)
            .await
            .expect("admin stop should refresh stale inactive local metadata")
            .expect("persisted active rebalance should replace the stale local terminal state");
        assert_eq!(expected_rebalance_id, PERSISTED_REBALANCE_ID);

        let stop_failures = stop_rebalance_admission_first(&store, None, expected_rebalance_id.as_str())
            .await
            .expect("admin stop should persist the refreshed run's terminal state");
        assert!(stop_failures.is_empty());

        *store.rebalance_meta.write().await = None;
        store
            .load_rebalance_meta()
            .await
            .expect("the refreshed run's persisted terminal metadata should remain readable");
        assert_eq!(store.current_rebalance_id().await.as_deref(), Some(PERSISTED_REBALANCE_ID));
        assert!(!store.is_rebalance_conflicting_with_decommission().await);
    }

    #[test]
    fn test_calculate_rebalance_progress_running() {
        let start = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        let now = OffsetDateTime::from_unix_timestamp(1_050).unwrap();

        let (elapsed, eta) = calculate_rebalance_progress(now, Some(start), None, 100, 200.0).unwrap();

        assert_eq!(elapsed, 50);
        assert_eq!(eta, 50);
    }

    #[test]
    fn test_rebalance_start_rollback_error_reports_successful_rollback() {
        let rollback_result = Ok(());
        let message = rebalance_start_rollback_error("peer a failed", &rollback_result);

        assert!(message.contains("failed to propagate rebalance start: peer a failed"));
        assert!(message.contains("rollback result: rollback_success"));
    }

    #[test]
    fn test_rebalance_start_rollback_error_reports_partial_peer_rollback() {
        let rollback_result = Err("peer b stop_rebalance failed: timeout".to_string());
        let message = rebalance_start_rollback_error("peer a failed", &rollback_result);

        assert_eq!(rollback_result_label(&rollback_result), "rollback_partial");
        assert!(message.contains("failed to propagate rebalance start: peer a failed"));
        assert!(message.contains("rollback result: rollback_partial"));
        assert!(message.contains("rollback error: peer b stop_rebalance failed: timeout"));
    }

    #[test]
    fn test_rebalance_rollback_stop_failure_message_lists_failures() {
        let failures = vec![
            "peer a stop_rebalance failed: timeout".to_string(),
            "peer b stop_rebalance failed: unavailable".to_string(),
        ];

        let message = rebalance_rollback_stop_failure_message("rebalance-id", &failures);

        assert!(message.contains("cluster stop_rebalance rollback for rebalance-id partial"));
        assert!(message.contains("peer a stop_rebalance failed: timeout"));
        assert!(message.contains("peer b stop_rebalance failed: unavailable"));
    }

    #[test]
    fn test_rebalance_rollback_failure_message_lists_stop_and_terminal_reload_failures() {
        let stop_failures = vec!["peer a stop_rebalance failed: timeout".to_string()];
        let terminal_reload_failures = vec!["peer b load_rebalance_meta(start=false) failed: unavailable".to_string()];

        let message = rebalance_rollback_failure_message("rebalance-id", &stop_failures, &terminal_reload_failures);

        assert!(message.contains("cluster stop_rebalance rollback for rebalance-id partial"));
        assert!(message.contains("peer a stop_rebalance failed: timeout"));
        assert!(message.contains("cluster terminal rebalance reload rollback for rebalance-id partial"));
        assert!(message.contains("peer b load_rebalance_meta(start=false) failed: unavailable"));
    }

    #[test]
    fn test_rebalance_query_present_detects_non_empty_query() {
        let uri = "/rustfs/admin/v3/rebalance/start?pool=0".parse().unwrap();

        assert!(rebalance_query_present(&uri));
    }

    #[test]
    fn test_rebalance_query_present_allows_no_or_empty_query() {
        let no_query = "/rustfs/admin/v3/rebalance/start".parse().unwrap();
        let empty_query = "/rustfs/admin/v3/rebalance/start?".parse().unwrap();

        assert!(!rebalance_query_present(&no_query));
        assert!(!rebalance_query_present(&empty_query));
    }

    #[test]
    fn test_rollback_result_label_reports_non_peer_failure_as_failed() {
        let rollback_result = Err("local stop_rebalance failed: disk error".to_string());

        assert_eq!(rollback_result_label(&rollback_result), "rollback_failed");
    }

    #[test]
    fn test_distributed_rebalance_start_fences_peers_before_workers() {
        assert_eq!(
            rebalance_start_steps(true),
            &[
                RebalanceStartStep::PropagateFence,
                RebalanceStartStep::StartLocal,
                RebalanceStartStep::PropagateWorkers
            ]
        );
    }

    #[test]
    fn test_local_rebalance_start_has_no_peer_propagation_steps() {
        assert_eq!(rebalance_start_steps(false), &[RebalanceStartStep::StartLocal]);
    }

    #[test]
    fn test_calculate_rebalance_progress_stopped_by_end_time() {
        let start = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        let terminal = OffsetDateTime::from_unix_timestamp(1_120).unwrap();

        let (elapsed, eta) = calculate_rebalance_progress(
            OffsetDateTime::from_unix_timestamp(1_200).unwrap(),
            Some(start),
            Some(terminal),
            100,
            200.0,
        )
        .unwrap();

        assert_eq!(elapsed, 120);
        assert_eq!(eta, 0);
    }

    #[test]
    fn test_calculate_rebalance_progress_invalid_target_is_zero_eta() {
        let start = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        let now = OffsetDateTime::from_unix_timestamp(1_010).unwrap();

        let (elapsed, eta) = calculate_rebalance_progress(now, Some(start), None, 100, f64::NAN).unwrap();

        assert_eq!(elapsed, 10);
        assert_eq!(eta, 0);
    }

    #[test]
    fn test_calculate_rebalance_progress_negative_target_is_zero_eta() {
        let start = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        let now = OffsetDateTime::from_unix_timestamp(1_010).unwrap();

        let (elapsed, eta) = calculate_rebalance_progress(now, Some(start), None, 100, -10.0).unwrap();

        assert_eq!(elapsed, 10);
        assert_eq!(eta, 0);
    }

    #[test]
    fn test_calculate_rebalance_progress_overflow_eta_is_zero() {
        let start = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        let now = OffsetDateTime::from_unix_timestamp(1_010).unwrap();

        let (elapsed, eta) = calculate_rebalance_progress(now, Some(start), None, 1, f64::MAX).unwrap();

        assert_eq!(elapsed, 10);
        assert_eq!(eta, 0);
    }

    #[test]
    fn test_calculate_rebalance_progress_no_start_time() {
        assert!(
            calculate_rebalance_progress(OffsetDateTime::from_unix_timestamp(1_000).unwrap(), None, None, 1, 100.0).is_none()
        );
    }

    #[test]
    fn test_build_rebalance_pool_progress_returns_none_without_start_time() {
        let ps = RebalanceStats {
            participating: true,
            info: RebalanceInfo {
                status: RebalStatus::Started,
                start_time: None,
                ..Default::default()
            },
            ..Default::default()
        };

        let progress = build_rebalance_pool_progress(OffsetDateTime::from_unix_timestamp(1_000).unwrap(), None, 0.3, &ps);
        assert!(progress.is_none());
    }

    #[test]
    fn test_build_rebalance_pool_progress_maps_fields_and_eta() {
        let ps = RebalanceStats {
            init_capacity: 1_000,
            init_free_space: 200,
            buckets: vec!["bucket-a".to_string(), "bucket-b".to_string(), "bucket-c".to_string()],
            rebalanced_buckets: vec!["bucket-a".to_string()],
            bucket: "bucket-b".to_string(),
            object: "obj-1".to_string(),
            num_objects: 3,
            num_versions: 5,
            bytes: 100,
            participating: true,
            info: RebalanceInfo {
                status: RebalStatus::Started,
                start_time: Some(OffsetDateTime::from_unix_timestamp(1_000).unwrap()),
                ..Default::default()
            },
            ..Default::default()
        };

        let progress = build_rebalance_pool_progress(OffsetDateTime::from_unix_timestamp(1_050).unwrap(), None, 0.3, &ps)
            .expect("progress should be generated");
        assert_eq!(progress.num_objects, 3);
        assert_eq!(progress.num_versions, 5);
        assert_eq!(progress.bytes, 100);
        assert_eq!(progress.remaining_buckets, 3);
        assert_eq!(progress.bucket, "bucket-b");
        assert_eq!(progress.object, "obj-1");
        assert_eq!(progress.elapsed, 50);
        assert_eq!(progress.eta, 0);
    }

    #[test]
    fn test_build_rebalance_pool_progress_stopped_uses_stop_time() {
        let ps = RebalanceStats {
            init_capacity: 1_000,
            init_free_space: 200,
            info: RebalanceInfo {
                status: RebalStatus::Started,
                start_time: Some(OffsetDateTime::from_unix_timestamp(1_000).unwrap()),
                ..Default::default()
            },
            participating: true,
            ..Default::default()
        };

        let stop_time = OffsetDateTime::from_unix_timestamp(1_200).unwrap();
        let progress = build_rebalance_pool_progress(stop_time, Some(stop_time), 0.3, &ps).expect("progress should be generated");

        assert_eq!(progress.elapsed, 200);
        assert_eq!(progress.eta, 0);
    }

    #[test]
    fn test_build_rebalance_pool_progress_prefers_info_end_time_over_stop_time() {
        let ps = RebalanceStats {
            init_capacity: 1_000,
            init_free_space: 200,
            info: RebalanceInfo {
                status: RebalStatus::Started,
                start_time: Some(OffsetDateTime::from_unix_timestamp(1_000).unwrap()),
                end_time: Some(OffsetDateTime::from_unix_timestamp(1_180).unwrap()),
                ..Default::default()
            },
            participating: true,
            ..Default::default()
        };

        let progress = build_rebalance_pool_progress(
            OffsetDateTime::from_unix_timestamp(1_300).unwrap(),
            Some(OffsetDateTime::from_unix_timestamp(1_250).unwrap()),
            0.3,
            &ps,
        )
        .expect("progress should be generated");

        assert_eq!(progress.elapsed, 180);
        assert_eq!(progress.eta, 0);
    }

    #[test]
    fn test_rebalance_used_pct_normal_and_zero_total() {
        assert_eq!(rebalance_used_pct(1_000, 650), 0.35);
        assert_eq!(rebalance_used_pct(0, 0), 0.0);
    }

    #[test]
    fn test_rebalance_used_pct_clamps_available_over_total() {
        assert_eq!(rebalance_used_pct(1_000, 1_500), 0.0);
    }

    #[test]
    fn test_rebalance_remaining_buckets_uses_pending_queue_len() {
        assert_eq!(rebalance_remaining_buckets(10, 7), 10);
        assert_eq!(rebalance_remaining_buckets(3, 10), 3);
    }

    #[test]
    fn test_rebalance_pool_used_defaults_to_zero_when_disk_stat_missing() {
        let disk_stats: Vec<DiskStat> = vec![];
        assert_eq!(rebalance_pool_used(&disk_stats, 0), 0.0);
    }

    #[test]
    fn test_build_rebalance_pool_statuses_tracks_progress_for_participants() {
        let pool_stats = vec![
            RebalanceStats {
                participating: true,
                init_capacity: 1_000,
                init_free_space: 200,
                num_objects: 2,
                num_versions: 2,
                bytes: 100,
                buckets: vec!["bucket-a".to_string(), "bucket-b".to_string()],
                rebalanced_buckets: vec!["bucket-a".to_string()],
                bucket: "bucket-b".to_string(),
                object: "obj-2".to_string(),
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(OffsetDateTime::from_unix_timestamp(1_000).unwrap()),
                    ..Default::default()
                },
                ..Default::default()
            },
            RebalanceStats {
                participating: false,
                info: RebalanceInfo {
                    status: RebalStatus::Completed,
                    start_time: Some(OffsetDateTime::from_unix_timestamp(1_000).unwrap()),
                    ..Default::default()
                },
                ..Default::default()
            },
        ];

        let disk_stats = vec![
            DiskStat {
                total_space: 1_000,
                available_space: 500,
            },
            DiskStat {
                total_space: 0,
                available_space: 0,
            },
        ];

        let statuses = build_rebalance_pool_statuses(
            OffsetDateTime::from_unix_timestamp(1_050).unwrap(),
            None,
            0.3,
            &pool_stats,
            &disk_stats,
        );

        assert_eq!(statuses.len(), 2);

        let active = &statuses[0];
        assert_eq!(active.id, 0);
        assert_eq!(active.status, "Started");
        assert_eq!(active.used, 0.5);
        assert_eq!(active.progress.as_ref().unwrap().bucket, "bucket-b");
        assert_eq!(active.progress.as_ref().unwrap().object, "obj-2");
        assert_eq!(active.progress.as_ref().unwrap().remaining_buckets, 2);

        let inactive = &statuses[1];
        assert_eq!(inactive.id, 1);
        assert_eq!(inactive.status, "Completed");
        assert_eq!(inactive.used, 0.0);
        assert!(inactive.progress.is_none());
    }

    #[test]
    fn test_build_rebalance_pool_statuses_uses_zero_used_for_missing_disk_stats() {
        let pool_stats = vec![
            RebalanceStats {
                participating: false,
                info: RebalanceInfo {
                    status: RebalStatus::Completed,
                    ..Default::default()
                },
                ..Default::default()
            },
            RebalanceStats {
                participating: true,
                init_capacity: 2_000,
                init_free_space: 400,
                num_objects: 1,
                num_versions: 1,
                bytes: 10,
                buckets: vec!["bucket-a".to_string()],
                rebalanced_buckets: vec![],
                bucket: "bucket-a".to_string(),
                object: "obj".to_string(),
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(OffsetDateTime::from_unix_timestamp(2_000).unwrap()),
                    ..Default::default()
                },
                ..Default::default()
            },
        ];

        let statuses =
            build_rebalance_pool_statuses(OffsetDateTime::from_unix_timestamp(2_010).unwrap(), None, 0.3, &pool_stats, &[]);

        assert_eq!(statuses[1].used, 0.0);
        assert!(statuses[1].progress.is_some());
    }

    #[test]
    fn test_build_rebalance_pool_statuses_reports_stopping() {
        let pool_stats = vec![RebalanceStats {
            participating: true,
            info: RebalanceInfo {
                status: RebalStatus::Started,
                stopping: true,
                start_time: Some(OffsetDateTime::from_unix_timestamp(2_000).unwrap()),
                ..Default::default()
            },
            ..Default::default()
        }];

        let statuses =
            build_rebalance_pool_statuses(OffsetDateTime::from_unix_timestamp(2_010).unwrap(), None, 0.3, &pool_stats, &[]);

        assert_eq!(statuses[0].status, "Started");
        assert!(statuses[0].stopping);
    }

    #[test]
    fn test_build_rebalance_pool_statuses_empty_inputs() {
        let statuses = build_rebalance_pool_statuses(
            OffsetDateTime::from_unix_timestamp(2_000).unwrap(),
            None,
            0.3,
            &[],
            &[DiskStat {
                total_space: 1_000,
                available_space: 500,
            }],
        );

        assert!(statuses.is_empty());
    }

    #[test]
    fn test_rebalance_status_serializes_new_fields() {
        let status = RebalanceAdminStatus {
            id: "id-1".to_string(),
            stopped_at: None,
            stop_propagation: RebalanceStopPropagationStatus::default(),
            pools: vec![RebalancePoolStatus {
                id: 0,
                status: "Started".to_string(),
                stopping: true,
                used: 0.5,
                last_error: Some("temporary error".to_string()),
                cleanup_warnings: RebalanceCleanupWarnings {
                    count: 1,
                    last_message: Some("cleanup warning".to_string()),
                    last_bucket: Some("bucket-a".to_string()),
                    last_object: Some("obj".to_string()),
                    last_at: Some(OffsetDateTime::from_unix_timestamp(1_001).unwrap()),
                    entries: vec![RebalanceCleanupWarningEntry {
                        bucket: "bucket-a".to_string(),
                        object: "obj".to_string(),
                        message: "cleanup warning".to_string(),
                        timestamp: Some(OffsetDateTime::from_unix_timestamp(1_001).unwrap()),
                    }],
                },
                progress: Some(RebalPoolProgress {
                    num_objects: 3,
                    num_versions: 5,
                    bytes: 1024,
                    remaining_buckets: 2,
                    bucket: "bucket-a".to_string(),
                    object: "obj".to_string(),
                    elapsed: 10,
                    eta: 20,
                }),
            }],
        };

        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"remainingBuckets\""));
        assert!(json.contains("\"lastError\""));
        assert!(json.contains("\"cleanupWarnings\""));
        assert!(json.contains("\"stopping\":true"));
        assert!(json.contains("\"lastMsg\":\"cleanup warning\""));
        assert!(json.contains("\"entries\""));
        assert!(json.contains("\"message\":\"cleanup warning\""));
        assert!(json.contains("\"stoppedAt\":null"));
        assert!(json.contains("\"stopPropagation\""));
        assert!(json.contains("\"pendingTerminalReload\":false"));
    }

    #[test]
    fn test_build_rebalance_admin_status_is_stable_for_same_persisted_meta() {
        let started = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        let disk_stats = vec![
            DiskStat {
                total_space: 2_000,
                available_space: 1_000,
            },
            DiskStat {
                total_space: 2_000,
                available_space: 1_500,
            },
        ];
        let meta = RebalanceMeta {
            id: "rebalance-id".to_string(),
            percent_free_goal: 0.6,
            pool_stats: vec![
                RebalanceStats {
                    participating: true,
                    init_capacity: 2_000,
                    init_free_space: 500,
                    buckets: vec!["bucket-a".to_string(), "bucket-b".to_string()],
                    rebalanced_buckets: vec!["bucket-a".to_string()],
                    bucket: "bucket-b".to_string(),
                    object: "object.txt".to_string(),
                    num_objects: 10,
                    num_versions: 12,
                    bytes: 300,
                    info: RebalanceInfo {
                        status: RebalStatus::Started,
                        start_time: Some(started),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                RebalanceStats {
                    participating: false,
                    info: RebalanceInfo {
                        status: RebalStatus::Completed,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let first = build_rebalance_admin_status(OffsetDateTime::from_unix_timestamp(1_030).unwrap(), &disk_stats, &meta);
        let second = build_rebalance_admin_status(OffsetDateTime::from_unix_timestamp(1_060).unwrap(), &disk_stats, &meta);

        assert_eq!(first.id, second.id);
        assert_eq!(first.stopped_at, second.stopped_at);
        assert_eq!(first.stop_propagation.failed_peers, second.stop_propagation.failed_peers);
        assert_eq!(first.pools.len(), second.pools.len());
        for (left, right) in first.pools.iter().zip(second.pools.iter()) {
            assert_eq!(left.id, right.id);
            assert_eq!(left.status, right.status);
            assert_eq!(left.stopping, right.stopping);
            assert_eq!(left.used, right.used);
            assert_eq!(left.last_error, right.last_error);
            assert_eq!(left.cleanup_warnings.count, right.cleanup_warnings.count);
            assert_eq!(
                left.progress.as_ref().map(|progress| (
                    progress.num_objects,
                    progress.num_versions,
                    progress.bytes,
                    progress.remaining_buckets,
                    progress.bucket.as_str(),
                    progress.object.as_str()
                )),
                right.progress.as_ref().map(|progress| (
                    progress.num_objects,
                    progress.num_versions,
                    progress.bytes,
                    progress.remaining_buckets,
                    progress.bucket.as_str(),
                    progress.object.as_str()
                ))
            );
        }

        assert_ne!(
            first.pools[0].progress.as_ref().map(|progress| progress.elapsed),
            second.pools[0].progress.as_ref().map(|progress| progress.elapsed)
        );
    }

    #[test]
    fn test_rebalance_status_serializes_stopped_at_when_present() {
        let stopped = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        let status = RebalanceAdminStatus {
            id: "id-2".to_string(),
            stopped_at: Some(stopped),
            stop_propagation: RebalanceStopPropagationStatus {
                last_attempt_at: Some(stopped),
                ..Default::default()
            },
            pools: vec![RebalancePoolStatus {
                id: 0,
                status: "Stopped".to_string(),
                stopping: false,
                used: 0.3,
                last_error: None,
                cleanup_warnings: RebalanceCleanupWarnings::default(),
                progress: None,
            }],
        };

        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"stoppedAt\""));
        assert!(json.contains("1970-01-01T00:16:40Z"));
        assert!(json.contains("\"lastAttemptAt\":\"1970-01-01T00:16:40Z\""));
    }

    #[test]
    fn test_rebalance_status_exposes_stop_propagation_failures() {
        let stop_attempt = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        let reload_attempt = OffsetDateTime::from_unix_timestamp(1_010).unwrap();
        let encoded_error = encode_rebalance_stop_propagation_record(&RebalanceStopPropagationRecord {
            stop_attempt_at: Some(stop_attempt),
            stop_failures: vec!["peer node-a stop_rebalance failed: timeout".to_string()],
            terminal_reload_attempt_at: Some(reload_attempt),
            terminal_reload_failures: vec!["peer node-b load_rebalance_meta(start=false) failed: timeout".to_string()],
        });
        let meta = RebalanceMeta {
            stopped_at: Some(stop_attempt),
            id: "id-3".to_string(),
            percent_free_goal: 0.3,
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    stopping: true,
                    last_error: Some(encoded_error),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        let status = build_rebalance_stop_propagation_status(&meta);

        assert_eq!(status.last_attempt_at, Some(stop_attempt));
        assert_eq!(status.terminal_reload_attempt_at, Some(reload_attempt));
        assert!(!status.pending_terminal_reload);
        assert_eq!(status.failed_peers, vec!["peer node-a stop_rebalance failed: timeout"]);
        assert_eq!(
            status.terminal_reload_failed_peers,
            vec!["peer node-b load_rebalance_meta(start=false) failed: timeout"]
        );
    }

    /// The rebalance handlers pre-check credentials before delegating to the
    /// shared admin gate, so a credential-less request keeps returning
    /// `InvalidRequest: authentication required` rather than the gate's own
    /// "get cred failed" wording (rustfs/backlog#1829).
    #[tokio::test]
    async fn rebalance_handlers_keep_their_missing_credentials_response() {
        assert_missing_credentials(&RebalanceStart {}, Method::POST, "/rustfs/admin/v3/rebalance/start").await;
        assert_missing_credentials(&RebalanceStatus {}, Method::GET, "/rustfs/admin/v3/rebalance/status").await;
        assert_missing_credentials(&RebalanceStop {}, Method::POST, "/rustfs/admin/v3/rebalance/stop").await;
    }

    fn source_block<'a>(production: &'a str, marker: &str) -> &'a str {
        let block = production
            .split_once(marker)
            .unwrap_or_else(|| panic!("{marker} should exist"))
            .1;
        let end = [
            "\npub struct ",
            "\nasync fn ",
            "\npub(crate) async fn ",
            "\nmod ",
            "\n#[cfg(test)]",
        ]
        .into_iter()
        .filter_map(|boundary| block.find(boundary))
        .min()
        .unwrap_or(block.len());
        &block[..end]
    }

    /// All three rebalance handlers authorize through the single shared gate with
    /// the same `RebalanceAdminAction` vector they used before the deduplication,
    /// and none of them binds the returned credentials (rustfs/backlog#1829).
    #[test]
    fn rebalance_handlers_use_the_shared_admin_gate_with_their_actions() {
        let production = include_str!("rebalance.rs")
            .split("\n#[cfg(test)]\nmod ")
            .next()
            .expect("production source must precede the test module");

        for handler in ["RebalanceStart", "RebalanceStatus", "RebalanceStop"] {
            let block = source_block(production, &format!("impl Operation for {handler}"));
            assert_eq!(
                block.matches("authorize_admin_request(").count(),
                1,
                "{handler} must use exactly one shared gate"
            );
            assert_eq!(
                block.matches("Action::AdminAction(").count(),
                1,
                "{handler} must preserve its exact action-vector length"
            );
            assert!(
                block.contains("AdminAction::RebalanceAdminAction"),
                "{handler} must authorize with RebalanceAdminAction"
            );
            assert!(
                !block.contains("let cred = authorize_admin_request("),
                "{handler} does not consume the authenticated credentials"
            );
            assert!(
                block.contains("let actor = MaskedAccessKey(&input_cred.access_key).to_string();"),
                "{handler} must keep masking the caller access key for its audit trail"
            );
        }

        assert!(!production.contains("check_key_valid(get_session_token"));
        assert!(!production.contains("validate_admin_request("));
    }
}
