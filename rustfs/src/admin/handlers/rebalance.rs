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

use crate::{
    admin::{
        auth::validate_admin_request,
        router::{AdminOperation, Operation, S3Router},
    },
    auth::{check_key_valid, get_session_token},
    server::{ADMIN_PREFIX, RemoteAddr},
};
use http::{HeaderMap, HeaderValue, StatusCode};
use hyper::Method;
use matchit::Params;
use rustfs_ecstore::rebalance::RebalanceMeta;
use rustfs_ecstore::{
    StorageAPI,
    error::StorageError,
    new_object_layer_fn,
    notification_sys::get_global_notification_sys,
    rebalance::{DiskStat, RebalSaveOpt},
    store_api::BucketOperations,
    store_api::BucketOptions,
};
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::{
    Body, S3Request, S3Response, S3Result,
    header::{CONTENT_LENGTH, CONTENT_TYPE},
    s3_error,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use time::OffsetDateTime;
use tracing::warn;

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
    #[serde(rename = "used")]
    pub used: f64, // Fraction of used space in range 0.0..=1.0
    #[serde(rename = "lastError")]
    pub last_error: Option<String>, // Last rebalance error message for this pool
    #[serde(rename = "progress")]
    pub progress: Option<RebalPoolProgress>, // None when rebalance is not running
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RebalanceAdminStatus {
    pub id: String, // Identifies the ongoing rebalance operation by a UUID
    #[serde(rename = "pools")]
    pub pools: Vec<RebalancePoolStatus>, // Contains all pools, including inactive
    #[serde(rename = "stoppedAt", with = "offsetdatetime_rfc3339")]
    pub stopped_at: Option<OffsetDateTime>, // Optional timestamp when rebalance was stopped
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
    ps: &rustfs_ecstore::rebalance::RebalanceStats,
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

fn rebalance_remaining_buckets(buckets: usize, rebalanced_buckets: usize) -> usize {
    buckets.saturating_sub(rebalanced_buckets)
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
    pool_stats: &[rustfs_ecstore::rebalance::RebalanceStats],
    disk_stats: &[DiskStat],
) -> Vec<RebalancePoolStatus> {
    pool_stats
        .iter()
        .enumerate()
        .map(|(i, ps)| {
            let mut status = RebalancePoolStatus {
                id: i,
                status: ps.info.status.to_string(),
                used: rebalance_pool_used(disk_stats, i),
                last_error: ps.info.last_error.clone(),
                progress: None,
            };

            if ps.participating {
                status.progress = build_rebalance_pool_progress(now, stop_time, percent_free_goal, ps);
            }

            status
        })
        .collect()
}

pub struct RebalanceStart {}

#[async_trait::async_trait]
impl Operation for RebalanceStart {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle RebalanceStart");

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "Failed to start rebalance: missing credentials"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::RebalanceAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "Failed to start rebalance: object layer not initialized"));
        };

        if store.pools.len() == 1 {
            return Err(s3_error!(NotImplemented));
        }

        if store.is_decommission_running().await {
            return Err(s3_error!(
                InvalidRequest,
                "Rebalance cannot be started, decommission is already in progress"
            ));
        }

        if store.is_rebalance_conflicting_with_decommission().await {
            return Err(s3_error!(OperationAborted, "Rebalance already in progress"));
        }

        let bucket_infos = store
            .list_bucket(&BucketOptions::default())
            .await
            .map_err(|e| s3_error!(InternalError, "Failed to list buckets for rebalance: {}", e))?;

        let buckets: Vec<String> = bucket_infos.into_iter().map(|bucket| bucket.name).collect();

        let id = match store.init_rebalance_meta(buckets).await {
            Ok(id) => id,
            Err(e) => {
                return Err(s3_error!(InternalError, "Failed to initialize rebalance metadata: {}", e));
            }
        };

        store
            .start_rebalance()
            .await
            .map_err(|e| s3_error!(InternalError, "Failed to start rebalance: {}", e))?;

        warn!("Rebalance started with id: {}", id);
        if let Some(notification_sys) = get_global_notification_sys() {
            warn!("RebalanceStart Loading rebalance meta start");
            if let Err(err) = notification_sys.load_rebalance_meta(true).await {
                warn!("rebalance start propagation failed after local state update: {err}");
            }
            warn!("RebalanceStart Loading rebalance meta done");
        }

        let resp = RebalanceResp { id };
        let data = serde_json::to_string(&resp)
            .map_err(|e| s3_error!(InternalError, "Failed to serialize rebalance start response: {}", e))?;

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
        warn!("handle RebalanceStatus");

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "Failed to load rebalance status: missing credentials"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::RebalanceAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "Failed to load rebalance status: object layer not initialized"));
        };

        if store.pools.is_empty() {
            return Err(s3_error!(InternalError, "Failed to load rebalance status: no storage pools available"));
        }

        let first_pool = store
            .pools
            .first()
            .cloned()
            .ok_or_else(|| s3_error!(InternalError, "Failed to load rebalance status: no storage pools available"))?;

        let mut meta = RebalanceMeta::new();
        if let Err(err) = meta.load(first_pool).await {
            if err == StorageError::ConfigNotFound {
                return Err(s3_error!(NoSuchResource, "Pool rebalance is not started"));
            }

            return Err(s3_error!(InternalError, "Failed to load rebalance metadata from pool 0: {}", err));
        }

        // Compute disk usage percentage
        let si = store.storage_info().await;
        let mut disk_stats = vec![DiskStat::default(); store.pools.len()];

        for disk in si.disks.iter() {
            if disk.pool_index < 0 || disk_stats.len() <= disk.pool_index as usize {
                continue;
            }
            disk_stats[disk.pool_index as usize].available_space += disk.available_space;
            disk_stats[disk.pool_index as usize].total_space += disk.total_space;
        }

        let stop_time = meta.stopped_at;
        let now = OffsetDateTime::now_utc();
        let admin_status = RebalanceAdminStatus {
            id: meta.id.clone(),
            stopped_at: meta.stopped_at,
            pools: build_rebalance_pool_statuses(now, stop_time, meta.percent_free_goal, &meta.pool_stats, &disk_stats),
        };

        let data = serde_json::to_string(&admin_status)
            .map_err(|e| s3_error!(InternalError, "Failed to serialize rebalance status response: {}", e))?;
        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

// RebalanceStop
pub struct RebalanceStop {}

#[async_trait::async_trait]
impl Operation for RebalanceStop {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle RebalanceStop");

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "Failed to stop rebalance: missing credentials"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::RebalanceAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "Failed to stop rebalance: object layer not initialized"));
        };

        if !store.is_rebalance_conflicting_with_decommission().await {
            return Err(s3_error!(NoSuchResource, "Pool rebalance is not started"));
        }

        if let Some(notification_sys) = get_global_notification_sys() {
            notification_sys
                .stop_rebalance()
                .await
                .map_err(|e| s3_error!(InternalError, "Failed to stop rebalance via notification system: {}", e))?;
        } else {
            store
                .stop_rebalance()
                .await
                .map_err(|e| s3_error!(InternalError, "Failed to stop rebalance: {}", e))?;

            store
                .save_rebalance_stats(usize::MAX, RebalSaveOpt::StoppedAt)
                .await
                .map_err(|e| s3_error!(InternalError, "Failed to persist rebalance stop metadata: {}", e))?;
        }

        warn!("handle RebalanceStop save_rebalance_stats done ");
        if let Some(notification_sys) = get_global_notification_sys() {
            warn!("handle RebalanceStop notification_sys load_rebalance_meta");
            if let Err(err) = notification_sys.load_rebalance_meta(false).await {
                warn!("rebalance stop propagation failed after local state update: {err}");
            }
            warn!("handle RebalanceStop notification_sys load_rebalance_meta done");
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
        RebalPoolProgress, RebalanceAdminStatus, RebalancePoolStatus, build_rebalance_pool_statuses, rebalance_pool_used,
        rebalance_remaining_buckets, rebalance_used_pct,
    };
    use rustfs_ecstore::rebalance::{DiskStat, RebalStatus, RebalanceInfo, RebalanceStats};
    use time::OffsetDateTime;

    #[test]
    fn test_calculate_rebalance_progress_running() {
        let start = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        let now = OffsetDateTime::from_unix_timestamp(1_050).unwrap();

        let (elapsed, eta) = calculate_rebalance_progress(now, Some(start), None, 100, 200.0).unwrap();

        assert_eq!(elapsed, 50);
        assert_eq!(eta, 50);
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
        };

        let progress = build_rebalance_pool_progress(OffsetDateTime::from_unix_timestamp(1_050).unwrap(), None, 0.3, &ps)
            .expect("progress should be generated");
        assert_eq!(progress.num_objects, 3);
        assert_eq!(progress.num_versions, 5);
        assert_eq!(progress.bytes, 100);
        assert_eq!(progress.remaining_buckets, 2);
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
    fn test_rebalance_remaining_buckets_is_saturating_sub() {
        assert_eq!(rebalance_remaining_buckets(10, 7), 3);
        assert_eq!(rebalance_remaining_buckets(3, 10), 0);
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
        assert_eq!(active.progress.as_ref().unwrap().remaining_buckets, 1);

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
            },
        ];

        let statuses =
            build_rebalance_pool_statuses(OffsetDateTime::from_unix_timestamp(2_010).unwrap(), None, 0.3, &pool_stats, &[]);

        assert_eq!(statuses[1].used, 0.0);
        assert!(statuses[1].progress.is_some());
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
            pools: vec![RebalancePoolStatus {
                id: 0,
                status: "Started".to_string(),
                used: 0.5,
                last_error: Some("temporary error".to_string()),
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
        assert!(json.contains("\"stoppedAt\":null"));
    }

    #[test]
    fn test_rebalance_status_serializes_stopped_at_when_present() {
        let stopped = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        let status = RebalanceAdminStatus {
            id: "id-2".to_string(),
            stopped_at: Some(stopped),
            pools: vec![RebalancePoolStatus {
                id: 0,
                status: "Stopped".to_string(),
                used: 0.3,
                last_error: None,
                progress: None,
            }],
        };

        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"stoppedAt\""));
        assert!(json.contains("1970-01-01T00:16:40Z"));
    }
}
