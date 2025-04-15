use ecstore::{
    new_object_layer_fn,
    notification_sys::get_global_notification_sys,
    rebalance::{DiskStat, RebalSaveOpt},
    store_api::BucketOptions,
    StorageAPI,
};
use http::{HeaderMap, StatusCode};
use matchit::Params;
use s3s::{header::CONTENT_TYPE, s3_error, Body, S3Request, S3Response, S3Result};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};
use tracing::warn;

use crate::admin::router::Operation;
use ecstore::rebalance::RebalanceMeta;

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
    #[serde(rename = "bucket")]
    pub bucket: String,
    #[serde(rename = "object")]
    pub object: String,
    #[serde(rename = "elapsed")]
    pub elapsed: Duration,
    #[serde(rename = "eta")]
    pub eta: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RebalancePoolStatus {
    #[serde(rename = "id")]
    pub id: usize, // Pool index (zero-based)
    #[serde(rename = "status")]
    pub status: String, // Active if rebalance is running, empty otherwise
    #[serde(rename = "used")]
    pub used: f64, // Percentage used space
    #[serde(rename = "progress")]
    pub progress: Option<RebalPoolProgress>, // None when rebalance is not running
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RebalanceAdminStatus {
    pub id: String, // Identifies the ongoing rebalance operation by a UUID
    #[serde(rename = "pools")]
    pub pools: Vec<RebalancePoolStatus>, // Contains all pools, including inactive
    #[serde(rename = "stoppedAt")]
    pub stopped_at: Option<SystemTime>, // Optional timestamp when rebalance was stopped
}

pub struct RebalanceStart {}

#[async_trait::async_trait]
impl Operation for RebalanceStart {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle RebalanceStart");

        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "Not init"));
        };

        if store.pools.len() == 1 {
            return Err(s3_error!(NotImplemented));
        }

        if store.is_decommission_running().await {
            return Err(s3_error!(
                InternalError,
                "Rebalance cannot be started, decommission is already in progress"
            ));
        }

        if store.is_rebalance_started().await {
            return Err(s3_error!(InternalError, "Rebalance already in progress"));
        }

        let bucket_infos = store
            .list_bucket(&BucketOptions::default())
            .await
            .map_err(|e| s3_error!(InternalError, "Failed to list buckets: {}", e))?;

        let buckets: Vec<String> = bucket_infos.into_iter().map(|bucket| bucket.name).collect();

        let id = match store.init_rebalance_meta(buckets).await {
            Ok(id) => id,
            Err(e) => {
                return Err(s3_error!(InternalError, "Failed to init rebalance meta: {}", e));
            }
        };

        if let Some(notification_sys) = get_global_notification_sys() {
            notification_sys.load_rebalance_meta(true).await;
        }

        let resp = RebalanceResp { id };
        let data = serde_json::to_string(&resp).map_err(|e| s3_error!(InternalError, "Failed to serialize response: {}", e))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

// RebalanceStatus
pub struct RebalanceStatus {}

#[async_trait::async_trait]
impl Operation for RebalanceStatus {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle RebalanceStatus");

        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "Not init"));
        };

        let mut meta = RebalanceMeta::new();
        meta.load(store.pools[0].clone())
            .await
            .map_err(|e| s3_error!(InternalError, "Failed to load rebalance meta: {}", e))?;

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
        let mut admin_status = RebalanceAdminStatus {
            id: meta.id.clone(),
            stopped_at: meta.stopped_at,
            pools: vec![RebalancePoolStatus::default(); meta.pool_stats.len()],
        };

        for (i, ps) in meta.pool_stats.iter().enumerate() {
            admin_status.pools[i] = RebalancePoolStatus {
                id: i,
                status: ps.info.status.to_string(),
                used: (disk_stats[i].total_space - disk_stats[i].available_space) as f64 / disk_stats[i].total_space as f64,
                progress: None,
            };

            if !ps.participating {
                continue;
            }

            // Calculate total bytes to be rebalanced
            let total_bytes_to_rebal = ps.init_capacity as f64 * meta.percent_free_goal - ps.init_free_space as f64;

            let elapsed = if let Some(start_time) = ps.info.start_time {
                SystemTime::now()
                    .duration_since(start_time)
                    .map_err(|e| s3_error!(InternalError, "Failed to calculate elapsed time: {}", e))?
            } else {
                return Err(s3_error!(InternalError, "Start time is not available"));
            };

            let eta = if ps.bytes > 0 {
                Duration::from_secs_f64(total_bytes_to_rebal * elapsed.as_secs_f64() / ps.bytes as f64)
            } else {
                Duration::ZERO
            };

            let stop_time = ps.info.end_time.unwrap_or(stop_time.unwrap_or(SystemTime::now()));

            let elapsed = if ps.info.end_time.is_some() || meta.stopped_at.is_some() {
                stop_time
                    .duration_since(ps.info.start_time.unwrap_or(stop_time))
                    .unwrap_or_default()
            } else {
                elapsed
            };

            admin_status.pools[i].progress = Some(RebalPoolProgress {
                num_objects: ps.num_objects,
                num_versions: ps.num_versions,
                bytes: ps.bytes,
                bucket: ps.bucket.clone(),
                object: ps.object.clone(),
                elapsed,
                eta,
            });
        }

        let data =
            serde_json::to_string(&admin_status).map_err(|e| s3_error!(InternalError, "Failed to serialize response: {}", e))?;
        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

// RebalanceStop
pub struct RebalanceStop {}

#[async_trait::async_trait]
impl Operation for RebalanceStop {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle RebalanceStop");

        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "Not init"));
        };

        if let Some(notification_sys) = get_global_notification_sys() {
            notification_sys.stop_rebalance().await;
        }

        store
            .save_rebalance_stats(0, RebalSaveOpt::StoppedAt)
            .await
            .map_err(|e| s3_error!(InternalError, "Failed to stop rebalance: {}", e))?;

        if let Some(notification_sys) = get_global_notification_sys() {
            notification_sys.load_rebalance_meta(true).await;
        }

        return Err(s3_error!(NotImplemented));
    }
}
