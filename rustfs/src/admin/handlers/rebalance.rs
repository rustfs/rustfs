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
    admin::{auth::validate_admin_request, router::Operation},
    auth::{check_key_valid, get_session_token},
    server::RemoteAddr,
};
use http::{HeaderMap, StatusCode};
use matchit::Params;
use rustfs_ecstore::rebalance::RebalanceMeta;
use rustfs_ecstore::{
    StorageAPI,
    error::StorageError,
    new_object_layer_fn,
    notification_sys::get_global_notification_sys,
    rebalance::{DiskStat, RebalSaveOpt},
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
    pub used: f64, // Percentage used space
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

pub struct RebalanceStart {}

#[async_trait::async_trait]
impl Operation for RebalanceStart {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle RebalanceStart");

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
            vec![Action::AdminAction(AdminAction::RebalanceAdminAction)],
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
        )
        .await?;

        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "Not init"));
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

        if store.is_rebalance_started().await {
            return Err(s3_error!(OperationAborted, "Rebalance already in progress"));
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

        store.start_rebalance().await;

        warn!("Rebalance started with id: {}", id);
        if let Some(notification_sys) = get_global_notification_sys() {
            warn!("RebalanceStart Loading rebalance meta start");
            notification_sys.load_rebalance_meta(true).await;
            warn!("RebalanceStart Loading rebalance meta done");
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
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle RebalanceStatus");

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
            vec![Action::AdminAction(AdminAction::RebalanceAdminAction)],
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
        )
        .await?;

        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "Not init"));
        };

        let mut meta = RebalanceMeta::new();
        if let Err(err) = meta.load(store.pools[0].clone()).await {
            if err == StorageError::ConfigNotFound {
                return Err(s3_error!(NoSuchResource, "Pool rebalance is not started"));
            }

            return Err(s3_error!(InternalError, "Failed to load rebalance meta: {}", err));
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

        let mut stop_time = meta.stopped_at;
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

            let mut elapsed = if let Some(start_time) = ps.info.start_time {
                let now = OffsetDateTime::now_utc();
                now - start_time
            } else {
                return Err(s3_error!(InternalError, "Start time is not available"));
            };

            let mut eta = if ps.bytes > 0 {
                Duration::from_secs_f64(total_bytes_to_rebal * elapsed.as_seconds_f64() / ps.bytes as f64)
            } else {
                Duration::ZERO
            };

            if ps.info.end_time.is_some() {
                stop_time = ps.info.end_time;
            }

            if let Some(stopped_at) = stop_time {
                if let Some(start_time) = ps.info.start_time {
                    elapsed = stopped_at - start_time;
                }

                eta = Duration::ZERO;
            }

            admin_status.pools[i].progress = Some(RebalPoolProgress {
                num_objects: ps.num_objects,
                num_versions: ps.num_versions,
                bytes: ps.bytes,
                bucket: ps.bucket.clone(),
                object: ps.object.clone(),
                elapsed: elapsed.whole_seconds() as u64,
                eta: eta.as_secs(),
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
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle RebalanceStop");

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
            vec![Action::AdminAction(AdminAction::RebalanceAdminAction)],
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
        )
        .await?;

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

        warn!("handle RebalanceStop save_rebalance_stats done ");
        if let Some(notification_sys) = get_global_notification_sys() {
            warn!("handle RebalanceStop notification_sys load_rebalance_meta");
            notification_sys.load_rebalance_meta(false).await;
            warn!("handle RebalanceStop notification_sys load_rebalance_meta done");
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
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
