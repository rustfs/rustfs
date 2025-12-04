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
    Result,
    scanner::metrics::{BucketMetrics, MetricsCollector},
};
use rustfs_common::data_usage::SizeSummary;
use rustfs_common::metrics::IlmAction;
use rustfs_ecstore::bucket::replication::{GLOBAL_REPLICATION_POOL, ReplicationConfig, get_heal_replicate_object_info};
use rustfs_ecstore::bucket::{
    lifecycle::{
        bucket_lifecycle_audit::LcEventSrc,
        bucket_lifecycle_ops::{GLOBAL_ExpiryState, apply_lifecycle_action, eval_action_from_lifecycle},
        lifecycle,
        lifecycle::Lifecycle,
    },
    metadata_sys::{get_bucket_targets_config, get_object_lock_config, get_replication_config},
    object_lock::objectlock_sys::{BucketObjectLockSys, enforce_retention_for_deletion},
    versioning::VersioningApi,
    versioning_sys::BucketVersioningSys,
};
use rustfs_ecstore::store_api::{ObjectInfo, ObjectToDelete};
use rustfs_filemeta::{FileInfo, ReplicationStatusType, replication_statuses_map};
use rustfs_utils::http::headers::{AMZ_BUCKET_REPLICATION_STATUS, HeaderExt, VERSION_PURGE_STATUS_KEY};
use s3s::dto::DefaultRetention;
use s3s::dto::{BucketLifecycleConfiguration as LifecycleConfig, VersioningConfiguration};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration as StdDuration,
};
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

static SCANNER_EXCESS_OBJECT_VERSIONS: AtomicU64 = AtomicU64::new(100);
static SCANNER_EXCESS_OBJECT_VERSIONS_TOTAL_SIZE: AtomicU64 = AtomicU64::new(1024 * 1024 * 1024 * 1024); // 1 TB

#[derive(Clone)]
pub struct ScannerItem {
    pub bucket: String,
    pub object_name: String,
    pub lifecycle: Option<Arc<LifecycleConfig>>,
    pub versioning: Option<Arc<VersioningConfiguration>>,
    pub object_lock_config: Option<DefaultRetention>,
    pub replication_pending_grace: StdDuration,
    pub replication_metrics: Option<ReplicationMetricsHandle>,
}

#[derive(Clone)]
pub struct ReplicationMetricsHandle {
    inner: Arc<ReplicationMetricsInner>,
}

struct ReplicationMetricsInner {
    metrics: Arc<MetricsCollector>,
    bucket_metrics: Arc<Mutex<HashMap<String, BucketMetrics>>>,
}

impl ReplicationMetricsHandle {
    pub fn new(metrics: Arc<MetricsCollector>, bucket_metrics: Arc<Mutex<HashMap<String, BucketMetrics>>>) -> Self {
        Self {
            inner: Arc::new(ReplicationMetricsInner { metrics, bucket_metrics }),
        }
    }

    pub async fn record_status(&self, bucket: &str, status: ReplicationStatusType, lagging: bool) {
        match status {
            ReplicationStatusType::Pending => self.inner.metrics.increment_replication_pending_objects(1),
            ReplicationStatusType::Failed => self.inner.metrics.increment_replication_failed_objects(1),
            _ => {}
        }
        if lagging {
            self.inner.metrics.increment_replication_lagging_objects(1);
        }

        let mut guard = self.inner.bucket_metrics.lock().await;
        let entry = guard.entry(bucket.to_string()).or_insert_with(|| BucketMetrics {
            bucket: bucket.to_string(),
            ..Default::default()
        });

        match status {
            ReplicationStatusType::Pending => {
                entry.replication_pending = entry.replication_pending.saturating_add(1);
            }
            ReplicationStatusType::Failed => {
                entry.replication_failed = entry.replication_failed.saturating_add(1);
            }
            _ => {}
        }

        if lagging {
            entry.replication_lagging = entry.replication_lagging.saturating_add(1);
        }
    }

    pub async fn record_task_submission(&self, bucket: &str) {
        self.inner.metrics.increment_replication_tasks_queued(1);
        let mut guard = self.inner.bucket_metrics.lock().await;
        let entry = guard.entry(bucket.to_string()).or_insert_with(|| BucketMetrics {
            bucket: bucket.to_string(),
            ..Default::default()
        });
        entry.replication_tasks_queued = entry.replication_tasks_queued.saturating_add(1);
    }
}

impl ScannerItem {
    const INTERNAL_REPLICATION_STATUS_KEY: &'static str = "x-rustfs-internal-replication-status";

    pub fn new(
        bucket: String,
        lifecycle: Option<Arc<LifecycleConfig>>,
        versioning: Option<Arc<VersioningConfiguration>>,
        object_lock_config: Option<DefaultRetention>,
        replication_pending_grace: StdDuration,
        replication_metrics: Option<ReplicationMetricsHandle>,
    ) -> Self {
        Self {
            bucket,
            object_name: "".to_string(),
            lifecycle,
            versioning,
            object_lock_config,
            replication_pending_grace,
            replication_metrics,
        }
    }

    pub async fn apply_versions_actions(&self, fivs: &[FileInfo]) -> Result<Vec<ObjectInfo>> {
        let obj_infos = self.apply_newer_noncurrent_version_limit(fivs).await?;
        if obj_infos.len() >= SCANNER_EXCESS_OBJECT_VERSIONS.load(Ordering::SeqCst) as usize {
            // todo
        }

        let mut cumulative_size = 0;
        for obj_info in obj_infos.iter() {
            cumulative_size += obj_info.size;
        }

        if cumulative_size >= SCANNER_EXCESS_OBJECT_VERSIONS_TOTAL_SIZE.load(Ordering::SeqCst) as i64 {
            //todo
        }

        Ok(obj_infos)
    }

    pub async fn apply_newer_noncurrent_version_limit(&self, fivs: &[FileInfo]) -> Result<Vec<ObjectInfo>> {
        let lock_enabled = if let Some(rcfg) = BucketObjectLockSys::get(&self.bucket).await {
            rcfg.mode.is_some()
        } else {
            false
        };
        let _vcfg = BucketVersioningSys::get(&self.bucket).await?;

        let versioned = match BucketVersioningSys::get(&self.bucket).await {
            Ok(vcfg) => vcfg.versioned(&self.object_name),
            Err(_) => false,
        };
        let mut object_infos = Vec::with_capacity(fivs.len());

        if self.lifecycle.is_none() {
            for info in fivs.iter() {
                object_infos.push(ObjectInfo::from_file_info(info, &self.bucket, &self.object_name, versioned));
            }
            return Ok(object_infos);
        }

        let event = self
            .lifecycle
            .as_ref()
            .expect("lifecycle err.")
            .clone()
            .noncurrent_versions_expiration_limit(&lifecycle::ObjectOpts {
                name: self.object_name.clone(),
                ..Default::default()
            })
            .await;
        let lim = event.newer_noncurrent_versions;
        if lim == 0 || fivs.len() <= lim + 1 {
            for fi in fivs.iter() {
                object_infos.push(ObjectInfo::from_file_info(fi, &self.bucket, &self.object_name, versioned));
            }
            return Ok(object_infos);
        }

        let overflow_versions = &fivs[lim + 1..];
        for fi in fivs[..lim + 1].iter() {
            object_infos.push(ObjectInfo::from_file_info(fi, &self.bucket, &self.object_name, versioned));
        }

        let mut to_del = Vec::<ObjectToDelete>::with_capacity(overflow_versions.len());
        for fi in overflow_versions.iter() {
            let obj = ObjectInfo::from_file_info(fi, &self.bucket, &self.object_name, versioned);
            if lock_enabled && enforce_retention_for_deletion(&obj) {
                //if enforce_retention_for_deletion(&obj) {
                /*if self.debug {
                    if obj.version_id.is_some() {
                        info!("lifecycle: {} v({}) is locked, not deleting\n", obj.name, obj.version_id.expect("err"));
                    } else {
                        info!("lifecycle: {} is locked, not deleting\n", obj.name);
                    }
                }*/
                object_infos.push(obj);
                continue;
            }

            if OffsetDateTime::now_utc().unix_timestamp()
                < lifecycle::expected_expiry_time(obj.successor_mod_time.expect("err"), event.noncurrent_days as i32)
                    .unix_timestamp()
            {
                object_infos.push(obj);
                continue;
            }

            to_del.push(ObjectToDelete {
                object_name: obj.name,
                version_id: obj.version_id,
                ..Default::default()
            });
        }

        if !to_del.is_empty() {
            let mut expiry_state = GLOBAL_ExpiryState.write().await;
            expiry_state.enqueue_by_newer_noncurrent(&self.bucket, to_del, event).await;
        }

        Ok(object_infos)
    }

    pub async fn apply_actions(&mut self, oi: &ObjectInfo, _size_s: &mut SizeSummary) -> (bool, i64) {
        let object_locked = self.is_object_lock_protected(oi);

        if let Err(err) = self.heal_replication(oi).await {
            warn!(
                "heal_replication failed for {}/{} (version {:?}): {}",
                oi.bucket, oi.name, oi.version_id, err
            );
        }

        if object_locked {
            info!(
                "apply_actions: Skipping lifecycle for {}/{} because object lock retention or legal hold is active",
                oi.bucket, oi.name
            );
            return (false, oi.size);
        }

        let (action, _size) = self.apply_lifecycle(oi).await;

        info!(
            "apply_actions {} {} {:?} {:?}",
            oi.bucket.clone(),
            oi.name.clone(),
            oi.version_id.clone(),
            oi.user_defined.clone()
        );

        if action.delete_all() {
            return (true, 0);
        }

        (false, oi.size)
    }

    async fn apply_lifecycle(&mut self, oi: &ObjectInfo) -> (IlmAction, i64) {
        let size = oi.size;
        if self.lifecycle.is_none() {
            info!("apply_lifecycle: No lifecycle config for object: {}", oi.name);
            return (IlmAction::NoneAction, size);
        }

        info!("apply_lifecycle: Lifecycle config exists for object: {}", oi.name);

        let (olcfg, rcfg) = if self.bucket != ".minio.sys" {
            (
                get_object_lock_config(&self.bucket).await.ok(),
                None, // FIXME: replication config
            )
        } else {
            (None, None)
        };

        info!("apply_lifecycle: Evaluating lifecycle for object: {}", oi.name);

        let lifecycle = match self.lifecycle.as_ref() {
            Some(lc) => lc,
            None => {
                info!("No lifecycle configuration found for object: {}", oi.name);
                return (IlmAction::NoneAction, 0);
            }
        };

        let lc_evt = eval_action_from_lifecycle(
            lifecycle,
            olcfg
                .as_ref()
                .and_then(|(c, _)| c.rule.as_ref().and_then(|r| r.default_retention.clone())),
            rcfg.clone(),
            oi, // Pass oi directly
        )
        .await;

        info!("lifecycle: {} Initial scan: {} (action: {:?})", oi.name, lc_evt.action, lc_evt.action);

        let mut new_size = size;
        match lc_evt.action {
            IlmAction::DeleteVersionAction | IlmAction::DeleteAllVersionsAction | IlmAction::DelMarkerDeleteAllVersionsAction => {
                info!("apply_lifecycle: Object {} marked for version deletion, new_size=0", oi.name);
                new_size = 0;
            }
            IlmAction::DeleteAction => {
                info!("apply_lifecycle: Object {} marked for deletion", oi.name);
                if let Some(vcfg) = &self.versioning {
                    if !vcfg.enabled() {
                        info!("apply_lifecycle: Versioning disabled, setting new_size=0");
                        new_size = 0;
                    }
                } else {
                    info!("apply_lifecycle: No versioning config, setting new_size=0");
                    new_size = 0;
                }
            }
            IlmAction::NoneAction => {
                info!("apply_lifecycle: No action for object {}", oi.name);
            }
            _ => {
                info!("apply_lifecycle: Other action {:?} for object {}", lc_evt.action, oi.name);
            }
        }

        if lc_evt.action != IlmAction::NoneAction {
            info!("apply_lifecycle: Applying lifecycle action {:?} for object {}", lc_evt.action, oi.name);
            apply_lifecycle_action(&lc_evt, &LcEventSrc::Scanner, oi).await;
        } else {
            info!("apply_lifecycle: Skipping lifecycle action for object {} as no action is needed", oi.name);
        }

        (lc_evt.action, new_size)
    }

    fn is_object_lock_protected(&self, oi: &ObjectInfo) -> bool {
        enforce_retention_for_deletion(oi)
    }

    async fn heal_replication(&self, oi: &ObjectInfo) -> Result<()> {
        let enriched = Self::hydrate_replication_metadata(oi);
        let pending_lagging = self.is_pending_lagging(&enriched);

        if let Some(handle) = &self.replication_metrics {
            handle
                .record_status(&self.bucket, enriched.replication_status.clone(), pending_lagging)
                .await;
        }

        debug!(
            "heal_replication: evaluating {}/{} with status {:?} and internal {:?}",
            enriched.bucket, enriched.name, enriched.replication_status, enriched.replication_status_internal
        );

        if !self.needs_replication_heal(&enriched, pending_lagging) {
            return Ok(());
        }

        let replication_cfg = match get_replication_config(&self.bucket).await {
            Ok((cfg, _)) => Some(cfg),
            Err(err) => {
                debug!("heal_replication: failed to fetch replication config for bucket {}: {}", self.bucket, err);
                None
            }
        };

        if replication_cfg.is_none() {
            return Ok(());
        }

        let bucket_targets = match get_bucket_targets_config(&self.bucket).await {
            Ok(targets) => Some(targets),
            Err(err) => {
                debug!("heal_replication: no bucket targets for bucket {}: {}", self.bucket, err);
                None
            }
        };

        let replication_cfg = ReplicationConfig::new(replication_cfg, bucket_targets);
        if replication_cfg.is_empty() {
            return Ok(());
        }

        let replicate_info = get_heal_replicate_object_info(&enriched, &replication_cfg).await;
        let should_replicate = replicate_info.dsc.replicate_any()
            || matches!(
                enriched.replication_status,
                ReplicationStatusType::Failed | ReplicationStatusType::Pending
            );
        if !should_replicate {
            debug!("heal_replication: no actionable targets for {}/{}", enriched.bucket, enriched.name);
            return Ok(());
        }

        if let Some(pool) = GLOBAL_REPLICATION_POOL.get() {
            pool.queue_replica_task(replicate_info).await;
            if let Some(handle) = &self.replication_metrics {
                handle.record_task_submission(&self.bucket).await;
            }
            debug!("heal_replication: queued replication heal task for {}/{}", enriched.bucket, enriched.name);
        } else {
            warn!(
                "heal_replication: GLOBAL_REPLICATION_POOL not initialized, skipping heal for {}/{}",
                enriched.bucket, enriched.name
            );
        }

        Ok(())
    }

    fn needs_replication_heal(&self, oi: &ObjectInfo, pending_lagging: bool) -> bool {
        if matches!(oi.replication_status, ReplicationStatusType::Failed) {
            return true;
        }

        if pending_lagging && matches!(oi.replication_status, ReplicationStatusType::Pending) {
            return true;
        }

        if let Some(raw) = oi.replication_status_internal.as_ref() {
            let statuses = replication_statuses_map(raw);
            if statuses
                .values()
                .any(|status| matches!(status, ReplicationStatusType::Failed))
            {
                return true;
            }

            if pending_lagging
                && statuses
                    .values()
                    .any(|status| matches!(status, ReplicationStatusType::Pending))
            {
                return true;
            }
        }

        false
    }

    fn hydrate_replication_metadata(oi: &ObjectInfo) -> ObjectInfo {
        let mut enriched = oi.clone();

        if enriched.replication_status.is_empty() {
            if let Some(status) = enriched.user_defined.lookup(AMZ_BUCKET_REPLICATION_STATUS) {
                enriched.replication_status = ReplicationStatusType::from(status);
            }
        }

        if enriched.replication_status_internal.is_none() {
            if let Some(raw) = enriched.user_defined.lookup(Self::INTERNAL_REPLICATION_STATUS_KEY) {
                if !raw.is_empty() {
                    enriched.replication_status_internal = Some(raw.to_string());
                }
            }
        }

        if enriched.version_purge_status_internal.is_none() {
            if let Some(raw) = enriched.user_defined.lookup(VERSION_PURGE_STATUS_KEY) {
                if !raw.is_empty() {
                    enriched.version_purge_status_internal = Some(raw.to_string());
                }
            }
        }

        enriched
    }

    fn is_pending_lagging(&self, oi: &ObjectInfo) -> bool {
        if !matches!(oi.replication_status, ReplicationStatusType::Pending) {
            return false;
        }

        let Some(mod_time) = oi.mod_time else {
            return false;
        };

        let grace = TimeDuration::try_from(self.replication_pending_grace).unwrap_or_else(|_| TimeDuration::seconds(0));
        if grace.is_zero() {
            return true;
        }

        let elapsed = OffsetDateTime::now_utc() - mod_time;
        elapsed >= grace
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn replication_metrics_handle_tracks_counts() {
        let metrics = Arc::new(MetricsCollector::new());
        let bucket_metrics = Arc::new(Mutex::new(HashMap::new()));
        let handle = ReplicationMetricsHandle::new(metrics.clone(), bucket_metrics.clone());

        handle
            .record_status("test-bucket", ReplicationStatusType::Pending, true)
            .await;
        handle
            .record_status("test-bucket", ReplicationStatusType::Failed, false)
            .await;
        handle.record_task_submission("test-bucket").await;

        let snapshot = metrics.get_metrics();
        assert_eq!(snapshot.replication_pending_objects, 1);
        assert_eq!(snapshot.replication_failed_objects, 1);
        assert_eq!(snapshot.replication_lagging_objects, 1);
        assert_eq!(snapshot.replication_tasks_queued, 1);

        let guard = bucket_metrics.lock().await;
        let bucket_entry = guard.get("test-bucket").expect("bucket metrics exists");
        assert_eq!(bucket_entry.replication_pending, 1);
        assert_eq!(bucket_entry.replication_failed, 1);
        assert_eq!(bucket_entry.replication_lagging, 1);
        assert_eq!(bucket_entry.replication_tasks_queued, 1);
    }
}
