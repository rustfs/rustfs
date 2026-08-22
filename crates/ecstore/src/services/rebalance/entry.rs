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

use super::meta::{
    clone_arc_by_index, ensure_valid_rebalance_pool_index, invalid_rebalance_pool_index_error,
    rebalance_metadata_not_initialized_error, should_ignore_rebalance_data_usage_cache,
};
use super::migration::{RebalanceMigrationBackend, migrate_entry_version};
use super::worker::{
    RebalanceEntryCleanupResult, RebalanceEntryTask, load_rebalance_bucket_configs, rebalance_max_attempts,
    resolve_rebalance_bucket_error, resolve_rebalance_entry_cleanup_delete_result, resolve_rebalance_file_info_versions_result,
    resolve_rebalance_migrate_result_error, resolve_rebalance_stats_update_result, resolve_rebalance_worker_result,
    run_rebalance_listing_with_retry, should_cleanup_rebalance_source_entry, should_count_rebalance_version_complete,
    should_defer_rebalance_entry_failure, should_skip_rebalance_delete_marker, wait_rebalance_entry_tasks,
    with_rebalance_entry_context,
};
use super::{
    EVENT_REBALANCE_BUCKET, EVENT_REBALANCE_ENTRY, EVENT_REBALANCE_STATE, LOG_COMPONENT_ECSTORE, LOG_SUBSYSTEM_REBALANCE,
    ObjectInfo, REBALANCE_DEFERRED_ENTRY_ERROR_PREFIX, RebalanceBucketConfigs, RebalanceBucketOutcome, RebalanceEntryOutcome,
};
use crate::core::pools::ListCallback;
use crate::data_movement;
use crate::data_movement::backpressure::{self, DataMovementOperation};
use crate::error::{Error, Result};
use crate::object_api::{GetObjectReader, ObjectOptions};
use crate::set_disk::SetDisks;
use crate::storage_api_contracts::object::ObjectOperations as _;
use crate::store::ECStore;
use rustfs_filemeta::{FileInfo, MetaCacheEntry};
use std::sync::Arc;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

impl ECStore {
    async fn finish_rebalance_entry_after_cleanup(
        &self,
        pool_index: usize,
        bucket: &str,
        object: &str,
        stats_updates: &[&FileInfo],
        cleanup: impl std::future::Future<Output = std::result::Result<ObjectInfo, data_movement::SourceCleanupError>>,
    ) -> Result<RebalanceEntryCleanupResult> {
        // Persisted stats can complete a pool on restart, so source cleanup must resolve first.
        let cleanup_result = resolve_rebalance_entry_cleanup_delete_result(cleanup.await, bucket, object);
        let RebalanceEntryCleanupResult::Completed { warning } = cleanup_result else {
            return Ok(cleanup_result);
        };
        if let Some(message) = warning.as_ref()
            && let Err(err) = self
                .record_rebalance_cleanup_warning(pool_index, bucket, object, message.clone())
                .await
        {
            error!(
                event = EVENT_REBALANCE_ENTRY,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REBALANCE,
                pool_index,
                bucket,
                object,
                stage = "cleanup_source",
                error = ?err,
                "Failed to record rebalance source cleanup warning"
            );
        }

        resolve_rebalance_stats_update_result(
            self.update_pool_stats_batch(pool_index, bucket.to_string(), stats_updates)
                .await,
            pool_index,
            bucket,
            object,
        )?;

        Ok(RebalanceEntryCleanupResult::Completed { warning })
    }

    #[allow(unused_assignments)]
    #[tracing::instrument(skip(self, set))]
    async fn rebalance_entry(
        self: Arc<Self>,
        bucket: String,
        pool_index: usize,
        entry: MetaCacheEntry,
        set: Arc<SetDisks>,
        bucket_configs: Arc<RebalanceBucketConfigs>,
        // wk: Arc<Workers>,
    ) -> Result<RebalanceEntryOutcome> {
        debug!(
            event = EVENT_REBALANCE_ENTRY,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            bucket = %bucket,
            object = %entry.name,
            pool_index,
            state = "started",
            "Starting rebalance entry"
        );

        // defer!(|| async {
        //     warn!("rebalance_entry: defer give worker start");
        //     wk.give().await;
        //     warn!("rebalance_entry: defer give worker done");
        // });

        if entry.is_dir() {
            debug!(
                event = EVENT_REBALANCE_ENTRY,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REBALANCE,
                bucket = %bucket,
                object = %entry.name,
                pool_index,
                state = "skipped",
                reason = "directory_entry",
                "Skipped rebalance entry"
            );
            return Ok(RebalanceEntryOutcome::Completed);
        }

        if self.check_if_rebalance_done(pool_index).await {
            debug!(
                event = EVENT_REBALANCE_ENTRY,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REBALANCE,
                pool_index,
                bucket = %bucket,
                object = %entry.name,
                state = "skipped",
                reason = "pool_completed",
                "Skipped rebalance entry"
            );
            return Ok(RebalanceEntryOutcome::Completed);
        }

        let bucket_incarnation_fence = match bucket_configs.bucket_incarnation_id {
            Some(expected) => Some(self.acquire_bucket_incarnation_fence(&bucket, expected).await?),
            None => None,
        };

        let mut fivs =
            resolve_rebalance_file_info_versions_result(entry.file_info_versions(&bucket), bucket.as_str(), entry.name.as_str())?;

        fivs.versions
            .sort_by_key(|v| (v.mod_time.is_none(), std::cmp::Reverse(v.mod_time)));

        let mut rebalanced: usize = 0;
        let mut expired: usize = 0;
        let mut cleanup_preflight_allowed_missing = Vec::new();
        let mut stats_updates = Vec::with_capacity(fivs.versions.len());
        for version in fivs.versions.iter() {
            if crate::core::pools::should_skip_lifecycle_for_data_movement(
                self.clone(),
                &bucket,
                version,
                bucket_configs.lifecycle_config.as_ref(),
                bucket_configs.object_lock_config.as_ref(),
                true,
                &crate::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc::Rebal,
            )
            .await?
            {
                expired += 1;
                // The lifecycle expiry above physically deleted this version from the source set.
                // Record its identity so the source-cleanup preflight tolerates its absence,
                // mirroring decommission; otherwise the entry can never be cleaned up.
                cleanup_preflight_allowed_missing.push(data_movement::source_cleanup_version_identity(version));
                debug!(
                    event = EVENT_REBALANCE_ENTRY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    pool_index,
                    bucket = %bucket,
                    object = %version.name,
                    state = "skipped",
                    reason = "expired_by_lifecycle",
                    "Skipped rebalance version"
                );
                continue;
            }

            let remaining_versions = fivs.versions.len() - expired;
            if should_skip_rebalance_delete_marker(version, remaining_versions, bucket_configs.replication_config.is_some()) {
                rebalanced += 1;
                debug!(
                    event = EVENT_REBALANCE_ENTRY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    pool_index,
                    bucket = %bucket,
                    object = %version.name,
                    state = "skipped",
                    reason = "last_delete_marker_without_replication",
                    "Skipped rebalance version"
                );
                continue;
            }

            let version_id = version.version_id.map(|v| v.to_string());
            let expected_bucket_incarnation_id = bucket_configs.bucket_incarnation_id;
            let mut transfer = |src_pool_idx: usize, bucket: String, rd: GetObjectReader| {
                let store = self.clone();
                async move {
                    store
                        .rebalance_object(src_pool_idx, bucket, rd, expected_bucket_incarnation_id)
                        .await
                }
            };
            // Route delete-marker migration through the store layer so it lands on the
            // cross-pool target (excluding the source pool), not back onto the source set.
            let mut delete_marker = |bucket: String, object: String, opts: ObjectOptions| {
                let store = self.clone();
                async move { store.delete_object(&bucket, &object, opts).await }
            };
            let result = migrate_entry_version(
                &RebalanceMigrationBackend::new(set.as_ref(), self.as_ref()),
                bucket.clone(),
                pool_index,
                version,
                version_id.clone(),
                expected_bucket_incarnation_id,
                rebalance_max_attempts(),
                should_ignore_rebalance_data_usage_cache(bucket.as_str()),
                &mut transfer,
                &mut delete_marker,
            )
            .await;

            if result.ignored {
                if should_count_rebalance_version_complete(&result) {
                    rebalanced += 1;
                }
                debug!(
                    event = EVENT_REBALANCE_ENTRY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    pool_index,
                    bucket = %bucket,
                    object = %version.name,
                    state = "skipped",
                    reason = "already_deleted",
                    "Skipped rebalance version"
                );
                continue;
            }

            if result.failed {
                let err = resolve_rebalance_migrate_result_error(
                    result.error,
                    pool_index,
                    bucket.as_str(),
                    version.name.as_str(),
                    version_id.as_deref(),
                );
                error!(
                    "rebalance_entry {} Error rebalancing entry {}/{:?}: {:?}",
                    &bucket, &version.name, &version.version_id, err
                );
                if should_defer_rebalance_entry_failure(&err) {
                    let deferred_error = format!("{REBALANCE_DEFERRED_ENTRY_ERROR_PREFIX} {err}");
                    debug!(
                        event = EVENT_REBALANCE_ENTRY,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REBALANCE,
                        pool_index,
                        bucket = %bucket,
                        object = %version.name,
                        state = "deferred",
                        error = %err,
                        "Deferred rebalance entry after transient migration failure"
                    );
                    if let Err(stats_err) = self.update_rebalance_last_error(pool_index, deferred_error.clone()).await {
                        error!(
                            "rebalance_entry {} failed to record deferred transient failure for {}: {}",
                            &bucket, &entry.name, stats_err
                        );
                    }
                    return Ok(RebalanceEntryOutcome::Deferred {
                        last_error: deferred_error,
                    });
                }
                let entry_err =
                    with_rebalance_entry_context(result.stage.unwrap_or("migrate"), bucket.as_str(), version.name.as_str(), err);

                if !stats_updates.is_empty()
                    && let Err(stats_err) = self
                        .update_pool_stats_batch(pool_index, bucket.clone(), stats_updates.as_slice())
                        .await
                {
                    error!(
                        "rebalance_entry {} failed to update stats before returning migration error for {}: {}",
                        &bucket, &entry.name, stats_err
                    );
                }

                return Err(entry_err);
            }

            stats_updates.push(version);
            if should_count_rebalance_version_complete(&result) {
                rebalanced += 1;
            }
        }

        if should_cleanup_rebalance_source_entry(rebalanced, fivs.versions.len(), expired) {
            if bucket_incarnation_fence.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
                return Err(Error::other("rebalance bucket incarnation fence was lost before source cleanup"));
            }
            let cleanup_result = self
                .finish_rebalance_entry_after_cleanup(
                    pool_index,
                    bucket.as_str(),
                    entry.name.as_str(),
                    stats_updates.as_slice(),
                    data_movement::cleanup_source_entry_if_unchanged(
                        set.clone(),
                        bucket.as_str(),
                        entry.name.as_str(),
                        &fivs,
                        &cleanup_preflight_allowed_missing,
                        data_movement::SourceCleanupBucketFence {
                            expected_incarnation_id: bucket_configs.bucket_incarnation_id,
                            lifecycle_guard: bucket_incarnation_fence
                                .as_ref()
                                .and_then(|guard| guard.namespace_lock_guard()),
                            ..Default::default()
                        },
                        "rebalance",
                    ),
                )
                .await?;
            match cleanup_result {
                RebalanceEntryCleanupResult::Deferred { last_error } => {
                    debug!(
                        event = EVENT_REBALANCE_ENTRY,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REBALANCE,
                        pool_index,
                        bucket = %bucket,
                        object = %entry.name,
                        state = "deferred",
                        error = %last_error,
                        "Deferred rebalance entry after source cleanup conflict"
                    );
                    return Ok(RebalanceEntryOutcome::Deferred { last_error });
                }
                RebalanceEntryCleanupResult::Completed { warning: Some(message) } => {
                    warn!(
                        event = EVENT_REBALANCE_ENTRY,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REBALANCE,
                        pool_index,
                        bucket = %bucket,
                        object = %entry.name,
                        stage = "cleanup_source",
                        cleanup_status = "failed_ignored",
                        error = %message,
                        "Ignored rebalance source cleanup failure"
                    );
                }
                RebalanceEntryCleanupResult::Completed { warning: None } => {
                    debug!(
                        event = EVENT_REBALANCE_ENTRY,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REBALANCE,
                        pool_index,
                        bucket = %bucket,
                        object = %entry.name,
                        state = "source_deleted",
                        "Deleted rebalance source entry"
                    );
                }
            }
        } else if rebalanced != fivs.versions.len() || expired > 0 {
            warn!(
                event = EVENT_REBALANCE_ENTRY,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REBALANCE,
                pool_index,
                bucket = %bucket,
                object = %entry.name,
                rebalanced,
                total_versions = fivs.versions.len(),
                expired,
                state = "source_retained",
                "Rebalance source object retained"
            );

            resolve_rebalance_stats_update_result(
                self.update_pool_stats_batch(pool_index, bucket.clone(), stats_updates.as_slice())
                    .await,
                pool_index,
                bucket.as_str(),
                entry.name.as_str(),
            )?;
        }

        Ok(RebalanceEntryOutcome::Completed)
    }

    #[tracing::instrument(skip(self, rd))]
    async fn rebalance_object(
        self: Arc<Self>,
        pool_idx: usize,
        bucket: String,
        rd: GetObjectReader,
        expected_bucket_incarnation_id: Option<uuid::Uuid>,
    ) -> Result<()> {
        data_movement::migrate_object(self, pool_idx, bucket, rd, expected_bucket_incarnation_id, "rebalance_object").await
    }

    async fn update_rebalance_last_error(&self, pool_idx: usize, message: String) -> Result<()> {
        let mut rebalance_meta = self.rebalance_meta.write().await;
        let Some(meta) = rebalance_meta.as_mut() else {
            return Err(rebalance_metadata_not_initialized_error("record rebalance last error"));
        };
        let pool_count = meta.pool_stats.len();
        ensure_valid_rebalance_pool_index(pool_count, pool_idx)?;
        let Some(pool_stat) = meta.pool_stats.get_mut(pool_idx) else {
            return Err(invalid_rebalance_pool_index_error(pool_idx, pool_count));
        };

        pool_stat.info.last_error = Some(message);
        meta.last_refreshed_at = Some(OffsetDateTime::now_utc());
        Ok(())
    }

    #[tracing::instrument(skip(self, rx))]
    pub(super) async fn rebalance_bucket(
        self: &Arc<Self>,
        rx: CancellationToken,
        bucket: String,
        pool_index: usize,
    ) -> Result<RebalanceBucketOutcome> {
        ensure_valid_rebalance_pool_index(self.pools.len(), pool_index)?;

        // Placeholder for actual bucket rebalance logic
        debug!(
            event = EVENT_REBALANCE_BUCKET,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            pool_index,
            bucket = %bucket,
            state = "entry_scan_started",
            "Rebalance bucket entry scan started"
        );

        let pool = clone_arc_by_index(self.pools.as_slice(), pool_index, "invalid rebalance pool index")?;
        let bucket_configs = Arc::new(load_rebalance_bucket_configs(self, &bucket).await?);

        let mut jobs = Vec::new();
        let entry_error = Arc::new(tokio::sync::Mutex::new(None::<Error>));
        let entry_workers = Arc::new(tokio::sync::Semaphore::new(pool.disk_set.len().max(1)));

        for (set_idx, set) in pool.disk_set.iter().enumerate() {
            let entry_tasks = Arc::new(tokio::sync::Mutex::new(Vec::<RebalanceEntryTask>::new()));
            let rebalance_entry: ListCallback = Arc::new({
                let this = Arc::clone(self);
                let bucket = bucket.clone();
                let entry_error = entry_error.clone();
                let callback_rx = rx.clone();
                let set = set.clone();
                let bucket_configs = bucket_configs.clone();
                let entry_tasks = entry_tasks.clone();
                let entry_workers = entry_workers.clone();
                move |entry: MetaCacheEntry| {
                    let this = this.clone();
                    let bucket = bucket.clone();
                    let entry_error = entry_error.clone();
                    let callback_rx = callback_rx.clone();
                    let set = set.clone();
                    let bucket_configs = bucket_configs.clone();
                    let entry_tasks = entry_tasks.clone();
                    let entry_workers = entry_workers.clone();
                    Box::pin(async move {
                        if callback_rx.is_cancelled() {
                            return;
                        }
                        if entry_error.lock().await.is_some() {
                            return;
                        }

                        if let Err(err) = backpressure::wait_for_data_movement_admission(
                            DataMovementOperation::Rebalance,
                            pool_index,
                            &callback_rx,
                        )
                        .await
                        {
                            if matches!(err, Error::OperationCanceled) {
                                return;
                            }
                            error!("rebalance_entry: data movement admission failed: {err}");
                            let mut first_err = entry_error.lock().await;
                            if first_err.is_none() {
                                *first_err = Some(err);
                                callback_rx.cancel();
                            }
                            return;
                        }

                        let permit = tokio::select! {
                            _ = callback_rx.cancelled() => return,
                            permit = entry_workers.clone().acquire_owned() => match permit {
                                Ok(permit) => permit,
                                Err(err) => {
                                    error!("rebalance_entry: worker semaphore closed: {err}");
                                    return;
                                }
                            },
                        };

                        if entry_error.lock().await.is_some() {
                            return;
                        }

                        let task = tokio::spawn(async move {
                            let _permit = permit;
                            debug!(
                                event = EVENT_REBALANCE_ENTRY,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_REBALANCE,
                                set_index = set_idx,
                                state = "task_started",
                                "Started rebalance entry task"
                            );
                            let result = this.rebalance_entry(bucket, pool_index, entry, set, bucket_configs).await;
                            if let Err(err) = &result {
                                error!("rebalance_entry: rebalance entry failed: {err}");
                                let mut first_err = entry_error.lock().await;
                                if first_err.is_none() {
                                    *first_err = Some(err.clone());
                                    callback_rx.cancel();
                                }
                            }
                            debug!(
                                event = EVENT_REBALANCE_ENTRY,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_REBALANCE,
                                set_index = set_idx,
                                state = "task_completed",
                                "Completed rebalance entry task"
                            );
                            result
                        });

                        entry_tasks.lock().await.push(task);
                    })
                }
            });

            let set = set.clone();
            let rx = rx.clone();
            let bucket = bucket.clone();
            let entry_tasks = entry_tasks.clone();

            let job = tokio::spawn(async move {
                let list_rx = rx.clone();
                let list_bucket = bucket.clone();
                let list_result = run_rebalance_listing_with_retry(
                    rx,
                    bucket,
                    rebalance_entry,
                    set_idx,
                    rebalance_max_attempts(),
                    entry_tasks.clone(),
                    move |cb| {
                        let set = set.clone();
                        let rx = list_rx.clone();
                        let bucket = list_bucket.clone();
                        async move { set.list_objects_to_rebalance(rx, bucket, cb).await }
                    },
                )
                .await;
                let entry_result = wait_rebalance_entry_tasks(set_idx, entry_tasks).await;
                let result = list_result.and(entry_result);
                if let Err(err) = &result {
                    error!("Rebalance worker {} error: {}", set_idx, err);
                } else {
                    debug!(
                        event = EVENT_REBALANCE_STATE,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REBALANCE,
                        set_index = set_idx,
                        state = "worker_completed",
                        "Completed rebalance worker"
                    );
                }
                result
            });

            jobs.push((set_idx, job));
        }

        let mut worker_error: Option<Error> = None;
        let mut deferred_error: Option<String> = None;
        for (set_idx, job) in jobs {
            match resolve_rebalance_worker_result(set_idx, job.await) {
                Ok(Some(last_error)) if deferred_error.is_none() => {
                    deferred_error = Some(last_error);
                }
                Ok(_) => {}
                Err(err) if worker_error.is_none() => {
                    worker_error = Some(err);
                }
                Err(_) => {}
            }
        }
        let entry_error = entry_error.lock().await.clone();
        resolve_rebalance_bucket_error(entry_error, worker_error)?;
        if let Some(last_error) = deferred_error {
            return Ok(RebalanceBucketOutcome::Deferred { last_error });
        }

        debug!(
            event = EVENT_REBALANCE_BUCKET,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            pool_index,
            bucket = %bucket,
            state = "completed",
            "Finished rebalance bucket"
        );
        Ok(RebalanceBucketOutcome::Completed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::rebalance::{RebalStatus, RebalanceInfo, RebalanceMeta, RebalanceStats};
    use rustfs_filemeta::FileInfo;
    use time::OffsetDateTime;

    #[tokio::test]
    async fn rebalance_stats_wait_for_source_cleanup_result() {
        let endpoint_pools: crate::layout::endpoints::EndpointServerPools = Vec::new().into();
        let store = Arc::new(ECStore {
            id: uuid::Uuid::new_v4(),
            disk_map: std::collections::HashMap::new(),
            pools: Vec::new(),
            peer_sys: crate::cluster::rpc::S3PeerSys::new(&endpoint_pools),
            pool_meta: tokio::sync::RwLock::new(crate::core::pools::PoolMeta::default()),
            rebalance_meta: tokio::sync::RwLock::new(Some(RebalanceMeta {
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
            })),
            decommission_cancelers: tokio::sync::RwLock::new(Vec::new()),
            start_gate: tokio::sync::Mutex::new(()),
            pool_meta_save_gate: tokio::sync::Mutex::new(()),
            ctx: crate::runtime::instance::bootstrap_ctx(),
            bucket_fence_registry: std::sync::Arc::default(),
        });
        let mut version = FileInfo::new("object.bin", 4, 2);
        version.name = "object.bin".to_string();
        version.size = 128;
        version.is_latest = true;
        let warning_version = version.clone();
        let (release_cleanup, cleanup_released) = tokio::sync::oneshot::channel();

        let finish_store = Arc::clone(&store);
        let finish = tokio::spawn(async move {
            finish_store
                .finish_rebalance_entry_after_cleanup(0, "bucket", "object.bin", &[&version], async move {
                    cleanup_released.await.expect("cleanup release sender should remain alive");
                    Ok(ObjectInfo::default())
                })
                .await
        });

        tokio::task::yield_now().await;
        assert_eq!(
            store
                .rebalance_meta
                .read()
                .await
                .as_ref()
                .expect("rebalance metadata should exist")
                .pool_stats[0]
                .bytes,
            0,
            "stats must not become visible before source cleanup resolves"
        );

        release_cleanup.send(()).expect("cleanup waiter should remain alive");
        assert_eq!(
            finish
                .await
                .expect("finish task should not panic")
                .expect("finish should succeed"),
            RebalanceEntryCleanupResult::Completed { warning: None }
        );
        assert!(
            store
                .rebalance_meta
                .read()
                .await
                .as_ref()
                .expect("rebalance metadata should exist")
                .pool_stats[0]
                .bytes
                > 0,
            "stats should become visible after source cleanup resolves"
        );

        {
            let mut meta = store.rebalance_meta.write().await;
            meta.as_mut().expect("rebalance metadata should exist").pool_stats[0].bytes = 0;
        }
        let warning_result = store
            .finish_rebalance_entry_after_cleanup(0, "bucket", "object.bin", &[&warning_version], async {
                Err(Error::SlowDown.into())
            })
            .await
            .expect("cleanup warnings should not fail the completed migration");
        assert!(matches!(warning_result, RebalanceEntryCleanupResult::Completed { warning: Some(_) }));
        let meta = store.rebalance_meta.read().await;
        let pool_stats = &meta.as_ref().expect("rebalance metadata should exist").pool_stats[0];
        assert_eq!(pool_stats.cleanup_warnings.count, 1, "cleanup warning must block pool completion");
        assert!(pool_stats.bytes > 0, "completed migration bytes should still be recorded");
        drop(meta);

        {
            let mut meta = store.rebalance_meta.write().await;
            meta.as_mut().expect("rebalance metadata should exist").pool_stats[0].bytes = 0;
        }
        let deferred = store
            .finish_rebalance_entry_after_cleanup(0, "bucket", "object.bin", &[&warning_version], async {
                Err(data_movement::SourceCleanupError::SourceChanged)
            })
            .await
            .expect("source changes should defer cleanup without failing the worker");
        assert!(matches!(deferred, RebalanceEntryCleanupResult::Deferred { .. }));
        let meta = store.rebalance_meta.read().await;
        let pool_stats = &meta.as_ref().expect("rebalance metadata should exist").pool_stats[0];
        assert_eq!(pool_stats.bytes, 0, "deferred cleanup must not commit completion stats");
        assert_eq!(pool_stats.cleanup_warnings.count, 1, "deferred cleanup must not add a permanent warning");
    }
}
