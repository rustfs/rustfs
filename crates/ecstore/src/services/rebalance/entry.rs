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
    record_rebalance_error, resolve_rebalance_bucket_error, resolve_rebalance_entry_cleanup_delete_result,
    resolve_rebalance_file_info_versions_result, resolve_rebalance_migrate_result_error, resolve_rebalance_stats_update_result,
    resolve_rebalance_worker_result, retry_rebalance_metadata_access, run_rebalance_listing_with_retry,
    should_cleanup_rebalance_source_entry, should_count_rebalance_version_complete, should_defer_rebalance_entry_failure,
    should_skip_rebalance_delete_marker, wait_rebalance_entry_tasks, with_rebalance_entry_context,
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

fn ensure_rebalance_entry_active(cancel: &CancellationToken) -> Result<()> {
    if cancel.is_cancelled() {
        return Err(Error::OperationCanceled);
    }
    Ok(())
}

#[cfg(any(test, feature = "test-util"))]
tokio::task_local! {
    static REBALANCE_ENTRY_RUN_FENCE_BARRIER: (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
}

#[derive(Debug)]
struct RebalanceEntryTarget {
    bucket: String,
    pool_index: usize,
}

struct RebalanceEntryCleanupContext<'a> {
    run_guard: &'a super::control::RebalanceRunGuard,
    pool_index: usize,
    bucket: &'a str,
    object: &'a str,
    stats_updates: &'a [&'a FileInfo],
    expected_id: &'a str,
    cancel: &'a CancellationToken,
}

#[cfg(test)]
static REBALANCE_RUN_SIGNAL_TEST_FENCES: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, Arc<std::sync::atomic::AtomicBool>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
struct RebalanceRunSignalTestFence {
    rebalance_id: String,
    loss_handle: Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(test)]
impl RebalanceRunSignalTestFence {
    fn install(rebalance_id: &str) -> Self {
        let loss_handle = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let previous = REBALANCE_RUN_SIGNAL_TEST_FENCES
            .get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
            .lock()
            .expect("rebalance run signal test fence should not be poisoned")
            .insert(rebalance_id.to_string(), Arc::clone(&loss_handle));
        assert!(previous.is_none(), "rebalance run signal test fence must be unique");
        Self {
            rebalance_id: rebalance_id.to_string(),
            loss_handle,
        }
    }

    fn mark_lost(&self) {
        self.loss_handle.store(true, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(test)]
impl Drop for RebalanceRunSignalTestFence {
    fn drop(&mut self) {
        REBALANCE_RUN_SIGNAL_TEST_FENCES
            .get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
            .lock()
            .expect("rebalance run signal test fence should not be poisoned")
            .remove(&self.rebalance_id);
    }
}

#[cfg(test)]
fn attach_rebalance_run_signal_test_fence(
    rebalance_id: &str,
    signal: &Arc<rustfs_lock::distributed_lock::LockLostSignal>,
) -> Option<crate::object_api::NamespaceLockSignalTestFence> {
    let loss_handle = REBALANCE_RUN_SIGNAL_TEST_FENCES
        .get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
        .lock()
        .expect("rebalance run signal test fence should not be poisoned")
        .get(rebalance_id)
        .cloned()?;
    Some(crate::object_api::NamespaceLockSignalTestFence::install_with_loss_handle(
        signal,
        loss_handle,
    ))
}

impl ECStore {
    async fn finish_rebalance_entry_after_cleanup(
        &self,
        context: &RebalanceEntryCleanupContext<'_>,
        cleanup: impl std::future::Future<Output = std::result::Result<ObjectInfo, data_movement::SourceCleanupError>>,
    ) -> Result<RebalanceEntryCleanupResult> {
        // Persisted stats can complete a pool on restart, so source cleanup must resolve first.
        ensure_rebalance_entry_active(context.cancel)?;
        context.run_guard.ensure_held("rebalance source cleanup")?;
        let cleanup_result = cleanup.await;
        ensure_rebalance_entry_active(context.cancel)?;
        context.run_guard.ensure_held("rebalance source cleanup")?;
        let cleanup_result = resolve_rebalance_entry_cleanup_delete_result(cleanup_result, context.bucket, context.object);
        let RebalanceEntryCleanupResult::Completed { warning } = cleanup_result else {
            return Ok(cleanup_result);
        };
        if let Some(message) = warning.as_ref() {
            context.run_guard.ensure_held("record rebalance cleanup warning")?;
            let warning_result = self
                .record_rebalance_cleanup_warning(
                    context.pool_index,
                    context.bucket,
                    context.object,
                    message.clone(),
                    context.expected_id,
                )
                .await;
            context.run_guard.ensure_held("record rebalance cleanup warning")?;
            if let Err(err) = warning_result {
                error!(
                    event = EVENT_REBALANCE_ENTRY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    pool_index = context.pool_index,
                    bucket = context.bucket,
                    object = context.object,
                    stage = "cleanup_source",
                    error = ?err,
                    "Failed to record rebalance source cleanup warning"
                );
            }
        }

        context.run_guard.ensure_held("record rebalance entry stats")?;
        let stats_result = self
            .update_pool_stats_batch_for_rebalance(
                context.pool_index,
                context.bucket.to_string(),
                context.stats_updates,
                context.expected_id,
            )
            .await;
        context.run_guard.ensure_held("record rebalance entry stats")?;
        resolve_rebalance_stats_update_result(stats_result, context.pool_index, context.bucket, context.object)?;

        Ok(RebalanceEntryCleanupResult::Completed { warning })
    }

    #[allow(unused_assignments)]
    #[tracing::instrument(skip(self, set, target), fields(bucket = %target.bucket, pool_index = target.pool_index))]
    async fn rebalance_entry(
        self: Arc<Self>,
        target: RebalanceEntryTarget,
        entry: MetaCacheEntry,
        set: Arc<SetDisks>,
        bucket_configs: Arc<RebalanceBucketConfigs>,
        rebalance_id: Arc<str>,
        cancel: CancellationToken,
        // wk: Arc<Workers>,
    ) -> Result<RebalanceEntryOutcome> {
        let RebalanceEntryTarget { bucket, pool_index } = target;
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

        if self.check_if_rebalance_done(pool_index, rebalance_id.as_ref()).await? {
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

        // Entry lock order is bucket incarnation -> activation_gate -> rebalance.bin -> movement gate.
        // Target capacity admission can then acquire pool.bin under the run fence.
        // Stop waits for in-flight entries through cleanup, but not for entries admitted later.
        ensure_rebalance_entry_active(&cancel)?;
        let run_guard = retry_rebalance_metadata_access(Some(&cancel), rebalance_max_attempts(), || {
            self.rebalance_run_guard(rebalance_id.as_ref(), "rebalance entry")
        })
        .await?;
        #[cfg(any(test, feature = "test-util"))]
        if let Ok((arrived, release)) = REBALANCE_ENTRY_RUN_FENCE_BARRIER.try_with(Clone::clone) {
            arrived.notify_one();
            release.notified().await;
        }
        let lock_lost_signal = run_guard.lock_lost_signal();
        #[cfg(test)]
        let _run_signal_test_fence = lock_lost_signal
            .as_ref()
            .and_then(|signal| attach_rebalance_run_signal_test_fence(rebalance_id.as_ref(), signal));

        let mut rebalanced: usize = 0;
        let mut expired: usize = 0;
        let mut cleanup_preflight_allowed_missing = Vec::new();
        let mut cleanup_preflight_vanished = Vec::new();
        let mut cleanup_transferred_tier_versions = Vec::new();
        let mut stats_updates = Vec::with_capacity(fivs.versions.len());
        for version in fivs.versions.iter() {
            ensure_rebalance_entry_active(&cancel)?;
            run_guard.ensure_held("rebalance lifecycle mutation")?;
            let lifecycle_result = crate::core::pools::should_skip_lifecycle_for_data_movement(
                self.clone(),
                &bucket,
                version,
                bucket_configs.lifecycle_config.as_ref(),
                bucket_configs.object_lock_config.as_ref(),
                true,
                &crate::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc::Rebal,
                lock_lost_signal.clone(),
            )
            .await;
            ensure_rebalance_entry_active(&cancel)?;
            run_guard.ensure_held("rebalance lifecycle mutation")?;
            let expired_by_lifecycle = lifecycle_result?;
            if expired_by_lifecycle {
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
            let transfer_lock_lost_signal = lock_lost_signal.clone();
            let mut transfer = |src_pool_idx: usize, bucket: String, rd: GetObjectReader| {
                let store = self.clone();
                let lock_lost_signal = transfer_lock_lost_signal.clone();
                async move {
                    store
                        .rebalance_object(src_pool_idx, bucket, rd, expected_bucket_incarnation_id, lock_lost_signal)
                        .await
                }
            };
            // Route delete-marker migration through the store layer so it lands on the
            // cross-pool target (excluding the source pool), not back onto the source set.
            let delete_marker_lock_lost_signal = lock_lost_signal.clone();
            let mut delete_marker = |bucket: String, object: String, mut opts: ObjectOptions| {
                let store = self.clone();
                if let Some(signal) = delete_marker_lock_lost_signal.as_ref() {
                    opts.add_namespace_lock_lost_signal(Arc::clone(signal));
                }
                async move {
                    // Keep the full delete path on a fresh poll stack while retaining abort-on-drop cancellation.
                    let mut deletion = tokio::task::JoinSet::new();
                    deletion.spawn(async move { store.delete_object(&bucket, &object, opts).await });
                    deletion
                        .join_next()
                        .await
                        .ok_or_else(|| Error::other("rebalance delete-marker task was not started"))?
                        .map_err(|err| Error::other(format!("rebalance delete-marker task join error: {err}")))?
                }
            };
            run_guard.ensure_held("rebalance version migration")?;
            let result = migrate_entry_version(
                &RebalanceMigrationBackend::new(set.as_ref(), self.clone(), lock_lost_signal.clone()),
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
            ensure_rebalance_entry_active(&cancel)?;
            run_guard.ensure_held("rebalance version migration")?;

            if result.ignored {
                if result.cleanup_ignored {
                    cleanup_preflight_vanished.push(data_movement::source_cleanup_version_identity(version));
                }
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
                    run_guard.ensure_held("record rebalance last error")?;
                    if let Err(stats_err) = self
                        .update_rebalance_last_error(pool_index, deferred_error.clone(), rebalance_id.as_ref())
                        .await
                    {
                        error!(
                            "rebalance_entry {} failed to record deferred transient failure for {}: {}",
                            &bucket, &entry.name, stats_err
                        );
                    }
                    run_guard.ensure_held("record rebalance last error")?;
                    return Ok(RebalanceEntryOutcome::Deferred {
                        last_error: deferred_error,
                    });
                }
                let entry_err =
                    with_rebalance_entry_context(result.stage.unwrap_or("migrate"), bucket.as_str(), version.name.as_str(), err);

                if !stats_updates.is_empty() {
                    run_guard.ensure_held("record rebalance stats before migration error")?;
                    let stats_result = self
                        .update_pool_stats_batch_for_rebalance(
                            pool_index,
                            bucket.clone(),
                            stats_updates.as_slice(),
                            rebalance_id.as_ref(),
                        )
                        .await;
                    run_guard.ensure_held("record rebalance stats before migration error")?;
                    if let Err(stats_err) = stats_result {
                        error!(
                            "rebalance_entry {} failed to update stats before returning migration error for {}: {}",
                            &bucket, &entry.name, stats_err
                        );
                    }
                }

                return Err(entry_err);
            }

            stats_updates.push(version);
            if should_count_rebalance_version_complete(&result) {
                rebalanced += 1;
            }
            if result.moved && !result.superseded && version.transition_status == rustfs_filemeta::TRANSITION_COMPLETE {
                cleanup_transferred_tier_versions.push(data_movement::source_cleanup_version_identity(version));
            }
        }

        if should_cleanup_rebalance_source_entry(rebalanced, fivs.versions.len(), expired) {
            if bucket_incarnation_fence.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
                return Err(Error::other("rebalance bucket incarnation fence was lost before source cleanup"));
            }
            let cleanup_expected = data_movement::source_cleanup_expected_without_vanished(&fivs, &cleanup_preflight_vanished);
            let cleanup_result = self
                .finish_rebalance_entry_after_cleanup(
                    &RebalanceEntryCleanupContext {
                        run_guard: &run_guard,
                        pool_index,
                        bucket: bucket.as_str(),
                        object: entry.name.as_str(),
                        stats_updates: stats_updates.as_slice(),
                        expected_id: rebalance_id.as_ref(),
                        cancel: &cancel,
                    },
                    data_movement::cleanup_source_entry_if_unchanged(
                        set.clone(),
                        bucket.as_str(),
                        entry.name.as_str(),
                        &cleanup_expected,
                        &cleanup_preflight_allowed_missing,
                        &cleanup_transferred_tier_versions,
                        data_movement::SourceCleanupBucketFence {
                            expected_incarnation_id: bucket_configs.bucket_incarnation_id,
                            lifecycle_guard: bucket_incarnation_fence
                                .as_ref()
                                .and_then(|guard| guard.namespace_lock_guard()),
                            namespace_lock_lost_signal: lock_lost_signal.clone(),
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

            run_guard.ensure_held("record retained rebalance entry stats")?;
            let stats_result = self
                .update_pool_stats_batch_for_rebalance(
                    pool_index,
                    bucket.clone(),
                    stats_updates.as_slice(),
                    rebalance_id.as_ref(),
                )
                .await;
            run_guard.ensure_held("record retained rebalance entry stats")?;
            resolve_rebalance_stats_update_result(stats_result, pool_index, bucket.as_str(), entry.name.as_str())?;
        }

        ensure_rebalance_entry_active(&cancel)?;
        run_guard.ensure_held("rebalance entry completion")?;
        Ok(RebalanceEntryOutcome::Completed)
    }

    #[tracing::instrument(skip(self, rd))]
    async fn rebalance_object(
        self: Arc<Self>,
        pool_idx: usize,
        bucket: String,
        rd: GetObjectReader,
        expected_bucket_incarnation_id: Option<uuid::Uuid>,
        lock_lost_signal: Option<Arc<rustfs_lock::distributed_lock::LockLostSignal>>,
    ) -> Result<data_movement::DataMovementCopy> {
        data_movement::migrate_object_with_lock_lost_signal(
            self,
            pool_idx,
            bucket,
            rd,
            expected_bucket_incarnation_id,
            "rebalance_object",
            lock_lost_signal,
        )
        .await
    }

    async fn update_rebalance_last_error(&self, pool_idx: usize, message: String, expected_id: &str) -> Result<()> {
        let mut rebalance_meta = self.rebalance_meta.write().await;
        super::control::ensure_rebalance_worker_active(rebalance_meta.as_ref(), expected_id, "record rebalance last error")?;
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
        rebalance_id: Arc<str>,
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
                let rebalance_id = Arc::clone(&rebalance_id);
                move |entry: MetaCacheEntry| {
                    let this = this.clone();
                    let bucket = bucket.clone();
                    let entry_error = entry_error.clone();
                    let callback_rx = callback_rx.clone();
                    let set = set.clone();
                    let bucket_configs = bucket_configs.clone();
                    let entry_tasks = entry_tasks.clone();
                    let entry_workers = entry_workers.clone();
                    let rebalance_id = Arc::clone(&rebalance_id);
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
                            record_rebalance_error(&mut first_err, err);
                            callback_rx.cancel();
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
                            let result = this
                                .rebalance_entry(
                                    RebalanceEntryTarget { bucket, pool_index },
                                    entry,
                                    set,
                                    bucket_configs,
                                    rebalance_id,
                                    callback_rx.clone(),
                                )
                                .await;
                            if let Err(err) = &result {
                                error!("rebalance_entry: rebalance entry failed: {err}");
                                let mut first_err = entry_error.lock().await;
                                record_rebalance_error(&mut first_err, err.clone());
                                callback_rx.cancel();
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
                Err(err) => record_rebalance_error(&mut worker_error, err),
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

#[cfg(feature = "test-util")]
pub mod test_util {
    use super::super::{RebalStatus, RebalanceInfo, RebalanceMeta, RebalanceStats};
    use super::*;
    use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};
    use rustfs_filemeta::FileMeta;

    pub struct PausedRebalanceEntryTestFixture {
        _temp_dirs: Vec<tempfile::TempDir>,
        store: Arc<ECStore>,
        cancel: CancellationToken,
        entry_arrived: Arc<tokio::sync::Notify>,
        entry_release: Arc<tokio::sync::Notify>,
        stop_probe: super::super::control::RebalanceStopWaitProbe,
        entry_task: Option<tokio::task::JoinHandle<Result<RebalanceEntryOutcome>>>,
    }

    impl PausedRebalanceEntryTestFixture {
        pub async fn new(rebalance_id: &'static str) -> Self {
            let cancel = CancellationToken::new();
            let (_temp_dirs, store) = super::super::test_store_with_persisted_rebalance_meta(RebalanceMeta {
                id: rebalance_id.to_string(),
                percent_free_goal: 1.0,
                cancel: Some(cancel.clone()),
                pool_stats: vec![RebalanceStats {
                    participating: true,
                    init_capacity: 100,
                    buckets: vec!["bucket".to_string()],
                    info: RebalanceInfo {
                        start_time: Some(OffsetDateTime::now_utc()),
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await;
            let bucket = "bucket";
            let object = "delete-marker";
            let set = store.pools[0].get_disks_by_key(object);
            set.make_bucket(bucket, &MakeBucketOptions::default())
                .await
                .expect("source bucket should be created");
            set.delete_object(
                bucket,
                object,
                ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("source delete marker should be created");
            let source_versions = set
                .load_file_info_versions_exact(bucket, object)
                .await
                .expect("source metadata should be readable")
                .expect("source delete marker should exist");
            assert_eq!(source_versions.versions.len(), 1);
            assert!(source_versions.versions[0].deleted);
            let mut file_meta = FileMeta::new();
            for version in source_versions.versions {
                file_meta
                    .add_version(version)
                    .expect("source version should encode into a metacache entry");
            }
            let entry = MetaCacheEntry {
                name: object.to_string(),
                metadata: file_meta.marshal_msg().expect("source metadata should marshal"),
                cached: Some(file_meta),
                reusable: false,
            };
            let entry_arrived = Arc::new(tokio::sync::Notify::new());
            let entry_release = Arc::new(tokio::sync::Notify::new());
            let entry_store = Arc::clone(&store);
            let entry_set = Arc::clone(&set);
            let entry_cancel = cancel.clone();
            let entry_task = tokio::spawn(REBALANCE_ENTRY_RUN_FENCE_BARRIER.scope(
                (Arc::clone(&entry_arrived), Arc::clone(&entry_release)),
                async move {
                    entry_store
                        .rebalance_entry(
                            RebalanceEntryTarget {
                                bucket: bucket.to_string(),
                                pool_index: 0,
                            },
                            entry,
                            entry_set,
                            Arc::new(RebalanceBucketConfigs::default()),
                            Arc::from(rebalance_id),
                            entry_cancel,
                        )
                        .await
                },
            ));

            Self {
                _temp_dirs,
                store,
                cancel,
                entry_arrived,
                entry_release,
                stop_probe: super::super::control::RebalanceStopWaitProbe::install(rebalance_id),
                entry_task: Some(entry_task),
            }
        }

        pub fn store(&self) -> Arc<ECStore> {
            Arc::clone(&self.store)
        }

        pub async fn wait_until_entry_paused(&self) {
            tokio::time::timeout(std::time::Duration::from_secs(30), self.entry_arrived.notified())
                .await
                .expect("the entry should acquire its run fence before pausing");
        }

        pub async fn wait_until_admission_cancelled(&self) {
            tokio::time::timeout(std::time::Duration::from_secs(5), self.cancel.cancelled())
                .await
                .expect("admin stop should close admission before waiting for the entry guard");
        }

        pub async fn wait_until_stop_waiting_for_entry(&self) {
            self.stop_probe.wait_until_attempted().await;
        }

        pub fn release_entry(&self) {
            self.entry_release.notify_one();
        }

        pub async fn assert_entry_cancelled(&mut self) {
            let error = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                self.entry_task.take().expect("entry task should still be available"),
            )
            .await
            .expect("the cancelled entry should finish")
            .expect("entry task should not panic")
            .expect_err("the entry must observe stop cancellation");
            assert!(matches!(error, Error::OperationCanceled));
        }
    }

    impl Drop for PausedRebalanceEntryTestFixture {
        fn drop(&mut self) {
            self.entry_release.notify_one();
            if let Some(task) = self.entry_task.take() {
                task.abort();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::lifecycle::lifecycle::TRANSITION_COMPLETE;
    use crate::object_api::PutObjReader;
    use crate::services::rebalance::{RebalStatus, RebalanceInfo, RebalanceMeta, RebalanceStats};
    use crate::set_disk::{
        DeleteObjectCommitBarrier, MultipartCommitBarrier, MultipartCommitPause, PutObjectCommitBarrier, PutObjectCommitPause,
        TieredMetadataCommitBarrier,
    };
    use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};
    use crate::storage_api_contracts::multipart::{CompletePart, MultipartOperations as _};
    use crate::storage_api_contracts::namespace::NamespaceLocking as _;
    use crate::storage_api_contracts::object::ObjectIO as _;
    use http::HeaderMap;
    use rustfs_filemeta::{FileInfo, FileMeta, ObjectPartInfo, TransitionVersionState};
    use s3s::dto::{BucketLifecycleConfiguration, ExpirationStatus, LifecycleExpiration, LifecycleRule};
    use std::time::Duration as StdDuration;
    use time::OffsetDateTime;
    use tokio::io::AsyncReadExt;

    fn active_rebalance_meta(rebalance_id: &str) -> RebalanceMeta {
        RebalanceMeta {
            id: rebalance_id.to_string(),
            percent_free_goal: 1.0,
            cancel: Some(CancellationToken::new()),
            pool_stats: vec![
                RebalanceStats {
                    participating: true,
                    init_capacity: 100,
                    buckets: vec![crate::disk::RUSTFS_META_BUCKET.to_string()],
                    info: RebalanceInfo {
                        start_time: Some(OffsetDateTime::now_utc()),
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                RebalanceStats::default(),
            ],
            ..Default::default()
        }
    }

    async fn prepare_rebalance_test_volumes(store: &ECStore) {
        let opts = MakeBucketOptions {
            force_create: true,
            ..Default::default()
        };
        for pool in &store.pools {
            pool.make_bucket(crate::disk::RUSTFS_META_BUCKET, &opts)
                .await
                .expect("rebalance test metadata volume should exist on every set");
            pool.make_bucket(crate::disk::RUSTFS_META_MULTIPART_BUCKET, &opts)
                .await
                .expect("rebalance test multipart volume should exist on every set");
        }
    }

    fn expired_delete_marker_lifecycle() -> BucketLifecycleConfiguration {
        BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    expired_object_delete_marker: Some(true),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("expired-marker".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        }
    }

    async fn metacache_entry_from_source(set: &SetDisks, bucket: &str, object: &str) -> MetaCacheEntry {
        let source_versions = set
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("source metadata should be readable")
            .expect("source version should exist");
        let mut file_meta = FileMeta::new();
        for version in source_versions.versions {
            file_meta
                .add_version(version)
                .expect("source version should encode into a metacache entry");
        }
        MetaCacheEntry {
            name: object.to_string(),
            metadata: file_meta.marshal_msg().expect("source metadata should marshal"),
            cached: Some(file_meta),
            reusable: false,
        }
    }

    fn spawn_real_rebalance_entry(
        store: Arc<ECStore>,
        set: Arc<SetDisks>,
        entry: MetaCacheEntry,
        rebalance_id: &'static str,
        bucket_configs: Arc<RebalanceBucketConfigs>,
    ) -> tokio::task::JoinHandle<Result<RebalanceEntryOutcome>> {
        tokio::spawn(async move {
            store
                .rebalance_entry(
                    RebalanceEntryTarget {
                        bucket: crate::disk::RUSTFS_META_BUCKET.to_string(),
                        pool_index: 0,
                    },
                    entry,
                    set,
                    bucket_configs,
                    Arc::from(rebalance_id),
                    CancellationToken::new(),
                )
                .await
        })
    }

    async fn assert_real_entry_rejected_after_run_fence_loss(task: tokio::task::JoinHandle<Result<RebalanceEntryOutcome>>) {
        let error = tokio::time::timeout(StdDuration::from_secs(5), task)
            .await
            .expect("fenced real rebalance entry should finish")
            .expect("rebalance entry task should not panic")
            .expect_err("lost run fence must reject the entry commit");
        assert!(error.to_string().contains("run fence lost"), "unexpected fence error: {error}");
    }

    #[tokio::test]
    async fn rebalance_stats_wait_for_source_cleanup_result() {
        let rebalance_id = "rebalance-test";
        let (_temp_dirs, store) = crate::services::rebalance::test_store_with_persisted_rebalance_meta(RebalanceMeta {
            id: rebalance_id.to_string(),
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
        })
        .await;
        let mut version = FileInfo::new("object.bin", 4, 2);
        version.name = "object.bin".to_string();
        version.size = 128;
        version.is_latest = true;
        let warning_version = version.clone();
        let (release_cleanup, cleanup_released) = tokio::sync::oneshot::channel();
        let cancel = CancellationToken::new();

        let finish_store = Arc::clone(&store);
        let finish = tokio::spawn(async move {
            let run_guard = finish_store
                .rebalance_run_guard(rebalance_id, "rebalance source cleanup test")
                .await
                .expect("rebalance source cleanup test guard should be acquired");
            let stats_updates = [&version];
            let cleanup_context = RebalanceEntryCleanupContext {
                run_guard: &run_guard,
                pool_index: 0,
                bucket: "bucket",
                object: "object.bin",
                stats_updates: &stats_updates,
                expected_id: rebalance_id,
                cancel: &cancel,
            };
            finish_store
                .finish_rebalance_entry_after_cleanup(&cleanup_context, async move {
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
        let warning_guard = store
            .rebalance_run_guard(rebalance_id, "rebalance cleanup warning test")
            .await
            .expect("rebalance cleanup warning test guard should be acquired");
        let warning_cancel = CancellationToken::new();
        let warning_stats_updates = [&warning_version];
        let warning_cleanup_context = RebalanceEntryCleanupContext {
            run_guard: &warning_guard,
            pool_index: 0,
            bucket: "bucket",
            object: "object.bin",
            stats_updates: &warning_stats_updates,
            expected_id: rebalance_id,
            cancel: &warning_cancel,
        };
        let warning_result = store
            .finish_rebalance_entry_after_cleanup(&warning_cleanup_context, async { Err(Error::FileAccessDenied.into()) })
            .await
            .expect("cleanup warnings should not fail the completed migration");
        assert!(matches!(warning_result, RebalanceEntryCleanupResult::Completed { warning: Some(_) }));
        let meta = store.rebalance_meta.read().await;
        let pool_stats = &meta.as_ref().expect("rebalance metadata should exist").pool_stats[0];
        assert_eq!(pool_stats.cleanup_warnings.count, 1, "cleanup warning must block pool completion");
        assert!(pool_stats.bytes > 0, "completed migration bytes should still be recorded");
        drop(meta);
        drop(warning_guard);

        {
            let mut meta = store.rebalance_meta.write().await;
            meta.as_mut().expect("rebalance metadata should exist").pool_stats[0].bytes = 0;
        }
        let deferred_guard = store
            .rebalance_run_guard(rebalance_id, "rebalance cleanup deferral test")
            .await
            .expect("rebalance cleanup deferral test guard should be acquired");
        let deferred_cancel = CancellationToken::new();
        let deferred_stats_updates = [&warning_version];
        let deferred_cleanup_context = RebalanceEntryCleanupContext {
            run_guard: &deferred_guard,
            pool_index: 0,
            bucket: "bucket",
            object: "object.bin",
            stats_updates: &deferred_stats_updates,
            expected_id: rebalance_id,
            cancel: &deferred_cancel,
        };
        let deferred = store
            .finish_rebalance_entry_after_cleanup(&deferred_cleanup_context, async {
                Err(data_movement::SourceCleanupError::SourceChanged)
            })
            .await
            .expect("source changes should defer cleanup without failing the worker");
        assert!(matches!(deferred, RebalanceEntryCleanupResult::Deferred { .. }));
        let meta = store.rebalance_meta.read().await;
        let pool_stats = &meta.as_ref().expect("rebalance metadata should exist").pool_stats[0];
        assert_eq!(pool_stats.bytes, 0, "deferred cleanup must not commit completion stats");
        assert_eq!(pool_stats.cleanup_warnings.count, 1, "deferred cleanup must not add a permanent warning");
        drop(meta);

        let transient_guard = store
            .rebalance_run_guard(rebalance_id, "rebalance transient cleanup deferral test")
            .await
            .expect("rebalance transient cleanup deferral test guard should be acquired");
        let transient_cancel = CancellationToken::new();
        let transient_stats_updates = [&warning_version];
        let transient_cleanup_context = RebalanceEntryCleanupContext {
            run_guard: &transient_guard,
            pool_index: 0,
            bucket: "bucket",
            object: "object.bin",
            stats_updates: &transient_stats_updates,
            expected_id: rebalance_id,
            cancel: &transient_cancel,
        };
        let transient = store
            .finish_rebalance_entry_after_cleanup(&transient_cleanup_context, async { Err(Error::SlowDown.into()) })
            .await
            .expect("retryable cleanup failures should defer without failing the worker");
        assert!(matches!(transient, RebalanceEntryCleanupResult::Deferred { .. }));
        let meta = store.rebalance_meta.read().await;
        let pool_stats = &meta.as_ref().expect("rebalance metadata should exist").pool_stats[0];
        assert_eq!(pool_stats.bytes, 0, "retryable cleanup failures must not commit completion stats");
        assert_eq!(
            pool_stats.cleanup_warnings.count, 1,
            "retryable cleanup failures must not add a permanent warning"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn real_rebalance_entry_recovers_after_run_fence_lock_timeout() {
        assert_real_rebalance_entry_metadata_retry(EntryRetryAction::Complete).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn real_rebalance_entry_cancels_after_run_fence_lock_timeout() {
        assert_real_rebalance_entry_metadata_retry(EntryRetryAction::Cancel).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn real_rebalance_entry_rechecks_persisted_run_after_lock_timeout() {
        assert_real_rebalance_entry_metadata_retry(EntryRetryAction::ReplaceRun).await;
    }

    enum EntryRetryAction {
        Complete,
        Cancel,
        ReplaceRun,
    }

    async fn assert_real_rebalance_entry_metadata_retry(action: EntryRetryAction) {
        const REBALANCE_ID: &str = "rebalance-entry-lock-retry";
        let (_temp_dirs, store, _peer) = crate::services::rebalance::test_two_pool_stores_with_isolated_node_contexts(Some(
            active_rebalance_meta(REBALANCE_ID),
        ))
        .await;
        let bucket = crate::disk::RUSTFS_META_BUCKET;
        let object = "rebalance-entry-lock-retry-object";
        let payload = b"metadata contention must not fail a rebalance entry".repeat(1024);
        let source_set = store.pools[0].get_disks_by_key(object);
        let target_set = store.pools[1].get_disks_by_key(object);
        let opts = ObjectOptions {
            versioned: true,
            version_id: Some(uuid::Uuid::new_v4().to_string()),
            ..Default::default()
        };
        let source_before = source_set
            .put_object(bucket, object, &mut PutObjReader::from_vec(payload.clone()), &opts)
            .await
            .expect("source version should be written");
        let entry = metacache_entry_from_source(&source_set, bucket, object).await;
        let ns_lock = store.pools[0]
            .new_ns_lock(bucket, super::super::REBAL_META_NAME)
            .await
            .expect("metadata lock should be available");
        let writer = ns_lock
            .get_write_lock(crate::set_disk::get_lock_acquire_timeout())
            .await
            .expect("hold the metadata write lock");
        let retried = Arc::new(tokio::sync::Notify::new());
        let cancel = CancellationToken::new();
        let transfer = super::super::worker::REBALANCE_METADATA_RETRY_PROBE.scope(
            Arc::clone(&retried),
            Arc::clone(&store).rebalance_entry(
                RebalanceEntryTarget {
                    bucket: bucket.to_string(),
                    pool_index: 0,
                },
                entry,
                source_set,
                Arc::new(RebalanceBucketConfigs::default()),
                Arc::from(REBALANCE_ID),
                cancel.clone(),
            ),
        );
        tokio::pin!(transfer);
        tokio::time::timeout(StdDuration::from_secs(30), async {
            tokio::select! {
                _ = retried.notified() => {},
                result = &mut transfer => panic!("entry admission must retry its contended run fence: {result:?}"),
            }
        })
        .await
        .expect("entry should encounter a real metadata lock timeout");
        match action {
            EntryRetryAction::Complete => {}
            EntryRetryAction::Cancel => cancel.cancel(),
            EntryRetryAction::ReplaceRun => {
                let mut save_opts = ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                };
                save_opts.add_namespace_lock_guard(&writer);
                active_rebalance_meta("replacement-rebalance-run")
                    .save_with_opts(Arc::clone(&store.pools[0]), save_opts)
                    .await
                    .expect("a peer may replace the persisted run while holding the metadata writer");
            }
        }
        drop(writer);
        let result = tokio::time::timeout(StdDuration::from_secs(30), transfer)
            .await
            .expect("entry should finish after the metadata writer releases");
        if !matches!(action, EntryRetryAction::Complete) {
            let err = result.expect_err("a cancelled or replaced run must not resume migration");
            match action {
                EntryRetryAction::Cancel => assert!(matches!(err, Error::OperationCanceled)),
                EntryRetryAction::ReplaceRun => assert!(err.to_string().contains("stale rebalance run rejected")),
                EntryRetryAction::Complete => unreachable!(),
            }
            let mut reader = store.pools[0]
                .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
                .await
                .expect("rejected entry must retain its source");
            let mut actual = Vec::new();
            reader
                .stream
                .read_to_end(&mut actual)
                .await
                .expect("retained source must remain readable");
            assert_eq!(actual, payload);
            let target_err = target_set
                .get_object_info(bucket, object, &opts)
                .await
                .expect_err("rejected entry must not publish a target version");
            assert!(crate::error::is_err_object_not_found(&target_err) || crate::error::is_err_version_not_found(&target_err));
            return;
        }
        assert!(matches!(
            result.expect("transient metadata contention must not fail the entry"),
            RebalanceEntryOutcome::Completed
        ));
        let mut reader = target_set
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
            .expect("target version must be readable");
        let mut actual = Vec::new();
        reader
            .stream
            .read_to_end(&mut actual)
            .await
            .expect("target body should drain completely");
        assert_eq!(actual, payload);
        assert_eq!(reader.object_info.version_id, source_before.version_id);
        assert_eq!(reader.object_info.etag, source_before.etag);
        let source_error = store.pools[0]
            .get_object_info(bucket, object, &opts)
            .await
            .expect_err("completed migration should remove the source version");
        assert!(crate::error::is_err_object_not_found(&source_error) || crate::error::is_err_version_not_found(&source_error));
        let meta = store.rebalance_meta.read().await;
        let stats = &meta.as_ref().expect("run must remain installed").pool_stats[0];
        assert_eq!((stats.num_objects, stats.num_versions, stats.cleanup_warnings.count), (1, 1, 0));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn real_rebalance_peer_activates_while_entry_retains_run_fence() {
        const REBALANCE_ID: &str = "rebalance-peer-activation-lock-order";
        let (_temp_dirs, store, peer) = crate::services::rebalance::test_two_pool_stores_with_isolated_node_contexts(Some(
            active_rebalance_meta(REBALANCE_ID),
        ))
        .await;
        assert!(!Arc::ptr_eq(&store.ctx, &peer.ctx), "node-local movement gates must be independent");
        {
            let mut meta = peer.rebalance_meta.write().await;
            let meta = meta.as_mut().expect("peer should know the durable run");
            meta.activation_gate = Arc::default();
            meta.cancel = None;
        }
        let bucket = crate::disk::RUSTFS_META_BUCKET;
        let object = "rebalance-peer-activation-object";
        let version_id = uuid::Uuid::new_v4();
        let payload = b"peer activation must not wait for this entry to drain".repeat(1024);
        let source_set = store.pools[0].get_disks_by_key(object);
        let target_set = store.pools[1].get_disks_by_key(object);
        let opts = ObjectOptions {
            versioned: true,
            version_id: Some(version_id.to_string()),
            ..Default::default()
        };
        let mut writer = PutObjReader::from_vec(payload.clone());
        let source_before = source_set
            .put_object(bucket, object, &mut writer, &opts)
            .await
            .expect("source version should be written");
        let entry = metacache_entry_from_source(&source_set, bucket, object).await;
        let arrived = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        // JoinSet aborts both scoped tasks if an assertion or timeout fails.
        let mut tasks = tokio::task::JoinSet::new();
        let entry_store = Arc::clone(&store);
        tasks.spawn(
            REBALANCE_ENTRY_RUN_FENCE_BARRIER.scope((Arc::clone(&arrived), Arc::clone(&release)), async move {
                entry_store
                    .rebalance_entry(
                        RebalanceEntryTarget {
                            bucket: bucket.to_string(),
                            pool_index: 0,
                        },
                        entry,
                        source_set,
                        Arc::new(RebalanceBucketConfigs::default()),
                        Arc::from(REBALANCE_ID),
                        CancellationToken::new(),
                    )
                    .await
            }),
        );
        tokio::time::timeout(StdDuration::from_secs(30), arrived.notified())
            .await
            .expect("real entry must acquire its persisted run read fence");

        let attempted = Arc::new(tokio::sync::Notify::new());
        let peer_pool = Arc::clone(&peer.pools[0]);
        let (activation_done, activation_result) = tokio::sync::oneshot::channel();
        tasks.spawn(
            crate::core::pools::REBALANCE_ACTIVATION_LOCK_ATTEMPT.scope(Arc::clone(&attempted), async move {
                let result = peer.fence_rebalance_worker_activation(peer_pool, REBALANCE_ID).await;
                let result = result.map(|fence| match fence {
                    super::super::control::RebalanceWorkerActivationFence::Ready(fence) => {
                        fence.ensure_held().expect("peer activation must retain both fences");
                    }
                    super::super::control::RebalanceWorkerActivationFence::NotStartedTerminal => {
                        panic!("the paused entry's run must still require activation");
                    }
                });
                activation_done.send(result).expect("activation receiver should remain alive");
                Ok(RebalanceEntryOutcome::Completed)
            }),
        );
        tokio::time::timeout(StdDuration::from_secs(30), attempted.notified())
            .await
            .expect("peer activation must attempt the persisted rebalance fence");
        tokio::time::timeout(StdDuration::from_secs(30), activation_result)
            .await
            .expect("peer activation must finish while the entry remains paused")
            .expect("peer activation result should be sent")
            .expect("joining the same run must not require its active entry to release the run fence");
        release.notify_one();

        tokio::time::timeout(StdDuration::from_secs(30), async {
            while let Some(result) = tasks.join_next().await {
                assert!(matches!(
                    result
                        .expect("scoped task must not panic")
                        .expect("entry must not fail or defer"),
                    RebalanceEntryOutcome::Completed
                ));
            }
        })
        .await
        .expect("entry and peer activation must both make progress");

        let mut reader = target_set
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
            .expect("the exact target version must be readable");
        let mut actual = Vec::new();
        reader
            .stream
            .read_to_end(&mut actual)
            .await
            .expect("target body should drain completely");
        assert_eq!(actual, payload);
        assert_eq!(reader.object_info.version_id, source_before.version_id);
        assert_eq!(reader.object_info.etag, source_before.etag);
        assert_eq!(reader.object_info.mod_time, source_before.mod_time);
        let source_error = store.pools[0]
            .get_object_info(bucket, object, &opts)
            .await
            .expect_err("completed entry must clean up the source version");
        assert!(crate::error::is_err_object_not_found(&source_error) || crate::error::is_err_version_not_found(&source_error));
        let meta = store.rebalance_meta.read().await;
        let stats = &meta.as_ref().expect("local run must remain installed").pool_stats[0];
        assert_eq!(stats.num_objects, 1);
        assert_eq!(stats.num_versions, 1);
        assert_eq!(stats.cleanup_warnings.count, 0);
    }

    /// What sits above the pre-versioning null version in the null-version
    /// fixtures (backlog#2873).
    #[derive(Clone, Copy, Debug)]
    enum NullVersionTop {
        DeleteMarker,
        NewerVersion,
    }

    const NULL_VERSION_REBALANCE_ID: &str = "rebalance-null-version";

    /// One rebalancing entry: a null version written before versioning was
    /// enabled, with a delete marker or a newer version above it, on pool 0 of a
    /// two-pool store with an active run.
    struct NullVersionRebalance {
        _temp_dirs: Vec<tempfile::TempDir>,
        store: Arc<ECStore>,
        bucket: &'static str,
        object: &'static str,
        incarnation: uuid::Uuid,
        payload: Vec<u8>,
        seeded: rustfs_filemeta::FileInfoVersions,
        source_set: Arc<SetDisks>,
    }

    impl NullVersionRebalance {
        async fn new(top: NullVersionTop) -> Self {
            let (temp_dirs, store, _peer) =
                crate::services::rebalance::test_two_pool_stores_with_isolated_node_contexts(None).await;
            let bucket = "rebalance-null-version";
            let object = "pre-versioning.bin";
            // An active run blocks the data-usage publication bucket creation needs.
            Box::pin(store.make_bucket(bucket, &MakeBucketOptions::default()))
                .await
                .expect("create bucket");
            let incarnation = store.bucket_incarnation_id(bucket).await.expect("bucket incarnation");
            let rebalance_meta = active_rebalance_meta(NULL_VERSION_REBALANCE_ID);
            rebalance_meta
                .save(Arc::clone(&store.pools[0]))
                .await
                .expect("persist the active rebalance run");
            *store.rebalance_meta.write().await = Some(rebalance_meta);
            let payload = b"written before versioning was enabled".repeat(128);
            let source_set = store.pools[0].get_disks_by_key(object);
            Box::pin(source_set.put_object(
                bucket,
                object,
                &mut PutObjReader::from_vec(payload.clone()),
                &ObjectOptions {
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            ))
            .await
            .expect("seed null version");
            let versioned = ObjectOptions {
                versioned: true,
                expected_bucket_incarnation_id: Some(incarnation),
                ..Default::default()
            };
            match top {
                NullVersionTop::DeleteMarker => {
                    let marker = Box::pin(source_set.delete_object(bucket, object, versioned))
                        .await
                        .expect("seed delete marker over the null version");
                    assert!(marker.delete_marker);
                }
                NullVersionTop::NewerVersion => {
                    Box::pin(source_set.put_object(
                        bucket,
                        object,
                        &mut PutObjReader::from_vec(b"written after versioning was enabled".to_vec()),
                        &versioned,
                    ))
                    .await
                    .expect("seed newer version over the null version");
                }
            }
            let seeded = source_set
                .load_file_info_versions_exact(bucket, object)
                .await
                .expect("read seeded versions")
                .expect("seeded versions must exist");
            assert_eq!(seeded.versions.len(), 2);
            assert_eq!(seeded.versions[0].deleted, matches!(top, NullVersionTop::DeleteMarker));
            assert!(seeded.versions[0].version_id.is_some(), "the latest version must have a real id");
            assert!(seeded.versions[1].version_id.is_none(), "the oldest version must be the null version");
            Self {
                _temp_dirs: temp_dirs,
                store,
                bucket,
                object,
                incarnation,
                payload,
                seeded,
                source_set,
            }
        }

        async fn run_entry(&self) -> RebalanceEntryOutcome {
            let entry = metacache_entry_from_source(&self.source_set, self.bucket, self.object).await;
            Box::pin(Arc::clone(&self.store).rebalance_entry(
                RebalanceEntryTarget {
                    bucket: self.bucket.to_string(),
                    pool_index: 0,
                },
                entry,
                Arc::clone(&self.source_set),
                Arc::new(RebalanceBucketConfigs {
                    bucket_incarnation_id: Some(self.incarnation),
                    ..Default::default()
                }),
                Arc::from(NULL_VERSION_REBALANCE_ID),
                CancellationToken::new(),
            ))
            .await
            .expect("rebalance entry")
        }

        async fn source_versions(&self) -> Option<rustfs_filemeta::FileInfoVersions> {
            self.source_set
                .load_file_info_versions_exact(self.bucket, self.object)
                .await
                .expect("read source versions")
        }

        /// Both versions are on the target with their identities, the null
        /// version's content and SHA-256 are intact, and the source is gone.
        async fn assert_moved(&self) {
            let target = self.store.pools[1]
                .get_disks_by_key(self.object)
                .load_file_info_versions_exact(self.bucket, self.object)
                .await
                .expect("read target versions")
                .expect("versions must reach the target pool");
            assert_eq!(target.versions.len(), 2, "both versions must move");
            assert_eq!(target.versions[0].version_id, self.seeded.versions[0].version_id);
            assert_eq!(target.versions[0].deleted, self.seeded.versions[0].deleted);
            assert!(target.versions[1].version_id.is_none(), "the null version must keep its null identity");
            assert_eq!(target.versions[1].mod_time, self.seeded.versions[1].mod_time);

            let mut reader = self.store.pools[1]
                .get_object_reader(
                    self.bucket,
                    self.object,
                    None,
                    HeaderMap::new(),
                    &ObjectOptions {
                        version_id: Some(uuid::Uuid::nil().to_string()),
                        ..Default::default()
                    },
                )
                .await
                .expect("the migrated null version must be readable");
            let mut actual = Vec::new();
            reader
                .stream
                .read_to_end(&mut actual)
                .await
                .expect("drain the migrated null version");
            assert_eq!(
                rustfs_utils::crypto::hex_sha256(&actual, str::to_owned),
                rustfs_utils::crypto::hex_sha256(&self.payload, str::to_owned)
            );
            assert_eq!(actual, self.payload);
            assert!(
                self.source_versions().await.is_none(),
                "the source entry must be removed only after both versions moved"
            );
        }
    }

    /// A pre-versioning null version hidden behind a delete marker must reach
    /// the target pool intact before the source entry is removed (backlog#2873).
    #[tokio::test]
    #[serial_test::serial]
    async fn real_rebalance_migrates_null_version_behind_delete_marker() {
        let fixture = NullVersionRebalance::new(NullVersionTop::DeleteMarker).await;
        assert_eq!(fixture.run_entry().await, RebalanceEntryOutcome::Completed);
        fixture.assert_moved().await;
    }

    /// Overwriting after versioning was enabled leaves a newer version above
    /// the null version. The old `None` read selected that newer version, the
    /// target accepted it as an equivalent copy, and cleanup then deleted the
    /// null version without ever reporting it as missing.
    #[tokio::test]
    #[serial_test::serial]
    async fn real_rebalance_migrates_null_version_behind_newer_version() {
        let fixture = NullVersionRebalance::new(NullVersionTop::NewerVersion).await;
        assert_eq!(fixture.run_entry().await, RebalanceEntryOutcome::Completed);
        fixture.assert_moved().await;
    }

    /// Runs a case on a thread whose stack fits the transitioned copy path in
    /// debug builds; libtest's 2 MiB default does not under plain `cargo test`.
    fn run_large_stack_async_test<C, F>(name: &str, case: C)
    where
        C: FnOnce() -> F + Send + 'static,
        F: std::future::Future<Output = ()> + 'static,
    {
        const STACK_SIZE: usize = if cfg!(debug_assertions) {
            8 * rustfs_config::DEFAULT_THREAD_STACK_SIZE
        } else if cfg!(target_os = "macos") {
            2 * rustfs_config::DEFAULT_THREAD_STACK_SIZE
        } else {
            rustfs_config::DEFAULT_THREAD_STACK_SIZE
        };
        std::thread::Builder::new()
            .name(name.to_string())
            .stack_size(STACK_SIZE)
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(2)
                    .thread_stack_size(STACK_SIZE)
                    .build()
                    .expect("large-stack rebalance test runtime should build");
                runtime.block_on(case());
            })
            .expect("large-stack rebalance test thread should spawn")
            .join()
            .expect("large-stack rebalance test thread should complete");
    }

    const TRANSITIONED_REBALANCE_ID: &str = "rebalance-transitioned-source-cleanup";

    /// One transitioned source version on pool 0 of a two-pool store with an
    /// active rebalance run. A restored source keeps its local copy, so the
    /// rebalance moves it as data that keeps the transition tuple instead of
    /// as tier metadata (rustfs/backlog#2873, defect D).
    struct TransitionedRebalance {
        _temp_dirs: Vec<tempfile::TempDir>,
        store: Arc<ECStore>,
        bucket: &'static str,
        object: &'static str,
        versioned: bool,
        incarnation: uuid::Uuid,
        payload: Vec<u8>,
        source: FileInfo,
        source_set: Arc<SetDisks>,
        backend: crate::services::tier::test_util::MockWarmBackend,
    }

    impl TransitionedRebalance {
        async fn new(versioned: bool, restored: bool) -> Self {
            let (temp_dirs, store, _peer) =
                crate::services::rebalance::test_two_pool_stores_with_isolated_node_contexts(None).await;
            let bucket = "rebalance-transitioned";
            let object = "transitioned.bin";
            // An active run blocks the data-usage publication bucket creation needs.
            Box::pin(store.make_bucket(
                bucket,
                &MakeBucketOptions {
                    versioning_enabled: versioned,
                    ..Default::default()
                },
            ))
            .await
            .expect("create bucket");
            let incarnation = store.bucket_incarnation_id(bucket).await.expect("bucket incarnation");
            let tier_name = format!("REBALTIER{}", &uuid::Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
            let backend = crate::services::tier::test_util::register_mock_tier(&store.tier_config_mgr(), &tier_name).await;
            let payload = b"transitioned before the rebalance".to_vec();
            let source_set = store.pools[0].get_disks_by_key(object);
            let written = Box::pin(source_set.put_object(
                bucket,
                object,
                &mut PutObjReader::from_vec(payload.clone()),
                &ObjectOptions {
                    versioned,
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            ))
            .await
            .expect("seed the source version");
            let version_id = written.version_id.map(|version| version.to_string());
            Box::pin(store.pools[0].transition_object(
                bucket,
                object,
                &ObjectOptions {
                    versioned,
                    version_id: version_id.clone(),
                    transition: crate::bucket::lifecycle::lifecycle::TransitionOptions {
                        status: crate::bucket::lifecycle::lifecycle::TRANSITION_PENDING.to_string(),
                        tier: tier_name,
                        etag: written.etag.clone().expect("the source should have an ETag"),
                        ..Default::default()
                    },
                    mod_time: written.mod_time,
                    ..Default::default()
                },
            ))
            .await
            .expect("transition the source before the rebalance");
            if restored {
                Box::pin(Arc::clone(&store).restore_transitioned_object(
                    bucket,
                    object,
                    &ObjectOptions {
                        versioned,
                        version_id,
                        transition: crate::bucket::lifecycle::lifecycle::TransitionOptions {
                            restore_request: s3s::dto::RestoreRequest {
                                days: Some(1),
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                ))
                .await
                .expect("restore the transitioned source");
            }
            let rebalance_meta = active_rebalance_meta(TRANSITIONED_REBALANCE_ID);
            rebalance_meta
                .save(Arc::clone(&store.pools[0]))
                .await
                .expect("persist the active rebalance run");
            *store.rebalance_meta.write().await = Some(rebalance_meta);

            let seeded = source_set
                .load_file_info_versions_exact(bucket, object)
                .await
                .expect("read seeded versions")
                .expect("the transitioned source must exist");
            assert_eq!(seeded.versions.len(), 1);
            let source = seeded.versions[0].clone();
            assert_eq!(source.transition_status, TRANSITION_COMPLETE);
            assert_eq!(source.version_id.is_some(), versioned);
            assert_eq!(!source.is_remote(), restored, "a restored source keeps its local copy");
            assert!(
                backend.metadata(&source.transitioned_objname).await.is_some(),
                "the transition must upload the remote object"
            );
            Self {
                _temp_dirs: temp_dirs,
                store,
                bucket,
                object,
                versioned,
                incarnation,
                payload,
                source,
                source_set,
                backend,
            }
        }

        async fn run_entry(&self) -> RebalanceEntryOutcome {
            let entry = metacache_entry_from_source(&self.source_set, self.bucket, self.object).await;
            Box::pin(Arc::clone(&self.store).rebalance_entry(
                RebalanceEntryTarget {
                    bucket: self.bucket.to_string(),
                    pool_index: 0,
                },
                entry,
                Arc::clone(&self.source_set),
                Arc::new(RebalanceBucketConfigs {
                    bucket_incarnation_id: Some(self.incarnation),
                    ..Default::default()
                }),
                Arc::from(TRANSITIONED_REBALANCE_ID),
                CancellationToken::new(),
            ))
            .await
            .expect("rebalance entry")
        }

        async fn versions_on(&self, pool_index: usize) -> Vec<FileInfo> {
            self.store.pools[pool_index]
                .get_disks_by_key(self.object)
                .load_file_info_versions_exact(self.bucket, self.object)
                .await
                .expect("read pool versions")
                .map(|versions| versions.versions)
                .unwrap_or_default()
        }

        fn assert_same_remote_tuple(&self, version: &FileInfo) {
            assert_eq!(version.transition_tier, self.source.transition_tier);
            assert_eq!(version.transitioned_objname, self.source.transitioned_objname);
            assert_eq!(version.transition_version_id, self.source.transition_version_id);
            assert_eq!(
                crate::services::tier::tier::tier_destination_id_from_metadata(&version.metadata)
                    .expect("the copy's destination identity should decode"),
                crate::services::tier::tier::tier_destination_id_from_metadata(&self.source.metadata)
                    .expect("the source destination identity should decode"),
            );
        }

        async fn assert_remote_object_kept(&self) {
            assert!(
                self.backend.metadata(&self.source.transitioned_objname).await.is_some(),
                "the remote object must stay in the tier"
            );
            assert_eq!(self.backend.remove_count().await, 0, "no remote delete may be issued");
            assert_eq!(self.backend.exact_remove_count(), 0, "no remote delete may be issued");
        }

        /// The target owns the version and its remote object, and the source
        /// keeps neither the version nor a tier free version.
        async fn assert_moved(&self) {
            let left = self.versions_on(0).await;
            assert!(left.is_empty(), "the source must keep no version or tier free version: {left:?}");
            let target = self.versions_on(1).await;
            assert_eq!(target.len(), 1, "the target must hold the version and no tier free version: {target:?}");
            let moved = &target[0];
            assert!(!moved.tier_free_version());
            assert_eq!(moved.version_id, self.source.version_id);
            assert_eq!(moved.transition_status, TRANSITION_COMPLETE);
            self.assert_same_remote_tuple(moved);
            self.assert_remote_object_kept().await;

            let mut reader = self.store.pools[1]
                .get_object_reader(
                    self.bucket,
                    self.object,
                    None,
                    HeaderMap::new(),
                    &ObjectOptions {
                        versioned: self.versioned,
                        version_id: self.source.version_id.map(|version| version.to_string()),
                        ..Default::default()
                    },
                )
                .await
                .expect("the moved version should be readable");
            let mut body = Vec::new();
            reader.stream.read_to_end(&mut body).await.expect("drain the moved version");
            assert_eq!(body, self.payload);
        }
    }

    #[test]
    #[serial_test::serial]
    fn real_rebalance_of_a_transitioned_version_leaves_no_source_free_version() {
        run_large_stack_async_test("rebalance-transitioned-version", || async {
            let fixture = TransitionedRebalance::new(true, false).await;
            assert_eq!(fixture.run_entry().await, RebalanceEntryOutcome::Completed);
            fixture.assert_moved().await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn real_rebalance_of_a_transitioned_null_version_leaves_no_source_free_version() {
        run_large_stack_async_test("rebalance-transitioned-null-version", || async {
            let fixture = TransitionedRebalance::new(false, false).await;
            assert_eq!(fixture.run_entry().await, RebalanceEntryOutcome::Completed);
            fixture.assert_moved().await;
        });
    }

    /// A restored source moves as data; the copy keeps its transition tuple,
    /// so the target owns the remote object as well.
    #[test]
    #[serial_test::serial]
    fn real_rebalance_of_a_restored_transitioned_version_moves_its_remote_owner() {
        run_large_stack_async_test("rebalance-restored-transitioned-version", || async {
            let fixture = TransitionedRebalance::new(true, true).await;
            assert_eq!(fixture.run_entry().await, RebalanceEntryOutcome::Completed);
            fixture.assert_moved().await;
        });
    }

    /// A newer unversioned object written to the target replaces a restored
    /// null source instead of receiving a copy of it. Nothing references the
    /// source's remote object any more, so its source cleanup must keep a tier
    /// free version to delete it.
    #[test]
    #[serial_test::serial]
    fn real_rebalance_keeps_a_free_version_for_a_restored_null_version_superseded_on_target() {
        run_large_stack_async_test("rebalance-superseded-restored-null-version", || async {
            let fixture = TransitionedRebalance::new(false, true).await;
            let newer = b"written to the target after the source".to_vec();
            Box::pin(fixture.store.pools[1].get_disks_by_key(fixture.object).put_object(
                fixture.bucket,
                fixture.object,
                &mut PutObjReader::from_vec(newer.clone()),
                &ObjectOptions {
                    expected_bucket_incarnation_id: Some(fixture.incarnation),
                    ..Default::default()
                },
            ))
            .await
            .expect("write a newer null version on the target");

            assert_eq!(fixture.run_entry().await, RebalanceEntryOutcome::Completed);

            let left = fixture.versions_on(0).await;
            assert_eq!(left.len(), 1, "the source must keep one tier free version: {left:?}");
            assert!(left[0].tier_free_version());
            fixture.assert_same_remote_tuple(&left[0]);
            let target = fixture.versions_on(1).await;
            assert_eq!(target.len(), 1, "the target keeps only its newer version: {target:?}");
            assert!(!target[0].tier_free_version());
            assert!(target[0].transition_status.is_empty(), "the newer version references no remote object");
            let mut reader = fixture.store.pools[1]
                .get_object_reader(fixture.bucket, fixture.object, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("the newer target version should be readable");
            let mut body = Vec::new();
            reader.stream.read_to_end(&mut body).await.expect("drain the newer version");
            assert_eq!(body, newer);
            fixture.assert_remote_object_kept().await;
        });
    }

    /// A version the mover reports as not found while it still exists must not
    /// be deleted with the entry; cleanup is deferred and the source retained.
    #[tokio::test]
    #[serial_test::serial]
    async fn real_rebalance_retains_source_when_null_version_read_reports_not_found() {
        let fixture = NullVersionRebalance::new(NullVersionTop::DeleteMarker).await;
        let object = fixture.object;
        let fault: super::super::migration::RebalanceSourceReadFault = Arc::new(move |bucket, read| {
            (read == object).then(|| Error::VersionNotFound(bucket.to_string(), read.to_string(), "null".to_string()))
        });
        let outcome = super::super::migration::REBALANCE_SOURCE_READ_FAULT
            .scope(fault, fixture.run_entry())
            .await;
        let RebalanceEntryOutcome::Deferred { last_error } = outcome else {
            panic!("cleanup must be deferred while the reported-missing version exists: {outcome:?}");
        };
        assert!(last_error.contains("source changed"), "{last_error}");
        let source = fixture.source_versions().await.expect("the source entry must be retained");
        assert_eq!(source.versions.len(), 2, "every source version must be retained");
        assert!(source.versions[1].version_id.is_none());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn real_rebalance_run_fence_loss_before_target_commit_preserves_target_and_source() {
        let rebalance_id = "rebalance-target-commit-fence";
        let (_temp_dirs, store, _unused_store) = crate::services::rebalance::test_two_pool_stores(Some(RebalanceMeta {
            id: rebalance_id.to_string(),
            percent_free_goal: 1.0,
            cancel: Some(CancellationToken::new()),
            pool_stats: vec![
                RebalanceStats {
                    participating: true,
                    init_capacity: 100,
                    buckets: vec![crate::disk::RUSTFS_META_BUCKET.to_string()],
                    info: RebalanceInfo {
                        start_time: Some(OffsetDateTime::now_utc()),
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                RebalanceStats::default(),
            ],
            ..Default::default()
        }))
        .await;
        let bucket = crate::disk::RUSTFS_META_BUCKET;
        let object = "rebalance-commit-fence-object";
        let version_id = uuid::Uuid::new_v4();
        let payload = b"rebalance target commit must not survive its run fence".repeat(1024);
        let source_set = store.pools[0].get_disks_by_key(object);
        let target_set = store.pools[1].get_disks_by_key(object);
        let mut writer = PutObjReader::from_vec(payload.clone());
        let source_before = source_set
            .put_object(
                bucket,
                object,
                &mut writer,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(version_id.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("source version should be written");
        let source_versions = source_set
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("source metadata should be readable")
            .expect("source version should exist");
        let mut file_meta = FileMeta::new();
        for version in &source_versions.versions {
            file_meta
                .add_version(version.clone())
                .expect("source version should encode into a metacache entry");
        }
        let entry = MetaCacheEntry {
            name: object.to_string(),
            metadata: file_meta.marshal_msg().expect("source metadata should marshal"),
            cached: Some(file_meta),
            reusable: false,
        };
        let run_signal_fence = RebalanceRunSignalTestFence::install(rebalance_id);
        let barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::BeforeQuotaRename);
        let entry_store = Arc::clone(&store);
        let entry_set = Arc::clone(&source_set);
        let mut entry_task = tokio::spawn(async move {
            entry_store
                .rebalance_entry(
                    RebalanceEntryTarget {
                        bucket: bucket.to_string(),
                        pool_index: 0,
                    },
                    entry,
                    entry_set,
                    Arc::new(RebalanceBucketConfigs::default()),
                    Arc::from(rebalance_id),
                    CancellationToken::new(),
                )
                .await
        });
        barrier.wait_until_paused().await;
        run_signal_fence.mark_lost();
        barrier.release();
        drop(barrier);
        let entry_error = tokio::time::timeout(StdDuration::from_secs(5), &mut entry_task)
            .await
            .expect("fenced real rebalance entry should finish")
            .expect("rebalance entry task should not panic")
            .expect_err("lost run fence must reject the target commit");
        assert!(entry_error.to_string().contains("run fence lost"));
        let target_error = target_set
            .get_object_info(
                bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(version_id.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect_err("target version must remain absent after the lost commit fence");
        assert!(
            crate::error::is_err_object_not_found(&target_error) || crate::error::is_err_version_not_found(&target_error),
            "target version must remain absent after the lost commit fence"
        );
        let mut source_body = Vec::new();
        let mut source_after = source_set
            .get_object_reader(
                bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(version_id.to_string()),
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("source version must remain readable");
        assert_eq!(source_after.object_info.version_id, source_before.version_id);
        assert_eq!(source_after.object_info.data_dir, source_before.data_dir);
        assert_eq!(source_after.object_info.mod_time, source_before.mod_time);
        assert_eq!(source_after.object_info.size, source_before.size);
        assert_eq!(source_after.object_info.etag, source_before.etag);
        source_after
            .stream
            .read_to_end(&mut source_body)
            .await
            .expect("source body should drain");
        assert_eq!(source_body, payload, "source version must remain byte-identical");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn real_rebalance_run_fence_loss_blocks_multipart_publication() {
        const REBALANCE_ID: &str = "rebalance-multipart-commit-fence";
        let bucket = crate::disk::RUSTFS_META_BUCKET;
        for (object, pause, staged_commit) in [
            (
                "rebalance-multipart-new-upload-fence",
                MultipartCommitPause::NewUploadBeforeLockLost,
                Some("upload metadata"),
            ),
            (
                "rebalance-multipart-part-fence",
                MultipartCommitPause::PutPartBeforeLockLost,
                Some("part"),
            ),
            ("rebalance-multipart-completion-fence", MultipartCommitPause::BeforeQuotaRename, None),
        ] {
            let (_temp_dirs, store, _unused_store) =
                crate::services::rebalance::test_two_pool_stores(Some(active_rebalance_meta(REBALANCE_ID))).await;
            prepare_rebalance_test_volumes(store.as_ref()).await;
            let source_set = store.pools[0].get_disks_by_key(object);
            let target_set = store.pools[1].get_disks_by_key(object);
            let upload = source_set
                .new_multipart_upload(bucket, object, &ObjectOptions::default())
                .await
                .expect("source multipart upload should be created");
            let mut reader = PutObjReader::from_vec(b"multipart source payload".repeat(1024));
            let part = source_set
                .put_object_part(bucket, object, &upload.upload_id, 1, &mut reader, &ObjectOptions::default())
                .await
                .expect("source multipart part should be written");
            source_set
                .clone()
                .complete_multipart_upload(
                    bucket,
                    object,
                    &upload.upload_id,
                    vec![CompletePart {
                        part_num: part.part_num,
                        etag: part.etag,
                        ..Default::default()
                    }],
                    &ObjectOptions::default(),
                )
                .await
                .expect("source multipart object should commit");

            let entry = metacache_entry_from_source(source_set.as_ref(), bucket, object).await;
            let run_signal_fence = RebalanceRunSignalTestFence::install(REBALANCE_ID);
            let barrier = MultipartCommitBarrier::install(bucket, object, pause);
            let task = spawn_real_rebalance_entry(
                Arc::clone(&store),
                Arc::clone(&source_set),
                entry,
                REBALANCE_ID,
                Arc::new(RebalanceBucketConfigs::default()),
            );
            barrier.wait_until_paused().await;
            run_signal_fence.mark_lost();
            barrier.release();
            assert_real_entry_rejected_after_run_fence_loss(task).await;

            if let Some(staged_commit) = staged_commit {
                assert!(
                    !barrier.commit_observed(),
                    "lost run fence must not publish target multipart {staged_commit}"
                );
            }
            drop(barrier);
            assert!(
                target_set
                    .load_file_info_versions_exact(bucket, object)
                    .await
                    .expect("target metadata lookup should succeed")
                    .is_none(),
                "lost run fence must not publish the target multipart object"
            );
            assert!(
                source_set
                    .load_file_info_versions_exact(bucket, object)
                    .await
                    .expect("source metadata lookup should succeed")
                    .is_some(),
                "lost run fence must preserve the source multipart object"
            );
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn real_rebalance_run_fence_loss_blocks_remote_tier_metadata_commit() {
        const REBALANCE_ID: &str = "rebalance-tiered-commit-fence";
        let bucket = crate::disk::RUSTFS_META_BUCKET;
        let object = "rebalance-tiered-commit-fence-object";
        let (_temp_dirs, store, _unused_store) =
            crate::services::rebalance::test_two_pool_stores(Some(active_rebalance_meta(REBALANCE_ID))).await;
        prepare_rebalance_test_volumes(store.as_ref()).await;
        crate::services::tier::test_util::register_mock_tier(&store.tier_config_mgr(), "WARM").await;
        let source_set = store.pools[0].get_disks_by_key(object);
        let target_set = store.pools[1].get_disks_by_key(object);
        let version_id = uuid::Uuid::new_v4();
        let source = FileInfo {
            volume: bucket.to_string(),
            name: object.to_string(),
            version_id: Some(version_id),
            mod_time: Some(OffsetDateTime::now_utc()),
            size: 32,
            parts: vec![ObjectPartInfo {
                number: 1,
                size: 32,
                actual_size: 32,
                etag: "tiered-part".to_string(),
                ..Default::default()
            }],
            transition_status: TRANSITION_COMPLETE.to_string(),
            transition_tier: "WARM".to_string(),
            transitioned_objname: "remote/rebalance-tiered-commit-fence-object".to_string(),
            transition_version: Some("remote-version".to_string()),
            transition_version_state: TransitionVersionState::Exact,
            fresh: true,
            ..Default::default()
        };
        source_set
            .decommission_tiered_object(
                bucket,
                object,
                &source,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(version_id.to_string()),
                    mod_time: source.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("source tiered metadata should be written");

        let entry = metacache_entry_from_source(source_set.as_ref(), bucket, object).await;
        let run_signal_fence = RebalanceRunSignalTestFence::install(REBALANCE_ID);
        let barrier = TieredMetadataCommitBarrier::install(bucket, object);
        let mut task = spawn_real_rebalance_entry(
            Arc::clone(&store),
            Arc::clone(&source_set),
            entry,
            REBALANCE_ID,
            Arc::new(RebalanceBucketConfigs::default()),
        );
        tokio::select! {
            _ = barrier.wait_until_paused() => {}
            result = &mut task => panic!("rebalance exited before the tiered commit barrier: {result:?}"),
        }
        run_signal_fence.mark_lost();
        barrier.release();
        drop(barrier);
        assert_real_entry_rejected_after_run_fence_loss(task).await;

        assert!(
            target_set
                .load_file_info_versions_exact(bucket, object)
                .await
                .expect("target metadata lookup should succeed")
                .is_none(),
            "lost run fence must not publish target tier metadata"
        );
        assert!(
            source_set
                .load_file_info_versions_exact(bucket, object)
                .await
                .expect("source metadata lookup should succeed")
                .is_some(),
            "lost run fence must preserve source tier metadata"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn real_rebalance_run_fence_loss_blocks_delete_marker_commit() {
        const REBALANCE_ID: &str = "rebalance-delete-marker-commit-fence";
        let bucket = crate::disk::RUSTFS_META_BUCKET;
        let object = "rebalance-delete-marker-commit-fence-object";
        let (_temp_dirs, store, _unused_store) =
            crate::services::rebalance::test_two_pool_stores(Some(active_rebalance_meta(REBALANCE_ID))).await;
        prepare_rebalance_test_volumes(store.as_ref()).await;
        let source_set = store.pools[0].get_disks_by_key(object);
        let target_set = store.pools[1].get_disks_by_key(object);
        let mut reader = PutObjReader::from_vec(b"source version beneath delete marker".to_vec());
        source_set
            .put_object(
                bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("source version should be written");
        source_set
            .delete_object(
                bucket,
                object,
                ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("source delete marker should be written");

        let entry = metacache_entry_from_source(source_set.as_ref(), bucket, object).await;
        let run_signal_fence = RebalanceRunSignalTestFence::install(REBALANCE_ID);
        let barrier = DeleteObjectCommitBarrier::install(bucket, object);
        let task = spawn_real_rebalance_entry(
            Arc::clone(&store),
            Arc::clone(&source_set),
            entry,
            REBALANCE_ID,
            Arc::new(RebalanceBucketConfigs::default()),
        );
        barrier.wait_until_paused().await;
        run_signal_fence.mark_lost();
        barrier.release();
        drop(barrier);
        assert_real_entry_rejected_after_run_fence_loss(task).await;

        assert!(
            target_set
                .load_file_info_versions_exact(bucket, object)
                .await
                .expect("target metadata lookup should succeed")
                .is_none(),
            "lost run fence must not publish the target delete marker"
        );
        assert_eq!(
            source_set
                .load_file_info_versions_exact(bucket, object)
                .await
                .expect("source metadata lookup should succeed")
                .expect("source versions should remain")
                .versions
                .len(),
            2,
            "lost run fence must preserve both source versions"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn real_rebalance_run_fence_loss_blocks_lifecycle_mutation_dispatch() {
        const REBALANCE_ID: &str = "rebalance-lifecycle-mutation-fence";
        let bucket = crate::disk::RUSTFS_META_BUCKET;
        let object = "rebalance-lifecycle-mutation-fence-object";
        let (_temp_dirs, store, _unused_store) =
            crate::services::rebalance::test_two_pool_stores(Some(active_rebalance_meta(REBALANCE_ID))).await;
        prepare_rebalance_test_volumes(store.as_ref()).await;
        let source_set = store.pools[0].get_disks_by_key(object);
        source_set
            .delete_object(
                bucket,
                object,
                ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("source delete marker should be written");

        let entry = metacache_entry_from_source(source_set.as_ref(), bucket, object).await;
        let run_signal_fence = RebalanceRunSignalTestFence::install(REBALANCE_ID);
        let barrier = crate::core::pools::LifecycleDataMovementMutationBarrier::install(bucket, object);
        let task = spawn_real_rebalance_entry(
            Arc::clone(&store),
            Arc::clone(&source_set),
            entry,
            REBALANCE_ID,
            Arc::new(RebalanceBucketConfigs {
                lifecycle_config: Some(expired_delete_marker_lifecycle()),
                ..Default::default()
            }),
        );
        barrier.wait_until_paused().await;
        run_signal_fence.mark_lost();
        barrier.release();
        drop(barrier);
        assert_real_entry_rejected_after_run_fence_loss(task).await;

        assert!(
            source_set
                .load_file_info_versions_exact(bucket, object)
                .await
                .expect("source metadata lookup should succeed")
                .is_some(),
            "lost run fence must preserve the source before lifecycle mutation"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn real_rebalance_run_fence_loss_blocks_source_cleanup_delete() {
        const REBALANCE_ID: &str = "rebalance-source-cleanup-fence";
        let bucket = crate::disk::RUSTFS_META_BUCKET;
        let object = "rebalance-source-cleanup-fence-object";
        let (_temp_dirs, store, _unused_store) =
            crate::services::rebalance::test_two_pool_stores(Some(active_rebalance_meta(REBALANCE_ID))).await;
        prepare_rebalance_test_volumes(store.as_ref()).await;
        let source_set = store.pools[0].get_disks_by_key(object);
        source_set
            .delete_object(
                bucket,
                object,
                ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("source delete marker should be written");

        let entry = metacache_entry_from_source(source_set.as_ref(), bucket, object).await;
        let run_signal_fence = RebalanceRunSignalTestFence::install(REBALANCE_ID);
        let barrier = data_movement::SourceCleanupDeleteBarrier::install(bucket, object);
        let task = spawn_real_rebalance_entry(
            Arc::clone(&store),
            Arc::clone(&source_set),
            entry,
            REBALANCE_ID,
            Arc::new(RebalanceBucketConfigs::default()),
        );
        barrier.wait_until_paused().await;
        run_signal_fence.mark_lost();
        barrier.release();
        drop(barrier);
        assert_real_entry_rejected_after_run_fence_loss(task).await;

        assert!(
            source_set
                .load_file_info_versions_exact(bucket, object)
                .await
                .expect("source metadata lookup should succeed")
                .is_some(),
            "lost run fence must preserve the source during cleanup"
        );
    }
}
