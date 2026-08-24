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

fn ensure_rebalance_entry_active(cancel: &CancellationToken) -> Result<()> {
    if cancel.is_cancelled() {
        return Err(Error::OperationCanceled);
    }
    Ok(())
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
        // Stop waits for in-flight entries through cleanup, but not for entries admitted later.
        ensure_rebalance_entry_active(&cancel)?;
        let run_guard = self.rebalance_run_guard(rebalance_id.as_ref(), "rebalance entry").await?;
        let lock_lost_signal = run_guard.lock_lost_signal();
        #[cfg(test)]
        let _run_signal_test_fence = lock_lost_signal
            .as_ref()
            .and_then(|signal| attach_rebalance_run_signal_test_fence(rebalance_id.as_ref(), signal));

        let mut rebalanced: usize = 0;
        let mut expired: usize = 0;
        let mut cleanup_preflight_allowed_missing = Vec::new();
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
                &RebalanceMigrationBackend::new(set.as_ref(), self.as_ref(), lock_lost_signal.clone()),
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
        }

        if should_cleanup_rebalance_source_entry(rebalanced, fivs.versions.len(), expired) {
            if bucket_incarnation_fence.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
                return Err(Error::other("rebalance bucket incarnation fence was lost before source cleanup"));
            }
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
                        &fivs,
                        &cleanup_preflight_allowed_missing,
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
    ) -> Result<()> {
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
        barrier: crate::data_movement::SourceCleanupDeleteBarrier,
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
            let barrier = crate::data_movement::SourceCleanupDeleteBarrier::install(bucket, object);
            let entry_store = Arc::clone(&store);
            let entry_set = Arc::clone(&set);
            let entry_cancel = cancel.clone();
            let entry_task = tokio::spawn(async move {
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
            });

            Self {
                _temp_dirs,
                store,
                cancel,
                barrier,
                stop_probe: super::super::control::RebalanceStopWaitProbe::install(rebalance_id),
                entry_task: Some(entry_task),
            }
        }

        pub fn store(&self) -> Arc<ECStore> {
            Arc::clone(&self.store)
        }

        pub async fn wait_until_entry_paused(&self) {
            self.barrier.wait_until_paused().await;
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
            self.barrier.release();
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
            self.barrier.release();
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
            .finish_rebalance_entry_after_cleanup(&warning_cleanup_context, async { Err(Error::SlowDown.into()) })
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
