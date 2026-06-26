use super::meta::{
    RebalanceMetaMergeOutcome, clone_first_arc, clone_rebalance_pool_stats, defer_bucket_in_rebalance_queue,
    ensure_valid_rebalance_pool_index, invalid_rebalance_pool_index_error, is_rebalance_actively_running,
    is_rebalance_conflicting_with_decommission, mark_rebalance_bucket_done, merge_rebalance_meta, percent_free_ratio,
    rebalance_metadata_not_initialized_error, record_rebalance_cleanup_warning_in_meta,
    record_rebalance_stop_propagation_snapshot, resolve_next_rebalance_bucket, rollback_rebalance_start_meta_snapshot_for_id,
    should_accept_rebalance_stats_update, should_pool_participate, stop_rebalance_meta_snapshot_for_id,
    validate_init_rebalance_state,
};
use super::worker::{
    rebalance_meta_lock_error, resolve_load_rebalance_stats_update_result, resolve_rebalance_meta_load_result,
    resolve_rebalance_meta_save_result,
};
use super::{
    DiskStat, EVENT_REBALANCE_BUCKET, EVENT_REBALANCE_STATE, LOG_COMPONENT_ECSTORE, LOG_SUBSYSTEM_REBALANCE, REBAL_META_NAME,
    RebalStatus, RebalanceInfo, RebalanceMeta, RebalanceStats, RebalanceStopPropagationRecord,
    encode_rebalance_stop_propagation_record,
};
use crate::error::{Error, Result};
use crate::object_api::ObjectOptions;
use crate::set_disk::get_lock_acquire_timeout;
use crate::storage_api_contracts::{
    admin::StorageAdminApi, namespace::NamespaceLocking as StorageNamespaceLocking, object::EcstoreObjectIO,
};
use crate::store::ECStore;
use rustfs_filemeta::FileInfo;
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::{debug, info};
use uuid::Uuid;

pub(super) fn validate_rebalance_disk_stats_coverage(disk_stats: &[DiskStat]) -> Result<()> {
    for (idx, disk_stat) in disk_stats.iter().enumerate() {
        if disk_stat.total_space == 0 {
            return Err(Error::other(format!(
                "rebalance storage info is incomplete: pool {idx} has no reported capacity"
            )));
        }
    }

    Ok(())
}

fn pool_rebalance_status_from_meta(meta: Option<&RebalanceMeta>, pool_index: usize) -> (RebalStatus, bool) {
    meta.and_then(|meta| meta.pool_stats.get(pool_index))
        .filter(|pool_stat| pool_stat.participating)
        .map(|pool_stat| (pool_stat.info.status, pool_stat.info.stopping))
        .unwrap_or_default()
}

fn merge_rebalance_status_refresh(current: &mut Option<RebalanceMeta>, persisted: RebalanceMeta) {
    if persisted.id.is_empty() && persisted.pool_stats.is_empty() {
        clear_rebalance_status_refresh(current);
        return;
    }

    match current.as_mut() {
        Some(current_meta) => {
            if merge_rebalance_meta(current_meta, &persisted) == RebalanceMetaMergeOutcome::RejectedActiveConflict
                && !is_rebalance_actively_running(current_meta)
            {
                *current = Some(persisted);
            }
        }
        None => {
            *current = Some(persisted);
        }
    }
}

fn clear_rebalance_status_refresh(current: &mut Option<RebalanceMeta>) {
    if current.as_ref().is_none_or(|meta| !is_rebalance_actively_running(meta)) {
        *current = None;
    }
}

impl ECStore {
    pub(super) async fn save_rebalance_meta_with_merge<S>(
        &self,
        pool: Arc<S>,
        local_snapshot: &RebalanceMeta,
        stage: &str,
    ) -> Result<()>
    where
        S: EcstoreObjectIO + StorageNamespaceLocking<Error = Error, NamespaceLock = rustfs_lock::NamespaceLockWrapper>,
    {
        let ns_lock = pool.new_ns_lock(crate::disk::RUSTFS_META_BUCKET, REBAL_META_NAME).await?;
        let _guard = ns_lock
            .get_write_lock(get_lock_acquire_timeout())
            .await
            .map_err(rebalance_meta_lock_error)?;

        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        let mut merged = RebalanceMeta::new();
        match merged.load_with_opts(pool.clone(), opts.clone()).await {
            Ok(()) => {
                if merge_rebalance_meta(&mut merged, local_snapshot) == RebalanceMetaMergeOutcome::RejectedActiveConflict {
                    return Err(Error::RebalanceAlreadyRunning);
                }
            }
            Err(Error::ConfigNotFound) => {
                merged = local_snapshot.clone();
            }
            Err(err) => return Err(Error::other(format!("rebalance meta load before save failed during {stage}: {err}"))),
        }

        merged.save_with_opts(pool, opts).await
    }

    #[tracing::instrument(skip_all)]
    pub async fn load_rebalance_meta(&self) -> Result<()> {
        let mut meta = RebalanceMeta::new();
        debug!(
            event = EVENT_REBALANCE_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            state = "metadata_loading",
            "Loading rebalance metadata"
        );
        let pool = clone_first_arc(&self.pools, "rebalanceMeta: no pools available")?;
        if resolve_rebalance_meta_load_result(meta.load(pool).await)? {
            {
                let mut rebalance_meta = self.rebalance_meta.write().await;

                *rebalance_meta = Some(meta);

                drop(rebalance_meta);
            }

            resolve_load_rebalance_stats_update_result(self.update_rebalance_stats().await)?;
            debug!(
                event = EVENT_REBALANCE_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REBALANCE,
                state = "metadata_loaded",
                "Loaded rebalance metadata"
            );
        } else {
            {
                let mut rebalance_meta = self.rebalance_meta.write().await;
                *rebalance_meta = None;
            }
            debug!(
                event = EVENT_REBALANCE_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REBALANCE,
                state = "metadata_missing",
                reason = "rebalance_not_started",
                "Rebalance metadata not found"
            );
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn refresh_rebalance_status_meta(&self) -> Result<()> {
        let pool = clone_first_arc(&self.pools, "refresh_rebalance_status_meta: no pools available")?;
        let mut persisted = RebalanceMeta::new();
        match persisted.load(pool).await {
            Ok(()) => {
                let mut rebalance_meta = self.rebalance_meta.write().await;
                merge_rebalance_status_refresh(&mut rebalance_meta, persisted);
            }
            Err(Error::ConfigNotFound) => {
                let mut rebalance_meta = self.rebalance_meta.write().await;
                clear_rebalance_status_refresh(&mut rebalance_meta);
            }
            Err(err) => {
                return Err(Error::other(format!("rebalance metadata refresh failed during pool status: {err}")));
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn update_rebalance_stats(&self) -> Result<()> {
        let mut ok = false;

        let pool_stats = {
            let rebalance_meta = self.rebalance_meta.read().await;
            clone_rebalance_pool_stats(rebalance_meta.as_ref())?
        };

        debug!(
            event = EVENT_REBALANCE_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            pool_count = pool_stats.len(),
            "Refreshing rebalance stats snapshot"
        );

        for i in 0..self.pools.len() {
            if pool_stats.get(i).is_none() {
                let mut rebalance_meta = self.rebalance_meta.write().await;
                debug!(
                    event = EVENT_REBALANCE_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    pool_index = i,
                    state = "pool_stat_missing",
                    "Adding missing rebalance pool stats entry"
                );
                if let Some(meta) = rebalance_meta.as_mut() {
                    meta.pool_stats.push(RebalanceStats::default());
                }
                ok = true;
                drop(rebalance_meta);
            }
        }

        if ok {
            debug!(
                event = EVENT_REBALANCE_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REBALANCE,
                state = "metadata_saving",
                "Saving rebalance metadata after stats refresh"
            );

            let rebalance_meta = self.rebalance_meta.read().await;
            if let Some(meta) = rebalance_meta.as_ref() {
                let pool = clone_first_arc(&self.pools, "update_rebalance_stats: no pools available")?;
                resolve_rebalance_meta_save_result(
                    self.save_rebalance_meta_with_merge(pool, meta, "update_rebalance_stats")
                        .await,
                    "update_rebalance_stats",
                )?;
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn init_rebalance_meta(&self, bucktes: Vec<String>) -> Result<String> {
        info!(
            event = EVENT_REBALANCE_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            state = "initializing",
            bucket_count = bucktes.len(),
            "Initializing rebalance metadata"
        );
        let si = StorageAdminApi::storage_info(self).await;

        let mut disk_stats = vec![DiskStat::default(); self.pools.len()];

        let mut total_cap = 0;
        let mut total_free = 0;
        for disk in si.disks.iter() {
            if disk.pool_index < 0 || disk_stats.len() <= disk.pool_index as usize {
                continue;
            }

            total_cap += disk.total_space;
            total_free += disk.available_space;

            disk_stats[disk.pool_index as usize].total_space += disk.total_space;
            disk_stats[disk.pool_index as usize].available_space += disk.available_space;
        }

        let percent_free_goal = percent_free_ratio(total_free, total_cap);
        validate_rebalance_disk_stats_coverage(&disk_stats)?;

        let mut pool_stats = Vec::with_capacity(self.pools.len());

        let now = OffsetDateTime::now_utc();

        for disk_stat in disk_stats.iter() {
            let mut pool_stat = RebalanceStats {
                init_free_space: disk_stat.available_space,
                init_capacity: disk_stat.total_space,
                buckets: bucktes.clone(),
                rebalanced_buckets: Vec::with_capacity(bucktes.len()),
                ..Default::default()
            };

            if should_pool_participate(disk_stat.available_space, disk_stat.total_space, percent_free_goal) {
                pool_stat.participating = true;
                pool_stat.info = RebalanceInfo {
                    start_time: Some(now),
                    status: RebalStatus::Started,
                    ..Default::default()
                };
            }

            pool_stats.push(pool_stat);
        }

        let meta = RebalanceMeta {
            id: Uuid::new_v4().to_string(),
            percent_free_goal,
            pool_stats,
            ..Default::default()
        };

        let pool = clone_first_arc(&self.pools, "init_rebalance_meta: no pools available")?;
        resolve_rebalance_meta_save_result(
            self.save_rebalance_meta_with_merge(pool, &meta, "init_rebalance_meta").await,
            "init_rebalance_meta",
        )?;

        info!(
            event = EVENT_REBALANCE_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            state = "metadata_initialized",
            bucket_count = bucktes.len(),
            "Rebalance metadata initialized"
        );

        let id = meta.id.clone();

        {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            *rebalance_meta = Some(meta);
            drop(rebalance_meta);
        }

        Ok(id)
    }

    #[tracing::instrument(skip(self, bucktes))]
    pub async fn init_and_start_rebalance(self: &Arc<Self>, bucktes: Vec<String>) -> Result<String> {
        let _start_guard = self.start_gate.lock().await;

        let decommission_running = self.is_decommission_running().await;
        {
            let rebalance_meta = self.rebalance_meta.read().await;
            validate_init_rebalance_state(decommission_running, rebalance_meta.as_ref())?;
        }

        let id = self.init_rebalance_meta(bucktes).await?;
        if let Err(start_err) = self.start_rebalance().await {
            if let Err(rollback_err) = self
                .rollback_rebalance_start_without_worker_for_id(Some(&id), start_err.to_string())
                .await
            {
                return Err(Error::other(format!(
                    "failed to start rebalance after metadata initialized for {id}: {start_err}; rollback failed: {rollback_err}"
                )));
            }

            return Err(Error::other(format!(
                "failed to start rebalance after metadata initialized for {id}; local metadata was finalized as failed: {start_err}"
            )));
        }

        Ok(id)
    }

    #[tracing::instrument(skip(self, fi))]
    pub async fn update_pool_stats(&self, pool_index: usize, bucket: String, fi: &FileInfo) -> Result<()> {
        self.update_pool_stats_batch(pool_index, bucket, &[fi]).await
    }

    #[tracing::instrument(skip(self, versions))]
    pub async fn update_pool_stats_batch(&self, pool_index: usize, bucket: String, versions: &[&FileInfo]) -> Result<()> {
        if versions.is_empty() {
            return Ok(());
        }

        let mut rebalance_meta = self.rebalance_meta.write().await;
        if let Some(meta) = rebalance_meta.as_mut() {
            if !should_accept_rebalance_stats_update(meta, pool_index) {
                return Ok(());
            }

            if let Some(pool_stat) = meta.pool_stats.get_mut(pool_index) {
                pool_stat.update_batch(bucket, versions);
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn next_rebal_bucket(&self, pool_index: usize) -> Result<Option<String>> {
        let rebalance_meta = self.rebalance_meta.read().await;
        debug!(
            event = EVENT_REBALANCE_BUCKET,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            pool_index,
            has_meta = rebalance_meta.is_some(),
            state = "next_bucket_lookup",
            "Rebalance next bucket lookup"
        );
        resolve_next_rebalance_bucket(rebalance_meta.as_ref(), pool_index)
    }

    #[tracing::instrument(skip(self))]
    pub async fn bucket_rebalance_done(&self, pool_index: usize, bucket: String) -> Result<()> {
        let mut rebalance_meta = self.rebalance_meta.write().await;
        mark_rebalance_bucket_done(rebalance_meta.as_mut(), pool_index, &bucket)
    }

    pub(super) async fn record_rebalance_cleanup_warning(
        &self,
        pool_index: usize,
        bucket: &str,
        object: &str,
        message: String,
    ) -> Result<()> {
        let mut rebalance_meta = self.rebalance_meta.write().await;
        record_rebalance_cleanup_warning_in_meta(
            rebalance_meta.as_mut(),
            pool_index,
            bucket,
            object,
            message,
            OffsetDateTime::now_utc(),
        )
    }

    pub(super) async fn defer_rebalance_bucket(&self, pool_index: usize, bucket: String, last_error: String) -> Result<()> {
        let mut rebalance_meta = self.rebalance_meta.write().await;
        let Some(meta) = rebalance_meta.as_mut() else {
            return Err(rebalance_metadata_not_initialized_error("defer rebalance bucket"));
        };
        let pool_count = meta.pool_stats.len();
        ensure_valid_rebalance_pool_index(pool_count, pool_index)?;
        let Some(pool_stat) = meta.pool_stats.get_mut(pool_index) else {
            return Err(invalid_rebalance_pool_index_error(pool_index, pool_count));
        };

        defer_bucket_in_rebalance_queue(pool_stat, &bucket)?;
        pool_stat.info.last_error = Some(last_error);
        meta.last_refreshed_at = Some(OffsetDateTime::now_utc());
        Ok(())
    }

    pub async fn is_rebalance_started(&self) -> bool {
        let rebalance_meta = self.rebalance_meta.read().await;
        if let Some(meta) = rebalance_meta.as_ref() {
            meta.pool_stats.iter().enumerate().for_each(|(i, v)| {
                debug!(
                    event = EVENT_REBALANCE_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    pool_index = i,
                    participating = v.participating,
                    status = ?v.info.status,
                    state = "status_inspected",
                    "Rebalance status inspected"
                );
            });

            let started = is_rebalance_conflicting_with_decommission(meta);
            if started {
                debug!(
                    event = EVENT_REBALANCE_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    state = "running",
                    "Rebalance is running"
                );
                return true;
            }
        }

        debug!(
            event = EVENT_REBALANCE_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            state = "not_running",
            "Rebalance is not running"
        );
        false
    }

    pub async fn is_rebalance_conflicting_with_decommission(&self) -> bool {
        let rebalance_meta = self.rebalance_meta.read().await;
        rebalance_meta
            .as_ref()
            .is_some_and(is_rebalance_conflicting_with_decommission)
    }

    pub async fn is_pool_rebalancing(&self, pool_index: usize) -> bool {
        let rebalance_meta = self.rebalance_meta.read().await;
        if let Some(ref meta) = *rebalance_meta {
            if meta.stopped_at.is_some() {
                return false;
            }

            if let Some(pool_stat) = meta.pool_stats.get(pool_index) {
                return pool_stat.participating && pool_stat.info.status == RebalStatus::Started;
            }
        }

        false
    }

    pub async fn pool_rebalance_status(&self, pool_index: usize) -> (RebalStatus, bool) {
        let rebalance_meta = self.rebalance_meta.read().await;
        pool_rebalance_status_from_meta(rebalance_meta.as_ref(), pool_index)
    }

    pub async fn current_rebalance_id(&self) -> Option<String> {
        let rebalance_meta = self.rebalance_meta.read().await;
        rebalance_meta
            .as_ref()
            .and_then(|meta| (!meta.id.is_empty()).then(|| meta.id.clone()))
    }

    #[tracing::instrument(skip(self))]
    pub async fn stop_rebalance(self: &Arc<Self>) -> Result<()> {
        self.stop_rebalance_for_id(None).await
    }

    #[tracing::instrument(skip(self))]
    pub async fn stop_rebalance_for_id(self: &Arc<Self>, expected_id: Option<&str>) -> Result<()> {
        let meta_to_save = {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            stop_rebalance_meta_snapshot_for_id(rebalance_meta.as_mut(), OffsetDateTime::now_utc(), expected_id)
        }?;

        if let Some(meta_to_save) = meta_to_save {
            let pool = clone_first_arc(self.pools.as_slice(), "stop_rebalance: no pools available")?;
            resolve_rebalance_meta_save_result(
                self.save_rebalance_meta_with_merge(pool, &meta_to_save, "stop_rebalance")
                    .await,
                "stop_rebalance",
            )?;
        }

        Ok(())
    }

    async fn rollback_rebalance_start_without_worker_for_id(
        self: &Arc<Self>,
        expected_id: Option<&str>,
        start_error: String,
    ) -> Result<()> {
        let meta_to_save = {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            rollback_rebalance_start_meta_snapshot_for_id(
                rebalance_meta.as_mut(),
                OffsetDateTime::now_utc(),
                expected_id,
                start_error,
            )
        };

        if let Some(meta_to_save) = meta_to_save {
            let pool = clone_first_arc(self.pools.as_slice(), "rollback_rebalance_start: no pools available")?;
            resolve_rebalance_meta_save_result(
                self.save_rebalance_meta_with_merge(pool, &meta_to_save, "rollback_rebalance_start")
                    .await,
                "rollback_rebalance_start",
            )?;
        }

        Ok(())
    }

    pub async fn record_rebalance_stop_propagation(self: &Arc<Self>, record: RebalanceStopPropagationRecord) -> Result<()> {
        if !record.has_failures() {
            return Ok(());
        }

        let encoded_error = encode_rebalance_stop_propagation_record(&record);
        let meta_to_save = {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            record_rebalance_stop_propagation_snapshot(rebalance_meta.as_mut(), encoded_error, OffsetDateTime::now_utc())
        };

        if let Some(meta_to_save) = meta_to_save {
            let pool = clone_first_arc(self.pools.as_slice(), "record_rebalance_stop_propagation: no pools available")?;
            resolve_rebalance_meta_save_result(
                self.save_rebalance_meta_with_merge(pool, &meta_to_save, "record_rebalance_stop_propagation")
                    .await,
                "record_rebalance_stop_propagation",
            )?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_rebalance_status_ignores_non_participating_pool_state() {
        let meta = RebalanceMeta {
            pool_stats: vec![
                RebalanceStats {
                    participating: false,
                    info: RebalanceInfo {
                        status: RebalStatus::Started,
                        stopping: true,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                RebalanceStats {
                    participating: true,
                    info: RebalanceInfo {
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert_eq!(pool_rebalance_status_from_meta(Some(&meta), 0), (RebalStatus::None, false));
        assert_eq!(pool_rebalance_status_from_meta(Some(&meta), 1), (RebalStatus::Started, false));
        assert_eq!(pool_rebalance_status_from_meta(Some(&meta), 2), (RebalStatus::None, false));
    }

    #[test]
    fn rebalance_status_refresh_applies_persisted_terminal_state() {
        let rebalance_id = "rebalance-id".to_string();
        let now = OffsetDateTime::from_unix_timestamp(1_000).expect("test timestamp should be valid");
        let mut current = Some(RebalanceMeta {
            id: rebalance_id.clone(),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        });
        let persisted = RebalanceMeta {
            id: rebalance_id,
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Completed,
                    start_time: Some(now),
                    end_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        merge_rebalance_status_refresh(&mut current, persisted);

        let refreshed = current.as_ref().expect("refresh should keep rebalance metadata");
        assert_eq!(refreshed.pool_stats[0].info.status, RebalStatus::Completed);
        assert_eq!(refreshed.pool_stats[0].info.end_time, Some(now));
    }

    #[test]
    fn rebalance_status_refresh_preserves_runtime_cancel_token() {
        let rebalance_id = "rebalance-id".to_string();
        let now = OffsetDateTime::from_unix_timestamp(1_000).expect("test timestamp should be valid");
        let mut current = Some(RebalanceMeta {
            id: rebalance_id.clone(),
            cancel: Some(tokio_util::sync::CancellationToken::new()),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        });
        let persisted = RebalanceMeta {
            id: rebalance_id,
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        merge_rebalance_status_refresh(&mut current, persisted);

        assert!(
            current.as_ref().and_then(|meta| meta.cancel.as_ref()).is_some(),
            "status refresh must not drop the runtime cancellation token"
        );
    }

    #[test]
    fn rebalance_status_refresh_preserves_local_active_different_id_conflict() {
        let now = OffsetDateTime::from_unix_timestamp(1_000).expect("test timestamp should be valid");
        let mut current = Some(RebalanceMeta {
            id: "old-active-id".to_string(),
            cancel: Some(tokio_util::sync::CancellationToken::new()),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        });
        let persisted = RebalanceMeta {
            id: "new-terminal-id".to_string(),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Completed,
                    start_time: Some(now),
                    end_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        merge_rebalance_status_refresh(&mut current, persisted);

        let refreshed = current.as_ref().expect("local active metadata should remain visible");
        assert_eq!(refreshed.id, "old-active-id");
        assert_eq!(refreshed.pool_stats[0].info.status, RebalStatus::Started);
        assert!(
            refreshed.cancel.is_some(),
            "status refresh must not drop a live runtime cancellation token"
        );
    }

    #[test]
    fn rebalance_status_refresh_replaces_stale_memory_without_runtime_token() {
        let now = OffsetDateTime::from_unix_timestamp(1_000).expect("test timestamp should be valid");
        let mut current = Some(RebalanceMeta {
            id: "old-stale-id".to_string(),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        });
        let persisted = RebalanceMeta {
            id: "new-terminal-id".to_string(),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Completed,
                    start_time: Some(now),
                    end_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        merge_rebalance_status_refresh(&mut current, persisted);

        let refreshed = current.as_ref().expect("persisted metadata should replace stale memory");
        assert_eq!(refreshed.id, "new-terminal-id");
        assert_eq!(refreshed.pool_stats[0].info.status, RebalStatus::Completed);
        assert!(refreshed.cancel.is_none());
    }
}
