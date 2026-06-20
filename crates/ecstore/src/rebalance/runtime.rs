use super::meta::{
    apply_rebalance_save_option, apply_rebalance_terminal_event, classify_rebalance_terminal_event, clone_first_arc,
    complete_rebalance_pools_at_goal, complete_rebalance_pools_with_empty_queue, ensure_valid_rebalance_pool_index,
    has_deferred_rebalance_error, is_rebalance_in_progress, rebalance_goal_reached, resolve_rebalance_participants,
    should_preserve_rebalance_stopped_state, should_skip_start_rebalance, validate_start_rebalance_state,
};
use super::worker::{
    resolve_rebalance_bucket_result, resolve_rebalance_meta_save_result, resolve_rebalance_save_task_result,
    resolve_rebalance_terminal_error, send_rebalance_done_signal,
};
use super::{
    EVENT_REBALANCE_BUCKET, EVENT_REBALANCE_STATE, LOG_COMPONENT_ECSTORE, LOG_SUBSYSTEM_REBALANCE, RebalSaveOpt, RebalStatus,
    RebalanceBucketOutcome,
};
use crate::error::{Error, Result};
use crate::global::get_global_endpoints;
use crate::store::ECStore;
use std::collections::HashSet;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

impl ECStore {
    #[tracing::instrument(skip_all)]
    pub async fn start_rebalance(self: &Arc<Self>) -> Result<()> {
        info!(
            event = EVENT_REBALANCE_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            state = "starting",
            "Starting rebalance"
        );
        let decommission_running = self.is_decommission_running().await;
        // let rebalance_meta = self.rebalance_meta.read().await;

        let cancel_tx = CancellationToken::new();
        let rx = cancel_tx.clone();
        let mut meta_to_save = None;

        {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            validate_start_rebalance_state(decommission_running, rebalance_meta.is_some())?;

            let Some(meta) = rebalance_meta.as_mut() else {
                return Err(Error::ConfigNotFound);
            };
            if should_skip_start_rebalance(meta.cancel.is_some(), is_rebalance_in_progress(meta)) {
                debug!(
                    event = EVENT_REBALANCE_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    state = "start_skipped",
                    reason = "already_in_progress",
                    "Skipped duplicate rebalance start"
                );
                return Ok(());
            }
            let now = OffsetDateTime::now_utc();
            if complete_rebalance_pools_at_goal(meta, now) {
                meta_to_save = Some(meta.clone());
            }
            if complete_rebalance_pools_with_empty_queue(meta, now) {
                meta_to_save = Some(meta.clone());
            }
            meta.cancel = Some(cancel_tx);

            drop(rebalance_meta);
        }

        if let Some(meta) = meta_to_save {
            let pool = clone_first_arc(self.pools.as_slice(), "start_rebalance: no pools available")?;
            resolve_rebalance_meta_save_result(
                self.save_rebalance_meta_with_merge(pool, &meta, "start_rebalance complete pools at goal")
                    .await,
                "start_rebalance complete pools at goal",
            )?;
        }

        let participants = if let Some(ref meta) = *self.rebalance_meta.read().await {
            resolve_rebalance_participants(meta.pool_stats.as_slice(), self.pools.len())
        } else {
            debug!(
                event = EVENT_REBALANCE_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REBALANCE,
                state = "start_skipped",
                reason = "metadata_missing",
                "Skipped rebalance start because metadata is unavailable"
            );
            Vec::new()
        };

        if !participants.iter().any(|participating| *participating) {
            debug!(
                event = EVENT_REBALANCE_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REBALANCE,
                state = "start_skipped",
                reason = "no_participants",
                "Skipped rebalance start because no pools are participating"
            );
            return Ok(());
        }

        let mut workers_started = 0usize;
        for (idx, participating) in participants.iter().enumerate() {
            if !*participating {
                debug!(
                    event = EVENT_REBALANCE_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    pool_index = idx,
                    state = "pool_skipped",
                    reason = "not_participating",
                    "Skipped rebalance pool"
                );
                continue;
            }

            if !get_global_endpoints()
                .as_ref()
                .get(idx)
                .is_some_and(|v| v.endpoints.as_ref().first().is_some_and(|e| e.is_local))
            {
                debug!(
                    event = EVENT_REBALANCE_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    pool_index = idx,
                    state = "pool_skipped",
                    reason = "not_local",
                    "Skipped non-local rebalance pool"
                );
                continue;
            }

            let pool_idx = idx;
            let store = self.clone();
            let rx_clone = rx.clone();
            workers_started += 1;
            tokio::spawn(async move {
                if let Err(err) = store.rebalance_buckets(rx_clone, pool_idx).await {
                    error!(
                        event = EVENT_REBALANCE_STATE,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REBALANCE,
                        pool_index = pool_idx,
                        state = "pool_failed",
                        error = %err,
                        "Rebalance pool failed"
                    );
                } else {
                    debug!(
                        event = EVENT_REBALANCE_STATE,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REBALANCE,
                        pool_index = pool_idx,
                        state = "completed",
                        "Rebalance pool completed"
                    );
                }
            });
        }

        if workers_started == 0 {
            debug!(
                event = EVENT_REBALANCE_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REBALANCE,
                state = "start_skipped",
                reason = "no_local_participants",
                "Skipped rebalance start because no local pools are participating"
            );
            return Ok(());
        }

        info!(
            event = EVENT_REBALANCE_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            state = "started",
            worker_count = workers_started,
            "Rebalance started"
        );
        Ok(())
    }

    #[tracing::instrument(skip(self, rx))]
    async fn rebalance_buckets(self: &Arc<Self>, rx: CancellationToken, pool_index: usize) -> Result<()> {
        ensure_valid_rebalance_pool_index(self.pools.len(), pool_index)?;

        let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<Result<()>>(1);

        // Save rebalance metadata periodically
        let store = self.clone();
        let save_task = tokio::spawn(async move {
            let mut timer = tokio::time::interval_at(Instant::now() + Duration::from_secs(30), Duration::from_secs(10));
            let mut msg: String;
            let mut quit = false;

            loop {
                tokio::select! {
                    result = done_rx.recv() => {
                        quit = true;
                        let now = OffsetDateTime::now_utc();
                        let terminal_event = classify_rebalance_terminal_event(result, now);
                        msg = terminal_event.message().to_string();
                        let mut rebalance_meta = store.rebalance_meta.write().await;
                        if let Some(meta) = rebalance_meta.as_mut() {
                            let meta_stopped = meta.stopped_at.is_some();
                            if let Some(pool_stat) = meta.pool_stats.get_mut(pool_index) {
                                if should_preserve_rebalance_stopped_state(
                                    meta_stopped,
                                    pool_stat.info.status,
                                    &terminal_event,
                                ) {
                                    debug!(
                                        event = EVENT_REBALANCE_STATE,
                                        component = LOG_COMPONENT_ECSTORE,
                                        subsystem = LOG_SUBSYSTEM_REBALANCE,
                                        pool_index,
                                        state = "stopped_preserved",
                                        "Preserved stopped rebalance status"
                                    );
                                } else {
                                    apply_rebalance_terminal_event(
                                        &mut pool_stat.info.status,
                                        &mut pool_stat.info.end_time,
                                        &mut pool_stat.info.last_error,
                                        terminal_event,
                                        now,
                                    );
                                }
                            }
                        }
                    }
                    _ = timer.tick() => {
                        let now = OffsetDateTime::now_utc();
                        msg = format!("Saving rebalance metadata at {now:?}");
                    }
                }

                if let Err(err) = store.save_rebalance_stats(pool_index, RebalSaveOpt::Stats).await {
                    let wrapped = Error::other(format!("rebalance save_task stats save failed for pool {pool_index}: {err}"));
                    error!("{} err: {:?}", msg, wrapped);
                    if quit {
                        return Err(wrapped);
                    }
                } else {
                    debug!(
                        event = EVENT_REBALANCE_STATE,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REBALANCE,
                        pool_index,
                        state = "metadata_saved",
                        message = %msg,
                        "Saved rebalance metadata"
                    );
                }

                if quit {
                    debug!(
                        event = EVENT_REBALANCE_STATE,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REBALANCE,
                        pool_index,
                        state = "save_task_exiting",
                        message = %msg,
                        "Exiting rebalance save task"
                    );
                    return Ok(());
                }

                timer.reset();
            }
        });

        info!(
            event = EVENT_REBALANCE_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            pool_index,
            state = "pool_started",
            "Rebalance worker started"
        );
        let mut final_result: Result<()> = Ok(());
        let mut deferred_buckets = HashSet::new();

        loop {
            if rx.is_cancelled() {
                info!(
                    event = EVENT_REBALANCE_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    pool_index,
                    state = "pool_stopped",
                    reason = "cancelled",
                    "Stopped rebalance worker"
                );
                let err = Error::OperationCanceled;
                final_result = Err(resolve_rebalance_terminal_error(
                    err.clone(),
                    send_rebalance_done_signal(&done_tx, Err(err.clone()), pool_index).await,
                ));
                break;
            }

            let next_bucket = match self.next_rebal_bucket(pool_index).await {
                Ok(bucket) => bucket,
                Err(err) => {
                    error!(
                        event = EVENT_REBALANCE_BUCKET,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REBALANCE,
                        pool_index,
                        state = "next_bucket_failed",
                        error = ?err,
                        "Rebalance next bucket lookup failed"
                    );
                    final_result = Err(resolve_rebalance_terminal_error(
                        err.clone(),
                        send_rebalance_done_signal(&done_tx, Err(err.clone()), pool_index).await,
                    ));
                    break;
                }
            };

            if let Some(bucket) = next_bucket {
                debug!(
                    event = EVENT_REBALANCE_BUCKET,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    pool_index,
                    bucket = %bucket,
                    state = "started",
                    "Starting rebalance bucket"
                );

                let outcome = match resolve_rebalance_bucket_result(
                    self.rebalance_bucket(rx.clone(), bucket.clone(), pool_index).await,
                    pool_index,
                    &bucket,
                ) {
                    Ok(outcome) => outcome,
                    Err(err) => {
                        error!(
                            event = EVENT_REBALANCE_BUCKET,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_REBALANCE,
                            pool_index,
                            bucket = %bucket,
                            state = "bucket_failed",
                            error = ?err,
                            "Rebalance bucket failed"
                        );
                        final_result = Err(resolve_rebalance_terminal_error(
                            err.clone(),
                            send_rebalance_done_signal(&done_tx, Err(err.clone()), pool_index).await,
                        ));
                        break;
                    }
                };

                if let RebalanceBucketOutcome::Deferred { last_error } = outcome {
                    if !deferred_buckets.insert(bucket.clone()) {
                        let err = Error::other(format!(
                            "rebalance bucket {bucket} deferred repeatedly due to transient object failures: {last_error}"
                        ));
                        error!(
                            event = EVENT_REBALANCE_BUCKET,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_REBALANCE,
                            pool_index,
                            bucket = %bucket,
                            state = "bucket_deferred_repeatedly",
                            error = ?err,
                            "Rebalance bucket failed after repeated deferral"
                        );
                        final_result = Err(resolve_rebalance_terminal_error(
                            err.clone(),
                            send_rebalance_done_signal(&done_tx, Err(err.clone()), pool_index).await,
                        ));
                        break;
                    }

                    warn!(
                        event = EVENT_REBALANCE_BUCKET,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REBALANCE,
                        pool_index,
                        bucket = %bucket,
                        state = "deferred",
                        error = %last_error,
                        "Deferred rebalance bucket after transient object failures"
                    );
                    if let Err(err) = self.defer_rebalance_bucket(pool_index, bucket.clone(), last_error).await {
                        error!(
                            event = EVENT_REBALANCE_BUCKET,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_REBALANCE,
                            pool_index,
                            bucket = %bucket,
                            state = "defer_failed",
                            error = ?err,
                            "Rebalance bucket defer failed"
                        );
                        final_result = Err(resolve_rebalance_terminal_error(
                            err.clone(),
                            send_rebalance_done_signal(&done_tx, Err(err.clone()), pool_index).await,
                        ));
                        break;
                    }
                    continue;
                }

                debug!(
                    event = EVENT_REBALANCE_BUCKET,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    pool_index,
                    bucket = %bucket,
                    state = "completed",
                    "Completed rebalance bucket"
                );
                if let Err(err) = self.bucket_rebalance_done(pool_index, bucket).await {
                    error!(
                        event = EVENT_REBALANCE_BUCKET,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REBALANCE,
                        pool_index,
                        state = "bucket_done_mark_failed",
                        error = ?err,
                        "Rebalance bucket completion mark failed"
                    );
                    final_result = Err(resolve_rebalance_terminal_error(
                        err.clone(),
                        send_rebalance_done_signal(&done_tx, Err(err.clone()), pool_index).await,
                    ));
                    break;
                }
            } else {
                debug!(
                    event = EVENT_REBALANCE_BUCKET,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    pool_index,
                    state = "idle",
                    reason = "no_bucket_to_rebalance",
                    "No rebalance bucket available"
                );
                break;
            }
        }

        info!(
            event = EVENT_REBALANCE_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            pool_index,
            state = "pool_done",
            "Rebalance worker finished"
        );

        if final_result.is_ok()
            && let Err(err) = send_rebalance_done_signal(&done_tx, Ok(()), pool_index).await
        {
            final_result = Err(err);
        }
        drop(done_tx);
        if let Err(err) = resolve_rebalance_save_task_result(pool_index, save_task.await)
            && final_result.is_ok()
        {
            final_result = Err(err);
        }
        debug!(
            event = EVENT_REBALANCE_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            pool_index,
            state = "pool_result_returned",
            "Rebalance worker result returned"
        );
        final_result
    }

    pub(super) async fn check_if_rebalance_done(&self, pool_index: usize) -> bool {
        let mut rebalance_meta = self.rebalance_meta.write().await;

        if let Some(meta) = rebalance_meta.as_mut()
            && let Some(pool_stat) = meta.pool_stats.get_mut(pool_index)
        {
            // Check if the pool's rebalance status is already completed
            if pool_stat.info.status == RebalStatus::Completed {
                debug!(
                    event = EVENT_REBALANCE_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    pool_index,
                    state = "already_completed",
                    "Rebalance pool is already completed"
                );
                return true;
            }

            // Mark pool rebalance as done only after it reaches the PercentFreeGoal.
            let pfi = if pool_stat.init_capacity == 0 {
                0.0
            } else {
                (pool_stat.init_free_space + pool_stat.bytes) as f64 / pool_stat.init_capacity as f64
            };

            if !has_deferred_rebalance_error(pool_stat)
                && rebalance_goal_reached(
                    pool_stat.init_free_space,
                    pool_stat.init_capacity,
                    pool_stat.bytes,
                    meta.percent_free_goal,
                )
            {
                pool_stat.info.status = RebalStatus::Completed;
                pool_stat.info.end_time = Some(OffsetDateTime::now_utc());
                info!(
                    event = EVENT_REBALANCE_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    pool_index,
                    state = "completed",
                    percent_free = pfi,
                    "Marked rebalance pool completed"
                );
                return true;
            }
        }

        false
    }
}

impl ECStore {
    #[tracing::instrument(skip(self))]
    pub async fn save_rebalance_stats(&self, pool_idx: usize, opt: RebalSaveOpt) -> Result<()> {
        let meta_to_save = {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            let Some(meta) = rebalance_meta.as_mut() else {
                return Ok(());
            };

            let now = OffsetDateTime::now_utc();
            apply_rebalance_save_option(meta, pool_idx, opt, now);
            meta.clone()
        };

        let pool = clone_first_arc(&self.pools, "save_rebalance_stats: no pools available")?;

        debug!(
            event = EVENT_REBALANCE_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            pool_index = pool_idx,
            save_opt = ?opt,
            state = "metadata_save_requested",
            "Rebalance metadata save requested"
        );
        let stage = format!("save_rebalance_stats for pool {pool_idx} opt {opt:?}");
        resolve_rebalance_meta_save_result(
            self.save_rebalance_meta_with_merge(pool, &meta_to_save, stage.as_str()).await,
            stage.as_str(),
        )?;

        Ok(())
    }
}
