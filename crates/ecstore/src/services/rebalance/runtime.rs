use super::control::RebalanceWorkerActivationFence;
use super::meta::{
    apply_rebalance_save_option, apply_rebalance_terminal_event, classify_rebalance_terminal_event, clone_first_arc,
    complete_rebalance_pools_at_goal, complete_rebalance_pools_with_empty_queue, ensure_valid_rebalance_pool_index,
    has_deferred_rebalance_error, has_rebalance_cleanup_warnings, is_rebalance_in_progress, rebalance_goal_reached,
    resolve_rebalance_participants, should_preserve_rebalance_stopped_state, should_skip_start_rebalance,
    validate_start_rebalance_state,
};
use super::worker::{
    resolve_rebalance_bucket_result, resolve_rebalance_meta_save_result, resolve_rebalance_save_task_result,
    resolve_rebalance_terminal_error, send_rebalance_done_signal,
};
use super::{
    EVENT_REBALANCE_BUCKET, EVENT_REBALANCE_STATE, LOG_COMPONENT_ECSTORE, LOG_SUBSYSTEM_REBALANCE,
    REBALANCE_LISTING_RETRY_BASE_DELAY, REBALANCE_SOURCE_CLEANUP_DEFERRED_ERROR_PREFIX, RebalSaveOpt, RebalStatus,
    RebalanceBucketOutcome,
};
use crate::error::{Error, Result};
use crate::runtime::sources as runtime_sources;
use crate::store::ECStore;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub(super) fn should_fail_repeated_rebalance_bucket_defer(
    deferred_buckets: &mut HashSet<String>,
    bucket: &str,
    source_cleanup_deferred: bool,
) -> bool {
    !source_cleanup_deferred && !deferred_buckets.insert(bucket.to_string())
}

pub(super) fn source_cleanup_defer_attempt(deferred_attempts: &mut HashMap<String, usize>, bucket: &str) -> usize {
    let attempts = deferred_attempts.entry(bucket.to_string()).or_default();
    *attempts = attempts.saturating_add(1);
    *attempts
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RebalanceLocalActivationOutcome {
    Started,
    NotStartedTerminal,
}

pub(super) fn commit_local_rebalance_worker_activation(
    meta: &mut super::RebalanceMeta,
    expected_id: &str,
    cancel: CancellationToken,
) -> Result<RebalanceLocalActivationOutcome> {
    if meta.id != expected_id {
        return Err(Error::other(format!(
            "rebalance metadata changed before local worker activation: expected {expected_id}, found {}",
            meta.id
        )));
    }
    if meta.stopped_at.is_some() || !is_rebalance_in_progress(meta) {
        return Ok(RebalanceLocalActivationOutcome::NotStartedTerminal);
    }
    meta.cancel = Some(cancel);
    Ok(RebalanceLocalActivationOutcome::Started)
}

pub(super) fn stage_local_rebalance_worker_activation(
    meta: &super::RebalanceMeta,
    expected_id: &str,
    cancel: CancellationToken,
    now: OffsetDateTime,
) -> Result<(super::RebalanceMeta, RebalanceLocalActivationOutcome, bool)> {
    let mut candidate = meta.clone();
    let completed_at_goal = complete_rebalance_pools_at_goal(&mut candidate, now);
    let completed_empty_queue = complete_rebalance_pools_with_empty_queue(&mut candidate, now);
    let outcome = commit_local_rebalance_worker_activation(&mut candidate, expected_id, cancel)?;
    let must_persist =
        completed_at_goal || completed_empty_queue || outcome == RebalanceLocalActivationOutcome::NotStartedTerminal;
    Ok((candidate, outcome, must_persist))
}

pub(super) fn commit_local_rebalance_worker_activation_candidate(
    current: &mut super::RebalanceMeta,
    expected_id: &str,
    expected_cancel: Option<&CancellationToken>,
    candidate: super::RebalanceMeta,
) -> Result<()> {
    if current.id != expected_id || candidate.id != expected_id {
        return Err(Error::other(format!(
            "rebalance metadata changed before local worker activation commit: expected {expected_id}, found {}",
            current.id
        )));
    }
    if !Arc::ptr_eq(&current.activation_gate, &candidate.activation_gate) {
        return Err(Error::other(format!(
            "rebalance activation gate changed before local worker activation commit: {expected_id}"
        )));
    }
    if current.cancel.as_ref() != expected_cancel {
        return Err(Error::other(format!(
            "rebalance worker token changed before local worker activation commit: {expected_id}"
        )));
    }
    *current = candidate;
    Ok(())
}

pub(super) fn rollback_local_rebalance_worker_activation(
    meta: Option<&mut super::RebalanceMeta>,
    expected_id: &str,
    activation_token: &CancellationToken,
) -> bool {
    let Some(meta) = meta else {
        return false;
    };
    if meta.id != expected_id || meta.cancel.as_ref() != Some(activation_token) {
        return false;
    }
    if let Some(cancel) = meta.cancel.take() {
        cancel.cancel();
        return true;
    }
    false
}

impl ECStore {
    #[tracing::instrument(skip_all)]
    pub async fn start_rebalance(self: &Arc<Self>) -> Result<()> {
        let _start_guard = self.start_gate.lock().await;
        let _activation_guard = self.rebalance_activation_write_guard(None, "start rebalance").await?;
        let movement_gate = self.ctx.data_movement_operation_gate();
        let _movement_guard = movement_gate.write().await;
        if self.start_rebalance_under_gate().await? {
            self.ctx.advance_data_movement_operation_epoch();
        }
        Ok(())
    }

    pub(super) async fn start_rebalance_under_gate(self: &Arc<Self>) -> Result<bool> {
        info!(
            event = EVENT_REBALANCE_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            state = "starting",
            "Starting rebalance"
        );
        let expected_id: Arc<str> = {
            let rebalance_meta = self.rebalance_meta.read().await;
            Arc::from(rebalance_meta.as_ref().ok_or(Error::ConfigNotFound)?.id.as_str())
        };
        let pool = clone_first_arc(self.pools.as_slice(), "start_rebalance: no pools available")?;
        let activation_fence = match self
            .fence_rebalance_worker_activation(pool.clone(), expected_id.as_ref())
            .await?
        {
            RebalanceWorkerActivationFence::Ready(fence) => fence,
            RebalanceWorkerActivationFence::NotStartedTerminal => return Ok(false),
        };

        let decommission_running = self.is_decommission_running().await;

        let cancel_tx = CancellationToken::new();
        let rx = cancel_tx.clone();
        let activation_outcome;
        let candidate;
        let expected_cancel;
        let must_persist;

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
                return Ok(false);
            }
            expected_cancel = meta.cancel.clone();
            (candidate, activation_outcome, must_persist) = stage_local_rebalance_worker_activation(
                meta,
                expected_id.as_ref(),
                cancel_tx.clone(),
                OffsetDateTime::now_utc(),
            )?;
            if let Err(err) = activation_fence.ensure_held() {
                cancel_tx.cancel();
                return Err(err);
            }
            if !must_persist
                && let Err(err) = commit_local_rebalance_worker_activation_candidate(
                    meta,
                    expected_id.as_ref(),
                    expected_cancel.as_ref(),
                    candidate.clone(),
                )
            {
                cancel_tx.cancel();
                return Err(err);
            }
        }

        if must_persist {
            let save_result = resolve_rebalance_meta_save_result(
                self.save_rebalance_meta_under_activation_fence(
                    pool,
                    &candidate,
                    "start_rebalance persist activation candidate",
                    activation_fence.as_ref(),
                    expected_id.as_ref(),
                )
                .await,
                "start_rebalance persist activation candidate",
            );
            if let Err(err) = save_result {
                cancel_tx.cancel();
                return Err(err);
            }
            let mut rebalance_meta = self.rebalance_meta.write().await;
            let Some(meta) = rebalance_meta.as_mut() else {
                cancel_tx.cancel();
                return Err(Error::ConfigNotFound);
            };
            if let Err(err) = commit_local_rebalance_worker_activation_candidate(
                meta,
                expected_id.as_ref(),
                expected_cancel.as_ref(),
                candidate,
            ) {
                cancel_tx.cancel();
                return Err(err);
            }
        }
        if !must_persist && let Err(err) = activation_fence.ensure_held() {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            rollback_local_rebalance_worker_activation(rebalance_meta.as_mut(), expected_id.as_ref(), &rx);
            return Err(err);
        }
        drop(activation_fence);

        if activation_outcome != RebalanceLocalActivationOutcome::Started {
            return Ok(must_persist);
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
            let mut rebalance_meta = self.rebalance_meta.write().await;
            rollback_local_rebalance_worker_activation(rebalance_meta.as_mut(), expected_id.as_ref(), &rx);
            debug!(
                event = EVENT_REBALANCE_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REBALANCE,
                state = "start_skipped",
                reason = "no_participants",
                "Skipped rebalance start because no pools are participating"
            );
            return Ok(must_persist);
        }

        #[cfg(test)]
        let endpoints = self.instance_endpoints().unwrap_or_else(|| self.endpoints());
        #[cfg(not(test))]
        let endpoints = self.endpoints();

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

            if !runtime_sources::endpoint_pool_is_local(&endpoints, idx) {
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
            let worker_id = Arc::clone(&expected_id);
            workers_started += 1;
            tokio::spawn(async move {
                if let Err(err) = store.rebalance_buckets(rx_clone, pool_idx, worker_id).await {
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
            let mut rebalance_meta = self.rebalance_meta.write().await;
            rollback_local_rebalance_worker_activation(rebalance_meta.as_mut(), expected_id.as_ref(), &rx);
            debug!(
                event = EVENT_REBALANCE_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REBALANCE,
                state = "start_skipped",
                reason = "no_local_participants",
                "Skipped rebalance start because no local pools are participating"
            );
            return Ok(must_persist);
        }

        info!(
            event = EVENT_REBALANCE_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            state = "started",
            worker_count = workers_started,
            "Rebalance started"
        );
        Ok(true)
    }

    #[tracing::instrument(skip(self, rx))]
    async fn rebalance_buckets(self: &Arc<Self>, rx: CancellationToken, pool_index: usize, rebalance_id: Arc<str>) -> Result<()> {
        ensure_valid_rebalance_pool_index(self.pools.len(), pool_index)?;

        let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<Result<()>>(1);

        // Save rebalance metadata periodically
        let store = self.clone();
        let save_rebalance_id = Arc::clone(&rebalance_id);
        let save_task = tokio::spawn(async move {
            let mut timer = tokio::time::interval_at(Instant::now() + Duration::from_secs(30), Duration::from_secs(10));
            let mut msg: String;
            let mut quit = false;

            loop {
                let mut terminal_state_saved = false;
                tokio::select! {
                    result = done_rx.recv() => {
                        quit = true;
                        let now = OffsetDateTime::now_utc();
                        let terminal_event = classify_rebalance_terminal_event(result, now);
                        msg = terminal_event.message().to_string();
                        let movement_gate = store.ctx.data_movement_operation_gate();
                        let movement_guard = movement_gate.write().await;
                        let previous_meta = store.rebalance_meta.read().await.clone();
                        let terminal_state_present = {
                            let mut rebalance_meta = store.rebalance_meta.write().await;
                            super::control::ensure_rebalance_run_id(
                                rebalance_meta.as_ref(),
                                save_rebalance_id.as_ref(),
                                "apply rebalance terminal event",
                            )?;
                            if let Some(meta) = rebalance_meta.as_mut() {
                                let meta_stopped = meta.stopped_at.is_some();
                                if let Some(pool_stat) = meta.pool_stats.get_mut(pool_index) {
                                    if matches!(&terminal_event, super::meta::RebalanceTerminalEvent::Completed { .. })
                                        && has_rebalance_cleanup_warnings(pool_stat)
                                    {
                                        pool_stat.info.stopping = false;
                                        pool_stat.info.status = RebalStatus::Failed;
                                        pool_stat.info.end_time = Some(now);
                                        pool_stat.info.last_error = Some(
                                            pool_stat
                                                .cleanup_warnings
                                                .last_message
                                                .clone()
                                                .unwrap_or_else(|| "rebalance source cleanup warnings prevented completion".to_string()),
                                        );
                                    } else if should_preserve_rebalance_stopped_state(
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
                                        pool_stat.info.stopping = false;
                                        apply_rebalance_terminal_event(
                                            &mut pool_stat.info.status,
                                            &mut pool_stat.info.end_time,
                                            &mut pool_stat.info.last_error,
                                            terminal_event,
                                            now,
                                        );
                                    }
                                    true
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        };

                        if terminal_state_present {
                            if let Err(err) = store
                                .save_rebalance_stats_inner(
                                    pool_index,
                                    RebalSaveOpt::Stats,
                                    Some(save_rebalance_id.as_ref()),
                                )
                                .await
                            {
                                let mut rebalance_meta = store.rebalance_meta.write().await;
                                *rebalance_meta = previous_meta;
                                drop(movement_guard);
                                return Err(Error::other(format!(
                                    "rebalance terminal state save failed for pool {pool_index}: {err}"
                                )));
                            }
                            store.ctx.advance_data_movement_operation_epoch();
                            terminal_state_saved = true;
                        }
                    }
                    _ = timer.tick() => {
                        let now = OffsetDateTime::now_utc();
                        msg = format!("Saving rebalance metadata at {now:?}");
                    }
                }

                if !terminal_state_saved
                    && let Err(err) = store
                        .save_rebalance_stats_for_id(pool_index, RebalSaveOpt::Stats, save_rebalance_id.as_ref())
                        .await
                {
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
        let mut source_cleanup_deferred_attempts = HashMap::new();

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

            let next_bucket = match self.next_rebal_bucket(pool_index, rebalance_id.as_ref()).await {
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
                    self.rebalance_bucket(rx.clone(), bucket.clone(), pool_index, Arc::clone(&rebalance_id))
                        .await,
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
                    let source_cleanup_deferred = last_error.starts_with(REBALANCE_SOURCE_CLEANUP_DEFERRED_ERROR_PREFIX);
                    if should_fail_repeated_rebalance_bucket_defer(&mut deferred_buckets, &bucket, source_cleanup_deferred) {
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

                    let source_cleanup_attempt = if source_cleanup_deferred {
                        source_cleanup_defer_attempt(&mut source_cleanup_deferred_attempts, &bucket)
                    } else {
                        0
                    };
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
                    if let Err(err) = self
                        .defer_rebalance_bucket(pool_index, bucket.clone(), last_error.clone(), rebalance_id.as_ref())
                        .await
                    {
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
                    if source_cleanup_deferred {
                        if source_cleanup_attempt >= super::REBALANCE_SOURCE_CLEANUP_MAX_DEFERS {
                            let err = Error::other(format!(
                                "rebalance bucket {bucket} source cleanup remained unstable after {} deferrals: {last_error}",
                                super::REBALANCE_SOURCE_CLEANUP_MAX_DEFERS
                            ));
                            warn!(
                                event = EVENT_REBALANCE_BUCKET,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_REBALANCE,
                                pool_index,
                                bucket = %bucket,
                                state = "source_cleanup_defer_limit",
                                error = ?err,
                                "Rebalance bucket failed after repeated source cleanup conflicts"
                            );
                            final_result = Err(resolve_rebalance_terminal_error(
                                err.clone(),
                                send_rebalance_done_signal(&done_tx, Err(err.clone()), pool_index).await,
                            ));
                            break;
                        }
                        if let Err(err) =
                            super::worker::wait_rebalance_listing_retry(&rx, REBALANCE_LISTING_RETRY_BASE_DELAY).await
                        {
                            final_result = Err(resolve_rebalance_terminal_error(
                                err.clone(),
                                send_rebalance_done_signal(&done_tx, Err(err.clone()), pool_index).await,
                            ));
                            break;
                        }
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
                source_cleanup_deferred_attempts.remove(&bucket);
                if let Err(err) = self.bucket_rebalance_done(pool_index, bucket, rebalance_id.as_ref()).await {
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

    pub(super) async fn check_if_rebalance_done(&self, pool_index: usize, expected_id: &str) -> Result<bool> {
        let mut rebalance_meta = self.rebalance_meta.write().await;
        super::control::ensure_rebalance_worker_active(rebalance_meta.as_ref(), expected_id, "check rebalance completion")?;

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
                return Ok(true);
            }

            // Mark pool rebalance as done only after it reaches the PercentFreeGoal.
            let pfi = if pool_stat.init_capacity == 0 {
                0.0
            } else {
                (pool_stat.init_free_space + pool_stat.bytes) as f64 / pool_stat.init_capacity as f64
            };

            if !has_deferred_rebalance_error(pool_stat)
                && !has_rebalance_cleanup_warnings(pool_stat)
                && rebalance_goal_reached(
                    pool_stat.init_free_space,
                    pool_stat.init_capacity,
                    pool_stat.bytes,
                    meta.percent_free_goal,
                )
            {
                info!(
                    event = EVENT_REBALANCE_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    pool_index,
                    state = "completion_ready",
                    percent_free = pfi,
                    "Rebalance pool reached completion goal"
                );
                return Ok(true);
            }
        }

        Ok(false)
    }
}

impl ECStore {
    #[tracing::instrument(skip(self))]
    pub async fn save_rebalance_stats(&self, pool_idx: usize, opt: RebalSaveOpt) -> Result<()> {
        let movement_gate = self.ctx.data_movement_operation_gate();
        let _movement_guard = movement_gate.write().await;
        self.save_rebalance_stats_inner(pool_idx, opt, None).await
    }

    pub async fn save_rebalance_stats_for_id(&self, pool_idx: usize, opt: RebalSaveOpt, expected_id: &str) -> Result<()> {
        let movement_gate = self.ctx.data_movement_operation_gate();
        let _movement_guard = movement_gate.write().await;
        self.save_rebalance_stats_inner(pool_idx, opt, Some(expected_id)).await
    }

    pub(super) async fn save_rebalance_stats_inner(
        &self,
        pool_idx: usize,
        opt: RebalSaveOpt,
        expected_id: Option<&str>,
    ) -> Result<()> {
        let meta_to_save = {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            if let Some(expected_id) = expected_id {
                super::control::ensure_rebalance_run_id(rebalance_meta.as_ref(), expected_id, "save rebalance stats")?;
            }
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
        let save_result = match expected_id {
            Some(expected_id) => {
                self.save_rebalance_meta_for_id_with_merge(pool, &meta_to_save, stage.as_str(), expected_id)
                    .await
            }
            None => self.save_rebalance_meta_with_merge(pool, &meta_to_save, stage.as_str()).await,
        };
        resolve_rebalance_meta_save_result(save_result, stage.as_str())?;

        Ok(())
    }
}
