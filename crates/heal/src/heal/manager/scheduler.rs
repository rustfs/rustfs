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
/// The heal scheduler: queue consumption loop and its skip/metric helpers.
use super::*;

impl HealManager {
    /// Start scheduler
    pub(super) async fn start_scheduler(&self) -> Result<()> {
        let config = self.config.clone();
        let heal_queue = self.heal_queue.clone();
        let active_heals = self.active_heals.clone();
        let completed_heals = self.completed_heals.clone();
        let displaced_terminals = self.displaced_terminals.clone();
        let task_aliases = self.task_aliases.clone();
        let retrying_heals = self.retrying_heals.clone();
        let mrf_repair_notice_targets = self.mrf_repair_notice_targets.clone();
        let replacement_recovery_anchors = self.replacement_recovery_anchors.clone();
        let cancel_token = self.cancel_token.clone();
        let statistics = self.statistics.clone();
        let storage = self.storage.clone();
        let notify = self.notify.clone();
        let workload_provider = self.workload_provider.clone();

        tokio::spawn(async move {
            let mut interval = interval(config.read().await.heal_interval);

            loop {
                let event_driven_scheduler_enable = config.read().await.event_driven_scheduler_enable;
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!(
                            target: "rustfs::heal::manager",
                            event = EVENT_HEAL_SCHEDULER_STATE,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_MANAGER,
                            state = "shutdown",
                            "Heal scheduler stopped"
                        );
                        break;
                    }
                    _ = notify.notified(), if event_driven_scheduler_enable => {
                        Self::process_heal_queue(HealQueueContext {
                            heal_queue: &heal_queue,
                            active_heals: &active_heals,
                            completed_heals: &completed_heals,
                            displaced_terminals: &displaced_terminals,
                            task_aliases: &task_aliases,
                            retrying_heals: &retrying_heals,
                            mrf_repair_notice_targets: &mrf_repair_notice_targets,
                            replacement_recovery_anchors: &replacement_recovery_anchors,
                            config: &config,
                            statistics: &statistics,
                            storage: &storage,
                            notify: &notify,
                            cancel_token: &cancel_token,
                            workload_provider: &workload_provider,
                        })
                        .await;
                    }
                    _ = interval.tick() => {
                        Self::process_heal_queue(HealQueueContext {
                            heal_queue: &heal_queue,
                            active_heals: &active_heals,
                            completed_heals: &completed_heals,
                            displaced_terminals: &displaced_terminals,
                            task_aliases: &task_aliases,
                            retrying_heals: &retrying_heals,
                            mrf_repair_notice_targets: &mrf_repair_notice_targets,
                            replacement_recovery_anchors: &replacement_recovery_anchors,
                            config: &config,
                            statistics: &statistics,
                            storage: &storage,
                            notify: &notify,
                            cancel_token: &cancel_token,
                            workload_provider: &workload_provider,
                        })
                        .await;
                    }
                }
            }
        });

        Ok(())
    }

    /// Process heal queue
    /// Processes multiple tasks per cycle when capacity allows and queue has high-priority items
    pub(super) async fn process_heal_queue(context: HealQueueContext<'_>) {
        let HealQueueContext {
            heal_queue,
            active_heals,
            completed_heals,
            displaced_terminals,
            task_aliases,
            retrying_heals,
            mrf_repair_notice_targets,
            replacement_recovery_anchors,
            config,
            statistics,
            storage,
            notify,
            cancel_token,
            workload_provider,
        } = context;

        let config = config.read().await;
        let mainline_pressure = Self::mainline_throttle_active(&config, workload_provider);
        let mut active_heals_guard = active_heals.lock().await;
        publish_active_heal_count(&active_heals_guard);

        // Check if new heal tasks can be started
        let active_count = active_heals_guard.len();
        if active_count >= config.max_concurrent_heals {
            return;
        }

        // Calculate how many tasks we can start this cycle
        let available_slots = config.max_concurrent_heals - active_count;

        let mut queue = heal_queue.lock().await;
        let queue_len = queue.len();
        publish_heal_queue_length(&queue);

        if queue_len == 0 {
            return;
        }

        let mut running_per_set = running_heal_set_counts(&active_heals_guard);
        let mut tasks_started = 0usize;
        let mut delayed_by_mainline_throttle = false;

        for _ in 0..available_slots {
            let selected_request = if config.set_bulkhead_enable || mainline_pressure.is_some() {
                let max_concurrent_per_set = config.max_concurrent_per_set;
                let (selected_request, skipped_sets) = queue.pop_runnable_with_skips(
                    |request| {
                        let set_allowed = !config.set_bulkhead_enable
                            || can_schedule_request(request, &running_per_set, max_concurrent_per_set);
                        let mainline_allowed = mainline_pressure.is_none() || Self::request_bypasses_mainline_throttle(request);
                        set_allowed && mainline_allowed
                    },
                    |request| heal_request_set_key(request).map(|_| heal_request_set_metric_label(request)),
                );
                for skipped_set in skipped_sets {
                    record_scheduler_skip(&skipped_set);
                }
                selected_request
            } else {
                queue.pop_next()
            };

            if let Some(mut request) = selected_request {
                request.options.timeout.get_or_insert(config.task_timeout);
                let task_priority = request.priority;
                let task_type_label = heal_request_type_label(&request).to_string();
                let task_set_label = heal_request_set_metric_label(&request);
                if config.set_bulkhead_enable
                    && let Some(set_key) = heal_request_set_key(&request)
                {
                    *running_per_set.entry(set_key).or_insert(0) += 1;
                }
                let replacement_resume_endpoint = replacement_recovery_anchors
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .get(&request.id)
                    .cloned();
                let task = Arc::new(HealTask::from_replacement_recovery_request(
                    request,
                    storage.clone(),
                    replacement_resume_endpoint,
                ));
                let task_id = task.id.clone();
                active_heals_guard.insert(task_id.clone(), task.clone());
                publish_active_heal_count(&active_heals_guard);
                update_task_running_metric_for_task(&active_heals_guard, task.as_ref());
                let active_heals_clone = active_heals.clone();
                let heal_queue_clone = heal_queue.clone();
                let completed_heals_clone = completed_heals.clone();
                let displaced_terminals_clone = displaced_terminals.clone();
                let task_aliases_clone = task_aliases.clone();
                let retrying_heals_clone = retrying_heals.clone();
                let mrf_repair_notice_targets_clone = mrf_repair_notice_targets.clone();
                let replacement_recovery_anchors_clone = replacement_recovery_anchors.clone();
                let statistics_clone = statistics.clone();
                let notify_clone = notify.clone();
                let manager_cancel_token = cancel_token.clone();
                let task_type_label_for_spawn = task_type_label.clone();
                let task_set_label_for_spawn = task_set_label.clone();
                let config_for_spawn = config.clone();

                // start heal task
                tokio::spawn(async move {
                    debug!(
                        target: "rustfs::heal::manager",
                        event = EVENT_HEAL_SCHEDULER_STATE,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_MANAGER,
                        task_id,
                        priority = ?task_priority,
                        heal_type = %task_type_label_for_spawn,
                        set = %task_set_label_for_spawn,
                        state = "task_started",
                        "Heal scheduler task started"
                    );
                    let result = task.execute().await;
                    let retry_request = retry_request_for_result_with_budget(task.as_ref(), &result).await;
                    match &result {
                        Ok(_) => {
                            debug!(
                                target: "rustfs::heal::manager",
                                event = EVENT_HEAL_SCHEDULER_STATE,
                                component = LOG_COMPONENT_HEAL,
                                subsystem = LOG_SUBSYSTEM_MANAGER,
                                task_id,
                                heal_type = %task_type_label_for_spawn,
                                set = %task_set_label_for_spawn,
                                state = "task_completed",
                                "Heal scheduler task completed"
                            );
                        }
                        Err(e) => {
                            let will_retry = retry_request.is_some();
                            if will_retry {
                                demote_to_debug_when!(task.heal_type.is_per_object(), warn, target: "rustfs::heal::manager", {
                                    event = EVENT_HEAL_SCHEDULER_STATE,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_MANAGER,
                                    task_id,
                                    heal_type = %task_type_label_for_spawn,
                                    set = %task_set_label_for_spawn,
                                    state = "task_retrying",
                                    retry_attempt = task.retry_attempts.saturating_add(1),
                                    error = %e,
                                    "Heal scheduler task retrying"
                                });
                            } else {
                                error!(
                                    target: "rustfs::heal::manager",
                                    event = EVENT_HEAL_SCHEDULER_STATE,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_MANAGER,
                                    task_id,
                                    heal_type = %task_type_label_for_spawn,
                                    set = %task_set_label_for_spawn,
                                    state = "task_failed",
                                    error = %e,
                                    "Heal scheduler task failed"
                                );
                            }
                        }
                    }
                    let retry_request_for_status = retry_request.as_ref().map(|(request, _, error)| HealTaskStatus::Retrying {
                        error: error.clone(),
                        retry_attempt: request.retry_attempts,
                    });
                    let retry_request_for_queue = retry_request;
                    let retry_cancel_token = retry_request_for_queue.as_ref().map(|_| CancellationToken::new());
                    if retry_request_for_queue.is_none() {
                        replacement_recovery_anchors_clone
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .remove(&task_id);
                    }
                    let mut active_heals_guard = active_heals_clone.lock().await;
                    // Keep retry ownership continuous: status snapshots acquire
                    // these locks in the same active -> retrying order.
                    let mut retrying_heals_guard = if let (Some((request, _, error)), Some(cancel_token)) =
                        (retry_request_for_queue.as_ref(), retry_cancel_token.as_ref())
                    {
                        let mut retrying = retrying_heals_clone.lock().await;
                        if active_heals_guard.contains_key(&task_id) {
                            retrying.insert(
                                request.id.clone(),
                                RetryingHeal {
                                    request: request.clone(),
                                    error: error.clone(),
                                    cancel_token: cancel_token.clone(),
                                },
                            );
                            #[cfg(test)]
                            pause_retry_ownership_transition(&task_id, false).await;
                        }
                        Some(retrying)
                    } else {
                        None
                    };
                    let completed_task = active_heals_guard.remove(&task_id);
                    if let Some(completed_task) = completed_task.as_ref() {
                        publish_active_heal_count(&active_heals_guard);
                        update_task_running_metric_for_task(&active_heals_guard, completed_task.as_ref());
                    }
                    let active_count = active_heals_guard.len();
                    drop(retrying_heals_guard.take());
                    drop(active_heals_guard);

                    if let Some(completed_task) = completed_task {
                        let completed_status = if let Some(status) = retry_request_for_status {
                            status
                        } else {
                            completed_task.get_status().await
                        };
                        let terminal_completion = !matches!(completed_status, HealTaskStatus::Retrying { .. });
                        let successful_completion = matches!(completed_status, HealTaskStatus::Completed);
                        let completed_progress = completed_task.get_progress().await;
                        // Single snapshot of the retained window: the task is
                        // finished and already off the active map, so there is
                        // no concurrent writer to race with.
                        let seqed_items = completed_task.get_seqed_result_items().await;
                        let (next_seq, min_seq) = completed_task.result_seq_cursors();
                        let completed_status_entry = CompletedHealStatus {
                            heal_type: completed_task.heal_type.clone(),
                            status: completed_status.clone(),
                            result_items_truncated: completed_task.result_items_truncated(),
                            completed_at: SystemTime::now(),
                            seqed_items,
                            next_seq,
                            min_seq,
                        };
                        let mut completed_heals_guard = completed_heals_clone.lock().await;
                        prune_completed_heal_statuses(&mut completed_heals_guard);
                        completed_heals_guard.insert(task_id.clone(), Arc::new(completed_status_entry));
                        drop(completed_heals_guard);
                        // update statistics
                        let mut stats = statistics_clone.write().await;
                        match completed_status {
                            HealTaskStatus::Completed => {
                                stats.update_task_completion(true);
                                stats.add_healed_objects(completed_progress.objects_healed, completed_progress.bytes_processed);
                            }
                            HealTaskStatus::Retrying { .. } => {}
                            _ => {
                                stats.update_task_completion(false);
                            }
                        }
                        stats.update_running_tasks(usize_to_u64_saturated(active_count));
                        drop(stats);
                        if terminal_completion {
                            let notice_targets = take_mrf_repair_notice_targets(&mrf_repair_notice_targets_clone, &task_id);
                            if successful_completion {
                                emit_mrf_repaired_events(notice_targets);
                            } else {
                                release_mrf_repair_notice_targets(notice_targets);
                            }
                            task_aliases_clone
                                .lock()
                                .await
                                .retain(|alias_id, alias| alias_id != &task_id && alias.task_id != task_id);
                        }
                    }

                    if let (Some((retry_request, retry_delay, retry_error)), Some(retry_cancel_token)) =
                        (retry_request_for_queue, retry_cancel_token)
                    {
                        let retry_request_id = retry_request.id.clone();
                        let retry_attempt = retry_request.retry_attempts;
                        let retry_key = PriorityHealQueue::make_dedup_key(&retry_request);
                        let retry_priority = retry_request.priority;
                        let retry_active_heals = active_heals_clone.clone();
                        let retry_heal_queue = heal_queue_clone.clone();
                        let retrying_heals_for_spawn = retrying_heals_clone.clone();
                        let retry_task_aliases = task_aliases_clone.clone();
                        let retry_displaced_terminals = displaced_terminals_clone.clone();
                        let retry_mrf_repair_notice_targets = mrf_repair_notice_targets_clone.clone();
                        let retry_completed_heals = completed_heals_clone.clone();
                        let retry_notify = notify_clone.clone();
                        let retry_manager_cancel_token = manager_cancel_token.clone();
                        let retry_config = config_for_spawn.clone();
                        tokio::spawn(async move {
                            loop {
                                tokio::select! {
                                    _ = retry_cancel_token.cancelled() => {
                                        debug!(
                                            target: "rustfs::heal::manager",
                                            event = EVENT_HEAL_QUEUE_ADMISSION,
                                            component = LOG_COMPONENT_HEAL,
                                            subsystem = LOG_SUBSYSTEM_MANAGER,
                                            request_id = %retry_request_id,
                                            priority = ?retry_priority,
                                            retry_attempt,
                                            result = "retry_cancelled",
                                            "Heal retry admission decided"
                                        );
                                        return;
                                    }
                                    _ = retry_manager_cancel_token.cancelled() => {
                                        retrying_heals_for_spawn.lock().await.remove(&retry_request_id);
                                        return;
                                    }
                                    _ = sleep(retry_delay) => {}
                                }

                                {
                                    let retrying_heals_guard = retrying_heals_for_spawn.lock().await;
                                    if !retrying_heals_guard.contains_key(&retry_request_id) {
                                        return;
                                    }
                                }

                                let active_duplicate_task_id = {
                                    let active_heals_guard = retry_active_heals.lock().await;
                                    active_heal_for_dedup_key(&active_heals_guard, &retry_key).map(|(task_id, _)| task_id)
                                };
                                if let Some(active_duplicate_task_id) = active_duplicate_task_id {
                                    retrying_heals_for_spawn.lock().await.remove(&retry_request_id);
                                    move_mrf_repair_notice_targets(
                                        &retry_mrf_repair_notice_targets,
                                        &retry_request_id,
                                        &active_duplicate_task_id,
                                    );
                                    debug!(
                                        target: "rustfs::heal::manager",
                                        event = EVENT_HEAL_QUEUE_ADMISSION,
                                        component = LOG_COMPONENT_HEAL,
                                        subsystem = LOG_SUBSYSTEM_MANAGER,
                                        request_id = %retry_request_id,
                                        priority = ?retry_priority,
                                        retry_attempt,
                                        result = "retry_merged_active_duplicate",
                                        "Heal retry admission decided"
                                    );
                                    return;
                                }

                                let mut queue = retry_heal_queue.lock().await;
                                let admission_decision =
                                    Self::admit_request_to_queue(&mut queue, retry_request.clone(), &retry_config, "retry");
                                let admission = admission_decision.result;
                                let should_notify = matches!(admission, HealAdmissionResult::Accepted)
                                    && retry_config.event_driven_scheduler_enable;
                                // Publish the terminal synchronously while the
                                // queue transition is protected. The subsequent
                                // queue -> retrying handoff retains the lock order
                                // used by operations_snapshot.
                                let displaced_terminal = admission_decision
                                    .displaced_request
                                    .as_ref()
                                    .map(|request| record_displaced_terminal(&retry_displaced_terminals, request));
                                match admission {
                                    HealAdmissionResult::Accepted => {
                                        // Transfer ownership while holding queue -> retrying,
                                        // matching operations_snapshot's lock order.
                                        #[cfg(test)]
                                        pause_retry_ownership_transition(&retry_request_id, true).await;
                                        retrying_heals_for_spawn.lock().await.remove(&retry_request_id);
                                        let displaced_task_id = admission_decision.displaced_task_id().map(ToOwned::to_owned);
                                        drop(queue);
                                        if let (Some(displaced_task_id), Some(displaced_terminal)) =
                                            (displaced_task_id, displaced_terminal)
                                        {
                                            remove_displaced_task_aliases(
                                                &retry_task_aliases,
                                                &retry_displaced_terminals,
                                                &displaced_task_id,
                                                &displaced_terminal,
                                            )
                                            .await;
                                            remove_mrf_repair_notice_targets(
                                                &retry_mrf_repair_notice_targets,
                                                &displaced_task_id,
                                            );
                                        }
                                        retry_completed_heals.lock().await.remove(&retry_request_id);
                                        debug!(
                                            target: "rustfs::heal::manager",
                                            event = EVENT_HEAL_QUEUE_ADMISSION,
                                            component = LOG_COMPONENT_HEAL,
                                            subsystem = LOG_SUBSYSTEM_MANAGER,
                                            request_id = %retry_request_id,
                                            priority = ?retry_priority,
                                            retry_attempt,
                                            retry_delay_ms = retry_delay.as_millis(),
                                            error = %retry_error,
                                            result = "retry_enqueued",
                                            "Heal retry admission decided"
                                        );
                                        if should_notify {
                                            retry_notify.notify_one();
                                        }
                                        return;
                                    }
                                    HealAdmissionResult::Merged => {
                                        let merged_task_id =
                                            queue.queued_request_id_for_dedup_key(&retry_key).map(ToOwned::to_owned);
                                        retrying_heals_for_spawn.lock().await.remove(&retry_request_id);
                                        drop(queue);
                                        if let Some(merged_task_id) = merged_task_id {
                                            move_mrf_repair_notice_targets(
                                                &retry_mrf_repair_notice_targets,
                                                &retry_request_id,
                                                &merged_task_id,
                                            );
                                        }
                                        debug!(
                                            target: "rustfs::heal::manager",
                                            event = EVENT_HEAL_QUEUE_ADMISSION,
                                            component = LOG_COMPONENT_HEAL,
                                            subsystem = LOG_SUBSYSTEM_MANAGER,
                                            request_id = %retry_request_id,
                                            priority = ?retry_priority,
                                            retry_attempt,
                                            result = "retry_merged_duplicate",
                                            "Heal retry admission decided"
                                        );
                                        return;
                                    }
                                    HealAdmissionResult::Full => {
                                        // admit_request_to_queue already logged the
                                        // rejection (context = "retry"); this repeats
                                        // every backoff cycle while the queue stays
                                        // full, so keep it at debug!.
                                        debug!(
                                            target: "rustfs::heal::manager",
                                            event = EVENT_HEAL_QUEUE_ADMISSION,
                                            component = LOG_COMPONENT_HEAL,
                                            subsystem = LOG_SUBSYSTEM_MANAGER,
                                            request_id = %retry_request_id,
                                            priority = ?retry_priority,
                                            retry_attempt,
                                            result = "retry_rejected_full",
                                            "Heal retry admission decided"
                                        );
                                    }
                                    HealAdmissionResult::Dropped(reason) => {
                                        debug!(
                                            target: "rustfs::heal::manager",
                                            event = EVENT_HEAL_QUEUE_ADMISSION,
                                            component = LOG_COMPONENT_HEAL,
                                            subsystem = LOG_SUBSYSTEM_MANAGER,
                                            request_id = %retry_request_id,
                                            priority = ?retry_priority,
                                            retry_attempt,
                                            reason = reason.as_str(),
                                            result = "retry_dropped",
                                            "Heal retry admission decided"
                                        );
                                    }
                                }
                            }
                        });
                    }
                    notify_clone.notify_one();
                });
                tasks_started += 1;
            } else {
                delayed_by_mainline_throttle = mainline_pressure.is_some();
                break;
            }
        }

        // Update statistics for all started tasks
        let mut stats = statistics.write().await;
        stats.total_tasks += tasks_started as u64;
        stats.update_running_tasks(active_heals_guard.len() as u64);
        publish_active_heal_count(&active_heals_guard);
        publish_heal_queue_length(&queue);

        if delayed_by_mainline_throttle && let Some(pressure) = mainline_pressure {
            Self::record_mainline_throttle_delay(pressure, &config);
            Self::schedule_mainline_throttle_recheck(notify.clone(), config.mainline_max_sleep);
        }

        // Log queue status if items remain
        if !queue.is_empty() {
            let remaining = queue.len();
            if remaining > 10 {
                info!(
                    target: "rustfs::heal::manager",
                    event = EVENT_HEAL_QUEUE_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_MANAGER,
                    queue_len = remaining,
                    active_tasks = active_heals_guard.len(),
                    state = "backlog_high",
                    "Heal queue backlog high"
                );
            }
        }
    }
}
pub(super) fn heal_request_set_key(request: &HealRequest) -> Option<String> {
    match &request.heal_type {
        HealType::ErasureSet { set_disk_id, .. } => Some(set_disk_id.clone()),
        HealType::Object { .. } => request.options.set_key(),
        _ => None,
    }
}

pub(super) fn heal_request_type_label(request: &HealRequest) -> &'static str {
    request.heal_type.kind_label()
}

pub(super) fn heal_request_set_metric_label(request: &HealRequest) -> String {
    heal_request_set_key(request).unwrap_or_else(|| request.options.set_metric_label())
}

pub(super) fn record_scheduler_skip(set_label: &str) {
    counter!(
        "rustfs_heal_scheduler_skip_total",
        "reason" => "set_limit".to_string(),
        "set" => set_label.to_string()
    )
    .increment(1);
}

pub(super) fn update_task_running_metric_for_task(active_heals: &HashMap<String, Arc<HealTask>>, task: &HealTask) {
    let type_label = task.metric_type_label();
    let set_label = task.metric_set_label();
    let count = active_heals
        .values()
        .filter(|active_task| active_task.metric_type_label() == type_label && active_task.metric_set_label() == set_label)
        .count();

    gauge!(
        "rustfs_heal_task_running",
        "type" => type_label.to_string(),
        "set" => set_label
    )
    .set(count as f64);
}

pub(super) fn running_heal_set_counts(active_heals: &HashMap<String, Arc<HealTask>>) -> HashMap<String, usize> {
    let mut running = HashMap::new();
    for task in active_heals.values() {
        if let Some(set_key) = heal_request_set_key_for_task(task) {
            *running.entry(set_key).or_insert(0) += 1;
        }
    }
    running
}

fn remove_mrf_repair_notice_targets(registry: &Arc<StdMutex<HashMap<String, Vec<MrfRepairNoticeTarget>>>>, task_id: &str) {
    let targets = lock_mrf_repair_notice_targets(registry).remove(task_id);
    if let Some(targets) = targets {
        for target in targets {
            rustfs_common::mrf_channel::release_mrf_identity(
                target.kind,
                &target.bucket,
                &target.object,
                target.version_id,
                target.scope,
                target.lease,
            );
        }
    }
}

fn take_mrf_repair_notice_targets(
    registry: &Arc<StdMutex<HashMap<String, Vec<MrfRepairNoticeTarget>>>>,
    task_id: &str,
) -> Vec<MrfRepairNoticeTarget> {
    lock_mrf_repair_notice_targets(registry).remove(task_id).unwrap_or_default()
}

fn move_mrf_repair_notice_targets(
    registry: &Arc<StdMutex<HashMap<String, Vec<MrfRepairNoticeTarget>>>>,
    from_task_id: &str,
    to_task_id: &str,
) {
    if from_task_id == to_task_id {
        return;
    }
    let mut registry = lock_mrf_repair_notice_targets(registry);
    let Some(moving) = registry.remove(from_task_id) else {
        return;
    };
    let targets = registry.entry(to_task_id.to_string()).or_default();
    for target in moving {
        if !targets.contains(&target) {
            targets.push(target);
        }
    }
}

fn emit_mrf_repaired_events(targets: Vec<MrfRepairNoticeTarget>) {
    for target in targets {
        rustfs_common::mrf_channel::note_mrf_repaired(&target.bucket, &target.object, target.version_id);
        rustfs_common::mrf_channel::release_mrf_identity(
            target.kind,
            &target.bucket,
            &target.object,
            target.version_id,
            target.scope,
            target.lease,
        );
    }
}

fn release_mrf_repair_notice_targets(targets: Vec<MrfRepairNoticeTarget>) {
    for target in targets {
        rustfs_common::mrf_channel::release_mrf_identity(
            target.kind,
            &target.bucket,
            &target.object,
            target.version_id,
            target.scope,
            target.lease,
        );
    }
}

pub(super) fn heal_request_set_key_for_task(task: &HealTask) -> Option<String> {
    match &task.heal_type {
        HealType::ErasureSet { set_disk_id, .. } => Some(set_disk_id.clone()),
        HealType::Object { .. } => task.options.set_key(),
        _ => None,
    }
}

pub(super) fn prune_completed_heal_statuses(completed_heals: &mut HashMap<String, Arc<CompletedHealStatus>>) {
    let Ok(now) = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) else {
        return;
    };

    completed_heals.retain(|_, completed| {
        completed
            .completed_at
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|completed_at| now.saturating_sub(completed_at) <= KEEP_HEAL_TASK_STATUS_DURATION)
            .unwrap_or(false)
    });
}

pub(super) fn can_schedule_request(
    request: &HealRequest,
    running_per_set: &HashMap<String, usize>,
    max_concurrent_per_set: usize,
) -> bool {
    match heal_request_set_key(request) {
        Some(set_key) => running_per_set.get(&set_key).copied().unwrap_or(0) < max_concurrent_per_set,
        None => true,
    }
}
