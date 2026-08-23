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
/// The automatic disk scanner: replacement discovery and unformatted-disk enqueue.
use super::*;

impl HealManager {
    /// Start background task to auto scan local disks and enqueue erasure set heal requests
    pub(super) async fn start_auto_disk_scanner(&self) -> Result<()> {
        let config = self.config.clone();
        let heal_queue = self.heal_queue.clone();
        let active_heals = self.active_heals.clone();
        let task_aliases = self.task_aliases.clone();
        let displaced_terminals = self.displaced_terminals.clone();
        let mrf_repair_notice_targets = self.mrf_repair_notice_targets.clone();
        let storage = self.storage.clone();
        let replacement_recovery_anchors = self.replacement_recovery_anchors.clone();
        let replacement_recovery_blocked_sets = self.replacement_recovery_blocked_sets.clone();
        let cancel_token = self.cancel_token.clone();
        let notify = self.notify.clone();
        let mut duration = {
            let config = config.read().await;
            config.heal_interval
        };
        if duration < Duration::from_secs(10) {
            duration = Duration::from_secs(10);
        }
        info!(
            target: "rustfs::heal::manager",
            event = EVENT_HEAL_AUTO_SCAN_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
            state = "started",
            interval = ?duration,
            "Heal auto disk scanner started"
        );

        tokio::spawn(async move {
            let mut interval = interval(duration);

            loop {
                let mut candidate_count = 0usize;
                let mut skipped_duplicate_count = 0usize;
                let mut skipped_invalid_count = 0usize;
                let mut enqueued_count = 0usize;
                let mut not_enqueued_count = 0usize;
                let mut dropped_count = 0usize;
                let mut full_count = 0usize;
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!(
                            target: "rustfs::heal::manager",
                            event = EVENT_HEAL_AUTO_SCAN_STATE,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                            state = "shutdown",
                            "Heal auto disk scanner stopped"
                        );
                        break;
                    }
                    _ = interval.tick() => {
                        // Build list of endpoints that need healing
                        let mut endpoints = HashMap::<String, Vec<Endpoint>>::new();
                        let mut durable_recoveries = HashMap::<String, (String, Vec<Endpoint>, Vec<String>, String)>::new();
                        let mut conflicted_recovery_sets = HashSet::<String>::new();
                        let mut deferred_replacement_endpoints = HashSet::<String>::new();
                        let local_disks = {
                            let local_disk_map = local_disk_map_read().await;
                            local_disk_map.values().flatten().cloned().collect::<Vec<_>>()
                        };
                        let local_endpoints = local_disks.iter().map(|disk| disk.endpoint()).collect::<Vec<_>>();
                        let blocked_sets = replacement_recovery_blocked_sets
                            .lock()
                            .expect("replacement recovery blocked set lock poisoned")
                            .clone();
                        if !blocked_sets.is_empty() {
                            let mut retry_succeeded = HashSet::new();
                            let mut retry_failed = HashSet::new();
                            for disk in &local_disks {
                                let endpoint = disk.endpoint();
                                let Some(set_disk_id) =
                                    crate::heal::utils::format_set_disk_id_from_i32(endpoint.pool_idx, endpoint.set_idx)
                                else {
                                    continue;
                                };
                                if !blocked_sets.contains(&set_disk_id) {
                                    continue;
                                }
                                match Self::validate_replacement_recovery_records(disk).await {
                                    Ok(()) => {
                                        retry_succeeded.insert(set_disk_id);
                                    }
                                    Err(error) => {
                                        retry_failed.insert(set_disk_id.clone());
                                        conflicted_recovery_sets.insert(set_disk_id);
                                        warn!(
                                            target: "rustfs::heal::manager",
                                            event = EVENT_HEAL_AUTO_SCAN_ENQUEUE,
                                            component = LOG_COMPONENT_HEAL,
                                            subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                            endpoint = %endpoint,
                                            error = %error,
                                            "Replacement recovery retry failed"
                                        );
                                    }
                                }
                            }
                            let mut blocked = replacement_recovery_blocked_sets
                                .lock()
                                .expect("replacement recovery blocked set lock poisoned");
                            unblock_replacement_recovery_sets_after_validation(&mut blocked, retry_succeeded, &retry_failed);
                        }
                        for disk in &local_disks {
                            let endpoint = disk.endpoint();
                                let runtime_state = disk.runtime_state();
                                let set_disk_id =
                                    crate::heal::utils::format_set_disk_id_from_i32(endpoint.pool_idx, endpoint.set_idx);
                            if set_disk_id.as_ref().is_some_and(|set_disk_id| {
                                replacement_recovery_blocked_sets
                                    .lock()
                                    .expect("replacement recovery blocked set lock poisoned")
                                    .contains(set_disk_id)
                            }) {
                                skipped_invalid_count += 1;
                                debug!(
                                    target: "rustfs::heal::manager",
                                    event = EVENT_HEAL_AUTO_SCAN_DISK,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                    endpoint = %endpoint,
                                    set_disk_id = set_disk_id.as_deref().unwrap_or_default(),
                                    disk_state = "replacement_recovery_blocked",
                                    "Heal auto-scan replacement deferred because durable recovery is blocked"
                                );
                                continue;
                            }

                            // detect unformatted disk via get_disk_id()
                            match disk.get_disk_id().await {
                                Err(DiskError::UnformattedDisk) => {
                                    if !super::super::replacement_readiness::auto_replacement_target_ready(disk, &local_disks)
                                        .await
                                    {
                                        deferred_replacement_endpoints.insert(endpoint.to_string());
                                        skipped_invalid_count += 1;
                                        debug!(
                                            target: "rustfs::heal::manager",
                                            event = EVENT_HEAL_AUTO_SCAN_DISK,
                                            component = LOG_COMPONENT_HEAL,
                                            subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                            endpoint = %endpoint,
                                            disk_state = "replacement_path_unavailable",
                                            "Heal auto-scan replacement deferred"
                                        );
                                        continue;
                                    }
                                    let Some(set_disk_id) = set_disk_id else {
                                        skipped_invalid_count += 1;
                                        continue;
                                    };
                                    candidate_count += 1;
                                    debug!(
                                        target: "rustfs::heal::manager",
                                        event = EVENT_HEAL_AUTO_SCAN_DISK,
                                        component = LOG_COMPONENT_HEAL,
                                        subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                        endpoint = %endpoint,
                                        disk_state = "unformatted",
                                        "Heal auto-scan candidate detected"
                                    );
                                    endpoints.entry(set_disk_id).or_default().push(endpoint);
                                }
                                Err(e) => {
                                    warn!(
                                        target: "rustfs::heal::manager",
                                        event = EVENT_HEAL_AUTO_SCAN_DISK,
                                        component = LOG_COMPONENT_HEAL,
                                        subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                        endpoint = %endpoint,
                                        disk_state = "check_failed",
                                        error = ?e,
                                        "Heal auto-scan disk inspection failed"
                                    );
                                }
                                Ok(_) => {
                                    if runtime_state.as_str() == "returning" && let Some(set_disk_id) = set_disk_id {
                                        candidate_count += 1;
                                        debug!(
                                            target: "rustfs::heal::manager",
                                            event = EVENT_HEAL_AUTO_SCAN_DISK,
                                            component = LOG_COMPONENT_HEAL,
                                            subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                            endpoint = %endpoint,
                                            set_disk_id,
                                            disk_state = "returning",
                                            "Heal auto-scan returning disk candidate detected"
                                        );
                                        endpoints.entry(set_disk_id).or_default().push(endpoint);
                                    }
                                }
                            }
                        }

                        // Once formatting succeeds a replacement is no longer
                        // discoverable as UnformattedDisk. Re-admit exactly one
                        // incomplete durable generation per set after bounded
                        // scheduler retries are exhausted, or re-admit its
                        // verified terminal cleanup. Multiple generations are a
                        // durable conflict: leave every marker/state intact and
                        // require reconciliation rather than choosing one.
                        for disk in &local_disks {
                            let endpoint = disk.endpoint();
                            let disk_set_disk_id =
                                crate::heal::utils::format_set_disk_id_from_i32(endpoint.pool_idx, endpoint.set_idx);
                            let replacement_task_ids = match ResumeUtils::get_replacement_intent_tasks(disk).await {
                                Ok(task_ids) => task_ids,
                                Err(error) => {
                                    let endpoint_string = endpoint.to_string();
                                    if replacement_discovery_error_is_expected_for_deferred_endpoint(
                                        &error,
                                        &endpoint_string,
                                        &deferred_replacement_endpoints,
                                    ) {
                                        debug!(
                                            target: "rustfs::heal::manager",
                                            event = EVENT_HEAL_AUTO_SCAN_ENQUEUE,
                                            component = LOG_COMPONENT_HEAL,
                                            subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                            endpoint = %endpoint,
                                            disk_state = "replacement_path_unavailable",
                                            result = "recovery_records_unavailable",
                                            "Replacement recovery discovery skipped for deferred replacement"
                                        );
                                        continue;
                                    }
                                    if let Some(set_disk_id) = &disk_set_disk_id {
                                        conflicted_recovery_sets.insert(set_disk_id.clone());
                                    }
                                    warn!(
                                        target: "rustfs::heal::manager",
                                        event = EVENT_HEAL_AUTO_SCAN_ENQUEUE,
                                        component = LOG_COMPONENT_HEAL,
                                        subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                        endpoint = %endpoint,
                                        error = %error,
                                        "Replacement recovery discovery failed"
                                    );
                                    continue;
                                }
                            };
                            for task_id in replacement_task_ids {
                                let resume_manager = match ResumeManager::load_replacement_intent(disk.clone(), &task_id).await {
                                    Ok(resume_manager) => resume_manager,
                                    Err(error) => {
                                        if let Some(set_disk_id) = &disk_set_disk_id {
                                            conflicted_recovery_sets.insert(set_disk_id.clone());
                                        }
                                        warn!(
                                            target: "rustfs::heal::manager",
                                            event = EVENT_HEAL_AUTO_SCAN_ENQUEUE,
                                            component = LOG_COMPONENT_HEAL,
                                            subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                            endpoint = %endpoint,
                                            task_id,
                                            error = %error,
                                            "Replacement recovery intent load failed"
                                        );
                                        continue;
                                    }
                                };
                                let state = resume_manager.get_state().await;
                                if !durable_replacement_recovery_is_due(&state, &task_id) {
                                    continue;
                                }
                                if !matches!(state.replacement_phase, ReplacementPhase::CleanupPending) {
                                    let Ok(identities) = storage.replacement_target_identities(&state.replacement_targets).await else {
                                        continue;
                                    };
                                    if identities != state.replacement_target_identities {
                                        continue;
                                    }
                                }
                                let targets = state
                                    .replacement_targets
                                    .iter()
                                    .filter_map(|target| {
                                        local_endpoints
                                            .iter()
                                            .find(|endpoint| endpoint.to_string() == *target)
                                            .cloned()
                                    })
                                    .collect::<Vec<_>>();
                                if targets.len() != state.replacement_targets.len() {
                                    continue;
                                }
                                let Some(set_disk_id) = crate::heal::utils::format_set_disk_id_from_i32(
                                    targets[0].pool_idx,
                                    targets[0].set_idx,
                                ) else {
                                    continue;
                                };
                                if targets.iter().any(|target| {
                                    crate::heal::utils::format_set_disk_id_from_i32(target.pool_idx, target.set_idx)
                                        .as_deref()
                                        != Some(set_disk_id.as_str())
                                }) {
                                    continue;
                                }
                                let resume_endpoint = disk.endpoint().to_string();
                                match durable_recoveries.get(&set_disk_id) {
                                    Some((existing_task_id, _, _, existing_anchor))
                                        if existing_task_id != &task_id || existing_anchor != &resume_endpoint => {
                                        replacement_recovery_blocked_sets
                                            .lock()
                                            .expect("replacement recovery blocked set lock poisoned")
                                            .insert(set_disk_id.clone());
                                        conflicted_recovery_sets.insert(set_disk_id);
                                    }
                                    Some(_) => {}
                                    None => {
                                        durable_recoveries.insert(
                                            set_disk_id,
                                            (task_id, targets, state.replacement_buckets, resume_endpoint),
                                        );
                                    }
                                }
                            }
                        }

                        for set_disk_id in &conflicted_recovery_sets {
                            durable_recoveries.remove(set_disk_id);
                            endpoints.remove(set_disk_id);
                            warn!(
                                target: "rustfs::heal::manager",
                                event = EVENT_HEAL_AUTO_SCAN_ENQUEUE,
                                component = LOG_COMPONENT_HEAL,
                                subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                set_disk_id,
                                result = "durable_generation_conflict",
                                "Replacement recovery deferred because multiple durable generations exist"
                            );
                        }

                        for (set_disk_id, (_, targets, _, _)) in &durable_recoveries {
                            let expected = targets.iter().map(ToString::to_string).collect::<HashSet<_>>();
                            let observed = endpoints
                                .get(set_disk_id)
                                .map(|endpoints| endpoints.iter().map(ToString::to_string).collect::<HashSet<_>>())
                                .unwrap_or_default();
                            if !observed.is_subset(&expected) {
                                replacement_recovery_blocked_sets
                                    .lock()
                                    .expect("replacement recovery blocked set lock poisoned")
                                    .insert(set_disk_id.clone());
                                conflicted_recovery_sets.insert(set_disk_id.clone());
                                continue;
                            }
                            endpoints.entry(set_disk_id.clone()).or_default().extend(targets.clone());
                        }
                        for set_disk_id in &conflicted_recovery_sets {
                            durable_recoveries.remove(set_disk_id);
                            endpoints.remove(set_disk_id);
                        }

                        for target_endpoints in endpoints.values_mut() {
                            target_endpoints.sort_by_key(ToString::to_string);
                            target_endpoints.dedup_by(|left, right| left.to_string() == right.to_string());
                        }

                        if endpoints.is_empty() {
                            debug!(
                                target: "rustfs::heal::manager",
                                event = EVENT_HEAL_AUTO_SCAN_STATE,
                                component = LOG_COMPONENT_HEAL,
                                subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                state = "idle",
                                "Heal auto disk scanner idle"
                            );
                            continue;
                        }

                        // Admit one set task with every ready replacement target. Queue deduplication is
                        // set-scoped, so admitting endpoints independently would silently drop later targets.
                        for (set_disk_id, endpoints) in endpoints {
                            if replacement_recovery_blocked_sets
                                .lock()
                                .expect("replacement recovery blocked set lock poisoned")
                                .contains(&set_disk_id)
                            {
                                skipped_invalid_count += 1;
                                debug!(
                                    target: "rustfs::heal::manager",
                                    event = EVENT_HEAL_AUTO_SCAN_ENQUEUE,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                    set_disk_id,
                                    result = "replacement_recovery_blocked",
                                    "Heal auto-scan replacement admission deferred because durable recovery is blocked"
                                );
                                continue;
                            }
                            // skip if already queued or healing
                            // Use consistent lock order: queue first, then active_heals to avoid deadlock
                            let mut skip = false;
                            {
                                let queue = heal_queue.lock().await;
                                if queue.contains_erasure_set(&set_disk_id) {
                                    skip = true;
                                }
                            }
                            if !skip {
                                let active = active_heals.lock().await;
                                if active.values().any(|task| {
                                    matches!(
                                        &task.heal_type,
                                        crate::heal::task::HealType::ErasureSet { set_disk_id: active_id, .. }
                                        if active_id == &set_disk_id
                                    )
                                }) {
                                    skip = true;
                                }
                            }

                            if skip {
                                skipped_duplicate_count += 1;
                                debug!(
                                    target: "rustfs::heal::manager",
                                    event = EVENT_HEAL_AUTO_SCAN_ENQUEUE,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                    endpoint_count = endpoints.len(),
                                    set_disk_id,
                                    result = "skipped_duplicate",
                                    "Heal auto-scan duplicate skipped"
                                );
                                continue;
                            }

                            // enqueue erasure set heal request for all ready replacements in this set
                            let recovery = durable_recoveries.remove(&set_disk_id);
                            let mut req = HealRequest::new(
                                HealType::ErasureSet {
                                    buckets: recovery
                                        .as_ref()
                                        .map(|(_, _, buckets, _)| buckets.clone())
                                        .unwrap_or_default(),
                                    set_disk_id: set_disk_id.clone(),
                                },
                                HealOptions {
                                    pool_index: endpoints
                                        .first()
                                        .and_then(|endpoint| usize::try_from(endpoint.pool_idx).ok()),
                                    set_index: endpoints
                                        .first()
                                        .and_then(|endpoint| usize::try_from(endpoint.set_idx).ok()),
                                    timeout: None,
                                    ..HealOptions::default()
                                },
                                HealPriority::Low,
                            );
                            let recovery_anchor = recovery.as_ref().map(|(_, _, _, anchor)| anchor.clone());
                            if let Some((task_id, _, _, _)) = recovery {
                                req.id = task_id;
                            }
                            req.source = HealRequestSource::AutoHeal;
                            req.heal_endpoints = endpoints.iter().map(ToString::to_string).collect();
                            let request_id = req.id.clone();
                            let endpoint_count = req.heal_endpoints.len();
                            let config = config.read().await;
                            let mut queue = heal_queue.lock().await;
                            let admission_decision = Self::admit_request_to_queue(&mut queue, req, &config, "auto_scan");
                            let admission = admission_decision.result;
                            let should_notify =
                                matches!(admission, HealAdmissionResult::Accepted) && config.event_driven_scheduler_enable;
                            let displaced_terminal = admission_decision
                                .displaced_request
                                .as_ref()
                                .map(|request| record_displaced_terminal(&displaced_terminals, request));
                            if matches!(admission, HealAdmissionResult::Accepted)
                                && let Some(anchor) = recovery_anchor
                            {
                                replacement_recovery_anchors
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                                    .insert(request_id, anchor);
                            }
                            drop(queue);
                            drop(config);
                            if let (Some(displaced_task_id), Some(displaced_terminal)) =
                                (admission_decision.displaced_task_id().map(ToOwned::to_owned), displaced_terminal)
                            {
                                remove_displaced_task_aliases(
                                    &task_aliases,
                                    &displaced_terminals,
                                    &displaced_task_id,
                                    &displaced_terminal,
                                )
                                .await;
                                if let Some(targets) = lock_mrf_repair_notice_targets(&mrf_repair_notice_targets).remove(&displaced_task_id) {
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
                            if matches!(admission, HealAdmissionResult::Accepted) {
                                if should_notify {
                                    notify.notify_one();
                                }
                                enqueued_count += 1;
                                debug!(
                                    target: "rustfs::heal::manager",
                                    event = EVENT_HEAL_AUTO_SCAN_ENQUEUE,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                    endpoint_count,
                                    set_disk_id,
                                    bucket_count = 0,
                                    result = "enqueued",
                                    "Heal auto-scan task enqueued"
                                );
                            } else {
                                if matches!(admission, HealAdmissionResult::Merged) {
                                    skipped_duplicate_count += 1;
                                } else {
                                    not_enqueued_count += 1;
                                }
                                if matches!(admission, HealAdmissionResult::Full) {
                                    full_count += 1;
                                }
                                if matches!(admission, HealAdmissionResult::Dropped(_)) {
                                    dropped_count += 1;
                                }
                                debug!(
                                    target: "rustfs::heal::manager",
                                    event = EVENT_HEAL_AUTO_SCAN_ENQUEUE,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                                    endpoint_count,
                                    set_disk_id,
                                    bucket_count = 0,
                                    admission = admission.result_label(),
                                    reason = admission.reason_label(),
                                    result = "not_enqueued",
                                    "Heal auto-scan task not enqueued"
                                );
                            }
                        }
                        info!(
                            target: "rustfs::heal::manager",
                            event = EVENT_HEAL_AUTO_SCAN_STATE,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_DISK_SCANNER,
                            state = "cycle_completed",
                            candidate_count,
                            enqueued_count,
                            not_enqueued_count,
                            dropped_count,
                            full_count,
                            skipped_duplicate_count,
                            skipped_invalid_count,
                            "Heal auto-scan cycle completed"
                        );
                    }
                }
            }
        });
        Ok(())
    }
}
