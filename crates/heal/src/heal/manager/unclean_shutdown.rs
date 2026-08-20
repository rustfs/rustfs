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
/// Unclean-shutdown recovery: durable replacement-intent discovery and healing-marker rewrite.
use super::*;

pub(super) fn durable_replacement_recovery_is_due(state: &ResumeState, task_id: &str) -> bool {
    state.replacement_generation.as_deref() == Some(task_id)
        && !state.replacement_targets.is_empty()
        && ((!state.completed
            && matches!(state.replacement_phase, ReplacementPhase::Intent | ReplacementPhase::Rebuilding)
            && state.retry_count >= state.max_retries)
            || (state.completed
                && matches!(state.replacement_phase, ReplacementPhase::Verified | ReplacementPhase::CleanupPending)))
}

pub(super) fn replacement_discovery_error_is_expected_for_deferred_endpoint(
    error: &Error,
    endpoint: &str,
    deferred_replacement_endpoints: &HashSet<String>,
) -> bool {
    matches!(error, Error::Disk(DiskError::UnformattedDisk)) && deferred_replacement_endpoints.contains(endpoint)
}

pub(super) fn unblock_replacement_recovery_sets_after_validation(
    blocked_sets: &mut HashSet<String>,
    retry_succeeded: HashSet<String>,
    retry_failed: &HashSet<String>,
) {
    for set_disk_id in retry_succeeded {
        if !retry_failed.contains(&set_disk_id) {
            blocked_sets.remove(&set_disk_id);
        }
    }
}

impl HealManager {
    /// Detect whether the previous run ended without a clean shutdown and, if so,
    /// enqueue a full erasure-set heal for every local set. Also (re)writes the
    /// marker for the current run; [`super::super::clear_unclean_shutdown_markers`]
    /// removes it again during graceful shutdown. Best-effort: failures only log.
    pub(super) async fn process_unclean_shutdown(&self) {
        let mut unclean = false;
        let mut set_disk_ids = HashSet::new();
        let mut replacement_intents = HashMap::<String, (String, Vec<String>, Vec<String>, String)>::new();
        let mut replacement_restarts = HashMap::<String, (String, Vec<String>)>::new();
        let mut conflicted_replacement_sets = HashSet::new();

        {
            let local_disks = {
                let local_disk_map = local_disk_map_read().await;
                local_disk_map.values().flatten().cloned().collect::<Vec<_>>()
            };
            for disk in &local_disks {
                let endpoint = disk.endpoint();
                match disk
                    .read_all(super::super::RUSTFS_META_BUCKET, super::super::UNCLEAN_SHUTDOWN_MARKER_PATH)
                    .await
                {
                    Ok(_) => unclean = true,
                    Err(DiskError::FileNotFound) | Err(DiskError::VolumeNotFound) => {}
                    Err(err) => {
                        debug!(
                            target: "rustfs::heal::manager",
                            event = EVENT_HEAL_UNCLEAN_SHUTDOWN,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_MANAGER,
                            endpoint = %endpoint,
                            error = ?err,
                            "Unclean-shutdown marker check failed"
                        );
                    }
                }

                let marker = SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs().to_string())
                    .unwrap_or_default();
                if let Err(err) = disk
                    .write_all(
                        super::super::RUSTFS_META_BUCKET,
                        super::super::UNCLEAN_SHUTDOWN_MARKER_PATH,
                        marker.into(),
                    )
                    .await
                {
                    warn!(
                        target: "rustfs::heal::manager",
                        event = EVENT_HEAL_UNCLEAN_SHUTDOWN,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_MANAGER,
                        endpoint = %endpoint,
                        error = ?err,
                        "Unclean-shutdown marker write failed"
                    );
                }

                let disk_set_disk_id = crate::heal::utils::format_set_disk_id_from_i32(endpoint.pool_idx, endpoint.set_idx);
                if let Some(set_disk_id) = &disk_set_disk_id {
                    set_disk_ids.insert(set_disk_id.clone());
                }

                // Legacy flat records are inspected only while starting. The
                // periodic scanner lists the dedicated replacement directory.
                if let Err(error) = ResumeUtils::migrate_legacy_replacement_records(disk).await {
                    if let Some(set_disk_id) = &disk_set_disk_id {
                        self.block_replacement_recovery_set(set_disk_id);
                    }
                    warn!(
                        target: "rustfs::heal::manager",
                        event = EVENT_HEAL_UNCLEAN_SHUTDOWN,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_MANAGER,
                        endpoint = %endpoint,
                        error = %error,
                        "Legacy replacement recovery migration failed"
                    );
                }
                let replacement_task_ids = match ResumeUtils::get_replacement_intent_tasks(disk).await {
                    Ok(task_ids) => task_ids,
                    Err(error) => {
                        if let Some(set_disk_id) = &disk_set_disk_id {
                            self.block_replacement_recovery_set(set_disk_id);
                        }
                        warn!(
                            target: "rustfs::heal::manager",
                            event = EVENT_HEAL_UNCLEAN_SHUTDOWN,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_MANAGER,
                            endpoint = %endpoint,
                            error = %error,
                            "Replacement recovery discovery failed"
                        );
                        continue;
                    }
                };
                for task_id in replacement_task_ids {
                    let manager = match ResumeManager::load_replacement_intent(disk.clone(), &task_id).await {
                        Ok(manager) => manager,
                        Err(error) => {
                            if let Some(set_disk_id) = &disk_set_disk_id {
                                self.block_replacement_recovery_set(set_disk_id);
                            }
                            warn!(
                                target: "rustfs::heal::manager",
                                event = EVENT_HEAL_UNCLEAN_SHUTDOWN,
                                component = LOG_COMPONENT_HEAL,
                                subsystem = LOG_SUBSYSTEM_MANAGER,
                                endpoint = %endpoint,
                                task_id,
                                error = %error,
                                "Replacement recovery intent load failed"
                            );
                            continue;
                        }
                    };
                    let state = manager.get_state().await;
                    let active_replacement = !state.completed
                        && matches!(state.replacement_phase, ReplacementPhase::Intent | ReplacementPhase::Rebuilding);
                    let verified_replacement = state.completed
                        && matches!(state.replacement_phase, ReplacementPhase::Verified | ReplacementPhase::CleanupPending);
                    if (active_replacement || verified_replacement)
                        && state.replacement_generation.as_deref() == Some(task_id.as_str())
                        && !state.replacement_targets.is_empty()
                    {
                        if matches!(state.replacement_phase, ReplacementPhase::CleanupPending) {
                            replacement_intents.entry(task_id).or_insert((
                                state.set_disk_id,
                                state.replacement_targets,
                                state.replacement_buckets,
                                endpoint.to_string(),
                            ));
                            continue;
                        }
                        match self.storage.replacement_target_identities(&state.replacement_targets).await {
                            Ok(identities) if identities == state.replacement_target_identities => {
                                let resume_endpoint = endpoint.to_string();
                                match replacement_intents.entry(task_id) {
                                    std::collections::hash_map::Entry::Vacant(entry) => {
                                        entry.insert((
                                            state.set_disk_id,
                                            state.replacement_targets,
                                            state.replacement_buckets,
                                            resume_endpoint,
                                        ));
                                    }
                                    std::collections::hash_map::Entry::Occupied(entry) => {
                                        let (existing_set_disk_id, existing_targets, existing_buckets, existing_anchor) =
                                            entry.get();
                                        if existing_set_disk_id != &state.set_disk_id
                                            || existing_targets != &state.replacement_targets
                                            || existing_buckets != &state.replacement_buckets
                                            || existing_anchor != &resume_endpoint
                                        {
                                            conflicted_replacement_sets.insert(state.set_disk_id.clone());
                                            self.block_replacement_recovery_set(&state.set_disk_id);
                                        }
                                    }
                                }
                            }
                            Ok(_) => {
                                if manager.abandon_replacement_intent().await.is_ok() {
                                    replacement_restarts
                                        .entry(task_id)
                                        .or_insert((state.set_disk_id, state.replacement_targets));
                                }
                            }
                            Err(_) => {}
                        }
                    }
                }
            }
        }

        if !unclean && replacement_intents.is_empty() && replacement_restarts.is_empty() {
            return;
        }

        let mut recovery_by_set = HashMap::<String, Vec<(Option<String>, Vec<String>, Vec<String>, Option<String>)>>::new();
        for (task_id, (set_disk_id, heal_endpoints, buckets, resume_endpoint)) in replacement_intents {
            recovery_by_set
                .entry(set_disk_id)
                .or_default()
                .push((Some(task_id), heal_endpoints, buckets, Some(resume_endpoint)));
        }
        for (_abandoned_task_id, (set_disk_id, heal_endpoints)) in replacement_restarts {
            recovery_by_set
                .entry(set_disk_id)
                .or_default()
                .push((None, heal_endpoints, Vec::new(), None));
        }

        for (set_disk_id, mut recoveries) in recovery_by_set {
            let Ok((pool_index, set_index)) = crate::heal::utils::parse_set_disk_id(&set_disk_id) else {
                continue;
            };
            if self.replacement_recovery_set_is_blocked(&set_disk_id) {
                debug!(
                    target: "rustfs::heal::manager",
                    event = EVENT_HEAL_UNCLEAN_SHUTDOWN,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_MANAGER,
                    set_disk_id,
                    recovery_count = recoveries.len(),
                    "Replacement recovery deferred because durable recovery validation is blocked"
                );
                continue;
            }
            if conflicted_replacement_sets.contains(&set_disk_id) || recoveries.len() != 1 {
                self.block_replacement_recovery_set(&set_disk_id);
                debug!(
                    target: "rustfs::heal::manager",
                    event = EVENT_HEAL_UNCLEAN_SHUTDOWN,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_MANAGER,
                    set_disk_id,
                    recovery_count = recoveries.len(),
                    "Replacement recovery deferred because multiple durable generations exist"
                );
                continue;
            }
            let reuse_single_generation = recoveries.len() == 1 && recoveries[0].0.is_some();
            let mut heal_endpoints = recoveries
                .iter_mut()
                .flat_map(|(_, targets, _, _)| std::mem::take(targets))
                .collect::<Vec<_>>();
            heal_endpoints.sort_unstable();
            heal_endpoints.dedup();
            let buckets = if reuse_single_generation {
                std::mem::take(&mut recoveries[0].2)
            } else {
                Vec::new()
            };
            let mut req = HealRequest::new(
                HealType::ErasureSet {
                    buckets,
                    set_disk_id: set_disk_id.clone(),
                },
                HealOptions {
                    pool_index: Some(pool_index),
                    set_index: Some(set_index),
                    timeout: None,
                    ..HealOptions::default()
                },
                HealPriority::Low,
            );
            if reuse_single_generation && let Some(task_id) = recoveries[0].0.take() {
                req.id = task_id;
            }
            let recovery_anchor = reuse_single_generation.then(|| recoveries[0].3.take()).flatten();
            req.source = HealRequestSource::AutoHeal;
            req.heal_endpoints = heal_endpoints;
            let request_id = req.id.clone();
            if let Some(anchor) = &recovery_anchor {
                self.replacement_recovery_anchors
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(request_id.clone(), anchor.clone());
            }
            match self.submit_heal_request(req).await {
                Ok(HealAdmissionResult::Accepted) => {}
                Ok(_) => {
                    self.replacement_recovery_anchors
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .remove(&request_id);
                }
                Err(err) => {
                    self.replacement_recovery_anchors
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .remove(&request_id);
                    warn!(
                        target: "rustfs::heal::manager",
                        event = EVENT_HEAL_UNCLEAN_SHUTDOWN,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_MANAGER,
                        set_disk_id,
                        error = %err,
                        "Replacement recovery enqueue failed"
                    );
                }
            }
        }

        if !unclean || set_disk_ids.is_empty() {
            return;
        }

        info!(
            target: "rustfs::heal::manager",
            event = EVENT_HEAL_UNCLEAN_SHUTDOWN,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_MANAGER,
            set_count = set_disk_ids.len(),
            "Unclean shutdown detected; scheduling erasure-set heal for local sets"
        );

        let buckets = match self.storage.list_buckets().await {
            Ok(buckets) => buckets.iter().map(|b| b.name.clone()).collect::<Vec<String>>(),
            Err(err) => {
                error!(
                    target: "rustfs::heal::manager",
                    event = EVENT_HEAL_UNCLEAN_SHUTDOWN,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_MANAGER,
                    error = %err,
                    "Unclean-shutdown heal skipped: bucket listing failed"
                );
                return;
            }
        };

        for set_disk_id in set_disk_ids {
            let mut req = HealRequest::new(
                HealType::ErasureSet {
                    buckets: buckets.clone(),
                    set_disk_id: set_disk_id.clone(),
                },
                HealOptions {
                    timeout: None,
                    ..HealOptions::default()
                },
                HealPriority::Low,
            );
            req.source = HealRequestSource::AutoHeal;
            if let Err(err) = self.submit_heal_request(req).await {
                warn!(
                    target: "rustfs::heal::manager",
                    event = EVENT_HEAL_UNCLEAN_SHUTDOWN,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_MANAGER,
                    set_disk_id,
                    error = %err,
                    "Unclean-shutdown heal enqueue failed"
                );
            }
        }
    }
}
