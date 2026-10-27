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
use crate::heal::DiskStore;
use crate::heal::resume::{
    ReplacementRecoveryBlock, abandoned_targets_are_owned_by_another_generation, replacement_generation_reserves_targets,
};
use std::collections::BTreeMap;

pub(super) fn durable_replacement_reserves_targets(state: &ResumeState) -> bool {
    replacement_generation_reserves_targets(state)
}

pub(super) fn durable_replacement_recovery_is_due(state: &ResumeState, task_id: &str) -> bool {
    state.replacement_generation.as_deref() == Some(task_id)
        && !state.replacement_targets.is_empty()
        && ((!state.completed
            && matches!(
                state.replacement_phase,
                ReplacementPhase::Intent
                    | ReplacementPhase::OwnershipPending
                    | ReplacementPhase::HandoffPending
                    | ReplacementPhase::Rebuilding
            )
            && state.retry_count < state.max_retries)
            || (state.completed
                && matches!(state.replacement_phase, ReplacementPhase::Verified | ReplacementPhase::CleanupPending)))
}

pub(super) fn durable_replacement_recovery_is_waiting_for_grace(state: &ResumeState, task_id: &str, now: u64) -> bool {
    !state.completed
        && durable_replacement_recovery_is_due(state, task_id)
        && state.dangling_delete_retry_not_before.is_some_and(|deadline| deadline > now)
}

pub(super) fn replacement_discovery_error_is_expected_for_deferred_endpoint(
    error: &Error,
    endpoint: &str,
    deferred_replacement_endpoints: &HashSet<String>,
) -> bool {
    matches!(error, Error::Disk(DiskError::UnformattedDisk)) && deferred_replacement_endpoints.contains(endpoint)
}

pub(super) fn replacement_targets_belong_to_local_node(
    replacement_targets: &[String],
    local_endpoints: &HashSet<String>,
) -> bool {
    !replacement_targets.is_empty() && replacement_targets.iter().all(|target| local_endpoints.contains(target))
}

/// Node-local registry of blocked replacement generations, shared by startup
/// discovery and the periodic scanner. Entries are keyed by generation, carry
/// the reason and observation times, and are re-evaluated on every discovery
/// pass; a block never widens to the generation's set.
#[derive(Clone, Default)]
pub(super) struct ReplacementRecoveryBlocks {
    entries: Arc<std::sync::Mutex<BTreeMap<String, ReplacementRecoveryBlock>>>,
}

impl ReplacementRecoveryBlocks {
    fn entries(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, ReplacementRecoveryBlock>> {
        self.entries.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Record or refresh a block. The first observation time is kept; the
    /// reason and reservation flag follow the latest pass.
    pub(super) fn block(&self, task_id: &str, set_disk_id: &str, reason: impl Into<String>, reserves_targets: bool) {
        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let reason = reason.into();
        let mut entries = self.entries();
        match entries.get_mut(task_id) {
            Some(entry) => {
                entry.set_disk_id = set_disk_id.to_string();
                entry.reason = reason;
                entry.reserves_targets = reserves_targets;
                entry.last_observed = now;
            }
            None => {
                entries.insert(
                    task_id.to_string(),
                    ReplacementRecoveryBlock {
                        task_id: task_id.to_string(),
                        set_disk_id: set_disk_id.to_string(),
                        reason,
                        reserves_targets,
                        blocked_since: now,
                        last_observed: now,
                    },
                );
            }
        }
    }

    /// Clear a block after the generation merged and resolved cleanly.
    pub(super) fn unblock(&self, task_id: &str) -> bool {
        self.entries().remove(task_id).is_some()
    }

    #[cfg(test)]
    pub(super) fn is_blocked(&self, task_id: &str) -> bool {
        self.entries().contains_key(task_id)
    }

    /// Fresh admission for a set waits while one of its blocked generations
    /// still reserves target slots; a retired or released block does not.
    pub(super) fn set_has_reserving_block(&self, set_disk_id: &str) -> bool {
        self.entries()
            .values()
            .any(|entry| entry.set_disk_id == set_disk_id && entry.reserves_targets)
    }

    pub(super) fn snapshot(&self) -> Vec<ReplacementRecoveryBlock> {
        self.entries().values().cloned().collect()
    }
}

/// Whether a durable generation still reserves its target slots against this
/// node's disks. An orphaned `Abandoned` record releases them once every slot's
/// healing marker names another generation of the same set.
pub(super) async fn replacement_reserves_targets_on(state: &ResumeState, disks: &[DiskStore]) -> bool {
    durable_replacement_reserves_targets(state) && !abandoned_targets_are_owned_by_another_generation(state, disks).await
}

impl HealManager {
    /// Detect whether the previous run ended without a clean shutdown and, if so,
    /// enqueue a full erasure-set heal for every local set. Also (re)writes the
    /// marker for the current run; [`super::super::clear_unclean_shutdown_markers`]
    /// removes it again during graceful shutdown. Best-effort: failures only log.
    pub(super) async fn process_unclean_shutdown(&self) {
        let mut unclean = false;
        let mut set_disk_ids = HashSet::new();
        let mut reserved_replacement_sets = HashSet::new();
        let mut replacement_intents = HashMap::<String, (String, Vec<String>, Vec<String>, String)>::new();
        let mut replacement_recovery_candidates = HashMap::<String, ReplacementRecoveryCandidate>::new();
        let mut conflicted_replacement_tasks = HashSet::new();
        // Sets whose records could not be inventoried in this pass. The skip is
        // pass-local: the next discovery pass reads them again.
        let mut conflicted_replacement_sets = HashSet::new();
        let blocks = self.replacement_recovery_blocks.clone();

        {
            let local_disks = {
                let local_disk_map = local_disk_map_read().await;
                local_disk_map.values().flatten().cloned().collect::<Vec<_>>()
            };
            let local_endpoints = local_disks
                .iter()
                .map(|disk| disk.endpoint().to_string())
                .collect::<HashSet<_>>();
            let mut recovery_disks = Vec::new();
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

                if !matches!(disk.get_disk_id().await, Ok(Some(id)) if !id.is_nil()) {
                    continue;
                }
                recovery_disks.push(disk.clone());

                // Legacy flat records are inspected only while starting. The
                // periodic scanner lists the dedicated replacement directory.
                let migration = match ResumeUtils::migrate_approved_legacy_replacements(disk, self.storage.as_ref()).await {
                    Ok(()) => ResumeUtils::migrate_legacy_replacement_records(disk).await,
                    Err(error) => Err(error),
                };
                if let Err(error) = migration {
                    if let Some(set_disk_id) = &disk_set_disk_id {
                        conflicted_replacement_sets.insert(set_disk_id.clone());
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
            }

            for set_disk_id in &set_disk_ids {
                match self.storage.replacement_intent_disks(set_disk_id).await {
                    Ok(disks) => recovery_disks.extend(disks),
                    Err(error) => {
                        conflicted_replacement_sets.insert(set_disk_id.clone());
                        warn!(
                            target: "rustfs::heal::manager",
                            event = EVENT_HEAL_UNCLEAN_SHUTDOWN,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_MANAGER,
                            set_disk_id,
                            error = %error,
                            "Replacement recovery disk discovery failed"
                        );
                    }
                }
            }
            recovery_disks.sort_by_key(|disk| disk.endpoint().to_string());
            recovery_disks.dedup_by(|left, right| left.endpoint().to_string() == right.endpoint().to_string());

            for disk in &recovery_disks {
                let endpoint = disk.endpoint();
                let disk_set_disk_id = crate::heal::utils::format_set_disk_id_from_i32(endpoint.pool_idx, endpoint.set_idx);
                let replacement_task_ids = match ResumeUtils::get_replacement_intent_tasks(disk).await {
                    Ok(task_ids) => task_ids,
                    Err(error) => {
                        if let Some(set_disk_id) = &disk_set_disk_id {
                            conflicted_replacement_sets.insert(set_disk_id.clone());
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
                            conflicted_replacement_tasks.insert(task_id.clone());
                            blocks.block(
                                &task_id,
                                disk_set_disk_id.as_deref().unwrap_or_default(),
                                format!("durable intent could not be loaded: {error}"),
                                true,
                            );
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
                    let reserves_targets = replacement_reserves_targets_on(&state, &local_disks).await;
                    if disk_set_disk_id
                        .as_deref()
                        .is_some_and(|disk_set| disk_set != state.set_disk_id)
                    {
                        let disk_set = disk_set_disk_id.as_deref().unwrap_or_default();
                        conflicted_replacement_tasks.insert(task_id.clone());
                        blocks.block(
                            &task_id,
                            &state.set_disk_id,
                            format!("durable intent binds to {} but was found on a disk of {disk_set}", state.set_disk_id),
                            reserves_targets,
                        );
                        warn!(
                            target: "rustfs::heal::manager",
                            event = EVENT_HEAL_UNCLEAN_SHUTDOWN,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_MANAGER,
                            endpoint = %endpoint,
                            task_id,
                            state_set_disk_id = %state.set_disk_id,
                            observed_set_disk_id = disk_set,
                            "Replacement recovery intent set binding is ambiguous"
                        );
                        continue;
                    }
                    if reserves_targets {
                        reserved_replacement_sets.insert(state.set_disk_id.clone());
                    }
                    if !replacement_targets_belong_to_local_node(&state.replacement_targets, &local_endpoints) {
                        continue;
                    }
                    let active_replacement = !state.completed
                        && matches!(
                            state.replacement_phase,
                            ReplacementPhase::Intent
                                | ReplacementPhase::OwnershipPending
                                | ReplacementPhase::HandoffPending
                                | ReplacementPhase::Rebuilding
                        )
                        && (state.retry_count < state.max_retries || replacement_retry_is_exhausted_active(&state));
                    let verified_replacement = state.completed
                        && matches!(state.replacement_phase, ReplacementPhase::Verified | ReplacementPhase::CleanupPending);
                    if !active_replacement && !verified_replacement {
                        if reserves_targets {
                            // Exhausted or orphaned: the record keeps its
                            // slots reserved until an operator repairs it, but
                            // only this generation is held back.
                            blocks.block(
                                &task_id,
                                &state.set_disk_id,
                                state
                                    .error_message
                                    .clone()
                                    .unwrap_or_else(|| "durable generation reserves its targets but cannot run".to_string()),
                                true,
                            );
                        }
                        continue;
                    }
                    if state.replacement_generation.as_deref() == Some(task_id.as_str()) && !state.replacement_targets.is_empty()
                    {
                        if conflicted_replacement_tasks.contains(&task_id) {
                            continue;
                        }
                        let mut selected = replacement_recovery_candidates.remove(&task_id);
                        let candidate =
                            match ReplacementRecoveryCandidate::new(state.clone(), endpoint.to_string(), endpoint.is_local) {
                                Ok(candidate) => candidate,
                                Err(error) => {
                                    conflicted_replacement_tasks.insert(task_id.clone());
                                    blocks.block(&task_id, &state.set_disk_id, error.to_string(), reserves_targets);
                                    warn!(
                                        target: "rustfs::heal::manager",
                                        event = EVENT_HEAL_UNCLEAN_SHUTDOWN,
                                        component = LOG_COMPONENT_HEAL,
                                        subsystem = LOG_SUBSYSTEM_MANAGER,
                                        endpoint = %endpoint,
                                        task_id,
                                        error = %error,
                                        "Replacement recovery generation binding is ambiguous"
                                    );
                                    continue;
                                }
                            };
                        if let Err(error) = merge_replacement_recovery_candidate(&mut selected, candidate) {
                            conflicted_replacement_tasks.insert(task_id.clone());
                            blocks.block(&task_id, &state.set_disk_id, error.to_string(), reserves_targets);
                            warn!(
                                target: "rustfs::heal::manager",
                                event = EVENT_HEAL_UNCLEAN_SHUTDOWN,
                                component = LOG_COMPONENT_HEAL,
                                subsystem = LOG_SUBSYSTEM_MANAGER,
                                endpoint = %endpoint,
                                task_id,
                                error = %error,
                                "Replacement recovery generation merge failed"
                            );
                        } else if let Some(selected) = selected {
                            replacement_recovery_candidates.insert(task_id, selected);
                        }
                    }
                }
            }

            // Replay only the canonical durable copy.  Replica locations are
            // observations of one generation and must never each advance it.
            for (task_id, candidate) in replacement_recovery_candidates {
                // Every copy merged cleanly: an earlier block for this
                // generation no longer applies. Later failures re-block it.
                blocks.unblock(&task_id);
                let candidate_set = candidate.state.set_disk_id.clone();
                let reserves_targets = replacement_reserves_targets_on(&candidate.state, &local_disks).await;
                let Some(anchor_disk) = recovery_disks
                    .iter()
                    .find(|disk| disk.endpoint().to_string() == candidate.anchor)
                    .cloned()
                else {
                    blocks.block(&task_id, &candidate_set, "durable anchor is unavailable", reserves_targets);
                    continue;
                };
                let manager = match ResumeManager::load_replacement_intent(anchor_disk, &task_id).await {
                    Ok(manager) => manager,
                    Err(error) => {
                        blocks.block(&task_id, &candidate_set, error.to_string(), reserves_targets);
                        warn!(
                            target: "rustfs::heal::manager",
                            event = EVENT_HEAL_UNCLEAN_SHUTDOWN,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_MANAGER,
                            task_id,
                            error = %error,
                            "Canonical replacement recovery intent load failed"
                        );
                        continue;
                    }
                };
                let state = manager.get_state().await;
                let now = SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if durable_replacement_recovery_is_waiting_for_grace(&state, &task_id, now) {
                    // Discovery already reserved this set. Keep the original
                    // generation and its markers until the storage deadline.
                    continue;
                }
                if replacement_retry_is_exhausted_active(&state) {
                    match manager.rearm_replacement_recovery_if_needed(self.storage.as_ref()).await {
                        // Resolution below reloads the re-armed state.
                        Ok(true) => {}
                        Ok(false) => {
                            // A ready target with an exhausted all-skip pass
                            // is terminal for this generation; preserve its
                            // reservation and do not enqueue a fresh task.
                            blocks.block(
                                &task_id,
                                &candidate_set,
                                state
                                    .error_message
                                    .clone()
                                    .unwrap_or_else(|| "replacement retry budget exhausted".to_string()),
                                reserves_targets,
                            );
                            continue;
                        }
                        Err(error) => {
                            blocks.block(&task_id, &candidate_set, error.to_string(), reserves_targets);
                            warn!(
                                target: "rustfs::heal::manager",
                                event = EVENT_HEAL_UNCLEAN_SHUTDOWN,
                                component = LOG_COMPONENT_HEAL,
                                subsystem = LOG_SUBSYSTEM_MANAGER,
                                task_id,
                                error = %error,
                                "Replacement recovery readiness probe failed"
                            );
                            continue;
                        }
                    }
                }
                let state = match manager.resolve_replacement_recovery(self.storage.as_ref()).await {
                    Ok(state) => state,
                    Err(Error::ReplacementTargetNotReady(_)) => continue,
                    Err(error) => {
                        blocks.block(&task_id, &candidate_set, error.to_string(), reserves_targets);
                        warn!(
                            target: "rustfs::heal::manager",
                            event = EVENT_HEAL_UNCLEAN_SHUTDOWN,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_MANAGER,
                            task_id,
                            error = %error,
                            "Canonical replacement recovery resolution failed"
                        );
                        continue;
                    }
                };
                blocks.unblock(&state.task_id);
                let resolved = (state.set_disk_id, state.replacement_targets, state.replacement_buckets, candidate.anchor);
                match replacement_intents.entry(state.task_id.clone()) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(resolved);
                    }
                    std::collections::hash_map::Entry::Occupied(entry) => {
                        let existing = entry.get();
                        if existing.0 != resolved.0 || existing.1 != resolved.1 || existing.2 != resolved.2 {
                            blocks.block(
                                &state.task_id,
                                &resolved.0,
                                "durable copies resolve to different set, targets, or bucket plans",
                                true,
                            );
                            entry.remove();
                        }
                    }
                }
            }
        }

        if !unclean && replacement_intents.is_empty() {
            return;
        }

        let mut recovery_by_set = HashMap::<String, Vec<(String, Vec<String>, Vec<String>, String)>>::new();
        for (task_id, (set_disk_id, heal_endpoints, buckets, resume_endpoint)) in replacement_intents {
            recovery_by_set
                .entry(set_disk_id)
                .or_default()
                .push((task_id, heal_endpoints, buckets, resume_endpoint));
        }

        for (set_disk_id, mut recoveries) in recovery_by_set {
            let Ok((pool_index, set_index)) = crate::heal::utils::parse_set_disk_id(&set_disk_id) else {
                continue;
            };
            if conflicted_replacement_sets.contains(&set_disk_id) {
                debug!(
                    target: "rustfs::heal::manager",
                    event = EVENT_HEAL_UNCLEAN_SHUTDOWN,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_MANAGER,
                    set_disk_id,
                    recovery_count = recoveries.len(),
                    "Replacement recovery deferred because durable recovery inventory is incomplete"
                );
                continue;
            }
            if recoveries.len() != 1 {
                // Two active generations resolved for one set. Neither may
                // run until an operator reconciles them; each is blocked on
                // its own so other sets keep recovering.
                for (task_id, _, _, _) in &recoveries {
                    blocks.block(task_id, &set_disk_id, "multiple active generations resolved for the same set", true);
                }
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
            let Some((task_id, heal_endpoints, buckets, recovery_anchor)) = recoveries.pop() else {
                continue;
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
            req.id = task_id;
            req.source = HealRequestSource::AutoHeal;
            req.heal_endpoints = heal_endpoints;
            let request_id = req.id.clone();
            self.replacement_recovery_anchors
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(request_id.clone(), recovery_anchor);
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
            if reserved_replacement_sets.contains(&set_disk_id)
                || conflicted_replacement_sets.contains(&set_disk_id)
                || blocks.set_has_reserving_block(&set_disk_id)
            {
                continue;
            }
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

#[cfg(test)]
mod tests {
    use super::replacement_targets_belong_to_local_node;
    use std::collections::HashSet;

    #[test]
    fn replacement_replay_requires_every_target_to_be_local() {
        let local = HashSet::from(["http://node-b:9000/disk".to_string()]);

        assert!(replacement_targets_belong_to_local_node(&["http://node-b:9000/disk".to_string()], &local));
        assert!(!replacement_targets_belong_to_local_node(&[], &local));
        assert!(!replacement_targets_belong_to_local_node(
            &["http://node-a:9000/disk".to_string()],
            &local
        ));
        assert!(!replacement_targets_belong_to_local_node(
            &["http://node-b:9000/disk".to_string(), "http://node-c:9000/disk".to_string(),],
            &local
        ));
    }
}
