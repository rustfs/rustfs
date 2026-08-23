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

use crate::{Error, Result};
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, warn};
use uuid::Uuid;

use super::super::{BUCKET_META_PREFIX, DiskError, DiskStore, HealDiskExt as _, RUSTFS_META_BUCKET};
use super::replacement::{ReplacementPhase, ReplacementRecoveryRecord};
use super::{
    CheckpointManager, EVENT_HEAL_RESUME_STATE, LOG_COMPONENT_HEAL, LOG_SUBSYSTEM_RESUME, REPLACEMENT_COMPLETION_PROOF_FILE,
    REPLACEMENT_INTENT_FILE, RESUME_STATE_FILE, ResumeManager, ResumeStateFile, is_replacement_intent, path_to_str,
    replacement_recovery_corruption_for_state_load, replacement_recovery_dir, validate_resume_task_id,
};

/// resume utils
pub struct ResumeUtils;

impl ResumeUtils {
    /// generate unique task id
    pub fn generate_task_id() -> String {
        Uuid::new_v4().to_string()
    }

    /// check if task can be resumed
    pub async fn can_resume_task(disk: &DiskStore, task_id: &str) -> bool {
        ResumeManager::has_resume_state(disk, task_id).await
    }

    /// get all resumable task ids
    pub async fn get_resumable_tasks(disk: &DiskStore) -> Result<Vec<String>> {
        // List all files in the buckets metadata directory
        let entries = match disk.list_dir("", RUSTFS_META_BUCKET, BUCKET_META_PREFIX, -1).await {
            Ok(entries) => entries,
            Err(e) => {
                debug!(
                    target: "rustfs::heal::resume",
                    event = EVENT_HEAL_RESUME_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_RESUME,
                    state = "list_failed",
                    error = %e,
                    "Heal resume state listing failed"
                );
                return Ok(Vec::new());
            }
        };

        let mut task_ids = Vec::new();

        // Filter files that end with ahm_resume_state.json and extract task IDs
        for entry in entries {
            if entry.ends_with(&format!("_{RESUME_STATE_FILE}")) {
                // Extract task ID from filename: {task_id}_ahm_resume_state.json
                if let Some(task_id) = entry.strip_suffix(&format!("_{RESUME_STATE_FILE}"))
                    && validate_resume_task_id(task_id).is_ok()
                    && CheckpointManager::is_resumable(disk, task_id).await?
                {
                    task_ids.push(task_id.to_string());
                }
            }
        }

        debug!(
            target: "rustfs::heal::resume",
            event = EVENT_HEAL_RESUME_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_RESUME,
            task_count = task_ids.len(),
            state = "listed",
            "Heal resume states listed"
        );
        Ok(task_ids)
    }

    /// Return replacement intent task IDs from the dedicated recovery
    /// directory. Periodic recovery must never enumerate the ordinary resume
    /// directory, whose cardinality is unrelated to replacement work.
    pub async fn get_replacement_intent_tasks(disk: &DiskStore) -> Result<Vec<String>> {
        let entries = Self::replacement_recovery_entries(disk).await?;
        let suffix = format!("_{REPLACEMENT_INTENT_FILE}");
        let mut task_ids = HashSet::new();

        for entry in entries {
            if let Some(task_id) = entry.strip_suffix(&suffix)
                && validate_resume_task_id(task_id).is_ok()
            {
                task_ids.insert(task_id.to_string());
                continue;
            }
        }

        let mut task_ids = task_ids.into_iter().collect::<Vec<_>>();
        task_ids.sort_unstable();
        Ok(task_ids)
    }

    async fn replacement_recovery_entries(disk: &DiskStore) -> Result<Vec<String>> {
        let recovery_dir = replacement_recovery_dir();
        let recovery_dir = path_to_str(&recovery_dir)?;
        match disk.list_dir("", RUSTFS_META_BUCKET, recovery_dir, -1).await {
            Ok(entries) => Ok(entries),
            Err(DiskError::FileNotFound) => Ok(Vec::new()),
            Err(error @ DiskError::UnformattedDisk) => Err(error.into()),
            Err(error) => Err(Error::TaskExecutionFailed {
                message: format!("Failed to list replacement recovery records: {error}"),
            }),
        }
    }

    /// Migrate flat replacement artifacts from earlier builds exactly once at
    /// manager startup. The normal scanner only uses the dedicated directory;
    /// ordinary resume JSON is never read on its periodic path.
    pub async fn migrate_legacy_replacement_records(disk: &DiskStore) -> Result<()> {
        let entries = disk
            .list_dir("", RUSTFS_META_BUCKET, BUCKET_META_PREFIX, -1)
            .await
            .map_err(|error| Error::TaskExecutionFailed {
                message: format!("Failed to list legacy replacement records: {error}"),
            })?;
        let ordinary_suffix = format!("_{RESUME_STATE_FILE}");
        let intent_suffix = format!("_{REPLACEMENT_INTENT_FILE}");
        let proof_suffix = format!("_{REPLACEMENT_COMPLETION_PROOF_FILE}");
        let mut ordinary_task_ids = HashSet::new();
        let mut intent_task_ids = HashSet::new();
        let mut proof_task_ids = HashSet::new();

        for entry in entries {
            if let Some(task_id) = entry.strip_suffix(&intent_suffix)
                && validate_resume_task_id(task_id).is_ok()
            {
                intent_task_ids.insert(task_id.to_string());
                continue;
            }
            if let Some(task_id) = entry.strip_suffix(&ordinary_suffix)
                && validate_resume_task_id(task_id).is_ok()
            {
                ordinary_task_ids.insert(task_id.to_string());
                continue;
            }
            if let Some(task_id) = entry.strip_suffix(&proof_suffix)
                && validate_resume_task_id(task_id).is_ok()
            {
                proof_task_ids.insert(task_id.to_string());
            }
        }

        let mut state_task_ids = intent_task_ids.into_iter().collect::<Vec<_>>();
        state_task_ids.extend(ordinary_task_ids);
        state_task_ids.sort_unstable();
        state_task_ids.dedup();
        for task_id in state_task_ids {
            let has_flat_intent = ResumeManager::has_state_file(disk, &task_id, ResumeStateFile::LegacyReplacementIntent).await;
            if !has_flat_intent {
                let manager = ResumeManager::load_from_disk(disk.clone(), &task_id).await.map_err(|error| {
                    replacement_recovery_corruption_for_state_load(
                        format!("Failed to load legacy replacement recovery candidate {task_id}"),
                        error,
                    )
                })?;
                if !is_replacement_intent(&manager.get_state().await) {
                    continue;
                }
            }
            ResumeManager::load_replacement_intent(disk.clone(), &task_id).await?;
        }

        for task_id in proof_task_ids {
            ResumeManager::migrate_legacy_replacement_completion_proof(disk, &task_id).await?;
        }
        Ok(())
    }

    /// Return all durable replacement states and completion proofs stored on
    /// one survivor disk. Unlike the legacy resumable-task helper, listing
    /// failures are returned to the caller so an observability surface cannot
    /// silently turn an unreadable durable record into a green result.
    pub async fn get_replacement_recovery_records(disk: &DiskStore) -> Result<Vec<ReplacementRecoveryRecord>> {
        let entries = Self::replacement_recovery_entries(disk).await?;
        let proof_suffix = format!("_{REPLACEMENT_COMPLETION_PROOF_FILE}");
        let mut records = Vec::new();
        let mut intent_task_ids = HashSet::new();

        for task_id in Self::get_replacement_intent_tasks(disk).await? {
            let state = ResumeManager::load_replacement_intent(disk.clone(), &task_id)
                .await?
                .get_state()
                .await;
            intent_task_ids.insert(task_id.clone());
            records.push(ReplacementRecoveryRecord::from_state(state).unwrap_or_else(|| {
                ReplacementRecoveryRecord::unknown(
                    task_id,
                    "isolated replacement intent violates its generation or target identity binding",
                )
            }));
        }

        for entry in entries {
            let Some(task_id) = entry.strip_suffix(&proof_suffix) else {
                continue;
            };
            if validate_resume_task_id(task_id).is_err() {
                continue;
            }
            if intent_task_ids.contains(task_id) {
                continue;
            }
            let proof = ResumeManager::load_replacement_completion_proof(disk.clone(), task_id).await?;
            records.push(ReplacementRecoveryRecord::from_completion_proof(&proof));
        }

        records.sort_by(|left, right| left.task_id.cmp(&right.task_id).then(left.state.cmp(&right.state)));
        Ok(records)
    }

    /// cleanup expired resume states
    pub async fn cleanup_expired_states(disk: &DiskStore, max_age_hours: u64) -> Result<()> {
        let task_ids = Self::get_resumable_tasks(disk).await?;
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        for task_id in task_ids {
            if let Ok(resume_manager) = ResumeManager::load_from_disk(disk.clone(), &task_id).await {
                let state = resume_manager.get_state().await;
                let age_hours = current_time.saturating_sub(state.last_update) / 3600;

                if !state.completed && matches!(state.replacement_phase, ReplacementPhase::Intent | ReplacementPhase::Rebuilding)
                {
                    continue;
                }
                if state.completed
                    && matches!(state.replacement_phase, ReplacementPhase::Verified | ReplacementPhase::CleanupPending)
                {
                    continue;
                }

                if age_hours > max_age_hours {
                    debug!(
                        target: "rustfs::heal::resume",
                        event = EVENT_HEAL_RESUME_STATE,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_RESUME,
                        task_id,
                        age_hours,
                        state = "expired_cleanup_started",
                        "Heal resume cleanup started"
                    );
                    if let Err(e) = resume_manager.cleanup().await {
                        warn!(
                            target: "rustfs::heal::resume",
                            event = EVENT_HEAL_RESUME_STATE,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_RESUME,
                            task_id,
                            age_hours,
                            state = "expired_cleanup_failed",
                            error = %e,
                            "Heal resume state cleanup failed"
                        );
                    }
                }
            }
        }

        for task_id in Self::get_replacement_intent_tasks(disk).await? {
            if let Ok(resume_manager) = ResumeManager::load_replacement_intent(disk.clone(), &task_id).await {
                let state = resume_manager.get_state().await;
                let age_hours = current_time.saturating_sub(state.last_update) / 3600;

                if !state.completed && matches!(state.replacement_phase, ReplacementPhase::Intent | ReplacementPhase::Rebuilding)
                {
                    continue;
                }
                if state.completed
                    && matches!(state.replacement_phase, ReplacementPhase::Verified | ReplacementPhase::CleanupPending)
                {
                    continue;
                }

                if age_hours > max_age_hours
                    && let Err(e) = resume_manager.cleanup().await
                {
                    warn!(
                        target: "rustfs::heal::resume",
                        event = EVENT_HEAL_RESUME_STATE,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_RESUME,
                        task_id,
                        age_hours,
                        state = "expired_cleanup_failed",
                        error = %e,
                        "Replacement intent cleanup failed"
                    );
                }
            }
        }

        Ok(())
    }
}
