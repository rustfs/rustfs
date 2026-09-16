// Copyright 2026 RustFS Team
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

use super::*;
use crate::heal::{
    storage::{HealStorageAPI, ReplacementExecution},
    storage_api::EcstoreConditionalFileUpdate,
};
use rustfs_utils::hash::HashAlgorithm;

pub(super) const LEGACY_APPROVAL_SUFFIX: &str = "_legacy_replacement_approval.json";
const STOPPED_WRITERS_ASSERTION: &str = "all-writers-stopped-before-upgrade";

/// Explicit maintenance authorization, never inferred from the new flock.
/// Legacy binaries do not participate in that lock protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyReplacementApproval {
    pub schema_version: u32,
    pub successor: String,
    pub set_disk_id: String,
    pub targets: Vec<String>,
    pub expected_markers: Vec<Option<String>>,
    pub sources: Vec<LegacyReplacementSource>,
    pub maintenance_assertion: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyReplacementSource {
    pub task_id: String,
    pub sha256: Vec<u8>,
}

impl LegacyReplacementApproval {
    pub(super) fn validate(&self) -> Result<()> {
        validate_resume_task_id(&self.successor)?;
        let mut targets = self.targets.clone();
        targets.sort();
        targets.dedup();
        let mut source_ids = std::collections::HashSet::new();
        if self.schema_version != 1
            || self.maintenance_assertion != STOPPED_WRITERS_ASSERTION
            || crate::heal::utils::parse_set_disk_id(&self.set_disk_id).is_err()
            || self.targets.is_empty()
            || self.targets != targets
            || self.expected_markers.len() != self.targets.len()
            || self.sources.is_empty()
            || self.sources.len() > 32
        {
            return Err(replacement_recovery_conflict("invalid legacy replacement maintenance approval"));
        }
        for source in &self.sources {
            validate_resume_task_id(&source.task_id)?;
            if source.task_id == self.successor || source.sha256.len() != 32 || !source_ids.insert(&source.task_id) {
                return Err(replacement_recovery_conflict("invalid legacy replacement source binding"));
            }
        }
        for marker in self.expected_markers.iter().flatten() {
            if !self
                .sources
                .iter()
                .any(|source| *marker == format!("{}:{}", self.set_disk_id, source.task_id))
            {
                return Err(replacement_recovery_conflict("legacy approval contains an unknown marker owner"));
            }
        }
        Ok(())
    }

    pub(super) fn validate_state(&self, state: &ResumeState) -> Result<()> {
        self.validate()?;
        let root = state
            .replacement_lineage
            .first()
            .map_or(state.task_id.as_str(), |link| link.predecessor.as_str());
        if root != self.successor || state.set_disk_id != self.set_disk_id || state.replacement_targets != self.targets {
            return Err(replacement_recovery_conflict("legacy migration receipt does not bind this generation"));
        }
        Ok(())
    }
}

impl ResumeUtils {
    /// Only startup consumes maintenance approvals. Publication is replayable:
    /// archive originals, reserve the successor, retire sources, then consume
    /// the approval. Marker transfer happens later under the successor's lease.
    pub(crate) async fn migrate_approved_legacy_replacements(disk: &DiskStore, storage: &dyn HealStorageAPI) -> Result<()> {
        for entry in Self::replacement_recovery_entries(disk).await? {
            let Some(successor_id) = entry.strip_suffix(LEGACY_APPROVAL_SUFFIX) else { continue };
            validate_resume_task_id(successor_id)?;
            let path = replacement_recovery_dir().join(&entry);
            let bytes = disk.read_all(RUSTFS_META_BUCKET, path_to_str(&path)?).await?;
            if bytes.len() > 64 * 1024 {
                return Err(replacement_recovery_conflict("legacy approval is too large"));
            }
            let approval: LegacyReplacementApproval =
                serde_json::from_slice(&bytes).map_err(|error| Error::Serialization(error.to_string()))?;
            approval.validate()?;
            if approval.successor != successor_id {
                return Err(replacement_recovery_conflict("legacy approval filename does not match its successor"));
            }
            let execution = storage.replacement_execution(&approval.targets).await?;
            let buckets = storage.list_buckets().await?.into_iter().map(|bucket| bucket.name).collect();
            Self::import_legacy_replacement(disk, &approval, &execution, buckets).await?;
            let receipt = replacement_recovery_dir().join(format!("{successor_id}_legacy_replacement_receipt.json"));
            publish_exact(disk, &receipt, None, bytes.clone()).await?;
            let result = crate::heal::storage_api::EcstoreDiskAPI::compare_and_update_file(
                disk.as_ref(),
                RUSTFS_META_BUCKET,
                path_to_str(&path)?,
                Some(bytes),
                None,
            )
            .await?;
            if !matches!(result, EcstoreConditionalFileUpdate::Updated | EcstoreConditionalFileUpdate::Missing) {
                return Err(replacement_recovery_conflict("legacy maintenance approval changed before consumption"));
            }
        }
        Ok(())
    }

    pub(super) async fn import_legacy_replacement(
        disk: &DiskStore,
        approval: &LegacyReplacementApproval,
        execution: &ReplacementExecution,
        mut buckets: Vec<String>,
    ) -> Result<ResumeManager> {
        approval.validate()?;
        if execution
            .identities()
            .iter()
            .map(|identity| &identity.endpoint)
            .collect::<Vec<_>>()
            != approval.targets.iter().collect::<Vec<_>>()
        {
            return Err(replacement_recovery_conflict("legacy migration target slots changed"));
        }
        let successor_marker = format!("{}:{}", approval.set_disk_id, approval.successor);
        let actual = execution.markers().await?;
        if actual
            .iter()
            .zip(&approval.expected_markers)
            .any(|(actual, expected)| actual != expected && actual.as_deref() != Some(successor_marker.as_str()))
        {
            return Err(replacement_recovery_conflict(
                "legacy migration marker differs from the approved snapshot",
            ));
        }
        let mut sources = Vec::new();
        for source in &approval.sources {
            let path = ResumeStateFile::ReplacementIntent.path(&source.task_id);
            let raw = disk.read_all(RUSTFS_META_BUCKET, path_to_str(&path)?).await?;
            let state: ResumeState = serde_json::from_slice(&raw).map_err(|error| Error::Serialization(error.to_string()))?;
            let archive =
                replacement_recovery_dir().join(format!("{}_{}_legacy_original.json", approval.successor, source.task_id));
            if state.schema_version == CURRENT_RESUME_SCHEMA
                && state.replacement_phase == ReplacementPhase::Abandoned
                && state.replacement_legacy_successor.as_deref() == Some(approval.successor.as_str())
            {
                let archived = disk.read_all(RUSTFS_META_BUCKET, path_to_str(&archive)?).await?;
                if HashAlgorithm::SHA256.hash_encode(&archived).as_ref() != source.sha256
                    || state.task_id != source.task_id
                    || state.set_disk_id != approval.set_disk_id
                    || state.replacement_targets != approval.targets
                {
                    return Err(replacement_recovery_conflict("legacy migration archive digest changed"));
                }
                buckets.extend(state.replacement_buckets.iter().cloned());
                continue;
            }
            if !matches!(state.schema_version, 5 | 6)
                || state.replacement_execution_protocol != 0
                || state.replacement_predecessor.is_some()
                || state.replacement_handoff.is_some()
                || !state.replacement_lineage.is_empty()
                || state.replacement_legacy_import.is_some()
                || state.replacement_legacy_successor.is_some()
                || state.task_id != source.task_id
                || state.replacement_generation.as_deref() != Some(source.task_id.as_str())
                || state.set_disk_id != approval.set_disk_id
                || state.replacement_targets != approval.targets
                || !replacement_targets_match_identities(&state.replacement_targets, &state.replacement_target_identities)
                || HashAlgorithm::SHA256.hash_encode(&raw).as_ref() != source.sha256
            {
                return Err(replacement_recovery_conflict(
                    "legacy source differs from its approved bytes or target scope",
                ));
            }
            // Archive with no replacement before changing the discoverable intent.
            publish_exact(disk, &archive, None, raw.clone()).await?;
            buckets.extend(state.replacement_buckets.iter().cloned());
            sources.push((path, raw, state));
        }
        buckets.sort();
        buckets.dedup();
        let manager = if ResumeManager::has_replacement_intent(disk, &approval.successor).await {
            let manager = ResumeManager::load_replacement_intent(disk.clone(), &approval.successor).await?;
            let state = manager.get_state().await;
            if state.replacement_legacy_import.as_ref() != Some(approval) {
                return Err(replacement_recovery_conflict("legacy successor conflicts with maintenance approval"));
            }
            if state.replacement_phase == ReplacementPhase::OwnershipPending {
                if state.processed_objects != 0
                    || state.resume_cursor.is_some()
                    || !state.completed_buckets.is_empty()
                    || CheckpointManager::has_checkpoint(disk, &state.task_id).await
                {
                    return Err(replacement_recovery_conflict("legacy successor has unexpected pre-admission progress"));
                }
                buckets.extend(state.replacement_buckets.iter().cloned());
                buckets.sort();
                buckets.dedup();
                if buckets != state.replacement_buckets {
                    let mut current = manager.state.write().await;
                    current.replacement_buckets = buckets.clone();
                    current.pending_buckets = buckets;
                    drop(current);
                    manager.save_state_strict().await?;
                }
            }
            manager
        } else {
            let mut state = ResumeState::replacement_intent(
                approval.successor.clone(),
                "erasure_set".to_string(),
                approval.set_disk_id.clone(),
                buckets,
                approval.targets.clone(),
                execution.identities().to_vec(),
            );
            state.replacement_phase = ReplacementPhase::OwnershipPending;
            state.replacement_legacy_import = Some(approval.clone());
            let manager = ResumeManager {
                disk: disk.clone(),
                state: Arc::new(RwLock::new(state)),
                throttle: Mutex::new(PersistThrottle::new()),
                persistence_lock: tokio::sync::Mutex::new(()),
                state_file: ResumeStateFile::ReplacementIntent,
            };
            manager.publish_new_replacement_intent(None).await?;
            manager.ensure_replacement_intent_seal().await?;
            manager
        };
        for (path, raw, mut state) in sources {
            state.schema_version = CURRENT_RESUME_SCHEMA;
            state.replacement_execution_protocol = 1;
            state.replacement_revision = 1;
            state.replacement_phase = ReplacementPhase::Abandoned;
            state.completed = false;
            state.error_message = None;
            state.replacement_legacy_successor = Some(approval.successor.clone());
            let retired =
                EcstoreDiskBytes::from(serde_json::to_vec(&state).map_err(|error| Error::Serialization(error.to_string()))?);
            publish_exact(disk, &path, Some(raw), retired).await?;
        }
        Ok(manager)
    }
}

/// Exactly observed bytes or an identical replay; never overwrite a different
/// archive, successor, or concurrently updated legacy record.
async fn publish_exact(disk: &DiskStore, path: &Path, expected: Option<EcstoreDiskBytes>, value: EcstoreDiskBytes) -> Result<()> {
    let path = path_to_str(path)?;
    let result = crate::heal::storage_api::EcstoreDiskAPI::compare_and_update_file(
        disk.as_ref(),
        RUSTFS_META_BUCKET,
        path,
        expected,
        Some(value.clone()),
    )
    .await?;
    if !matches!(result, EcstoreConditionalFileUpdate::Updated) && disk.read_all(RUSTFS_META_BUCKET, path).await? != value {
        return Err(replacement_recovery_conflict("legacy migration record changed during publication"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
