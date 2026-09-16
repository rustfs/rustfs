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
use crate::heal::storage::ReplacementExecution;
use rustfs_utils::hash::HashAlgorithm;

const MAX_HANDOFF_LINEAGE: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplacementHandoffPhase {
    Prepared,
    MarkersOwned,
    Committed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplacementHandoffLink {
    pub transaction_id: String,
    pub predecessor: String,
    pub successor: String,
    pub set_disk_id: String,
    pub source_sha256: Vec<u8>,
    pub targets: Vec<ReplacementTargetIdentity>,
    pub expected_markers: Vec<Option<String>>,
    pub buckets: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplacementHandoff {
    pub phase: ReplacementHandoffPhase,
    pub link: ReplacementHandoffLink,
}

impl ReplacementHandoffLink {
    fn validate(&self) -> Result<()> {
        for id in [&self.transaction_id, &self.predecessor, &self.successor] {
            validate_resume_task_id(id)?;
        }
        let endpoints = self.targets.iter().map(|target| target.endpoint.clone()).collect::<Vec<_>>();
        let expected_owner = format!("{}:{}", self.set_disk_id, self.predecessor);
        if self.predecessor == self.successor
            || self.source_sha256.len() != 32
            || crate::heal::utils::parse_set_disk_id(&self.set_disk_id).is_err()
            || !replacement_targets_match_identities(&endpoints, &self.targets)
            || self.expected_markers.len() != self.targets.len()
            || self.expected_markers.iter().flatten().any(|marker| marker != &expected_owner)
        {
            return Err(replacement_recovery_conflict("invalid replacement handoff binding"));
        }
        Ok(())
    }
}

pub(super) fn validate_lineage(task_id: &str, set_disk_id: &str, lineage: &[ReplacementHandoffLink]) -> Result<()> {
    if lineage.len() > MAX_HANDOFF_LINEAGE {
        return Err(replacement_recovery_conflict("replacement handoff lineage exceeds its bound"));
    }
    let mut seen = std::collections::HashSet::new();
    let mut previous = None;
    for link in lineage {
        link.validate()?;
        if link.set_disk_id != set_disk_id
            || previous.is_some_and(|previous| previous != link.predecessor)
            || !seen.insert(link.predecessor.as_str())
        {
            return Err(replacement_recovery_conflict("replacement handoff lineage is forked or cyclic"));
        }
        previous = Some(link.successor.as_str());
    }
    if previous.is_some_and(|last| last != task_id) || seen.contains(task_id) {
        return Err(replacement_recovery_conflict("replacement handoff lineage has a different successor"));
    }
    Ok(())
}

impl ResumeManager {
    /// Shared startup/scanner decision. A changed mount starts a full scan in
    /// one durable successor; an unfinished transfer always reuses that UUID.
    pub(crate) async fn resolve_replacement_recovery(
        &self,
        storage: &dyn crate::heal::storage::HealStorageAPI,
    ) -> Result<ResumeState> {
        let attempt = self.get_state().await.retry_count.saturating_add(1);
        match self.resolve_replacement_recovery_inner(storage).await {
            Ok(state) if !state.completed && state.retry_count >= state.max_retries => {
                Err(Error::ReplacementRetryBudgetExhausted)
            }
            Ok(state) => Ok(state),
            Err(error) if matches!(&error, Error::Disk(DiskError::Io(io)) if io.kind() == std::io::ErrorKind::WouldBlock) => {
                Err(error)
            }
            // A target can be temporarily absent while its process or mount
            // restarts.  This is a readiness observation, not a failed
            // generation attempt, so do not consume the durable retry budget.
            Err(error @ Error::ReplacementTargetNotReady(_)) => Err(error),
            Err(failure) => match self.record_replacement_failure(&failure, attempt).await {
                Ok(()) => Err(failure),
                Err(persistence) => Err(Error::ReplacementFailurePersistence {
                    failure: Box::new(failure),
                    persistence: Box::new(persistence),
                }),
            },
        }
    }

    async fn resolve_replacement_recovery_inner(
        &self,
        storage: &dyn crate::heal::storage::HealStorageAPI,
    ) -> Result<ResumeState> {
        let state = self.get_state().await;
        if state.replacement_phase == ReplacementPhase::CleanupPending {
            return Ok(state);
        }
        let identities = storage.replacement_target_identities(&state.replacement_targets).await?;
        if state.replacement_phase != ReplacementPhase::HandoffPending
            && state.replacement_phase != ReplacementPhase::OwnershipPending
            && identities == state.replacement_target_identities
        {
            return Ok(state);
        }
        let execution = storage.replacement_execution(&state.replacement_targets).await?;
        if state.replacement_phase == ReplacementPhase::OwnershipPending
            && state.replacement_predecessor.is_none()
            && state.replacement_legacy_import.is_some()
        {
            if state.replacement_target_identities != execution.identities() {
                if !successor_has_no_progress(&state)
                    || CheckpointManager::has_checkpoint(&self.disk, &state.task_id).await
                    || Self::replacement_completion_proof_if_present(self.disk.clone(), &state.task_id)
                        .await?
                        .is_some()
                {
                    return Err(replacement_recovery_conflict("legacy successor has already started scanning"));
                }
                self.state.write().await.replacement_target_identities = execution.identities().to_vec();
                self.save_state_strict().await?;
            }
            return Ok(self.get_state().await);
        }
        if state.replacement_phase == ReplacementPhase::OwnershipPending {
            let predecessor = Self::load_replacement_intent(
                self.disk.clone(),
                state
                    .replacement_predecessor
                    .as_deref()
                    .ok_or_else(|| replacement_recovery_conflict("ownership-pending generation has no predecessor"))?,
            )
            .await?;
            let prior = predecessor.get_state().await;
            if prior
                .replacement_handoff
                .as_ref()
                .is_some_and(|handoff| handoff.phase == ReplacementHandoffPhase::Committed)
                && state.replacement_target_identities != execution.identities()
            {
                let buckets = storage.list_buckets().await?.into_iter().map(|bucket| bucket.name).collect();
                return Ok(self.prepare_replacement_handoff(&execution, buckets).await?.get_state().await);
            }
            return Ok(predecessor
                .prepare_replacement_handoff(&execution, Vec::new())
                .await?
                .get_state()
                .await);
        }
        let buckets = storage.list_buckets().await?.into_iter().map(|bucket| bucket.name).collect();
        Ok(self.prepare_replacement_handoff(&execution, buckets).await?.get_state().await)
    }

    /// Select a successor only after every old executor is fenced by the target
    /// execution leases. The durable predecessor is the sole handoff authority.
    pub(crate) async fn prepare_replacement_handoff(
        &self,
        execution: &ReplacementExecution,
        mut buckets: Vec<String>,
    ) -> Result<ResumeManager> {
        let state = self.get_state().await;
        if let Some(handoff) = &state.replacement_handoff {
            Self::validate_handoff(handoff, &state)?;
            let raw = Self::read_state_file(&self.disk, &state.task_id, self.state_file).await?;
            let durable: ResumeState = serde_json::from_slice(&raw).map_err(|error| Error::Serialization(error.to_string()))?;
            if durable.replacement_revision != state.replacement_revision
                || durable.replacement_handoff != state.replacement_handoff
            {
                return Err(replacement_recovery_conflict(
                    "handoff authority has not been durably published at this revision",
                ));
            }
            if handoff.phase != ReplacementHandoffPhase::Committed && handoff.link.targets != execution.identities() {
                self.rebind_pending_handoff(execution).await?;
            }
            return self.ensure_handoff_successor().await;
        }
        if state.replacement_execution_protocol != 1
            || !matches!(
                state.replacement_phase,
                ReplacementPhase::Intent
                    | ReplacementPhase::OwnershipPending
                    | ReplacementPhase::Rebuilding
                    | ReplacementPhase::Verified
            )
            || state.replacement_lineage.len() >= MAX_HANDOFF_LINEAGE
        {
            return Err(replacement_recovery_conflict(
                "replacement generation is not eligible for an online handoff",
            ));
        }
        if state.retry_count.saturating_add(1) >= state.max_retries {
            return Err(Error::ReplacementRetryBudgetExhausted);
        }
        if state.replacement_phase == ReplacementPhase::OwnershipPending {
            let predecessor_id = state
                .replacement_predecessor
                .as_deref()
                .ok_or_else(|| replacement_recovery_conflict("uncommitted ownership cannot start another handoff"))?;
            let predecessor = Self::load_replacement_intent(self.disk.clone(), predecessor_id)
                .await?
                .get_state()
                .await;
            if !predecessor.replacement_handoff.as_ref().is_some_and(|handoff| {
                handoff.phase == ReplacementHandoffPhase::Committed
                    && handoff.link.successor == state.task_id
                    && state.replacement_lineage.last() == Some(&handoff.link)
            }) {
                return Err(replacement_recovery_conflict("ownership handoff has not committed"));
            }
        }
        let endpoints = execution
            .identities()
            .iter()
            .map(|target| target.endpoint.clone())
            .collect::<Vec<_>>();
        if endpoints != state.replacement_targets {
            return Err(replacement_recovery_conflict("replacement handoff changed its target slots"));
        }
        let markers = execution.markers().await?;
        let old_marker = format!("{}:{}", state.set_disk_id, state.task_id);
        if markers.iter().flatten().any(|marker| marker != &old_marker) {
            return Err(Error::ReplacementOwnershipConflict(
                "handoff found an unknown healing marker owner".to_string(),
            ));
        }
        // Keep obligations from the old pass as well as newly created buckets.
        // A deleted bucket must be handled by the scan's incarnation checks.
        buckets.extend(state.replacement_buckets.iter().cloned());
        buckets.sort();
        buckets.dedup();
        let raw = Self::read_state_file(&self.disk, &state.task_id, self.state_file).await?;
        let observed: ResumeState = serde_json::from_slice(&raw).map_err(|error| Error::Serialization(error.to_string()))?;
        if observed.replacement_revision != state.replacement_revision {
            return Err(replacement_recovery_conflict("replacement changed before handoff preparation"));
        }
        let link = ReplacementHandoffLink {
            transaction_id: Uuid::new_v4().to_string(),
            predecessor: state.task_id.clone(),
            successor: Uuid::new_v4().to_string(),
            set_disk_id: state.set_disk_id.clone(),
            source_sha256: HashAlgorithm::SHA256.hash_encode(&raw).as_ref().to_vec(),
            targets: execution.identities().to_vec(),
            expected_markers: markers,
            buckets,
        };
        link.validate()?;
        {
            let mut current = self.state.write().await;
            if current.replacement_revision != state.replacement_revision {
                return Err(replacement_recovery_conflict("replacement changed during handoff preparation"));
            }
            current.replacement_handoff = Some(ReplacementHandoff {
                phase: ReplacementHandoffPhase::Prepared,
                link,
            });
            current.replacement_phase = ReplacementPhase::HandoffPending;
            current.completed = false;
            current.error_message = None;
        }
        self.save_state_strict().await?;
        #[cfg(test)]
        tests::fail_at(&state.task_id, "prepared")?;
        self.ensure_handoff_successor().await
    }

    pub(super) fn validate_handoff(handoff: &ReplacementHandoff, state: &ResumeState) -> Result<()> {
        handoff.link.validate()?;
        let expected_phase = if handoff.phase == ReplacementHandoffPhase::Committed {
            ReplacementPhase::Abandoned
        } else {
            ReplacementPhase::HandoffPending
        };
        if state.completed
            || state.replacement_phase != expected_phase
            || state.replacement_legacy_successor.is_some()
            || handoff.link.predecessor != state.task_id
            || handoff.link.set_disk_id != state.set_disk_id
            || handoff.link.targets.iter().map(|target| &target.endpoint).collect::<Vec<_>>()
                != state.replacement_targets.iter().collect::<Vec<_>>()
        {
            return Err(replacement_recovery_conflict("replacement handoff does not match its predecessor"));
        }
        let mut lineage = state.replacement_lineage.clone();
        lineage.push(handoff.link.clone());
        validate_lineage(&handoff.link.successor, &state.set_disk_id, &lineage)
    }

    async fn rebind_pending_handoff(&self, execution: &ReplacementExecution) -> Result<()> {
        let state = self.get_state().await;
        let handoff = state
            .replacement_handoff
            .as_ref()
            .ok_or_else(|| replacement_recovery_conflict("missing handoff"))?;
        if handoff.phase == ReplacementHandoffPhase::Committed {
            return Err(replacement_recovery_conflict("committed handoff cannot change its mount binding"));
        }
        let successor_marker = format!("{}:{}", state.set_disk_id, handoff.link.successor);
        let markers = execution.markers().await?;
        if execution
            .identities()
            .iter()
            .map(|target| &target.endpoint)
            .collect::<Vec<_>>()
            != state.replacement_targets.iter().collect::<Vec<_>>()
            || markers.len() != handoff.link.expected_markers.len()
            || markers
                .iter()
                .zip(&handoff.link.expected_markers)
                .any(|(actual, expected)| actual != expected && actual.as_deref() != Some(successor_marker.as_str()))
        {
            return Err(replacement_recovery_conflict("pending handoff target or marker changed"));
        }
        if Self::has_replacement_intent(&self.disk, &handoff.link.successor).await {
            let successor = Self::load_replacement_intent(self.disk.clone(), &handoff.link.successor).await?;
            let successor_state = successor.get_state().await;
            if !successor_can_be_rebound(&successor_state, &state.task_id)
                || CheckpointManager::has_checkpoint(&self.disk, &successor_state.task_id).await
            {
                return Err(replacement_recovery_conflict("handoff successor has already started scanning"));
            }
        }
        self.state
            .write()
            .await
            .replacement_handoff
            .as_mut()
            .ok_or_else(|| replacement_recovery_conflict("missing handoff"))?
            .link
            .targets = execution.identities().to_vec();
        self.save_state_strict().await
    }

    pub(crate) async fn ensure_handoff_successor(&self) -> Result<ResumeManager> {
        let predecessor = self.get_state().await;
        let handoff = predecessor
            .replacement_handoff
            .as_ref()
            .ok_or_else(|| replacement_recovery_conflict("missing handoff"))?;
        Self::validate_handoff(handoff, &predecessor)?;
        let link = &handoff.link;
        let mut state = ResumeState::replacement_intent(
            link.successor.clone(),
            predecessor.task_type.clone(),
            predecessor.set_disk_id.clone(),
            link.buckets.clone(),
            predecessor.replacement_targets.clone(),
            link.targets.clone(),
        );
        state.replacement_phase = ReplacementPhase::OwnershipPending;
        state.replacement_predecessor = Some(predecessor.task_id.clone());
        state.retry_count = predecessor
            .retry_count
            .checked_add(1)
            .ok_or_else(|| replacement_recovery_conflict("handoff retry overflow"))?;
        state.max_retries = predecessor.max_retries;
        state.replacement_lineage = predecessor.replacement_lineage.clone();
        state.replacement_legacy_import = predecessor.replacement_legacy_import.clone();
        state.replacement_lineage.push(link.clone());
        validate_lineage(&state.task_id, &state.set_disk_id, &state.replacement_lineage)?;
        if Self::has_replacement_intent(&self.disk, &state.task_id).await {
            let existing = Self::load_replacement_intent(self.disk.clone(), &state.task_id).await?;
            let current = existing.get_state().await;
            if current.replacement_predecessor != state.replacement_predecessor
                || current.set_disk_id != state.set_disk_id
                || current.replacement_targets != state.replacement_targets
                || current.replacement_buckets != state.replacement_buckets
            {
                return Err(replacement_recovery_conflict("reserved handoff successor conflicts with its intent"));
            }
            if current.replacement_target_identities != state.replacement_target_identities
                || current.replacement_lineage != state.replacement_lineage
            {
                if handoff.phase == ReplacementHandoffPhase::Committed
                    || !successor_can_be_rebound(&current, &predecessor.task_id)
                    || CheckpointManager::has_checkpoint(&self.disk, &current.task_id).await
                {
                    return Err(replacement_recovery_conflict("handoff successor changed after admission"));
                }
                state.replacement_revision = current.replacement_revision;
                state.retry_count = state.retry_count.max(current.retry_count);
                state.max_retries = state.max_retries.min(current.max_retries);
                state.error_message = current.error_message;
                state.start_time = current.start_time;
                *existing.state.write().await = state;
                existing.save_state_strict().await?;
            }
            return Ok(existing);
        }
        if handoff.phase == ReplacementHandoffPhase::Committed {
            return Err(replacement_recovery_conflict("committed handoff successor is missing"));
        }
        let successor = Self {
            disk: self.disk.clone(),
            state: Arc::new(RwLock::new(state)),
            throttle: Mutex::new(PersistThrottle::new()),
            persistence_lock: tokio::sync::Mutex::new(()),
            state_file: ResumeStateFile::ReplacementIntent,
        };
        successor.publish_new_replacement_intent(None).await?;
        #[cfg(test)]
        tests::fail_at(&predecessor.task_id, "successor_published")?;
        successor.ensure_replacement_intent_seal().await?;
        Ok(successor)
    }

    pub(crate) async fn acquire_replacement_markers(&self, execution: &ReplacementExecution) -> Result<()> {
        let state = self.get_state().await;
        if execution.identities() != state.replacement_target_identities {
            return Err(replacement_recovery_conflict("replacement execution has a different mount binding"));
        }
        let marker = format!("{}:{}", state.set_disk_id, state.task_id);
        let Some(predecessor_id) = &state.replacement_predecessor else {
            if let Some(approval) = &state.replacement_legacy_import {
                approval.validate_state(&state)?;
                return execution.transfer_markers(&approval.expected_markers, &marker).await;
            }
            return execution.acquire_markers(&marker).await;
        };
        let predecessor = Self::load_replacement_intent(self.disk.clone(), predecessor_id).await?;
        let prior = predecessor.get_state().await;
        let handoff = prior
            .replacement_handoff
            .as_ref()
            .ok_or_else(|| replacement_recovery_conflict("successor has no durable handoff"))?;
        Self::validate_handoff(handoff, &prior)?;
        if handoff.link.successor != state.task_id
            || handoff.link.targets != state.replacement_target_identities
            || state.replacement_lineage.last() != Some(&handoff.link)
        {
            return Err(replacement_recovery_conflict("successor does not match its handoff"));
        }
        execution.transfer_markers(&handoff.link.expected_markers, &marker).await?;
        #[cfg(test)]
        tests::fail_at(&prior.task_id, "markers_transferred")?;
        if handoff.phase != ReplacementHandoffPhase::Committed {
            {
                let mut state = predecessor.state.write().await;
                state
                    .replacement_handoff
                    .as_mut()
                    .ok_or_else(|| replacement_recovery_conflict("missing handoff"))?
                    .phase = ReplacementHandoffPhase::MarkersOwned;
            }
            predecessor.save_state_strict().await?;
            #[cfg(test)]
            tests::fail_at(&prior.task_id, "markers_owned")?;
            {
                let mut state = predecessor.state.write().await;
                state
                    .replacement_handoff
                    .as_mut()
                    .ok_or_else(|| replacement_recovery_conflict("missing handoff"))?
                    .phase = ReplacementHandoffPhase::Committed;
                state.replacement_phase = ReplacementPhase::Abandoned;
                state.error_message = None;
            }
            predecessor.save_state_strict().await?;
            #[cfg(test)]
            tests::fail_at(&prior.task_id, "committed")?;
        }
        Ok(())
    }
}

fn successor_can_be_rebound(state: &ResumeState, predecessor: &str) -> bool {
    state.replacement_predecessor.as_deref() == Some(predecessor) && successor_has_no_progress(state)
}

fn successor_has_no_progress(state: &ResumeState) -> bool {
    state.replacement_phase == ReplacementPhase::OwnershipPending
        && !state.completed
        && state.processed_objects == 0
        && state.resume_cursor.is_none()
        && state.completed_buckets.is_empty()
}

#[cfg(test)]
mod tests;
