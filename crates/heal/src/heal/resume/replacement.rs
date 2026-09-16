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
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use super::super::HealDiskExt as _;

use super::super::storage_api::owner::{EcstoreConditionalFileUpdate, EcstoreDiskBytes};
use super::{
    DiskError, DiskStore, RUSTFS_META_BUCKET, ResumeManager, ResumeState, delete_resume_file, ensure_replacement_recovery_dir,
    injected_replacement_proof_write_error, is_replacement_intent, legacy_replacement_completion_proof_path, path_to_str,
    replacement_completion_proof_path, replacement_intent_seal_path, replacement_recovery_conflict,
    replacement_recovery_corruption, validate_resume_task_id,
};

/// Durable-proof schema version.
const CURRENT_REPLACEMENT_COMPLETION_PROOF_SCHEMA: u32 = 2;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplacementPhase {
    #[default]
    None,
    Intent,
    OwnershipPending,
    HandoffPending,
    Rebuilding,
    Verified,
    CleanupPending,
    Abandoned,
}

/// Target-specific state for a durable automatic replacement generation.
///
/// This is deliberately separate from the legacy background-heal status
/// contract. Consumers must treat [`Self::Unknown`] as non-definitive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplacementRecoveryState {
    WaitingForReplacement,
    Running,
    Incomplete,
    Unrecoverable,
    CleanupPending,
    Completed,
    Unknown,
}

/// Read-only status derived from one durable replacement generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplacementRecoveryRecord {
    pub task_id: String,
    pub state: ReplacementRecoveryState,
    pub generation: Option<String>,
    pub set_disk_id: Option<String>,
    pub target_slots: Vec<String>,
    pub reason: Option<String>,
    pub verified_at: Option<u64>,
}

impl ReplacementRecoveryRecord {
    pub(super) fn from_state(state: ResumeState) -> Option<Self> {
        if !is_replacement_intent(&state) {
            return None;
        }

        let invariant_holds = state.replacement_generation.as_deref() == Some(state.task_id.as_str())
            && replacement_targets_match_identities(&state.replacement_targets, &state.replacement_target_identities);
        if !invariant_holds {
            return Some(Self::unknown(
                state.task_id,
                "durable replacement state violates its generation or target identity binding",
            ));
        }

        let (state_kind, reason) = if !state.completed && state.retry_count >= state.max_retries {
            (
                ReplacementRecoveryState::Unrecoverable,
                Some(
                    state
                        .error_message
                        .clone()
                        .unwrap_or_else(|| "replacement retry budget exhausted".to_string()),
                ),
            )
        } else if let Some(reason) = state.error_message.clone() {
            (ReplacementRecoveryState::Incomplete, Some(reason))
        } else {
            match state.replacement_phase {
                ReplacementPhase::Intent | ReplacementPhase::OwnershipPending | ReplacementPhase::HandoffPending => {
                    (ReplacementRecoveryState::WaitingForReplacement, None)
                }
                ReplacementPhase::Rebuilding => (ReplacementRecoveryState::Running, None),
                ReplacementPhase::Verified | ReplacementPhase::CleanupPending => (ReplacementRecoveryState::CleanupPending, None),
                ReplacementPhase::Abandoned => (
                    ReplacementRecoveryState::Unrecoverable,
                    Some(state.replacement_handoff.as_ref().map_or_else(
                        || {
                            state.replacement_legacy_successor.as_ref().map_or_else(
                                || "replacement generation was abandoned".to_string(),
                                |successor| {
                                    format!("replacement responsibility transferred by approved legacy migration to {successor}")
                                },
                            )
                        },
                        |handoff| format!("replacement responsibility transferred to {}", handoff.link.successor),
                    )),
                ),
                ReplacementPhase::None => (ReplacementRecoveryState::Unknown, Some("replacement phase is missing".to_string())),
            }
        };

        Some(Self {
            task_id: state.task_id,
            state: state_kind,
            generation: state.replacement_generation,
            set_disk_id: Some(state.set_disk_id),
            target_slots: state.replacement_targets,
            reason,
            verified_at: None,
        })
    }

    pub(super) fn from_completion_proof(proof: &ReplacementCompletionProof) -> Self {
        Self {
            task_id: proof.task_id.clone(),
            state: ReplacementRecoveryState::Completed,
            generation: Some(proof.replacement_generation.clone()),
            set_disk_id: Some(proof.set_disk_id.clone()),
            target_slots: proof.replacement_targets.clone(),
            reason: None,
            verified_at: Some(proof.verified_at),
        }
    }

    pub(super) fn unknown(task_id: String, reason: &str) -> Self {
        Self {
            task_id,
            state: ReplacementRecoveryState::Unknown,
            generation: None,
            set_disk_id: None,
            target_slots: Vec::new(),
            reason: Some(reason.to_string()),
            verified_at: None,
        }
    }
}

pub(super) fn replacement_targets_match_identities(targets: &[String], identities: &[ReplacementTargetIdentity]) -> bool {
    !targets.is_empty()
        && targets.len() == identities.len()
        && targets.iter().collect::<HashSet<_>>().len() == targets.len()
        && identities.iter().map(|identity| &identity.endpoint).eq(targets.iter())
}

/// A durable intent can be visible through more than one survivor after a
/// restart.  The endpoint that exposes the record is a replica location, not
/// a generation identity, so discovery must merge those observations before it
/// decides whether a set is conflicted.
#[derive(Debug, Clone)]
pub(crate) struct ReplacementRecoveryCandidate {
    pub(crate) state: ResumeState,
    pub(crate) anchor: String,
    pub(crate) anchor_is_local: bool,
}

/// An exhausted replacement data pass is eligible for a readiness probe.
/// Handoff-pending generations stay on their durable handoff protocol; an
/// ordinary rebuild may re-arm only for a currently unavailable target or the
/// one-shot legacy transient-skip compatibility marker.
pub(crate) fn replacement_retry_is_exhausted_active(state: &ResumeState) -> bool {
    !state.completed
        && state.retry_count >= state.max_retries
        && replacement_targets_match_identities(&state.replacement_targets, &state.replacement_target_identities)
        && matches!(
            state.replacement_phase,
            ReplacementPhase::Intent | ReplacementPhase::OwnershipPending | ReplacementPhase::Rebuilding
        )
}

/// Releases before readiness-aware retry persistence encoded a target restart
/// as an ordinary transient-skip retry.  Such a state is recoverable once
/// after the same generation is rediscovered, but must not reopen an already
/// bounded generation on every scanner tick.
pub(crate) fn replacement_retry_is_legacy_transient_skip(state: &ResumeState) -> bool {
    !state.replacement_legacy_retry_compatibility_done
        && state.error_message.as_deref().is_some_and(|message| {
            message.starts_with("Transient heal skip:")
                && message.contains("Replacement erasure set heal incomplete")
                && message.contains("retry scheduled")
                && !message.contains("target readiness deferred retry")
        })
}

impl ReplacementRecoveryCandidate {
    pub(crate) fn new(state: ResumeState, anchor: impl Into<String>, anchor_is_local: bool) -> Result<Self> {
        let candidate = Self {
            state,
            anchor: anchor.into(),
            anchor_is_local,
        };
        candidate.validate()?;
        Ok(candidate)
    }

    fn validate(&self) -> Result<()> {
        if !is_replacement_intent(&self.state)
            || self.state.replacement_generation.as_deref() != Some(self.state.task_id.as_str())
            || crate::heal::utils::parse_set_disk_id(&self.state.set_disk_id).is_err()
            || !replacement_targets_match_identities(&self.state.replacement_targets, &self.state.replacement_target_identities)
        {
            return Err(Error::ReplacementGenerationConflict {
                task_id: self.state.task_id.clone(),
                reason: "durable intent does not bind one generation to its target identities".to_string(),
            });
        }
        Ok(())
    }
}

/// Merge one durable observation into the generation selected for a set.
///
/// `replacement_revision` is a per-generation CAS fence.  A higher revision
/// wins; equal revisions must describe the exact same state, otherwise the
/// on-disk copies cannot be ordered safely and the caller must surface a typed
/// conflict.  Local anchors are preferred only when the durable revisions and
/// state are identical, followed by endpoint order for deterministic replay.
pub(crate) fn merge_replacement_recovery_candidate(
    selected: &mut Option<ReplacementRecoveryCandidate>,
    candidate: ReplacementRecoveryCandidate,
) -> Result<()> {
    candidate.validate()?;
    let Some(current) = selected.as_ref() else {
        *selected = Some(candidate);
        return Ok(());
    };

    if !same_replacement_generation_binding(&current.state, &candidate.state) {
        return Err(Error::ReplacementGenerationConflict {
            task_id: candidate.state.task_id,
            reason: "durable copies disagree on set, target slot, target incarnation, or lineage".to_string(),
        });
    }

    let candidate_revision = candidate.state.replacement_revision;
    let current_revision = current.state.replacement_revision;
    if candidate_revision == current_revision && candidate.state != current.state {
        return Err(Error::ReplacementGenerationConflict {
            task_id: candidate.state.task_id,
            reason: format!("durable copies diverge at replacement revision {candidate_revision}"),
        });
    }

    let candidate_wins = candidate_revision > current_revision
        || (candidate_revision == current_revision
            && (candidate.anchor_is_local, std::cmp::Reverse(candidate.anchor.as_str()))
                > (current.anchor_is_local, std::cmp::Reverse(current.anchor.as_str())));
    if candidate_wins {
        *selected = Some(candidate);
    }
    Ok(())
}

/// Fields that identify a generation independently of its mutable progress.
/// Handoff phase and counters intentionally remain mutable and are ordered by
/// the revision fence above.
fn same_replacement_generation_binding(left: &ResumeState, right: &ResumeState) -> bool {
    left.task_id == right.task_id
        && left.task_type == right.task_type
        && left.schema_version == right.schema_version
        && left.set_disk_id == right.set_disk_id
        && left.replacement_targets == right.replacement_targets
        && left.replacement_target_identities == right.replacement_target_identities
        && left.replacement_buckets == right.replacement_buckets
        && left.replacement_generation == right.replacement_generation
        && left.replacement_execution_protocol == right.replacement_execution_protocol
        && left.replacement_predecessor == right.replacement_predecessor
        && left.replacement_lineage == right.replacement_lineage
        && left.replacement_legacy_import == right.replacement_legacy_import
        && left.max_retries == right.max_retries
}

/// Stable evidence for the mounted replacement instance that owns a repair
/// generation. Endpoint text alone is not sufficient because a later disk can
/// be mounted at the same configured path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplacementTargetIdentity {
    pub endpoint: String,
    pub canonical_path: String,
    pub physical_device_ids: Vec<String>,
    pub filesystem_identity: String,
}

/// Durable terminal evidence for one automatic replacement generation. This
/// lives on the healthy non-target anchor rather than in the resumable state,
/// because resume cleanup must not erase proof that the generation completed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ReplacementCompletionProof {
    pub schema_version: u32,
    pub task_id: String,
    pub replacement_generation: String,
    pub set_disk_id: String,
    pub replacement_targets: Vec<String>,
    pub replacement_target_identities: Vec<ReplacementTargetIdentity>,
    #[serde(default)]
    pub replacement_lineage: Vec<super::ReplacementHandoffLink>,
    #[serde(default)]
    pub replacement_legacy_import: Option<super::LegacyReplacementApproval>,
    pub verified_at: u64,
}

impl ReplacementCompletionProof {
    pub(super) fn from_state(state: &ResumeState, verified_at: u64) -> Result<Self> {
        let replacement_generation = state
            .replacement_generation
            .clone()
            .ok_or_else(|| Error::TaskExecutionFailed {
                message: format!("Replacement completion has no generation for task {}", state.task_id),
            })?;
        if replacement_generation != state.task_id
            || state.replacement_targets.is_empty()
            || state
                .replacement_target_identities
                .iter()
                .map(|identity| &identity.endpoint)
                .collect::<Vec<_>>()
                != state.replacement_targets.iter().collect::<Vec<_>>()
        {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement completion identity does not match task {}", state.task_id),
            });
        }

        Ok(Self {
            schema_version: CURRENT_REPLACEMENT_COMPLETION_PROOF_SCHEMA,
            task_id: state.task_id.clone(),
            replacement_generation,
            set_disk_id: state.set_disk_id.clone(),
            replacement_targets: state.replacement_targets.clone(),
            replacement_target_identities: state.replacement_target_identities.clone(),
            replacement_lineage: state.replacement_lineage.clone(),
            replacement_legacy_import: state.replacement_legacy_import.clone(),
            verified_at,
        })
    }

    fn matches_state(&self, state: &ResumeState) -> bool {
        (self.schema_version == CURRENT_REPLACEMENT_COMPLETION_PROOF_SCHEMA
            || (self.schema_version == 1 && state.replacement_lineage.is_empty()))
            && self.task_id == state.task_id
            && state.replacement_generation.as_deref() == Some(self.replacement_generation.as_str())
            && self.set_disk_id == state.set_disk_id
            && self.replacement_targets == state.replacement_targets
            && self.replacement_target_identities == state.replacement_target_identities
            && self.replacement_lineage == state.replacement_lineage
            && self.replacement_legacy_import == state.replacement_legacy_import
    }

    fn validate(&self, expected_task_id: &str) -> Result<()> {
        if self.schema_version != CURRENT_REPLACEMENT_COMPLETION_PROOF_SCHEMA
            && !(self.schema_version == 1 && self.replacement_lineage.is_empty())
        {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement completion proof schema {} is unsupported", self.schema_version),
            });
        }
        validate_resume_task_id(expected_task_id)?;
        super::handoff::validate_lineage(expected_task_id, &self.set_disk_id, &self.replacement_lineage)?;
        if self
            .replacement_lineage
            .last()
            .is_some_and(|link| link.targets != self.replacement_target_identities)
        {
            return Err(replacement_recovery_conflict("completion proof lineage has a different target binding"));
        }
        if let Some(approval) = &self.replacement_legacy_import {
            approval.validate()?;
            let root = self
                .replacement_lineage
                .first()
                .map_or(self.task_id.as_str(), |link| link.predecessor.as_str());
            if self.schema_version < 2
                || root != approval.successor
                || self.set_disk_id != approval.set_disk_id
                || self.replacement_targets != approval.targets
            {
                return Err(replacement_recovery_conflict("completion proof has an invalid legacy migration receipt"));
            }
        }
        if self.task_id != expected_task_id
            || self.replacement_generation != self.task_id
            || self.set_disk_id.is_empty()
            || self.verified_at == 0
            || !replacement_targets_match_identities(&self.replacement_targets, &self.replacement_target_identities)
        {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement completion proof does not match task {expected_task_id}"),
            });
        }
        Ok(())
    }
}

pub(crate) fn replacement_target_identities_match(
    expected: &[ReplacementTargetIdentity],
    actual: &[ReplacementTargetIdentity],
) -> bool {
    let mut expected = expected.to_vec();
    let mut actual = actual.to_vec();
    expected.sort_by(|left, right| left.endpoint.cmp(&right.endpoint));
    actual.sort_by(|left, right| left.endpoint.cmp(&right.endpoint));
    expected == actual
}

/// Build the canonical, provably-injective dedup identity for an object
/// version. Length-prefixing the object key makes the encoding injective: no
/// two distinct `(object, version_id)` pairs can collide, even for adversarial
/// keys containing `:` or embedded null bytes. This is the single source of
/// truth for per-version dedup across the heal loop and the checkpoint sets.
pub fn compose_key(object: &str, version_id: Option<&str>) -> String {
    format!("{}:{}{}", object.len(), object, version_id.unwrap_or(""))
}

impl ResumeManager {
    /// Seal a durably published intent before the caller may format a target.
    /// A torn intent without this seal is known to have failed before its
    /// creator returned and can be atomically recreated on retry.
    pub(super) async fn ensure_replacement_intent_seal(&self) -> Result<()> {
        let task_id = self.state.read().await.task_id.clone();
        validate_resume_task_id(&task_id)?;
        let path = replacement_intent_seal_path(&task_id);
        let path = path_to_str(&path)?;
        match self.disk.read_all(RUSTFS_META_BUCKET, path).await {
            Ok(_) => return Ok(()),
            Err(DiskError::FileNotFound) => {}
            Err(error) => {
                return Err(Error::TaskExecutionFailed {
                    message: format!("Failed to read replacement intent seal: {error}"),
                });
            }
        }
        self.disk
            .write_all(RUSTFS_META_BUCKET, path, b"sealed".as_slice().into())
            .await
            .map_err(|error| Error::TaskExecutionFailed {
                message: format!("Failed to save replacement intent seal: {error}"),
            })
    }

    pub async fn mark_replacement_rebuilding(
        &self,
        mut replacement_target_identities: Vec<ReplacementTargetIdentity>,
    ) -> Result<()> {
        replacement_target_identities.sort_by(|left, right| left.endpoint.cmp(&right.endpoint));
        replacement_target_identities.dedup_by(|left, right| left.endpoint == right.endpoint);
        let mut state = self.state.write().await;
        if !matches!(
            state.replacement_phase,
            ReplacementPhase::Intent | ReplacementPhase::OwnershipPending | ReplacementPhase::Rebuilding
        ) {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement intent is not active for task {}", state.task_id),
            });
        }
        if replacement_target_identities
            .iter()
            .map(|identity| &identity.endpoint)
            .collect::<Vec<_>>()
            != state.replacement_targets.iter().collect::<Vec<_>>()
        {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement identities do not match targets for task {}", state.task_id),
            });
        }
        if !replacement_target_identities_match(&state.replacement_target_identities, &replacement_target_identities) {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement target changed after format for task {}", state.task_id),
            });
        }
        state.replacement_phase = ReplacementPhase::Rebuilding;
        state.error_message = None;
        state.replacement_legacy_retry_compatibility_done = true;
        state.replacement_retry_waiting_for_target = false;
        state.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        drop(state);
        self.save_state_strict().await
    }

    pub(crate) async fn record_replacement_failure(&self, error: &Error, retry_attempts: u32) -> Result<()> {
        let mut state = self.state.write().await;
        if state.replacement_generation.as_deref() != Some(state.task_id.as_str()) {
            return Err(replacement_recovery_conflict("replacement failure has no matching generation"));
        }
        let readiness_deferred = matches!(error, Error::ReplacementTargetNotReady(_))
            || (state.replacement_retry_waiting_for_target
                && matches!(error, Error::TransientSkip { message } if message.contains("target readiness deferred retry")));
        if readiness_deferred {
            if state.retry_count >= state.max_retries && state.max_retries > 0 {
                state.retry_count = state.max_retries.saturating_sub(1);
            }
            state.replacement_retry_waiting_for_target = true;
            state.error_message = Some(super::REPLACEMENT_TARGET_READINESS_DEFERRED.to_string());
            state.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
            drop(state);
            return self.save_state_strict().await;
        }
        state.error_message = Some(error.to_string());
        state.retry_count = state.retry_count.max(retry_attempts);
        if matches!(
            error,
            Error::ReplacementRetryBudgetExhausted
                | Error::ReplacementOwnershipConflict(_)
                | Error::ReplacementGenerationConflict { .. }
        ) {
            state.retry_count = state.max_retries;
        }
        state.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        drop(state);
        self.save_state_strict().await
    }

    /// Persist survivor-anchor completion proof before transitioning this
    /// resumable state to `Verified`. If proof persistence fails, this state
    /// stays rebuildable and the caller must retain the healing marker.
    pub async fn mark_replacement_completed_and_verified(&self) -> Result<()> {
        let state = self.state.read().await.clone();
        if !matches!(state.replacement_phase, ReplacementPhase::Intent | ReplacementPhase::Rebuilding) {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement verification is not active for task {}", state.task_id),
            });
        }
        let proof = self.write_replacement_completion_proof(&state, None).await?;

        let mut state = self.state.write().await;
        if !matches!(state.replacement_phase, ReplacementPhase::Intent | ReplacementPhase::Rebuilding) {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement verification changed for task {}", state.task_id),
            });
        }
        state.mark_completed();
        state.replacement_phase = ReplacementPhase::Verified;
        state.last_update = proof.verified_at;
        drop(state);
        self.save_state_strict().await
    }

    /// Verify or backfill the terminal proof before marker removal or resume
    /// cleanup. This supports restart recovery from a `Verified` state written
    /// by a prior binary that did not yet have a separate proof record.
    pub(crate) async fn ensure_replacement_completion_proof(&self) -> Result<ReplacementCompletionProof> {
        let state = self.state.read().await.clone();
        if !state.completed || !matches!(state.replacement_phase, ReplacementPhase::Verified | ReplacementPhase::CleanupPending) {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement completion is not verified for task {}", state.task_id),
            });
        }
        self.write_replacement_completion_proof(&state, Some(state.last_update)).await
    }

    /// Record that the healing markers have been removed, so a later retry can
    /// safely delete the remaining resume artifacts without touching markers.
    pub async fn mark_replacement_cleanup_pending(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if !state.completed || !matches!(state.replacement_phase, ReplacementPhase::Verified | ReplacementPhase::CleanupPending) {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement cleanup is not ready for task {}", state.task_id),
            });
        }
        state.replacement_phase = ReplacementPhase::CleanupPending;
        state.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        drop(state);
        self.save_state_strict().await
    }

    /// Load the durable terminal proof from the healthy survivor anchor.
    pub(crate) async fn load_replacement_completion_proof(disk: DiskStore, task_id: &str) -> Result<ReplacementCompletionProof> {
        Self::replacement_completion_proof_if_present(disk, task_id)
            .await?
            .ok_or_else(|| Error::TaskExecutionFailed {
                message: format!("Failed to read replacement completion proof: proof is missing for task {task_id}"),
            })
    }

    pub(super) async fn replacement_completion_proof_if_present(
        disk: DiskStore,
        task_id: &str,
    ) -> Result<Option<ReplacementCompletionProof>> {
        validate_resume_task_id(task_id)?;
        let mut proofs = Vec::new();
        for path in [
            replacement_completion_proof_path(task_id),
            legacy_replacement_completion_proof_path(task_id),
        ] {
            let path_str = path_to_str(&path)?;
            let bytes = match disk.read_all(RUSTFS_META_BUCKET, path_str).await {
                Ok(bytes) => bytes,
                Err(DiskError::FileNotFound) => continue,
                Err(error) => {
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to read replacement completion proof: {error}"),
                    });
                }
            };
            let proof: ReplacementCompletionProof =
                serde_json::from_slice(&bytes).map_err(|error| Error::TaskExecutionFailed {
                    message: format!("Failed to deserialize replacement completion proof: {error}"),
                })?;
            proof.validate(task_id)?;
            proofs.push(proof);
        }

        match proofs.as_slice() {
            [] => Ok(None),
            [proof] => Ok(Some(proof.clone())),
            [proof, legacy_proof] if proof == legacy_proof => Ok(Some(proof.clone())),
            _ => Err(replacement_recovery_conflict(format!(
                "Replacement completion proof conflicts with legacy proof for task {task_id}"
            ))),
        }
    }

    /// Reconcile the proof-first publication order after a crash. A matching
    /// proof is durable evidence that rebuilding finished, so it must win over
    /// an older active state before a retry may format the target again.
    pub(super) async fn reconcile_replacement_completion_proof(&self) -> Result<()> {
        let state = self.get_state().await;
        if state.replacement_handoff.is_some() || state.replacement_legacy_successor.is_some() {
            // Old proofs describe the retired target incarnation. Keep them as
            // evidence without reviving the predecessor or authorizing cleanup.
            return Ok(());
        }
        let task_id = self.state.read().await.task_id.clone();
        let Some(proof) = Self::replacement_completion_proof_if_present(self.disk.clone(), &task_id).await? else {
            return Ok(());
        };

        let mut state = self.state.write().await;
        if !proof.matches_state(&state) {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement completion proof does not match active intent for task {}", state.task_id),
            });
        }
        if state.completed && matches!(state.replacement_phase, ReplacementPhase::Verified | ReplacementPhase::CleanupPending) {
            return Ok(());
        }
        if state.completed || !matches!(state.replacement_phase, ReplacementPhase::Intent | ReplacementPhase::Rebuilding) {
            return Err(replacement_recovery_conflict(format!(
                "Replacement completion proof conflicts with state for task {}",
                state.task_id
            )));
        }

        state.mark_completed();
        state.replacement_phase = ReplacementPhase::Verified;
        state.last_update = proof.verified_at;
        drop(state);
        self.save_state_strict().await
    }

    pub(super) async fn migrate_legacy_replacement_completion_proof(disk: &DiskStore, task_id: &str) -> Result<bool> {
        validate_resume_task_id(task_id)?;
        let legacy_path = legacy_replacement_completion_proof_path(task_id);
        let legacy_path_str = path_to_str(&legacy_path)?;
        let legacy_bytes = match disk.read_all(RUSTFS_META_BUCKET, legacy_path_str).await {
            Ok(bytes) => bytes,
            Err(DiskError::FileNotFound) => return Ok(false),
            Err(error) => {
                return Err(Error::TaskExecutionFailed {
                    message: format!("Failed to read legacy replacement completion proof: {error}"),
                });
            }
        };
        let legacy_proof: ReplacementCompletionProof = serde_json::from_slice(&legacy_bytes).map_err(|error| {
            replacement_recovery_corruption(format!("Failed to deserialize legacy replacement completion proof: {error}"))
        })?;
        legacy_proof
            .validate(task_id)
            .map_err(|error| replacement_recovery_corruption(format!("Invalid legacy replacement completion proof: {error}")))?;

        ensure_replacement_recovery_dir(disk)
            .await
            .map_err(|error| Error::TaskExecutionFailed {
                message: format!("Failed to create replacement recovery directory: {error}"),
            })?;
        let path = replacement_completion_proof_path(task_id);
        let path_str = path_to_str(&path)?;
        for _ in 0..2 {
            match disk.read_all(RUSTFS_META_BUCKET, path_str).await {
                Ok(bytes) => {
                    let proof: ReplacementCompletionProof =
                        serde_json::from_slice(&bytes).map_err(|error| Error::TaskExecutionFailed {
                            message: format!("Failed to deserialize replacement completion proof: {error}"),
                        })?;
                    proof.validate(task_id).map_err(|error| {
                        replacement_recovery_corruption(format!("Invalid replacement completion proof: {error}"))
                    })?;
                    if proof != legacy_proof {
                        return Err(replacement_recovery_conflict(format!(
                            "Replacement completion proof conflicts with legacy proof for task {task_id}"
                        )));
                    }
                    delete_resume_file(disk, &legacy_path).await?;
                    return Ok(true);
                }
                Err(DiskError::FileNotFound) => {}
                Err(error) => {
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to read replacement completion proof: {error}"),
                    });
                }
            }

            match super::super::storage_api::owner::EcstoreDiskAPI::compare_and_update_file(
                disk.as_ref(),
                RUSTFS_META_BUCKET,
                path_str,
                None,
                Some(legacy_bytes.clone()),
            )
            .await
            {
                Ok(EcstoreConditionalFileUpdate::Updated) => {
                    delete_resume_file(disk, &legacy_path).await?;
                    return Ok(true);
                }
                Ok(EcstoreConditionalFileUpdate::Missing | EcstoreConditionalFileUpdate::Mismatch) => continue,
                Err(error) => {
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to migrate replacement completion proof: {error}"),
                    });
                }
            }
        }

        Err(Error::TaskExecutionFailed {
            message: format!("Replacement completion proof changed while migrating task {task_id}"),
        })
    }

    pub async fn abandon_replacement_intent(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if matches!(state.replacement_phase, ReplacementPhase::Abandoned) {
            return Ok(());
        }
        state.replacement_phase = ReplacementPhase::Abandoned;
        state.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        drop(state);
        self.save_state_strict().await
    }

    pub async fn set_replacement_targets(&self, replacement_targets: Vec<String>) -> Result<()> {
        {
            let mut state = self.state.write().await;
            state.replacement_targets = replacement_targets;
        }
        self.save_state().await
    }

    pub(super) async fn publish_new_replacement_intent(&self, expected: Option<EcstoreDiskBytes>) -> Result<()> {
        let state = self.state.read().await.clone();
        validate_resume_task_id(&state.task_id)?;
        let state_data = EcstoreDiskBytes::from(serde_json::to_vec(&state).map_err(|error| Error::TaskExecutionFailed {
            message: format!("Failed to serialize resume state: {error}"),
        })?);
        let path = self.state_file.path(&state.task_id);
        let path = path_to_str(&path)?;

        ensure_replacement_recovery_dir(&self.disk)
            .await
            .map_err(|error| Error::TaskExecutionFailed {
                message: format!("Failed to create replacement recovery directory: {error}"),
            })?;
        match super::super::storage_api::owner::EcstoreDiskAPI::compare_and_update_file(
            self.disk.as_ref(),
            RUSTFS_META_BUCKET,
            path,
            expected,
            Some(state_data),
        )
        .await
        {
            Ok(EcstoreConditionalFileUpdate::Updated) => Ok(()),
            Ok(EcstoreConditionalFileUpdate::Missing | EcstoreConditionalFileUpdate::Mismatch) => {
                Err(Error::TaskExecutionFailed {
                    message: format!("Replacement intent changed before publication for task {}", state.task_id),
                })
            }
            Err(error) => Err(Error::TaskExecutionFailed {
                message: format!("Failed to save resume state: {error}"),
            }),
        }
    }

    async fn write_replacement_completion_proof(
        &self,
        state: &ResumeState,
        verified_at: Option<u64>,
    ) -> Result<ReplacementCompletionProof> {
        ensure_replacement_recovery_dir(&self.disk)
            .await
            .map_err(|error| Error::TaskExecutionFailed {
                message: format!("Failed to create replacement recovery directory: {error}"),
            })?;
        let path = replacement_completion_proof_path(&state.task_id);
        let path_str = path_to_str(&path)?;
        let proof = ReplacementCompletionProof::from_state(
            state,
            verified_at.unwrap_or_else(|| SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()),
        )?;
        let proof_data = EcstoreDiskBytes::from(serde_json::to_vec(&proof).map_err(|e| Error::TaskExecutionFailed {
            message: format!("Failed to serialize replacement completion proof: {e}"),
        })?);
        if let Some(error) = injected_replacement_proof_write_error(path_str) {
            return Err(Error::TaskExecutionFailed {
                message: format!("Failed to save replacement completion proof: {error}"),
            });
        }

        // Publish through the disk CAS primitive: `write_all` can expose a
        // partially written proof to a crash/restart reader. If a prior
        // version left torn bytes behind, replace exactly the observed bytes;
        // a concurrently published valid proof is never overwritten.
        for _ in 0..2 {
            let expected = match self.disk.read_all(RUSTFS_META_BUCKET, path_str).await {
                Ok(existing) => match serde_json::from_slice::<ReplacementCompletionProof>(&existing) {
                    Ok(existing_proof) => {
                        existing_proof.validate(&state.task_id)?;
                        if existing_proof.matches_state(state) {
                            return Ok(existing_proof);
                        }
                        return Err(Error::TaskExecutionFailed {
                            message: format!("Replacement completion proof does not match task {}", state.task_id),
                        });
                    }
                    Err(_) => Some(existing),
                },
                Err(DiskError::FileNotFound) => None,
                Err(error) => {
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to read replacement completion proof: {error}"),
                    });
                }
            };

            match super::super::storage_api::owner::EcstoreDiskAPI::compare_and_update_file(
                self.disk.as_ref(),
                RUSTFS_META_BUCKET,
                path_str,
                expected,
                Some(proof_data.clone()),
            )
            .await
            {
                Ok(EcstoreConditionalFileUpdate::Updated) => return Ok(proof),
                Ok(EcstoreConditionalFileUpdate::Missing | EcstoreConditionalFileUpdate::Mismatch) => continue,
                Err(error) => {
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to save replacement completion proof: {error}"),
                    });
                }
            }
        }

        Err(Error::TaskExecutionFailed {
            message: format!("Replacement completion proof changed while publishing task {}", state.task_id),
        })
    }

    pub(super) async fn write_replacement_intent_state(
        &self,
        path: &str,
        state_data: EcstoreDiskBytes,
    ) -> std::result::Result<(), DiskError> {
        ensure_replacement_recovery_dir(&self.disk).await?;
        let mut candidate: ResumeState = serde_json::from_slice(&state_data).map_err(DiskError::other)?;
        let expected_revision = candidate.replacement_revision;
        candidate.replacement_revision = expected_revision
            .checked_add(1)
            .ok_or_else(|| DiskError::other("replacement intent revision exhausted"))?;
        let existing = self.disk.read_all(RUSTFS_META_BUCKET, path).await?;
        let observed: ResumeState = serde_json::from_slice(&existing).map_err(DiskError::other)?;
        if observed.replacement_revision != expected_revision || observed.task_id != candidate.task_id {
            return Err(DiskError::other("replacement intent changed while publishing"));
        }
        let bytes = serde_json::to_vec(&candidate).map_err(DiskError::other)?;
        match super::super::storage_api::owner::EcstoreDiskAPI::compare_and_update_file(
            self.disk.as_ref(),
            RUSTFS_META_BUCKET,
            path,
            Some(existing),
            Some(bytes.into()),
        )
        .await?
        {
            EcstoreConditionalFileUpdate::Updated => {
                self.state.write().await.replacement_revision = candidate.replacement_revision;
                Ok(())
            }
            EcstoreConditionalFileUpdate::Missing | EcstoreConditionalFileUpdate::Mismatch => {
                Err(DiskError::other("replacement intent changed while publishing"))
            }
        }
    }
}
