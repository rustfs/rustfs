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

use std::collections::BTreeMap;

use rustfs_utils::crypto::{hex_sha256, is_sha256_checksum};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    bucket_lifecycle_ops::{
        ManualTransitionQueueSnapshot, ManualTransitionRunReport, decode_manual_transition_continuation_token,
    },
    manual_transition_job, recovery_control, recovery_disposition, recovery_export, tier_delete_journal, transition_transaction,
};
use crate::error::{Error, Result};
use crate::services::tier::tier_probe_intent;

pub(crate) const ILM_META_PREFIX: &str = "ilm";
const ILM_META_OBJECT_PREFIX: &str = "ilm/";
const MANUAL_TRANSITION_CURSOR_MARKER_PROOF_MAX_SIZE: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DurableIlmRecordKind {
    TierDeleteJournal,
    TierDeleteDispatchManifest,
    TransitionTransaction,
    TierProbeIntent,
    ManualTransitionJob,
    ManualTransitionScope,
    ManualTransitionTask,
    ManualTransitionWorkerResult,
    RecoveryControl,
    RecoveryExport,
    RecoveryDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DurableIlmNamespace {
    pub(crate) name: &'static str,
    pub(crate) prefix: &'static str,
    pub(crate) max_record_size: usize,
    kind: DurableIlmRecordKind,
}

pub(crate) const TIER_DELETE_JOURNAL_NAMESPACE: DurableIlmNamespace = DurableIlmNamespace {
    name: "tier-delete-journal",
    prefix: "ilm/tier-delete-journal/",
    max_record_size: 64 * 1024,
    kind: DurableIlmRecordKind::TierDeleteJournal,
};
pub(crate) const TIER_DELETE_JOURNAL_V6_NAMESPACE: DurableIlmNamespace = DurableIlmNamespace {
    name: "tier-delete-journal-v6",
    prefix: "ilm/tier-delete-journal-v6/",
    max_record_size: 64 * 1024,
    kind: DurableIlmRecordKind::TierDeleteJournal,
};
pub(crate) const TIER_DELETE_DISPATCH_MANIFEST_NAMESPACE: DurableIlmNamespace = DurableIlmNamespace {
    name: "tier-delete-dispatch-manifest",
    prefix: tier_delete_journal::TIER_DELETE_DISPATCH_MANIFEST_PREFIX,
    max_record_size: tier_delete_journal::MAX_TIER_DELETE_DISPATCH_MANIFEST_SIZE,
    kind: DurableIlmRecordKind::TierDeleteDispatchManifest,
};
pub(crate) const TRANSITION_TRANSACTION_NAMESPACE: DurableIlmNamespace = DurableIlmNamespace {
    name: "transition-transaction",
    prefix: "ilm/transition-transactions/records",
    max_record_size: transition_transaction::MAX_TRANSITION_TRANSACTION_SIZE,
    kind: DurableIlmRecordKind::TransitionTransaction,
};
pub(crate) const TIER_PROBE_INTENT_NAMESPACE: DurableIlmNamespace = DurableIlmNamespace {
    name: "tier-probe-intent",
    prefix: tier_probe_intent::TIER_PROBE_INTENT_RECORD_PREFIX,
    max_record_size: tier_probe_intent::MAX_TIER_PROBE_INTENT_SIZE,
    kind: DurableIlmRecordKind::TierProbeIntent,
};
pub(crate) const MANUAL_TRANSITION_JOB_NAMESPACE: DurableIlmNamespace = DurableIlmNamespace {
    name: "manual-transition-job",
    prefix: "ilm/manual-transition/jobs",
    max_record_size: manual_transition_job::MAX_MANUAL_TRANSITION_JOB_RECORD_SIZE,
    kind: DurableIlmRecordKind::ManualTransitionJob,
};
pub(crate) const MANUAL_TRANSITION_SCOPE_NAMESPACE: DurableIlmNamespace = DurableIlmNamespace {
    name: "manual-transition-scope",
    prefix: "ilm/manual-transition/scopes",
    max_record_size: manual_transition_job::MAX_MANUAL_TRANSITION_JOB_RECORD_SIZE,
    kind: DurableIlmRecordKind::ManualTransitionScope,
};
pub(crate) const MANUAL_TRANSITION_TASK_NAMESPACE: DurableIlmNamespace = DurableIlmNamespace {
    name: "manual-transition-task",
    prefix: "ilm/manual-transition/tasks",
    max_record_size: manual_transition_job::MAX_MANUAL_TRANSITION_TASK_RECORD_SIZE,
    kind: DurableIlmRecordKind::ManualTransitionTask,
};
pub(crate) const MANUAL_TRANSITION_WORKER_RESULT_NAMESPACE: DurableIlmNamespace = DurableIlmNamespace {
    name: "manual-transition-worker-result",
    prefix: "ilm/manual-transition/results",
    max_record_size: manual_transition_job::MAX_MANUAL_TRANSITION_WORKER_RESULT_RECORD_SIZE,
    kind: DurableIlmRecordKind::ManualTransitionWorkerResult,
};
pub(crate) const RECOVERY_CONTROL_NAMESPACE: DurableIlmNamespace = DurableIlmNamespace {
    name: "recovery-control",
    prefix: recovery_control::ILM_RECOVERY_CONTROL_PREFIX,
    max_record_size: recovery_control::MAX_ILM_RECOVERY_CONTROL_SIZE,
    kind: DurableIlmRecordKind::RecoveryControl,
};
pub(crate) const RECOVERY_EXPORT_NAMESPACE: DurableIlmNamespace = DurableIlmNamespace {
    name: "recovery-export",
    prefix: recovery_export::ILM_RECOVERY_EXPORT_PREFIX,
    max_record_size: recovery_export::MAX_ILM_RECOVERY_EXPORT_SIZE,
    kind: DurableIlmRecordKind::RecoveryExport,
};
pub(crate) const RECOVERY_DISPOSITION_NAMESPACE: DurableIlmNamespace = DurableIlmNamespace {
    name: "recovery-disposition",
    prefix: recovery_disposition::ILM_RECOVERY_DISPOSITION_PREFIX,
    max_record_size: recovery_disposition::MAX_ILM_RECOVERY_DISPOSITION_SIZE,
    kind: DurableIlmRecordKind::RecoveryDisposition,
};

pub(crate) const DURABLE_ILM_NAMESPACES: [DurableIlmNamespace; 12] = [
    TIER_DELETE_JOURNAL_NAMESPACE,
    TIER_DELETE_JOURNAL_V6_NAMESPACE,
    TIER_DELETE_DISPATCH_MANIFEST_NAMESPACE,
    TRANSITION_TRANSACTION_NAMESPACE,
    TIER_PROBE_INTENT_NAMESPACE,
    MANUAL_TRANSITION_JOB_NAMESPACE,
    MANUAL_TRANSITION_SCOPE_NAMESPACE,
    MANUAL_TRANSITION_TASK_NAMESPACE,
    MANUAL_TRANSITION_WORKER_RESULT_NAMESPACE,
    RECOVERY_CONTROL_NAMESPACE,
    RECOVERY_EXPORT_NAMESPACE,
    RECOVERY_DISPOSITION_NAMESPACE,
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedDurableIlmRecord {
    pub(crate) namespace: &'static str,
    pub(crate) id_kind: &'static str,
    pub(crate) id: String,
    pub(crate) checkpoint: DurableIlmRecordCheckpoint,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManualTransitionJobProgressCheckpoint {
    report: ManualTransitionRunReport,
    queue_snapshot: ManualTransitionQueueSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManualTransitionJobProgressProof {
    scope_sha256: String,
    scanned: u64,
    eligible: u64,
    enqueued: u64,
    dry_run_eligible: u64,
    skipped_not_transition: u64,
    skipped_tier: u64,
    skipped_delete_marker: u64,
    skipped_directory: u64,
    skipped_replication: u64,
    skipped_already_transitioned: u64,
    skipped_already_in_flight: u64,
    skipped_queue_full: u64,
    skipped_queue_closed: u64,
    skipped_queue_timeout: u64,
    transition_completed: u64,
    transition_failed: u64,
    tier_failure: u64,
    tier_failure_by_reason: BTreeMap<manual_transition_job::ManualTransitionWorkerFailureReason, u64>,
    lifecycle_config_found: bool,
    truncated_by_limit: bool,
    truncated_by_duration: bool,
    cancelled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    continuation_token_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cursor_marker: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cursor_revision: Option<u64>,
    queue_snapshot: ManualTransitionQueueSnapshot,
}

impl ValidatedDurableIlmRecord {
    pub(crate) fn context(&self) -> String {
        format!("namespace `{}` {} `{}`", self.namespace, self.id_kind, self.id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum DurableIlmRecordCheckpoint {
    TierDeleteJournal {
        content_sha256: String,
        identity_sha256: String,
        committed: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dispatch_identity_sha256: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        state: Option<super::tier_sweeper::TierDeleteJournalState>,
    },
    TierDeleteDispatchManifest {
        content_sha256: String,
        identity_sha256: String,
        state: tier_delete_journal::TierDeleteDispatchManifestState,
    },
    TierDeleteDispatchParent {
        content_sha256: String,
        identity_sha256: String,
        revision: u64,
        next_chunk_sequence: u64,
        completed_journal_count: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        active_chunk_identity_sha256: Option<String>,
        completed: bool,
    },
    TransitionTransaction {
        content_sha256: String,
        identity_sha256: String,
        remote_version_sha256: String,
        remote_version_known: bool,
        revision: u64,
        state: transition_transaction::TransitionTransactionState,
    },
    TierProbeIntent {
        content_sha256: String,
        identity_sha256: String,
        remote_version_sha256: String,
        remote_version_known: bool,
        owner_fence_sha256: String,
        revision: u64,
        state: tier_probe_intent::TierProbeIntentState,
    },
    ManualTransitionJob {
        content_sha256: String,
        identity_sha256: String,
        updated_at_unix_nanos: i64,
        state: manual_transition_job::ManualTransitionJobState,
        scan_completed: bool,
        cancel_requested: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        progress: Option<Box<ManualTransitionJobProgressCheckpoint>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        progress_proof: Option<Box<ManualTransitionJobProgressProof>>,
    },
    ManualTransitionScope {
        content_sha256: String,
        identity_sha256: String,
        updated_at_unix_nanos: i64,
    },
    ManualTransitionTask {
        content_sha256: String,
    },
    ManualTransitionWorkerResult {
        content_sha256: String,
    },
    RecoveryControl {
        content_sha256: String,
        identity_sha256: String,
        source_generation_sha256: String,
        first_seen_at_unix_nanos: i64,
        revision: u64,
        classification: recovery_control::IlmRecoveryClassification,
        attempt_count: u64,
        consecutive_failure_count: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        owner_fence_sha256: Option<String>,
    },
    RecoveryExport {
        content_sha256: String,
        source_generation_sha256: String,
        topology_generation: String,
        member_epochs_sha256: String,
        creator_sha256: String,
        retain_until_unix_nanos: i64,
    },
    RecoveryDisposition {
        content_sha256: String,
        identity_sha256: String,
        copy_manifest_sha256: String,
        copy_manifest_count: usize,
        created_at_unix_nanos: i64,
        revision: u64,
        state: recovery_disposition::IlmRecoveryDispositionState,
        owner_fence_sha256: Option<String>,
        owner_lease_acquired_at_unix_nanos: Option<i64>,
        owner_lease_expires_at_unix_nanos: Option<i64>,
        confirmed_absent_sha256: Vec<String>,
        retain_until_unix_nanos: i64,
    },
}

impl DurableIlmRecordCheckpoint {
    pub(crate) fn content_sha256(&self) -> &str {
        match self {
            Self::TierDeleteJournal { content_sha256, .. }
            | Self::TierDeleteDispatchManifest { content_sha256, .. }
            | Self::TierDeleteDispatchParent { content_sha256, .. }
            | Self::TransitionTransaction { content_sha256, .. }
            | Self::TierProbeIntent { content_sha256, .. }
            | Self::ManualTransitionJob { content_sha256, .. }
            | Self::ManualTransitionScope { content_sha256, .. }
            | Self::ManualTransitionTask { content_sha256 }
            | Self::ManualTransitionWorkerResult { content_sha256 }
            | Self::RecoveryControl { content_sha256, .. }
            | Self::RecoveryExport { content_sha256, .. }
            | Self::RecoveryDisposition { content_sha256, .. } => content_sha256,
        }
    }

    pub(crate) fn compacted(&self) -> Result<Self> {
        let mut checkpoint = self.clone();
        if let Self::ManualTransitionJob {
            progress,
            progress_proof,
            ..
        } = &mut checkpoint
        {
            match (progress.take(), progress_proof.take()) {
                (Some(progress), None) => {
                    *progress_proof = Some(Box::new(ManualTransitionJobProgressProof::new(
                        &progress.report,
                        &progress.queue_snapshot,
                        None,
                    )?));
                }
                (None, Some(proof)) if proof.is_valid() => *progress_proof = Some(proof),
                (None, None) => {}
                _ => return Err(Error::other("durable ILM manual transition checkpoint is invalid")),
            }
        }
        Ok(checkpoint)
    }

    pub(crate) fn validate_successor(&self, next: &Self) -> Result<()> {
        for checkpoint in [self, next] {
            if let Self::TierDeleteJournal {
                committed,
                dispatch_identity_sha256,
                state,
                ..
            } = checkpoint
                && (state.is_some() != dispatch_identity_sha256.is_some()
                    || state.is_some_and(|state| *committed != (state == super::tier_sweeper::TierDeleteJournalState::Committed)))
            {
                return Err(Error::other("durable ILM tier delete journal checkpoint is invalid"));
            }
            if !recovery_disposition_checkpoint_is_valid(checkpoint) {
                return Err(Error::other("durable ILM recovery disposition checkpoint is invalid"));
            }
        }
        if self == next {
            if let Self::ManualTransitionJob {
                progress,
                progress_proof,
                ..
            } = self
                && !manual_job_progress_checkpoint_is_valid(progress.as_deref(), progress_proof.as_deref())
            {
                return Err(Error::other("durable ILM manual transition checkpoint is invalid"));
            }
            return Ok(());
        }

        let valid = match (self, next) {
            (
                Self::TierDeleteJournal {
                    content_sha256: previous_content,
                    identity_sha256: previous_identity,
                    committed: previous_committed,
                    dispatch_identity_sha256: previous_dispatch_identity,
                    state: previous_state,
                    ..
                },
                Self::TierDeleteJournal {
                    content_sha256: next_content,
                    identity_sha256: next_identity,
                    committed: next_committed,
                    dispatch_identity_sha256: next_dispatch_identity,
                    state: next_state,
                    ..
                },
            ) => {
                use super::tier_sweeper::TierDeleteJournalState::{Committed, Dispatched, Prepared};

                let dispatch_identity_is_monotonic = match (previous_dispatch_identity, next_dispatch_identity) {
                    (Some(previous), Some(next)) => previous == next,
                    (None, None) => true,
                    // Old receipts did not record the v6 dispatch binding. A
                    // byte-identical observation may adopt the stronger proof,
                    // but an in-flight mutation must fail closed instead of
                    // guessing which operation owned the journal.
                    (None, Some(_)) => previous_content == next_content,
                    (Some(_), None) => false,
                };
                let state_is_monotonic = match (previous_state, next_state) {
                    (Some(previous), Some(next)) => {
                        previous == next || matches!((previous, next), (Prepared, Dispatched) | (Dispatched, Committed))
                    }
                    (None, None) => previous_committed == next_committed || (!previous_committed && *next_committed),
                    (None, Some(_)) => previous_content == next_content,
                    (Some(_), None) => false,
                };
                previous_identity == next_identity && dispatch_identity_is_monotonic && state_is_monotonic
            }
            (
                Self::TierDeleteDispatchManifest {
                    identity_sha256: previous_identity,
                    state: previous_state,
                    ..
                },
                Self::TierDeleteDispatchManifest {
                    identity_sha256: next_identity,
                    state: next_state,
                    ..
                },
            ) => {
                use tier_delete_journal::TierDeleteDispatchManifestState::{
                    Aborted, Aborting, Completed, DispatchAuthorized, Preparing,
                };
                previous_identity == next_identity
                    && matches!(
                        (previous_state, next_state),
                        (Preparing, DispatchAuthorized | Aborting) | (Aborting, Aborted) | (DispatchAuthorized, Completed)
                    )
            }
            (
                Self::TierDeleteDispatchParent {
                    identity_sha256: previous_identity,
                    revision: previous_revision,
                    next_chunk_sequence: previous_sequence,
                    completed_journal_count: previous_completed_journals,
                    active_chunk_identity_sha256: previous_active,
                    completed: previous_completed,
                    ..
                },
                Self::TierDeleteDispatchParent {
                    identity_sha256: next_identity,
                    revision: next_revision,
                    next_chunk_sequence: next_sequence,
                    completed_journal_count: next_completed_journals,
                    active_chunk_identity_sha256: next_active,
                    completed: next_completed,
                    ..
                },
            ) => {
                let Some((sequence_delta, completed_journal_delta)) = tier_delete_dispatch_parent_progress_delta(
                    *previous_sequence,
                    *previous_completed_journals,
                    *next_sequence,
                    *next_completed_journals,
                ) else {
                    return Err(Error::other("durable ILM record generation is not a monotonic successor"));
                };
                let same_position_transition = sequence_delta == 0
                    && completed_journal_delta == 0
                    && matches!(
                        (previous_active.as_ref(), next_active.as_ref(), previous_completed, next_completed),
                        (None, Some(_), false, false) | (Some(_), None, false, false) | (None, None, false, true)
                    );
                let progress_transition = sequence_delta > 0
                    && completed_journal_delta > 0
                    && !matches!(
                        (previous_active.as_ref(), next_active.as_ref()),
                        (Some(previous), Some(next)) if previous == next
                    );
                previous_identity == next_identity
                    && !previous_completed
                    && next_revision > previous_revision
                    && (!next_completed || next_active.is_none())
                    && (same_position_transition || progress_transition)
            }
            (
                Self::TransitionTransaction {
                    identity_sha256: previous_identity,
                    remote_version_sha256: previous_remote_version,
                    remote_version_known: previous_remote_version_known,
                    revision: previous_revision,
                    state: previous_state,
                    ..
                },
                Self::TransitionTransaction {
                    identity_sha256: next_identity,
                    remote_version_sha256: next_remote_version,
                    revision: next_revision,
                    state: next_state,
                    ..
                },
            ) => {
                previous_identity == next_identity
                    && transition_state_revision_is_successor(*previous_state, *previous_revision, *next_state, *next_revision)
                    && (!previous_remote_version_known || previous_remote_version == next_remote_version)
            }
            (
                Self::TierProbeIntent {
                    identity_sha256: previous_identity,
                    remote_version_sha256: previous_remote_version,
                    remote_version_known: previous_remote_version_known,
                    owner_fence_sha256: previous_owner_fence,
                    revision: previous_revision,
                    state: previous_state,
                    ..
                },
                Self::TierProbeIntent {
                    identity_sha256: next_identity,
                    remote_version_sha256: next_remote_version,
                    owner_fence_sha256: next_owner_fence,
                    revision: next_revision,
                    state: next_state,
                    ..
                },
            ) => {
                previous_identity == next_identity
                    && previous_owner_fence == next_owner_fence
                    && next_revision
                        .checked_sub(*previous_revision)
                        .is_some_and(|distance| distance == 1 && tier_probe_state_reaches(*previous_state, *next_state, distance))
                    && (!previous_remote_version_known || previous_remote_version == next_remote_version)
            }
            (
                Self::ManualTransitionJob {
                    content_sha256: previous_content,
                    identity_sha256: previous_identity,
                    updated_at_unix_nanos: previous_updated_at,
                    state: previous_state,
                    scan_completed: previous_scan_completed,
                    cancel_requested: previous_cancel_requested,
                    progress: previous_progress,
                    progress_proof: previous_progress_proof,
                    ..
                },
                Self::ManualTransitionJob {
                    content_sha256: next_content,
                    identity_sha256: next_identity,
                    updated_at_unix_nanos: next_updated_at,
                    state: next_state,
                    scan_completed: next_scan_completed,
                    cancel_requested: next_cancel_requested,
                    progress: next_progress,
                    progress_proof: next_progress_proof,
                    ..
                },
            ) => {
                let same_generation = previous_content == next_content
                    && previous_identity == next_identity
                    && previous_updated_at == next_updated_at
                    && previous_state == next_state
                    && previous_scan_completed == next_scan_completed
                    && previous_cancel_requested == next_cancel_requested
                    && manual_job_progress_equivalent(
                        previous_progress.as_deref(),
                        previous_progress_proof.as_deref(),
                        next_progress.as_deref(),
                        next_progress_proof.as_deref(),
                    );
                same_generation
                    || (previous_identity == next_identity
                        && next_updated_at > previous_updated_at
                        && manual_job_state_reaches(*previous_state, *next_state)
                        && (!previous_scan_completed || *next_scan_completed)
                        && (!previous_cancel_requested || *next_cancel_requested)
                        && manual_job_progress_reaches(
                            previous_progress.as_deref(),
                            previous_progress_proof.as_deref(),
                            next_progress.as_deref(),
                            next_progress_proof.as_deref(),
                            *next_scan_completed,
                        ))
            }
            (
                Self::ManualTransitionScope {
                    identity_sha256: previous_identity,
                    updated_at_unix_nanos: previous_updated_at,
                    ..
                },
                Self::ManualTransitionScope {
                    identity_sha256: next_identity,
                    updated_at_unix_nanos: next_updated_at,
                    ..
                },
            ) => previous_identity == next_identity && next_updated_at > previous_updated_at,
            (
                Self::RecoveryControl {
                    identity_sha256: previous_identity,
                    source_generation_sha256: previous_generation,
                    first_seen_at_unix_nanos: previous_first_seen,
                    revision: previous_revision,
                    classification: previous_classification,
                    attempt_count: previous_attempts,
                    consecutive_failure_count: previous_failures,
                    owner_fence_sha256: previous_owner,
                    ..
                },
                Self::RecoveryControl {
                    identity_sha256: next_identity,
                    source_generation_sha256: next_generation,
                    first_seen_at_unix_nanos: next_first_seen,
                    revision: next_revision,
                    classification: next_classification,
                    attempt_count: next_attempts,
                    consecutive_failure_count: next_failures,
                    owner_fence_sha256: next_owner,
                    ..
                },
            ) => {
                let adjacent = previous_identity == next_identity
                    && previous_first_seen == next_first_seen
                    && previous_revision.checked_add(1) == Some(*next_revision);
                let claim = next_owner.is_some()
                    && *previous_classification == recovery_control::IlmRecoveryClassification::Retrying
                    && *next_classification == recovery_control::IlmRecoveryClassification::Retrying
                    && previous_attempts.checked_add(1) == Some(*next_attempts)
                    && previous_failures == next_failures;
                let source_refresh = previous_owner.is_some()
                    && previous_owner == next_owner
                    && *previous_classification == recovery_control::IlmRecoveryClassification::Retrying
                    && *next_classification == recovery_control::IlmRecoveryClassification::Retrying
                    && previous_attempts == next_attempts
                    && previous_failures == next_failures
                    && previous_generation != next_generation;
                let completion = previous_owner.is_some()
                    && next_owner.is_none()
                    && previous_generation == next_generation
                    && previous_attempts == next_attempts;
                adjacent && (claim || source_refresh || completion)
            }
            (
                Self::RecoveryDisposition {
                    identity_sha256: previous_identity,
                    copy_manifest_sha256: previous_manifest,
                    copy_manifest_count: previous_manifest_count,
                    created_at_unix_nanos: previous_created_at,
                    revision: previous_revision,
                    state: previous_state,
                    owner_fence_sha256: previous_owner,
                    owner_lease_acquired_at_unix_nanos: previous_owner_acquired,
                    owner_lease_expires_at_unix_nanos: previous_owner_expires,
                    confirmed_absent_sha256: previous_confirmed,
                    retain_until_unix_nanos: previous_retain_until,
                    ..
                },
                Self::RecoveryDisposition {
                    identity_sha256: next_identity,
                    copy_manifest_sha256: next_manifest,
                    copy_manifest_count: next_manifest_count,
                    created_at_unix_nanos: next_created_at,
                    revision: next_revision,
                    state: next_state,
                    owner_fence_sha256: next_owner,
                    owner_lease_acquired_at_unix_nanos: next_owner_acquired,
                    owner_lease_expires_at_unix_nanos: next_owner_expires,
                    confirmed_absent_sha256: next_confirmed,
                    retain_until_unix_nanos: next_retain_until,
                    ..
                },
            ) => {
                use recovery_disposition::IlmRecoveryDispositionState::{Applying, Completed, Prepared};

                let immutable_identity_matches = previous_identity == next_identity
                    && previous_manifest == next_manifest
                    && previous_manifest_count == next_manifest_count
                    && previous_created_at == next_created_at
                    && previous_retain_until == next_retain_until;
                let adjacent = previous_revision.checked_add(1) == Some(*next_revision);
                let progress_is_monotonic = sorted_sha256_set_is_subset(previous_confirmed, next_confirmed);
                let legal_edge = match (previous_state, next_state) {
                    (Prepared, Prepared) => {
                        let claim = previous_owner.is_none() && next_owner.is_some();
                        let takeover = previous_owner.is_some()
                            && previous_owner != next_owner
                            && previous_owner_expires
                                .zip(*next_owner_acquired)
                                .is_some_and(|(expires, acquired)| acquired >= expires);
                        previous_confirmed == next_confirmed && (claim || takeover)
                    }
                    (Prepared, Applying) => {
                        previous_confirmed == next_confirmed
                            && previous_owner.is_some()
                            && previous_owner == next_owner
                            && previous_owner_acquired == next_owner_acquired
                            && previous_owner_expires == next_owner_expires
                    }
                    (Applying, Applying) => {
                        let progress = previous_owner == next_owner
                            && previous_owner_acquired == next_owner_acquired
                            && previous_owner_expires == next_owner_expires
                            && previous_confirmed.len().checked_add(1) == Some(next_confirmed.len());
                        let takeover = previous_owner.is_some()
                            && previous_owner != next_owner
                            && previous_confirmed == next_confirmed
                            && previous_owner_expires
                                .zip(*next_owner_acquired)
                                .is_some_and(|(expires, acquired)| acquired >= expires);
                        progress || takeover
                    }
                    (Applying, Completed) => {
                        previous_owner.is_some() && next_owner.is_none() && previous_confirmed == next_confirmed
                    }
                    _ => false,
                };

                immutable_identity_matches && adjacent && progress_is_monotonic && legal_edge
            }
            _ => false,
        };

        if valid {
            Ok(())
        } else {
            Err(Error::other("durable ILM record generation is not a monotonic successor"))
        }
    }

    /// Whether `self` is an older generation of the same immutable record
    /// that can reach `terminal` through one or more valid state transitions.
    /// This is deliberately broader than `validate_successor`, which remains
    /// adjacent-only for receipt advancement. Terminal cleanup uses this only
    /// after the exact terminal ETag and terminal receipt were committed, to
    /// purge older object versions exposed by that deletion.
    pub(crate) fn is_predecessor_of_terminal(&self, terminal: &Self) -> bool {
        for checkpoint in [self, terminal] {
            if !recovery_disposition_checkpoint_is_valid(checkpoint) {
                return false;
            }
        }
        if let Self::TierProbeIntent { state, .. } = terminal
            && !matches!(
                state,
                tier_probe_intent::TierProbeIntentState::AbortedNoRemote | tier_probe_intent::TierProbeIntentState::Completed
            )
        {
            return false;
        }
        if let Self::RecoveryControl { classification, .. } = terminal
            && !matches!(
                classification,
                recovery_control::IlmRecoveryClassification::Terminal | recovery_control::IlmRecoveryClassification::Abandoned
            )
        {
            return false;
        }
        if let Self::RecoveryDisposition { state, .. } = terminal
            && state != &recovery_disposition::IlmRecoveryDispositionState::Completed
        {
            return false;
        }
        if self == terminal || self.validate_successor(terminal).is_ok() {
            return true;
        }
        match (self, terminal) {
            (
                Self::TierDeleteJournal {
                    identity_sha256: previous_identity,
                    dispatch_identity_sha256: previous_dispatch,
                    state: Some(super::tier_sweeper::TierDeleteJournalState::Prepared),
                    ..
                },
                Self::TierDeleteJournal {
                    identity_sha256: terminal_identity,
                    dispatch_identity_sha256: terminal_dispatch,
                    state: Some(super::tier_sweeper::TierDeleteJournalState::Committed),
                    ..
                },
            ) => previous_identity == terminal_identity && previous_dispatch == terminal_dispatch,
            (
                Self::TierDeleteDispatchManifest {
                    identity_sha256: previous_identity,
                    state: tier_delete_journal::TierDeleteDispatchManifestState::Preparing,
                    ..
                },
                Self::TierDeleteDispatchManifest {
                    identity_sha256: terminal_identity,
                    state:
                        tier_delete_journal::TierDeleteDispatchManifestState::Aborted
                        | tier_delete_journal::TierDeleteDispatchManifestState::Completed,
                    ..
                },
            ) => previous_identity == terminal_identity,
            (
                Self::TierDeleteDispatchParent {
                    identity_sha256: previous_identity,
                    revision: previous_revision,
                    next_chunk_sequence: previous_sequence,
                    completed_journal_count: previous_completed_journals,
                    active_chunk_identity_sha256: previous_active,
                    completed: false,
                    ..
                },
                Self::TierDeleteDispatchParent {
                    identity_sha256: terminal_identity,
                    revision: terminal_revision,
                    next_chunk_sequence: terminal_sequence,
                    completed_journal_count: terminal_completed_journals,
                    active_chunk_identity_sha256: None,
                    completed: true,
                    ..
                },
            ) => {
                previous_identity == terminal_identity
                    && terminal_revision > previous_revision
                    && tier_delete_dispatch_parent_progress_delta(
                        *previous_sequence,
                        *previous_completed_journals,
                        *terminal_sequence,
                        *terminal_completed_journals,
                    )
                    .is_some_and(|(sequence_delta, completed_journal_delta)| {
                        if sequence_delta == 0 && completed_journal_delta == 0 {
                            previous_active.is_none()
                        } else {
                            sequence_delta > 0 && completed_journal_delta > 0
                        }
                    })
            }
            (
                Self::TierProbeIntent {
                    identity_sha256: previous_identity,
                    remote_version_sha256: previous_remote_version,
                    remote_version_known: previous_remote_version_known,
                    owner_fence_sha256: previous_owner_fence,
                    revision: previous_revision,
                    state: previous_state,
                    ..
                },
                Self::TierProbeIntent {
                    identity_sha256: terminal_identity,
                    remote_version_sha256: terminal_remote_version,
                    owner_fence_sha256: terminal_owner_fence,
                    revision: terminal_revision,
                    state: terminal_state,
                    ..
                },
            ) => {
                previous_identity == terminal_identity
                    && previous_owner_fence == terminal_owner_fence
                    && matches!(
                        terminal_state,
                        tier_probe_intent::TierProbeIntentState::AbortedNoRemote
                            | tier_probe_intent::TierProbeIntentState::Completed
                    )
                    && terminal_revision
                        .checked_sub(*previous_revision)
                        .is_some_and(|distance| tier_probe_state_reaches(*previous_state, *terminal_state, distance))
                    && (!previous_remote_version_known || previous_remote_version == terminal_remote_version)
            }
            (
                Self::RecoveryControl {
                    identity_sha256: previous_identity,
                    source_generation_sha256: previous_generation,
                    first_seen_at_unix_nanos: previous_first_seen,
                    revision: previous_revision,
                    attempt_count: previous_attempts,
                    ..
                },
                Self::RecoveryControl {
                    identity_sha256: terminal_identity,
                    source_generation_sha256: terminal_generation,
                    first_seen_at_unix_nanos: terminal_first_seen,
                    revision: terminal_revision,
                    attempt_count: terminal_attempts,
                    classification:
                        recovery_control::IlmRecoveryClassification::Terminal | recovery_control::IlmRecoveryClassification::Abandoned,
                    ..
                },
            ) => {
                previous_identity == terminal_identity
                    && (previous_generation == terminal_generation || terminal_attempts > previous_attempts)
                    && previous_first_seen == terminal_first_seen
                    && terminal_revision > previous_revision
                    && terminal_attempts >= previous_attempts
            }
            (
                Self::RecoveryDisposition {
                    identity_sha256: previous_identity,
                    copy_manifest_sha256: previous_manifest,
                    copy_manifest_count: previous_manifest_count,
                    created_at_unix_nanos: previous_created_at,
                    revision: previous_revision,
                    state: previous_state,
                    owner_fence_sha256: previous_owner,
                    confirmed_absent_sha256: previous_confirmed,
                    retain_until_unix_nanos: previous_retain_until,
                    ..
                },
                Self::RecoveryDisposition {
                    identity_sha256: terminal_identity,
                    copy_manifest_sha256: terminal_manifest,
                    copy_manifest_count: terminal_manifest_count,
                    created_at_unix_nanos: terminal_created_at,
                    revision: terminal_revision,
                    state: recovery_disposition::IlmRecoveryDispositionState::Completed,
                    confirmed_absent_sha256: terminal_confirmed,
                    retain_until_unix_nanos: terminal_retain_until,
                    ..
                },
            ) => {
                matches!(
                    previous_state,
                    recovery_disposition::IlmRecoveryDispositionState::Prepared
                        | recovery_disposition::IlmRecoveryDispositionState::Applying
                ) && previous_identity == terminal_identity
                    && previous_manifest == terminal_manifest
                    && previous_manifest_count == terminal_manifest_count
                    && previous_created_at == terminal_created_at
                    && previous_retain_until == terminal_retain_until
                    && terminal_revision.checked_sub(*previous_revision).is_some_and(|distance| {
                        let minimum_distance = match previous_state {
                            recovery_disposition::IlmRecoveryDispositionState::Prepared if previous_owner.is_some() => 3,
                            recovery_disposition::IlmRecoveryDispositionState::Prepared => 4,
                            recovery_disposition::IlmRecoveryDispositionState::Applying
                                if previous_confirmed.len() == *previous_manifest_count =>
                            {
                                1
                            }
                            recovery_disposition::IlmRecoveryDispositionState::Applying => 2,
                            recovery_disposition::IlmRecoveryDispositionState::Completed => u64::MAX,
                        };
                        distance >= minimum_distance
                    })
                    && sorted_sha256_set_is_subset(previous_confirmed, terminal_confirmed)
            }
            _ => false,
        }
    }
}

fn recovery_disposition_checkpoint_is_valid(checkpoint: &DurableIlmRecordCheckpoint) -> bool {
    use recovery_disposition::IlmRecoveryDispositionState::{Applying, Completed, Prepared};

    let DurableIlmRecordCheckpoint::RecoveryDisposition {
        content_sha256,
        identity_sha256,
        copy_manifest_sha256,
        copy_manifest_count,
        created_at_unix_nanos,
        revision,
        state,
        owner_fence_sha256,
        owner_lease_acquired_at_unix_nanos,
        owner_lease_expires_at_unix_nanos,
        confirmed_absent_sha256,
        retain_until_unix_nanos,
    } = checkpoint
    else {
        return true;
    };
    let owner_fence_sha256 = owner_fence_sha256.as_deref();

    is_canonical_sha256(content_sha256)
        && is_canonical_sha256(identity_sha256)
        && is_canonical_sha256(copy_manifest_sha256)
        && *copy_manifest_count > 0
        && *created_at_unix_nanos > 0
        && *revision > 0
        && *retain_until_unix_nanos > 0
        && owner_fence_sha256.is_none_or(is_canonical_sha256)
        && match (
            owner_fence_sha256,
            *owner_lease_acquired_at_unix_nanos,
            *owner_lease_expires_at_unix_nanos,
        ) {
            (None, None, None) => true,
            (Some(_), Some(acquired), Some(expires)) => acquired > 0 && expires > acquired,
            _ => false,
        }
        && confirmed_absent_sha256.len() <= *copy_manifest_count
        && confirmed_absent_sha256.iter().all(|digest| is_canonical_sha256(digest))
        && confirmed_absent_sha256.windows(2).all(|pair| pair[0] < pair[1])
        && match *state {
            Prepared => confirmed_absent_sha256.is_empty(),
            Applying => owner_fence_sha256.is_some(),
            Completed => owner_fence_sha256.is_none() && confirmed_absent_sha256.len() == *copy_manifest_count,
        }
}

fn is_canonical_sha256(value: &str) -> bool {
    is_sha256_checksum(value)
        && !value
            .bytes()
            .any(|byte| byte.is_ascii_hexdigit() && byte.is_ascii_uppercase())
}

fn sorted_sha256_set_is_subset(subset: &[String], superset: &[String]) -> bool {
    subset.iter().all(|candidate| superset.binary_search(candidate).is_ok())
}

fn tier_delete_dispatch_parent_progress_delta(
    previous_sequence: u64,
    previous_completed_journals: u64,
    next_sequence: u64,
    next_completed_journals: u64,
) -> Option<(u64, u64)> {
    let sequence_delta = next_sequence.checked_sub(previous_sequence)?;
    let completed_journal_delta = next_completed_journals.checked_sub(previous_completed_journals)?;
    (sequence_delta <= completed_journal_delta).then_some((sequence_delta, completed_journal_delta))
}

fn transition_state_distance(
    from: transition_transaction::TransitionTransactionState,
    to: transition_transaction::TransitionTransactionState,
) -> Option<u64> {
    use transition_transaction::TransitionTransactionState::{
        AbortedNoRemote, CleanupPending, Committed, LocalCommitStarted, UploadOutcomeUnknown, UploadStarted, Uploaded,
    };

    match (from, to) {
        (UploadStarted, UploadOutcomeUnknown | AbortedNoRemote | Uploaded) => Some(1),
        (UploadStarted, LocalCommitStarted | CleanupPending) => Some(2),
        (UploadStarted, Committed) => Some(3),
        (UploadOutcomeUnknown, Uploaded | CleanupPending) => Some(1),
        (UploadOutcomeUnknown, LocalCommitStarted) => Some(2),
        (UploadOutcomeUnknown, Committed) => Some(3),
        (Uploaded, LocalCommitStarted | CleanupPending) => Some(1),
        (Uploaded, Committed) => Some(2),
        (LocalCommitStarted, Committed | CleanupPending) => Some(1),
        _ => None,
    }
}

fn transition_state_revision_is_successor(
    from: transition_transaction::TransitionTransactionState,
    from_revision: u64,
    to: transition_transaction::TransitionTransactionState,
    to_revision: u64,
) -> bool {
    use transition_transaction::TransitionTransactionState::{LocalCommitStarted, UploadOutcomeUnknown};

    if from == UploadOutcomeUnknown && from_revision == 1 && to == LocalCommitStarted {
        return to_revision == 2;
    }
    transition_state_distance(from, to)
        .and_then(|distance| from_revision.checked_add(distance))
        .is_some_and(|expected_revision| to_revision == expected_revision)
}

fn tier_probe_state_reaches(
    from: tier_probe_intent::TierProbeIntentState,
    to: tier_probe_intent::TierProbeIntentState,
    revision_distance: u64,
) -> bool {
    use tier_probe_intent::TierProbeIntentState::{AbortedNoRemote, CleanupPending, Completed, UploadOutcomeUnknown, Uploaded};

    match (from, to) {
        (UploadOutcomeUnknown, Uploaded | CleanupPending | AbortedNoRemote) => revision_distance == 1,
        (UploadOutcomeUnknown, Completed) => matches!(revision_distance, 2 | 3),
        (Uploaded, CleanupPending) => revision_distance == 1,
        (Uploaded, Completed) => revision_distance == 2,
        (CleanupPending, Completed) => revision_distance == 1,
        _ => false,
    }
}

fn manual_job_state_reaches(
    from: manual_transition_job::ManualTransitionJobState,
    to: manual_transition_job::ManualTransitionJobState,
) -> bool {
    from == to || from == manual_transition_job::ManualTransitionJobState::Running
}

impl ManualTransitionJobProgressProof {
    fn new(
        report: &ManualTransitionRunReport,
        queue_snapshot: &ManualTransitionQueueSnapshot,
        cursor_revision: Option<u64>,
    ) -> Result<Self> {
        let cursor_revision = cursor_revision.or_else(|| manual_transition_job::manual_transition_cursor_revision(report));
        let cursor_marker = match report.continuation_token.as_deref() {
            Some(token) => {
                let marker = decode_manual_transition_continuation_token(token)?
                    .0
                    .ok_or_else(|| Error::other("durable ILM manual transition cursor marker is missing"))?;
                (marker.len() <= MANUAL_TRANSITION_CURSOR_MARKER_PROOF_MAX_SIZE).then_some(marker)
            }
            None => None,
        };
        if !manual_job_worker_results_are_valid(report)
            || !manual_job_queue_snapshot_is_valid(queue_snapshot)
            || (report.continuation_token.is_some() && cursor_revision == Some(0))
        {
            return Err(Error::other("durable ILM manual transition progress is invalid"));
        }
        Ok(Self {
            scope_sha256: checkpoint_hash(&(report.bucket.as_str(), report.prefix.as_str(), &report.tier, report.dry_run))?,
            scanned: report.scanned,
            eligible: report.eligible,
            enqueued: report.enqueued,
            dry_run_eligible: report.dry_run_eligible,
            skipped_not_transition: report.skipped_not_transition,
            skipped_tier: report.skipped_tier,
            skipped_delete_marker: report.skipped_delete_marker,
            skipped_directory: report.skipped_directory,
            skipped_replication: report.skipped_replication,
            skipped_already_transitioned: report.skipped_already_transitioned,
            skipped_already_in_flight: report.skipped_already_in_flight,
            skipped_queue_full: report.skipped_queue_full,
            skipped_queue_closed: report.skipped_queue_closed,
            skipped_queue_timeout: report.skipped_queue_timeout,
            transition_completed: report.transition_completed,
            transition_failed: report.transition_failed,
            tier_failure: report.tier_failure,
            tier_failure_by_reason: report.tier_failure_by_reason.clone(),
            lifecycle_config_found: report.lifecycle_config_found,
            truncated_by_limit: report.truncated_by_limit,
            truncated_by_duration: report.truncated_by_duration,
            cancelled: report.cancelled,
            continuation_token_sha256: report
                .continuation_token
                .as_deref()
                .map(|token| hex_sha256(token.as_bytes(), ToOwned::to_owned)),
            cursor_marker,
            cursor_revision,
            queue_snapshot: *queue_snapshot,
        })
    }

    fn is_valid(&self) -> bool {
        let reason_total = self
            .tier_failure_by_reason
            .values()
            .try_fold(0u64, |total, count| total.checked_add(*count));
        is_sha256_checksum(&self.scope_sha256)
            && self.continuation_token_sha256.as_deref().is_none_or(is_sha256_checksum)
            && match (&self.continuation_token_sha256, &self.cursor_marker) {
                (None, None) | (Some(_), None) => true,
                (Some(_), Some(marker)) => !marker.is_empty() && marker.len() <= MANUAL_TRANSITION_CURSOR_MARKER_PROOF_MAX_SIZE,
                (None, Some(_)) => false,
            }
            && !(self.continuation_token_sha256.is_some() && self.cursor_revision == Some(0))
            && self
                .transition_completed
                .checked_add(self.transition_failed)
                .is_some_and(|total| total <= self.enqueued)
            && self.transition_failed <= self.tier_failure
            && reason_total.is_some_and(|total| total <= self.tier_failure)
            && manual_job_queue_snapshot_is_valid(&self.queue_snapshot)
    }
}

fn manual_job_progress_checkpoint_is_valid(
    progress: Option<&ManualTransitionJobProgressCheckpoint>,
    proof: Option<&ManualTransitionJobProgressProof>,
) -> bool {
    match (progress, proof) {
        (Some(progress), None) => manual_job_progress_is_valid(progress),
        (None, Some(proof)) => proof.is_valid(),
        (None, None) => true,
        (Some(_), Some(_)) => false,
    }
}

fn manual_job_progress_proof(
    progress: Option<&ManualTransitionJobProgressCheckpoint>,
    proof: Option<&ManualTransitionJobProgressProof>,
) -> Option<ManualTransitionJobProgressProof> {
    match (progress, proof) {
        (Some(progress), None) => ManualTransitionJobProgressProof::new(&progress.report, &progress.queue_snapshot, None).ok(),
        (None, Some(proof)) if proof.is_valid() => Some(proof.clone()),
        _ => None,
    }
}

fn manual_job_progress_equivalent(
    previous: Option<&ManualTransitionJobProgressCheckpoint>,
    previous_proof: Option<&ManualTransitionJobProgressProof>,
    next: Option<&ManualTransitionJobProgressCheckpoint>,
    next_proof: Option<&ManualTransitionJobProgressProof>,
) -> bool {
    if !manual_job_progress_checkpoint_is_valid(previous, previous_proof)
        || !manual_job_progress_checkpoint_is_valid(next, next_proof)
    {
        return false;
    }
    match (
        manual_job_progress_proof(previous, previous_proof),
        manual_job_progress_proof(next, next_proof),
    ) {
        (Some(previous), Some(next)) => previous == next,
        (None, None) => true,
        _ => false,
    }
}

fn manual_job_progress_reaches(
    previous: Option<&ManualTransitionJobProgressCheckpoint>,
    previous_proof: Option<&ManualTransitionJobProgressProof>,
    next: Option<&ManualTransitionJobProgressCheckpoint>,
    next_proof: Option<&ManualTransitionJobProgressProof>,
    next_scan_completed: bool,
) -> bool {
    if previous.is_none() && previous_proof.is_none() {
        return (next.is_some() || next_proof.is_some()) && manual_job_progress_checkpoint_is_valid(next, next_proof);
    }
    let (Some(previous_compact), Some(next_compact)) = (
        manual_job_progress_proof(previous, previous_proof),
        manual_job_progress_proof(next, next_proof),
    ) else {
        return false;
    };

    macro_rules! counters_do_not_regress {
        ($($field:ident),+ $(,)?) => {
            $(previous_compact.$field <= next_compact.$field)&&+
        };
    }

    let counters_monotonic = counters_do_not_regress!(
        scanned,
        eligible,
        enqueued,
        dry_run_eligible,
        skipped_not_transition,
        skipped_tier,
        skipped_delete_marker,
        skipped_directory,
        skipped_replication,
        skipped_already_transitioned,
        skipped_already_in_flight,
        skipped_queue_full,
        skipped_queue_closed,
        skipped_queue_timeout,
        transition_completed,
        transition_failed,
        tier_failure,
    );
    let failure_reasons_monotonic = previous_compact
        .tier_failure_by_reason
        .iter()
        .all(|(reason, previous_count)| {
            next_compact
                .tier_failure_by_reason
                .get(reason)
                .is_some_and(|next_count| next_count >= previous_count)
        });
    let flags_monotonic = (!previous_compact.lifecycle_config_found || next_compact.lifecycle_config_found)
        && (!previous_compact.truncated_by_limit || next_compact.truncated_by_limit)
        && (!previous_compact.truncated_by_duration || next_compact.truncated_by_duration)
        && (!previous_compact.cancelled || next_compact.cancelled);
    let cursor_monotonic = manual_job_cursor_reaches(
        &previous_compact,
        &next_compact,
        previous.map(|progress| &progress.report),
        next.map(|progress| &progress.report),
        next_scan_completed,
    );

    previous_compact.scope_sha256 == next_compact.scope_sha256
        && counters_monotonic
        && failure_reasons_monotonic
        && flags_monotonic
        && cursor_monotonic
        && previous_compact.is_valid()
        && next_compact.is_valid()
}

fn manual_job_progress_is_valid(progress: &ManualTransitionJobProgressCheckpoint) -> bool {
    manual_job_worker_results_are_valid(&progress.report)
        && manual_job_queue_snapshot_is_valid(&progress.queue_snapshot)
        && manual_job_cursor_is_valid(progress.report.continuation_token.as_deref())
}

fn manual_job_worker_results_are_valid(report: &ManualTransitionRunReport) -> bool {
    let reason_total = report
        .tier_failure_by_reason
        .values()
        .try_fold(0u64, |total, count| total.checked_add(*count));
    report
        .transition_completed
        .checked_add(report.transition_failed)
        .is_some_and(|total| total <= report.enqueued)
        && report.transition_failed <= report.tier_failure
        && reason_total.is_some_and(|total| total <= report.tier_failure)
}

fn manual_job_cursor_reaches(
    previous: &ManualTransitionJobProgressProof,
    next: &ManualTransitionJobProgressProof,
    previous_legacy: Option<&ManualTransitionRunReport>,
    next_legacy: Option<&ManualTransitionRunReport>,
    next_scan_completed: bool,
) -> bool {
    if previous.continuation_token_sha256 == next.continuation_token_sha256 {
        return previous.cursor_marker == next.cursor_marker && previous.cursor_revision == next.cursor_revision;
    }
    if let (Some(previous_marker), Some(next_marker)) = (&previous.cursor_marker, &next.cursor_marker)
        && previous_marker != next_marker
    {
        return next.scanned > previous.scanned && next_marker > previous_marker;
    }
    match (&previous.continuation_token_sha256, &next.continuation_token_sha256) {
        (None, Some(_)) => {
            next.scanned > previous.scanned
                && (manual_job_cursor_revision_advances(previous.cursor_revision, next.cursor_revision)
                    || (previous.cursor_revision.is_none() && next.cursor_revision.is_none()))
        }
        (Some(_), None) => next_scan_completed,
        (Some(_), Some(_)) if next.scanned > previous.scanned => {
            manual_job_cursor_revision_advances(previous.cursor_revision, next.cursor_revision)
                || manual_job_legacy_cursor_reaches(previous, next, previous_legacy, next_legacy)
        }
        _ => false,
    }
}

fn manual_job_cursor_revision_advances(previous: Option<u64>, next: Option<u64>) -> bool {
    match (previous, next) {
        (Some(previous), Some(next)) => next > previous,
        (None, Some(next)) => next > 0,
        _ => false,
    }
}

fn manual_job_legacy_cursor_reaches(
    previous_proof: &ManualTransitionJobProgressProof,
    next_proof: &ManualTransitionJobProgressProof,
    previous_legacy: Option<&ManualTransitionRunReport>,
    next_legacy: Option<&ManualTransitionRunReport>,
) -> bool {
    if let (Some(previous_marker), Some(next_marker)) = (&previous_proof.cursor_marker, &next_proof.cursor_marker) {
        return next_marker > previous_marker;
    }
    let (Some(previous_token), Some(next_token)) = (
        previous_legacy.and_then(|report| report.continuation_token.as_deref()),
        next_legacy.and_then(|report| report.continuation_token.as_deref()),
    ) else {
        return false;
    };
    let (Ok((Some(previous_marker), _)), Ok((Some(next_marker), _))) = (
        decode_manual_transition_continuation_token(previous_token),
        decode_manual_transition_continuation_token(next_token),
    ) else {
        return false;
    };
    next_marker > previous_marker
}

fn manual_job_cursor_is_valid(token: Option<&str>) -> bool {
    let Some(token) = token else {
        return true;
    };
    matches!(decode_manual_transition_continuation_token(token), Ok((Some(_), _)))
}

fn manual_job_queue_snapshot_is_valid(snapshot: &ManualTransitionQueueSnapshot) -> bool {
    (snapshot.queue_capacity > 0 || snapshot.queued == 0)
        && (snapshot.queue_capacity == 0 || snapshot.queued <= snapshot.queue_capacity)
        && (snapshot.workers > 0 || snapshot.active == 0)
        && (snapshot.workers == 0 || snapshot.active <= snapshot.workers)
}

fn checkpoint_hash<T: Serialize>(value: &T) -> Result<String> {
    let encoded = serde_json::to_vec(value).map_err(Error::other)?;
    Ok(hex_sha256(&encoded, ToOwned::to_owned))
}

fn path_is_in_namespace(path: &str, namespace: &DurableIlmNamespace) -> bool {
    let Some(suffix) = path.strip_prefix(namespace.prefix) else {
        return false;
    };
    if namespace.prefix.ends_with('/') {
        !suffix.is_empty()
    } else {
        suffix.starts_with('/') && suffix.len() > 1
    }
}

pub(crate) fn classify_durable_ilm_record(path: &str) -> Result<Option<&'static DurableIlmNamespace>> {
    if path != ILM_META_PREFIX && !path.starts_with(ILM_META_OBJECT_PREFIX) {
        return Ok(None);
    }

    DURABLE_ILM_NAMESPACES
        .iter()
        .find(|namespace| path_is_in_namespace(path, namespace))
        .map(Some)
        .ok_or_else(|| Error::other(format!("unregistered durable ILM namespace for path `{path}`")))
}

fn parse_manual_sharded_record(path: &str, prefix: &str) -> Result<(Uuid, String)> {
    let suffix = path
        .strip_prefix(prefix)
        .and_then(|suffix| suffix.strip_prefix('/'))
        .ok_or_else(|| Error::other("manual transition record path has wrong prefix"))?;
    let mut parts = suffix.split('/');
    let first = parts
        .next()
        .ok_or_else(|| Error::other("manual transition record first shard is missing"))?;
    let second = parts
        .next()
        .ok_or_else(|| Error::other("manual transition record second shard is missing"))?;
    let job_key = parts
        .next()
        .ok_or_else(|| Error::other("manual transition record job id is missing"))?;
    let task_key = parts
        .next()
        .and_then(|file| file.strip_suffix(".json"))
        .ok_or_else(|| Error::other("manual transition record task key is missing"))?;
    if parts.next().is_some()
        || job_key.len() != 32
        || first != &job_key[..2]
        || second != &job_key[2..4]
        || !job_key
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(Error::other("manual transition record job id or shards are invalid"));
    }
    let job_id = Uuid::parse_str(job_key).map_err(|_| Error::other("manual transition record job id is invalid"))?;
    Ok((job_id, task_key.to_string()))
}

pub(crate) fn validate_durable_ilm_record(path: &str, data: &[u8]) -> Result<ValidatedDurableIlmRecord> {
    let namespace =
        classify_durable_ilm_record(path)?.ok_or_else(|| Error::other(format!("path `{path}` is not a durable ILM record")))?;
    if data.len() > namespace.max_record_size {
        return Err(Error::other(format!(
            "durable ILM record exceeds {} byte limit",
            namespace.max_record_size
        )));
    }

    let content_sha256 = hex_sha256(data, ToOwned::to_owned);
    let (id_kind, id, checkpoint) = match namespace.kind {
        DurableIlmRecordKind::TierDeleteJournal => {
            let entry = tier_delete_journal::decode_tier_delete_journal_entry(data)?;
            if tier_delete_journal::tier_delete_journal_object_name(&entry) != path {
                return Err(Error::other("tier delete journal content does not match its path"));
            }
            let legacy_operation_id = path
                .strip_prefix(namespace.prefix)
                .and_then(|suffix| suffix.strip_suffix(".json"))
                .ok_or_else(|| Error::other("tier delete journal path is invalid"))?;
            // Legacy v1-v5 paths already expose a 64-hex operation id and
            // must remain receipt-compatible. V6 uses an operation-scoped
            // nested path, so derive a fixed, path-unique receipt id instead
            // of embedding slashes in the receipt locator.
            let operation_id = if entry.persisted_version == 6 {
                hex_sha256(path.as_bytes(), ToOwned::to_owned)
            } else {
                legacy_operation_id.to_string()
            };
            let identity_sha256 = checkpoint_hash(&(
                &entry.obj_name,
                &entry.version_id,
                &entry.tier_name,
                entry.backend_identity,
                entry.version_id_exact,
                entry.version_state,
                &entry.source,
            ))?;
            let dispatch_identity_sha256 = entry.dispatch.as_ref().map(checkpoint_hash).transpose()?;
            (
                "operation_id",
                operation_id,
                DurableIlmRecordCheckpoint::TierDeleteJournal {
                    content_sha256,
                    identity_sha256,
                    committed: entry.state == super::tier_sweeper::TierDeleteJournalState::Committed,
                    dispatch_identity_sha256,
                    state: (entry.persisted_version == 6).then_some(entry.state),
                },
            )
        }
        DurableIlmRecordKind::TierDeleteDispatchManifest => {
            match tier_delete_journal::validate_tier_delete_dispatch_manifest_record(path, data)? {
                tier_delete_journal::TierDeleteDispatchDurableRecord::Manifest {
                    operation_id,
                    identity_sha256,
                    state,
                } => (
                    "operation_id",
                    hex_sha256(operation_id.as_bytes(), ToOwned::to_owned),
                    DurableIlmRecordCheckpoint::TierDeleteDispatchManifest {
                        content_sha256,
                        identity_sha256,
                        state,
                    },
                ),
                tier_delete_journal::TierDeleteDispatchDurableRecord::Parent {
                    operation_id,
                    identity_sha256,
                    revision,
                    next_chunk_sequence,
                    completed_journal_count,
                    active_chunk_identity_sha256,
                    completed,
                } => (
                    "operation_id",
                    hex_sha256(operation_id.as_bytes(), ToOwned::to_owned),
                    DurableIlmRecordCheckpoint::TierDeleteDispatchParent {
                        content_sha256,
                        identity_sha256,
                        revision,
                        next_chunk_sequence,
                        completed_journal_count,
                        active_chunk_identity_sha256,
                        completed,
                    },
                ),
            }
        }
        DurableIlmRecordKind::TransitionTransaction => {
            let transaction = transition_transaction::decode_transition_transaction_record(path, data)
                .map_err(|err| Error::other(err.to_string()))?;
            let identity_sha256 = checkpoint_hash(&(
                transaction.deployment_id,
                transaction.transaction_id,
                transaction.owner_epoch,
                transaction.write_id,
                &transaction.source,
                &transaction.tier_name,
                transaction.backend_fingerprint,
                &transaction.remote_object,
                transaction.not_after_unix_nanos,
            ))?;
            let remote_version_sha256 = checkpoint_hash(&transaction.remote_version)?;
            (
                "transaction_id",
                transaction.transaction_id.to_string(),
                DurableIlmRecordCheckpoint::TransitionTransaction {
                    content_sha256,
                    identity_sha256,
                    remote_version_sha256,
                    remote_version_known: !transaction.remote_version.is_unknown(),
                    revision: transaction.revision,
                    state: transaction.state,
                },
            )
        }
        DurableIlmRecordKind::TierProbeIntent => {
            let probe_id = tier_probe_intent::tier_probe_intent_id_from_record_object_name(path)
                .map_err(|err| Error::other(err.to_string()))?;
            let intent =
                tier_probe_intent::TierProbeIntent::decode(probe_id, data).map_err(|err| Error::other(err.to_string()))?;
            let canonical =
                tier_probe_intent::tier_probe_intent_record_object_name(probe_id).map_err(|err| Error::other(err.to_string()))?;
            if canonical != path {
                return Err(Error::other("tier probe intent path is not canonical"));
            }
            let identity_sha256 = checkpoint_hash(&(
                intent.probe_id,
                &intent.operation,
                &intent.tier_name,
                intent.destination_id,
                &intent.probe_object,
                &intent.creator_id,
                intent.creator_epoch,
                intent.created_at_unix_nanos,
            ))?;
            let remote_version_sha256 = checkpoint_hash(&intent.remote_version)?;
            let owner_fence_sha256 = checkpoint_hash(&intent.owner)?;
            (
                "probe_id",
                probe_id.to_string(),
                DurableIlmRecordCheckpoint::TierProbeIntent {
                    content_sha256,
                    identity_sha256,
                    remote_version_sha256,
                    remote_version_known: !intent.remote_version.is_unknown(),
                    owner_fence_sha256,
                    revision: intent.revision,
                    state: intent.state,
                },
            )
        }
        DurableIlmRecordKind::RecoveryControl => {
            let (protocol, control_id) = recovery_control::recovery_control_id_from_record_object_name(path)
                .map_err(|err| Error::other(err.to_string()))?;
            let control =
                recovery_control::IlmRecoveryControl::decode(&control_id, data).map_err(|err| Error::other(err.to_string()))?;
            let canonical = recovery_control::recovery_control_record_object_name(protocol, &control_id)
                .map_err(|err| Error::other(err.to_string()))?;
            if canonical != path || control.identity.protocol != protocol {
                return Err(Error::other("ILM recovery control path is not canonical"));
            }
            let identity_sha256 = checkpoint_hash(&control.identity)?;
            let source_generation_sha256 = checkpoint_hash(&control.observed_source_generation)?;
            let owner_fence_sha256 = control.owner.as_ref().map(checkpoint_hash).transpose()?;
            (
                "control_id",
                control_id,
                DurableIlmRecordCheckpoint::RecoveryControl {
                    content_sha256,
                    identity_sha256,
                    source_generation_sha256,
                    first_seen_at_unix_nanos: control.first_seen_at_unix_nanos,
                    revision: control.revision,
                    classification: control.classification,
                    attempt_count: control.attempt_count,
                    consecutive_failure_count: control.consecutive_failure_count,
                    owner_fence_sha256,
                },
            )
        }
        DurableIlmRecordKind::RecoveryExport => {
            let (protocol, export_id) = recovery_export::recovery_export_id_from_record_object_name(path)?;
            let export = recovery_export::IlmRecoveryExport::decode(&export_id, data)?;
            let canonical = recovery_export::recovery_export_record_object_name(protocol, &export_id)?;
            if canonical != path || export.protocol != protocol {
                return Err(Error::other("ILM recovery export path is not canonical"));
            }
            let source_generation_sha256 = checkpoint_hash(&export.source_generation)?;
            (
                "export_id",
                export_id,
                DurableIlmRecordCheckpoint::RecoveryExport {
                    content_sha256,
                    source_generation_sha256,
                    topology_generation: export.topology_generation,
                    member_epochs_sha256: export.member_epochs_sha256,
                    creator_sha256: export.creator_sha256,
                    retain_until_unix_nanos: export.retain_until_unix_nanos,
                },
            )
        }
        DurableIlmRecordKind::RecoveryDisposition => {
            // The disposition module owns strict schema, checksum, canonical
            // path, immutable-manifest, and state-specific validation. Keep
            // this boundary limited to decommission identity/checkpoint
            // projection so the two readers cannot accept different records.
            let disposition = recovery_disposition::decode_recovery_disposition_checkpoint(path, data)?;
            if disposition.content_sha256 != content_sha256 {
                return Err(Error::other("ILM recovery disposition checkpoint content digest is invalid"));
            }
            (
                "disposition_id",
                disposition.disposition_id,
                DurableIlmRecordCheckpoint::RecoveryDisposition {
                    content_sha256: disposition.content_sha256,
                    identity_sha256: disposition.identity_sha256,
                    copy_manifest_sha256: disposition.copy_manifest_sha256,
                    copy_manifest_count: disposition.copy_manifest_count,
                    created_at_unix_nanos: disposition.created_at_unix_nanos,
                    revision: disposition.revision,
                    state: disposition.state,
                    owner_fence_sha256: disposition.owner_fence_sha256,
                    owner_lease_acquired_at_unix_nanos: disposition.owner_lease_acquired_at_unix_nanos,
                    owner_lease_expires_at_unix_nanos: disposition.owner_lease_expires_at_unix_nanos,
                    confirmed_absent_sha256: disposition.confirmed_absent_sha256,
                    retain_until_unix_nanos: disposition.retain_until_unix_nanos,
                },
            )
        }
        DurableIlmRecordKind::ManualTransitionJob => {
            let job_id = manual_transition_job::manual_transition_job_id_from_record_object_name(path)
                .map_err(|err| Error::other(err.to_string()))?;
            let canonical = manual_transition_job::manual_transition_job_record_object_name(job_id)
                .map_err(|err| Error::other(err.to_string()))?;
            if canonical != path {
                return Err(Error::other("manual transition job path is not canonical"));
            }
            let job = manual_transition_job::ManualTransitionJobRecord::decode(job_id, data)
                .map_err(|err| Error::other(err.to_string()))?;
            let identity_sha256 = checkpoint_hash(&(
                job.job_id,
                &job.scope_key,
                &job.bucket,
                &job.prefix,
                &job.tier,
                job.dry_run,
                job.max_objects,
                job.max_duration,
                job.created_at_unix_nanos,
            ))?;
            let progress_proof = ManualTransitionJobProgressProof::new(&job.report, &job.queue_snapshot, job.cursor_revision)?;
            let updated_at_unix_nanos = i64::try_from(job.updated_at_unix_nanos)
                .map_err(|_| Error::other("manual transition job updated_at exceeds durable ILM checkpoint range"))?;
            (
                "job_id",
                job_id.to_string(),
                DurableIlmRecordCheckpoint::ManualTransitionJob {
                    content_sha256,
                    identity_sha256,
                    updated_at_unix_nanos,
                    state: job.state,
                    scan_completed: job.scan_completed,
                    cancel_requested: job.cancel_requested,
                    progress: None,
                    progress_proof: Some(Box::new(progress_proof)),
                },
            )
        }
        DurableIlmRecordKind::ManualTransitionScope => {
            let admission: manual_transition_job::ManualTransitionScopeAdmission =
                serde_json::from_slice(data).map_err(Error::other)?;
            admission.validate().map_err(|err| Error::other(err.to_string()))?;
            let canonical = manual_transition_job::manual_transition_scope_record_object_name(&admission.scope_key)
                .map_err(|err| Error::other(err.to_string()))?;
            if canonical != path {
                return Err(Error::other("manual transition scope content does not match its path"));
            }
            let identity_sha256 = checkpoint_hash(&(
                &admission.schema,
                &admission.scope_key,
                admission.job_id,
                &admission.bucket,
                &admission.prefix,
                &admission.tier,
                admission.dry_run,
            ))?;
            let updated_at_unix_nanos = i64::try_from(admission.updated_at_unix_nanos)
                .map_err(|_| Error::other("manual transition scope updated_at exceeds durable ILM checkpoint range"))?;
            (
                "job_id",
                admission.job_id.to_string(),
                DurableIlmRecordCheckpoint::ManualTransitionScope {
                    content_sha256,
                    identity_sha256,
                    updated_at_unix_nanos,
                },
            )
        }
        DurableIlmRecordKind::ManualTransitionTask => {
            let (job_id, task_key) = parse_manual_sharded_record(path, namespace.prefix)?;
            let canonical = manual_transition_job::manual_transition_task_object_name(job_id, &task_key)
                .map_err(|err| Error::other(err.to_string()))?;
            if canonical != path {
                return Err(Error::other("manual transition task path is not canonical"));
            }
            manual_transition_job::ManualTransitionTaskRecord::decode(job_id, &task_key, data)
                .map_err(|err| Error::other(err.to_string()))?;
            (
                "job_id",
                job_id.to_string(),
                DurableIlmRecordCheckpoint::ManualTransitionTask { content_sha256 },
            )
        }
        DurableIlmRecordKind::ManualTransitionWorkerResult => {
            let (job_id, task_key) = parse_manual_sharded_record(path, namespace.prefix)?;
            let canonical = manual_transition_job::manual_transition_worker_result_object_name(job_id, &task_key)
                .map_err(|err| Error::other(err.to_string()))?;
            if canonical != path {
                return Err(Error::other("manual transition worker result path is not canonical"));
            }
            manual_transition_job::ManualTransitionWorkerResultRecord::decode(job_id, &task_key, data)
                .map_err(|err| Error::other(err.to_string()))?;
            (
                "job_id",
                job_id.to_string(),
                DurableIlmRecordCheckpoint::ManualTransitionWorkerResult { content_sha256 },
            )
        }
    };

    Ok(ValidatedDurableIlmRecord {
        namespace: namespace.name,
        id_kind,
        id,
        checkpoint,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn try_manual_job_checkpoint(job: &manual_transition_job::ManualTransitionJobRecord) -> Result<DurableIlmRecordCheckpoint> {
        let path =
            manual_transition_job::manual_transition_job_record_object_name(job.job_id).expect("manual job path should build");
        let encoded = job.encode().map_err(|err| Error::other(err.to_string()))?;
        Ok(validate_durable_ilm_record(&path, &encoded)?.checkpoint)
    }

    fn manual_job_checkpoint(job: &manual_transition_job::ManualTransitionJobRecord) -> DurableIlmRecordCheckpoint {
        try_manual_job_checkpoint(job).expect("manual job checkpoint should validate")
    }

    fn continuation_token_with_version(marker: &str, version_marker: Option<&str>) -> String {
        let encoded = serde_json::to_vec(&serde_json::json!({ "marker": marker, "version_marker": version_marker }))
            .expect("continuation token should encode");
        base64_simd::URL_SAFE_NO_PAD.encode_to_string(&encoded)
    }

    fn continuation_token(marker: &str) -> String {
        continuation_token_with_version(marker, None)
    }

    #[test]
    fn unknown_ilm_record_requires_namespace_registration() {
        let err = classify_durable_ilm_record("ilm/future-durable/jobs/one.json")
            .expect_err("unknown durable ILM path must fail closed");

        assert!(err.to_string().contains("ilm/future-durable/jobs/one.json"));
    }

    #[test]
    fn durable_ilm_registry_has_unique_non_overlapping_prefixes() {
        for (index, namespace) in DURABLE_ILM_NAMESPACES.iter().enumerate() {
            assert!(namespace.prefix.starts_with(ILM_META_OBJECT_PREFIX));
            assert!(namespace.max_record_size > 0);
            for other in DURABLE_ILM_NAMESPACES.iter().skip(index + 1) {
                assert_ne!(namespace.prefix, other.prefix);
                assert!(!path_is_in_namespace(namespace.prefix, other));
                assert!(!path_is_in_namespace(other.prefix, namespace));
            }
        }
    }

    fn recovery_disposition_checkpoint(
        revision: u64,
        state: recovery_disposition::IlmRecoveryDispositionState,
        owner_fence: Option<&str>,
        confirmed_absent_sha256: Vec<String>,
    ) -> DurableIlmRecordCheckpoint {
        let (owner_lease_acquired_at_unix_nanos, owner_lease_expires_at_unix_nanos) = match owner_fence {
            Some("f") => (Some(10), Some(20)),
            Some(_) => (Some(1), Some(10)),
            None => (None, None),
        };
        DurableIlmRecordCheckpoint::RecoveryDisposition {
            content_sha256: format!("{revision:064x}"),
            identity_sha256: "a".repeat(64),
            copy_manifest_sha256: "d".repeat(64),
            copy_manifest_count: 2,
            created_at_unix_nanos: 1_700_000_000_000_000_000,
            revision,
            state,
            owner_fence_sha256: owner_fence.map(|digest| digest.repeat(64)),
            owner_lease_acquired_at_unix_nanos,
            owner_lease_expires_at_unix_nanos,
            confirmed_absent_sha256,
            retain_until_unix_nanos: 1_820_000_000_000_000_000,
        }
    }

    #[test]
    fn recovery_disposition_namespace_is_registered_without_shadowing_its_root() {
        let disposition_id = "a".repeat(64);
        let path = format!(
            "{}/tier_delete_journal/{}/{}/{}.json",
            recovery_disposition::ILM_RECOVERY_DISPOSITION_PREFIX,
            &disposition_id[..2],
            &disposition_id[2..4],
            disposition_id
        );
        let namespace = classify_durable_ilm_record(&path)
            .expect("recovery disposition path should classify")
            .expect("recovery disposition should be durable");

        assert_eq!(namespace, &RECOVERY_DISPOSITION_NAMESPACE);
        assert!(classify_durable_ilm_record(recovery_disposition::ILM_RECOVERY_DISPOSITION_PREFIX).is_err());
    }

    #[test]
    fn recovery_disposition_checkpoint_accepts_only_monotonic_progress() {
        use recovery_disposition::IlmRecoveryDispositionState::{Applying, Completed, Prepared};

        let first_copy = "b".repeat(64);
        let second_copy = "c".repeat(64);
        let prepared = recovery_disposition_checkpoint(1, Prepared, None, Vec::new());
        let claimed = recovery_disposition_checkpoint(2, Prepared, Some("e"), Vec::new());
        let applying = recovery_disposition_checkpoint(3, Applying, Some("e"), Vec::new());
        let first_absent = recovery_disposition_checkpoint(4, Applying, Some("e"), vec![first_copy.clone()]);
        let taken_over = recovery_disposition_checkpoint(5, Applying, Some("f"), vec![first_copy.clone()]);
        let all_absent = recovery_disposition_checkpoint(6, Applying, Some("f"), vec![first_copy.clone(), second_copy.clone()]);
        let completed = recovery_disposition_checkpoint(7, Completed, None, vec![first_copy.clone(), second_copy.clone()]);

        prepared
            .validate_successor(&claimed)
            .expect("Prepared should record an owner claim without absence progress");
        claimed
            .validate_successor(&applying)
            .expect("Prepared should advance to Applying without folding in deletion progress");
        applying
            .validate_successor(&first_absent)
            .expect("Applying should append newly confirmed absent copies");
        first_absent
            .validate_successor(&taken_over)
            .expect("Applying should record a fenced owner takeover without losing progress");
        taken_over
            .validate_successor(&all_absent)
            .expect("Applying should preserve every earlier confirmation while making progress");
        all_absent
            .validate_successor(&completed)
            .expect("a fully confirmed manifest should advance to Completed");

        assert!(
            prepared.validate_successor(&completed).is_err(),
            "adjacent receipt updates must not skip Applying"
        );
        assert!(
            first_absent
                .validate_successor(&recovery_disposition_checkpoint(5, Applying, Some("e"), Vec::new()))
                .is_err(),
            "confirmed-absent progress must not move backwards"
        );
        assert!(
            applying
                .validate_successor(&recovery_disposition_checkpoint(4, Completed, None, vec![first_copy.clone()]))
                .is_err(),
            "Completed must cover the complete immutable copy manifest"
        );
        assert!(
            completed
                .validate_successor(&recovery_disposition_checkpoint(7, Applying, Some("e"), vec![second_copy]))
                .is_err(),
            "Completed is terminal"
        );
        assert!(
            first_absent
                .validate_successor(&recovery_disposition_checkpoint(5, Applying, Some("e"), vec![first_copy.clone()]))
                .is_err(),
            "a same-state revision bump must change the owner fence or absence progress"
        );
        assert!(
            applying
                .validate_successor(&recovery_disposition_checkpoint(4, Applying, None, vec![first_copy]))
                .is_err(),
            "Applying must retain a fenced owner"
        );

        let mut noncanonical_identity = claimed.clone();
        if let DurableIlmRecordCheckpoint::RecoveryDisposition { identity_sha256, .. } = &mut noncanonical_identity {
            *identity_sha256 = "A".repeat(64);
        }
        assert!(prepared.validate_successor(&noncanonical_identity).is_err());
        let mut changed_created_at = claimed;
        if let DurableIlmRecordCheckpoint::RecoveryDisposition {
            created_at_unix_nanos, ..
        } = &mut changed_created_at
        {
            *created_at_unix_nanos += 1;
        }
        assert!(prepared.validate_successor(&changed_created_at).is_err());

        let mut early_takeover = taken_over;
        if let DurableIlmRecordCheckpoint::RecoveryDisposition {
            owner_lease_acquired_at_unix_nanos,
            ..
        } = &mut early_takeover
        {
            *owner_lease_acquired_at_unix_nanos = Some(9);
        }
        assert!(first_absent.validate_successor(&early_takeover).is_err());
        assert!(
            applying
                .validate_successor(&recovery_disposition_checkpoint(
                    4,
                    Applying,
                    Some("e"),
                    vec!["c".repeat(64), "b".repeat(64)],
                ))
                .is_err(),
            "confirmed-absent entries must be a canonical sorted set"
        );
    }

    #[test]
    fn recovery_disposition_terminal_predecessor_requires_exact_identity_and_full_manifest() {
        use recovery_disposition::IlmRecoveryDispositionState::{Applying, Completed, Prepared};

        let first_copy = "b".repeat(64);
        let second_copy = "c".repeat(64);
        let prepared = recovery_disposition_checkpoint(1, Prepared, None, Vec::new());
        let applying = recovery_disposition_checkpoint(3, Applying, Some("e"), vec![first_copy.clone()]);
        let completed = recovery_disposition_checkpoint(5, Completed, None, vec![first_copy.clone(), second_copy]);

        assert!(prepared.is_predecessor_of_terminal(&completed));
        assert!(applying.is_predecessor_of_terminal(&completed));
        assert!(
            !prepared.is_predecessor_of_terminal(&recovery_disposition_checkpoint(2, Applying, Some("e"), Vec::new())),
            "a nonterminal disposition must not authorize terminal cleanup"
        );
        assert!(
            !prepared.is_predecessor_of_terminal(&recovery_disposition_checkpoint(
                4,
                Completed,
                None,
                vec![first_copy.clone(), "c".repeat(64)],
            )),
            "terminal proof must leave enough revisions for claim, apply, progress, and completion"
        );
        assert!(
            !applying.is_predecessor_of_terminal(&recovery_disposition_checkpoint(
                4,
                Completed,
                None,
                vec![first_copy.clone(), "c".repeat(64)],
            )),
            "an incomplete Applying checkpoint cannot complete without a progress generation"
        );

        let mut other_identity = completed;
        if let DurableIlmRecordCheckpoint::RecoveryDisposition { identity_sha256, .. } = &mut other_identity {
            *identity_sha256 = "e".repeat(64);
        }
        assert!(!prepared.is_predecessor_of_terminal(&other_identity));

        let incomplete_terminal = recovery_disposition_checkpoint(4, Completed, None, vec![first_copy]);
        assert!(
            !prepared.is_predecessor_of_terminal(&incomplete_terminal),
            "a partial confirmed-absent set must not become terminal proof"
        );
    }

    fn tier_probe_intent_fixture() -> tier_probe_intent::TierProbeIntent {
        let probe_id = Uuid::parse_str("36e2220e-9ad2-495b-b3bc-c4d2caf70a31").expect("fixture uuid should parse");
        tier_probe_intent::TierProbeIntent {
            probe_id,
            revision: 1,
            state: tier_probe_intent::TierProbeIntentState::UploadOutcomeUnknown,
            operation: tier_probe_intent::TierProbeOperationIdentity::Verify {
                config_etag: "config-etag".to_string(),
                backend_identity: [1; 32],
            },
            tier_name: "COLD-A".to_string(),
            destination_id: [1; 32],
            probe_object: tier_probe_intent::tier_probe_object_name(probe_id),
            creator_id: "node-a".to_string(),
            creator_epoch: Uuid::parse_str("76746062-c05a-40b7-9e38-d2722d7e0332").expect("fixture creator epoch should parse"),
            created_at_unix_nanos: 1_780_000_000_000_000_000,
            owner: tier_probe_intent::TierProbeOwnerFence {
                owner_id: "node-a".to_string(),
                owner_epoch: Uuid::parse_str("76746062-c05a-40b7-9e38-d2722d7e0332").expect("fixture owner epoch should parse"),
                not_after_unix_nanos: 1_780_000_900_000_000_000,
            },
            remote_version: tier_probe_intent::TierProbeRemoteVersion::default(),
        }
    }

    fn tier_probe_checkpoint(intent: &tier_probe_intent::TierProbeIntent) -> DurableIlmRecordCheckpoint {
        let path =
            tier_probe_intent::tier_probe_intent_record_object_name(intent.probe_id).expect("tier probe path should build");
        let encoded = intent.encode().expect("tier probe intent should encode");
        let namespace = classify_durable_ilm_record(&path)
            .expect("tier probe namespace should classify")
            .expect("tier probe intent should be durable");
        assert_eq!(namespace, &TIER_PROBE_INTENT_NAMESPACE);
        validate_durable_ilm_record(&path, &encoded)
            .expect("tier probe intent should validate")
            .checkpoint
    }

    fn recovery_control_fixture() -> recovery_control::IlmRecoveryControl {
        let source_path = "ilm/transition-transactions/records/12/34/1234567890abcdef1234567890abcdef.json";
        let generation = recovery_control::IlmRecoverySourceGeneration::new(
            transition_transaction::TRANSITION_TRANSACTION_SCHEMA,
            "source-etag",
            "a".repeat(64),
            vec![recovery_control::IlmRecoverySourceCopy {
                authority: "pool-0/set-0".to_string(),
                canonical_path: source_path.to_string(),
                etag: "source-etag".to_string(),
                encoded_len: 128,
                content_sha256: "a".repeat(64),
            }],
        )
        .expect("source generation should build");
        recovery_control::IlmRecoveryControl::new(
            recovery_control::IlmRecoveryControlIdentity {
                protocol: recovery_control::IlmRecoveryProtocol::TransitionTransaction,
                canonical_source_path: source_path.to_string(),
                stable_operation_identity: "12345678-90ab-cdef-1234-567890abcdef".to_string(),
                record_class: "transition_transaction_v1".to_string(),
            },
            generation,
            recovery_control::IlmRecoveryClassification::Retrying,
            1_000_000_000,
            recovery_control::IlmRecoveryErrorCode::None,
        )
        .expect("recovery control should build")
    }

    fn recovery_control_checkpoint(control: &recovery_control::IlmRecoveryControl) -> DurableIlmRecordCheckpoint {
        let control_id = control.identity.source_operation_digest().expect("control id should derive");
        let path = recovery_control::recovery_control_record_object_name(control.identity.protocol, &control_id)
            .expect("control path should build");
        let encoded = control.encode().expect("control should encode");
        let namespace = classify_durable_ilm_record(&path)
            .expect("recovery control namespace should classify")
            .expect("recovery control should be durable");
        assert_eq!(namespace, &RECOVERY_CONTROL_NAMESPACE);
        validate_durable_ilm_record(&path, &encoded)
            .expect("recovery control should validate")
            .checkpoint
    }

    #[test]
    fn recovery_control_checkpoint_tracks_claim_retry_and_terminal_generations() {
        let initial_control = recovery_control_fixture();
        let initial = recovery_control_checkpoint(&initial_control);

        let mut claimed_control = initial_control;
        let mut advanced_generation = claimed_control.observed_source_generation.clone();
        advanced_generation.source_schema = "rustfs-transition-transaction-v2".to_string();
        claimed_control
            .claim_for_source_generation("node-a", Uuid::new_v4(), 2_000_000_000, 300_000_000_000, advanced_generation)
            .expect("control should claim");
        let claimed = recovery_control_checkpoint(&claimed_control);
        initial.validate_successor(&claimed).expect("claim should advance receipt");

        let mut retry_control = claimed_control;
        retry_control
            .record_retryable_failure(3_000_000_000, recovery_control::IlmRecoveryErrorCode::BackendTimeout)
            .expect("retry should persist");
        let retry = recovery_control_checkpoint(&retry_control);
        claimed.validate_successor(&retry).expect("retry should advance receipt");

        let ready_at = retry_control
            .next_attempt_at_unix_nanos
            .expect("retry deadline should persist");
        let mut terminal_control = retry_control;
        terminal_control
            .claim("node-b", Uuid::new_v4(), ready_at, 300_000_000_000)
            .expect("retry should claim");
        let reclaimed = recovery_control_checkpoint(&terminal_control);
        retry.validate_successor(&reclaimed).expect("reclaim should advance receipt");
        terminal_control
            .finish_attempt(
                recovery_control::IlmRecoveryClassification::Terminal,
                recovery_control::IlmRecoveryErrorCode::None,
            )
            .expect("control should terminate");
        let terminal = recovery_control_checkpoint(&terminal_control);
        reclaimed
            .validate_successor(&terminal)
            .expect("terminal state should advance receipt");
        assert!(initial.is_predecessor_of_terminal(&terminal));
        assert!(!initial.is_predecessor_of_terminal(&retry));
    }

    #[test]
    fn tier_probe_intent_checkpoint_tracks_exact_monotonic_generations() {
        let initial_intent = tier_probe_intent_fixture();
        let initial = tier_probe_checkpoint(&initial_intent);

        let mut uploaded_intent = initial_intent;
        uploaded_intent
            .advance(
                tier_probe_intent::TierProbeIntentState::Uploaded,
                tier_probe_intent::TierProbeRemoteVersion::versioned("opaque-v1"),
            )
            .expect("uploaded state should advance");
        let uploaded = tier_probe_checkpoint(&uploaded_intent);
        initial
            .validate_successor(&uploaded)
            .expect("durable receipt may adopt the exact uploaded generation");

        let mut cleanup_intent = uploaded_intent.clone();
        cleanup_intent
            .advance(
                tier_probe_intent::TierProbeIntentState::CleanupPending,
                uploaded_intent.remote_version.clone(),
            )
            .expect("cleanup state should advance");
        let cleanup = tier_probe_checkpoint(&cleanup_intent);
        uploaded
            .validate_successor(&cleanup)
            .expect("durable receipt may adopt the exact cleanup generation");

        let mut completed_intent = cleanup_intent.clone();
        completed_intent
            .advance(tier_probe_intent::TierProbeIntentState::Completed, cleanup_intent.remote_version.clone())
            .expect("completed state should advance");
        let completed = tier_probe_checkpoint(&completed_intent);
        cleanup
            .validate_successor(&completed)
            .expect("durable receipt may adopt the exact terminal generation");
        assert!(
            initial.is_predecessor_of_terminal(&completed),
            "terminal cleanup must recognize the full acknowledged-PUT path"
        );
        assert!(
            initial.validate_successor(&completed).is_err(),
            "ordinary receipt advancement must not skip intermediate generations"
        );
        assert!(
            !initial.is_predecessor_of_terminal(&uploaded),
            "a nonterminal generation must not be accepted as terminal proof"
        );

        let mut rebound = uploaded_intent;
        rebound.owner.owner_epoch = Uuid::new_v4();
        assert!(
            rebound.encode().is_err(),
            "dormant v1 must reject owner takeover before producing a checkpoint"
        );
    }

    #[test]
    fn transition_checkpoint_accepts_only_the_distinguishable_compact_edge() {
        let identity_sha256 = "a".repeat(64);
        let unknown_remote_sha256 = "b".repeat(64);
        let known_remote_sha256 = "c".repeat(64);
        let checkpoint = |revision, state, remote_version_sha256: String, remote_version_known| {
            DurableIlmRecordCheckpoint::TransitionTransaction {
                content_sha256: format!("{revision:064x}"),
                identity_sha256: identity_sha256.clone(),
                remote_version_sha256,
                remote_version_known,
                revision,
                state,
            }
        };
        let compact_unknown = checkpoint(
            1,
            transition_transaction::TransitionTransactionState::UploadOutcomeUnknown,
            unknown_remote_sha256.clone(),
            false,
        );
        let compact_local_commit = checkpoint(
            2,
            transition_transaction::TransitionTransactionState::LocalCommitStarted,
            known_remote_sha256.clone(),
            true,
        );
        compact_unknown
            .validate_successor(&compact_local_commit)
            .expect("compact pre-upload fence should advance directly to the exact local-commit fence");

        let legacy_unknown = checkpoint(
            2,
            transition_transaction::TransitionTransactionState::UploadOutcomeUnknown,
            unknown_remote_sha256,
            false,
        );
        let invalid_legacy_skip = checkpoint(
            3,
            transition_transaction::TransitionTransactionState::LocalCommitStarted,
            known_remote_sha256.clone(),
            true,
        );
        assert!(
            legacy_unknown.validate_successor(&invalid_legacy_skip).is_err(),
            "legacy UploadOutcomeUnknown@2 must not masquerade as the compact edge"
        );
        let valid_legacy_skip = checkpoint(
            4,
            transition_transaction::TransitionTransactionState::LocalCommitStarted,
            known_remote_sha256,
            true,
        );
        legacy_unknown
            .validate_successor(&valid_legacy_skip)
            .expect("legacy receipts may still observe the existing two-edge state advance");
    }

    #[test]
    fn tier_delete_dispatch_manifest_namespace_validates_monotonic_branches() {
        use tier_delete_journal::TierDeleteDispatchManifestState::{Aborted, Aborting, Completed, DispatchAuthorized, Preparing};

        let operation_id = Uuid::new_v4();
        let checkpoint = |state| {
            let (path, data) = tier_delete_journal::test_tier_delete_dispatch_manifest_record(operation_id, state);
            let namespace = classify_durable_ilm_record(&path)
                .expect("dispatch manifest namespace should classify")
                .expect("dispatch manifest should be durable");
            assert_eq!(namespace, &TIER_DELETE_DISPATCH_MANIFEST_NAMESPACE);
            validate_durable_ilm_record(&path, &data)
                .expect("dispatch manifest should validate")
                .checkpoint
        };

        let preparing = checkpoint(Preparing);
        let authorized = checkpoint(DispatchAuthorized);
        let completed = checkpoint(Completed);
        let aborting = checkpoint(Aborting);
        let aborted = checkpoint(Aborted);

        preparing
            .validate_successor(&authorized)
            .expect("Preparing may become DispatchAuthorized");
        authorized
            .validate_successor(&completed)
            .expect("DispatchAuthorized may become Completed");
        preparing.validate_successor(&aborting).expect("Preparing may enter rollback");
        aborting.validate_successor(&aborted).expect("Aborting may become Aborted");
        assert!(authorized.validate_successor(&aborting).is_err());
        assert!(completed.validate_successor(&authorized).is_err());
        assert!(aborted.validate_successor(&preparing).is_err());
    }

    #[test]
    fn tier_delete_dispatch_parent_checkpoint_is_monotonic_across_chunks() {
        let identity = "a".repeat(64);
        let checkpoint = |revision, sequence, completed_journals, active: Option<&str>, completed| {
            DurableIlmRecordCheckpoint::TierDeleteDispatchParent {
                content_sha256: format!("{revision:064x}"),
                identity_sha256: identity.clone(),
                revision,
                next_chunk_sequence: sequence,
                completed_journal_count: completed_journals,
                active_chunk_identity_sha256: active.map(ToOwned::to_owned),
                completed,
            }
        };
        let idle = checkpoint(0, 0, 0, None, false);
        let first_child = "b".repeat(64);
        let second_child = "c".repeat(64);
        let bound = checkpoint(1, 0, 0, Some(&first_child), false);
        let advanced = checkpoint(2, 1, 2, None, false);
        let next_bound = checkpoint(3, 1, 2, Some(&second_child), false);
        let completed = checkpoint(4, 2, 3, None, true);
        let terminal_after_more_chunks = checkpoint(6, 4, 7, None, true);

        idle.validate_successor(&bound).expect("an idle parent may bind one child");
        bound
            .validate_successor(&advanced)
            .expect("a completed child may advance the parent sequence");
        advanced
            .validate_successor(&next_bound)
            .expect("the next sequence may bind a new immutable child");
        next_bound
            .validate_successor(&completed)
            .expect("receipt progress may skip directly to a later terminal checkpoint");
        assert!(
            bound.is_predecessor_of_terminal(&terminal_after_more_chunks),
            "terminal cleanup may still recognize a valid multi-chunk predecessor"
        );
        assert!(
            advanced.is_predecessor_of_terminal(&terminal_after_more_chunks),
            "terminal cleanup may still skip over later valid parent generations"
        );
        assert!(
            idle.validate_successor(&checkpoint(1, 0, 1, Some(&first_child), false))
                .is_err()
        );
        assert!(bound.validate_successor(&checkpoint(2, 1, 0, None, false)).is_err());
        assert!(
            bound.validate_successor(&checkpoint(2, 2, 1, None, false)).is_err(),
            "sequence cannot advance beyond completed journal evidence"
        );
        assert!(
            advanced.validate_successor(&checkpoint(3, 1, 3, None, false)).is_err(),
            "completed journal count cannot grow without a completed child sequence"
        );
        assert!(
            bound.validate_successor(&checkpoint(2, 0, 0, None, true)).is_err(),
            "an active child cannot be marked completed without completion evidence"
        );
        assert!(
            bound
                .validate_successor(&checkpoint(2, 0, 0, Some(&second_child), false))
                .is_err(),
            "an active child cannot be replaced at the same parent sequence"
        );
        assert!(
            bound
                .validate_successor(&checkpoint(2, 1, 1, Some(&first_child), false))
                .is_err(),
            "sequence growth cannot retain the same active child identity"
        );
        assert!(
            !bound.is_predecessor_of_terminal(&checkpoint(2, 0, 0, None, true)),
            "terminal cleanup must not treat an active child as completed without count evidence"
        );
        assert!(completed.validate_successor(&checkpoint(5, 3, 4, None, true)).is_err());
        assert!(completed.validate_successor(&advanced).is_err());
        assert!(advanced.validate_successor(&idle).is_err());
    }

    #[test]
    fn tier_delete_journal_checkpoint_binds_dispatch_and_full_state_monotonically() {
        use crate::bucket::lifecycle::tier_sweeper::TierDeleteJournalState::{Committed, Dispatched, Prepared};

        let checkpoint = |content: &str, dispatch: Option<&str>, state| DurableIlmRecordCheckpoint::TierDeleteJournal {
            content_sha256: content.repeat(64),
            identity_sha256: "i".repeat(64),
            committed: state == Some(Committed),
            dispatch_identity_sha256: dispatch.map(|value| value.repeat(64)),
            state,
        };
        let prepared = checkpoint("a", Some("d"), Some(Prepared));
        let dispatched = checkpoint("b", Some("d"), Some(Dispatched));
        let committed = checkpoint("c", Some("d"), Some(Committed));
        prepared
            .validate_successor(&dispatched)
            .expect("Prepared may advance to Dispatched");
        dispatched
            .validate_successor(&committed)
            .expect("Dispatched may advance to Committed");
        assert!(prepared.validate_successor(&committed).is_err());
        assert!(dispatched.validate_successor(&prepared).is_err());

        let rebound = checkpoint("b", Some("e"), Some(Dispatched));
        assert!(dispatched.validate_successor(&rebound).is_err());

        let legacy: DurableIlmRecordCheckpoint = serde_json::from_value(serde_json::json!({
            "kind": "tier_delete_journal",
            "content_sha256": "a".repeat(64),
            "identity_sha256": "i".repeat(64),
            "committed": false
        }))
        .expect("legacy tier-delete checkpoint should remain decodable");
        legacy
            .validate_successor(&prepared)
            .expect("byte-identical legacy receipt may adopt the stronger v6 proof");
        let changed_legacy = DurableIlmRecordCheckpoint::TierDeleteJournal {
            content_sha256: "z".repeat(64),
            identity_sha256: "i".repeat(64),
            committed: false,
            dispatch_identity_sha256: None,
            state: None,
        };
        assert!(changed_legacy.validate_successor(&prepared).is_err());
    }

    #[test]
    fn manual_transition_job_checkpoint_compacts_legacy_progress_compatibly() {
        let options = super::super::bucket_lifecycle_ops::ManualTransitionRunOptions::default();
        let mut job =
            manual_transition_job::ManualTransitionJobRecord::new(Uuid::new_v4(), "legacy-checkpoint-bucket", &options, "owner");
        let mut report = job.report.clone();
        report.scanned = 1;
        report.continuation_token = Some(continuation_token("logs/a"));
        job.update_running_progress(report, ManualTransitionQueueSnapshot::default());
        let compact = manual_job_checkpoint(&job);
        let mut legacy = compact.clone();
        let DurableIlmRecordCheckpoint::ManualTransitionJob {
            progress,
            progress_proof,
            ..
        } = &mut legacy
        else {
            panic!("manual job should produce a manual checkpoint");
        };
        *progress = Some(Box::new(ManualTransitionJobProgressCheckpoint {
            report: job.report.clone(),
            queue_snapshot: job.queue_snapshot,
        }));
        *progress_proof = None;

        compact
            .validate_successor(&legacy)
            .expect("bounded checkpoints should accept the same legacy generation");
        legacy
            .validate_successor(&compact)
            .expect("legacy checkpoints should accept the same bounded generation");
        assert_eq!(legacy.compacted().expect("legacy checkpoint should compact"), compact);
    }

    #[test]
    fn manual_transition_job_checkpoint_rejects_timestamp_outside_wire_range() {
        let options = super::super::bucket_lifecycle_ops::ManualTransitionRunOptions::default();
        let mut job = manual_transition_job::ManualTransitionJobRecord::new(
            Uuid::new_v4(),
            "checkpoint-timestamp-bucket",
            &options,
            "owner",
        );
        job.updated_at_unix_nanos = i128::from(i64::MAX) + 1;

        let err = try_manual_job_checkpoint(&job).expect_err("out-of-range checkpoint timestamp must fail closed");

        assert!(err.to_string().contains("updated_at exceeds durable ILM checkpoint range"));
    }

    #[test]
    fn manual_transition_scope_checkpoint_rejects_timestamp_outside_wire_range() {
        let options = super::super::bucket_lifecycle_ops::ManualTransitionRunOptions::default();
        let job = manual_transition_job::ManualTransitionJobRecord::new(
            Uuid::new_v4(),
            "scope-checkpoint-timestamp-bucket",
            &options,
            "owner",
        );
        let mut admission = manual_transition_job::ManualTransitionScopeAdmission::from_job(&job);
        admission.updated_at_unix_nanos = i128::from(i64::MAX) + 1;
        let path = manual_transition_job::manual_transition_scope_record_object_name(&admission.scope_key)
            .expect("manual transition scope path should build");
        let encoded = serde_json::to_vec(&admission).expect("manual transition scope should encode");

        let err =
            validate_durable_ilm_record(&path, &encoded).expect_err("out-of-range scope checkpoint timestamp must fail closed");

        assert!(err.to_string().contains("updated_at exceeds durable ILM checkpoint range"));
    }

    #[test]
    fn manual_transition_job_checkpoint_rejects_progress_poison() {
        let options = super::super::bucket_lifecycle_ops::ManualTransitionRunOptions::default();
        let mut initial =
            manual_transition_job::ManualTransitionJobRecord::new(Uuid::new_v4(), "manual-checkpoint-bucket", &options, "owner");
        let initial_checkpoint = manual_job_checkpoint(&initial);
        let mut first_page = initial.report.clone();
        first_page.scanned = 1;
        first_page.continuation_token = Some(continuation_token("logs/a"));
        initial.update_running_progress(first_page, ManualTransitionQueueSnapshot::default());
        let first_page_checkpoint = manual_job_checkpoint(&initial);
        initial_checkpoint
            .validate_successor(&first_page_checkpoint)
            .expect("the first durable cursor should advance from no cursor");

        let mut legacy_checkpoint = initial_checkpoint;
        let DurableIlmRecordCheckpoint::ManualTransitionJob {
            progress,
            progress_proof,
            ..
        } = &mut legacy_checkpoint
        else {
            panic!("manual job should produce a manual checkpoint");
        };
        *progress = None;
        *progress_proof = None;
        legacy_checkpoint
            .validate_successor(&first_page_checkpoint)
            .expect("legacy checkpoints should upgrade to validated progress");

        let mut previous = initial;
        let mut previous_report = previous.report.clone();
        previous_report.scanned = 10;
        previous_report.eligible = 8;
        previous_report.enqueued = 2;
        previous_report.continuation_token = Some(continuation_token("logs/b"));
        let previous_queue = ManualTransitionQueueSnapshot {
            queue_capacity: 10,
            queued: 1,
            active: 1,
            workers: 2,
            queue_full: 2,
            queue_send_timeout: 1,
            ..Default::default()
        };
        previous.update_running_progress(previous_report, previous_queue);
        previous.report.transition_completed = 1;
        let previous_checkpoint = manual_job_checkpoint(&previous);

        let mut next = previous.clone();
        let mut next_report = next.report.clone();
        next_report.scanned = 11;
        next_report.eligible = 9;
        next_report.continuation_token = Some(continuation_token("logs/c"));
        let mut next_queue = next.queue_snapshot;
        next_queue.queued = 0;
        next_queue.active = 0;
        next_queue.queue_full = 3;
        next.update_running_progress(next_report, next_queue);
        next.report.transition_completed = 2;
        let next_checkpoint = manual_job_checkpoint(&next);
        previous_checkpoint
            .validate_successor(&next_checkpoint)
            .expect("forward job progress should validate");

        let mut counter_rollback = next.clone();
        counter_rollback.updated_at_unix_nanos += 1;
        counter_rollback.report.scanned = 9;
        assert!(
            try_manual_job_checkpoint(&counter_rollback)
                .and_then(|checkpoint| previous_checkpoint.validate_successor(&checkpoint))
                .is_err()
        );

        let mut cursor_rollback = previous.clone();
        cursor_rollback.updated_at_unix_nanos += 1;
        cursor_rollback.report.scanned += 1;
        cursor_rollback.report.continuation_token = Some(continuation_token("logs/a"));
        assert!(
            try_manual_job_checkpoint(&cursor_rollback)
                .and_then(|checkpoint| previous_checkpoint.validate_successor(&checkpoint))
                .is_err()
        );

        let mut same_marker_version_previous = previous.clone();
        let mut same_marker_report = same_marker_version_previous.report.clone();
        same_marker_report.continuation_token = Some(continuation_token_with_version("logs/b", Some("opaque-z-version")));
        same_marker_version_previous.update_running_progress(same_marker_report, same_marker_version_previous.queue_snapshot);
        let same_marker_version_previous_checkpoint = manual_job_checkpoint(&same_marker_version_previous);
        let mut same_marker_version_next = same_marker_version_previous.clone();
        let mut same_marker_next_report = same_marker_version_next.report.clone();
        same_marker_next_report.scanned += 1;
        same_marker_next_report.continuation_token = Some(continuation_token_with_version("logs/b", Some("opaque-a-version")));
        same_marker_version_next.update_running_progress(same_marker_next_report, same_marker_version_next.queue_snapshot);
        same_marker_version_previous_checkpoint
            .validate_successor(&manual_job_checkpoint(&same_marker_version_next))
            .expect("producer cursor revision should prove same-marker version progress");

        let mut same_marker_version_rollback = same_marker_version_previous.clone();
        same_marker_version_rollback.updated_at_unix_nanos += 1;
        same_marker_version_rollback.report.scanned += 1;
        same_marker_version_rollback.report.continuation_token =
            Some(continuation_token_with_version("logs/b", Some("opaque-arbitrary-version")));
        assert!(
            try_manual_job_checkpoint(&same_marker_version_rollback)
                .and_then(|checkpoint| same_marker_version_previous_checkpoint.validate_successor(&checkpoint))
                .is_err(),
            "a different opaque version marker without producer evidence must fail closed"
        );

        let mut worker_result_rollback = next.clone();
        worker_result_rollback.updated_at_unix_nanos += 1;
        worker_result_rollback.report.transition_completed = 0;
        assert!(
            previous_checkpoint
                .validate_successor(&manual_job_checkpoint(&worker_result_rollback))
                .is_err()
        );

        let mut worker_result_overflow = next.clone();
        worker_result_overflow.updated_at_unix_nanos += 1;
        worker_result_overflow.report.enqueued = u64::MAX;
        worker_result_overflow.report.transition_completed = u64::MAX;
        worker_result_overflow.report.transition_failed = 1;
        worker_result_overflow.report.tier_failure = 1;
        assert!(try_manual_job_checkpoint(&worker_result_overflow).is_err());

        let mut invalid_cursor = next.clone();
        invalid_cursor.updated_at_unix_nanos += 1;
        invalid_cursor.report.continuation_token = Some("not-base64".to_string());
        assert!(try_manual_job_checkpoint(&invalid_cursor).is_err());

        let mut queue_state_poison = next;
        queue_state_poison.updated_at_unix_nanos += 1;
        queue_state_poison.queue_snapshot.queued = queue_state_poison.queue_snapshot.queue_capacity + 1;
        assert!(try_manual_job_checkpoint(&queue_state_poison).is_err());
    }
}
