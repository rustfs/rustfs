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

use rustfs_utils::crypto::hex_sha256;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{manual_transition_job, tier_delete_journal, transition_transaction};
use crate::error::{Error, Result};

pub(crate) const ILM_META_PREFIX: &str = "ilm";
const ILM_META_OBJECT_PREFIX: &str = "ilm/";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DurableIlmRecordKind {
    TierDeleteJournal,
    TransitionTransaction,
    ManualTransitionJob,
    ManualTransitionScope,
    ManualTransitionTask,
    ManualTransitionWorkerResult,
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
pub(crate) const TRANSITION_TRANSACTION_NAMESPACE: DurableIlmNamespace = DurableIlmNamespace {
    name: "transition-transaction",
    prefix: "ilm/transition-transactions/records",
    max_record_size: transition_transaction::MAX_TRANSITION_TRANSACTION_SIZE,
    kind: DurableIlmRecordKind::TransitionTransaction,
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

pub(crate) const DURABLE_ILM_NAMESPACES: [DurableIlmNamespace; 6] = [
    TIER_DELETE_JOURNAL_NAMESPACE,
    TRANSITION_TRANSACTION_NAMESPACE,
    MANUAL_TRANSITION_JOB_NAMESPACE,
    MANUAL_TRANSITION_SCOPE_NAMESPACE,
    MANUAL_TRANSITION_TASK_NAMESPACE,
    MANUAL_TRANSITION_WORKER_RESULT_NAMESPACE,
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedDurableIlmRecord {
    pub(crate) namespace: &'static str,
    pub(crate) id_kind: &'static str,
    pub(crate) id: String,
    pub(crate) checkpoint: DurableIlmRecordCheckpoint,
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
    },
    TransitionTransaction {
        content_sha256: String,
        identity_sha256: String,
        remote_version_sha256: String,
        remote_version_known: bool,
        revision: u64,
        state: transition_transaction::TransitionTransactionState,
    },
    ManualTransitionJob {
        content_sha256: String,
        identity_sha256: String,
        updated_at_unix_nanos: i128,
        state: manual_transition_job::ManualTransitionJobState,
        scan_completed: bool,
        cancel_requested: bool,
    },
    ManualTransitionScope {
        content_sha256: String,
        identity_sha256: String,
        updated_at_unix_nanos: i128,
    },
    ManualTransitionTask {
        content_sha256: String,
    },
    ManualTransitionWorkerResult {
        content_sha256: String,
    },
}

impl DurableIlmRecordCheckpoint {
    pub(crate) fn content_sha256(&self) -> &str {
        match self {
            Self::TierDeleteJournal { content_sha256, .. }
            | Self::TransitionTransaction { content_sha256, .. }
            | Self::ManualTransitionJob { content_sha256, .. }
            | Self::ManualTransitionScope { content_sha256, .. }
            | Self::ManualTransitionTask { content_sha256 }
            | Self::ManualTransitionWorkerResult { content_sha256 } => content_sha256,
        }
    }

    pub(crate) fn validate_successor(&self, next: &Self) -> Result<()> {
        if self == next {
            return Ok(());
        }

        let valid = match (self, next) {
            (
                Self::TierDeleteJournal {
                    identity_sha256: previous_identity,
                    committed: previous_committed,
                    ..
                },
                Self::TierDeleteJournal {
                    identity_sha256: next_identity,
                    committed: next_committed,
                    ..
                },
            ) => {
                previous_identity == next_identity
                    && (previous_committed == next_committed || (!previous_committed && *next_committed))
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
                    && transition_state_distance(*previous_state, *next_state)
                        .and_then(|distance| previous_revision.checked_add(distance))
                        .is_some_and(|expected_revision| *next_revision == expected_revision)
                    && (!previous_remote_version_known || previous_remote_version == next_remote_version)
            }
            (
                Self::ManualTransitionJob {
                    identity_sha256: previous_identity,
                    updated_at_unix_nanos: previous_updated_at,
                    state: previous_state,
                    scan_completed: previous_scan_completed,
                    cancel_requested: previous_cancel_requested,
                    ..
                },
                Self::ManualTransitionJob {
                    identity_sha256: next_identity,
                    updated_at_unix_nanos: next_updated_at,
                    state: next_state,
                    scan_completed: next_scan_completed,
                    cancel_requested: next_cancel_requested,
                    ..
                },
            ) => {
                previous_identity == next_identity
                    && next_updated_at > previous_updated_at
                    && manual_job_state_reaches(*previous_state, *next_state)
                    && (!previous_scan_completed || *next_scan_completed)
                    && (!previous_cancel_requested || *next_cancel_requested)
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
            _ => false,
        };

        if valid {
            Ok(())
        } else {
            Err(Error::other("durable ILM record generation is not a monotonic successor"))
        }
    }
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

fn manual_job_state_reaches(
    from: manual_transition_job::ManualTransitionJobState,
    to: manual_transition_job::ManualTransitionJobState,
) -> bool {
    from == to || from == manual_transition_job::ManualTransitionJobState::Running
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
            let operation_id = path
                .strip_prefix(namespace.prefix)
                .and_then(|suffix| suffix.strip_suffix(".json"))
                .ok_or_else(|| Error::other("tier delete journal path is invalid"))?;
            let identity_sha256 = checkpoint_hash(&(
                &entry.obj_name,
                &entry.version_id,
                &entry.tier_name,
                entry.backend_identity,
                entry.version_id_exact,
                entry.version_state,
                &entry.source,
            ))?;
            (
                "operation_id",
                operation_id.to_string(),
                DurableIlmRecordCheckpoint::TierDeleteJournal {
                    content_sha256,
                    identity_sha256,
                    committed: entry.state == super::tier_sweeper::TierDeleteJournalState::Committed,
                },
            )
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
            (
                "job_id",
                job_id.to_string(),
                DurableIlmRecordCheckpoint::ManualTransitionJob {
                    content_sha256,
                    identity_sha256,
                    updated_at_unix_nanos: job.updated_at_unix_nanos,
                    state: job.state,
                    scan_completed: job.scan_completed,
                    cancel_requested: job.cancel_requested,
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
            (
                "job_id",
                admission.job_id.to_string(),
                DurableIlmRecordCheckpoint::ManualTransitionScope {
                    content_sha256,
                    identity_sha256,
                    updated_at_unix_nanos: admission.updated_at_unix_nanos,
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
}
