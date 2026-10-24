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

use super::*;
use crate::{CheckpointRecoveryStatus, CheckpointValidation, CheckpointValidationStage};

pub(super) struct ValidationFailure {
    pub summary: CheckpointValidation,
    pub error: Error,
    pub block_snapshot: bool,
}

impl CheckpointValidation {
    fn rejected(mut self, stage: CheckpointValidationStage, error: Error, block_snapshot: bool) -> Box<ValidationFailure> {
        self.stage = stage;
        self.reader_accepted = Some(false);
        self.recovery_status = CheckpointRecoveryStatus::Rejected;
        Box::new(ValidationFailure {
            summary: self,
            error,
            block_snapshot,
        })
    }
}

pub(super) struct PreparedCheckpoint {
    checkpoint: ResumeCheckpoint,
    persisted: CheckpointJson,
    supported_schema: u32,
    summary: CheckpointValidation,
}

pub(super) struct ValidatedCheckpoint {
    pub checkpoint: ResumeCheckpoint,
    pub supported_schema: u32,
    pub summary: CheckpointValidation,
}

impl PreparedCheckpoint {
    pub(super) fn decode(task_id: &str, bytes: &[u8]) -> std::result::Result<Self, Box<ValidationFailure>> {
        let mut summary = CheckpointValidation::default();
        let (checkpoint, persisted) = serde_json::from_slice::<ResumeCheckpoint>(bytes)
            .and_then(|checkpoint| serde_json::from_slice::<CheckpointJson>(bytes).map(|value| (checkpoint, value)))
            .map_err(|error| {
                summary.clone().rejected(
                    CheckpointValidationStage::Malformed,
                    Error::TaskExecutionFailed {
                        message: format!("Failed to deserialize checkpoint: {error}"),
                    },
                    true,
                )
            })?;
        summary.schema_version = Some(checkpoint.schema_version);
        summary.signature_present = Some(checkpoint.integrity_digest.is_some());
        summary.task_identity_matches = Some(checkpoint.task_id == task_id);
        if checkpoint.task_id != task_id {
            return Err(summary.rejected(
                CheckpointValidationStage::IdentityConflict,
                Error::TaskExecutionFailed {
                    message: "Resume checkpoint task id does not match filename".to_string(),
                },
                true,
            ));
        }
        if checkpoint
            .dangling_delete_retry_not_before
            .is_some_and(|deadline| !dangling_delete_deadline_is_representable(deadline))
        {
            return Err(summary.rejected(
                CheckpointValidationStage::InvalidDeadline,
                Error::InvalidCheckpoint("Unrepresentable dangling-delete grace deadline".to_string()),
                true,
            ));
        }
        let supported_schema = if checkpoint.admin.is_some() {
            ADMIN_CHECKPOINT_SCHEMA
        } else {
            CURRENT_CHECKPOINT_SCHEMA
        };
        if checkpoint.admin.is_some() && checkpoint.schema_version < ADMIN_CHECKPOINT_SCHEMA {
            return Err(summary.rejected(
                CheckpointValidationStage::InvalidAdminSchema,
                Error::InvalidCheckpoint("Administrator outcome requires checkpoint schema 8".to_string()),
                false,
            ));
        }
        if checkpoint.schema_version > supported_schema {
            return Err(summary.rejected(
                CheckpointValidationStage::FutureSchema,
                Error::TaskExecutionFailed {
                    message: format!(
                        "Checkpoint schema {} is newer than supported schema {supported_schema}",
                        checkpoint.schema_version
                    ),
                },
                true,
            ));
        }
        Ok(Self {
            checkpoint,
            persisted,
            supported_schema,
            summary,
        })
    }

    pub(super) fn needs_legacy_digest(&self) -> bool {
        self.checkpoint.integrity_digest.is_none() && self.checkpoint.schema_version < CHECKPOINT_PER_VERSION_SCHEMA
    }

    pub(super) fn verify(
        mut self,
        task_id: &str,
        bytes: &[u8],
        legacy_digest: Option<&[u8]>,
    ) -> std::result::Result<ValidatedCheckpoint, Box<ValidationFailure>> {
        let integrity_verified = if let Some(expected) = self.checkpoint.integrity_digest.as_deref() {
            let unsigned = self.persisted.unsigned_bytes().map_err(|error| {
                self.summary
                    .clone()
                    .rejected(CheckpointValidationStage::Malformed, error, false)
            })?;
            let mut actual = CheckpointManager::checkpoint_digest(&unsigned);
            if expected != actual {
                // RUSTFS_COMPAT_TODO(#2871): retain until snapshots whose
                // payload order differs from their digest order are retired.
                self.persisted.sort_all_objects();
                let unsigned = self.persisted.unsigned_bytes().map_err(|error| {
                    self.summary
                        .clone()
                        .rejected(CheckpointValidationStage::Malformed, error, false)
                })?;
                actual = CheckpointManager::checkpoint_digest(&unsigned);
            }
            self.summary.signature_valid = Some(expected == actual);
            if expected != actual {
                return Err(self.summary.rejected(
                    CheckpointValidationStage::DigestMismatch,
                    Error::InvalidCheckpoint(format!("Resume checkpoint digest does not match task {task_id}")),
                    true,
                ));
            }
            true
        } else if self.checkpoint.schema_version >= CHECKPOINT_PER_VERSION_SCHEMA {
            return Err(self.summary.rejected(
                CheckpointValidationStage::SignatureMissing,
                Error::InvalidCheckpoint(format!("Resume checkpoint digest is missing for task {task_id}")),
                true,
            ));
        } else if let Some(expected) = legacy_digest {
            self.summary.signature_present = Some(true);
            let actual = CheckpointManager::checkpoint_digest(bytes);
            self.summary.signature_valid = Some(expected == actual.as_bytes());
            if expected != actual.as_bytes() {
                return Err(self.summary.rejected(
                    CheckpointValidationStage::DigestMismatch,
                    Error::InvalidCheckpoint(format!("Resume checkpoint digest does not match task {task_id}")),
                    true,
                ));
            }
            true
        } else {
            false
        };
        self.summary.legacy_rewind = Some(self.checkpoint.schema_version < CHECKPOINT_PER_VERSION_SCHEMA || !integrity_verified);
        self.summary.reader_accepted = Some(true);
        self.summary.stage = CheckpointValidationStage::Validated;
        self.summary.recovery_status = CheckpointRecoveryStatus::CandidateExecutionUnauthorized;
        Ok(ValidatedCheckpoint {
            checkpoint: self.checkpoint,
            supported_schema: self.supported_schema,
            summary: self.summary,
        })
    }
}

// Diagnostic budgets do not change what the execution reader accepts.
const MAX_SNAPSHOT_BYTES: usize = 1024 * 1024;
const MAX_DIGEST_BYTES: usize = 64;

pub(super) struct DiagnosticBudget {
    bytes: usize,
    records: usize,
}

impl Default for DiagnosticBudget {
    fn default() -> Self {
        Self {
            bytes: 4 * MAX_SNAPSHOT_BYTES,
            records: 64,
        }
    }
}

async fn read_bounded(
    disk: &DiskStore,
    path: &Path,
    max: usize,
    budget: &mut DiagnosticBudget,
) -> std::result::Result<Vec<u8>, CheckpointValidationStage> {
    let limit = max.min(budget.bytes);
    if limit == 0 {
        return Err(CheckpointValidationStage::BudgetExhausted);
    }
    let path = path_to_str(path).map_err(|_| CheckpointValidationStage::InvalidTaskId)?;
    let reader = EcstoreDiskAPI::read_file(disk.as_ref(), RUSTFS_META_BUCKET, path)
        .await
        .map_err(|error| match error {
            crate::heal::DiskError::FileNotFound => CheckpointValidationStage::SnapshotMissing,
            _ => CheckpointValidationStage::ReadUnavailable,
        })?;
    let mut bytes = Vec::new();
    let read = reader
        .take(u64::try_from(limit + 1).map_err(|_| CheckpointValidationStage::TooLarge)?)
        .read_to_end(&mut bytes)
        .await;
    budget.bytes = budget.bytes.saturating_sub(bytes.len());
    read.map_err(|_| CheckpointValidationStage::ReadUnavailable)?;
    if bytes.len() > limit {
        return Err(if limit < max {
            CheckpointValidationStage::BudgetExhausted
        } else {
            CheckpointValidationStage::TooLarge
        });
    }
    Ok(bytes)
}

pub(super) async fn inspect(disk: &DiskStore, task_id: &str, budget: &mut DiagnosticBudget) -> CheckpointValidation {
    let mut unknown = CheckpointValidation::default();
    if validate_resume_task_id(task_id).is_err() {
        unknown.stage = CheckpointValidationStage::InvalidTaskId;
        return unknown;
    }
    if budget.records == 0 {
        unknown.stage = CheckpointValidationStage::BudgetExhausted;
        return unknown;
    }
    budget.records -= 1;
    let path = Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{RESUME_CHECKPOINT_FILE}"));
    let bytes = match read_bounded(disk, &path, MAX_SNAPSHOT_BYTES, budget).await {
        Ok(bytes) => bytes,
        Err(stage) => {
            unknown.stage = stage;
            return unknown;
        }
    };
    let prepared = match PreparedCheckpoint::decode(task_id, &bytes) {
        Ok(prepared) => prepared,
        Err(failure) => return failure.summary,
    };
    let digest = if prepared.needs_legacy_digest() {
        match read_bounded(disk, &CheckpointManager::digest_path(task_id), MAX_DIGEST_BYTES, budget).await {
            Ok(bytes) => Some(bytes),
            Err(CheckpointValidationStage::SnapshotMissing) => None,
            Err(stage) => {
                unknown = prepared.summary;
                unknown.stage = stage;
                unknown.signature_present = None;
                return unknown;
            }
        }
    } else {
        None
    };
    match prepared.verify(task_id, &bytes, digest.as_deref()) {
        Ok(validated) => validated.summary,
        Err(failure) => failure.summary,
    }
}
