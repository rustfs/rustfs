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
use crate::heal::outcome::{
    HealDeferredReason, HealFailureClass, HealObjectDisposition, HealObjectIdentity, HealObjectKind,
    HealObjectOutcome as CanonicalObjectOutcome, HealTaskOutcome,
};
use crate::heal::resume::AdminErasureCheckpoint;
use crate::heal::task::{HealTask, MAX_BUCKET_OBJECT_HEAL_RETRIES};
use std::collections::BTreeMap;
use uuid::Uuid;

pub(super) type ObjectRecord = (CanonicalObjectOutcome, u32);

impl ErasureSetHealer {
    pub(crate) fn with_admin_task(mut self, task: Option<HealTask>) -> Self {
        self.admin_task = task;
        self
    }

    pub(super) async fn prepare_admin_checkpoint(
        &self,
        manager: &CheckpointManager,
        buckets: &[String],
        set_disk_id: &str,
    ) -> Result<Vec<String>> {
        let snapshot = manager.get_checkpoint().await;
        let admin = if let Some(admin) = snapshot.admin {
            admin.validate_scope(set_disk_id, &self.heal_opts)?;
            admin
        } else {
            if snapshot.current_bucket_index != 0
                || snapshot.current_object_index != 0
                || snapshot.successful_objects != 0
                || snapshot.failed_object_count != 0
                || snapshot.skipped_object_count != 0
            {
                return Err(Error::InvalidCheckpoint("Administrator checkpoint lacks its outcome ledger".to_string()));
            }
            let resolved;
            let buckets = if buckets.is_empty() {
                resolved = self
                    .storage
                    .list_buckets()
                    .await?
                    .into_iter()
                    .map(|bucket| bucket.name)
                    .collect::<Vec<_>>();
                resolved.as_slice()
            } else {
                buckets
            };
            let mut identities = BTreeMap::new();
            for bucket in buckets {
                let incarnation = if self.heal_opts.dry_run {
                    None
                } else {
                    Some(self.storage.admit_bucket_incarnation(bucket).await?)
                };
                identities.insert(bucket.clone(), incarnation);
            }
            let mut outcome = HealTaskOutcome::default();
            outcome.start();
            let admin = AdminErasureCheckpoint {
                set_disk_id: set_disk_id.to_owned(),
                options: self.heal_opts,
                buckets: identities,
                outcome,
                completed: false,
            };
            admin.validate_scope(set_disk_id, &self.heal_opts)?;
            manager.set_admin(admin.clone()).await?;
            admin
        };
        let buckets = admin.buckets.keys().cloned().collect();
        if let Some(task) = &self.admin_task {
            task.restore_outcome(admin.outcome).await;
        }
        Ok(buckets)
    }

    pub(super) async fn admin_bucket_incarnation(&self, checkpoint: &CheckpointManager, bucket: &str) -> Result<Option<Uuid>> {
        if self.admin_task.is_none() {
            return Ok(None);
        }
        checkpoint
            .get_checkpoint()
            .await
            .admin
            .and_then(|admin| admin.buckets.get(bucket).copied())
            .ok_or_else(|| Error::InvalidCheckpoint("Bucket is outside administrator checkpoint scope".to_string()))
    }

    pub(super) fn admin_identity(
        &self,
        bucket: &str,
        object: &str,
        version: Option<&str>,
        incarnation: Option<Uuid>,
    ) -> Option<HealObjectIdentity> {
        self.admin_task.as_ref().map(|_| HealObjectIdentity {
            kind: HealObjectKind::Object,
            bucket: bucket.to_owned(),
            object: object.to_owned(),
            version_id: version.map(str::to_owned),
            bucket_incarnation_id: incarnation,
            pool_index: self.heal_opts.pool,
            set_index: self.heal_opts.set,
        })
    }

    pub(super) async fn record_checkpoint_outcome(
        &self,
        checkpoint: &CheckpointManager,
        record: CheckpointObjectOutcomeRecord,
        canonical: Option<ObjectRecord>,
    ) -> Result<()> {
        let (canonical, attempts) = canonical.map_or((None, 0), |(record, attempts)| (Some(record), attempts));
        let outcome = checkpoint.record_outcome(record, canonical, attempts).await?;
        if let Some(task) = &self.admin_task {
            task.restore_outcome(
                outcome.ok_or_else(|| Error::InvalidCheckpoint("Administrator checkpoint lost its outcome".to_string()))?,
            )
            .await;
        }
        Ok(())
    }
}

pub(super) fn skipped(identity: Option<HealObjectIdentity>, reason: &str) -> Option<ObjectRecord> {
    identity.map(|identity| {
        (
            CanonicalObjectOutcome {
                identity,
                disposition: HealObjectDisposition::Unknown,
                detail: Some(reason.to_owned()),
            },
            0,
        )
    })
}

pub(super) async fn heal_object(
    storage: &dyn HealStorageAPI,
    options: &HealOpts,
    identity: HealObjectIdentity,
    task_id: &str,
    cancel: &tokio_util::sync::CancellationToken,
) -> ((u64, Result<bool>), Option<ObjectRecord>) {
    let mut failures = 0;
    loop {
        if cancel.is_cancelled() {
            return ((0, Err(Error::TaskCancelled)), None);
        }
        let result = match identity.bucket_incarnation_id {
            Some(expected) => {
                storage
                    .heal_object_at_incarnation(
                        &identity.bucket,
                        &identity.object,
                        identity.version_id.as_deref(),
                        expected,
                        options,
                    )
                    .await
            }
            None if options.dry_run => {
                storage
                    .heal_object_with_receipt(&identity.bucket, &identity.object, identity.version_id.as_deref(), options)
                    .await
            }
            None => {
                return (
                    (0, Err(Error::InvalidCheckpoint("Administrator object has no incarnation".to_string()))),
                    None,
                );
            }
        };
        let (size, receipt, error) = match result {
            Ok(result) => (result_object_size_u64(&result.item), result.receipt, result.error),
            Err(error) => (0, None, Some(error)),
        };
        if matches!(error, Some(Error::TaskCancelled | Error::TaskTimeout)) {
            return ((size, Err(error.unwrap_or(Error::TaskCancelled))), None);
        }
        if let Some(error) = error.as_ref()
            && error.is_recoverable_heal()
            && failures < MAX_BUCKET_OBJECT_HEAL_RETRIES
        {
            failures += 1;
            tokio::select! {
                _ = cancel.cancelled() => return ((size, Err(Error::TaskCancelled)), None),
                _ = tokio::time::sleep(HealTask::bucket_object_retry_delay(task_id, failures)) => {}
            }
            continue;
        }
        let mut disposition = if options.dry_run {
            HealObjectDisposition::DryRunObserved
        } else {
            HealObjectDisposition::Unknown
        };
        // Storage errors cannot become absence proofs. Only an exact receipt
        // from the incarnation-fenced mutation can establish a positive result.
        if error.is_none()
            && !options.dry_run
            && let Some(receipt) = receipt.filter(|receipt| receipt.verified_for(&identity))
        {
            disposition = receipt.disposition;
        }
        let detail = error.as_ref().map(ToString::to_string);
        let legacy = match error {
            None => Ok(!matches!(disposition, HealObjectDisposition::AuthoritativelyAbsent)),
            Some(error) => {
                failures += 1;
                if error.is_recoverable_heal() {
                    disposition = HealObjectDisposition::Deferred {
                        reason: if error.is_dangling_delete_grace() {
                            HealDeferredReason::DanglingDeleteGrace
                        } else {
                            HealDeferredReason::TransientExistenceCheck
                        },
                        retry_not_before: None,
                    };
                    Err(Error::transient_skip(error.to_string()))
                } else {
                    disposition = HealObjectDisposition::Failed(HealFailureClass::Permanent);
                    Err(error)
                }
            }
        };
        return (
            (size, legacy),
            Some((
                CanonicalObjectOutcome {
                    identity,
                    disposition,
                    detail,
                },
                failures,
            )),
        );
    }
}
