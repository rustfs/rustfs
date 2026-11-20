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

//! Read-only exact-key evidence and conditional legacy-only disposal for administrators.

use super::*;
use crate::disk::{DELETE_RECOVERY_MAX_DISCOVERY_ENTRIES, DELETE_RECOVERY_MAX_INSPECTION_ENTRIES, DeleteRecoveryRpcReply};
use crate::error::Result as StorageResult;
use crate::object_api::DeleteRecoveryWorkBudget;
use rustfs_madmin::heal_commands::{
    DeleteResidueCopy, DeleteResidueInspection, DeleteResidueParticipant, DeleteResidueTransaction,
};
use sha2::{Digest, Sha256};

fn status(error: &DiskError) -> &'static str {
    if unsupported_recovery(error) {
        "unsupported"
    } else {
        match error {
            DiskError::DeleteRecoveryWorkLimit => "work_limit",
            DiskError::FileCorrupt => "corrupt",
            _ if unavailable(error) => "unavailable",
            _ => "failed",
        }
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(bytes, hex_simd::AsciiCase::Lower)
}

fn evidence_copy(slot: usize, inspection: &DeleteRecoveryInspection) -> DeleteResidueCopy {
    let state = match inspection.state {
        DeleteRecoveryState::Absent => "absent",
        DeleteRecoveryState::NewPrepared => "prepared",
        DeleteRecoveryState::QuorumCommitted => "committed",
        DeleteRecoveryState::CommitFinished => "finished",
        DeleteRecoveryState::LegacyAmbiguous => "legacy_ambiguous",
    };
    DeleteResidueCopy {
        slot,
        state: state.to_owned(),
        current_metadata_present: Some(inspection.current_digest.is_some()),
        backup_present: Some(inspection.backup_digest.is_some()),
        journal_present: Some(inspection.journal.is_some()),
        fingerprint: Some(hex_digest(&inspection.fingerprint)),
    }
}

impl SetDisks {
    /// Retains the original exact-key and transaction guards through the discard write tail.
    pub(crate) async fn operator_delete_residue(
        &self,
        object: &str,
        mut report: DeleteResidueInspection,
        expected: Option<&str>,
        bucket_allows_discard: bool,
        expected_disks: &[Uuid],
        can_commit: &(dyn Fn() -> bool + Sync),
    ) -> StorageResult<DeleteResidueInspection> {
        let bucket = &report.bucket;
        // Lock order matches normal recovery: caller's lifecycle and movement fences,
        // raw object write lock, then its set-local transaction lock.
        let object_guard = Arc::new(self.acquire_write_lock_diag("inspect_delete_residue", bucket, object).await?);
        let transaction_guard = self.acquire_delete_transaction_lock(bucket, object).await?;
        let fence = || can_commit() && !object_guard.is_lock_lost() && !transaction_guard.is_lock_lost();
        let disks = self.disk_inventory().await;
        let budget = DeleteRecoveryWorkBudget::for_disks(self.set_drive_count);
        let pages = join_all(disks.iter().map(|disk| async {
            let disk = disk.as_ref().ok_or(DiskError::DiskNotFound)?;
            let id = disk.probe_delete_residue_disk(bucket).await?;
            let admitted = budget.admit(DELETE_RECOVERY_MAX_DISCOVERY_ENTRIES)?;
            let page = match disk.read_delete_residue_on_disk(bucket, object, id, None, admitted).await {
                Ok(DeleteRecoveryRpcReply::DiscoveredBounded(page)) => bounded_discovery_ids(page, object, admitted, &budget),
                Ok(_) => Err(DiskError::FileCorrupt),
                Err(error) => Err(error),
            };
            Ok::<_, DiskError>((id, page))
        }))
        .await;
        let mut ids = BTreeSet::new();
        let mut disk_ids = HashSet::new();
        report.complete = disks.len() == self.set_drive_count && !disks.is_empty() && expected_disks.len() == disks.len();
        for (slot, page) in pages.into_iter().enumerate() {
            let (disk_id, discovery) = match page {
                Ok((id, Ok(page))) if expected_disks.get(slot) == Some(&id) && disk_ids.insert(id) => {
                    ids.extend(page.into_iter().map(|item| item.transaction));
                    (Some(id.to_string()), "complete")
                }
                Ok((id, Ok(_))) => (Some(id.to_string()), "disk_identity_mismatch"),
                Ok((id, Err(error))) => (Some(id.to_string()), status(&error)),
                Err(error) => (None, status(&error)),
            };
            report.complete &= discovery == "complete";
            report.participants.push(DeleteResidueParticipant {
                slot,
                disk_id,
                discovery: discovery.to_owned(),
            });
        }

        let mut groups = BTreeMap::new();
        let mut legacy = false;
        let mut valid_generation = true;
        // A bounded exact-key discovery admits at most 256 records per disk.
        // A severely divergent set cannot multiply that bound into an unbounded response.
        let truncated = ids.len() > 256;
        report.complete &= !truncated;
        for id in ids.into_iter().take(256) {
            let observations = join_all(disks.iter().enumerate().map(|(slot, disk)| {
                let budget = &budget;
                async move {
                    let disk = disk.as_ref().ok_or(DiskError::DiskNotFound)?;
                    let physical_id = *expected_disks.get(slot).ok_or(DiskError::InconsistentDisk)?;
                    let admitted = budget.admit(DELETE_RECOVERY_MAX_INSPECTION_ENTRIES)?;
                    let DeleteRecoveryRpcReply::InspectedBounded(reply) = disk
                        .read_delete_residue_on_disk(bucket, object, physical_id, Some(id), admitted)
                        .await?
                    else {
                        return Err(DiskError::FileCorrupt);
                    };
                    if reply.visited > admitted {
                        return Err(DiskError::FileCorrupt);
                    }
                    budget.charge(reply.visited);
                    validate(&reply.inspection, object, id)?;
                    Ok(reply.inspection)
                }
            }))
            .await;
            let mut copies = Vec::with_capacity(observations.len());
            let mut states = Vec::with_capacity(observations.len());
            for (slot, observation) in observations.into_iter().enumerate() {
                match observation {
                    Ok(mut state) => {
                        legacy |= state.state == DeleteRecoveryState::LegacyAmbiguous;
                        valid_generation &= state.journal.as_ref().is_none_or(|journal| {
                            journal
                                .bucket_incarnation
                                .is_some_and(|incarnation| incarnation.to_string() == report.incarnation)
                        });
                        copies.push(evidence_copy(slot, &state));
                        // Only the causal edges, classification and fingerprints survive
                        // this iteration. Never retain full version histories or request
                        // payloads across the bounded list of sibling transactions.
                        state.current_versions = Vec::new();
                        if let Some(journal) = &mut state.journal {
                            journal.requests = Vec::new();
                            journal.original_versions = Vec::new();
                            journal.data_dirs = Vec::new();
                            journal.deleted_versions = Vec::new();
                        }
                        states.push(state);
                    }
                    Err(error) => {
                        report.complete = false;
                        copies.push(DeleteResidueCopy {
                            slot,
                            state: status(&error).to_owned(),
                            current_metadata_present: None,
                            backup_present: None,
                            journal_present: None,
                            fingerprint: None,
                        });
                    }
                }
            }
            report.transactions.push(DeleteResidueTransaction {
                id: id.to_string(),
                copies,
            });
            groups.insert(id, states);
        }

        let reason = if truncated {
            "work_limit"
        } else if !report.complete {
            "incomplete"
        } else if !legacy {
            "no_legacy_residue"
        } else if !bucket_allows_discard {
            "bucket_protected"
        } else if !valid_generation || causal_transaction_order(&groups).is_err() {
            "conflicting_history"
        } else if groups.values().flatten().any(|copy| copy.current_digest.is_some()) {
            "current_object_present"
        } else if !groups.values().flatten().all(explicit_legacy_copy_safe) {
            "unsafe_layout"
        } else {
            "discard_eligible"
        };
        report.reason = reason.to_owned();
        if !fence() {
            return Err(StorageError::PreconditionFailed);
        }
        if reason == "discard_eligible" {
            // Fixed-order structs and sorted transaction UUIDs cover the exact action,
            // logical/physical identities, all siblings and every disk fingerprint.
            let serialized = serde_json::to_vec(&("rustfs:discard-legacy:v1", &report)).map_err(StorageError::other)?;
            report.fingerprint = Some(hex_digest(&Sha256::digest(serialized)));
        }
        let Some(expected) = expected else { return Ok(report) };
        if report.fingerprint.as_deref() != Some(expected) {
            return Err(StorageError::PreconditionFailed);
        }

        let owner: Arc<dyn Send + Sync> = Arc::new((
            self.ctx.begin_namespace_commit_for_bucket(bucket),
            object_guard.clone(),
            transaction_guard.clone(),
        ));
        let work = RecoveryWorkScope {
            namespace_owner: Some(&owner),
            budget: Some(&budget),
        };
        for copies in groups
            .values()
            .filter(|copies| copies.iter().any(|copy| copy.state == DeleteRecoveryState::LegacyAmbiguous))
        {
            // Compare both the physical disk ID and its original evidence on the owning node.
            // A late reply or a partial failure requires a new preview of the remaining evidence.
            let results = join_all(
                copies
                    .iter()
                    .enumerate()
                    .filter(|(_, copy)| copy.state == DeleteRecoveryState::LegacyAmbiguous)
                    .map(|(slot, copy)| {
                        let fence = &fence;
                        let disks = &disks;
                        async move {
                            let disk = disks[slot].as_ref().ok_or(DiskError::DiskNotFound)?;
                            settle(
                                disk,
                                bucket,
                                copy,
                                DeleteRecoveryAction::DiscardLegacyOnDisk {
                                    disk_id: expected_disks[slot],
                                },
                                fence,
                                work,
                            )
                            .await
                        }
                    }),
            )
            .await;
            for result in results {
                match result {
                    Err(DiskError::DiskOngoingReq | DiskError::OutdatedXLMeta) => return Err(StorageError::PreconditionFailed),
                    Ok(next) if next.state == DeleteRecoveryState::Absent && next.current_digest.is_none() => {}
                    Ok(_) => return Err(StorageError::PreconditionFailed),
                    Err(error) => return Err(error.into()),
                }
            }
            if !fence() {
                return Err(StorageError::PreconditionFailed);
            }
            report.reclaimed += 1;
        }
        report.reason = "discarded".to_owned();
        report.fingerprint = None;
        Ok(report)
    }
}
