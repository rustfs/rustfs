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

use std::any::Any;

use uuid::Uuid;

use crate::storage_api::DeletedObject;
use crate::{
    MrfOpKind, MrfReplicateEntry, NULL_VERSION_ID, REPLICATE_EXISTING_DELETE, ReplicateObjectInfo, ReplicationState,
    ReplicationStatusType, ReplicationType, ReplicationWorkerOperation,
};

#[derive(Debug, Clone, Default)]
pub struct DeletedObjectReplicationInfo {
    pub delete_object: DeletedObject,
    pub bucket: String,
    pub event_type: String,
    pub op_type: ReplicationType,
    pub reset_id: String,
    pub target_arn: String,
}

impl DeletedObjectReplicationInfo {
    pub fn admitted_target_arns(&self) -> Vec<String> {
        if !self.target_arn.is_empty() {
            return vec![self.target_arn.clone()];
        }

        let mut target_arns = if !self.delete_object.force_delete_target_arns.is_empty() {
            self.delete_object.force_delete_target_arns.clone()
        } else {
            self.delete_object
                .replication_state
                .as_ref()
                .map(admitted_target_arns_from_replication_state)
                .unwrap_or_default()
        };
        target_arns.sort();
        target_arns.dedup();
        target_arns
    }
}

impl ReplicationWorkerOperation for DeletedObjectReplicationInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_mrf_entry(&self) -> MrfReplicateEntry {
        MrfReplicateEntry {
            bucket: self.bucket.clone(),
            object: self.delete_object.object_name.clone(),
            version_id: self.delete_object.version_id,
            retry_count: 0,
            size: 0,
            op: MrfOpKind::Delete,
            force_delete: self.delete_object.force_delete,
            delete_marker_version_id: self.delete_object.delete_marker_version_id,
            delete_marker: self.delete_object.delete_marker,
            // Persist the original delete-marker mtime as Unix nanoseconds so replay after a
            // restart stamps the replica with the source timestamp rather than the replay time
            // (backlog#867). None when unknown; replay then falls back to the current time.
            delete_marker_mtime: self
                .delete_object
                .delete_marker_mtime
                .and_then(|t| i64::try_from(t.unix_timestamp_nanos()).ok()),
            target_arns: self.admitted_target_arns(),
            force_delete_id: self.delete_object.force_delete_id,
            force_delete_generation: self.delete_object.force_delete_generation,
            force_delete_local_commit: self.delete_object.force_delete,
        }
    }

    fn get_bucket(&self) -> &str {
        &self.bucket
    }

    fn get_object(&self) -> &str {
        &self.delete_object.object_name
    }

    fn get_size(&self) -> i64 {
        0
    }

    fn is_delete_marker(&self) -> bool {
        true
    }

    fn get_op_type(&self) -> ReplicationType {
        self.op_type
    }
}

pub fn is_version_delete_replication(dobj: &DeletedObject) -> bool {
    dobj.version_id.is_some() || (dobj.delete_marker_version_id.is_some() && !dobj.delete_marker)
}

pub fn should_retry_delete_marker_purge(dobj: &DeletedObject) -> bool {
    dobj.delete_marker_version_id.is_some()
}

fn admitted_target_arns_from_replication_state(state: &ReplicationState) -> Vec<String> {
    let mut target_arns = state.targets.keys().cloned().collect::<Vec<_>>();
    target_arns.extend(state.purge_targets.keys().cloned());
    target_arns
}

pub fn is_retryable_delete_replication_head_error(is_not_found: bool, code: Option<&str>) -> bool {
    !(is_not_found || matches!(code, Some("MethodNotAllowed" | "405")))
}

/// Build the delete-replication work item for an existing delete marker or
/// version purge discovered during a resync scan.
pub fn resync_existing_delete_replication_info(roi: &ReplicateObjectInfo, target_arn: &str) -> DeletedObjectReplicationInfo {
    let (version_id, dm_version_id) = if roi.version_purge_status.is_empty() {
        (None, roi.version_id)
    } else {
        (roi.version_id, None)
    };

    DeletedObjectReplicationInfo {
        delete_object: DeletedObject {
            object_name: roi.name.clone(),
            delete_marker_version_id: dm_version_id,
            version_id,
            replication_state: roi.replication_state.clone(),
            delete_marker: roi.delete_marker,
            delete_marker_mtime: roi.mod_time,
            ..Default::default()
        },
        bucket: roi.bucket.clone(),
        event_type: REPLICATE_EXISTING_DELETE.to_string(),
        op_type: ReplicationType::ExistingObject,
        target_arn: target_arn.to_string(),
        ..Default::default()
    }
}

/// Whether a delete replication fully succeeded — the MRF replay acknowledges
/// (drops) an entry exactly when this returns true.
///
/// The delayed purge is deliberately NOT an input: holding the outcome hostage
/// to it (`&& !requires_delayed_purge`) forced `false` for every delete-marker
/// entry and retained them all in the durable MRF journal forever. Purge
/// failures persist their own purge-intent entry instead
/// (`watch_and_purge_source_delete_marker`), and replays of those entries
/// report purge success through `purge_stale_delete_marker_targets`.
pub fn replicate_delete_outcome(
    expected_targets: usize,
    replicated_targets: usize,
    state_persisted: bool,
    source_state_verified: bool,
    replication_status: &ReplicationStatusType,
) -> bool {
    expected_targets > 0
        && replicated_targets == expected_targets
        && state_persisted
        && source_state_verified
        && *replication_status == ReplicationStatusType::Completed
}

pub fn target_delete_version_id(version_id: Uuid, version_purge: bool) -> Option<String> {
    if version_id.is_nil() {
        version_purge.then(|| NULL_VERSION_ID.to_string())
    } else {
        Some(version_id.to_string())
    }
}

/// Which version a delete-marker purge should address on one target.
///
/// `None` means do not purge at all: the recorded mapping disagreed across the
/// dual internal prefixes, and guessing an id could destroy a live version on
/// the target. `Some(id)` is the exact version the target reported when it
/// accepted the marker; falling back to a source-derived id is only correct
/// when the target mirrors source version ids, which a generic S3 target does
/// not.
pub fn delete_marker_purge_version_id(
    state: Option<&ReplicationState>,
    arn: &str,
    delete_marker_version_id: Uuid,
) -> Option<Option<String>> {
    if state.is_some_and(|state| state.target_delete_marker_version_ids_corrupt) {
        return None;
    }
    let recorded = state.and_then(|state| state.target_delete_marker_version_ids.get(arn).cloned());
    Some(match recorded {
        Some(version_id) => Some(version_id),
        None => target_delete_version_id(delete_marker_version_id, true),
    })
}

/// Shape an exhausted purge intent as a marker-creation delete entry. Replay
/// reconstructs it with `delete_marker: true`, finds the source marker gone,
/// and funnels into the stale-marker branch of `replicate_delete_with_outcome`
/// — which re-runs the purge without touching source state and reports purge
/// success as the replay outcome.
pub fn delete_marker_purge_mrf_entry(dobj: &DeletedObjectReplicationInfo, failed_arns: Vec<String>) -> MrfReplicateEntry {
    let mut entry = dobj.to_mrf_entry();
    entry.delete_marker = true;
    entry.version_id = None;
    entry.retry_count = 0;
    entry.target_arns = failed_arns;
    entry
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        DeletedObjectReplicationInfo, delete_marker_purge_mrf_entry, delete_marker_purge_version_id,
        is_retryable_delete_replication_head_error, is_version_delete_replication, replicate_delete_outcome,
        should_retry_delete_marker_purge, target_delete_version_id,
    };
    use crate::storage_api::DeletedObject;
    use crate::{
        MrfOpKind, NULL_VERSION_ID, ReplicationState, ReplicationStatusType, ReplicationType, ReplicationWorkerOperation,
        VersionPurgeStatusType,
    };
    use uuid::Uuid;

    #[test]
    fn deleted_object_replication_info_encodes_delete_mrf_entry() {
        let version_id = Uuid::new_v4();
        let delete_marker_version_id = Uuid::new_v4();
        let mtime = time::OffsetDateTime::from_unix_timestamp_nanos(1_705_312_200_123_456_789).expect("valid mtime");
        let info = DeletedObjectReplicationInfo {
            bucket: "bucket".to_string(),
            op_type: ReplicationType::Delete,
            delete_object: DeletedObject {
                object_name: "object".to_string(),
                version_id: Some(version_id),
                delete_marker_version_id: Some(delete_marker_version_id),
                delete_marker: true,
                delete_marker_mtime: Some(mtime),
                ..Default::default()
            },
            target_arn: "arn:target-a".to_string(),
            ..Default::default()
        };

        let entry = info.to_mrf_entry();

        assert_eq!(entry.bucket, "bucket");
        assert_eq!(entry.object, "object");
        assert_eq!(entry.version_id, Some(version_id));
        assert!(!entry.force_delete);
        assert_eq!(entry.delete_marker_version_id, Some(delete_marker_version_id));
        assert_eq!(entry.op, MrfOpKind::Delete);
        assert!(entry.delete_marker);
        // The original mtime must be persisted (as Unix nanos) so replay keeps the source
        // timestamp instead of stamping the replica with the replay time (backlog#867).
        assert_eq!(
            entry.delete_marker_mtime,
            Some(mtime.unix_timestamp_nanos() as i64),
            "delete-marker mtime must be persisted in the MRF entry"
        );
        assert_eq!(entry.target_arns, vec!["arn:target-a".to_string()]);
        assert_eq!(info.get_object(), "object");
    }

    #[test]
    fn deleted_object_replication_info_without_mtime_yields_none() {
        // Absent source mtime must persist as None so replay falls back to the current time,
        // preserving pre-#867 behaviour.
        let info = DeletedObjectReplicationInfo {
            bucket: "bucket".to_string(),
            delete_object: DeletedObject {
                object_name: "object".to_string(),
                delete_marker: true,
                delete_marker_mtime: None,
                ..Default::default()
            },
            ..Default::default()
        };

        assert_eq!(info.to_mrf_entry().delete_marker_mtime, None);
        assert!(info.to_mrf_entry().target_arns.is_empty());
        assert!(!info.to_mrf_entry().force_delete);
    }

    #[test]
    fn deleted_object_replication_info_uses_explicit_target_over_replay_state() {
        let info = DeletedObjectReplicationInfo {
            bucket: "bucket".to_string(),
            delete_object: DeletedObject {
                object_name: "object".to_string(),
                replication_state: Some(ReplicationState {
                    targets: HashMap::from([("arn:target-state".to_string(), ReplicationStatusType::Pending)]),
                    purge_targets: HashMap::from([("arn:purge-state".to_string(), VersionPurgeStatusType::Pending)]),
                    ..Default::default()
                }),
                ..Default::default()
            },
            target_arn: "arn:target-explicit".to_string(),
            ..Default::default()
        };

        assert_eq!(info.to_mrf_entry().target_arns, vec!["arn:target-explicit".to_string()]);
    }

    #[test]
    fn deleted_object_replication_info_serializes_replay_targets_when_target_is_implicit() {
        let info = DeletedObjectReplicationInfo {
            bucket: "bucket".to_string(),
            delete_object: DeletedObject {
                object_name: "object".to_string(),
                replication_state: Some(ReplicationState {
                    targets: HashMap::from([("arn:target-b".to_string(), ReplicationStatusType::Pending)]),
                    purge_targets: HashMap::from([
                        ("arn:target-a".to_string(), VersionPurgeStatusType::Pending),
                        ("arn:target-b".to_string(), VersionPurgeStatusType::Complete),
                    ]),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        };

        assert_eq!(
            info.to_mrf_entry().target_arns,
            vec!["arn:target-a".to_string(), "arn:target-b".to_string()],
            "MRF deletes must preserve the admitted target identities in stable order"
        );
    }

    #[test]
    fn deleted_object_replication_info_preserves_force_delete_handoff() {
        let operation_id = Uuid::new_v4();
        let info = DeletedObjectReplicationInfo {
            bucket: "bucket".to_string(),
            delete_object: DeletedObject {
                object_name: "prefix/".to_string(),
                force_delete: true,
                force_delete_id: Some(operation_id),
                force_delete_target_arns: vec![
                    "arn:target-b".to_string(),
                    "arn:target-a".to_string(),
                    "arn:target-b".to_string(),
                ],
                force_delete_generation: Some(17),
                ..Default::default()
            },
            ..Default::default()
        };

        let entry = info.to_mrf_entry();

        assert!(entry.force_delete);
        assert_eq!(entry.force_delete_id, Some(operation_id));
        assert_eq!(entry.force_delete_generation, Some(17));
        assert!(entry.force_delete_local_commit);
        assert_eq!(entry.target_arns, vec!["arn:target-a", "arn:target-b"]);
    }

    #[test]
    fn version_delete_replication_tracks_delete_marker_version_purge() {
        let dobj = DeletedObject {
            delete_marker: false,
            delete_marker_version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        assert!(is_version_delete_replication(&dobj));
    }

    #[test]
    fn version_delete_replication_tracks_explicit_version_id() {
        let dobj = DeletedObject {
            version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        assert!(is_version_delete_replication(&dobj));
    }

    #[test]
    fn version_delete_replication_keeps_delete_marker_creation_separate() {
        let dobj = DeletedObject {
            delete_marker: true,
            delete_marker_version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        assert!(!is_version_delete_replication(&dobj));
    }

    #[test]
    fn delete_marker_purge_retry_covers_version_purge_and_marker_creation() {
        let version_purge = DeletedObject {
            delete_marker: false,
            delete_marker_version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };
        let marker_creation = DeletedObject {
            delete_marker: true,
            delete_marker_version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        assert!(should_retry_delete_marker_purge(&version_purge));
        assert!(should_retry_delete_marker_purge(&marker_creation));

        let marker_without_version = DeletedObject {
            delete_marker: true,
            ..Default::default()
        };
        assert!(!should_retry_delete_marker_purge(&marker_without_version));
    }

    #[test]
    fn retryable_delete_replication_head_error_allows_expected_delete_marker_responses() {
        assert!(!is_retryable_delete_replication_head_error(false, Some("405")));
        assert!(!is_retryable_delete_replication_head_error(false, Some("MethodNotAllowed")));
        assert!(!is_retryable_delete_replication_head_error(true, Some("NoSuchKey")));
        assert!(is_retryable_delete_replication_head_error(false, Some("AccessDenied")));
    }

    /// P1-21 regression guard for the outcome formula. A fully successful
    /// delete-marker replication must acknowledge its MRF entry: the formula
    /// once carried `&& !requires_delayed_purge`, which pinned every
    /// delete-marker entry to Missed and retained the whole backlog forever.
    /// (Deterministically staging a marker-creation entry in the durable
    /// journal from e2e would require saturating the worker queues, so the
    /// formula is pinned here instead; the purge-intent replay half is pinned
    /// by the delayed-purge e2e pair.)
    #[test]
    fn test_replicate_delete_outcome_is_not_held_hostage_by_the_delayed_purge() {
        assert!(
            replicate_delete_outcome(1, 1, true, true, &ReplicationStatusType::Completed),
            "a completed delete-marker replication must be acknowledgeable even though a delayed purge watch is pending"
        );
        assert!(!replicate_delete_outcome(0, 0, true, true, &ReplicationStatusType::Completed));
        assert!(!replicate_delete_outcome(2, 1, true, true, &ReplicationStatusType::Completed));
        assert!(!replicate_delete_outcome(1, 1, false, true, &ReplicationStatusType::Completed));
        assert!(!replicate_delete_outcome(1, 1, true, false, &ReplicationStatusType::Completed));
        assert!(!replicate_delete_outcome(1, 1, true, true, &ReplicationStatusType::Failed));
    }

    #[test]
    fn test_delete_marker_purge_mrf_entry_replays_through_the_stale_marker_branch() {
        let delete_marker_version_id = Uuid::new_v4();
        let dobj = DeletedObjectReplicationInfo {
            delete_object: DeletedObject {
                object_name: "doc.txt".to_string(),
                // A version-purge flavored source event: the entry must still
                // be reshaped as a marker-creation delete so replay funnels
                // into the stale-marker branch instead of re-running the full
                // delete replication (whose source-state stamping would fail
                // against the already-purged version).
                delete_marker: false,
                version_id: Some(Uuid::new_v4()),
                delete_marker_version_id: Some(delete_marker_version_id),
                ..Default::default()
            },
            bucket: "bucket-a".to_string(),
            ..Default::default()
        };

        let entry = delete_marker_purge_mrf_entry(&dobj, vec!["arn:a".to_string()]);

        assert!(entry.delete_marker, "purge intents must replay as marker-creation deletes");
        assert_eq!(entry.version_id, None, "the purged data version must not leak into the replay");
        assert_eq!(entry.delete_marker_version_id, Some(delete_marker_version_id));
        assert_eq!(
            entry.target_arns,
            vec!["arn:a".to_string()],
            "only the targets whose purge failed may be retried"
        );
        assert_eq!(entry.retry_count, 0);
        assert_eq!(entry.bucket, "bucket-a");
        assert_eq!(entry.object, "doc.txt");
    }

    #[test]
    fn target_delete_version_id_preserves_explicit_null_purges() {
        let version_id = Uuid::new_v4();

        assert_eq!(target_delete_version_id(version_id, true), Some(version_id.to_string()));
        assert_eq!(target_delete_version_id(Uuid::nil(), true).as_deref(), Some(NULL_VERSION_ID));
        assert_eq!(target_delete_version_id(Uuid::nil(), false), None);
    }

    #[test]
    fn delete_marker_purge_prefers_the_recorded_target_version() {
        let source = Uuid::new_v4();
        let arn = "arn:rustfs:replication::target:bucket";

        // No recorded mapping: fall back to deriving from the source uuid.
        assert_eq!(delete_marker_purge_version_id(None, arn, source), Some(Some(source.to_string())));

        // Recorded mapping wins — a generic S3 target assigns its own id, so the
        // derived one would purge the wrong version or nothing at all.
        let mut state = ReplicationState::default();
        state
            .target_delete_marker_version_ids
            .insert(arn.to_string(), "target-assigned-id".to_string());
        assert_eq!(
            delete_marker_purge_version_id(Some(&state), arn, source),
            Some(Some("target-assigned-id".to_string()))
        );

        // A mapping recorded for a different ARN must not be reused.
        assert_eq!(
            delete_marker_purge_version_id(Some(&state), "arn:rustfs:replication::other:bucket", source),
            Some(Some(source.to_string()))
        );

        // Inconsistent persisted metadata: refuse to purge rather than guess.
        let mut corrupt = state.clone();
        corrupt.target_delete_marker_version_ids_corrupt = true;
        assert_eq!(delete_marker_purge_version_id(Some(&corrupt), arn, source), None);
    }
}
