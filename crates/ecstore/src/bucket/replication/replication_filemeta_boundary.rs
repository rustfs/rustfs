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

pub use rustfs_replication::{MrfOpKind, MrfReplicateEntry};
pub(crate) use rustfs_replication::{
    REPLICATE_EXISTING, REPLICATE_HEAL_DELETE, ReplicateTargetDecision, ReplicatedInfos, ReplicatedTargetInfo, ReplicationAction,
    ReplicationWorkerOperation, ResyncDecision, get_replication_state, parse_replicate_decision,
    replicate_decision_for_admitted_targets, target_reset_header, version_purge_statuses_map,
};
pub use rustfs_replication::{
    REPLICATE_INCOMING_DELETE, ReplicateDecision, ReplicateObjectInfo, ReplicationState, ReplicationStatusType, ReplicationType,
    VersionPurgeStatusType, replication_statuses_map,
};

pub(crate) fn replication_status_from_filemeta(status: rustfs_filemeta::ReplicationStatusType) -> ReplicationStatusType {
    ReplicationStatusType::from(status.as_str())
}

pub(crate) fn version_purge_status_from_filemeta(status: rustfs_filemeta::VersionPurgeStatusType) -> VersionPurgeStatusType {
    VersionPurgeStatusType::from(status.as_str())
}

pub(crate) fn replication_state_from_filemeta(state: &rustfs_filemeta::ReplicationState) -> ReplicationState {
    ReplicationState {
        replica_timestamp: state.replica_timestamp,
        replica_status: replication_status_from_filemeta(state.replica_status.clone()),
        delete_marker: state.delete_marker,
        replication_timestamp: state.replication_timestamp,
        replication_status_internal: state.replication_status_internal.clone(),
        version_purge_status_internal: state.version_purge_status_internal.clone(),
        replicate_decision_str: state.replicate_decision_str.clone(),
        targets: state
            .targets
            .iter()
            .map(|(arn, status)| (arn.clone(), replication_status_from_filemeta(status.clone())))
            .collect(),
        purge_targets: state
            .purge_targets
            .iter()
            .map(|(arn, status)| (arn.clone(), version_purge_status_from_filemeta(status.clone())))
            .collect(),
        reset_statuses_map: state.reset_statuses_map.clone(),
        target_delete_marker_version_ids: state.target_delete_marker_version_ids.clone(),
        target_delete_marker_version_ids_corrupt: state.target_delete_marker_version_ids_corrupt,
    }
}

pub fn replication_status_to_filemeta(status: ReplicationStatusType) -> rustfs_filemeta::ReplicationStatusType {
    rustfs_filemeta::ReplicationStatusType::from(status.as_str())
}

pub fn version_purge_status_to_filemeta(status: VersionPurgeStatusType) -> rustfs_filemeta::VersionPurgeStatusType {
    rustfs_filemeta::VersionPurgeStatusType::from(status.as_str())
}

pub fn replication_state_to_filemeta(state: &ReplicationState) -> rustfs_filemeta::ReplicationState {
    rustfs_filemeta::ReplicationState {
        replica_timestamp: state.replica_timestamp,
        replica_status: replication_status_to_filemeta(state.replica_status.clone()),
        delete_marker: state.delete_marker,
        replication_timestamp: state.replication_timestamp,
        replication_status_internal: state.replication_status_internal.clone(),
        version_purge_status_internal: state.version_purge_status_internal.clone(),
        replicate_decision_str: state.replicate_decision_str.clone(),
        targets: state
            .targets
            .iter()
            .map(|(arn, status)| (arn.clone(), replication_status_to_filemeta(status.clone())))
            .collect(),
        purge_targets: state
            .purge_targets
            .iter()
            .map(|(arn, status)| (arn.clone(), version_purge_status_to_filemeta(status.clone())))
            .collect(),
        reset_statuses_map: state.reset_statuses_map.clone(),
        target_delete_marker_version_ids: state.target_delete_marker_version_ids.clone(),
        target_delete_marker_version_ids_corrupt: state.target_delete_marker_version_ids_corrupt,
    }
}

// Reconciliation tests for the deliberately duplicated wire types.
//
// `rustfs-filemeta` (xl.meta disk format) and `rustfs-replication` (MRF/resync
// persistence format) each own a copy of `ReplicationStatusType`,
// `VersionPurgeStatusType` and `ReplicationState`; the conversions above hop
// between them via `as_str()`, whose `From<&str>` impls fall back to `Empty`
// on any unknown token. That fallback silently degrades data the moment one
// side gains a variant the other lacks, so these tests pin the two sides
// together:
//
// - the `match` statements are exhaustive with no `_` arm — adding a variant
//   on either side fails compilation here until the mapping is reconsidered;
// - the round-trips assert the string token survives both directions — a
//   variant whose token the other side does not recognize fails the assert
//   instead of quietly becoming `Empty`.
//
// Struct-shaped drift on `ReplicationState` is already compile-guarded by the
// exhaustive struct literals in the two conversion functions above; the
// round-trip test below additionally pins value fidelity for every field.
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn replication_status_variants_round_trip_across_boundary() {
        use rustfs_replication::ReplicationStatusType as Repl;

        let all = [
            Repl::Pending,
            Repl::Completed,
            Repl::CompletedLegacy,
            Repl::Failed,
            Repl::Replica,
            Repl::Empty,
        ];
        for status in all {
            // Exhaustive on the replication side: a new variant breaks this match.
            match status {
                Repl::Pending | Repl::Completed | Repl::CompletedLegacy | Repl::Failed | Repl::Replica | Repl::Empty => {}
            }
            let filemeta = replication_status_to_filemeta(status.clone());
            assert_eq!(
                filemeta.as_str(),
                status.as_str(),
                "replication->filemeta conversion must not degrade {status:?} (unknown tokens fall back to Empty)"
            );
            assert_eq!(replication_status_from_filemeta(filemeta), status);
        }

        // Exhaustive on the filemeta side: a new variant breaks this match.
        fn _filemeta_side_is_covered(status: rustfs_filemeta::ReplicationStatusType) {
            use rustfs_filemeta::ReplicationStatusType as Meta;
            match status {
                Meta::Pending | Meta::Completed | Meta::CompletedLegacy | Meta::Failed | Meta::Replica | Meta::Empty => {}
            }
        }
    }

    #[test]
    fn version_purge_status_variants_round_trip_across_boundary() {
        use rustfs_replication::VersionPurgeStatusType as Repl;

        let all = [Repl::Pending, Repl::Complete, Repl::Failed, Repl::Empty];
        for status in all {
            // Exhaustive on the replication side: a new variant breaks this match.
            match status {
                Repl::Pending | Repl::Complete | Repl::Failed | Repl::Empty => {}
            }
            let filemeta = version_purge_status_to_filemeta(status.clone());
            assert_eq!(
                filemeta.as_str(),
                status.as_str(),
                "replication->filemeta conversion must not degrade {status:?} (unknown tokens fall back to Empty)"
            );
            assert_eq!(version_purge_status_from_filemeta(filemeta), status);
        }

        // Exhaustive on the filemeta side: a new variant breaks this match.
        fn _filemeta_side_is_covered(status: rustfs_filemeta::VersionPurgeStatusType) {
            use rustfs_filemeta::VersionPurgeStatusType as Meta;
            match status {
                Meta::Pending | Meta::Complete | Meta::Failed | Meta::Empty => {}
            }
        }
    }

    #[test]
    fn replication_state_round_trips_every_field_across_boundary() {
        let timestamp = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
        let state = ReplicationState {
            replica_timestamp: Some(timestamp),
            replica_status: ReplicationStatusType::Replica,
            delete_marker: true,
            replication_timestamp: Some(timestamp),
            replication_status_internal: Some("arn:a=PENDING;".to_string()),
            version_purge_status_internal: Some("arn:a=FAILED;".to_string()),
            replicate_decision_str: "arn:a=true;false;;".to_string(),
            targets: HashMap::from([
                ("arn:a".to_string(), ReplicationStatusType::Completed),
                ("arn:b".to_string(), ReplicationStatusType::Failed),
            ]),
            purge_targets: HashMap::from([("arn:a".to_string(), VersionPurgeStatusType::Pending)]),
            reset_statuses_map: HashMap::from([("reset-arn:a".to_string(), "reset-id;ts".to_string())]),
            target_delete_marker_version_ids: HashMap::from([("arn:a".to_string(), "version-1".to_string())]),
            target_delete_marker_version_ids_corrupt: true,
        };

        let round_tripped = replication_state_from_filemeta(&replication_state_to_filemeta(&state));
        assert_eq!(round_tripped, state);
    }
}
