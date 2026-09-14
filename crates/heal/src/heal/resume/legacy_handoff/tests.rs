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
use crate::heal::{HEALING_MARKER_PATH, resume::tests::schema_test_disk};

#[tokio::test]
async fn legacy_migration_preserves_both_orphans_and_starts_a_fresh_scan() {
    let (_anchor_dir, anchor) = schema_test_disk().await;
    let (_target_dir, target) = schema_test_disk().await;
    let old_identity = ReplacementTargetIdentity {
        endpoint: "replacement".to_string(),
        canonical_path: "/mnt/replacement".to_string(),
        physical_device_ids: vec!["device".to_string()],
        filesystem_identity: "old-mount:fs:root".to_string(),
    };
    let mut sources = Vec::new();
    let mut originals = Vec::new();
    for schema in [5, 6] {
        let id = Uuid::new_v4().to_string();
        let mut state = ResumeState::replacement_intent(
            id.clone(),
            "erasure_set".to_string(),
            "pool_0_set_0".to_string(),
            vec![format!("bucket-{schema}")],
            vec!["replacement".to_string()],
            vec![old_identity.clone()],
        );
        state.schema_version = schema;
        state.replacement_execution_protocol = 0;
        state.replacement_phase = if schema == 5 {
            ReplacementPhase::Abandoned
        } else {
            ReplacementPhase::Rebuilding
        };
        state.resume_cursor = Some("legacy-position".to_string());
        state.processed_objects = 80;
        let raw = EcstoreDiskBytes::from(serde_json::to_vec(&state).expect("legacy fixture"));
        ensure_replacement_recovery_dir(&anchor).await.expect("intent directory");
        anchor
            .write_all(
                RUSTFS_META_BUCKET,
                path_to_str(&ResumeStateFile::ReplacementIntent.path(&id)).expect("intent path"),
                raw.clone(),
            )
            .await
            .expect("legacy intent");
        assert!(
            ResumeManager::load_replacement_intent(anchor.clone(), &id).await.is_err(),
            "ordinary loading must refuse unfenced legacy executors"
        );
        sources.push(LegacyReplacementSource {
            task_id: id,
            sha256: HashAlgorithm::SHA256.hash_encode(&raw).as_ref().to_vec(),
        });
        originals.push(raw);
    }
    let old_marker = format!("pool_0_set_0:{}", sources[0].task_id);
    target
        .write_all(RUSTFS_META_BUCKET, HEALING_MARKER_PATH, old_marker.clone().into())
        .await
        .expect("orphan marker");
    let approval = LegacyReplacementApproval {
        schema_version: 1,
        successor: Uuid::new_v4().to_string(),
        set_disk_id: "pool_0_set_0".to_string(),
        targets: vec!["replacement".to_string()],
        expected_markers: vec![Some(old_marker)],
        sources,
        maintenance_assertion: STOPPED_WRITERS_ASSERTION.to_string(),
    };
    let mut current = old_identity;
    current.filesystem_identity = "new-mount:fs:root".to_string();
    let execution = ReplacementExecution::for_test(vec![target], vec![current]);
    let mut bad = approval.clone();
    bad.maintenance_assertion.clear();
    assert!(
        ResumeUtils::import_legacy_replacement(&anchor, &bad, &execution, Vec::new())
            .await
            .is_err()
    );
    bad = approval.clone();
    bad.sources[0].sha256[0] ^= 1;
    assert!(
        ResumeUtils::import_legacy_replacement(&anchor, &bad, &execution, Vec::new())
            .await
            .is_err()
    );
    let child = ResumeUtils::import_legacy_replacement(&anchor, &approval, &execution, vec!["new-bucket".to_string()])
        .await
        .expect("approved migration");
    // Crash before consuming the approval must reuse the already reserved UUID.
    let replay = ResumeUtils::import_legacy_replacement(&anchor, &approval, &execution, vec!["new-bucket".to_string()])
        .await
        .expect("replay import");
    let state = replay.get_state().await;
    assert_eq!(state.task_id, approval.successor);
    assert_eq!(state.replacement_phase, ReplacementPhase::OwnershipPending);
    assert_eq!(state.pending_buckets, ["bucket-5", "bucket-6", "new-bucket"]);
    assert_eq!(state.processed_objects, 0);
    assert!(state.resume_cursor.is_none());
    assert!(state.replacement_lineage.is_empty(), "migration must not invent historical handoff edges");
    assert_eq!(state.replacement_legacy_import.as_ref(), Some(&approval));
    for (source, original) in approval.sources.iter().zip(originals) {
        let archive = replacement_recovery_dir().join(format!("{}_{}_legacy_original.json", approval.successor, source.task_id));
        assert_eq!(
            anchor
                .read_all(RUSTFS_META_BUCKET, path_to_str(&archive).expect("archive path"))
                .await
                .expect("original bytes"),
            original
        );
        let retired = ResumeManager::load_replacement_intent(anchor.clone(), &source.task_id)
            .await
            .expect("retired source")
            .get_state()
            .await;
        assert_eq!(retired.replacement_phase, ReplacementPhase::Abandoned);
        assert_eq!(retired.replacement_legacy_successor.as_deref(), Some(approval.successor.as_str()));
    }
    child
        .acquire_replacement_markers(&execution)
        .await
        .expect("transfer approved marker");
    assert_eq!(
        execution.markers().await.expect("new marker"),
        [Some(format!("pool_0_set_0:{}", approval.successor))]
    );
    child
        .mark_replacement_rebuilding(execution.identities().to_vec())
        .await
        .expect("start fresh scan");
    child
        .mark_replacement_completed_and_verified()
        .await
        .expect("fresh scan complete");
    assert_eq!(
        child
            .ensure_replacement_completion_proof()
            .await
            .expect("migration proof")
            .replacement_legacy_import,
        Some(approval)
    );
}

#[test]
fn legacy_approval_rejects_unknown_fields_paths_and_owners() {
    let approval = LegacyReplacementApproval {
        schema_version: 1,
        successor: Uuid::new_v4().to_string(),
        set_disk_id: "pool_0_set_0".to_string(),
        targets: vec!["replacement".to_string()],
        expected_markers: vec![None],
        sources: vec![LegacyReplacementSource {
            task_id: Uuid::new_v4().to_string(),
            sha256: vec![0; 32],
        }],
        maintenance_assertion: STOPPED_WRITERS_ASSERTION.to_string(),
    };
    approval.validate().expect("valid maintenance approval");
    let mut bad = approval.clone();
    bad.sources[0].task_id = "../escape".to_string();
    assert!(bad.validate().is_err());
    bad = approval.clone();
    bad.expected_markers[0] = Some(format!("pool_0_set_0:{}", Uuid::new_v4()));
    assert!(bad.validate().is_err());
    let mut json = serde_json::to_value(approval).expect("approval JSON");
    json["unexpected"] = true.into();
    assert!(serde_json::from_value::<LegacyReplacementApproval>(json).is_err());
}
