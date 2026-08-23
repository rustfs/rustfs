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

use super::checkpoint::CURRENT_CHECKPOINT_SCHEMA;
use super::replacement::ReplacementCompletionProof;
use super::*;

async fn schema_test_disk() -> (tempfile::TempDir, DiskStore) {
    use super::super::{DiskOption, Endpoint, new_disk};

    let temp_dir = tempfile::TempDir::new().expect("create schema test directory");
    let endpoint = Endpoint::try_from(temp_dir.path().to_string_lossy().as_ref()).expect("create schema test disk endpoint");
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("create schema test disk");
    match disk.make_volume(RUSTFS_META_BUCKET).await {
        Ok(()) | Err(DiskError::VolumeExists) => {}
        Err(error) => panic!("create metadata volume: {error}"),
    }
    match disk.make_volume(&format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}")).await {
        Ok(()) | Err(DiskError::VolumeExists) => {}
        Err(error) => panic!("create resume metadata volume: {error}"),
    }

    (temp_dir, disk)
}

#[tokio::test]
async fn test_resume_state_creation() {
    let task_id = ResumeUtils::generate_task_id();
    let buckets = vec!["bucket1".to_string(), "bucket2".to_string()];
    let state = ResumeState::new(task_id.clone(), "erasure_set".to_string(), "pool_0_set_0".to_string(), buckets);

    assert_eq!(state.task_id, task_id);
    assert_eq!(state.task_type, "erasure_set");
    assert!(!state.completed);
    assert_eq!(state.processed_objects, 0);
    assert_eq!(state.pending_buckets.len(), 2);
}

#[test]
fn replacement_intent_binds_a_generation_before_format() {
    let state = ResumeState::replacement_intent(
        "generation-a".to_string(),
        "erasure_set".to_string(),
        "pool_0_set_0".to_string(),
        vec!["bucket-a".to_string()],
        vec!["replacement-a".to_string()],
        vec![ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        }],
    );

    assert_eq!(state.replacement_generation.as_deref(), Some("generation-a"));
    assert_eq!(state.replacement_phase, ReplacementPhase::Intent);
    assert_eq!(state.replacement_targets, ["replacement-a"]);
    assert!(state.resume_cursor.is_none(), "a new replacement must start from the beginning");

    let mut state = state;
    state.complete_bucket("bucket-a");
    assert_eq!(
        state.replacement_buckets,
        ["bucket-a"],
        "recovery must retain the original positional bucket plan"
    );
}

#[tokio::test]
async fn replacement_terminal_phases_are_durable() {
    use super::super::{DiskOption, Endpoint, new_disk};
    use tempfile::TempDir;

    let temp_dir = TempDir::new().expect("create replacement phase test directory");
    let endpoint = Endpoint::try_from(temp_dir.path().to_string_lossy().as_ref()).expect("create test disk endpoint");
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("create test disk");
    match disk.make_volume(RUSTFS_META_BUCKET).await {
        Ok(()) | Err(DiskError::VolumeExists) => {}
        Err(err) => panic!("create metadata volume for replacement phase test: {err}"),
    }

    let task_id = ResumeUtils::generate_task_id();
    let manager = ResumeManager::new_replacement_intent(
        disk.clone(),
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        }],
    )
    .await
    .expect("replacement intent should persist");
    manager
        .mark_replacement_completed_and_verified()
        .await
        .expect("completion and verified phase must persist together");

    let verified = ResumeManager::load_replacement_intent(disk.clone(), &task_id)
        .await
        .expect("verified phase must survive a restart")
        .get_state()
        .await;
    assert!(verified.completed);
    assert_eq!(verified.replacement_phase, ReplacementPhase::Verified);

    let resumed = ResumeManager::new_replacement_intent(
        disk.clone(),
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["ignored-after-restart".to_string()],
        vec!["replacement-a".to_string()],
        vec![ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        }],
    )
    .await
    .expect("verified replacement must be reopenable for terminal cleanup");
    assert_eq!(
        resumed.get_state().await.replacement_buckets,
        ["bucket"],
        "terminal recovery must preserve the original generation bucket plan"
    );

    manager
        .mark_replacement_cleanup_pending()
        .await
        .expect("cleanup-pending phase must persist after marker removal");
    let cleanup_pending = ResumeManager::load_replacement_intent(disk, &task_id)
        .await
        .expect("cleanup-pending phase must survive a restart")
        .get_state()
        .await;
    assert!(cleanup_pending.completed);
    assert_eq!(cleanup_pending.replacement_phase, ReplacementPhase::CleanupPending);
}

#[tokio::test]
async fn replacement_proof_before_verified_state_is_reconciled_after_restart() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let manager = ResumeManager::new_replacement_intent(
        disk.clone(),
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        }],
    )
    .await
    .expect("replacement intent should persist");

    let proof = ReplacementCompletionProof::from_state(&manager.get_state().await, 42)
        .expect("active replacement state should build a completion proof");
    let proof_path = replacement_completion_proof_path(&task_id);
    disk.write_all(
        RUSTFS_META_BUCKET,
        proof_path.to_str().expect("completion proof path must be UTF-8"),
        serde_json::to_vec(&proof)
            .expect("completion proof fixture should serialize")
            .into(),
    )
    .await
    .expect("proof-first crash fixture should persist");

    let recovered = ResumeManager::load_replacement_intent(disk.clone(), &task_id)
        .await
        .expect("matching completion proof must prevent another rebuild")
        .get_state()
        .await;
    assert!(recovered.completed);
    assert_eq!(recovered.replacement_phase, ReplacementPhase::Verified);
    assert_eq!(recovered.last_update, proof.verified_at);

    let records = ResumeUtils::get_replacement_recovery_records(&disk)
        .await
        .expect("reconciled replacement state should be observable");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].state, ReplacementRecoveryState::CleanupPending);
}

#[tokio::test]
async fn replacement_proof_conflicting_with_active_state_fails_closed_after_restart() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let manager = ResumeManager::new_replacement_intent(
        disk.clone(),
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        }],
    )
    .await
    .expect("replacement intent should persist");

    let mut proof = ReplacementCompletionProof::from_state(&manager.get_state().await, 42)
        .expect("active replacement state should build a completion proof");
    proof.set_disk_id = "pool_0_set_1".to_string();
    let proof_path = replacement_completion_proof_path(&task_id);
    disk.write_all(
        RUSTFS_META_BUCKET,
        proof_path.to_str().expect("completion proof path must be UTF-8"),
        serde_json::to_vec(&proof)
            .expect("completion proof fixture should serialize")
            .into(),
    )
    .await
    .expect("conflicting proof fixture should persist");

    let error = match ResumeManager::load_replacement_intent(disk, &task_id).await {
        Ok(_) => panic!("a mismatched proof must not permit another rebuild"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("does not match active intent"));
}

#[tokio::test]
async fn replacement_intent_is_not_an_ordinary_resumable_task() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let manager = ResumeManager::new_replacement_intent(
        disk.clone(),
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        }],
    )
    .await
    .expect("replacement intent should persist in its isolated namespace");

    assert!(
        !ResumeManager::has_resume_state(&disk, &task_id).await,
        "an old ordinary-resume lookup must not discover a replacement intent"
    );
    assert!(ResumeManager::has_replacement_intent(&disk, &task_id).await);
    assert!(
        !ResumeUtils::get_resumable_tasks(&disk)
            .await
            .expect("ordinary resume listing should succeed")
            .contains(&task_id),
        "the old filename enumeration must not return replacement work"
    );
    assert_eq!(
        ResumeUtils::get_replacement_intent_tasks(&disk)
            .await
            .expect("replacement intent listing should succeed"),
        vec![task_id.clone()]
    );
    assert_eq!(
        ResumeManager::load_replacement_intent(disk, &task_id)
            .await
            .expect("new replacement reader should load the isolated state")
            .get_state()
            .await,
        manager.get_state().await
    );
}

#[tokio::test]
async fn replacement_intent_recovers_from_torn_publication_before_formatting() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let intent_path = ResumeStateFile::ReplacementIntent.path(&task_id);
    disk.write_all(
        RUSTFS_META_BUCKET,
        intent_path.to_str().expect("replacement intent path must be UTF-8"),
        b"{torn replacement intent".as_slice().into(),
    )
    .await
    .expect("torn replacement intent fixture should persist");

    let manager = ResumeManager::new_replacement_intent(
        disk.clone(),
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        }],
    )
    .await
    .expect("a retry must atomically replace only a torn pre-format intent");

    let recovered = ResumeManager::load_replacement_intent(disk, &task_id)
        .await
        .expect("recovered intent should be readable after restart")
        .get_state()
        .await;
    assert_eq!(recovered, manager.get_state().await);
    assert_eq!(recovered.replacement_phase, ReplacementPhase::Intent);
}

#[tokio::test]
async fn torn_intent_recovery_cas_preserves_a_concurrent_valid_binding() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let intent_path = ResumeStateFile::ReplacementIntent.path(&task_id);
    let intent_path = intent_path.to_str().expect("replacement intent path must be UTF-8");
    let torn = EcstoreDiskBytes::from_static(b"{torn replacement intent");
    disk.write_all(RUSTFS_META_BUCKET, intent_path, torn)
        .await
        .expect("torn intent fixture should persist");

    let expected = ResumeManager::torn_replacement_intent_bytes(&disk, &task_id)
        .await
        .expect("torn intent should be recoverable before a seal exists")
        .expect("torn intent bytes should be retained as the CAS precondition");
    let winner = ResumeState::replacement_intent(
        task_id.clone(),
        "erasure_set".to_string(),
        "pool_0_set_winner".to_string(),
        vec!["winner-bucket".to_string()],
        vec!["replacement-winner".to_string()],
        vec![ReplacementTargetIdentity {
            endpoint: "replacement-winner".to_string(),
            canonical_path: "/mnt/replacement-winner".to_string(),
            physical_device_ids: vec!["device-winner".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        }],
    );
    let winner_bytes = EcstoreDiskBytes::from(serde_json::to_vec(&winner).expect("winner intent should serialize"));
    disk.write_all(RUSTFS_META_BUCKET, intent_path, winner_bytes.clone())
        .await
        .expect("concurrent valid intent fixture should persist");

    let loser = ResumeManager {
        disk: disk.clone(),
        state: Arc::new(RwLock::new(ResumeState::replacement_intent(
            task_id.clone(),
            "erasure_set".to_string(),
            "pool_0_set_loser".to_string(),
            vec!["loser-bucket".to_string()],
            vec!["replacement-loser".to_string()],
            vec![ReplacementTargetIdentity {
                endpoint: "replacement-loser".to_string(),
                canonical_path: "/mnt/replacement-loser".to_string(),
                physical_device_ids: vec!["device-loser".to_string()],
                filesystem_identity: "4:5:6".to_string(),
            }],
        ))),
        throttle: Mutex::new(PersistThrottle::new()),
        state_file: ResumeStateFile::ReplacementIntent,
    };
    let error = match loser.publish_new_replacement_intent(Some(expected)).await {
        Ok(()) => panic!("a stale torn-intent recovery must not overwrite a concurrent valid binding"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("changed before publication"));
    assert_eq!(
        disk.read_all(RUSTFS_META_BUCKET, intent_path)
            .await
            .expect("concurrent valid intent must remain durable"),
        winner_bytes
    );
}

#[tokio::test]
async fn replacement_intent_does_not_recreate_torn_state_after_seal_publication() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let intent_path = ResumeStateFile::ReplacementIntent.path(&task_id);
    let intent_path = intent_path.to_str().expect("replacement intent path must be UTF-8");
    let torn = b"{torn replacement intent";
    disk.write_all(RUSTFS_META_BUCKET, intent_path, torn.as_slice().into())
        .await
        .expect("torn replacement intent fixture should persist");
    let marker_path = replacement_intent_seal_path(&task_id);
    disk.write_all(
        RUSTFS_META_BUCKET,
        marker_path.to_str().expect("replacement seal path must be UTF-8"),
        b"sealed".as_slice().into(),
    )
    .await
    .expect("replacement seal fixture should persist");

    let error = match ResumeManager::new_replacement_intent(
        disk.clone(),
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        }],
    )
    .await
    {
        Ok(_) => panic!("a seal means a torn state may have crossed the format boundary"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("Failed to deserialize resume state"));
    assert_eq!(
        disk.read_all(RUSTFS_META_BUCKET, intent_path)
            .await
            .expect("torn intent must remain for operator recovery"),
        torn.as_slice()
    );
}

#[tokio::test]
async fn replacement_intent_migrates_from_legacy_resume_filename() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let legacy = ResumeState::replacement_intent(
        task_id.clone(),
        "erasure_set".to_string(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        }],
    );
    let legacy_path = ResumeStateFile::Ordinary.path(&task_id);
    disk.write_all(
        RUSTFS_META_BUCKET,
        legacy_path.to_str().expect("legacy resume path must be UTF-8"),
        serde_json::to_vec(&legacy)
            .expect("serialize legacy replacement state")
            .into(),
    )
    .await
    .expect("write legacy replacement state");

    let migrated = ResumeManager::load_replacement_intent(disk.clone(), &task_id)
        .await
        .expect("new binary should migrate a legacy replacement state");
    assert_eq!(migrated.get_state().await, legacy);
    assert!(
        !ResumeManager::has_resume_state(&disk, &task_id).await,
        "migration must remove the old-binary-visible state only after the new state is durable"
    );
    assert!(ResumeManager::has_replacement_intent(&disk, &task_id).await);
}

#[tokio::test]
async fn ordinary_targeted_resume_is_not_migrated_as_a_replacement_intent() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let manager = ResumeManager::new(
        disk.clone(),
        task_id.clone(),
        "erasure_set".to_string(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
    )
    .await
    .expect("ordinary targeted resume should persist");
    manager
        .set_replacement_targets(vec!["manual-target".to_string()])
        .await
        .expect("ordinary targeted resume should retain its target filter");

    assert!(!ResumeManager::has_replacement_intent(&disk, &task_id).await);
    assert!(ResumeManager::load_replacement_intent(disk.clone(), &task_id).await.is_err());
    assert!(ResumeManager::has_resume_state(&disk, &task_id).await);
}

#[tokio::test]
async fn malformed_isolated_replacement_intent_is_reported_as_unknown() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let malformed = ResumeState::new(
        task_id.clone(),
        "erasure_set".to_string(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
    );
    let path = ResumeStateFile::ReplacementIntent.path(&task_id);
    disk.write_all(
        RUSTFS_META_BUCKET,
        path.to_str().expect("isolated replacement path must be UTF-8"),
        serde_json::to_vec(&malformed)
            .expect("malformed replacement fixture should serialize")
            .into(),
    )
    .await
    .expect("malformed isolated replacement state should persist");

    let records = ResumeUtils::get_replacement_recovery_records(&disk)
        .await
        .expect("isolated replacement listing should succeed");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].task_id, task_id);
    assert_eq!(records[0].state, ReplacementRecoveryState::Unknown);
}

#[tokio::test]
async fn startup_migration_moves_flat_replacement_artifacts_to_dedicated_directory() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let mut state = ResumeState::replacement_intent(
        task_id.clone(),
        "erasure_set".to_string(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        }],
    );
    state.mark_completed();
    state.replacement_phase = ReplacementPhase::Verified;
    let legacy_intent = ResumeStateFile::LegacyReplacementIntent.path(&task_id);
    disk.write_all(
        RUSTFS_META_BUCKET,
        legacy_intent.to_str().expect("legacy intent path must be UTF-8"),
        serde_json::to_vec(&state)
            .expect("serialize legacy replacement intent")
            .into(),
    )
    .await
    .expect("write legacy replacement intent");
    let proof = ReplacementCompletionProof::from_state(&state, state.last_update).expect("build legacy completion proof");
    let legacy_proof = legacy_replacement_completion_proof_path(&task_id);
    disk.write_all(
        RUSTFS_META_BUCKET,
        legacy_proof.to_str().expect("legacy proof path must be UTF-8"),
        serde_json::to_vec(&proof).expect("serialize legacy completion proof").into(),
    )
    .await
    .expect("write legacy completion proof");

    ResumeUtils::migrate_legacy_replacement_records(&disk)
        .await
        .expect("startup migration should move flat replacement artifacts");

    assert_eq!(
        ResumeUtils::get_replacement_intent_tasks(&disk)
            .await
            .expect("dedicated intent listing should succeed"),
        vec![task_id.clone()]
    );
    assert_eq!(
        ResumeManager::load_replacement_completion_proof(disk.clone(), &task_id)
            .await
            .expect("dedicated completion proof should be readable"),
        proof
    );
    assert!(matches!(
        disk.read_all(RUSTFS_META_BUCKET, legacy_intent.to_str().expect("legacy intent path must be UTF-8"))
            .await,
        Err(DiskError::FileNotFound)
    ));
    assert!(matches!(
        disk.read_all(RUSTFS_META_BUCKET, legacy_proof.to_str().expect("legacy proof path must be UTF-8"))
            .await,
        Err(DiskError::FileNotFound)
    ));
}

#[tokio::test]
async fn startup_migration_moves_ordinary_replacement_resume_to_dedicated_directory() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let state = ResumeState::replacement_intent(
        task_id.clone(),
        "erasure_set".to_string(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        }],
    );
    let legacy_resume = ResumeStateFile::Ordinary.path(&task_id);
    disk.write_all(
        RUSTFS_META_BUCKET,
        legacy_resume.to_str().expect("legacy resume path must be UTF-8"),
        serde_json::to_vec(&state)
            .expect("serialize legacy ordinary replacement state")
            .into(),
    )
    .await
    .expect("write legacy ordinary replacement state");

    ResumeUtils::migrate_legacy_replacement_records(&disk)
        .await
        .expect("startup migration should move ordinary replacement state");

    assert_eq!(
        ResumeUtils::get_replacement_intent_tasks(&disk)
            .await
            .expect("dedicated replacement listing should succeed"),
        vec![task_id.clone()]
    );
    assert_eq!(
        ResumeManager::load_replacement_intent(disk.clone(), &task_id)
            .await
            .expect("migrated replacement intent should be readable")
            .get_state()
            .await,
        state
    );
    assert!(matches!(
        disk.read_all(RUSTFS_META_BUCKET, legacy_resume.to_str().expect("legacy resume path must be UTF-8"))
            .await,
        Err(DiskError::FileNotFound)
    ));
}

#[tokio::test]
async fn startup_migration_reports_corrupt_ordinary_replacement_candidate() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let legacy_resume = ResumeStateFile::Ordinary.path(&task_id);
    let legacy_resume = legacy_resume.to_str().expect("legacy resume path must be UTF-8");
    disk.write_all(RUSTFS_META_BUCKET, legacy_resume, b"{corrupt replacement resume".as_slice().into())
        .await
        .expect("corrupt legacy replacement candidate should persist");

    let error = ResumeUtils::migrate_legacy_replacement_records(&disk)
        .await
        .expect_err("a corrupt UUID-named legacy candidate must fail closed");

    assert!(replacement_recovery_error_requires_block(&error));
    assert!(error.to_string().contains("replacement recovery corruption"));
    assert_eq!(
        disk.read_all(RUSTFS_META_BUCKET, legacy_resume)
            .await
            .expect("corrupt legacy state must remain for operator recovery"),
        b"{corrupt replacement resume".as_slice()
    );
}

#[tokio::test]
async fn startup_migration_reports_corrupt_flat_replacement_intent() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let legacy_intent = ResumeStateFile::LegacyReplacementIntent.path(&task_id);
    let legacy_intent = legacy_intent.to_str().expect("legacy intent path must be UTF-8");
    disk.write_all(RUSTFS_META_BUCKET, legacy_intent, b"{corrupt replacement intent".as_slice().into())
        .await
        .expect("corrupt legacy replacement intent should persist");

    let error = ResumeUtils::migrate_legacy_replacement_records(&disk)
        .await
        .expect_err("a corrupt flat legacy intent must fail closed");

    assert!(replacement_recovery_error_requires_block(&error));
    assert!(error.to_string().contains("replacement recovery corruption"));
    assert_eq!(
        disk.read_all(RUSTFS_META_BUCKET, legacy_intent)
            .await
            .expect("corrupt legacy intent must remain for operator recovery"),
        b"{corrupt replacement intent".as_slice()
    );
}

#[tokio::test]
async fn startup_migration_preserves_conflicting_dedicated_and_legacy_state() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    ResumeManager::new_replacement_intent(
        disk.clone(),
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        }],
    )
    .await
    .expect("dedicated replacement intent should persist");
    let dedicated_path = ResumeStateFile::ReplacementIntent.path(&task_id);
    let dedicated_bytes = disk
        .read_all(RUSTFS_META_BUCKET, dedicated_path.to_str().expect("dedicated intent path must be UTF-8"))
        .await
        .expect("dedicated replacement intent should be readable");

    let legacy_state = ResumeState::replacement_intent(
        task_id.clone(),
        "erasure_set".to_string(),
        "pool_0_set_1".to_string(),
        vec!["other-bucket".to_string()],
        vec!["replacement-b".to_string()],
        vec![ReplacementTargetIdentity {
            endpoint: "replacement-b".to_string(),
            canonical_path: "/mnt/replacement-b".to_string(),
            physical_device_ids: vec!["device-b".to_string()],
            filesystem_identity: "4:5:6".to_string(),
        }],
    );
    let legacy_path = ResumeStateFile::LegacyReplacementIntent.path(&task_id);
    let legacy_bytes = serde_json::to_vec(&legacy_state).expect("serialize conflicting legacy replacement state");
    disk.write_all(
        RUSTFS_META_BUCKET,
        legacy_path.to_str().expect("legacy intent path must be UTF-8"),
        legacy_bytes.clone().into(),
    )
    .await
    .expect("conflicting legacy replacement intent should persist");

    let error = ResumeUtils::migrate_legacy_replacement_records(&disk)
        .await
        .expect_err("conflicting replacement states must fail closed");
    assert!(error.to_string().contains("conflicting legacy state"));
    assert_eq!(
        disk.read_all(RUSTFS_META_BUCKET, dedicated_path.to_str().expect("dedicated intent path must be UTF-8"),)
            .await
            .expect("dedicated state must remain after conflict"),
        dedicated_bytes
    );
    assert_eq!(
        disk.read_all(RUSTFS_META_BUCKET, legacy_path.to_str().expect("legacy intent path must be UTF-8"),)
            .await
            .expect("legacy state must remain after conflict"),
        legacy_bytes
    );
}

#[tokio::test]
async fn startup_migration_preserves_conflicting_dedicated_and_legacy_proof() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let manager = ResumeManager::new_replacement_intent(
        disk.clone(),
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        }],
    )
    .await
    .expect("dedicated replacement intent should persist");
    manager
        .mark_replacement_completed_and_verified()
        .await
        .expect("dedicated completion proof should persist");
    let dedicated_path = replacement_completion_proof_path(&task_id);
    let dedicated_bytes = disk
        .read_all(RUSTFS_META_BUCKET, dedicated_path.to_str().expect("dedicated proof path must be UTF-8"))
        .await
        .expect("dedicated completion proof should be readable");

    let mut legacy_proof = ReplacementCompletionProof::from_state(&manager.get_state().await, 42)
        .expect("legacy completion proof fixture should build");
    legacy_proof.set_disk_id = "pool_0_set_1".to_string();
    let legacy_path = legacy_replacement_completion_proof_path(&task_id);
    let legacy_bytes = serde_json::to_vec(&legacy_proof).expect("serialize conflicting legacy completion proof");
    disk.write_all(
        RUSTFS_META_BUCKET,
        legacy_path.to_str().expect("legacy proof path must be UTF-8"),
        legacy_bytes.clone().into(),
    )
    .await
    .expect("conflicting legacy completion proof should persist");

    let error = ResumeUtils::migrate_legacy_replacement_records(&disk)
        .await
        .expect_err("conflicting completion proofs must fail closed");
    assert!(error.to_string().contains("conflicts with legacy proof"));
    assert_eq!(
        disk.read_all(RUSTFS_META_BUCKET, dedicated_path.to_str().expect("dedicated proof path must be UTF-8"),)
            .await
            .expect("dedicated proof must remain after conflict"),
        dedicated_bytes
    );
    assert_eq!(
        disk.read_all(RUSTFS_META_BUCKET, legacy_path.to_str().expect("legacy proof path must be UTF-8"),)
            .await
            .expect("legacy proof must remain after conflict"),
        legacy_bytes
    );
    let error = match ResumeManager::load_replacement_intent(disk.clone(), &task_id).await {
        Ok(_) => panic!("a conflicting legacy proof must not be ignored during recovery"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("conflicts with legacy proof"));
}

#[tokio::test]
async fn replacement_discovery_does_not_read_ordinary_resume_directory() {
    let (_temp_dir, disk) = schema_test_disk().await;
    for _ in 0..3 {
        ResumeManager::new(
            disk.clone(),
            ResumeUtils::generate_task_id(),
            "erasure_set".to_string(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
        )
        .await
        .expect("ordinary resume state should persist");
    }
    let corrupt_task_id = ResumeUtils::generate_task_id();
    let corrupt_path = ResumeStateFile::Ordinary.path(&corrupt_task_id);
    disk.write_all(
        RUSTFS_META_BUCKET,
        corrupt_path.to_str().expect("ordinary resume path must be UTF-8"),
        b"not-json".to_vec().into(),
    )
    .await
    .expect("corrupt ordinary resume state should persist");

    let replacement_task_id = ResumeUtils::generate_task_id();
    ResumeManager::new_replacement_intent(
        disk.clone(),
        replacement_task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        }],
    )
    .await
    .expect("replacement intent should persist in the dedicated directory");

    assert_eq!(
        ResumeUtils::get_replacement_intent_tasks(&disk)
            .await
            .expect("dedicated replacement listing should not parse ordinary JSON"),
        vec![replacement_task_id]
    );
}

#[tokio::test]
async fn empty_replacement_recovery_directory_is_not_an_error() {
    let (_temp_dir, disk) = schema_test_disk().await;
    assert!(
        ResumeUtils::get_replacement_intent_tasks(&disk)
            .await
            .expect("missing recovery directory should be empty")
            .is_empty()
    );
    assert!(
        ResumeUtils::get_replacement_recovery_records(&disk)
            .await
            .expect("missing recovery directory should have no records")
            .is_empty()
    );
    ResumeUtils::cleanup_expired_states(&disk, 0)
        .await
        .expect("missing recovery directory should not block expiry cleanup");
}

#[tokio::test]
async fn replacement_completion_proof_survives_resume_cleanup() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let identity = ReplacementTargetIdentity {
        endpoint: "replacement-a".to_string(),
        canonical_path: "/mnt/replacement-a".to_string(),
        physical_device_ids: vec!["device-a".to_string()],
        filesystem_identity: "1:2:3".to_string(),
    };
    let manager = ResumeManager::new_replacement_intent(
        disk.clone(),
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![identity.clone()],
    )
    .await
    .expect("replacement intent should persist");
    manager
        .mark_replacement_completed_and_verified()
        .await
        .expect("verified replacement must persist proof before completion");

    let proof = ResumeManager::load_replacement_completion_proof(disk.clone(), &task_id)
        .await
        .expect("completion proof must be readable from the survivor anchor");
    assert_eq!(proof.task_id, task_id);
    assert_eq!(proof.replacement_generation, proof.task_id);
    assert_eq!(proof.set_disk_id, "pool_0_set_0");
    assert_eq!(proof.replacement_targets, ["replacement-a"]);
    assert_eq!(proof.replacement_target_identities, vec![identity]);
    assert!(proof.verified_at > 0);

    manager.cleanup().await.expect("resume cleanup should succeed");
    assert!(
        !ResumeManager::has_replacement_intent(&disk, &proof.task_id).await,
        "completion cleanup must remove the resumable state"
    );
    assert_eq!(
        ResumeManager::load_replacement_completion_proof(disk, &proof.task_id)
            .await
            .expect("survivor proof must outlive resume cleanup"),
        proof
    );
}

#[tokio::test]
async fn replacement_recovery_records_distinguish_active_and_proven_completion() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let manager = ResumeManager::new_replacement_intent(
        disk.clone(),
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        }],
    )
    .await
    .expect("replacement intent should persist");

    let active = ResumeUtils::get_replacement_recovery_records(&disk)
        .await
        .expect("active replacement record should be readable");
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].task_id, task_id);
    assert_eq!(active[0].state, ReplacementRecoveryState::WaitingForReplacement);
    assert_eq!(active[0].generation.as_deref(), Some(task_id.as_str()));

    manager
        .mark_replacement_completed_and_verified()
        .await
        .expect("completion proof should persist");

    let cleanup_pending = ResumeUtils::get_replacement_recovery_records(&disk)
        .await
        .expect("cleanup-pending replacement record should be readable");
    assert_eq!(cleanup_pending.len(), 1);
    assert_eq!(cleanup_pending[0].state, ReplacementRecoveryState::CleanupPending);

    manager.cleanup().await.expect("resume state cleanup should succeed");

    let completed = ResumeUtils::get_replacement_recovery_records(&disk)
        .await
        .expect("completion proof should remain readable");
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].state, ReplacementRecoveryState::Completed);
    assert!(completed[0].verified_at.is_some());
}

#[tokio::test]
async fn replacement_completion_write_failure_cannot_mark_completed() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let manager = ResumeManager::new_replacement_intent(
        disk,
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        }],
    )
    .await
    .expect("replacement intent should persist");
    let proof_path = replacement_completion_proof_path(&task_id)
        .to_str()
        .expect("completion proof path must be UTF-8")
        .to_string();
    let _failure = ReplacementProofWriteFailure::install(proof_path, DiskError::DiskAccessDenied);

    let error = manager
        .mark_replacement_completed_and_verified()
        .await
        .expect_err("completion must fail closed when durable proof cannot be written");
    assert!(error.to_string().contains("Failed to save replacement completion proof"));
    let state = manager.get_state().await;
    assert!(!state.completed, "a failed proof write must not produce a completed state");
    assert_eq!(state.replacement_phase, ReplacementPhase::Intent);
}

#[tokio::test]
async fn replacement_completion_repairs_torn_proof_without_wedging_rebuild() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let identity = ReplacementTargetIdentity {
        endpoint: "replacement-a".to_string(),
        canonical_path: "/mnt/replacement-a".to_string(),
        physical_device_ids: vec!["device-a".to_string()],
        filesystem_identity: "1:2:3".to_string(),
    };
    let manager = ResumeManager::new_replacement_intent(
        disk.clone(),
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![identity.clone()],
    )
    .await
    .expect("replacement intent should persist");
    manager
        .mark_replacement_rebuilding(vec![identity])
        .await
        .expect("replacement fixture should enter rebuilding before proof publication");
    let proof_path = replacement_completion_proof_path(&task_id);
    let proof_path = proof_path.to_str().expect("completion proof path must be UTF-8");
    disk.write_all(RUSTFS_META_BUCKET, proof_path, b"{torn completion proof".as_slice().into())
        .await
        .expect("torn proof fixture should persist");

    manager
        .mark_replacement_completed_and_verified()
        .await
        .expect("a rebuilding generation must replace only its torn completion proof");

    let proof = ResumeManager::load_replacement_completion_proof(disk, &task_id)
        .await
        .expect("repaired completion proof should be durable and readable");
    assert_eq!(proof.task_id, task_id);
    assert_eq!(proof.replacement_generation, proof.task_id);
}

#[tokio::test]
async fn replacement_completion_does_not_replace_a_valid_mismatched_proof() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let manager = ResumeManager::new_replacement_intent(
        disk.clone(),
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        }],
    )
    .await
    .expect("replacement intent should persist");
    let mut conflicting = ReplacementCompletionProof::from_state(&manager.get_state().await, 1)
        .expect("replacement state should build a proof fixture");
    conflicting.set_disk_id = "pool_0_set_1".to_string();
    let proof_path = replacement_completion_proof_path(&task_id);
    let proof_path = proof_path.to_str().expect("completion proof path must be UTF-8");
    disk.write_all(
        RUSTFS_META_BUCKET,
        proof_path,
        serde_json::to_vec(&conflicting)
            .expect("proof fixture should serialize")
            .into(),
    )
    .await
    .expect("conflicting proof fixture should persist");

    let error = manager
        .mark_replacement_completed_and_verified()
        .await
        .expect_err("a distinct durable generation binding must not be overwritten");
    assert!(error.to_string().contains("does not match task"));
    assert_eq!(
        ResumeManager::load_replacement_completion_proof(disk, &task_id)
            .await
            .expect("valid conflicting proof should remain intact"),
        conflicting
    );
}

#[tokio::test]
async fn cleanup_expired_states_keeps_all_durable_replacement_phases() {
    use super::super::{DiskOption, Endpoint, new_disk};
    use tempfile::TempDir;

    let temp_dir = TempDir::new().expect("create replacement expiry test directory");
    let endpoint = Endpoint::try_from(temp_dir.path().to_string_lossy().as_ref()).expect("create test disk endpoint");
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("create test disk");
    match disk.make_volume(RUSTFS_META_BUCKET).await {
        Ok(()) | Err(DiskError::VolumeExists) => {}
        Err(err) => panic!("create metadata volume for replacement expiry test: {err}"),
    }

    let target = ReplacementTargetIdentity {
        endpoint: "replacement-a".to_string(),
        canonical_path: "/mnt/replacement-a".to_string(),
        physical_device_ids: vec!["device-a".to_string()],
        filesystem_identity: "1:2:3".to_string(),
    };
    let intent_task_id = ResumeUtils::generate_task_id();
    let rebuilding_task_id = ResumeUtils::generate_task_id();
    let verified_task_id = ResumeUtils::generate_task_id();
    let cleanup_pending_task_id = ResumeUtils::generate_task_id();
    let abandoned_task_id = ResumeUtils::generate_task_id();
    let ordinary_task_id = ResumeUtils::generate_task_id();
    let intent = ResumeManager::new_replacement_intent(
        disk.clone(),
        intent_task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![target.clone()],
    )
    .await
    .expect("replacement intent should persist");
    let rebuilding = ResumeManager::new_replacement_intent(
        disk.clone(),
        rebuilding_task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![target.clone()],
    )
    .await
    .expect("replacement rebuilding state should persist");
    rebuilding
        .mark_replacement_rebuilding(vec![target.clone()])
        .await
        .expect("replacement rebuilding phase should persist");
    let verified = ResumeManager::new_replacement_intent(
        disk.clone(),
        verified_task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![target.clone()],
    )
    .await
    .expect("replacement verified state should persist");
    verified
        .mark_replacement_completed_and_verified()
        .await
        .expect("replacement verified phase should persist");
    let cleanup_pending = ResumeManager::new_replacement_intent(
        disk.clone(),
        cleanup_pending_task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![target.clone()],
    )
    .await
    .expect("replacement cleanup-pending state should persist");
    cleanup_pending
        .mark_replacement_completed_and_verified()
        .await
        .expect("replacement completion should persist");
    cleanup_pending
        .mark_replacement_cleanup_pending()
        .await
        .expect("replacement cleanup-pending phase should persist");
    let abandoned = ResumeManager::new_replacement_intent(
        disk.clone(),
        abandoned_task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
        vec!["replacement-a".to_string()],
        vec![target],
    )
    .await
    .expect("replacement abandoned state should persist");
    abandoned
        .abandon_replacement_intent()
        .await
        .expect("replacement abandoned phase should persist");
    let ordinary = ResumeManager::new(
        disk.clone(),
        ordinary_task_id.clone(),
        "erasure_set".to_string(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
    )
    .await
    .expect("ordinary resume state should persist");

    for manager in [&intent, &rebuilding, &verified, &cleanup_pending, &abandoned, &ordinary] {
        manager.state.write().await.last_update = 0;
        manager.save_state_strict().await.expect("persist expired resume state");
    }

    ResumeUtils::cleanup_expired_states(&disk, 0)
        .await
        .expect("replacement expiry cleanup should complete");

    for (task_id, expected_phase) in [
        (intent_task_id.as_str(), ReplacementPhase::Intent),
        (rebuilding_task_id.as_str(), ReplacementPhase::Rebuilding),
        (verified_task_id.as_str(), ReplacementPhase::Verified),
        (cleanup_pending_task_id.as_str(), ReplacementPhase::CleanupPending),
    ] {
        let state = ResumeManager::load_replacement_intent(disk.clone(), task_id)
            .await
            .expect("durable replacement state must survive expiry cleanup")
            .get_state()
            .await;
        assert_eq!(state.replacement_phase, expected_phase);
    }
    assert!(
        !ResumeManager::has_replacement_intent(&disk, &abandoned_task_id).await,
        "an abandoned replacement must expire"
    );
    assert!(
        !ResumeManager::has_resume_state(&disk, &ordinary_task_id).await,
        "an ordinary expired resume must expire"
    );
}

#[tokio::test]
async fn test_resume_state_progress() {
    let task_id = ResumeUtils::generate_task_id();
    let buckets = vec!["bucket1".to_string()];
    let mut state = ResumeState::new(task_id, "erasure_set".to_string(), "pool_0_set_0".to_string(), buckets);

    state.update_progress(10, 8, 1, 1);
    assert_eq!(state.processed_objects, 10);
    assert_eq!(state.successful_objects, 8);
    assert_eq!(state.failed_objects, 1);
    assert_eq!(state.skipped_objects, 1);

    let progress = state.get_progress_percentage();
    assert_eq!(progress, 0.0); // total_objects is 0

    state.total_objects = 100;
    state.baseline_known = true;
    let progress = state.get_progress_percentage();
    assert_eq!(progress, 10.0);
}

#[tokio::test]
async fn replacement_intent_rejects_a_new_mount_at_the_same_endpoint() {
    use super::super::{DiskOption, Endpoint, new_disk};
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let disk_path = temp_dir.path().join("resume_disk");
    std::fs::create_dir_all(&disk_path).unwrap();
    let endpoint = Endpoint::try_from(disk_path.to_string_lossy().as_ref()).unwrap();
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .unwrap();
    let _ = disk.make_volume(RUSTFS_META_BUCKET).await;
    let _ = disk.make_volume(&format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}")).await;

    let task_id = ResumeUtils::generate_task_id();
    let targets = vec!["replacement-a".to_string()];
    let first = ReplacementTargetIdentity {
        endpoint: "replacement-a".to_string(),
        canonical_path: "/mnt/replacement-a".to_string(),
        physical_device_ids: vec!["device-a".to_string()],
        filesystem_identity: "1:2:3".to_string(),
    };
    ResumeManager::new_replacement_intent(
        disk.clone(),
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket-a".to_string()],
        targets.clone(),
        vec![first.clone()],
    )
    .await
    .unwrap();

    let reused = ResumeManager::new_replacement_intent(
        disk.clone(),
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket-b".to_string()],
        targets.clone(),
        vec![first.clone()],
    )
    .await
    .unwrap();
    assert_eq!(
        reused.get_state().await.replacement_buckets,
        ["bucket-a"],
        "retries must keep the first generation's bucket plan"
    );

    let mut second = ReplacementTargetIdentity {
        endpoint: "replacement-a".to_string(),
        canonical_path: "/mnt/replacement-a".to_string(),
        physical_device_ids: first.physical_device_ids.clone(),
        filesystem_identity: first.filesystem_identity.clone(),
    };
    for changed_identity in [
        {
            second.physical_device_ids = vec!["device-b".to_string()];
            second.clone()
        },
        {
            second.physical_device_ids = first.physical_device_ids.clone();
            second.filesystem_identity = "4:5:6".to_string();
            second.clone()
        },
        {
            second.filesystem_identity = first.filesystem_identity.clone();
            second.canonical_path = "/mnt/replacement-b".to_string();
            second.clone()
        },
    ] {
        let result = ResumeManager::new_replacement_intent(
            disk.clone(),
            task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["bucket-a".to_string()],
            targets.clone(),
            vec![changed_identity],
        )
        .await;
        assert!(result.is_err(), "a new mounted instance must not reuse the old replacement cursor");
    }
    temp_dir.close().unwrap();
}

#[test]
fn reset_for_retry_clears_progress_but_keeps_retry_budget() {
    // backlog#855 / #799 B6: a retry must re-scan from the start without
    // spending the retry budget's identity.
    let buckets = vec!["bucket1".to_string(), "bucket2".to_string()];
    let mut state = ResumeState::new("t".to_string(), "erasure_set".to_string(), "pool_0_set_0".to_string(), buckets);
    state.update_progress(10, 8, 2, 0);
    state.complete_bucket("bucket1");
    state.increment_retry();
    state.mark_completed();

    state.reset_for_retry();

    assert!(!state.completed, "retry must un-complete the task");
    assert_eq!(state.completed_buckets.len(), 0, "all buckets must be re-scanned");
    assert_eq!(state.processed_objects, 0);
    assert_eq!(state.successful_objects, 0);
    assert_eq!(state.failed_objects, 0);
    assert_eq!(state.skipped_objects, 0);
    assert_eq!(state.retry_count, 1, "retry budget must be preserved");
}

#[test]
fn can_retry_is_bounded_by_max_retries() {
    let mut state = ResumeState::new("t".to_string(), "erasure_set".to_string(), "pool_0_set_0".to_string(), vec![]);
    assert!(state.can_retry());
    for _ in 0..state.max_retries {
        assert!(state.can_retry());
        state.increment_retry();
    }
    assert!(!state.can_retry(), "retries must stop after max_retries");
}

#[test]
fn checkpoint_reset_for_retry_rewinds_position_and_clears_sets() {
    let mut checkpoint = ResumeCheckpoint::new("task".to_string());
    checkpoint.update_position(3, 42);
    checkpoint.add_processed_object("bucket/a".to_string());
    checkpoint.add_failed_object("bucket/b".to_string());
    checkpoint.add_skipped_object("bucket/c".to_string());

    checkpoint.reset_for_retry();

    assert_eq!(checkpoint.current_bucket_index, 0);
    assert_eq!(checkpoint.current_object_index, 0);
    assert!(checkpoint.processed_objects.is_empty());
    assert!(checkpoint.failed_objects.is_empty());
    assert!(checkpoint.skipped_objects.is_empty());
}

#[tokio::test]
async fn test_resume_state_bucket_completion() {
    let task_id = ResumeUtils::generate_task_id();
    let buckets = vec!["bucket1".to_string(), "bucket2".to_string()];
    let mut state = ResumeState::new(task_id, "erasure_set".to_string(), "pool_0_set_0".to_string(), buckets);

    assert_eq!(state.pending_buckets.len(), 2);
    assert_eq!(state.completed_buckets.len(), 0);

    state.complete_bucket("bucket1");
    assert_eq!(state.pending_buckets.len(), 1);
    assert_eq!(state.completed_buckets.len(), 1);
    assert!(state.completed_buckets.contains(&"bucket1".to_string()));
}

#[test]
fn test_checkpoint_object_sets_dedupe_and_prune() {
    let mut checkpoint = ResumeCheckpoint::new("task".to_string());
    checkpoint.add_processed_object("bucket/a".to_string());
    checkpoint.add_processed_object("bucket/a".to_string());
    checkpoint.add_skipped_object("bucket/b".to_string());
    checkpoint.add_failed_object("bucket/c".to_string());
    assert_eq!(checkpoint.processed_objects.len(), 1);
    assert!(checkpoint.processed_objects.contains("bucket/a"));

    checkpoint.complete_page(2, 2000);
    assert_eq!(checkpoint.current_bucket_index, 2);
    assert_eq!(checkpoint.current_object_index, 2000);
    assert!(checkpoint.processed_objects.is_empty());
    assert!(checkpoint.skipped_objects.is_empty());
    assert!(checkpoint.failed_objects.is_empty());
}

#[tokio::test]
async fn checkpoint_page_commit_keeps_ledger_until_cursor_is_durable() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let checkpoint = CheckpointManager::new(disk.clone(), task_id.clone()).await.unwrap();

    checkpoint
        .record_object_outcome(CheckpointObjectOutcomeRecord {
            object: "bucket/object:v1".to_string(),
            outcome: CheckpointObjectOutcome::Processed,
            successful: 1,
            failed: 0,
            skipped: 0,
            bytes: 128,
            skipped_new_versions: 0,
            skipped_ilm_expired: 0,
            counter_unknown: false,
        })
        .await
        .unwrap();
    checkpoint.advance_page(0, 1).await.unwrap();

    let reloaded = CheckpointManager::load_from_disk(disk.clone(), &task_id).await.unwrap();
    let snapshot = reloaded.get_checkpoint().await;
    assert_eq!(snapshot.current_object_index, 1);
    assert_eq!(snapshot.successful_objects, 1);
    assert_eq!(snapshot.processed_bytes, 128);
    assert!(snapshot.processed_objects.contains("bucket/object:v1"));

    checkpoint.prune_completed_page().await.unwrap();
    let reloaded = CheckpointManager::load_from_disk(disk, &task_id).await.unwrap();
    assert!(reloaded.get_checkpoint().await.processed_objects.is_empty());
}

#[test]
fn test_checkpoint_loads_legacy_vec_format() {
    // Checkpoints written before the HashSet migration stored the object
    // lists as JSON arrays (possibly with duplicates); they must still load.
    let legacy = r#"{
            "task_id": "t1",
            "checkpoint_time": 1700000000,
            "current_bucket_index": 1,
            "current_object_index": 42,
            "processed_objects": ["a", "b", "a"],
            "failed_objects": [],
            "skipped_objects": ["c"]
        }"#;
    let checkpoint: ResumeCheckpoint = serde_json::from_str(legacy).unwrap();
    assert_eq!(checkpoint.current_object_index, 42);
    assert_eq!(checkpoint.processed_objects.len(), 2);
    assert!(checkpoint.processed_objects.contains("a"));
    assert!(checkpoint.skipped_objects.contains("c"));
}

#[test]
fn test_compose_key_injective_with_adversarial_keys() {
    // Length-prefixing must keep the encoding injective even when keys
    // contain the delimiter, embedded nulls, or look like a composed key.
    assert_ne!(compose_key("a\0b", None), compose_key("a", Some("b")));
    assert_ne!(compose_key("3:xy", None), compose_key("x", Some("y")));
    assert_ne!(compose_key("a:b", None), compose_key("a", Some("b")));
    assert_ne!(compose_key("", Some("x")), compose_key("x", None));
    // Identical inputs must produce identical keys (stable identity).
    assert_eq!(compose_key("obj", Some("v1")), compose_key("obj", Some("v1")));
}

#[test]
fn test_composite_key_dedup_distinguishes_versions() {
    // Two versions of the same object must be distinct dedup identities, and
    // the delete-marker/nil (None) version must not collide with a real one.
    let mut checkpoint = ResumeCheckpoint::new("task".to_string());
    checkpoint.add_processed_object(compose_key("obj", Some("v1")));
    checkpoint.add_processed_object(compose_key("obj", Some("v2")));
    checkpoint.add_processed_object(compose_key("obj", None));
    assert_eq!(checkpoint.processed_objects.len(), 3);
    assert!(checkpoint.processed_objects.contains(&compose_key("obj", Some("v1"))));
    assert!(checkpoint.processed_objects.contains(&compose_key("obj", Some("v2"))));
    assert!(checkpoint.processed_objects.contains(&compose_key("obj", None)));
    // A different object with the same version id is still distinct.
    assert!(!checkpoint.processed_objects.contains(&compose_key("other", Some("v1"))));
}

#[tokio::test]
async fn test_resumestate_schema_v0_discarded_on_load() {
    let (temp_dir, disk) = schema_test_disk().await;

    // Legacy snapshot: no schema_version, a stale positional cursor and progress.
    let legacy = r#"{
            "task_id": "old-task",
            "task_type": "erasure_set",
            "set_disk_id": "pool_0_set_0",
            "start_time": 1700000000,
            "last_update": 1700000000,
            "completed": true,
            "total_objects": 100,
            "processed_objects": 50,
            "successful_objects": 40,
            "failed_objects": 10,
            "skipped_objects": 0,
            "current_bucket": null,
            "current_object": null,
            "completed_buckets": ["b1"],
            "pending_buckets": [],
            "error_message": null,
            "retry_count": 1,
            "max_retries": 3,
            "resume_cursor": "v1:stale-token"
        }"#;
    let task_id = "00000000-0000-4000-8000-000000000001";
    let legacy = legacy.replace("old-task", task_id);
    let file_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_STATE_FILE}");
    disk.write_all(RUSTFS_META_BUCKET, &file_path, legacy.as_bytes().to_vec().into())
        .await
        .expect("write legacy resume state");

    let manager = ResumeManager::load_from_disk(disk.clone(), task_id).await.unwrap();
    let state = manager.get_state().await;
    assert_eq!(state.schema_version, CURRENT_RESUME_SCHEMA, "schema must be stamped current");
    assert_eq!(state.resume_cursor, None, "stale cursor must be cleared");
    assert_eq!(state.processed_objects, 0);
    assert_eq!(state.successful_objects, 0);
    assert_eq!(state.failed_objects, 0);
    assert!(!state.completed);
    temp_dir.close().expect("remove schema test directory");
}

#[tokio::test]
async fn test_checkpoint_schema_v5_discarded_on_load() {
    let (temp_dir, disk) = schema_test_disk().await;

    // Schema v5 can persist failed identities without the aggregate counters
    // that make those identities safe to deduplicate after an upgrade.
    let task_id = "00000000-0000-4000-8000-000000000002";
    let legacy = r#"{
            "schema_version": 5,
            "task_id": "00000000-0000-4000-8000-000000000002",
            "checkpoint_time": 1700000000,
            "current_bucket_index": 2,
            "current_object_index": 500,
            "processed_objects": ["a", "b"],
            "failed_objects": ["c"],
            "skipped_objects": ["d"]
        }"#;
    let file_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_CHECKPOINT_FILE}");
    disk.write_all(RUSTFS_META_BUCKET, &file_path, legacy.as_bytes().to_vec().into())
        .await
        .expect("write legacy checkpoint");

    let manager = CheckpointManager::load_from_disk(disk.clone(), task_id).await.unwrap();
    let checkpoint = manager.get_checkpoint().await;
    assert_eq!(checkpoint.schema_version, CURRENT_CHECKPOINT_SCHEMA, "schema must be stamped current");
    assert_eq!(checkpoint.current_bucket_index, 0, "stale bucket position must be reset");
    assert_eq!(checkpoint.current_object_index, 0, "stale position must be reset");
    assert!(checkpoint.processed_objects.is_empty());
    assert!(checkpoint.failed_objects.is_empty());
    assert!(checkpoint.skipped_objects.is_empty());
    temp_dir.close().expect("remove schema test directory");
}

#[tokio::test]
async fn downgraded_unsigned_checkpoint_resets_untrusted_progress() {
    let (temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let manager = CheckpointManager::new(disk.clone(), task_id.clone()).await.unwrap();
    manager.add_processed_object("victim-a".to_string()).await.unwrap();
    manager.update_position(2, 500).await.unwrap();
    let checkpoint_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_CHECKPOINT_FILE}");
    let bytes = disk.read_all(RUSTFS_META_BUCKET, &checkpoint_path).await.unwrap();
    let mut downgraded: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    downgraded["schema_version"] = serde_json::json!(CURRENT_CHECKPOINT_SCHEMA - 1);
    downgraded.as_object_mut().unwrap().remove("integrity_digest");
    downgraded["processed_objects"] = serde_json::json!(["victim-b"]);
    disk.write_all(RUSTFS_META_BUCKET, &checkpoint_path, serde_json::to_vec(&downgraded).unwrap().into())
        .await
        .expect("write downgraded checkpoint");

    let manager = CheckpointManager::load_from_disk(disk, &task_id).await.unwrap();
    let checkpoint = manager.get_checkpoint().await;
    assert_eq!(checkpoint.schema_version, CURRENT_CHECKPOINT_SCHEMA);
    assert_eq!(checkpoint.current_bucket_index, 0);
    assert_eq!(checkpoint.current_object_index, 0);
    assert!(checkpoint.processed_objects.is_empty());
    temp_dir.close().unwrap();
}

#[tokio::test]
async fn current_normal_resume_schema_preserves_progress() {
    let (temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let mut state = ResumeState::new(
        task_id.clone(),
        "erasure_set".to_string(),
        "pool_0_set_0".to_string(),
        vec!["bucket-b".to_string()],
    );
    state.resume_cursor = Some("opaque-marker".to_string());
    state.processed_objects = 7;
    state.successful_objects = 6;
    state.failed_objects = 1;
    state.completed_buckets = vec!["bucket-a".to_string()];
    let file_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_STATE_FILE}");
    let state_data = serde_json::to_vec(&state).expect("serialize current normal resume state");
    disk.write_all(RUSTFS_META_BUCKET, &file_path, state_data.into())
        .await
        .expect("write current normal resume state");

    let restored = ResumeManager::load_from_disk(disk.clone(), &task_id)
        .await
        .expect("load current normal resume state")
        .get_state()
        .await;

    assert_eq!(restored.schema_version, CURRENT_RESUME_SCHEMA);
    assert_eq!(restored.resume_cursor.as_deref(), Some("opaque-marker"));
    assert_eq!(restored.processed_objects, 7);
    assert_eq!(restored.successful_objects, 6);
    assert_eq!(restored.failed_objects, 1);
    assert_eq!(restored.completed_buckets, ["bucket-a"]);
    assert!(restored.replacement_targets.is_empty());
    assert_eq!(restored.replacement_generation, None);
    assert_eq!(restored.replacement_phase, ReplacementPhase::None);
    temp_dir.close().expect("remove schema test directory");
}

#[test]
fn progress_checkpoint_restores_bytes_and_generation() {
    let mut checkpoint = ResumeCheckpoint::new("progress-checkpoint".to_string());
    checkpoint.set_progress_baseline(9, 4096, Some(77));
    checkpoint.update_progress(4, 1, 2, 2048);
    checkpoint.set_skipped_version_counts(3, 1);
    checkpoint.mark_counter_unknown();

    let restored: ResumeCheckpoint =
        serde_json::from_slice(&serde_json::to_vec(&checkpoint).expect("serialize checkpoint")).expect("deserialize checkpoint");
    assert_eq!(restored.processed_bytes, 2048);
    assert_eq!(restored.total_objects, 9);
    assert_eq!(restored.total_bytes, 4096);
    assert_eq!(restored.baseline_generation, Some(77));
    assert!(restored.baseline_known);
    assert_eq!(restored.skipped_new_versions, 3);
    assert_eq!(restored.skipped_ilm_expired, 1);
    assert!(restored.counter_unknown);
}

#[test]
fn old_progress_schema_migrates_missing_fields_to_unknown() {
    let state = ResumeState::new(
        "legacy-progress".to_string(),
        "erasure_set".to_string(),
        "pool_0_set_0".to_string(),
        Vec::new(),
    );
    let mut value = serde_json::to_value(state).expect("serialize legacy-compatible state");
    let object = value.as_object_mut().expect("state must be an object");
    for field in [
        "processed_bytes",
        "total_bytes",
        "baseline_generation",
        "baseline_known",
        "skipped_new_versions",
        "skipped_ilm_expired",
    ] {
        object.remove(field);
    }
    object.insert("total_objects".to_string(), serde_json::json!(10));
    object.insert("processed_objects".to_string(), serde_json::json!(5));
    let restored: ResumeState = serde_json::from_value(value).expect("deserialize old progress state");
    assert_eq!(restored.processed_bytes, 0);
    assert_eq!(restored.total_bytes, 0);
    assert_eq!(restored.baseline_generation, None);
    assert!(!restored.baseline_known, "missing baseline must remain unknown");
    assert_eq!(restored.get_progress_percentage(), 0.0);
    assert_eq!(restored.skipped_new_versions, 0);
    assert_eq!(restored.skipped_ilm_expired, 0);
}

#[test]
fn progress_counter_unknown_survives_resume_round_trip() {
    let mut state = ResumeState::new(
        "overflow-progress".to_string(),
        "erasure_set".to_string(),
        "pool_0_set_0".to_string(),
        Vec::new(),
    );
    state.mark_counter_unknown();

    let restored: ResumeState =
        serde_json::from_slice(&serde_json::to_vec(&state).expect("serialize resume state")).expect("deserialize resume state");
    assert!(restored.counter_unknown);
}

#[tokio::test]
async fn checkpoint_progress_survives_a_torn_resume_summary_write() {
    let (_temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let _resume = ResumeManager::new(
        disk.clone(),
        task_id.clone(),
        "erasure_set".to_string(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
    )
    .await
    .expect("resume state should persist");
    let checkpoint = CheckpointManager::new(disk.clone(), task_id.clone())
        .await
        .expect("checkpoint should persist");

    // This is the ordering used by the erasure-set loop: the checkpoint is
    // durable before the summary write.  Stop here to model a crash in the
    // inter-store window and verify that the recovery authority retains the
    // telemetry fence and bytes.
    checkpoint
        .update_progress(3, 0, 0, 1024)
        .await
        .expect("checkpoint progress should persist");
    checkpoint.mark_counter_unknown().await.expect("unknown fence should persist");
    checkpoint
        .update_position(0, 3)
        .await
        .expect("checkpoint position should persist");

    let restored_checkpoint = CheckpointManager::load_from_disk(disk.clone(), &task_id)
        .await
        .expect("checkpoint should reload")
        .get_checkpoint()
        .await;
    let restored_resume = ResumeManager::load_from_disk(disk, &task_id)
        .await
        .expect("resume summary should reload")
        .get_state()
        .await;
    assert!(restored_checkpoint.counter_unknown);
    assert_eq!(restored_checkpoint.processed_bytes, 1024);
    assert_eq!(restored_checkpoint.current_object_index, 3);
    assert!(!restored_resume.counter_unknown, "summary is intentionally the torn/older store");
}

#[tokio::test]
async fn future_resume_and_checkpoint_schemas_are_rejected() {
    let (temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let mut state = ResumeState::new(task_id.clone(), "erasure_set".to_string(), "pool_0_set_0".to_string(), Vec::new());
    state.schema_version = CURRENT_RESUME_SCHEMA + 1;
    let state_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_STATE_FILE}");
    let state_data = serde_json::to_vec(&state).expect("serialize future resume state");
    disk.write_all(RUSTFS_META_BUCKET, &state_path, state_data.into())
        .await
        .expect("write future resume state");

    let resume_error = match ResumeManager::load_from_disk(disk.clone(), &task_id).await {
        Ok(_) => panic!("future resume schema must not load"),
        Err(error) => error,
    };
    assert!(matches!(resume_error, Error::TaskExecutionFailed { .. }));
    assert!(resume_error.to_string().contains("newer than supported schema"));

    let mut checkpoint = ResumeCheckpoint::new(task_id.clone());
    checkpoint.schema_version = CURRENT_CHECKPOINT_SCHEMA + 1;
    let checkpoint_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_CHECKPOINT_FILE}");
    let checkpoint_data = serde_json::to_vec(&checkpoint).expect("serialize future checkpoint");
    disk.write_all(RUSTFS_META_BUCKET, &checkpoint_path, checkpoint_data.into())
        .await
        .expect("write future checkpoint");

    let checkpoint_error = match CheckpointManager::load_from_disk(disk.clone(), &task_id).await {
        Ok(_) => panic!("future checkpoint schema must not load"),
        Err(error) => error,
    };
    assert!(matches!(checkpoint_error, Error::TaskExecutionFailed { .. }));
    assert!(checkpoint_error.to_string().contains("newer than supported schema"));
    temp_dir.close().expect("remove schema test directory");
}

#[tokio::test]
async fn checkpoint_save_does_not_replace_a_non_empty_truncated_snapshot() {
    let (temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let manager = CheckpointManager::new(disk.clone(), task_id.clone())
        .await
        .expect("create checkpoint manager");
    let checkpoint_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_CHECKPOINT_FILE}");
    let truncated = b"{\"schema_version\":5,\"task_id\":";
    disk.write_all(RUSTFS_META_BUCKET, &checkpoint_path, truncated.as_slice().into())
        .await
        .expect("write truncated checkpoint fixture");

    let error = manager
        .update_position(2, 7)
        .await
        .expect_err("a truncated checkpoint must fail closed during save");
    assert!(error.to_string().contains("Existing checkpoint is corrupt"));
    assert_eq!(
        disk.read_all(RUSTFS_META_BUCKET, &checkpoint_path)
            .await
            .expect("read truncated checkpoint fixture"),
        truncated.as_slice()
    );
    assert!(CheckpointManager::is_blocked(&disk, &task_id).await);
    temp_dir.close().expect("remove checkpoint save test directory");
}

#[tokio::test]
async fn checkpoint_save_does_not_replace_a_future_schema_snapshot() {
    let (temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let manager = CheckpointManager::new(disk.clone(), task_id.clone())
        .await
        .expect("create checkpoint manager");
    let checkpoint_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_CHECKPOINT_FILE}");
    let mut future = ResumeCheckpoint::new(task_id.clone());
    future.schema_version = CURRENT_CHECKPOINT_SCHEMA + 1;
    let future_bytes = serde_json::to_vec(&future).expect("serialize future checkpoint fixture");
    disk.write_all(RUSTFS_META_BUCKET, &checkpoint_path, future_bytes.clone().into())
        .await
        .expect("write future checkpoint fixture");

    let error = manager
        .update_position(2, 7)
        .await
        .expect_err("a future schema must fail closed during save");
    assert!(error.to_string().contains("Existing checkpoint schema"));
    assert_eq!(
        disk.read_all(RUSTFS_META_BUCKET, &checkpoint_path)
            .await
            .expect("read future checkpoint fixture"),
        future_bytes
    );
    assert!(CheckpointManager::is_blocked(&disk, &task_id).await);
    temp_dir.close().expect("remove future schema test directory");
}

#[tokio::test]
async fn checkpoint_digest_rejects_same_length_progress_tampering() {
    let (temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let manager = CheckpointManager::new(disk.clone(), task_id.clone())
        .await
        .expect("create checkpoint manager");
    manager
        .add_processed_object("victim-a".to_string())
        .await
        .expect("persist checkpoint progress");
    manager.update_position(1, 1).await.expect("flush checkpoint progress");
    let checkpoint_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_CHECKPOINT_FILE}");
    let original = disk
        .read_all(RUSTFS_META_BUCKET, &checkpoint_path)
        .await
        .expect("read checkpoint fixture");
    let tampered = original
        .windows(b"victim-a".len())
        .position(|window| window == b"victim-a")
        .map(|index| {
            let mut bytes = original.to_vec();
            bytes[index..index + b"victim-a".len()].copy_from_slice(b"victim-b");
            bytes
        })
        .expect("checkpoint should contain the processed object");
    disk.write_all(RUSTFS_META_BUCKET, &checkpoint_path, tampered.into())
        .await
        .expect("write tampered checkpoint fixture");

    assert!(CheckpointManager::load_from_disk(disk.clone(), &task_id).await.is_err());
    assert!(CheckpointManager::is_blocked(&disk, &task_id).await);
    temp_dir.close().expect("remove digest test directory");
}

#[tokio::test]
async fn checkpoint_integrity_survives_missing_legacy_sidecar() {
    let (temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let manager = CheckpointManager::new(disk.clone(), task_id.clone()).await.unwrap();
    manager.update_position(2, 9).await.unwrap();

    let digest_path = format!("{BUCKET_META_PREFIX}/{task_id}_ahm_checkpoint.sha256");
    delete_resume_file(&disk, Path::new(&digest_path)).await.unwrap();

    let restored = CheckpointManager::load_from_disk(disk, &task_id).await.unwrap();
    let checkpoint = restored.get_checkpoint().await;
    assert_eq!(checkpoint.current_bucket_index, 2);
    assert_eq!(checkpoint.current_object_index, 9);
    assert!(checkpoint.integrity_digest.is_some());
    temp_dir.close().unwrap();
}

#[tokio::test]
async fn checkpoint_integrity_survives_multi_object_reload() {
    let (temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let manager = CheckpointManager::new(disk.clone(), task_id.clone()).await.unwrap();
    for index in 0..32 {
        manager.add_processed_object(format!("processed-{index}")).await.unwrap();
        manager.add_failed_object(format!("failed-{index}")).await.unwrap();
        manager.add_skipped_object(format!("skipped-{index}")).await.unwrap();
    }
    manager.update_position(2, 9).await.unwrap();

    CheckpointManager::load_from_disk(disk, &task_id)
        .await
        .expect("a healthy multi-object checkpoint must survive reload");
    temp_dir.close().unwrap();
}

#[tokio::test]
async fn checkpoint_integrity_rejects_a_removed_embedded_digest() {
    let (temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let manager = CheckpointManager::new(disk.clone(), task_id.clone()).await.unwrap();
    manager.update_position(2, 9).await.unwrap();

    let checkpoint_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_CHECKPOINT_FILE}");
    let bytes = disk
        .read_all(RUSTFS_META_BUCKET, &checkpoint_path)
        .await
        .expect("read checkpoint fixture");
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value["current_object_index"] = serde_json::json!(10);
    value.as_object_mut().unwrap().remove("integrity_digest");
    disk.write_all(RUSTFS_META_BUCKET, &checkpoint_path, serde_json::to_vec(&value).unwrap().into())
        .await
        .expect("write tampered checkpoint fixture");

    assert!(
        CheckpointManager::load_from_disk(disk.clone(), &task_id).await.is_err(),
        "a current checkpoint without its embedded digest must fail closed"
    );
    assert!(CheckpointManager::is_blocked(&disk, &task_id).await);
    temp_dir.close().unwrap();
}

#[tokio::test]
async fn new_checkpoint_manager_rebuilds_an_empty_snapshot() {
    let (temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let checkpoint_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_CHECKPOINT_FILE}");
    disk.write_all(RUSTFS_META_BUCKET, &checkpoint_path, EcstoreDiskBytes::new())
        .await
        .expect("write empty checkpoint fixture");

    let manager = CheckpointManager::new(disk.clone(), task_id.clone())
        .await
        .expect("a new manager must rebuild an empty checkpoint");
    manager
        .update_position(3, 11)
        .await
        .expect("rebuilt checkpoint must remain writable");
    assert!(CheckpointManager::has_checkpoint(&disk, &task_id).await);
    temp_dir.close().expect("remove empty checkpoint test directory");
}

#[tokio::test]
async fn deleted_checkpoint_is_not_recreated_by_an_old_manager() {
    let (temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let manager = CheckpointManager::new(disk.clone(), task_id.clone())
        .await
        .expect("create checkpoint manager");
    manager.cleanup().await.expect("delete checkpoint fixture");

    let error = manager
        .update_position(1, 2)
        .await
        .expect_err("an old manager must not resurrect a deleted checkpoint");
    assert!(error.to_string().contains("removed after this manager saved it"));
    assert!(!CheckpointManager::has_checkpoint(&disk, &task_id).await);
    temp_dir.close().expect("remove deleted checkpoint test directory");
}

#[cfg(unix)]
#[tokio::test]
async fn checkpoint_cleanup_leaves_no_task_specific_lock_artifact() {
    let (temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let manager = CheckpointManager::new(disk.clone(), task_id.clone())
        .await
        .expect("create checkpoint manager");
    let lock_path = Path::new(BUCKET_META_PREFIX)
        .join(format!("{task_id}_{RESUME_CHECKPOINT_FILE}"))
        .with_extension("rustfs-cas.lock");
    let lock_path = temp_dir.path().join(RUSTFS_META_BUCKET).join(lock_path);

    manager.cleanup().await.expect("delete checkpoint fixture");

    assert!(
        !lock_path.exists(),
        "successful checkpoint cleanup must not leave a task-specific lock artifact"
    );
}

#[tokio::test]
async fn an_empty_blocked_marker_still_blocks_resume_selection() {
    let (temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let manager = CheckpointManager::new(disk.clone(), task_id.clone())
        .await
        .expect("create checkpoint manager");
    let blocked_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_CHECKPOINT_BLOCKED_FILE}");
    disk.write_all(RUSTFS_META_BUCKET, &blocked_path, EcstoreDiskBytes::new())
        .await
        .expect("write empty blocked marker fixture");

    assert!(CheckpointManager::is_blocked(&disk, &task_id).await);
    assert!(CheckpointManager::is_resumable(&disk, &task_id).await.is_err());
    // Recovery requires replacing/cleaning the snapshot, then removing the
    // marker; ordinary selector retries are intentionally not an unlock path.
    manager.cleanup().await.expect("clean blocked checkpoint");
    assert!(!CheckpointManager::is_blocked(&disk, &task_id).await);
    temp_dir.close().expect("remove empty blocked marker test directory");
}

#[tokio::test]
async fn resumable_selector_skips_healthy_tasks_with_blocked_markers() {
    let (temp_dir, disk) = schema_test_disk().await;
    let tasks = [
        (ResumeUtils::generate_task_id(), EcstoreDiskBytes::new()),
        (ResumeUtils::generate_task_id(), EcstoreDiskBytes::from_static(b"blocked")),
    ];
    for (task_id, marker) in &tasks {
        ResumeManager::new(
            disk.clone(),
            task_id.clone(),
            "erasure_set".to_string(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
        )
        .await
        .expect("create healthy resume state");
        CheckpointManager::new(disk.clone(), task_id.clone())
            .await
            .expect("create healthy checkpoint");
        let checkpoint_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_CHECKPOINT_FILE}");
        let checkpoint_bytes = disk
            .read_all(RUSTFS_META_BUCKET, &checkpoint_path)
            .await
            .expect("read healthy checkpoint before blocking");
        let marker_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_CHECKPOINT_BLOCKED_FILE}");
        disk.write_all(RUSTFS_META_BUCKET, &marker_path, marker.clone())
            .await
            .expect("write blocked marker");

        assert!(ResumeUtils::get_resumable_tasks(&disk).await.is_err());
        assert_eq!(
            disk.read_all(RUSTFS_META_BUCKET, &checkpoint_path)
                .await
                .expect("read healthy checkpoint after blocking"),
            checkpoint_bytes
        );
    }
    temp_dir.close().expect("remove blocked selector test directory");
}

#[tokio::test]
async fn stale_checkpoint_manager_cannot_overwrite_newer_progress() {
    let (temp_dir, disk) = schema_test_disk().await;
    let task_id = ResumeUtils::generate_task_id();
    let first = CheckpointManager::new(disk.clone(), task_id.clone())
        .await
        .expect("create first checkpoint manager");
    let second = CheckpointManager::load_from_disk(disk.clone(), &task_id)
        .await
        .expect("load second checkpoint manager");

    second
        .update_position(4, 20)
        .await
        .expect("persist newer checkpoint progress");
    let error = first
        .update_position(1, 3)
        .await
        .expect_err("stale checkpoint manager must not overwrite newer progress");
    assert!(error.to_string().contains("newer progress"));

    let persisted = CheckpointManager::load_from_disk(disk.clone(), &task_id)
        .await
        .expect("load newer checkpoint progress")
        .get_checkpoint()
        .await;
    assert_eq!(persisted.current_bucket_index, 4);
    assert_eq!(persisted.current_object_index, 20);
    temp_dir.close().expect("remove stale manager test directory");
}

#[tokio::test]
async fn resumable_selector_isolates_future_and_corrupt_checkpoints() {
    let (temp_dir, disk) = schema_test_disk().await;
    let future_task = ResumeUtils::generate_task_id();
    let corrupt_task = ResumeUtils::generate_task_id();
    for task_id in [&future_task, &corrupt_task] {
        ResumeManager::new(
            disk.clone(),
            task_id.to_string(),
            "erasure_set".to_string(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
        )
        .await
        .expect("create resumable state fixture");
    }

    let future_path = format!("{BUCKET_META_PREFIX}/{future_task}_{RESUME_CHECKPOINT_FILE}");
    let mut future = ResumeCheckpoint::new(future_task.clone());
    future.schema_version = CURRENT_CHECKPOINT_SCHEMA + 1;
    let future_bytes = serde_json::to_vec(&future).expect("serialize future checkpoint fixture");
    disk.write_all(RUSTFS_META_BUCKET, &future_path, future_bytes.clone().into())
        .await
        .expect("write future checkpoint fixture");
    let corrupt_path = format!("{BUCKET_META_PREFIX}/{corrupt_task}_{RESUME_CHECKPOINT_FILE}");
    let corrupt_bytes = b"{truncated";
    disk.write_all(RUSTFS_META_BUCKET, &corrupt_path, corrupt_bytes.as_slice().into())
        .await
        .expect("write corrupt checkpoint fixture");

    assert!(CheckpointManager::is_resumable(&disk, &future_task).await.is_err());
    assert!(CheckpointManager::is_resumable(&disk, &corrupt_task).await.is_err());
    assert!(ResumeUtils::get_resumable_tasks(&disk).await.is_err());
    for (task_id, path, bytes) in [
        (&future_task, future_path, future_bytes),
        (&corrupt_task, corrupt_path, corrupt_bytes.to_vec()),
    ] {
        assert_eq!(
            disk.read_all(RUSTFS_META_BUCKET, &path)
                .await
                .expect("read isolated checkpoint bytes"),
            bytes
        );
        let blocked_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_CHECKPOINT_BLOCKED_FILE}");
        assert!(
            !disk
                .read_all(RUSTFS_META_BUCKET, &blocked_path)
                .await
                .expect("read checkpoint blocked marker")
                .is_empty()
        );
    }
    temp_dir.close().expect("remove selector isolation test directory");
}

#[test]
fn test_persist_throttle_batches_until_threshold() {
    let mut throttle = PersistThrottle::new();
    for _ in 0..PERSIST_EVERY_MUTATIONS - 1 {
        assert!(!throttle.record(), "must not flush below the mutation threshold");
    }
    assert!(throttle.record(), "must flush at the mutation threshold");
    throttle.mark_saved();
    assert!(!throttle.record(), "counter must reset after a save");
}

#[tokio::test]
async fn completion_persists_immediately_and_cleanup_propagates_delete_errors() {
    use super::super::{DiskOption, Endpoint, new_disk};
    use tempfile::TempDir;

    let temp_dir = TempDir::new().expect("create resume persistence test directory");
    let endpoint = Endpoint::try_from(temp_dir.path().to_string_lossy().as_ref()).expect("create test disk endpoint");
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("create resume persistence test disk");
    match disk.make_volume(RUSTFS_META_BUCKET).await {
        Ok(()) | Err(DiskError::VolumeExists) => {}
        Err(err) => panic!("create metadata volume for resume persistence test: {err}"),
    }

    let task_id = ResumeUtils::generate_task_id();
    let manager = ResumeManager::new(
        disk.clone(),
        task_id.clone(),
        "erasure_set".to_string(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
    )
    .await
    .expect("create resume manager");
    manager
        .update_progress(1, 1, 0, 0)
        .await
        .expect("buffer progress below the persistence threshold");
    manager.mark_completed().await.expect("persist completed resume state");

    let persisted = ResumeManager::load_from_disk(disk.clone(), &task_id)
        .await
        .expect("reload completed resume state")
        .get_state()
        .await;
    assert!(persisted.completed, "completion must be persisted without waiting for the throttle");
    assert_eq!(persisted.processed_objects, 1, "the completion write must include buffered progress");

    let state_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_STATE_FILE}");
    let failure = ResumeDeleteFailure::install(state_path, DiskError::DiskAccessDenied);
    let error = manager
        .cleanup()
        .await
        .expect_err("resume cleanup must propagate a real delete failure");
    assert!(matches!(error, Error::Disk(DiskError::DiskAccessDenied)));
    drop(failure);
    manager.cleanup().await.expect("resume cleanup must be retryable");
    manager
        .cleanup()
        .await
        .expect("missing resume files must be idempotent success");

    let checkpoint = CheckpointManager::new(disk.clone(), task_id.clone())
        .await
        .expect("create checkpoint manager");
    let checkpoint_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_CHECKPOINT_FILE}");
    let failure = ResumeDeleteFailure::install(checkpoint_path, DiskError::DiskAccessDenied);
    let error = checkpoint
        .cleanup()
        .await
        .expect_err("checkpoint cleanup must propagate a real delete failure");
    assert!(matches!(error, Error::Disk(DiskError::DiskAccessDenied)));
    drop(failure);
    checkpoint.cleanup().await.expect("checkpoint cleanup must be retryable");
    checkpoint
        .cleanup()
        .await
        .expect("missing checkpoint must be idempotent success");
}

#[tokio::test]
async fn checkpoint_rejects_a_task_id_mismatched_to_its_file_name() {
    use super::super::{DiskOption, Endpoint, new_disk};
    use tempfile::TempDir;

    let temp_dir = TempDir::new().expect("create checkpoint binding test directory");
    let endpoint =
        Endpoint::try_from(temp_dir.path().to_string_lossy().as_ref()).expect("create checkpoint binding test endpoint");
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("create checkpoint binding test disk");
    match disk.make_volume(RUSTFS_META_BUCKET).await {
        Ok(()) | Err(DiskError::VolumeExists) => {}
        Err(err) => panic!("create checkpoint binding metadata volume: {err}"),
    }

    let requested_task_id = ResumeUtils::generate_task_id();
    let checkpoint = ResumeCheckpoint::new(ResumeUtils::generate_task_id());
    let checkpoint_path = format!("{BUCKET_META_PREFIX}/{requested_task_id}_{RESUME_CHECKPOINT_FILE}");
    disk.write_all(
        RUSTFS_META_BUCKET,
        &checkpoint_path,
        serde_json::to_vec(&checkpoint)
            .expect("serialize mismatched checkpoint")
            .into(),
    )
    .await
    .expect("persist mismatched checkpoint");

    let error = match CheckpointManager::load_from_disk(disk, &requested_task_id).await {
        Ok(_) => panic!("checkpoint task id must be bound to its file name"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("does not match"));
    temp_dir.close().expect("remove checkpoint binding test directory");
}

#[tokio::test]
async fn test_resume_utils() {
    let task_id1 = ResumeUtils::generate_task_id();
    let task_id2 = ResumeUtils::generate_task_id();

    assert_ne!(task_id1, task_id2);
    assert_eq!(task_id1.len(), 36); // UUID length
    assert_eq!(task_id2.len(), 36);
    assert!(validate_resume_task_id(&task_id1).is_ok());
    assert!(validate_resume_task_id(&format!("pool_0_set_0_{task_id1}")).is_err());
    assert!(validate_resume_task_id(&task_id1.to_uppercase()).is_err());
}

#[tokio::test]
async fn test_get_resumable_tasks_integration() {
    use super::super::{DiskOption, Endpoint, new_disk};
    use tempfile::TempDir;

    // Create a temporary directory for testing
    let temp_dir = TempDir::new().unwrap();
    let disk_path = temp_dir.path().join("test_disk");
    std::fs::create_dir_all(&disk_path).unwrap();

    // Create a local disk for testing
    let endpoint = Endpoint::try_from(disk_path.to_string_lossy().as_ref()).unwrap();
    let disk_option = DiskOption {
        cleanup: false,
        health_check: false,
    };
    let disk = new_disk(&endpoint, &disk_option).await.unwrap();

    // Create necessary directories first (ignore if already exist)
    let _ = disk.make_volume(RUSTFS_META_BUCKET).await;
    let _ = disk.make_volume(&format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}")).await;

    // Create some test resume state files
    let task_ids = vec![
        ResumeUtils::generate_task_id(),
        ResumeUtils::generate_task_id(),
        ResumeUtils::generate_task_id(),
    ];

    // Save resume state files for each task
    for task_id in &task_ids {
        let state = ResumeState::new(
            task_id.clone(),
            "erasure_set".to_string(),
            "pool_0_set_0".to_string(),
            vec!["bucket1".to_string(), "bucket2".to_string()],
        );

        let state_data = serde_json::to_vec(&state).unwrap();
        let file_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_STATE_FILE}");

        disk.write_all(RUSTFS_META_BUCKET, &file_path, state_data.into())
            .await
            .unwrap();
    }

    // Also create some non-resume state files to test filtering
    let non_resume_files = vec![
        "other_file.txt",
        "task4_ahm_checkpoint.json",
        "task5_ahm_progress.json",
        "_ahm_resume_state.json", // Invalid: empty task ID
        "not-a-uuid_ahm_resume_state.json",
        "00000000-0000-4000-8000-000000000001_extra_ahm_resume_state.json",
    ];

    for file_name in non_resume_files {
        let file_path = format!("{BUCKET_META_PREFIX}/{file_name}");
        disk.write_all(RUSTFS_META_BUCKET, &file_path, b"test data".to_vec().into())
            .await
            .unwrap();
    }

    // Now call get_resumable_tasks to see if it finds the correct files
    let found_task_ids = ResumeUtils::get_resumable_tasks(&disk).await.unwrap();

    // Verify that only the valid resume state files are found
    assert_eq!(found_task_ids.len(), 3);
    for task_id in &task_ids {
        assert!(found_task_ids.contains(task_id), "Task ID {task_id} not found");
    }

    // Verify that invalid files are not included
    assert!(!found_task_ids.contains(&"".to_string()));
    assert!(!found_task_ids.contains(&"task4".to_string()));
    assert!(!found_task_ids.contains(&"task5".to_string()));
    assert!(!found_task_ids.contains(&"not-a-uuid".to_string()));

    let error = match ResumeManager::load_from_disk(disk.clone(), "../not-a-uuid").await {
        Ok(_) => panic!("a traversal-like task id must be rejected before reading metadata"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        Error::TaskExecutionFailed { message } if message == "Invalid resume task id"
    ));

    // Clean up
    temp_dir.close().unwrap();
}

#[tokio::test]
async fn resume_state_rejects_filename_and_json_task_id_mismatch() {
    use super::super::{DiskOption, Endpoint, new_disk};
    use tempfile::TempDir;

    let temp_dir = TempDir::new().expect("create resume mismatch test directory");
    let endpoint = Endpoint::try_from(temp_dir.path().to_string_lossy().as_ref()).expect("create test disk endpoint");
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("create resume mismatch test disk");
    match disk.make_volume(RUSTFS_META_BUCKET).await {
        Ok(()) | Err(DiskError::VolumeExists) => {}
        Err(error) => panic!("create metadata volume for resume mismatch test: {error}"),
    }

    let filename_task_id = ResumeUtils::generate_task_id();
    let state = ResumeState::new(
        ResumeUtils::generate_task_id(),
        "erasure_set".to_string(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
    );
    let path = format!("{BUCKET_META_PREFIX}/{filename_task_id}_{RESUME_STATE_FILE}");
    disk.write_all(
        RUSTFS_META_BUCKET,
        &path,
        serde_json::to_vec(&state).expect("serialize mismatched resume state").into(),
    )
    .await
    .expect("write mismatched resume state");

    let error = match ResumeManager::load_from_disk(disk.clone(), &filename_task_id).await {
        Ok(_) => panic!("resume state task id must match its filename"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        Error::TaskExecutionFailed { message } if message == "Resume state task id does not match filename"
    ));

    let checkpoint_filename_task_id = ResumeUtils::generate_task_id();
    let checkpoint = ResumeCheckpoint::new(ResumeUtils::generate_task_id());
    let checkpoint_path = format!("{BUCKET_META_PREFIX}/{checkpoint_filename_task_id}_{RESUME_CHECKPOINT_FILE}");
    disk.write_all(
        RUSTFS_META_BUCKET,
        &checkpoint_path,
        serde_json::to_vec(&checkpoint)
            .expect("serialize mismatched resume checkpoint")
            .into(),
    )
    .await
    .expect("write mismatched resume checkpoint");

    let error = match CheckpointManager::load_from_disk(disk, &checkpoint_filename_task_id).await {
        Ok(_) => panic!("resume checkpoint task id must match its filename"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        Error::TaskExecutionFailed { message } if message == "Resume checkpoint task id does not match filename"
    ));
}
