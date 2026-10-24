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
use crate::heal::BUCKET_META_PREFIX;

#[tokio::test]
async fn local_recovery_diagnostics_list_conflicts_without_quarantining_or_admitting() {
    let (_first_temp, first) = recovery_disk().await;
    let (_second_temp, second) = recovery_disk().await;
    let manager = recovery_manager(vec![first.clone(), second.clone()]);
    let conflict = root_request();
    manager
        .root_recovery
        .persist(&conflict)
        .await
        .expect("persist accepted responsibility");
    let path = format!("root-heal-{}.json", conflict.id);
    let bytes = first.read_all(RUSTFS_META_BUCKET, &path).await.expect("first owner");
    second
        .write_all(RUSTFS_META_BUCKET, &path, bytes.clone())
        .await
        .expect("duplicate owner");
    let corrupt = Uuid::new_v4().to_string();
    let corrupt_path = format!("root-heal-{corrupt}.json");
    first
        .write_all(RUSTFS_META_BUCKET, &corrupt_path, b"invalid".to_vec().into())
        .await
        .expect("malformed record");
    for _ in 0..2 {
        let diagnostics = manager.local_recovery_diagnostics().await;
        assert!(diagnostics.definitive, "{:?}", diagnostics.reason);
        assert_eq!(diagnostics.root_blocked.len(), 2);
        assert!(
            diagnostics
                .root_blocked
                .iter()
                .any(|block| block.task_id == conflict.id && block.reason.contains("Multiple root heal recovery owners"))
        );
        assert!(
            diagnostics
                .root_blocked
                .iter()
                .any(|block| block.task_id == corrupt && block.reason.contains("Invalid root heal recovery record"))
        );
        assert!(manager.heal_queue.lock().await.requests().next().is_none(), "listing cannot admit work");
        assert!(
            matches!(
                first
                    .read_all(RUSTFS_META_BUCKET, &format!("quarantined-root-heal-intent-{corrupt}.json"))
                    .await,
                Err(DiskError::FileNotFound)
            ),
            "listing must never write a quarantine marker"
        );
    }
    assert_eq!(first.read_all(RUSTFS_META_BUCKET, &path).await.expect("retained first owner"), bytes);
    assert_eq!(
        second
            .read_all(RUSTFS_META_BUCKET, &path)
            .await
            .expect("retained second owner"),
        bytes
    );
}

#[tokio::test]
async fn local_recovery_diagnostics_keep_due_and_legacy_checkpoint_markers_unchanged() {
    let (_temp, disk) = recovery_disk().await;
    match disk.make_volume(&format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}")).await {
        Ok(()) | Err(DiskError::VolumeExists) => {}
        Err(error) => panic!("checkpoint fixture directory: {error}"),
    }
    let task_id = Uuid::new_v4().to_string();
    let marker_path = format!("{BUCKET_META_PREFIX}/{task_id}_ahm_checkpoint.blocked");
    let checkpoint_path = format!("{BUCKET_META_PREFIX}/{task_id}_ahm_checkpoint.json");
    let marker = serde_json::to_vec(
        &serde_json::json!({"schema": 1, "blocked_at": 7, "revalidate_after": 0, "reason": "digest mismatch"}),
    )
    .expect("marker bytes");
    disk.write_all(RUSTFS_META_BUCKET, &marker_path, marker.clone().into())
        .await
        .expect("due marker");
    disk.write_all(RUSTFS_META_BUCKET, &checkpoint_path, b"tampered".to_vec().into())
        .await
        .expect("retained checkpoint");
    let legacy_task = Uuid::new_v4().to_string();
    let legacy_path = format!("{BUCKET_META_PREFIX}/{legacy_task}_ahm_checkpoint.blocked");
    disk.write_all(RUSTFS_META_BUCKET, &legacy_path, b"blocked".to_vec().into())
        .await
        .expect("legacy marker");
    let invalid_path = format!("{BUCKET_META_PREFIX}/invalid-task_ahm_checkpoint.blocked");
    disk.write_all(RUSTFS_META_BUCKET, &invalid_path, b"invalid name".to_vec().into())
        .await
        .expect("invalid marker name");
    let manager = recovery_manager(vec![disk.clone()]);
    for _ in 0..2 {
        let diagnostics = manager.local_recovery_diagnostics().await;
        assert!(diagnostics.definitive, "{:?}", diagnostics.reason);
        assert_eq!(diagnostics.checkpoint_blocked.len(), 3);
        assert!(
            diagnostics
                .checkpoint_blocked
                .iter()
                .any(|record| record.task_id == "invalid-task" && record.reason == "invalid_checkpoint_task_id")
        );
        let current = diagnostics
            .checkpoint_blocked
            .iter()
            .find(|record| record.task_id == task_id)
            .expect("due marker listed");
        assert_eq!(current.disk_id, None, "unformatted recovery roots have no disk UUID");
        assert_eq!(current.reason, "digest mismatch");
        assert_eq!(current.blocked_at, Some(7));
        assert_eq!(current.revalidate_after, Some(0));
        let legacy = diagnostics
            .checkpoint_blocked
            .iter()
            .find(|record| record.task_id == legacy_task)
            .expect("legacy marker listed");
        assert_eq!(legacy.reason, "legacy_or_unreadable_blocked_marker");
        assert_eq!(legacy.blocked_at, None);
        assert_eq!(
            disk.read_all(RUSTFS_META_BUCKET, &marker_path)
                .await
                .expect("due marker retained")
                .as_ref(),
            marker
        );
        assert_eq!(
            disk.read_all(RUSTFS_META_BUCKET, &checkpoint_path)
                .await
                .expect("checkpoint retained")
                .as_ref(),
            b"tampered"
        );
        assert_eq!(
            disk.read_all(RUSTFS_META_BUCKET, &legacy_path)
                .await
                .expect("legacy marker retained")
                .as_ref(),
            b"blocked"
        );
    }
}

#[tokio::test]
async fn local_recovery_diagnostics_fail_closed_when_a_local_inventory_is_unreadable() {
    let (temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk]);
    std::fs::remove_dir_all(temp.path().join(RUSTFS_META_BUCKET)).expect("remove only fixture metadata volume");
    let diagnostics = manager.local_recovery_diagnostics().await;
    assert!(!diagnostics.definitive);
    assert_eq!(diagnostics.reason.as_deref(), Some("local_recovery_inventory_unavailable"));
}
